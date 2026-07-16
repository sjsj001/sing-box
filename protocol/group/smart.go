package group

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

func RegisterSmart(registry *outbound.Registry) {
	outbound.Register[option.SmartOutboundOptions](registry, C.TypeSmart, NewSmart)
}

var (
	_ adapter.Outbound      = (*Smart)(nil)
	_ adapter.OutboundGroup = (*Smart)(nil)
)

const (
	smartDefaultAnchor           = "1.1.1.1:80"
	smartDefaultAnycastThreshold = 10 * time.Millisecond
	smartDefaultTolerance        = 20 * time.Millisecond
	smartDefaultImproveRatio     = 0.25
	smartDefaultImproveMin       = 30 * time.Millisecond
	smartDefaultDwell            = 5 * time.Minute
	smartDefaultConfirmations    = 2
	smartDefaultFailLimit        = 2
	smartDefaultCooldownBase     = time.Minute
	smartDefaultCooldownMax      = 15 * time.Minute
	smartDefaultProbeConcurrency = 8
	smartDefaultSampleTTL        = 15 * time.Minute
	smartDefaultProbeTimeout     = 5 * time.Second
	smartDefaultExploreInterval  = 16
	smartDefaultMaxTargets       = 8192
	smartDefaultRecordTTL        = 14 * 24 * time.Hour
	smartBaselineInterval        = time.Minute
	smartPersistInterval         = 5 * time.Minute
	smartMemberDownThreshold     = 3
	smartMemberUpThreshold       = 2
	// smartLegFactorStreak: rounds of consistent re-classification required
	// before the protocol leg factor flips — one noisy dial measurement must
	// not halve or double every remote estimate (spec R3-1).
	smartLegFactorStreak = 2

	// Adaptive in-flight behavior (spec R2): both windows derive from the
	// expected first-write→first-read total for this (target, member).
	// Failure is declared at 3× expected; the hedge standby arms earlier, at
	// 1.5× (Surge's suspicion factor), so a rescue starts before the primary
	// is condemned.
	smartEarlyFailFactor  = 3.0
	smartEarlyFailMin     = 500 * time.Millisecond
	smartHedgeFactor      = 1.5
	smartHedgeMinDelay    = 200 * time.Millisecond
	smartHedgeMaxDelay    = 2 * time.Second
	smartHedgeNoDataDelay = time.Second
)

// smartMember is one selectable member (typically a per-region urltest
// group) plus its runtime measurement state.
type smartMember struct {
	tag       string
	detour    adapter.Outbound
	interrupt *interrupt.Group
	udp       bool

	mu             sync.Mutex
	baseline       *smartWindow // anchor totals; Min() is the local→member floor
	legFactor      float64
	legStreak      int // consecutive rounds disagreeing with the current factor
	alive          bool
	failStreak     int
	okStreak       int
	lastAnchorOK   time.Time
	lastAnchorFail time.Time
}

// smartMemberSet is the immutable snapshot of the member list, published
// once at Start(); the atomic pointer keeps every reader lock-free.
type smartMemberSet struct {
	members []*smartMember
	byTag   map[string]*smartMember
	tags    []string // declared order (All() surface)
	order   []string // effective priority: preferred prefix + tags
}

type Smart struct {
	outbound.Adapter
	ctx        context.Context
	cancel     context.CancelFunc
	logger     log.ContextLogger
	outbound   adapter.OutboundManager
	connection adapter.ConnectionManager

	staticTags []string
	preferred  []string
	anchorHost string
	anchorPort uint16
	cachePath  string
	cfg        smartConfig

	interruptExternalConnections bool

	memberSet atomic.Pointer[smartMemberSet]
	table     *smartTable
	engine    *smartEngine
	prober    *smartProber

	persistMu sync.Mutex  // serializes snapshot writers (ticker vs Close)
	dirty     atomic.Bool // learned state changed since the last snapshot
	lastUsed  common.TypedValue[string]
}

func NewSmart(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.SmartOutboundOptions) (adapter.Outbound, error) {
	if len(options.Outbounds) == 0 {
		return nil, E.New("missing outbound tags")
	}
	outboundSet := make(map[string]bool)
	for _, outboundTag := range options.Outbounds {
		outboundSet[outboundTag] = true
	}
	for i, preferredTag := range options.Preferred {
		if !outboundSet[preferredTag] {
			return nil, E.New("preferred entry ", i, " not found in outbounds: ", preferredTag)
		}
	}
	anchor := options.HealthCheck
	if anchor == "" {
		anchor = smartDefaultAnchor
	}
	anchorHost, anchorPortStr, err := net.SplitHostPort(anchor)
	if err != nil {
		anchorHost = anchor
		anchorPortStr = "443"
	}
	anchorPort, err := strconv.ParseUint(anchorPortStr, 10, 16)
	if err != nil {
		return nil, E.Cause(err, "invalid anchor port")
	}
	cfg := smartConfig{
		anycastThreshold: durationOrDefault(time.Duration(options.AnycastThreshold), smartDefaultAnycastThreshold),
		tolerance:        durationOrDefault(time.Duration(options.Tolerance), smartDefaultTolerance),
		improveRatio:     options.Switch.ImproveRatio,
		improveMin:       durationOrDefault(time.Duration(options.Switch.ImproveMin), smartDefaultImproveMin),
		dwell:            durationOrDefault(time.Duration(options.Switch.Dwell), smartDefaultDwell),
		confirmations:    intOrDefault(options.Switch.Confirmations, smartDefaultConfirmations),
		failLimit:        intOrDefault(options.Cooldown.FailLimit, smartDefaultFailLimit),
		cooldownBase:     durationOrDefault(time.Duration(options.Cooldown.Base), smartDefaultCooldownBase),
		cooldownMax:      durationOrDefault(time.Duration(options.Cooldown.Max), smartDefaultCooldownMax),
		sampleTTL:        durationOrDefault(time.Duration(options.Probe.SampleTTL), smartDefaultSampleTTL),
		exploreInterval:  smartDefaultExploreInterval,
		maxTargets:       intOrDefault(options.MaxTargets, smartDefaultMaxTargets),
		recordTTL:        durationOrDefault(time.Duration(options.RecordTTL), smartDefaultRecordTTL),
	}
	if cfg.improveRatio <= 0 {
		cfg.improveRatio = smartDefaultImproveRatio
	}
	if options.ExploreInterval != nil {
		cfg.exploreInterval = *options.ExploreInterval
	}
	// Effective priority: preferred prefix, then remaining declared order.
	order := make([]string, 0, len(options.Outbounds))
	for _, preferredTag := range options.Preferred {
		if common.Contains(options.Outbounds, preferredTag) {
			order = append(order, preferredTag)
		}
	}
	for _, outboundTag := range options.Outbounds {
		if !common.Contains(order, outboundTag) {
			order = append(order, outboundTag)
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	smart := &Smart{
		Adapter:                      outbound.NewAdapter(C.TypeSmart, tag, []string{N.NetworkTCP, N.NetworkUDP}, options.Outbounds),
		ctx:                          ctx,
		cancel:                       cancel,
		logger:                       logger,
		outbound:                     service.FromContext[adapter.OutboundManager](ctx),
		connection:                   service.FromContext[adapter.ConnectionManager](ctx),
		staticTags:                   options.Outbounds,
		preferred:                    options.Preferred,
		anchorHost:                   anchorHost,
		anchorPort:                   uint16(anchorPort),
		cachePath:                    options.CachePath,
		cfg:                          cfg,
		interruptExternalConnections: options.InterruptExistConnections,
		table:                        newSmartTable(cfg.maxTargets, cfg.recordTTL),
		prober: newSmartProber(ctx,
			intOrDefault(options.Probe.Concurrency, smartDefaultProbeConcurrency),
			durationOrDefault(time.Duration(options.Probe.Timeout), smartDefaultProbeTimeout)),
	}
	smart.engine = newSmartEngine(cfg, order)
	smart.engine.emit = func(format string, args ...any) {
		smart.logger.Info(fmt.Sprintf(format, args...))
	}
	smart.engine.probeRequest = smart.requestProbe
	return smart, nil
}

func durationOrDefault(value, def time.Duration) time.Duration {
	if value <= 0 {
		return def
	}
	return value
}

func intOrDefault(value, def int) int {
	if value <= 0 {
		return def
	}
	return value
}

func (s *Smart) Start() error {
	if err := s.buildMembers(); err != nil {
		return err
	}
	if s.cachePath != "" {
		snapshot, err := smartLoadSnapshot(s.cachePath)
		switch {
		case err == nil:
			set := s.memberSet.Load()
			knownTags := make(map[string]bool)
			for _, member := range set.members {
				knownTags[member.tag] = true
			}
			snapshot.apply(s.table, knownTags, s.cfg.recordTTL, time.Now())
			for _, member := range set.members {
				member.mu.Lock()
				for _, sample := range snapshot.Baselines[member.tag] {
					member.baseline.Push(sample)
				}
				if factor := snapshot.LegFactors[member.tag]; factor > 0 {
					member.legFactor = factor
				}
				member.mu.Unlock()
			}
			s.logger.Info("loaded smart cache: ", s.table.len(), " targets")
		case os.IsNotExist(err):
		default:
			s.logger.Warn("smart cache discarded: ", err)
		}
	}
	return nil
}

// buildMembers resolves the member outbounds and publishes the immutable
// member-set snapshot.
func (s *Smart) buildMembers() error {
	members := make([]*smartMember, 0, len(s.staticTags))
	byTag := make(map[string]*smartMember, len(s.staticTags))
	for i, tag := range s.staticTags {
		detour, loaded := s.outbound.Outbound(tag)
		if !loaded {
			return E.New("outbound ", i, " not found: ", tag)
		}
		member := &smartMember{
			tag:       tag,
			detour:    detour,
			interrupt: interrupt.NewGroup(),
			udp:       common.Contains(detour.Network(), N.NetworkUDP),
			baseline:  newSmartWindow(smartWindowCap),
			legFactor: 1,
			alive:     true,
		}
		byTag[tag] = member
		members = append(members, member)
	}
	order := make([]string, 0, len(s.staticTags))
	for _, tag := range s.preferred {
		if byTag[tag] != nil {
			order = append(order, tag)
		}
	}
	for _, tag := range s.staticTags {
		if !common.Contains(order, tag) {
			order = append(order, tag)
		}
	}
	s.memberSet.Store(&smartMemberSet{members: members, byTag: byTag, tags: s.staticTags, order: order})
	s.engine.setOrder(order)
	return nil
}

func (s *Smart) PostStart() error {
	go s.baselineLoop()
	if s.cachePath != "" {
		go s.persistLoop()
	}
	return nil
}

func (s *Smart) Close() error {
	s.cancel()
	if s.cachePath != "" {
		s.saveSnapshot()
	}
	return nil
}

func (s *Smart) Now() string {
	if tag := s.lastUsed.Load(); tag != "" {
		return tag
	}
	if order := s.engine.orderNow(); len(order) > 0 {
		return order[0]
	}
	return ""
}

func (s *Smart) All() []string {
	if set := s.memberSet.Load(); set != nil {
		return set.tags
	}
	return s.staticTags
}

func (s *Smart) memberViews() smartMemberViews {
	set := s.memberSet.Load()
	views := make(smartMemberViews, len(set.members))
	for _, member := range set.members {
		member.mu.Lock()
		baseline, _ := member.baseline.Min()
		views[member.tag] = &smartMemberView{
			tag:       member.tag,
			alive:     member.alive,
			udp:       member.udp,
			baseline:  baseline,
			legFactor: member.legFactor,
		}
		member.mu.Unlock()
	}
	return views
}

func (s *Smart) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	metadata := adapter.ContextFrom(ctx)
	exactKey, aggKey, probeHost, hasKey := smartKeys(metadata)
	views := s.memberViews()
	now := time.Now()
	if !hasKey {
		return s.dialUntracked(ctx, network, destination, views)
	}
	target := s.table.ensure(exactKey, aggKey, probeHost, destination.Port, now)
	s.dirty.Store(true)
	decision := s.engine.decide(target, views, network, now)
	if len(decision.attempts) == 0 {
		if decision.reason == smartReasonTargetDown {
			return nil, E.New("target down, retry rate-limited")
		}
		return nil, E.New("no available member")
	}
	s.logger.DebugContext(ctx, "smart decide key=", exactKey, " via=", decision.attempts[0], " reason=", decision.reason)
	if decision.coldStart && network == N.NetworkTCP && len(decision.attempts) > 1 &&
		(destination.Port == 443 || (metadata != nil && metadata.Protocol == C.ProtocolTLS)) {
		return s.dialRace(ctx, target, decision.attempts, destination, views, metadata)
	}
	var lastErr error
	for attemptIndex, memberTag := range decision.attempts {
		member := s.memberSet.Load().byTag[memberTag]
		if member == nil || !common.Contains(member.detour.Network(), network) {
			continue
		}
		// Adaptive dial budget (spec B9): a hung member must not consume the
		// caller's whole timeout before the chain advances.
		dialCtx := ctx
		var cancelDial context.CancelFunc
		if timeout := s.adaptiveDialTimeout(target, memberTag, views); timeout > 0 {
			dialCtx, cancelDial = context.WithTimeout(ctx, timeout)
		}
		conn, err := member.detour.DialContext(dialCtx, network, destination)
		if cancelDial != nil {
			cancelDial()
		}
		if err != nil {
			lastErr = err
			s.logger.ErrorContext(ctx, "smart dial via ", memberTag, ": ", err)
			s.engine.onDialFailure(target, memberTag, views, network, now)
			continue
		}
		s.engine.onDialSuccess(target, memberTag, now)
		s.lastUsed.Store(memberTag)
		if metadata != nil {
			metadata.AppendRealOutbound(memberTag)
		}
		if decision.coldStart {
			// Sequential cold start covered only one member; fill in the
			// rest so the walk has data to compare (spec §3.3 step 4).
			s.backfillProbes(target, memberTag, "coldstart-fill")
		}
		if network == N.NetworkTCP {
			if standbys := s.hedgeStandbys(decision.attempts[attemptIndex+1:]); len(standbys) > 0 {
				return s.dialHedged(ctx, target, member, conn, standbys, destination,
					decision.reason != smartReasonExplore), nil
			}
		}
		measured := newSmartMeasureConn(conn, s.connCallbacks(target, memberTag, network))
		return member.interrupt.NewConn(measured,
			interrupt.IsExternalConnectionFromContext(ctx),
			interrupt.IsProviderConnectionFromContext(ctx)), nil
	}
	if lastErr == nil {
		lastErr = E.New("no available member")
	}
	return nil, lastErr
}

// adaptiveDialTimeout derives a per-attempt dial budget from history:
// max(3×median, 1s) capped at 5s (spec B9). Zero (no budget) without history.
func (s *Smart) adaptiveDialTimeout(target *smartTarget, memberTag string, views smartMemberViews) time.Duration {
	var med float64
	target.mu.Lock()
	if stats, ok := target.members[memberTag]; ok {
		med, _ = stats.window.Median()
	}
	target.mu.Unlock()
	if med <= 0 {
		if view := views[memberTag]; view != nil {
			med = view.baseline
		}
	}
	if med <= 0 {
		return 0
	}
	timeout := time.Duration(3 * med * float64(time.Millisecond))
	if timeout < time.Second {
		timeout = time.Second
	}
	if timeout > 5*time.Second {
		timeout = 5 * time.Second
	}
	return timeout
}

// expectedTotalMs is the best available estimate of one first-write→first-read
// round trip through tag for this target: the exact-level median, falling back
// to the aggregate-level (site prior) median. Zero without history.
func (s *Smart) expectedTotalMs(target *smartTarget, memberTag string) float64 {
	var med float64
	target.mu.Lock()
	if stats, ok := target.members[memberTag]; ok {
		med, _ = stats.window.Median()
	}
	aggregate := target.aggregate
	target.mu.Unlock()
	if med > 0 || aggregate == nil {
		return med
	}
	aggregate.mu.Lock()
	if stats, ok := aggregate.members[memberTag]; ok {
		med, _ = stats.window.Median()
	}
	aggregate.mu.Unlock()
	return med
}

// earlyFailWindow scales the hang/early-failure window to the path's own
// expected duration: clamp(3×expected, 500ms, 3s). Zero (keep the default
// 3s) without history.
func (s *Smart) earlyFailWindow(target *smartTarget, memberTag string) time.Duration {
	expected := s.expectedTotalMs(target, memberTag)
	if expected <= 0 {
		return 0
	}
	window := time.Duration(smartEarlyFailFactor * expected * float64(time.Millisecond))
	if window < smartEarlyFailMin {
		window = smartEarlyFailMin
	}
	if window > smartEarlyFailWindow {
		window = smartEarlyFailWindow
	}
	return window
}

// hedgeDelay is how long the primary may go without a response byte before
// the standby dials: clamp(1.5×expected, 200ms, 2s), 1s without history.
func (s *Smart) hedgeDelay(target *smartTarget, memberTag string) time.Duration {
	expected := s.expectedTotalMs(target, memberTag)
	if expected <= 0 {
		return smartHedgeNoDataDelay
	}
	delay := time.Duration(smartHedgeFactor * expected * float64(time.Millisecond))
	if delay < smartHedgeMinDelay {
		delay = smartHedgeMinDelay
	}
	if delay > smartHedgeMaxDelay {
		delay = smartHedgeMaxDelay
	}
	return delay
}

// backfillProbes schedules sample-fill probes for members that have no data
// for this target yet. Probes are staggered: many targets cold-starting at
// once would otherwise fire a probe burst whose mutual congestion poisons
// the very first sample of every member.
func (s *Smart) backfillProbes(target *smartTarget, except string, reason string) {
	target.mu.Lock()
	var missing []string
	for _, tag := range s.engine.orderNow() {
		if tag == except {
			continue
		}
		if stats, ok := target.members[tag]; !ok || stats.window.Count() == 0 {
			missing = append(missing, tag)
		}
	}
	target.mu.Unlock()
	for i, tag := range missing {
		if i == 0 {
			s.requestProbe(target, tag, reason)
			continue
		}
		tag := tag
		time.AfterFunc(time.Duration(i)*700*time.Millisecond, func() {
			s.requestProbe(target, tag, reason)
		})
	}
}

func (s *Smart) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	metadata := adapter.ContextFrom(ctx)
	exactKey, aggKey, probeHost, hasKey := smartKeys(metadata)
	views := s.memberViews()
	now := time.Now()
	if !hasKey {
		return s.listenPacketUntracked(ctx, destination, views)
	}
	target := s.table.ensure(exactKey, aggKey, probeHost, destination.Port, now)
	s.dirty.Store(true)
	decision := s.engine.decide(target, views, N.NetworkUDP, now)
	if len(decision.attempts) == 0 && decision.reason == smartReasonTargetDown {
		return nil, E.New("target down, retry rate-limited")
	}
	var lastErr error
	for _, memberTag := range decision.attempts {
		member := s.memberSet.Load().byTag[memberTag]
		if member == nil || !member.udp {
			continue
		}
		conn, err := member.detour.ListenPacket(ctx, destination)
		if err != nil {
			lastErr = err
			s.engine.onDialFailure(target, memberTag, views, N.NetworkUDP, now)
			continue
		}
		s.engine.onDialSuccess(target, memberTag, now)
		s.lastUsed.Store(memberTag)
		if metadata != nil {
			metadata.AppendRealOutbound(memberTag)
		}
		if decision.coldStart {
			s.backfillProbes(target, memberTag, "coldstart-fill")
		}
		measured := newSmartMeasurePacketConn(conn, s.connCallbacks(target, memberTag, N.NetworkUDP))
		return member.interrupt.NewPacketConn(measured,
			interrupt.IsExternalConnectionFromContext(ctx),
			interrupt.IsProviderConnectionFromContext(ctx)), nil
	}
	if lastErr == nil {
		lastErr = E.New("no available UDP member")
	}
	return nil, lastErr
}

func (s *Smart) dialUntracked(ctx context.Context, network string, destination M.Socksaddr, views smartMemberViews) (net.Conn, error) {
	metadata := adapter.ContextFrom(ctx)
	var lastErr error
	// Two passes: alive members in priority order, then — when every member
	// is marked down (e.g. the health anchor died, not the members) — fail
	// open in the same order instead of erroring all untracked traffic
	// (spec R3-3; the tracked path has the same fail-open in decide()).
	for _, ignoreAlive := range []bool{false, true} {
		if ignoreAlive && lastErr != nil {
			break
		}
		for _, memberTag := range s.engine.orderNow() {
			view := views[memberTag]
			member := s.memberSet.Load().byTag[memberTag]
			if view == nil || member == nil || (!view.alive && !ignoreAlive) || (view.alive && ignoreAlive) {
				continue
			}
			if !common.Contains(member.detour.Network(), network) {
				continue
			}
			conn, err := member.detour.DialContext(ctx, network, destination)
			if err != nil {
				lastErr = err
				continue
			}
			s.lastUsed.Store(memberTag)
			if metadata != nil {
				metadata.AppendRealOutbound(memberTag)
			}
			return member.interrupt.NewConn(conn,
				interrupt.IsExternalConnectionFromContext(ctx),
				interrupt.IsProviderConnectionFromContext(ctx)), nil
		}
	}
	if lastErr == nil {
		lastErr = E.New("no available member")
	}
	return nil, lastErr
}

func (s *Smart) listenPacketUntracked(ctx context.Context, destination M.Socksaddr, views smartMemberViews) (net.PacketConn, error) {
	metadata := adapter.ContextFrom(ctx)
	var lastErr error
	for _, ignoreAlive := range []bool{false, true} {
		if ignoreAlive && lastErr != nil {
			break
		}
		for _, memberTag := range s.engine.orderNow() {
			view := views[memberTag]
			member := s.memberSet.Load().byTag[memberTag]
			if view == nil || member == nil || !member.udp || (!view.alive && !ignoreAlive) || (view.alive && ignoreAlive) {
				continue
			}
			conn, err := member.detour.ListenPacket(ctx, destination)
			if err != nil {
				lastErr = err
				continue
			}
			s.lastUsed.Store(memberTag)
			if metadata != nil {
				metadata.AppendRealOutbound(memberTag)
			}
			return member.interrupt.NewPacketConn(conn,
				interrupt.IsExternalConnectionFromContext(ctx),
				interrupt.IsProviderConnectionFromContext(ctx)), nil
		}
	}
	if lastErr == nil {
		lastErr = E.New("no available UDP member")
	}
	return nil, lastErr
}

// hedgeStandbys filters the remaining attempt chain down to members that can
// actually carry a TCP hedge, capped at two.
func (s *Smart) hedgeStandbys(remaining []string) []string {
	set := s.memberSet.Load()
	var standbys []string
	for _, tag := range remaining {
		member := set.byTag[tag]
		if member == nil || !common.Contains(member.detour.Network(), N.NetworkTCP) {
			continue
		}
		standbys = append(standbys, tag)
		if len(standbys) == 2 {
			break
		}
	}
	return standbys
}

// dialHedged wraps an already-dialed primary connection in a hedged race
// (spec R2-2): each standby arms after an adaptive delay (or as soon as an
// earlier candidate fails outright) and the first member delivering a
// response byte carries the connection — the caller never sees the switch.
// A standby win is fast-worse-channel evidence against the primary and
// counts as one dial failure, deduplicated against the primary's own
// early-failure path through a shared flag (spec R2-4). Exploration
// primaries are exempt: losing a hedge only proves the explored member is
// slower than the incumbent, not broken.
func (s *Smart) dialHedged(ctx context.Context, target *smartTarget, primary *smartMember, primaryConn net.Conn, standbys []string, destination M.Socksaddr, penalize bool) net.Conn {
	set := s.memberSet.Load()
	primaryTag := primary.tag
	var primaryAccounted atomic.Bool
	primaryFlags := &smartRaceFlags{}
	callbacks := s.connCallbacks(target, primaryTag, N.NetworkTCP)
	primaryFail := callbacks.onEarlyFail
	callbacks.onEarlyFail = func(err error) {
		if primaryFlags.abandoned.Load() {
			return
		}
		if penalize && !primaryAccounted.CompareAndSwap(false, true) {
			return
		}
		primaryFail(err)
	}
	measured := newSmartMeasureConn(primaryConn, callbacks)
	wrapped := primary.interrupt.NewConn(measured,
		interrupt.IsExternalConnectionFromContext(ctx),
		interrupt.IsProviderConnectionFromContext(ctx))
	candidates := []*smartRaceCandidate{{
		tag:   primaryTag,
		flags: primaryFlags,
		dial:  func() (net.Conn, error) { return wrapped, nil },
	}}
	delay := s.hedgeDelay(target, primaryTag)
	for i, standbyTag := range standbys {
		member := set.byTag[standbyTag]
		if member == nil {
			continue
		}
		flags := &smartRaceFlags{}
		standbyTag := standbyTag
		candidates = append(candidates, &smartRaceCandidate{
			tag:   standbyTag,
			flags: flags,
			delay: time.Duration(i+1) * delay,
			dial: func() (net.Conn, error) {
				dialCtx := ctx
				var cancelDial context.CancelFunc
				if timeout := s.adaptiveDialTimeout(target, standbyTag, s.memberViews()); timeout > 0 {
					dialCtx, cancelDial = context.WithTimeout(ctx, timeout)
				}
				conn, err := member.detour.DialContext(dialCtx, N.NetworkTCP, destination)
				if cancelDial != nil {
					cancelDial()
				}
				if err != nil {
					if !flags.abandoned.Load() {
						s.engine.onDialFailure(target, standbyTag, s.memberViews(), N.NetworkTCP, time.Now())
					}
					return nil, err
				}
				standbyCallbacks := s.connCallbacks(target, standbyTag, N.NetworkTCP)
				earlyFail := standbyCallbacks.onEarlyFail
				standbyCallbacks.onEarlyFail = func(err error) {
					if !flags.abandoned.Load() {
						earlyFail(err)
					}
				}
				standbyMeasured := newSmartMeasureConn(conn, standbyCallbacks)
				return member.interrupt.NewConn(standbyMeasured,
					interrupt.IsExternalConnectionFromContext(ctx),
					interrupt.IsProviderConnectionFromContext(ctx)), nil
			},
		})
	}
	if len(candidates) == 1 {
		return wrapped
	}
	return newSmartRaceConn(candidates, func(winner string) {
		if winner == primaryTag {
			return
		}
		now := time.Now()
		s.engine.onDialSuccess(target, winner, now)
		s.lastUsed.Store(winner)
		s.logger.Info("smart hedge key=", target.key, " rescued by ", winner, " from ", primaryTag)
		if penalize && primaryAccounted.CompareAndSwap(false, true) {
			s.engine.onDialFailure(target, primaryTag, s.memberViews(), N.NetworkTCP, now)
		}
	})
}

// dialRace fans the connection out to the cold-start candidates; the first
// member delivering a response byte wins the target.
func (s *Smart) dialRace(ctx context.Context, target *smartTarget, attempts []string, destination M.Socksaddr, views smartMemberViews, metadata *adapter.InboundContext) (net.Conn, error) {
	candidates := make([]*smartRaceCandidate, 0, len(attempts))
	for _, memberTag := range attempts {
		member := s.memberSet.Load().byTag[memberTag]
		if member == nil {
			continue
		}
		flags := &smartRaceFlags{}
		memberTag := memberTag
		candidates = append(candidates, &smartRaceCandidate{
			tag:   memberTag,
			flags: flags,
			dial: func() (net.Conn, error) {
				conn, err := member.detour.DialContext(ctx, N.NetworkTCP, destination)
				if err != nil {
					if !flags.abandoned.Load() {
						s.engine.onDialFailure(target, memberTag, s.memberViews(), N.NetworkTCP, time.Now())
					}
					return nil, err
				}
				callbacks := s.connCallbacks(target, memberTag, N.NetworkTCP)
				earlyFail := callbacks.onEarlyFail
				callbacks.onEarlyFail = func(err error) {
					if !flags.abandoned.Load() {
						earlyFail(err)
					}
				}
				measured := newSmartMeasureConn(conn, callbacks)
				return member.interrupt.NewConn(measured,
					interrupt.IsExternalConnectionFromContext(ctx),
					interrupt.IsProviderConnectionFromContext(ctx)), nil
			},
		})
	}
	if len(candidates) == 0 {
		return nil, E.New("no race candidates")
	}
	s.logger.DebugContext(ctx, "smart race key=", target.key, " candidates=", len(candidates))
	// The winner is unknown until after this returns, so raced connections
	// cannot AppendRealOutbound: mutating metadata from the winner callback
	// would race the logging/tracker readers.
	race := newSmartRaceConn(candidates, func(winner string) {
		s.engine.onDialSuccess(target, winner, time.Now())
		s.lastUsed.Store(winner)
		s.logger.Info("smart race key=", target.key, " winner=", winner)
		// Neither the race losers nor the members outside the ≤3-candidate
		// pool produced a timing sample; without probing them the threshold
		// walk could never elect a region member that was not raced
		// (spec §3.2 ①: backfill members the race did not cover).
		s.backfillProbes(target, winner, "race-fill")
	})
	return race, nil
}

func (s *Smart) connCallbacks(target *smartTarget, memberTag string, network string) smartConnCallbacks {
	return smartConnCallbacks{
		earlyFailWindow: s.earlyFailWindow(target, memberTag),
		onTotal: func(ms float64) {
			s.engine.onPassiveSample(target, memberTag, ms, s.memberViews(), network, time.Now())
		},
		onTTFB: func(ms float64) {
			s.engine.onTTFBSample(target, memberTag, ms, s.memberViews(), network, time.Now())
		},
		onEarlyFail: func(err error) {
			s.logger.Debug("smart early failure key=", target.key, " via=", memberTag, ": ", err)
			s.engine.onDialFailure(target, memberTag, s.memberViews(), network, time.Now())
		},
	}
}

// requestProbe is invoked by the engine while it holds target.mu; the body
// runs on its own goroutine so re-acquiring the lock cannot deadlock.
func (s *Smart) requestProbe(target *smartTarget, memberTag string, reason string) {
	go s.requestProbeSync(target, memberTag, reason)
}

func (s *Smart) requestProbeSync(target *smartTarget, memberTag string, reason string) {
	member := s.memberSet.Load().byTag[memberTag]
	if member == nil {
		return
	}
	member.mu.Lock()
	alive := member.alive
	member.mu.Unlock()
	if !alive {
		return
	}
	target.mu.Lock()
	probeHost := target.probeHost
	port := target.port
	key := target.key
	target.mu.Unlock()
	if probeHost == "" {
		return
	}
	s.prober.schedule(key+"|"+memberTag, func() {
		// 443 and 80 carry protocol-correct probes that yield a timing sample
		// (spec R3-2); other ports may be server-first protocols where a
		// client-anchored timing would be fiction, so they only get a
		// liveness check.
		if port == 443 || port == 80 {
			var result smartProbeResult
			var err error
			if port == 443 {
				result, err = s.prober.probeTLS(member.detour, probeHost, port)
			} else {
				result, err = s.prober.probeHTTP(member.detour, probeHost, port)
			}
			if err != nil {
				s.logger.Debug("smart probe key=", key, " via=", memberTag, " reason=", reason, " failed: ", err)
			} else {
				s.logger.Debug("smart probe key=", key, " via=", memberTag, " reason=", reason, " total=", int(result.totalMs), "ms")
			}
			s.engine.onProbeResult(target, memberTag, result.totalMs, err, s.memberViews(), N.NetworkTCP, time.Now())
			s.dirty.Store(true)
			return
		}
		err := s.prober.probeAlive(member.detour, probeHost, port)
		s.engine.onProbeResult(target, memberTag, 0, err, s.memberViews(), N.NetworkTCP, time.Now())
		s.dirty.Store(true)
	})
}

// baselineLoop periodically measures each member against the anycast anchor,
// maintaining the local→member baseline floor, the protocol leg factor, and
// member-level liveness.
func (s *Smart) baselineLoop() {
	s.runBaselineRound()
	ticker := time.NewTicker(smartBaselineInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.runBaselineRound()
		}
	}
}

func (s *Smart) runBaselineRound() {
	for _, member := range s.memberSet.Load().members {
		member := member
		s.prober.schedule("baseline|"+member.tag, func() {
			result, err := s.prober.probeAnchor(member.detour, s.anchorHost, s.anchorPort)
			if err != nil {
				s.noteAnchorFailure(member, err)
				return
			}
			s.noteAnchorSuccess(member, result)
		})
	}
}

// noteAnchorFailure records one failed health round. Member demotion is
// suppressed while the anchor looks unreachable through every member: that is
// anchor trouble, not member trouble, and marking everyone down would error
// out all untracked traffic (spec R3-3).
func (s *Smart) noteAnchorFailure(member *smartMember, err error) {
	now := time.Now()
	member.mu.Lock()
	member.okStreak = 0
	member.failStreak++
	member.lastAnchorFail = now
	overThreshold := member.alive && member.failStreak >= smartMemberDownThreshold
	member.mu.Unlock()
	if !overThreshold {
		return
	}
	if s.anchorDown(now) {
		s.logger.Warn("smart anchor unreachable via every member; keeping ", member.tag, " alive: ", err)
		return
	}
	member.mu.Lock()
	demote := member.alive && member.failStreak >= smartMemberDownThreshold
	if demote {
		member.alive = false
	}
	member.mu.Unlock()
	if demote {
		s.logger.Warn("smart member ", member.tag, " is down: ", err)
		member.interrupt.Interrupt(s.interruptExternalConnections)
	}
}

// anchorDown reports whether the health endpoint itself looks dead: at least
// two members were probed recently and every one of them failed with no
// recent success. A single-member group cannot distinguish the two cases and
// keeps the plain demotion behavior.
func (s *Smart) anchorDown(now time.Time) bool {
	members := s.memberSet.Load().members
	if len(members) < 2 {
		return false
	}
	window := 2*smartBaselineInterval + smartDefaultProbeTimeout
	for _, member := range members {
		member.mu.Lock()
		recentFail := !member.lastAnchorFail.IsZero() && now.Sub(member.lastAnchorFail) <= window
		recentOK := !member.lastAnchorOK.IsZero() && now.Sub(member.lastAnchorOK) <= window
		member.mu.Unlock()
		if recentOK || !recentFail {
			return false
		}
	}
	return true
}

func (s *Smart) noteAnchorSuccess(member *smartMember, result smartProbeResult) {
	member.mu.Lock()
	member.failStreak = 0
	member.okStreak++
	member.lastAnchorOK = time.Now()
	member.baseline.Push(result.totalMs)
	if classified, definitive := legFactorFor(result); definitive {
		if classified == member.legFactor {
			member.legStreak = 0
		} else {
			member.legStreak++
			if member.legStreak >= smartLegFactorStreak {
				member.legFactor = classified
				member.legStreak = 0
			}
		}
	}
	recovered := !member.alive && member.okStreak >= smartMemberUpThreshold
	if recovered {
		member.alive = true
	}
	baseline, _ := member.baseline.Min()
	member.mu.Unlock()
	s.dirty.Store(true)
	if recovered {
		s.logger.Info("smart member ", member.tag, " is back up")
	}
	s.logger.Debug("smart baseline ", member.tag, " total=", int(result.totalMs), "ms floor=", int(baseline), "ms")
}

func (s *Smart) persistLoop() {
	ticker := time.NewTicker(smartPersistInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			// Dirty-gated: idle instances skip the periodic write.
			if s.dirty.Swap(false) {
				s.saveSnapshot()
			}
		}
	}
}

func (s *Smart) saveSnapshot() {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	snapshot := smartSnapshotFromTable(s.table, s.memberSet.Load().members)
	if err := smartSaveSnapshot(s.cachePath, snapshot); err != nil {
		s.logger.Warn("smart cache save failed: ", err)
	}
}

func (s *Smart) NewConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	s.connection.NewConnection(ctx, s, conn, metadata, onClose)
}

func (s *Smart) NewPacketConnection(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	s.connection.NewPacketConnection(ctx, s, conn, metadata, onClose)
}
