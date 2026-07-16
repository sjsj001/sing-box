package group

import (
	"container/list"
	"context"
	"net"
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

func RegisterFailover(registry *outbound.Registry) {
	outbound.Register[option.FailoverOutboundOptions](registry, C.TypeFailover, NewFailover)
}

var (
	_ adapter.Outbound      = (*Failover)(nil)
	_ adapter.OutboundGroup = (*Failover)(nil)
)

const (
	failoverStrategyOrder = "order"
	failoverStrategyAuto  = "auto"

	failoverTableSize     = 4096
	failoverHealthPeriod  = time.Minute
	failoverDownThreshold = 3
	failoverUpThreshold   = 2

	// auto election hysteresis: a challenger must stay below this fraction of
	// the elected primary's baseline for this many consecutive health rounds.
	failoverElectRatio  = 0.8
	failoverElectStreak = 3
)

// failoverMember is one ordered candidate plus its member-level health state.
type failoverMember struct {
	tag       string
	detour    adapter.Outbound
	interrupt *interrupt.Group
	udp       bool

	mu             sync.Mutex
	baseline       *smartWindow // health-check totals; Min() is the floor
	alive          bool
	failStreak     int
	okStreak       int
	electStreak    int // consecutive rounds beating the elected primary
	lastHealthOK   time.Time
	lastHealthFail time.Time
}

func (m *failoverMember) baselineMin() (float64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.baseline.Min()
}

// failoverTargetStats is the per-(target, member) cooldown record; guarded by
// the owning failoverTarget mutex.
type failoverTargetStats struct {
	consecFail    int
	lastFail      time.Time
	cooldownUntil time.Time
	backoff       int
	needsRecovery bool
	lastRecovery  time.Time
}

type failoverTarget struct {
	mu        sync.Mutex
	key       string
	probeHost string
	port      uint16
	members   map[string]*failoverTargetStats
}

func (t *failoverTarget) stats(tag string) *failoverTargetStats {
	s, ok := t.members[tag]
	if !ok {
		s = &failoverTargetStats{}
		t.members[tag] = s
	}
	return s
}

// failoverTable is a slim LRU of per-target cooldown records.
type failoverTable struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	lru     *list.List
	max     int
}

func newFailoverTable(max int) *failoverTable {
	return &failoverTable{
		entries: make(map[string]*list.Element),
		lru:     list.New(),
		max:     max,
	}
}

func (tb *failoverTable) ensure(key, probeHost string, port uint16) *failoverTarget {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if elem, ok := tb.entries[key]; ok {
		tb.lru.MoveToFront(elem)
		target := elem.Value.(*failoverTarget)
		target.mu.Lock()
		target.port = port
		target.mu.Unlock()
		return target
	}
	target := &failoverTarget{
		key:       key,
		probeHost: probeHost,
		port:      port,
		members:   make(map[string]*failoverTargetStats),
	}
	tb.entries[key] = tb.lru.PushFront(target)
	for tb.lru.Len() > tb.max {
		oldest := tb.lru.Back()
		if oldest == nil {
			break
		}
		tb.lru.Remove(oldest)
		delete(tb.entries, oldest.Value.(*failoverTarget).key)
	}
	return target
}

type Failover struct {
	outbound.Adapter
	ctx        context.Context
	cancel     context.CancelFunc
	logger     log.ContextLogger
	outbound   adapter.OutboundManager
	connection adapter.ConnectionManager

	tags         []string
	strategy     string
	healthHost   string
	healthPort   uint16
	hedgeDelay   time.Duration
	failLimit    int
	cooldownBase time.Duration
	cooldownMax  time.Duration

	interruptExternalConnections bool

	members []*failoverMember
	byTag   map[string]*failoverMember
	table   *failoverTable
	prober  *smartProber

	elected  common.TypedValue[string] // auto strategy's current primary
	lastUsed common.TypedValue[string]

	recoveryHook func(*failoverTarget, string) // test seam; nil in production
}

func NewFailover(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.FailoverOutboundOptions) (adapter.Outbound, error) {
	if len(options.Outbounds) == 0 {
		return nil, E.New("missing outbound tags")
	}
	strategy := options.Strategy
	switch strategy {
	case "":
		strategy = failoverStrategyOrder
	case failoverStrategyOrder, failoverStrategyAuto:
	default:
		return nil, E.New("unknown strategy: ", options.Strategy)
	}
	healthCheck := options.HealthCheck
	if healthCheck == "" {
		healthCheck = smartDefaultAnchor
	}
	healthHost, healthPortStr, err := net.SplitHostPort(healthCheck)
	if err != nil {
		healthHost = healthCheck
		healthPortStr = "80"
	}
	healthPort, err := strconv.ParseUint(healthPortStr, 10, 16)
	if err != nil {
		return nil, E.Cause(err, "invalid health_check port")
	}
	if time.Duration(options.HedgeDelay) < 0 {
		return nil, E.New("invalid hedge_delay")
	}
	ctx, cancel := context.WithCancel(ctx)
	return &Failover{
		Adapter:                      outbound.NewAdapter(C.TypeFailover, tag, []string{N.NetworkTCP, N.NetworkUDP}, options.Outbounds),
		ctx:                          ctx,
		cancel:                       cancel,
		logger:                       logger,
		outbound:                     service.FromContext[adapter.OutboundManager](ctx),
		connection:                   service.FromContext[adapter.ConnectionManager](ctx),
		tags:                         options.Outbounds,
		strategy:                     strategy,
		healthHost:                   healthHost,
		healthPort:                   uint16(healthPort),
		hedgeDelay:                   time.Duration(options.HedgeDelay),
		failLimit:                    intOrDefault(options.Cooldown.FailLimit, smartDefaultFailLimit),
		cooldownBase:                 durationOrDefault(time.Duration(options.Cooldown.Base), smartDefaultCooldownBase),
		cooldownMax:                  durationOrDefault(time.Duration(options.Cooldown.Max), smartDefaultCooldownMax),
		interruptExternalConnections: options.InterruptExistConnections,
		byTag:                        make(map[string]*failoverMember),
		table:                        newFailoverTable(failoverTableSize),
		prober:                       newSmartProber(ctx, smartDefaultProbeConcurrency, smartDefaultProbeTimeout),
	}, nil
}

func (s *Failover) Start() error {
	for i, memberTag := range s.tags {
		detour, loaded := s.outbound.Outbound(memberTag)
		if !loaded {
			return E.New("outbound ", i, " not found: ", memberTag)
		}
		member := &failoverMember{
			tag:       memberTag,
			detour:    detour,
			interrupt: interrupt.NewGroup(),
			udp:       common.Contains(detour.Network(), N.NetworkUDP),
			baseline:  newSmartWindow(smartWindowCap),
			alive:     true,
		}
		s.members = append(s.members, member)
		s.byTag[memberTag] = member
	}
	return nil
}

func (s *Failover) PostStart() error {
	go s.healthLoop()
	return nil
}

func (s *Failover) Close() error {
	s.cancel()
	return nil
}

func (s *Failover) Now() string {
	if tag := s.lastUsed.Load(); tag != "" {
		return tag
	}
	order := s.memberOrder()
	if len(order) > 0 {
		return order[0]
	}
	return ""
}

func (s *Failover) All() []string {
	return s.tags
}

// memberOrder returns the preference order: declared order, with the elected
// primary moved to the front under the auto strategy.
func (s *Failover) memberOrder() []string {
	if s.strategy == failoverStrategyAuto {
		if elected := s.elected.Load(); elected != "" {
			order := make([]string, 0, len(s.tags))
			order = append(order, elected)
			for _, tag := range s.tags {
				if tag != elected {
					order = append(order, tag)
				}
			}
			return order
		}
	}
	return s.tags
}

// candidates filters the preference order for one dial. Exclusion levels are
// dropped one by one if they would empty the list (fail-open): per-target
// cooldown first, then member liveness.
func (s *Failover) candidates(target *failoverTarget, network string, now time.Time) []string {
	order := s.memberOrder()
	supports := func(tag string) bool {
		member := s.byTag[tag]
		if member == nil {
			return false
		}
		if network == N.NetworkUDP && !member.udp {
			return false
		}
		return true
	}
	isAlive := func(tag string) bool {
		member := s.byTag[tag]
		member.mu.Lock()
		defer member.mu.Unlock()
		return member.alive
	}
	var full []string
	for _, tag := range order {
		if supports(tag) {
			full = append(full, tag)
		}
	}
	if target == nil {
		var alive []string
		for _, tag := range full {
			if isAlive(tag) {
				alive = append(alive, tag)
			}
		}
		if len(alive) > 0 {
			return alive
		}
		return full
	}
	var open []string
	target.mu.Lock()
	for _, tag := range full {
		stats, tracked := target.members[tag]
		if tracked {
			if stats.cooldownUntil.After(now) {
				continue
			}
			if stats.needsRecovery {
				if s.recoveryHook != nil {
					s.recoveryHook(target, tag)
				} else {
					s.requestRecoveryProbe(target, tag)
				}
				continue
			}
		}
		if isAlive(tag) {
			open = append(open, tag)
		}
	}
	target.mu.Unlock()
	if len(open) > 0 {
		return open
	}
	// Everything cooled or down: fail open in preference order.
	var alive []string
	for _, tag := range full {
		if isAlive(tag) {
			alive = append(alive, tag)
		}
	}
	if len(alive) > 0 {
		return alive
	}
	return full
}

// dialBudget bounds one attempt so a hung member cannot consume the caller's
// whole timeout: max(3×member baseline, 1s), capped at 5s; 5s without data.
func (s *Failover) dialBudget(tag string) time.Duration {
	member := s.byTag[tag]
	if member == nil {
		return smartDefaultProbeTimeout
	}
	baseline, ok := member.baselineMin()
	if !ok || baseline <= 0 {
		return smartDefaultProbeTimeout
	}
	budget := time.Duration(3 * baseline * float64(time.Millisecond))
	if budget < time.Second {
		budget = time.Second
	}
	if budget > smartDefaultProbeTimeout {
		budget = smartDefaultProbeTimeout
	}
	return budget
}

func (s *Failover) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	metadata := adapter.ContextFrom(ctx)
	exactKey, _, probeHost, hasKey := smartKeys(metadata)
	now := time.Now()
	var target *failoverTarget
	if hasKey {
		target = s.table.ensure(exactKey, probeHost, destination.Port)
	}
	attempts := s.candidates(target, network, now)
	if len(attempts) == 0 {
		return nil, E.New("no available member")
	}
	if s.hedgeDelay > 0 && network == N.NetworkTCP && len(attempts) >= 2 && target != nil {
		return s.dialHedged(ctx, target, attempts[0], attempts[1], destination, metadata)
	}
	var lastErr error
	for _, memberTag := range attempts {
		member := s.byTag[memberTag]
		if member == nil || !common.Contains(member.detour.Network(), network) {
			continue
		}
		dialCtx, cancelDial := context.WithTimeout(ctx, s.dialBudget(memberTag))
		conn, err := member.detour.DialContext(dialCtx, network, destination)
		cancelDial()
		if err != nil {
			lastErr = err
			s.logger.ErrorContext(ctx, "failover dial via ", memberTag, ": ", err)
			if target != nil {
				s.onDialFailure(target, memberTag, now)
			}
			continue
		}
		s.lastUsed.Store(memberTag)
		if metadata != nil {
			metadata.AppendRealOutbound(memberTag)
		}
		if target != nil {
			conn = newSmartMeasureConn(conn, s.connCallbacks(target, memberTag))
		}
		return member.interrupt.NewConn(conn,
			interrupt.IsExternalConnectionFromContext(ctx),
			interrupt.IsProviderConnectionFromContext(ctx)), nil
	}
	if lastErr == nil {
		lastErr = E.New("no available member")
	}
	return nil, lastErr
}

// dialHedged sends the connection through the primary and, if no response
// byte arrives within hedge_delay (or the primary fails outright), fans the
// buffered client bytes out to the standby; the first response byte wins.
// A standby win charges the primary one dial failure toward the per-target
// cooldown (spec addendum R3): a persistently silent primary would otherwise
// tax every connection with hedge_delay forever and never be demoted. The
// charge is CAS-deduplicated with the primary's own dial-error and
// early-failure paths so one event never counts twice. Standby losers stay
// unpenalized.
func (s *Failover) dialHedged(ctx context.Context, target *failoverTarget, primaryTag, standbyTag string, destination M.Socksaddr, metadata *adapter.InboundContext) (net.Conn, error) {
	var primaryAccounted atomic.Bool
	makeCandidate := func(memberTag string, delay time.Duration, isPrimary bool) *smartRaceCandidate {
		member := s.byTag[memberTag]
		flags := &smartRaceFlags{}
		return &smartRaceCandidate{
			tag:   memberTag,
			flags: flags,
			delay: delay,
			dial: func() (net.Conn, error) {
				dialCtx, cancelDial := context.WithTimeout(ctx, s.dialBudget(memberTag))
				conn, err := member.detour.DialContext(dialCtx, N.NetworkTCP, destination)
				cancelDial()
				if err != nil {
					if !flags.abandoned.Load() && (!isPrimary || primaryAccounted.CompareAndSwap(false, true)) {
						s.onDialFailure(target, memberTag, time.Now())
					}
					return nil, err
				}
				callbacks := s.connCallbacks(target, memberTag)
				earlyFail := callbacks.onEarlyFail
				callbacks.onEarlyFail = func(err error) {
					// A hedge loser closed by the winner is not a failure.
					if !flags.abandoned.Load() && (!isPrimary || primaryAccounted.CompareAndSwap(false, true)) {
						earlyFail(err)
					}
				}
				measured := newSmartMeasureConn(conn, callbacks)
				return member.interrupt.NewConn(measured,
					interrupt.IsExternalConnectionFromContext(ctx),
					interrupt.IsProviderConnectionFromContext(ctx)), nil
			},
		}
	}
	candidates := []*smartRaceCandidate{
		makeCandidate(primaryTag, 0, true),
		makeCandidate(standbyTag, s.hedgeDelay, false),
	}
	s.logger.DebugContext(ctx, "failover hedge key=", target.key, " primary=", primaryTag, " standby=", standbyTag)
	race := newSmartRaceConn(candidates, func(winner string) {
		s.lastUsed.Store(winner)
		if winner != primaryTag {
			s.logger.Debug("failover hedge key=", target.key, " standby ", winner, " won")
			if primaryAccounted.CompareAndSwap(false, true) {
				s.onDialFailure(target, primaryTag, time.Now())
			}
		}
	})
	return race, nil
}

func (s *Failover) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	metadata := adapter.ContextFrom(ctx)
	exactKey, _, probeHost, hasKey := smartKeys(metadata)
	now := time.Now()
	var target *failoverTarget
	if hasKey {
		target = s.table.ensure(exactKey, probeHost, destination.Port)
	}
	attempts := s.candidates(target, N.NetworkUDP, now)
	var lastErr error
	for _, memberTag := range attempts {
		member := s.byTag[memberTag]
		if member == nil || !member.udp {
			continue
		}
		conn, err := member.detour.ListenPacket(ctx, destination)
		if err != nil {
			lastErr = err
			if target != nil {
				s.onDialFailure(target, memberTag, now)
			}
			continue
		}
		s.lastUsed.Store(memberTag)
		if metadata != nil {
			metadata.AppendRealOutbound(memberTag)
		}
		if target != nil {
			measured := newSmartMeasurePacketConn(conn, s.connCallbacks(target, memberTag))
			return member.interrupt.NewPacketConn(measured,
				interrupt.IsExternalConnectionFromContext(ctx),
				interrupt.IsProviderConnectionFromContext(ctx)), nil
		}
		return member.interrupt.NewPacketConn(conn,
			interrupt.IsExternalConnectionFromContext(ctx),
			interrupt.IsProviderConnectionFromContext(ctx)), nil
	}
	if lastErr == nil {
		lastErr = E.New("no available UDP member")
	}
	return nil, lastErr
}

func (s *Failover) connCallbacks(target *failoverTarget, memberTag string) smartConnCallbacks {
	return smartConnCallbacks{
		onTotal: func(float64) {
			s.onFirstByte(target, memberTag)
		},
		onEarlyFail: func(err error) {
			s.logger.Debug("failover early failure key=", target.key, " via=", memberTag, ": ", err)
			s.onDialFailure(target, memberTag, time.Now())
		},
	}
}

// onFirstByte: a response byte through the member proves the path for this
// target; the failure streak resets.
func (s *Failover) onFirstByte(target *failoverTarget, tag string) {
	target.mu.Lock()
	stats := target.stats(tag)
	stats.consecFail = 0
	if stats.needsRecovery {
		stats.needsRecovery = false
		stats.lastRecovery = time.Now()
	}
	target.mu.Unlock()
}

func (s *Failover) onDialFailure(target *failoverTarget, tag string, now time.Time) {
	target.mu.Lock()
	defer target.mu.Unlock()
	stats := target.stats(tag)
	stats.lastFail = now
	if stats.cooldownUntil.After(now) {
		return
	}
	stats.consecFail++
	if stats.consecFail < s.failLimit {
		return
	}
	if !stats.lastRecovery.IsZero() && now.Sub(stats.lastRecovery) > smartRelapseWindow {
		stats.backoff = 0
	}
	stats.backoff++
	duration := cooldownDuration(s.cooldownBase, s.cooldownMax, stats.backoff)
	stats.cooldownUntil = now.Add(duration)
	stats.needsRecovery = true
	stats.consecFail = 0
	s.logger.Info("failover target ", target.key, ": member ", tag, " cooling down for ", duration, " (backoff ", stats.backoff, ")")
}

// requestRecoveryProbe verifies a cooled-down member against the actual
// target before letting it back in; target.mu is held by the caller.
func (s *Failover) requestRecoveryProbe(target *failoverTarget, tag string) {
	probeHost := target.probeHost
	port := target.port
	key := target.key
	member := s.byTag[tag]
	if member == nil || probeHost == "" {
		return
	}
	go s.prober.schedule(key+"|"+tag, func() {
		var err error
		switch port {
		case 443:
			_, err = s.prober.probeTLS(member.detour, probeHost, port)
		case 80:
			_, err = s.prober.probeHTTP(member.detour, probeHost, port)
		default:
			err = s.prober.probeAlive(member.detour, probeHost, port)
		}
		now := time.Now()
		target.mu.Lock()
		stats := target.stats(tag)
		if err != nil {
			stats.backoff++
			stats.cooldownUntil = now.Add(cooldownDuration(s.cooldownBase, s.cooldownMax, stats.backoff))
			target.mu.Unlock()
			s.logger.Debug("failover target ", key, ": member ", tag, " recovery probe failed: ", err)
			return
		}
		stats.needsRecovery = false
		stats.consecFail = 0
		stats.lastRecovery = now
		target.mu.Unlock()
		s.logger.Info("failover target ", key, ": member ", tag, " recovered")
	})
}

// healthLoop probes every member against health_check each round,
// maintaining baselines, member liveness, and (auto strategy) the elected
// primary.
func (s *Failover) healthLoop() {
	s.runHealthRound()
	ticker := time.NewTicker(failoverHealthPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.runHealthRound()
		}
	}
}

func (s *Failover) runHealthRound() {
	if s.strategy == failoverStrategyAuto {
		s.electPrimary()
	}
	for _, member := range s.members {
		member := member
		s.prober.schedule("health|"+member.tag, func() {
			result, err := s.prober.probeAnchor(member.detour, s.healthHost, s.healthPort)
			if err != nil {
				s.noteHealthFailure(member, err)
				return
			}
			member.mu.Lock()
			member.failStreak = 0
			member.okStreak++
			member.lastHealthOK = time.Now()
			member.baseline.Push(result.totalMs)
			recovered := !member.alive && member.okStreak >= failoverUpThreshold
			if recovered {
				member.alive = true
			}
			member.mu.Unlock()
			if recovered {
				s.logger.Info("failover member ", member.tag, " is back up")
			}
		})
	}
}

// noteHealthFailure mirrors smart's anchor-down guard (spec R3-3): when the
// health endpoint fails through every member it is the endpoint that died,
// and demoting all members for it would be wrong.
func (s *Failover) noteHealthFailure(member *failoverMember, err error) {
	now := time.Now()
	member.mu.Lock()
	member.okStreak = 0
	member.failStreak++
	member.lastHealthFail = now
	overThreshold := member.alive && member.failStreak >= failoverDownThreshold
	member.mu.Unlock()
	if !overThreshold {
		return
	}
	if s.healthEndpointDown(now) {
		s.logger.Warn("failover health endpoint unreachable via every member; keeping ", member.tag, " alive: ", err)
		return
	}
	member.mu.Lock()
	demote := member.alive && member.failStreak >= failoverDownThreshold
	if demote {
		member.alive = false
	}
	member.mu.Unlock()
	if demote {
		s.logger.Warn("failover member ", member.tag, " is down: ", err)
		member.interrupt.Interrupt(s.interruptExternalConnections)
	}
}

func (s *Failover) healthEndpointDown(now time.Time) bool {
	if len(s.members) < 2 {
		return false
	}
	window := 2*failoverHealthPeriod + smartDefaultProbeTimeout
	for _, member := range s.members {
		member.mu.Lock()
		recentFail := !member.lastHealthFail.IsZero() && now.Sub(member.lastHealthFail) <= window
		recentOK := !member.lastHealthOK.IsZero() && now.Sub(member.lastHealthOK) <= window
		member.mu.Unlock()
		if recentOK || !recentFail {
			return false
		}
	}
	return true
}

// electPrimary re-evaluates the auto-strategy primary from health baselines:
// initial election takes the best available; replacing an incumbent needs a
// >20% advantage held for 3 consecutive rounds. A dead incumbent is replaced
// immediately.
func (s *Failover) electPrimary() {
	type ranked struct {
		tag      string
		baseline float64
	}
	var best *ranked
	for _, member := range s.members {
		member.mu.Lock()
		alive := member.alive
		baseline, ok := member.baseline.Min()
		member.mu.Unlock()
		if !alive || !ok {
			continue
		}
		if best == nil || baseline < best.baseline {
			best = &ranked{member.tag, baseline}
		}
	}
	if best == nil {
		return
	}
	current := s.elected.Load()
	if current == best.tag {
		for _, member := range s.members {
			member.mu.Lock()
			member.electStreak = 0
			member.mu.Unlock()
		}
		return
	}
	currentMember := s.byTag[current]
	if current == "" || currentMember == nil || !s.memberAlive(currentMember) {
		s.elected.Store(best.tag)
		s.logger.Info("failover elected primary: ", best.tag)
		return
	}
	currentBaseline, ok := currentMember.baselineMin()
	challenger := s.byTag[best.tag]
	if !ok || challenger == nil {
		return
	}
	if best.baseline < failoverElectRatio*currentBaseline {
		challenger.mu.Lock()
		challenger.electStreak++
		streak := challenger.electStreak
		challenger.mu.Unlock()
		if streak >= failoverElectStreak {
			challenger.mu.Lock()
			challenger.electStreak = 0
			challenger.mu.Unlock()
			s.elected.Store(best.tag)
			s.logger.Info("failover primary switched: ", current, " -> ", best.tag,
				" (baseline ", int(best.baseline), "ms vs ", int(currentBaseline), "ms)")
		}
	} else {
		challenger.mu.Lock()
		challenger.electStreak = 0
		challenger.mu.Unlock()
	}
}

func (s *Failover) memberAlive(member *failoverMember) bool {
	member.mu.Lock()
	defer member.mu.Unlock()
	return member.alive
}

func (s *Failover) NewConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	s.connection.NewConnection(ctx, s, conn, metadata, onClose)
}

func (s *Failover) NewPacketConnection(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	s.connection.NewPacketConnection(ctx, s, conn, metadata, onClose)
}
