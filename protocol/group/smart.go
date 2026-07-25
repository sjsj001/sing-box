package group

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
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
	"github.com/sagernet/sing/service/pause"
)

func RegisterSmart(registry *outbound.Registry) {
	outbound.Register[option.SmartOutboundOptions](registry, C.TypeSmart, NewSmart)
}

var (
	_ adapter.OutboundGroup           = (*Smart)(nil)
	_ adapter.InterfaceUpdateListener = (*Smart)(nil)
)

const (
	healthHealthy int8 = iota
	healthSuspect
	healthLocalDown
)

const (
	confLow int8 = iota
	confNormal
)

// Health damping constants (G3). A node is declared dead only on sustained
// evidence, and readmitted only on sustained health, so an intermittently
// lossy / RST-injected link cannot flap the whole node (and clear every sticky)
// every ping interval — the churn that most violates the "don't hop nodes" goal.
const (
	healthOKStreak   = 2                // consecutive clean pings to readmit a downed node
	healthHardStreak = 2                // consecutive hard-error pings to declare a node down
	linkDeathWindow  = 60 * time.Second // two distinct hosts failing within this = the link, not the targets
	flapWindow       = 2 * time.Minute  // a down within this of recovering escalates the re-entry penalty
	maxFlapLevel     = 4                // cap on the escalation
	localSlowConfirm = 2                // consecutive inflated rounds before penalizing a node's local leg (G5)
)

type nodeRuntime struct {
	outbound      adapter.Outbound
	tag           string
	cleanPriority uint16
	biasMs        int
	localMin      *rollingMin
	health        int8
	suppressCount int
	lastSample    pingSample
	interrupt     *interrupt.Group
	// probeGen names the probe's socket-pool partition; bumping it forces a
	// fresh session after a "probe session died but path alive" verdict (§4).
	probeGen atomic.Uint64

	// Health damping state (G3), all guarded by Smart.mu.
	pingFailStreak int       // consecutive hard-error ping rounds
	pingOKStreak   int       // consecutive clean pings accrued while downed
	flapLevel      int       // re-entry penalty: raises the OK streak needed to readmit
	lastUpAt       time.Time // last localDown→healthy transition, for flap accounting
	lastFailHost   string    // host of the most recent live-conn hard failure (link-death gate)
	lastFailAt     time.Time // when that failure happened

	// Local-leg degradation state (G5), guarded by Smart.mu.
	localSlowStreak int     // consecutive inflated-but-alive ping rounds
	localPenaltyMs  float64 // additive ranking penalty while degraded (the measured inflation)

	// dataFailStreak counts consecutive failed data-partition keepalives. Purely
	// for reporting — it feeds no routing decision (see noteDataPath).
	dataFailStreak int

	everProbedOK bool // has any probe ever succeeded (else the server likely lacks the sbprobe fork) (G10)
}

// localDegraded reports whether this node's local leg is currently penalized.
// Derived from the penalty rather than tracked as its own flag so the two can
// never disagree.
func (nr *nodeRuntime) localDegraded() bool { return nr.localPenaltyMs > 0 }

// requiredOKStreak is how many consecutive clean pings a downed node needs to be
// readmitted, escalated by the flap penalty so a chronically flapping link stays
// out until it genuinely stabilizes (G3 exponential re-entry).
func (nr *nodeRuntime) requiredOKStreak() int {
	return healthOKStreak + nr.flapLevel
}

// noteDown records a localDown transition. A node that goes down again soon
// after recovering escalates its flap penalty; one that had been stably healthy
// starts fresh.
func (nr *nodeRuntime) noteDown(now time.Time) {
	if !nr.lastUpAt.IsZero() && now.Sub(nr.lastUpAt) < flapWindow {
		if nr.flapLevel < maxFlapLevel {
			nr.flapLevel++
		}
	} else {
		nr.flapLevel = 0
	}
	nr.pingOKStreak = 0
	nr.pingFailStreak = 0
}

// noteUp records a return to healthy and clears the transient streak state.
func (nr *nodeRuntime) noteUp(now time.Time) {
	nr.lastUpAt = now
	nr.pingOKStreak = 0
	nr.pingFailStreak = 0
	nr.suppressCount = 0
	nr.clearLinkFails()
}

// linkDead records a live-connection hard failure to host and reports whether
// the client→node link itself now looks dead, i.e. a SECOND DISTINCT host failed
// within linkDeathWindow. A single dead/black-holed target only ever repeats the
// same host, so it can never fell a healthy node on its own (G3 link-death gate).
//
// Only the most recent failure needs remembering: the gate trips at two distinct
// hosts, so one remembered host plus the incoming one is the whole decision.
// `now` is a parameter (not time.Now()) to keep linkDeathWindow — an explicitly
// un-calibrated threshold — testable without sleeping out the window.
func (nr *nodeRuntime) linkDead(host string, now time.Time) bool {
	if host == "" {
		return false // unattributable failure: says nothing about the link
	}
	prev, prevAt := nr.lastFailHost, nr.lastFailAt
	nr.lastFailHost, nr.lastFailAt = host, now
	// Inclusive bound, matching the `at.Before(now-window)` eviction this replaced:
	// a failure exactly linkDeathWindow old still counts.
	return prev != "" && prev != host && now.Sub(prevAt) <= linkDeathWindow
}

// clearLinkFails drops the link-death evidence: any success proves the link is
// up, so earlier failures were the targets' fault, not the link's.
func (nr *nodeRuntime) clearLinkFails() {
	nr.lastFailHost, nr.lastFailAt = "", time.Time{}
}

// probeKeyFor is the isolation key for a node's regular measurement probes at a
// given generation. Bumping the generation moves probes to a fresh session.
func probeKeyFor(gen uint64) string {
	return fmt.Sprintf("https://smart-probe-%d:443", gen)
}

type hostNodeStat struct {
	remote       ewma
	blockedUntil time.Time
	failCount    int
	backoff      int // block backoff level: 1→30s, 2→2m, 3+→10m (§5)
	outlierCount int // consecutive remote outliers; 2 in a row = real degradation (§3, G4)
}

// backoffDur is the block TTL for a given escalation level (§5). Repeated
// failures of the same (node,host) back off exponentially instead of retrying
// every 30s.
func backoffDur(level int) time.Duration {
	switch {
	case level <= 1:
		return 30 * time.Second
	case level == 2:
		return 2 * time.Minute
	default:
		return 10 * time.Minute
	}
}

// How many failures against one (node,host) arm its block, by evidence
// strength. A dial or probe that never connected can be a target hiccup, so it
// takes two (§5). A live connection that was established and then hard-failed is
// strong enough on its own — that single-error block is what makes an abruptly
// dead node cost one blip instead of a whole ping interval (§4 failover).
const (
	hostFailThreshold = 2
	liveFailThreshold = 1
)

// noteFail records a failure against this (node,host) and reports whether it
// armed the block. Shared by the dial-error, refresh-probe and live-connection
// paths so all three count against one budget; they differ only in the threshold
// they pass, by how much the failure proves (see hostFailThreshold above).
func (hs *hostNodeStat) noteFail(now time.Time, threshold int) bool {
	hs.failCount++
	if hs.failCount < threshold {
		return false
	}
	return hs.block(now)
}

// block escalates and arms the block on a (node,host) stat, reporting whether it
// actually armed one.
//
// An already-armed block is left alone. A single bad target commonly fails many
// times in a burst — every open connection to it reports once, and a page holds
// several — and re-arming per report would ratchet the backoff to its 10-minute
// ceiling within one page load, over evidence that is really a single event.
// Escalation is meant to measure "failed AGAIN after we let it back in", which
// is precisely a failure arriving when no block is armed.
func (hs *hostNodeStat) block(now time.Time) bool {
	if hs.blockedUntil.After(now) {
		return false
	}
	hs.backoff++
	hs.blockedUntil = now.Add(backoffDur(hs.backoff))
	return true
}

// clearBlock lifts the block after a successful probe (probe-before-unblock)
// and decays the escalation so a recovered path returns to the short TTL.
func (hs *hostNodeStat) clearBlock() {
	hs.blockedUntil = time.Time{}
	if hs.backoff > 0 {
		hs.backoff--
	}
	hs.failCount = 0
}

type stickyEntry struct {
	node       string
	reason     stickyReason
	confidence int8
	lastSwitch time.Time
	// Sustained-lead tracking: the current challenger and how many consecutive
	// evaluations it has beaten the sticky by margin (§3 anti-flap).
	challenger     string
	challengeCount int
	// challengeGen is the evidence generation at which challengeCount last
	// advanced, so re-judging the SAME measurements cannot advance it again.
	challengeGen uint64
}

type smartParams struct {
	probeInterval       time.Duration
	pingInterval        time.Duration
	idleTimeout         time.Duration
	minDwell            time.Duration
	handshakeVerdict    time.Duration
	firstConnectTimeout time.Duration
	maxAge              time.Duration
	queueDelayMs        float64
	congestSuppressMax  int
	sel                 selectParams
	cdn                 cdnParams
	interruptExternal   bool
}

// Smart is a per-destination selector group: it splits each request's latency
// into local (client→proxy) and remote (proxy→target) legs, measures both, and
// picks a node per destination with a sticky/hysteresis policy tuned against
// node flapping. See tingly-riding-parrot-final.md.
type Smart struct {
	outbound.Adapter
	ctx        context.Context
	logger     log.ContextLogger
	outbound   adapter.OutboundManager
	connection adapter.ConnectionManager
	pause      pause.Manager
	tags       []string
	nodeOpts   map[string]option.SmartNodeOptions
	region     *regionModel
	params     smartParams

	mu         sync.Mutex
	nodes      []*nodeRuntime
	nodeByTag  map[string]*nodeRuntime
	hostStats  map[string]map[string]*hostNodeStat
	sticky     map[string]*stickyEntry
	cdnClass   map[string]bool
	cdnClassAt map[string]time.Time
	active     map[string]time.Time
	tcpPort    map[string]uint16        // last TCP dial port per host, for refresh probing (G16)
	measure    map[string]*measureState // first-connect measurement bookkeeping
	measuring  int                      // hosts currently measuring, for the concurrency valve

	lastValveWarn time.Time // rate-limits the concurrency-valve warning

	// evidenceGen counts measurement arrivals. It exists because
	// reEvaluateStickyLocked has two callers on two different clocks — the
	// refresh round, which always brings new samples, and the per-dial degraded
	// fast path, which usually re-judges the SAME ones. The sustained-lead gate
	// asks for two INDEPENDENT observations, so it must count generations of
	// evidence, not invocations.
	//
	// Deliberately global rather than per-host: a selection reads both per-host
	// evidence (the remote EWMA) and member-wide evidence (the local baseline and
	// degradation penalty, which is exactly what the degraded fast path reacts
	// to), and one counter covers both. The cost is that a ping can advance the
	// gate for a host whose own probes failed that round — over-counting, in the
	// same direction as the bug this fixes but bounded to one round, and that
	// host's node is being blocked by those probe failures anyway.
	evidenceGen uint64

	verifyCounter uint64 // fresh-session key counter for handshake verdicts

	close   chan struct{}
	started bool
}

// Data-mux warmth constants (G8/G9). The smart group's regular probes ride
// isolated socket-pool partitions, so they never keep the real data connections
// warm; these dial the DEFAULT partition (empty isolation key) to do that.
const (
	statusInterval    = 60 * time.Second // heartbeat cadence; switches are their own events, so this needn't be fast
	keepaliveInterval = 60 * time.Second // data-mux keepalive cadence (< typical NAT idle eviction)
	prewarmDials      = 2                // parallel data dials per member to warm the mux at start / after a network change
)

func NewSmart(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.SmartOutboundOptions) (adapter.Outbound, error) {
	if len(options.Providers) > 0 || options.UseAllProviders {
		return nil, E.New("smart: outbound providers are not supported yet; list outbounds explicitly")
	}
	if len(options.Outbounds) == 0 {
		return nil, E.New("smart: missing outbounds")
	}
	nodeOpts := make(map[string]option.SmartNodeOptions)
	for _, n := range options.Nodes {
		nodeOpts[n.Tag] = n
	}
	s := &Smart{
		Adapter:    outbound.NewAdapter(C.TypeSmart, tag, []string{N.NetworkTCP, N.NetworkUDP}, options.Outbounds),
		ctx:        ctx,
		logger:     logger,
		outbound:   service.FromContext[adapter.OutboundManager](ctx),
		connection: service.FromContext[adapter.ConnectionManager](ctx),
		pause:      service.FromContext[pause.Manager](ctx),
		tags:       options.Outbounds,
		nodeOpts:   nodeOpts,
		region:     buildRegionModel(options.Regions),
		params:     resolveParams(options),
		nodeByTag:  make(map[string]*nodeRuntime),
		hostStats:  make(map[string]map[string]*hostNodeStat),
		sticky:     make(map[string]*stickyEntry),
		cdnClass:   make(map[string]bool),
		cdnClassAt: make(map[string]time.Time),
		active:     make(map[string]time.Time),
		tcpPort:    make(map[string]uint16),
		measure:    make(map[string]*measureState),
		close:      make(chan struct{}),
	}
	return s, nil
}

func resolveParams(o option.SmartOutboundOptions) smartParams {
	pick := func(v time.Duration, def time.Duration) time.Duration {
		if v == 0 {
			return def
		}
		return v
	}
	pickU16 := func(v uint16, def float64) float64 {
		if v == 0 {
			return def
		}
		return float64(v)
	}
	probeInterval := pick(time.Duration(o.ProbeInterval), 45*time.Second)
	tolerance := pickU16(o.Tolerance, 20)
	cdnEnter := pickU16(o.CDNThreshold, 25)
	quorum := o.CDNQuorum
	if quorum == 0 {
		quorum = 2
	}
	suppressMax := int(o.CongestSuppressMax)
	if suppressMax == 0 {
		suppressMax = 3
	}
	switchPct := o.SwitchMargin
	if switchPct == 0 {
		switchPct = 15
	}
	return smartParams{
		probeInterval:       probeInterval,
		pingInterval:        pick(time.Duration(o.PingInterval), 5*time.Second),
		idleTimeout:         pick(time.Duration(o.IdleTimeout), 30*time.Minute),
		minDwell:            pick(time.Duration(o.MinDwell), 30*time.Second),
		handshakeVerdict:    pick(time.Duration(o.HandshakeVerdictTimeout), 8*time.Second),
		firstConnectTimeout: 3 * time.Second,
		maxAge:              2 * probeInterval,
		queueDelayMs:        pickU16(o.QueueDelayThreshold, 200),
		congestSuppressMax:  suppressMax,
		sel: selectParams{
			toleranceMs: tolerance,
			cdnBandMs:   pickU16(o.CDNBand, 15),
			switchPct:   switchPct,
		},
		cdn: cdnParams{
			enterMs: cdnEnter,
			exitMs:  cdnEnter + 10,
			quorum:  quorum,
		},
		interruptExternal: o.InterruptExistConnections,
	}
}

// biasBoundMs is the absolute cap on a node's bias_ms manual offset (§0.3).
const biasBoundMs = 100

// clampBias bounds a configured bias_ms to ±biasBoundMs.
func clampBias(v int) int {
	if v > biasBoundMs {
		return biasBoundMs
	}
	if v < -biasBoundMs {
		return -biasBoundMs
	}
	return v
}

func (s *Smart) Start() error {
	for i, tag := range s.tags {
		detour, loaded := s.outbound.Outbound(tag)
		if !loaded {
			return E.New("smart: outbound ", i, " not found: ", tag)
		}
		if detour.Type() != C.TypeNaive {
			return E.New("smart: member ", tag, " is ", detour.Type(), ", but smart requires naive members")
		}
		clean := uint16(100)
		bias := 0
		if opt, ok := s.nodeOpts[tag]; ok {
			if opt.CleanPriority != 0 {
				clean = opt.CleanPriority
			}
			// bias_ms is a small manual thumb-on-the-scale, bounded to ±100ms so a
			// mis-typed value cannot silently hijack every per-host selection by
			// swamping the measured local+remote totals (G14).
			bias = clampBias(opt.BiasMs)
			if bias != opt.BiasMs {
				s.logger.Warn("smart: node ", tag, " bias_ms ", opt.BiasMs, " out of range, clamped to ", bias)
			}
		}
		nr := &nodeRuntime{
			outbound:      detour,
			tag:           tag,
			cleanPriority: clean,
			biasMs:        bias,
			localMin:      newRollingMin(90 * time.Second),
			health:        healthHealthy,
			interrupt:     interrupt.NewGroup(),
		}
		s.nodes = append(s.nodes, nr)
		s.nodeByTag[tag] = nr
	}
	return nil
}

func (s *Smart) PostStart() error {
	s.mu.Lock()
	s.started = true
	s.mu.Unlock()
	go s.pingLoop()
	go s.refreshLoop()
	go s.statusLoop()
	go s.keepaliveLoop()
	go s.prewarm() // warm the data mux so the first request isn't a cold burst (G9)
	return nil
}

// statusLoop periodically logs a concise per-node health/baseline heartbeat at
// Info (so a long-running log always carries the liveness timeline), and the
// verbose per-host sticky/remote dump at Debug. Individual sticky switches are
// their own Info events (G15), so the Info heartbeat stays a fixed-size line
// regardless of how many hosts are active.
func (s *Smart) statusLoop() {
	ticker := time.NewTicker(statusInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.close:
			return
		case <-ticker.C:
			s.logger.Info("smart nodes: ", s.nodeStatusString())
			s.logger.Debug("smart sticky: ", s.stickyStatusString())
		}
	}
}

// nodeStatusString is the fixed-size per-node health/baseline heartbeat.
func (s *Smart) nodeStatusString() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var b strings.Builder
	for _, nr := range s.nodes {
		base := -1.0
		if m, ok := nr.localMin.min(now); ok {
			base = m
		}
		fmt.Fprintf(&b, "%s[health=%d local=%.0fms", nr.tag, nr.health, base)
		if nr.localDegraded() {
			fmt.Fprintf(&b, " +%.0fpen", nr.localPenaltyMs)
		}
		fmt.Fprintf(&b, "] ")
	}
	return b.String()
}

// stickyStatusString is the verbose per-host sticky + remote-EWMA dump, bounded
// so a many-host process can't build a multi-KB line under the lock (G6).
func (s *Smart) stickyStatusString() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var b strings.Builder
	const maxStickyLines = 20
	shown := 0
	for host, e := range s.sticky {
		if shown >= maxStickyLines {
			fmt.Fprintf(&b, "|| (+%d more)", len(s.sticky)-shown)
			break
		}
		shown++
		cdn := ""
		if s.cdnClass[host] {
			cdn = " CDN"
		}
		fmt.Fprintf(&b, "|| %s→%s(reason=%s conf=%d%s) remote{", host, e.node, e.reason, e.confidence, cdn)
		for _, nr := range s.nodes {
			if hs := s.hostStatLocked(host, nr.tag, false); hs != nil && hs.remote.has {
				fresh := "s"
				if hs.remote.state(now, s.params.maxAge) == sampleFresh {
					fresh = "f"
				}
				fmt.Fprintf(&b, "%s=%.0f%s ", nr.tag, hs.remote.value, fresh)
			}
		}
		fmt.Fprintf(&b, "} ")
	}
	return b.String()
}

func (s *Smart) Close() error {
	s.mu.Lock()
	if s.started {
		close(s.close)
		s.started = false
	}
	s.mu.Unlock()
	return nil
}

// Now returns "" because a smart group has no single current node — the choice
// is per-destination. Returning "" (as load balance does) makes the connection
// tracker stop descending here and instead show each connection's real member
// via the per-connection real-outbound chain (NG4); returning a fixed node
// would mislabel every connection in the dashboard as that one node.
func (s *Smart) Now() string {
	return ""
}

func (s *Smart) All() []string { return s.tags }

// NodeStatus is an observability snapshot of one member: whether it is currently
// healthy and its rolling-min local baseline (BaselineMs = -1 before any
// successful ping). Used by tests and available for a status panel.
type NodeStatus struct {
	Tag        string
	Healthy    bool
	BaselineMs float64
}

func (s *Smart) DebugNodes() []NodeStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	out := make([]NodeStatus, 0, len(s.nodes))
	for _, nr := range s.nodes {
		st := NodeStatus{Tag: nr.tag, Healthy: nr.health == healthHealthy, BaselineMs: -1}
		if m, ok := nr.localMin.min(now); ok {
			st.BaselineMs = m
		}
		out = append(out, st)
	}
	return out
}

func (s *Smart) InterfaceUpdated() {
	if s.pause.IsDevicePaused() || s.pause.IsNetworkPaused() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Local baseline is a client-network property: reset it on a network change
	// (§4). Keep remote stats and sticky (proxy→target is unaffected). The health
	// damping state is also cleared — a network change is not a node flap.
	for _, nr := range s.nodes {
		nr.localMin = newRollingMin(90 * time.Second)
		nr.health = healthHealthy
		nr.suppressCount = 0
		nr.pingFailStreak = 0
		nr.pingOKStreak = 0
		nr.flapLevel = 0
		// Zero, not now: a network change is not a recovery, so the next down
		// must not be counted as a flap against it.
		nr.lastUpAt = time.Time{}
		nr.clearLinkFails()
		nr.localSlowStreak = 0
		nr.localPenaltyMs = 0
	}
	// The naive members close all connections on a network change, so re-warm the
	// data mux in the background before the first request pays the cold burst (G9).
	go s.prewarm()
}

func (s *Smart) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	metadata := adapter.ContextFrom(ctx)
	key := keyOf(metadata)
	// Retry on dial failure across distinct nodes so an abruptly dead node is
	// transparent — the request completes on a backup within the same call
	// instead of surfacing an error (§4 failover). Bounded to len(nodes) tries.
	var failed map[string]bool
	var lastErr error
	for attempt := 0; attempt < len(s.nodes) && attempt < 3; attempt++ {
		nr := s.pickNode(ctx, key, network, destination.Port, failed)
		if nr == nil {
			break
		}
		conn, err := nr.outbound.DialContext(ctx, network, destination)
		if err == nil {
			if metadata != nil {
				metadata.AppendRealOutbound(nr.tag)
			}
			// Wrap so an early hard failure (naive dials optimistically) is fed
			// back as evidence against this (host,node), then hand to the
			// interrupt group.
			node := nr
			host := key.host
			fc := &failoverConn{Conn: conn, report: func(e error) { s.reportConnFailure(node, host, e) }}
			return nr.interrupt.NewConn(fc, interrupt.IsExternalConnectionFromContext(ctx), interrupt.IsProviderConnectionFromContext(ctx)), nil
		}
		s.onDialError(key, nr)
		if failed == nil {
			failed = make(map[string]bool)
		}
		failed[nr.tag] = true
		lastErr = err
		s.logger.ErrorContext(ctx, "smart: dial via ", nr.tag, " failed, retrying: ", err)
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, E.New("smart: no available outbound for ", destination)
}

func (s *Smart) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	metadata := adapter.ContextFrom(ctx)
	key := keyOf(metadata)
	nr := s.pickNode(ctx, key, N.NetworkUDP, destination.Port, nil)
	if nr == nil {
		return nil, E.New("smart: no available outbound for ", destination)
	}
	if metadata != nil {
		metadata.AppendRealOutbound(nr.tag)
	}
	conn, err := nr.outbound.ListenPacket(ctx, destination)
	if err != nil {
		s.onDialError(key, nr)
		s.logger.ErrorContext(ctx, "smart: listen via ", nr.tag, " failed: ", err)
		return nil, err
	}
	return nr.interrupt.NewPacketConn(conn, interrupt.IsExternalConnectionFromContext(ctx), interrupt.IsProviderConnectionFromContext(ctx)), nil
}

func (s *Smart) NewConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	s.connection.NewConnection(ctx, s, conn, metadata, onClose)
}

func (s *Smart) NewPacketConnection(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	s.connection.NewPacketConnection(ctx, s, conn, metadata, onClose)
}

// pickNode selects a node for a destination. Everything is keyed per host (not
// per eTLD+1) so that a multi-region provider — e.g. every leaseweb location
// under leaseweb.net — is optimal per location, not forced onto one node.
// Fast path: a live sticky, or an immediate engine pick if the host is already
// measured. Otherwise a synchronous first-connect measurement runs so the very
// first request to a new host already lands on the geographically optimal node
// (or the CDN-preferred clean node), instead of fallback-then-correct (§0.3,§3,§5).
func (s *Smart) pickNode(ctx context.Context, key destKey, network string, port uint16, exclude map[string]bool) *nodeRuntime {
	s.mu.Lock()
	var probePort uint16
	if key.host != "" {
		s.active[key.host] = time.Now()
		s.trimHostsLocked()
		// Record the probe port from TCP dials only. UDP flows share this host's
		// sticky but their port is a UDP port; probing it with a TCP connect would
		// falsely fail and block the host, so UDP never sets the refresh port (G16).
		if network == N.NetworkTCP && port != 0 {
			s.tcpPort[key.host] = port
		}
		// The remote probe is a server-side TCP connect, so it needs a TCP port —
		// the one this dial is using for TCP, or whatever a previous TCP flow to
		// this host learned. A UDP-only host (STUN/WebRTC) has none, and probing
		// its UDP port over TCP would fail on every node, costing the caller the
		// full first-connect timeout for a measurement that can never succeed.
		// Leave probePort zero there: the pick falls through to the stable
		// region/fallback choice instead of stalling (G16, extended to the
		// first-connect path).
		probePort = s.tcpPort[key.host]
	}
	if nr := s.stickyNodeLocked(key.host, network, exclude); nr != nil {
		// A locally-degraded node has its sticky re-judged through the ORDINARY
		// hysteresis path (margin + sustained-lead + MinDwell) before being used.
		// The penalty still moves traffic off it, but it cannot bypass the
		// anti-flap gates, and the request never falls through to a synchronous
		// probe — the two failure modes of the earlier bare re-pick (G5, review fix).
		if nr.localDegraded() {
			s.reEvaluateStickyLocked(key.host, network)
			// Re-read through the same exclude-aware accessor: the re-evaluation
			// judges the whole node set, so it can land on a node that already
			// failed this very request. Returning nil here just falls through to
			// selectFreshLocked, which honours exclude.
			nr = s.stickyNodeLocked(key.host, network, exclude)
		}
		if nr != nil {
			s.mu.Unlock()
			return nr
		}
	}
	if nr := s.selectFreshLocked(key.host, network, exclude); nr != nil {
		s.mu.Unlock()
		return nr
	}
	s.mu.Unlock()

	// Slow path: measure the new host from every usable node before deciding, so
	// the first request is already optimal. firstConnectMeasure returns
	// immediately when no node is usable (dead target), so this never becomes a
	// per-request stall (G12).
	if key.host != "" && probePort != 0 {
		s.firstConnectMeasure(ctx, key.host, probePort, network)
		s.mu.Lock()
		if nr := s.selectFreshLocked(key.host, network, exclude); nr != nil {
			s.mu.Unlock()
			return nr
		}
		s.mu.Unlock()
	}

	// Fallback: nothing measurable (all probes failed / empty key / no TCP port).
	s.mu.Lock()
	defer s.mu.Unlock()
	nr := s.fallbackNodeLocked(key.host, network, exclude)
	if nr != nil && key.host != "" {
		// Nothing was measurable, so this pick is a guess: confLow, to be
		// corrected on the first evaluation that has real samples.
		s.setStickyLocked(key.host, nr.tag, reasonTotal, confLow)
	}
	return nr
}

// stickyNodeLocked returns the host's current sticky node, or nil if it has no
// sticky or the sticky node is not selectable for this request right now.
func (s *Smart) stickyNodeLocked(host, network string, exclude map[string]bool) *nodeRuntime {
	e, ok := s.sticky[host]
	if !ok {
		return nil
	}
	nr := s.nodeByTag[e.node]
	if nr == nil || !s.selectable(nr, host, network, exclude) {
		return nil
	}
	return nr
}

// selectFreshLocked runs the engine over the current snapshot and commits the
// sticky, returning a node only if there is a rankable fresh candidate.
func (s *Smart) selectFreshLocked(host, network string, exclude map[string]bool) *nodeRuntime {
	if host == "" {
		return nil
	}
	cands := s.snapshotLocked(host, network, exclude)
	isCDN := s.cdnClass[host]
	if tag, reason, ok := selectNode(cands, isCDN, s.params.sel); ok {
		// This runs when the previous sticky node was momentarily unselectable,
		// so the host's measurement standing carries over to its replacement.
		s.setStickyLocked(host, tag, reason, s.carriedConfidenceLocked(host))
		return s.nodeByTag[tag]
	}
	return nil
}

// measureState is the per-host first-connect bookkeeping: single-flight for
// concurrent siblings, plus when the last attempt finished so a retry burst
// cannot re-pay for it.
type measureState struct {
	done chan struct{} // non-nil while a measurement is in flight
	last time.Time     // when the last measurement finished
}

const (
	// firstConnectCooldown suppresses a repeat measurement for a host that was
	// just measured and yielded nothing selectable. Reaching this function again
	// so soon means the previous fan-out did not help, so re-running it only
	// re-pays firstConnectTimeout — which is exactly what the dial retry loop
	// used to do, once per attempt, before the request finally errored.
	firstConnectCooldown = 10 * time.Second
	// firstConnectConcurrency caps how many hosts may measure at once. It is a
	// safety valve, not a routine throttle: a page's third-party hosts arrive
	// spread over a few hundred ms and each measurement takes well under a
	// second, so ordinary browsing never reaches it. Only a pathological burst
	// does — and there, taking the fallback pick (which the next refresh round
	// corrects) beats a hundred-way probe fan-out in which every request stalls
	// for the full timeout. It fires visibly, at Warn, rather than silently
	// degrading selection quality.
	firstConnectConcurrency = 16
	// valveWarnInterval bounds how often that warning is emitted.
	valveWarnInterval = time.Minute
)

// firstConnectMeasure synchronously probes the remote leg to a brand-new host
// from every usable node (single-flighted per host, bounded by firstConnectTimeout)
// and classifies CDN, so the caller's re-select is optimal on the first request.
//
// The usable filter also gives "the target is dead" handling for free: when every
// node is blocked for this host, there is nothing to probe and this returns at
// once, so a page with a dead resource does not stall each request for the probe
// timeout. Blocks expire on their own TTL, which is what un-freezes the host (G12).
// ctx is the requesting connection's context. It bounds only the WAIT: a caller
// that goes away stops blocking, while the fan-out it started runs to
// completion, because the samples are for the host, not for that one request —
// a sibling may already be waiting on them, and the next request certainly
// wants them.
func (s *Smart) firstConnectMeasure(ctx context.Context, host string, port uint16, network string) {
	s.mu.Lock()
	st := s.measure[host]
	if st != nil {
		if ch := st.done; ch != nil {
			// A sibling is already measuring this host: wait for its result
			// rather than duplicating the fan-out.
			s.mu.Unlock()
			s.awaitMeasure(ctx, ch)
			return
		}
		if time.Since(st.last) < firstConnectCooldown {
			s.mu.Unlock()
			return
		}
	}
	// Compute the probe set BEFORE the single-flight bookkeeping: an empty set must
	// not publish an in-flight entry, or concurrent siblings for the same host would
	// wait out the timeout for a measurement that never runs (review fix).
	nodes := make([]*nodeRuntime, 0, len(s.nodes))
	for _, nr := range s.nodes {
		if s.selectable(nr, host, network, nil) {
			nodes = append(nodes, nr)
		}
	}
	if len(nodes) == 0 {
		s.mu.Unlock()
		return
	}
	if s.measuring >= firstConnectConcurrency {
		// Rate-limited: a closed valve rejects every arriving host, so logging per
		// skip would bury the one fact worth knowing under hundreds of identical
		// lines — at exactly the moment the log needs to be readable.
		warn := time.Since(s.lastValveWarn) >= valveWarnInterval
		if warn {
			s.lastValveWarn = time.Now()
		}
		s.mu.Unlock()
		if warn {
			s.logger.Warn("smart: skipping first-connect measurement (", host,
				" and any others arriving now), ", firstConnectConcurrency,
				" already in flight; falling back until a slot frees")
		}
		return
	}
	if st == nil {
		st = &measureState{}
		s.measure[host] = st
	}
	ch := make(chan struct{})
	st.done = ch
	s.measuring++
	s.mu.Unlock()

	// The fan-out owns its own goroutine so that the owner waits exactly the way
	// a sibling does. Doing the work inline would tie the measurement's lifetime
	// — and the single-flight bookkeeping that releases the waiters — to whichever
	// request happened to arrive first.
	go s.runMeasure(host, port, nodes, st, ch)
	s.awaitMeasure(ctx, ch)
}

// runMeasure probes host from every node in the set, then releases the
// single-flight entry. Probes hang off s.ctx, not any request's context, for the
// reason given on firstConnectMeasure.
func (s *Smart) runMeasure(host string, port uint16, nodes []*nodeRuntime, st *measureState, ch chan struct{}) {
	var wg sync.WaitGroup
	for _, nr := range nodes {
		nr := nr
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(s.ctx, s.params.firstConnectTimeout)
			remote, err := probeRemote(ctx, nr.outbound, host, port, probeKeyFor(nr.probeGen.Load()))
			cancel()
			if err != nil {
				return
			}
			s.mu.Lock()
			hs := s.hostStatLocked(host, nr.tag, true)
			hs.remote.update(time.Now(), remote, 0.25, s.params.maxAge)
			hs.failCount = 0
			hs.outlierCount = 0 // synchronous fresh baseline: not a degradation streak
			s.noteEvidenceLocked()
			s.mu.Unlock()
		}()
	}
	wg.Wait()

	s.mu.Lock()
	s.reclassifyCDNLocked(host)
	// st is held by pointer, so releasing it stays correct even if the host was
	// evicted from the map while the fan-out was running.
	st.done = nil
	st.last = time.Now()
	s.measuring--
	close(ch)
	s.mu.Unlock()
}

// awaitMeasure blocks until the measurement finishes, the requesting connection
// goes away, or a backstop expires. The backstop covers a probe that ignores its
// deadline; without it a stuck fan-out would hold every later request for this
// host as well.
func (s *Smart) awaitMeasure(ctx context.Context, ch <-chan struct{}) {
	backstop := time.NewTimer(s.params.firstConnectTimeout + time.Second)
	defer backstop.Stop()
	select {
	case <-ch:
	case <-ctx.Done():
	case <-backstop.C:
	}
}

// selectable is THE answer to "may this node carry a request for host on network
// right now". Every path that filters nodes — the sticky fast path, the
// first-connect probe set, the engine snapshot, and the fallback — goes through
// it, so a node can never be selectable on one path and not another. Three
// independently-drifting copies of this predicate is what let a dial retry be
// handed back the node that had just failed.
//
// exclude carries the per-request retry set (nodes that already failed this
// dial); pass nil outside DialContext's retry loop.
func (s *Smart) selectable(nr *nodeRuntime, host, network string, exclude map[string]bool) bool {
	return !exclude[nr.tag] && nr.aliveFor(network) && !s.blockedLocked(nr, host)
}

// aliveFor is the node-level half of selectable: not declared dead, and it
// speaks this network.
func (nr *nodeRuntime) aliveFor(network string) bool {
	return nr.health != healthLocalDown && common.Contains(nr.outbound.Network(), network)
}

// blockedLocked is the per-(node,host) half of selectable: this pairing is
// serving out a failure backoff (§5).
func (s *Smart) blockedLocked(nr *nodeRuntime, host string) bool {
	hs := s.hostStatLocked(host, nr.tag, false)
	return hs != nil && hs.blockedUntil.After(time.Now())
}

// fallbackNodeLocked is the last-resort pick when nothing is rankable. It
// relaxes selectable's clauses in order of how much information each carries:
// a per-host block first (if EVERY node is blocked for this host the block no
// longer discriminates), then liveness (a node believed dead still beats
// returning no route at all). Each tier prefers a region primary.
func (s *Smart) fallbackNodeLocked(host, network string, exclude map[string]bool) *nodeRuntime {
	tiers := []func(*nodeRuntime) bool{
		func(nr *nodeRuntime) bool { return s.selectable(nr, host, network, exclude) },
		func(nr *nodeRuntime) bool { return !exclude[nr.tag] && nr.aliveFor(network) },
		func(nr *nodeRuntime) bool {
			return !exclude[nr.tag] && common.Contains(nr.outbound.Network(), network)
		},
	}
	for _, ok := range tiers {
		if nr := s.preferPrimaryLocked(ok); nr != nil {
			return nr
		}
	}
	return nil
}

// preferPrimaryLocked returns the first matching node, upgrading to a region
// primary if one matches.
func (s *Smart) preferPrimaryLocked(ok func(*nodeRuntime) bool) *nodeRuntime {
	var first *nodeRuntime
	for _, nr := range s.nodes {
		if !ok(nr) {
			continue
		}
		if s.region.primary[s.region.region[nr.tag]] == nr.tag {
			return nr
		}
		if first == nil {
			first = nr
		}
	}
	return first
}

func (s *Smart) snapshotLocked(host, network string, exclude map[string]bool) []nodeCandidate {
	now := time.Now()
	cands := make([]nodeCandidate, 0, len(s.nodes))
	for _, nr := range s.nodes {
		c := nodeCandidate{
			tag:            nr.tag,
			cleanPriority:  nr.cleanPriority,
			biasMs:         nr.biasMs,
			localPenaltyMs: nr.localPenaltyMs,
			usable:         s.selectable(nr, host, network, exclude),
		}
		if m, ok := nr.localMin.min(now); ok {
			c.localMin = m
		}
		if hs := s.hostStatLocked(host, nr.tag, false); hs != nil {
			c.remoteEWMA = hs.remote.value
			c.remoteFresh = hs.remote.state(now, s.params.maxAge) == sampleFresh
		}
		s.region.annotate(&c)
		cands = append(cands, c)
	}
	return cands
}

// noteEvidenceLocked records that a new measurement landed. Call it wherever a
// sample enters the state a selection reads — the local baseline (ping) and the
// per-(host,node) remote EWMA (refresh, first-connect). It is the input clock
// for the sustained-lead gate; nothing else consumes it.
func (s *Smart) noteEvidenceLocked() { s.evidenceGen++ }

// carriedConfidenceLocked is the confidence a REPLACEMENT sticky for host starts
// at. A host whose sticky node became momentarily unselectable — a per-host
// probe block, or a member that doesn't speak this network — keeps its entry
// while the request is routed around it. That host is exactly as well measured
// as it was a moment ago, so the replacement inherits its confidence. Starting
// it at confLow instead threw away the dwell timer and the sustained-lead
// requirement, and the choice flipped back undamped as soon as the block
// expired: one probe hiccup, two switches.
//
// The fallback path deliberately does NOT use this — an unmeasured last-resort
// pick must stay confLow so the next evaluation can correct it immediately.
func (s *Smart) carriedConfidenceLocked(host string) int8 {
	if e, ok := s.sticky[host]; ok {
		return e.confidence
	}
	return confLow
}

func (s *Smart) setStickyLocked(host, tag string, reason stickyReason, conf int8) {
	if host == "" {
		return
	}
	now := time.Now()
	if e, ok := s.sticky[host]; ok && e.node == tag {
		e.reason = reason
		return
	}
	// A fresh selection that replaces a different node is a switch — the event
	// that matters for the "don't hop nodes" goal, so it's logged at Info with
	// the winning dimension; a first-ever assignment stays at Debug (G15).
	if e, ok := s.sticky[host]; ok {
		s.logger.Info("smart: ", host, " switched ", e.node, " -> ", tag, " by=", reason)
	} else {
		s.logger.Debug("smart: ", host, " -> ", tag, " by=", reason)
	}
	s.sticky[host] = &stickyEntry{
		node: tag, reason: reason, confidence: conf,
		lastSwitch: now,
	}
}

// maxActiveHosts bounds how many hosts may hold learned state at once.
// idleTimeout is a TIME bound, not a SIZE one, and this group is built for
// "everything goes through the proxy": keyOf falls back to a per-IP key for
// destinations that carry no domain, so a peer-heavy workload (P2P, a scan, a
// chatty game) can mint thousands of distinct hosts well inside one idle window,
// each holding a sticky, a CDN class, a probe port and a per-node stat map.
//
// Measured cost with four members: ~1.15 KB per host, so this cap is ~19 MB
// worst case, and the per-round sort it feeds costs ~0.6 ms under the lock.
// Neither is the real ceiling — refreshActive can only re-probe on the order of
// a thousand hosts per round, so anything past that keeps its sticky but stops
// being re-optimised. A larger table therefore does not buy better routing; it
// buys not re-paying the ~0.5 s first-connect measurement when a cold host is
// revisited. Sized generously on that basis: high enough that ordinary
// multi-device browsing never reaches it, low enough to stay a bound.
const maxActiveHosts = 16384

// hostEvictBatch is how far below the cap a trim goes. Trimming in batches
// amortises the O(n log n) sweep over that many insertions — at the cap that is
// well under a microsecond per dial — so the common case, a dial on a host set
// nowhere near the cap, costs a single length check.
const hostEvictBatch = 1024

// trimHostsLocked enforces maxActiveHosts by dropping the least recently used
// hosts. The host being dialled was just marked active, so it is always the most
// recent and can never be the one evicted.
func (s *Smart) trimHostsLocked() {
	if len(s.active) <= maxActiveHosts {
		return
	}
	type entry struct {
		host string
		last time.Time
	}
	all := make([]entry, 0, len(s.active))
	for host, last := range s.active {
		all = append(all, entry{host, last})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].last.Before(all[j].last) })
	target := maxActiveHosts - hostEvictBatch
	evicted := 0
	for _, e := range all {
		if len(s.active) <= target {
			break
		}
		if s.evictHostLocked(e.host) {
			evicted++
		}
	}
	s.logger.Debug("smart: host table over ", maxActiveHosts, ", evicted ", evicted,
		" least-recently-used, now ", len(s.active))
}

// evictHostLocked drops every learned entry for a host across all the learning
// maps, and reports whether it did. Keys in these maps are only ever created for
// hosts that were made active by a pickNode, so evicting on active-eviction
// keeps them all bounded (G6).
//
// A host with a measurement in flight is kept: the fan-out writes its samples
// through hostStatLocked(create: true), which would rebuild hostStats for a host
// that no longer has an active entry — and nothing sweeps those, so the very
// bookkeeping meant to bound this state would leak it instead.
func (s *Smart) evictHostLocked(host string) bool {
	if st := s.measure[host]; st != nil && st.done != nil {
		return false
	}
	delete(s.active, host)
	delete(s.hostStats, host)
	delete(s.sticky, host)
	delete(s.cdnClass, host)
	delete(s.cdnClassAt, host)
	delete(s.tcpPort, host)
	delete(s.measure, host)
	return true
}

func (s *Smart) hostStatLocked(host, tag string, create bool) *hostNodeStat {
	if host == "" {
		return nil
	}
	m := s.hostStats[host]
	if m == nil {
		if !create {
			return nil
		}
		m = make(map[string]*hostNodeStat)
		s.hostStats[host] = m
	}
	hs := m[tag]
	if hs == nil && create {
		hs = &hostNodeStat{}
		m[tag] = hs
	}
	return hs
}

// onDialError records a real-connection failure against the (host,node) stat;
// two within the window block that (node,host) for a backoff TTL (§5, v1).
func (s *Smart) onDialError(key destKey, nr *nodeRuntime) {
	s.mu.Lock()
	defer s.mu.Unlock()
	hs := s.hostStatLocked(key.host, nr.tag, true)
	if hs == nil {
		return
	}
	hs.noteFail(time.Now(), hostFailThreshold)
	// Drop sticky so the next dial re-selects around the failing node.
	if e, ok := s.sticky[key.host]; ok && e.node == nr.tag {
		delete(s.sticky, key.host)
	}
}

// downNodeLocked marks a node localDown, records the transition for flap
// accounting, drops every sticky pointing at it so new requests reroute at once,
// and interrupts the node's live connections when configured. Caller holds s.mu
// and must have confirmed the node was not already down. This is the single
// down ritual shared by the hard-error ping path, the handshake verdict (G2),
// and the link-death gate (G3), so all three treat existing/new connections
// consistently.
func (s *Smart) downNodeLocked(nr *nodeRuntime, now time.Time, reason string) {
	if !nr.everProbedOK {
		// A member that has never once answered a probe is most likely running a
		// stock naive server (no sbprobe branch) or a version mismatch, not a dead
		// link — call that out so it isn't misdiagnosed as node death (G10).
		reason += "; note: never answered a probe — verify this member's server runs the sbprobe-capable naive build"
	}
	s.logger.Warn("smart: node ", nr.tag, " localDown (", reason, ")")
	nr.health = healthLocalDown
	nr.noteDown(now)
	for h, e := range s.sticky {
		if e.node == nr.tag {
			delete(s.sticky, h)
		}
	}
	if s.params.interruptExternal {
		nr.interrupt.Interrupt(true)
	}
}

// reportConnFailure is invoked by failoverConn on the first hard error of a live
// data connection to host. The failure is real evidence against this
// (host,node): block it and drop the host's sticky so the retry reroutes off it
// at once. But a single dead/black-holed target must not fell the whole node —
// only when distinct hosts fail within the window (a genuinely dead or lossy
// client→node link) is the node marked down. This gate is what stops a
// single-target RST/loss from flapping the node and clearing every sticky each
// ping interval (G3).
func (s *Smart) reportConnFailure(nr *nodeRuntime, host string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	// Mid-stream failure for this host: block this (host,node) and drop only this
	// host's sticky, so the retry avoids the failing node without disturbing
	// every other host's tenure on it (G3 per-host degrade).
	if host != "" {
		// Same bookkeeping as the dial and probe paths — one failCount budget per
		// (node,host), just armed at a lower threshold because a live connection
		// dying is stronger evidence than a dial that never completed.
		if hs := s.hostStatLocked(host, nr.tag, true); hs != nil {
			hs.noteFail(now, liveFailThreshold)
		}
		if e, ok := s.sticky[host]; ok && e.node == nr.tag {
			delete(s.sticky, host)
		}
	}
	if nr.health == healthLocalDown {
		return // already down
	}
	if !nr.linkDead(host, now) {
		return // only one target failing so far: blame the target, not the link
	}
	s.downNodeLocked(nr, now, "link down: multiple targets failing; last: "+err.Error())
}

func (s *Smart) pingLoop() {
	ticker := time.NewTicker(s.params.pingInterval)
	defer ticker.Stop()
	s.pingRound()
	for {
		select {
		case <-s.close:
			return
		case <-ticker.C:
			if s.pause.IsDevicePaused() || s.pause.IsNetworkPaused() {
				continue
			}
			s.pingRound()
		}
	}
}

func (s *Smart) pingRound() {
	s.mu.Lock()
	nodes := append([]*nodeRuntime(nil), s.nodes...)
	s.mu.Unlock()
	var wg sync.WaitGroup
	for _, nr := range nodes {
		nr := nr
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.pingOnce(nr)
		}()
	}
	wg.Wait()
}

func (s *Smart) pingOnce(nr *nodeRuntime) {
	s.mu.Lock()
	timeout := s.pingTimeoutLocked(nr)
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(s.ctx, timeout)
	rtt, err := probePing(ctx, nr.outbound, probeKeyFor(nr.probeGen.Load()))
	timedOut := ctx.Err() == context.DeadlineExceeded
	cancel()
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil {
		nr.everProbedOK = true
		nr.localMin.add(now, rtt)
		s.noteEvidenceLocked()
		m, _ := nr.localMin.min(now)
		nr.lastSample = pingSample{ok: true, rttMs: rtt, localMin: m}
		nr.pingFailStreak = 0
		// Any clean ping clears the transient failure streaks; congestion
		// suppression and link-failure evidence don't survive a success.
		nr.suppressCount = 0
		nr.clearLinkFails()
		if nr.health == healthLocalDown {
			// Damped readmission: a downed node returns to healthy only after
			// enough consecutive clean pings — escalated by the flap penalty — so
			// a lossy link that yields one lucky probe can't flap it back up (G3).
			nr.pingOKStreak++
			if nr.pingOKStreak < nr.requiredOKStreak() {
				return
			}
			s.logger.Info("smart: node ", nr.tag, " healthy again")
			// Record the recovery moment ONLY here, on the real localDown→healthy
			// transition — not on every clean ping — so noteDown can tell a node
			// that just recovered (flap) from one that has been stably healthy (G3).
			nr.noteUp(now)
		}
		nr.health = healthHealthy
		s.evaluateLocalDegradationLocked(nr, rtt, m)
		return
	}
	nr.lastSample = pingSample{ok: false}
	nr.pingOKStreak = 0
	if !timedOut {
		// Hard error (connection refused/reset/no route): likely dead, but a lone
		// injected RST is not death — require healthHardStreak consecutive hard
		// errors before downing the node, so an intermittent RST can't flap a
		// healthy node (G3). The actively-used host still reroutes in ~1 blip via
		// reportConnFailure's per-host block.
		nr.pingFailStreak++
		if nr.pingFailStreak < healthHardStreak {
			if nr.health == healthHealthy {
				nr.health = healthSuspect
			}
			return
		}
		if nr.health != healthLocalDown {
			s.downNodeLocked(nr, now, err.Error())
		}
		return
	}
	// Timeout: could be congestion, not death — use the common-mode judgment. A
	// timeout is not a hard error, so it doesn't advance the hard-error streak.
	nr.pingFailStreak = 0
	if nr.health == healthLocalDown {
		// A black-holed link times out; keep an already-downed node down instead of
		// promoting it back to (selectable) suspect. Readmission only comes through
		// the damped clean-ping path above, so a dead node can't oscillate
		// down→suspect→down and be picked during each suspect window (review fix).
		return
	}
	s.evaluateSuspectLocked(nr)
}

func (s *Smart) pingTimeoutLocked(nr *nodeRuntime) time.Duration {
	base := 0.0
	if m, ok := nr.localMin.min(time.Now()); ok {
		base = m
	}
	t := time.Duration(4*base) * time.Millisecond
	if t < time.Second {
		t = time.Second
	}
	return t
}

// evaluateSuspectLocked applies the common-mode / differential judgment (§4).
func (s *Smart) evaluateSuspectLocked(nr *nodeRuntime) {
	others := make([]pingSample, 0, len(s.nodes))
	for _, o := range s.nodes {
		if o == nr {
			continue
		}
		others = append(others, o.lastSample)
	}
	nr.health = healthSuspect
	if localCongested(others, s.params.queueDelayMs) && nr.suppressCount < s.params.congestSuppressMax {
		nr.suppressCount++
		return
	}
	// No live witness, or suppressed past the bound → generous handshake verdict.
	nr.suppressCount = 0
	go s.handshakeVerdict(nr)
}

// evaluateLocalDegradationLocked applies the differential local-leg penalty
// (G5). When a node's ping is persistently inflated above its own baseline while
// a clean witness proves the inflation is not shared client-side congestion, the
// node is member-wide penalized so ranking reflects the degraded local leg
// immediately — instead of leaving every host stuck on it for ~90s until the
// rolling-min baseline catches up and its total finally rises. The penalty is
// self-releasing: once the baseline rises to the new level, or the leg recovers,
// the inflation falls below threshold and it clears.
func (s *Smart) evaluateLocalDegradationLocked(nr *nodeRuntime, rtt, localMin float64) {
	inflation := rtt - localMin
	if inflation <= s.params.queueDelayMs || !s.hasCleanLocalWitnessLocked(nr) {
		nr.localSlowStreak = 0
		nr.localPenaltyMs = 0
		return
	}
	nr.localSlowStreak++
	if nr.localSlowStreak < localSlowConfirm {
		return
	}
	nr.localPenaltyMs = inflation // nonzero penalty IS the degraded state
}

// hasCleanLocalWitnessLocked reports whether some OTHER node's last ping is
// present and NOT inflated — evidence a local-leg slowdown is specific to nr,
// not shared client-side congestion (which would slow every node's local leg).
func (s *Smart) hasCleanLocalWitnessLocked(nr *nodeRuntime) bool {
	for _, o := range s.nodes {
		if o == nr {
			continue
		}
		if o.lastSample.ok && o.lastSample.rttMs-o.lastSample.localMin <= s.params.queueDelayMs {
			return true
		}
	}
	return false
}

func (s *Smart) handshakeVerdict(nr *nodeRuntime) {
	// Verdict on a FRESH session (unique isolation key) so it is independent of
	// the regular probe session — this separates "probe session died" from
	// "path died" (§4). Requires the cronet-go isolation-key fork.
	s.mu.Lock()
	s.verifyCounter++
	vkey := fmt.Sprintf("https://smart-verify-%d:443", s.verifyCounter)
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(s.ctx, s.params.handshakeVerdict)
	_, err := probePing(ctx, nr.outbound, vkey)
	cancel()

	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil {
		// Path is alive on a brand-new session. If the regular probe session had
		// silently died (NAT/idle eviction), rotate its generation so subsequent
		// pings use a fresh session and recover, instead of looping on the dead
		// one. Congestion (path alive but slow) is also covered: stay suspect,
		// the next regular ping recovers it.
		nr.probeGen.Add(1)
		return
	}
	// The node may have recovered (a ping succeeded) or already been downed by
	// another path during the 8s handshake; only act on a still-suspect node so a
	// stale verdict can't fell a node that is healthy again.
	if nr.health != healthSuspect {
		return
	}
	// A fresh-session handshake failing after the common-mode judgment already
	// ruled out shared congestion is high-confidence path death, not a slow leg:
	// down it like a hard error — interrupting the now-black-holed live
	// connections (when configured) instead of leaving them to hang until the
	// application times out (G2).
	s.downNodeLocked(nr, time.Now(), "handshake verdict timeout")
}

// keepaliveLoop periodically dials each member's DATA partition (empty
// isolation key) so the real mux connections stay warm and their NAT mappings
// stay open. Without it, only the isolated probe sessions are kept alive, and
// the main data session can be silently idle-evicted — making the first request
// after a quiet period hang until the application times out (G8).
func (s *Smart) keepaliveLoop() {
	ticker := time.NewTicker(keepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.close:
			return
		case <-ticker.C:
			if s.pause.IsDevicePaused() || s.pause.IsNetworkPaused() {
				continue
			}
			s.noteDataPath(s.dataDialRound(1, keepaliveInterval/2))
		}
	}
}

// noteDataPath reports on the keepalive round. This is the ONLY observation of
// the data partition anywhere in the group: pings, refresh probes and handshake
// verdicts all ride isolated partitions, so a data mux that is failing while the
// probe session stays healthy is otherwise completely invisible — which is the
// exact failure G8 introduced the keepalive to prevent, and discarding its
// result left it just as unobservable as before.
//
// Deliberately reporting only. Routing off a member is a decision with real
// flap cost, and it already has three carefully damped inputs; a fourth,
// undamped one is not worth a log line's worth of information. The healthy
// qualifier is what makes each line meaningful: data failing while the probe leg
// is fine is the signature, whereas a member that is simply down says nothing new.
func (s *Smart) noteDataPath(errs map[string]error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, nr := range s.nodes {
		err := errs[nr.tag]
		if err == nil {
			if nr.dataFailStreak > 0 {
				s.logger.Info("smart: node ", nr.tag, " data path reachable again after ",
					nr.dataFailStreak, " failed keepalive(s)")
				nr.dataFailStreak = 0
			}
			continue
		}
		nr.dataFailStreak++
		if nr.health == healthHealthy {
			s.logger.Warn("smart: node ", nr.tag, " data-partition keepalive failed (",
				nr.dataFailStreak, " in a row) while its probe leg is healthy — ",
				"live connections may be hanging: ", err)
		}
	}
}

// prewarm opens a couple of parallel data-partition connections per member so
// the mux is confirmed before real traffic arrives — otherwise cronet releases a
// cold page-load burst at ~1 request/RTT until each mux connection is confirmed
// (G9). Deliberately NOT coalesced: the triggers (start, network change) are
// rare, and a CAS guard would silently drop an InterfaceUpdated that lands while
// a prewarm is running — exactly the moment the members just closed every
// connection and warmth matters most (review fix).
func (s *Smart) prewarm() {
	// Errors are ignored here on purpose: prewarm fires at start and on a network
	// change, when a member legitimately may not be reachable yet, and the ping
	// loop reports on liveness at a much finer cadence anyway.
	s.dataDialRound(prewarmDials, 10*time.Second)
}

// dataDialRound dials each member's data partition `perNode` times in parallel,
// each bounded by timeout, and returns the tags that could not be reached at
// all. The probe marker elicits a tiny server reply; the point is the
// connection, not the measurement, so the timing is discarded — but whether the
// dial worked is the one thing only this round can see.
//
// A tag is reported failed only when EVERY one of its dials failed: with
// perNode > 1 a single loser is just one connection, not a dead path.
func (s *Smart) dataDialRound(perNode int, timeout time.Duration) map[string]error {
	s.mu.Lock()
	nodes := append([]*nodeRuntime(nil), s.nodes...)
	s.mu.Unlock()
	var (
		mu   sync.Mutex
		errs = make(map[string]error, len(nodes))
		ok   = make(map[string]bool, len(nodes))
		wg   sync.WaitGroup
	)
	for _, nr := range nodes {
		nr := nr
		for i := 0; i < perNode; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(s.ctx, timeout)
				_, err := probePing(ctx, nr.outbound, "") // empty key → default (data) partition
				cancel()
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					errs[nr.tag] = err
					return
				}
				ok[nr.tag] = true
			}()
		}
	}
	wg.Wait()
	for tag := range ok {
		delete(errs, tag)
	}
	return errs
}

func (s *Smart) refreshLoop() {
	ticker := time.NewTicker(s.params.probeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.close:
			return
		case <-ticker.C:
			if s.pause.IsDevicePaused() || s.pause.IsNetworkPaused() {
				continue
			}
			s.refreshActive()
		}
	}
}

// refreshConcurrency is how many hosts are probed at once. Each host in turn
// fans out to one probe per member, so the real ceiling on concurrent probes is
// this times the member count — small enough to stay well inside one mux
// session, large enough that a single slow host cannot hold up the round.
const refreshConcurrency = 4

// refreshJob is one host's refresh work: which host, on which TCP port, and how
// recently it was used (the dispatch order).
type refreshJob struct {
	host string
	port uint16
	last time.Time
}

// refreshActive re-probes the active hosts, bounded so that ONE ROUND ≈ ONE
// ProbeInterval by construction.
//
// This matters more than it looks: every anti-flap threshold in this group is
// priced in rounds — maxAge = 2×ProbeInterval, sustained-lead = 2 rounds, the
// CDN hysteresis window — so a round that quietly stretches to minutes voids all
// of them at once, with no symptom other than selections that no longer settle.
// Two guarantees keep that from happening: hosts run refreshConcurrency at a
// time, and dispatch stops once the interval is spent. Jobs are ordered
// most-recently-used first, so when the budget runs out it is the cold tail that
// waits for the next round, not whatever the map iteration happened to reach
// last.
func (s *Smart) refreshActive() {
	s.mu.Lock()
	jobs := s.refreshJobsLocked()
	s.mu.Unlock()
	if len(jobs) == 0 {
		return
	}
	start := time.Now()
	deadline := start.Add(s.params.probeInterval)

	queue := make(chan refreshJob)
	workers := refreshConcurrency
	if workers > len(jobs) {
		workers = len(jobs)
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range queue {
				s.refreshHost(job.host, job.port)
			}
		}()
	}
	dispatched := 0
dispatch:
	for _, job := range jobs {
		if !time.Now().Before(deadline) {
			break
		}
		select {
		case queue <- job:
			dispatched++
		case <-s.close:
			break dispatch
		}
	}
	close(queue)
	wg.Wait()

	// Overshoot past the deadline is bounded by the last dispatched host's probe
	// timeout, so the round can run slightly long — but it can no longer grow with
	// the size of the active set.
	elapsed := time.Since(start)
	if deferred := len(jobs) - dispatched; deferred > 0 {
		s.logger.Warn("smart refresh round: ", dispatched, " hosts in ", elapsed,
			", deferred ", deferred, " to the next round to stay within ", s.params.probeInterval)
		return
	}
	s.logger.Debug("smart refresh round: hosts=", dispatched, " took=", elapsed,
		" interval=", s.params.probeInterval)
}

// refreshJobsLocked sweeps the active set: it evicts idle hosts (G6) and returns
// the hosts to refresh, most-recently-used first, skipping any host with no TCP
// probe port (UDP-only, G16). Caller holds s.mu.
func (s *Smart) refreshJobsLocked() []refreshJob {
	now := time.Now()
	jobs := make([]refreshJob, 0, len(s.active))
	for host, last := range s.active {
		if now.Sub(last) > s.params.idleTimeout {
			// Idle past the timeout: drop all learned state for this host, not
			// just its active mark, so the four learning maps don't grow without
			// bound over weeks of uptime. A later revisit rebuilds via the ~0.5s
			// first-connect measurement (the same reason persistence is skipped) (G6).
			s.evictHostLocked(host)
			continue
		}
		// Refresh probes the remote leg with a server-side TCP connect, so it needs
		// a TCP port. A host only ever seen over UDP (STUN/WebRTC) has none —
		// probing its UDP port over TCP would falsely fail and block it, churning
		// the host between nodes. Skip those; their selection stays on the stable
		// fallback/region pick without misleading remote samples (G16).
		port, ok := s.tcpPort[host]
		if !ok || port == 0 {
			continue
		}
		jobs = append(jobs, refreshJob{host: host, port: port, last: last})
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].last.After(jobs[j].last) })
	return jobs
}

func (s *Smart) refreshHost(host string, port uint16) {
	s.mu.Lock()
	nodes := append([]*nodeRuntime(nil), s.nodes...)
	s.mu.Unlock()
	var wg sync.WaitGroup
	degraded := make(map[string]bool) // tags confirmed severely degraded this round (guarded by s.mu)
	for _, nr := range nodes {
		nr := nr
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
			remote, err := probeRemote(ctx, nr.outbound, host, port, probeKeyFor(nr.probeGen.Load()))
			cancel()
			now := time.Now()
			s.mu.Lock()
			hs := s.hostStatLocked(host, nr.tag, true)
			if err != nil {
				hs.noteFail(now, hostFailThreshold)
				s.mu.Unlock()
				return
			}
			// Probe-before-unblock: a successful probe is what lifts a block, so
			// real traffic only returns to a (node,host) once a probe confirms it
			// is reachable again — never as a live test subject (§5).
			if !hs.blockedUntil.IsZero() {
				hs.clearBlock()
			}
			hs.failCount = 0
			if severe := hs.observeRemote(now, remote, 0.25, s.params.maxAge); severe {
				degraded[nr.tag] = true // confirmed 2nd-consecutive outlier (G4)
			}
			s.noteEvidenceLocked()
			s.mu.Unlock()
		}()
	}
	wg.Wait()
	s.mu.Lock()
	s.reclassifyCDNLocked(host)
	// A severely degraded (2 consecutive outliers) node that the sticky points at
	// is dropped so new connections reroute immediately, instead of waiting out
	// the sustained-lead + MinDwell window the ordinary re-evaluation requires
	// (G4). Existing connections are not interrupted — this is a latency
	// improvement, not a failover (§6).
	if e := s.sticky[host]; e != nil && degraded[e.node] {
		s.logger.Info("smart: ", host, " leaving ", e.node, " by=remote-degraded")
		delete(s.sticky, host)
	}
	s.reEvaluateStickyLocked(host, N.NetworkTCP)
	s.mu.Unlock()
}

func (s *Smart) reclassifyCDNLocked(host string) {
	now := time.Now()
	var fresh []float64
	for _, nr := range s.nodes {
		if hs := s.hostStatLocked(host, nr.tag, false); hs != nil && hs.remote.state(now, s.params.maxAge) == sampleFresh {
			fresh = append(fresh, hs.remote.value)
		}
	}
	cur := s.cdnClass[host]
	next := classifyCDN(fresh, cur, s.params.cdn)
	if next == cur {
		return
	}
	// 60s classification hysteresis: suppress a flip that comes too soon after
	// the last one, so a borderline CDN host cannot oscillate the selection
	// (§3/§13). The first classification (no prior timestamp) applies at once.
	if last, ok := s.cdnClassAt[host]; ok && time.Since(last) < 60*time.Second {
		return
	}
	s.cdnClass[host] = next
	s.cdnClassAt[host] = time.Now()
}

func (s *Smart) reEvaluateStickyLocked(host, network string) {
	e := s.sticky[host]
	if e == nil {
		return
	}
	cands := s.snapshotLocked(host, network, nil)
	isCDN := s.cdnClass[host]
	// Only make confident (normal, MinDwell-locked) decisions once every alive
	// node has a fresh sample. Promoting confidence on a partial measurement
	// (e.g. only the sticky probed so far) can lock in a wrong choice before a
	// better node is even measured — found on real hardware for req.3.
	fullyMeasured := allAliveFresh(cands)
	tag, reason, ok := selectNode(cands, isCDN, s.params.sel)
	if !ok {
		return
	}
	if tag == e.node {
		// Sticky is still the winner: re-designate its reason to the current
		// flow's dimension (§0.4 — handles a fallback sticky whose site later
		// becomes CDN, or a CDN→non-CDN flip) and reset any pending challenger.
		e.reason = reason
		e.challenger = ""
		e.challengeCount = 0
		if fullyMeasured {
			e.confidence = confNormal
		}
		return
	}
	sticky := findCandidate(cands, e.node)
	challenger := findCandidate(cands, tag)
	if sticky == nil || challenger == nil || !sticky.remoteFresh || !challenger.remoteFresh {
		return
	}
	// The CDN band MUST be measured over exactly the set selectNode ranked
	// (selectCDN's own band is min-over-rankable). Taking the min over the full
	// snapshot instead lets an unusable node — declared dead, or blocked for this
	// host, yet still holding a fresh low sample — shrink the band below the
	// winner's own remote, so challengerBeatsSticky rejects the very node
	// selectNode just chose and the host stays pinned to the dirtier one.
	bandLimit := minRemoteFresh(rankable(cands)) + s.params.sel.cdnBandMs
	// Judge takeover on the CURRENT flow's dimension, not the stale sticky
	// reason: this is the reason re-designation (§0.4). A fallback sticky
	// created with reason=total is re-judged on the CDN dimension once the site
	// is classified CDN, so req.1's clean-node preference actually takes effect.
	if !challengerBeatsSticky(*sticky, *challenger, reason, bandLimit, s.params.sel) {
		e.challenger = ""
		e.challengeCount = 0
		return
	}
	// Sustained-lead: track how many consecutive evaluations this same
	// challenger has won by margin. A low-confidence (fallback/first) sticky
	// gets one immediate correction; an established one needs the challenger to
	// win ≥2 in a row AND MinDwell elapsed, so a single noisy sample can't flip
	// the selection (§3 anti-flap).
	//
	// The count advances only when the evidence generation has moved. Two
	// callers drive this function on two clocks: the refresh round, which brings
	// fresh samples every time, and the per-dial degraded fast path, which
	// normally re-judges an identical snapshot. Counting per invocation let two
	// back-to-back dials — microseconds apart, zero new measurements — satisfy
	// "won twice in a row" off a single observation, which is precisely what
	// this gate exists to prevent.
	if e.challenger == tag {
		if e.challengeGen != s.evidenceGen {
			e.challengeCount++
			e.challengeGen = s.evidenceGen
		}
	} else {
		e.challenger = tag
		e.challengeCount = 1
		e.challengeGen = s.evidenceGen
	}
	if e.confidence != confLow {
		if e.challengeCount < 2 || time.Since(e.lastSwitch) < s.params.minDwell {
			return
		}
	}
	// Latency-improvement switch: never interrupt existing connections (§6).
	prev := e.node
	e.node = tag
	e.reason = reason
	e.lastSwitch = time.Now()
	e.challenger = ""
	e.challengeCount = 0
	if fullyMeasured {
		e.confidence = confNormal
	}
	s.logger.Info("smart: ", host, " switched ", prev, " -> ", tag, " by=", reason)
}

// allAliveFresh reports whether every selectable candidate has a fresh remote
// sample — i.e. the site has a complete measurement to decide on.
func allAliveFresh(cands []nodeCandidate) bool {
	any := false
	for _, c := range cands {
		if !c.usable {
			continue
		}
		any = true
		if !c.remoteFresh {
			return false
		}
	}
	return any
}

func findCandidate(cands []nodeCandidate, tag string) *nodeCandidate {
	for i := range cands {
		if cands[i].tag == tag {
			return &cands[i]
		}
	}
	return nil
}

func minRemoteFresh(cands []nodeCandidate) float64 {
	min := 0.0
	has := false
	for _, c := range cands {
		if !c.remoteFresh {
			continue
		}
		if !has || c.remote() < min {
			min = c.remote()
			has = true
		}
	}
	return min
}
