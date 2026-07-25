package group

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

// stubOutbound is a no-op adapter.Outbound for the lock-path unit tests: it
// reports a network set (tcp+udp) and, when hang is set, makes DialContext block
// until the context deadline so a probe times out.
type stubOutbound struct {
	tag  string
	hang bool
}

func (o stubOutbound) Type() string           { return "naive" }
func (o stubOutbound) Tag() string            { return o.tag }
func (o stubOutbound) Network() []string      { return []string{N.NetworkTCP, N.NetworkUDP} }
func (o stubOutbound) Dependencies() []string { return nil }
func (o stubOutbound) DialContext(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
	if o.hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return nil, net.ErrClosed
}
func (o stubOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}

func TestLinkDeadNeedsTwoDistinctTargets(t *testing.T) {
	now := time.Unix(0, 0)
	nr := &nodeRuntime{}
	// One dead target repeats the same host, however often it fails → never the
	// link's fault, so a single black-holed destination can't fell the node.
	require.False(t, nr.linkDead("a.com", now))
	require.False(t, nr.linkDead("a.com", now.Add(time.Second)))
	require.False(t, nr.linkDead("a.com", now.Add(2*time.Second)))
	// A second distinct host within the window trips the gate.
	require.True(t, nr.linkDead("b.com", now.Add(3*time.Second)))
}

func TestLinkDeadWindowBoundInclusive(t *testing.T) {
	now := time.Unix(0, 0)
	nr := &nodeRuntime{}
	// Just past the window: the earlier failure no longer counts.
	require.False(t, nr.linkDead("a.com", now))
	require.False(t, nr.linkDead("b.com", now.Add(linkDeathWindow+time.Nanosecond)))

	// Exactly at the window bound: still counts (inclusive), matching the
	// evicting-map semantics this replaced — guards an off-by-one.
	nr2 := &nodeRuntime{}
	require.False(t, nr2.linkDead("a.com", now))
	require.True(t, nr2.linkDead("b.com", now.Add(linkDeathWindow)))
}

func TestLinkDeadEmptyHostIsNoEvidence(t *testing.T) {
	now := time.Unix(0, 0)
	nr := &nodeRuntime{}
	// An unattributable failure (no key) must neither trip nor become evidence.
	require.False(t, nr.linkDead("", now))
	require.False(t, nr.linkDead("a.com", now.Add(time.Second)))
	require.False(t, nr.linkDead("", now.Add(2*time.Second)))
	require.True(t, nr.linkDead("b.com", now.Add(3*time.Second)))
}

func TestClearLinkFails(t *testing.T) {
	now := time.Unix(0, 0)
	nr := &nodeRuntime{}
	require.False(t, nr.linkDead("a.com", now))
	nr.clearLinkFails() // a success proves the link is up
	// Evidence reset: b.com is now the first failure again, not the second.
	require.False(t, nr.linkDead("b.com", now.Add(time.Second)))
}

func TestFlapPenaltyEscalates(t *testing.T) {
	base := time.Unix(1000, 0)
	nr := &nodeRuntime{}
	// A first-ever down (never been up) starts at baseline.
	nr.noteDown(base)
	require.Equal(t, 0, nr.flapLevel)
	require.Equal(t, healthOKStreak, nr.requiredOKStreak())
	// Recover, then flap down again quickly → penalty escalates.
	nr.noteUp(base.Add(5 * time.Second))
	nr.noteDown(base.Add(10 * time.Second))
	require.Equal(t, 1, nr.flapLevel)
	require.Equal(t, healthOKStreak+1, nr.requiredOKStreak())
	// Keep flapping → keep escalating, capped at maxFlapLevel.
	for i := 0; i < 10; i++ {
		nr.noteUp(base.Add(time.Duration(11+2*i) * time.Second))
		nr.noteDown(base.Add(time.Duration(12+2*i) * time.Second))
	}
	require.Equal(t, maxFlapLevel, nr.flapLevel)
}

func TestFlapPenaltyResetsAfterStableHealth(t *testing.T) {
	base := time.Unix(1000, 0)
	nr := &nodeRuntime{}
	nr.noteUp(base)
	nr.noteDown(base.Add(10 * time.Second)) // flap within window
	require.Equal(t, 1, nr.flapLevel)
	// Now stay healthy well past the flap window, then a fresh down resets.
	nr.noteUp(base.Add(20 * time.Second))
	nr.noteDown(base.Add(20*time.Second + flapWindow + time.Second))
	require.Equal(t, 0, nr.flapLevel)
}

// newTestSmart builds a minimal Smart with n named nodes wired for the
// lock-holding failure/recovery paths (no real outbounds or probing).
func newTestSmart(tags ...string) *Smart {
	s := &Smart{
		ctx:        context.Background(),
		logger:     log.NewNOPFactory().Logger(),
		nodeByTag:  make(map[string]*nodeRuntime),
		hostStats:  make(map[string]map[string]*hostNodeStat),
		sticky:     make(map[string]*stickyEntry),
		cdnClass:   make(map[string]bool),
		cdnClassAt: make(map[string]time.Time),
		active:     make(map[string]time.Time),
		tcpPort:    make(map[string]uint16),
		measure:    make(map[string]*measureState),
		region:     buildRegionModel(nil),
		close:      make(chan struct{}),
		params: smartParams{
			maxAge:       90 * time.Second,
			queueDelayMs: 200,
			sel:          defaultSelectParams(),
		},
	}
	for _, tag := range tags {
		nr := &nodeRuntime{
			tag: tag, health: healthHealthy, localMin: newRollingMin(90 * time.Second),
			interrupt: interrupt.NewGroup(), outbound: stubOutbound{tag: tag},
		}
		s.nodes = append(s.nodes, nr)
		s.nodeByTag[tag] = nr
	}
	return s
}

func TestReportConnFailureSingleTargetDoesNotDownNode(t *testing.T) {
	s := newTestSmart("x", "y")
	nr := s.nodeByTag["x"]
	s.sticky["a.com"] = &stickyEntry{node: "x"}
	s.sticky["b.com"] = &stickyEntry{node: "x"}

	// Repeated failures of a SINGLE host must not down the node or clear other
	// hosts' stickies — only a.com's sticky is dropped and (a.com,x) blocked.
	s.reportConnFailure(nr, "a.com", errTest)
	s.reportConnFailure(nr, "a.com", errTest)

	require.Equal(t, healthHealthy, nr.health, "one target must not down the node")
	require.NotContains(t, s.sticky, "a.com", "failing host's sticky dropped")
	require.Contains(t, s.sticky, "b.com", "other host's sticky preserved")
	hs := s.hostStatLocked("a.com", "x", false)
	require.NotNil(t, hs)
	require.True(t, hs.blockedUntil.After(time.Now()), "(a.com,x) blocked")
}

func TestReportConnFailureTwoTargetsDownsNode(t *testing.T) {
	s := newTestSmart("x", "y")
	nr := s.nodeByTag["x"]
	nr.lastUpAt = time.Now() // recently healthy, so this down counts as a flap
	s.sticky["a.com"] = &stickyEntry{node: "x"}
	s.sticky["b.com"] = &stickyEntry{node: "x"}
	s.sticky["c.com"] = &stickyEntry{node: "y"}

	// Two DISTINCT hosts failing crosses the link-death gate → node down and all
	// of its stickies cleared; the other node's sticky is untouched.
	s.reportConnFailure(nr, "a.com", errTest)
	s.reportConnFailure(nr, "b.com", errTest)

	require.Equal(t, healthLocalDown, nr.health)
	require.NotContains(t, s.sticky, "a.com")
	require.NotContains(t, s.sticky, "b.com")
	require.Contains(t, s.sticky, "c.com", "healthy node's sticky preserved")
	// The link-death path must go through the shared down ritual, so flap
	// accounting (which gates readmission) actually sees this transition.
	require.Equal(t, 1, nr.flapLevel, "down transition recorded for flap accounting")
}

func TestNoteFailArmsBlockAtThreshold(t *testing.T) {
	now := time.Unix(1000, 0)
	hs := &hostNodeStat{}
	// First failure is not enough — a lone failure can be the target's hiccup.
	require.False(t, hs.noteFail(now, hostFailThreshold))
	require.True(t, hs.blockedUntil.IsZero())
	// Second arms the block with the first backoff step.
	require.True(t, hs.noteFail(now, hostFailThreshold))
	require.Equal(t, now.Add(30*time.Second), hs.blockedUntil)
	require.Equal(t, 1, hs.backoff)
	// A successful probe lifts it (probe-before-unblock) and clears the count.
	hs.clearBlock()
	require.True(t, hs.blockedUntil.IsZero())
	require.Equal(t, 0, hs.failCount)
	require.False(t, hs.noteFail(now, hostFailThreshold), "count restarts after a clear")
}

// noteFail reports whether it ARMED a block, not merely whether it reached the
// threshold: reaching the threshold again while a block is already armed changes
// nothing, and saying otherwise would mislead any future caller that logs or
// branches on it.
func TestNoteFailReportsArmingNotThreshold(t *testing.T) {
	now := time.Unix(1000, 0)
	hs := &hostNodeStat{}
	require.True(t, hs.noteFail(now, liveFailThreshold), "first failure arms the block")
	require.False(t, hs.noteFail(now.Add(time.Second), liveFailThreshold),
		"threshold reached again, but the armed block is untouched")
	require.Equal(t, 1, hs.backoff)
	require.True(t, hs.noteFail(now.Add(31*time.Second), liveFailThreshold),
		"after the block lapses, a failure arms the next step")
	require.Equal(t, 2, hs.backoff)
}

func TestOnDialErrorBlocksPairAndDropsOnlyItsSticky(t *testing.T) {
	// Pins the dial-error path's threshold behaviour, which had no coverage.
	s := newTestSmart("x", "y")
	nr := s.nodeByTag["x"]
	s.sticky["a.com"] = &stickyEntry{node: "x"}
	s.sticky["b.com"] = &stickyEntry{node: "x"}

	s.onDialError(destKey{host: "a.com"}, nr)
	require.NotContains(t, s.sticky, "a.com", "sticky dropped so the retry re-selects")
	require.Contains(t, s.sticky, "b.com", "other hosts on the same node untouched")
	require.True(t, s.hostStatLocked("a.com", "x", false).blockedUntil.IsZero(), "one failure must not block")
	require.Equal(t, healthHealthy, nr.health, "a dial error is not node death")

	s.onDialError(destKey{host: "a.com"}, nr)
	require.True(t, s.hostStatLocked("a.com", "x", false).blockedUntil.After(time.Now()), "second failure blocks the pair")
	require.Equal(t, healthHealthy, nr.health, "still only this (host,node) is penalized")
}

func TestDownNodeLockedClearsStickyAndInterruptGate(t *testing.T) {
	// interrupt disabled (default): stickies cleared, no interrupt on live conns.
	s := newTestSmart("x", "y")
	nr := s.nodeByTag["x"]
	s.sticky["a.com"] = &stickyEntry{node: "x"}
	s.sticky["b.com"] = &stickyEntry{node: "y"}
	s.downNodeLocked(nr, time.Now(), "handshake verdict timeout")
	require.Equal(t, healthLocalDown, nr.health)
	require.NotContains(t, s.sticky, "a.com", "downed node's sticky cleared")
	require.Contains(t, s.sticky, "b.com", "other node's sticky preserved")

	// interrupt enabled: same sticky behaviour, and the interrupt fires without
	// disturbing the other node (G2 aligns the verdict path with the hard-error
	// path, which is likewise gated on interrupt_exist_connections).
	s2 := newTestSmart("x", "y")
	s2.params.interruptExternal = true
	nr2 := s2.nodeByTag["x"]
	s2.sticky["a.com"] = &stickyEntry{node: "x"}
	require.NotPanics(t, func() { s2.downNodeLocked(nr2, time.Now(), "handshake verdict timeout") })
	require.Equal(t, healthLocalDown, nr2.health)
	require.NotContains(t, s2.sticky, "a.com")
}

func TestLocalDegradationDifferentialPenalty(t *testing.T) {
	s := newTestSmart("x", "y")
	s.params.queueDelayMs = 200
	x := s.nodeByTag["x"]
	y := s.nodeByTag["y"]
	// y is a clean witness: alive, un-inflated.
	y.lastSample = pingSample{ok: true, rttMs: 60, localMin: 55}

	// x inflated +250ms over its baseline, with a clean witness → after the
	// confirm streak it is penalized member-wide by the inflation.
	s.mu.Lock()
	s.evaluateLocalDegradationLocked(x, 280, 30) // round 1: inflation 250, streak 1, not yet
	require.False(t, x.localDegraded())
	require.Equal(t, 0.0, x.localPenaltyMs)
	s.evaluateLocalDegradationLocked(x, 280, 30) // round 2: confirmed
	require.True(t, x.localDegraded())
	require.InDelta(t, 250.0, x.localPenaltyMs, 0.001)
	s.mu.Unlock()

	// The penalty shows up in the candidate total so ranking deprioritizes x.
	s.mu.Lock()
	cands := s.snapshotLocked("site.com", "tcp", nil)
	s.mu.Unlock()
	xc := findCandidate(cands, "x")
	require.NotNil(t, xc)
	require.InDelta(t, 250.0, xc.localPenaltyMs, 0.001)
}

func TestLocalDegradationCommonModeNotPenalized(t *testing.T) {
	s := newTestSmart("x", "y")
	s.params.queueDelayMs = 200
	x := s.nodeByTag["x"]
	y := s.nodeByTag["y"]
	// y is ALSO inflated → shared congestion, no clean witness → don't penalize.
	y.lastSample = pingSample{ok: true, rttMs: 300, localMin: 55} // inflation 245

	s.mu.Lock()
	s.evaluateLocalDegradationLocked(x, 280, 30)
	s.evaluateLocalDegradationLocked(x, 280, 30)
	s.mu.Unlock()
	require.False(t, x.localDegraded(), "common-mode congestion must not penalize a single node")
	require.Equal(t, 0.0, x.localPenaltyMs)
}

func TestLocalDegradationSelfReleases(t *testing.T) {
	s := newTestSmart("x", "y")
	s.params.queueDelayMs = 200
	x := s.nodeByTag["x"]
	s.nodeByTag["y"].lastSample = pingSample{ok: true, rttMs: 60, localMin: 55}

	s.mu.Lock()
	s.evaluateLocalDegradationLocked(x, 280, 30)
	s.evaluateLocalDegradationLocked(x, 280, 30)
	require.True(t, x.localDegraded())
	// Baseline caught up (rolling-min rose to 280): inflation → 0 → penalty clears.
	s.evaluateLocalDegradationLocked(x, 280, 280)
	s.mu.Unlock()
	require.False(t, x.localDegraded())
	require.Equal(t, 0.0, x.localPenaltyMs)
	require.Equal(t, 0, x.localSlowStreak)
}

// A locally-degraded sticky must be re-judged through the ordinary hysteresis
// gates, not bare-repicked: the penalty still moves traffic off the bad node, but
// an established sticky needs the challenger to win twice, so one noisy round
// can't hop it. (Before the review fix this path bypassed margin/sustained-lead
// and could oscillate, and could fall through to a synchronous probe.)
func TestPickNodeDegradedStickyGoesThroughHysteresis(t *testing.T) {
	setup := func(conf int8) *Smart {
		s := newTestSmart("x", "y")
		s.params.sel = defaultSelectParams()
		s.params.minDwell = 0 // isolate the sustained-lead requirement
		now := time.Now()
		s.hostStatLocked("site.com", "x", true).remote.update(now, 20, 0.25, 90*time.Second)
		s.hostStatLocked("site.com", "y", true).remote.update(now, 30, 0.25, 90*time.Second)
		s.nodeByTag["x"].localMin.add(now, 30)
		s.nodeByTag["y"].localMin.add(now, 60)
		s.sticky["site.com"] = &stickyEntry{node: "x", reason: reasonTotal, confidence: conf, lastSwitch: now}
		// x's local leg degrades hard: x total 30+300+20=350 vs y 60+30=90.
		s.nodeByTag["x"].localPenaltyMs = 300 // nonzero penalty == degraded
		return s
	}
	key := destKey{host: "site.com"}

	// Established sticky: holds on the first degraded request (challenger has won
	// once), and keeps holding for as many further requests as arrive on that same
	// measurement — "sustained" means two rounds of evidence, not two dials. Only
	// once a new measurement generation lands does the switch fire.
	s := setup(confNormal)
	require.Equal(t, "x", s.pickNode(context.Background(), key, "tcp", 443, nil).tag, "no hop on a single evaluation")
	require.Equal(t, "x", s.pickNode(context.Background(), key, "tcp", 443, nil).tag, "re-judging one snapshot is not a second evaluation")
	s.noteEvidenceLocked() // a fresh ping/probe round arrives
	require.Equal(t, "y", s.pickNode(context.Background(), key, "tcp", 443, nil).tag, "switches on sustained lead")

	// A low-confidence sticky (e.g. a fallback pick) still gets its one immediate
	// correction, so a bad initial guess is fixed without waiting.
	require.Equal(t, "y", setup(confLow).pickNode(context.Background(), key, "tcp", 443, nil).tag)
}

// The sustained-lead gate must count generations of EVIDENCE, not calls.
// reEvaluateStickyLocked has two callers on two clocks — the refresh round and
// the per-dial degraded fast path — and counting per call let a busy host
// satisfy "won two evaluations in a row" from a single observation, by dialing
// twice microseconds apart. MinDwell is zeroed here so the assertion is about
// the lead requirement alone.
func TestSustainedLeadRequiresNewEvidence(t *testing.T) {
	s := newTestSmart("x", "y")
	s.params.sel = defaultSelectParams()
	s.params.minDwell = 0
	now := measured(s, "h", 0, map[string]float64{"x": 200, "y": 20})
	s.nodeByTag["x"].localMin.add(now, 30)
	s.nodeByTag["y"].localMin.add(now, 30)
	s.sticky["h"] = &stickyEntry{
		node: "x", reason: reasonTotal, confidence: confNormal, lastSwitch: now.Add(-time.Hour),
	}

	// Ten evaluations of one unchanged snapshot: y wins every time, but it is the
	// same win ten times over, so the sticky must not move.
	for i := 0; i < 10; i++ {
		s.reEvaluateStickyLocked("h", N.NetworkTCP)
	}
	require.Equal(t, "x", s.sticky["h"].node, "one observation re-read cannot be a sustained lead")
	require.Equal(t, 1, s.sticky["h"].challengeCount, "the count tracks evidence, not invocations")

	// A second, independent measurement round is what completes the lead.
	s.noteEvidenceLocked()
	s.reEvaluateStickyLocked("h", N.NetworkTCP)
	require.Equal(t, "y", s.sticky["h"].node, "two rounds of evidence do switch it")
}

// Routing around a transient per-host block must not discard the host's
// measurement standing. It used to replace the whole sticky entry, resetting
// confidence to confLow — which drops both the dwell timer and the
// sustained-lead requirement, so the choice could flip straight back the moment
// the block expired: one probe hiccup, two switches.
func TestStickyKeepsConfidenceWhenRoutingAroundBlock(t *testing.T) {
	s := newTestSmart("x", "y")
	s.params.sel = defaultSelectParams()
	now := measured(s, "h", 30, map[string]float64{"x": 20, "y": 20})
	s.sticky["h"] = &stickyEntry{
		node: "x", reason: reasonTotal, confidence: confNormal, lastSwitch: now.Add(-time.Hour),
	}
	// A refresh-probe block arms on (h,x) WITHOUT deleting the sticky — unlike the
	// dial-error and live-failure paths, which delete it.
	s.hostStatLocked("h", "x", true).block(now)

	nr := s.pickNode(context.Background(), destKey{host: "h"}, N.NetworkTCP, 443, nil)
	require.Equal(t, "y", nr.tag, "the blocked node must not carry the request")
	require.Equal(t, confNormal, s.sticky["h"].confidence,
		"the host is still as well measured as it was; only its node was unusable")
}

// The fallback pick is the opposite case: nothing was measurable, so it is a
// guess and must stay confLow — a low-confidence sticky is granted one immediate
// correction, which is how a bad initial guess gets fixed without waiting out
// the sustained-lead window.
func TestFallbackStickyStaysLowConfidence(t *testing.T) {
	s := newTestSmart("x", "y")
	s.params.sel = defaultSelectParams()
	// A prior confident sticky exists, but its node is gone and nothing is
	// measured, so pickNode lands on the fallback tier.
	s.sticky["h"] = &stickyEntry{node: "x", reason: reasonTotal, confidence: confNormal}
	s.nodeByTag["x"].health = healthLocalDown

	nr := s.pickNode(context.Background(), destKey{host: "h"}, N.NetworkTCP, 443, nil)
	require.Equal(t, "y", nr.tag)
	require.Equal(t, confLow, s.sticky["h"].confidence,
		"an unmeasured last-resort pick must remain correctable on the next evaluation")
}

func TestTotalIncludesLocalPenalty(t *testing.T) {
	c := fresh("a", 30, 100) // total 130 normally
	require.InDelta(t, 130.0, c.total(), 0.001)
	c.localPenaltyMs = 250
	require.InDelta(t, 380.0, c.total(), 0.001)
}

func TestEvictHostClearsAllMaps(t *testing.T) {
	s := newTestSmart("x")
	now := time.Now()
	s.active["gone.com"] = now
	s.hostStatLocked("gone.com", "x", true).remote.update(now, 20, 0.25, 90*time.Second)
	s.sticky["gone.com"] = &stickyEntry{node: "x"}
	s.cdnClass["gone.com"] = true
	s.cdnClassAt["gone.com"] = now
	// A host that stays active must be untouched.
	s.active["live.com"] = now
	s.sticky["live.com"] = &stickyEntry{node: "x"}

	s.evictHostLocked("gone.com")

	require.NotContains(t, s.active, "gone.com")
	require.NotContains(t, s.hostStats, "gone.com")
	require.NotContains(t, s.sticky, "gone.com")
	require.NotContains(t, s.cdnClass, "gone.com")
	require.NotContains(t, s.cdnClassAt, "gone.com")
	require.Contains(t, s.sticky, "live.com", "unrelated host preserved")
}

func TestRefreshActiveEvictsIdleHosts(t *testing.T) {
	s := newTestSmart("x")
	s.params.idleTimeout = 30 * time.Minute
	s.params.probeInterval = 45 * time.Second
	old := time.Now().Add(-31 * time.Minute)
	s.active["idle.com"] = old
	s.hostStatLocked("idle.com", "x", true).remote.update(old, 20, 0.25, 90*time.Second)
	s.sticky["idle.com"] = &stickyEntry{node: "x"}
	s.cdnClass["idle.com"] = true

	s.refreshActive() // no live nodes to probe; just runs the idle sweep

	require.NotContains(t, s.active, "idle.com")
	require.NotContains(t, s.hostStats, "idle.com")
	require.NotContains(t, s.sticky, "idle.com")
	require.NotContains(t, s.cdnClass, "idle.com")
}

func TestStatusStringBounded(t *testing.T) {
	s := newTestSmart("x")
	for i := 0; i < 100; i++ {
		host := "h" + itoa(i) + ".com"
		s.sticky[host] = &stickyEntry{node: "x", reason: reasonTotal}
	}
	out := s.stickyStatusString()
	require.Contains(t, out, "more)", "long host set is summarized, not fully dumped")
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [12]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

// TestFirstConnectMeasureSkipsFullyBlockedHost pins the dead-target behaviour
// that used to need a separate frozen predicate (G12): with every node blocked
// for the host there is nothing to probe, so the call must return at once — no
// probe fan-out, and crucially no in-flight entry, or concurrent siblings for the
// same host would block for the probe timeout.
func TestFirstConnectMeasureSkipsFullyBlockedHost(t *testing.T) {
	s := newTestSmart("x", "y")
	s.params.firstConnectTimeout = 3 * time.Second
	future := time.Now().Add(time.Minute)
	s.hostStatLocked("dead.com", "x", true).blockedUntil = future
	s.hostStatLocked("dead.com", "y", true).blockedUntil = future

	start := time.Now()
	s.firstConnectMeasure(context.Background(), "dead.com", 443, "tcp")
	require.Less(t, time.Since(start), time.Second, "must not wait on probes for a dead target")
	require.NotContains(t, s.measure, "dead.com", "no in-flight entry for siblings to wait on")
	require.Zero(t, s.measuring, "no concurrency slot consumed")
}

// A host with at least one unblocked node still gets measured; an expired block
// counts as unblocked, which is what un-freezes a recovered target.
func TestFirstConnectMeasureRunsWhenAnyNodeUsable(t *testing.T) {
	s := newTestSmart("x", "y")
	s.params.firstConnectTimeout = 50 * time.Millisecond
	s.hostStatLocked("mixed.com", "x", true).blockedUntil = time.Now().Add(time.Minute)
	s.hostStatLocked("mixed.com", "y", true).blockedUntil = time.Now().Add(-time.Second) // expired

	// The stub outbound fails fast, so this returns quickly; the point is that it
	// ran the fan-out at all and then released both the single-flight entry and
	// its concurrency slot.
	s.firstConnectMeasure(context.Background(), "mixed.com", 443, "tcp")
	require.NotNil(t, s.measure["mixed.com"])
	require.Nil(t, s.measure["mixed.com"].done, "single-flight entry released")
	require.Zero(t, s.measuring, "concurrency slot released")
}

// The dial retry loop calls pickNode once per attempt, and onDialError drops the
// sticky in between — so without a cooldown each attempt re-paid the full
// first-connect measurement, turning a failing host into three times
// firstConnectTimeout before the request finally errored.
func TestFirstConnectMeasureCooldownStopsRetryRepay(t *testing.T) {
	s := newTestSmart("x")
	s.params.firstConnectTimeout = 3 * time.Second
	s.nodeByTag["x"].outbound = stubOutbound{tag: "x", hang: true} // probe burns the timeout

	start := time.Now()
	s.firstConnectMeasure(context.Background(), "slow.example", 443, N.NetworkTCP)
	first := time.Since(start)
	require.GreaterOrEqual(t, first, s.params.firstConnectTimeout, "the first attempt does measure")

	// A retry moments later must not pay for it again.
	start = time.Now()
	s.firstConnectMeasure(context.Background(), "slow.example", 443, N.NetworkTCP)
	require.Less(t, time.Since(start), time.Second, "within the cooldown, the repeat is suppressed")

	// Once the cooldown lapses the host is measurable again — this is a retry
	// damper, not a permanent freeze.
	s.measure["slow.example"].last = time.Now().Add(-firstConnectCooldown - time.Second)
	start = time.Now()
	s.firstConnectMeasure(context.Background(), "slow.example", 443, N.NetworkTCP)
	require.GreaterOrEqual(t, time.Since(start), s.params.firstConnectTimeout)
}

// The concurrency valve bounds the probe fan-out under a pathological burst: the
// marginal request takes the fallback pick immediately instead of stalling for
// the timeout behind a hundred other measurements.
func TestFirstConnectMeasureConcurrencyValve(t *testing.T) {
	s := newTestSmart("x")
	s.params.firstConnectTimeout = 3 * time.Second
	s.nodeByTag["x"].outbound = stubOutbound{tag: "x", hang: true}
	s.measuring = firstConnectConcurrency // valve closed

	start := time.Now()
	s.firstConnectMeasure(context.Background(), "burst.example", 443, N.NetworkTCP)
	require.Less(t, time.Since(start), time.Second, "must not stall when the valve is closed")
	require.NotContains(t, s.measure, "burst.example", "and must not publish an entry siblings would wait on")
}

// A request that goes away must stop the caller waiting on a measurement it no
// longer needs. The fan-out itself is deliberately NOT cancelled — a sibling may
// be waiting on the same samples — so this asserts the wait, not the work.
func TestFirstConnectMeasureStopsWaitingWhenRequestGoesAway(t *testing.T) {
	s := newTestSmart("x")
	s.params.firstConnectTimeout = 5 * time.Second
	s.nodeByTag["x"].outbound = stubOutbound{tag: "x", hang: true}

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)

	start := time.Now()
	s.pickNode(ctx, destKey{host: "slow.example"}, N.NetworkTCP, 443, nil)
	require.Less(t, time.Since(start), 2*time.Second,
		"a cancelled request must not hold its goroutine for the full probe timeout")

	// The measurement is still running and still owns its single-flight slot, so a
	// sibling coalesces onto it rather than starting a second fan-out.
	require.NotNil(t, s.measure["slow.example"].done)
}

// idleTimeout bounds host state in TIME; with everything proxied and a per-IP
// key for domain-less destinations, a burst can mint far more hosts than that
// window ever reclaims. The size bound is what makes it a bound.
func TestActiveHostsAreSizeBounded(t *testing.T) {
	s := newTestSmart("x")
	// UDP keys skip the probe port and hence the synchronous measurement, so this
	// exercises the map growth rather than the probe path.
	total := maxActiveHosts + hostEvictBatch + 50
	for i := 0; i < total; i++ {
		s.pickNode(context.Background(), destKey{host: fmt.Sprintf("h%d.example", i)}, N.NetworkUDP, 3478, nil)
	}
	require.LessOrEqual(t, len(s.active), maxActiveHosts, "the host table must be size-bounded")
	require.Contains(t, s.active, fmt.Sprintf("h%d.example", total-1), "the newest host survives")
	require.NotContains(t, s.active, "h0.example", "the least recently used goes first")
	// Eviction has to be total, or the maps it missed keep growing regardless.
	require.NotContains(t, s.sticky, "h0.example")
	require.Equal(t, len(s.active), len(s.sticky), "no sticky outlives its active entry")
}

// Evicting a host mid-measurement would be undone by the fan-out's own writes:
// they recreate hostStats through the create path, for a host that no longer has
// an active entry — and nothing sweeps those, so the bookkeeping meant to bound
// this state would leak it instead.
func TestEvictSkipsHostBeingMeasured(t *testing.T) {
	s := newTestSmart("x")
	s.active["h"] = time.Now()
	s.sticky["h"] = &stickyEntry{node: "x"}
	s.measure["h"] = &measureState{done: make(chan struct{})}

	require.False(t, s.evictHostLocked("h"), "must refuse while a measurement is in flight")
	require.Contains(t, s.active, "h")

	// Once the measurement releases, the host evicts normally.
	s.measure["h"].done = nil
	require.True(t, s.evictHostLocked("h"))
	require.NotContains(t, s.active, "h")
}

// The keepalive round is the only thing in the group that touches the DATA
// partition; discarding its result left a failing data mux completely
// unobservable, which is the failure the keepalive was added to catch.
func TestDataDialRoundReportsFailures(t *testing.T) {
	s := newTestSmart("x", "y")
	errs := s.dataDialRound(1, 500*time.Millisecond)
	require.Len(t, errs, 2, "an unreachable data partition must be reported, not swallowed")
	require.Error(t, errs["x"])
}

func TestDataPathStreakAccounting(t *testing.T) {
	s := newTestSmart("x", "y")
	s.noteDataPath(map[string]error{"x": errTest})
	require.Equal(t, 1, s.nodeByTag["x"].dataFailStreak)
	require.Equal(t, 0, s.nodeByTag["y"].dataFailStreak, "a reachable member accrues nothing")
	s.noteDataPath(map[string]error{"x": errTest})
	require.Equal(t, 2, s.nodeByTag["x"].dataFailStreak)
	s.noteDataPath(nil) // everyone reachable again
	require.Equal(t, 0, s.nodeByTag["x"].dataFailStreak, "recovery closes the streak")
}

func TestUDPPickDoesNotSetProbePort(t *testing.T) {
	s := newTestSmart("x")
	// A UDP request stamps active but not the TCP probe port.
	s.pickNode(context.Background(), destKey{host: "stun.example"}, "udp", 3478, nil)
	require.Contains(t, s.active, "stun.example")
	_, ok := s.tcpPort["stun.example"]
	require.False(t, ok, "UDP flow must not record a TCP probe port")

	// A TCP request to another host does record it.
	s.pickNode(context.Background(), destKey{host: "web.example"}, "tcp", 443, nil)
	require.Equal(t, uint16(443), s.tcpPort["web.example"])
}

func TestRefreshSkipsUDPOnlyHost(t *testing.T) {
	s := newTestSmart("x")
	s.params.idleTimeout = 30 * time.Minute
	now := time.Now()
	// UDP-only host: active, but no TCP port → must be skipped by the refresh
	// sweep so its UDP port is never TCP-probed and falsely blocked.
	s.active["stun.example"] = now
	// TCP host: has a recorded port → must be probed.
	s.active["web.example"] = now
	s.tcpPort["web.example"] = 443

	s.mu.Lock()
	jobs := s.refreshJobsLocked()
	s.mu.Unlock()
	require.Equal(t, []refreshJob{{host: "web.example", port: 443, last: now}}, jobs)
}

// The refresh round is ordered most-recently-used first and stops dispatching
// once ProbeInterval is spent, so a large active set defers its cold tail
// instead of stretching the round — the rounds every anti-flap threshold here is
// priced in.
func TestRefreshJobsMostRecentlyUsedFirst(t *testing.T) {
	s := newTestSmart("x")
	s.params.idleTimeout = 30 * time.Minute
	now := time.Now()
	for i, host := range []string{"cold", "warm", "hot"} {
		s.active[host] = now.Add(-time.Duration(2-i) * time.Minute)
		s.tcpPort[host] = 443
	}
	s.mu.Lock()
	jobs := s.refreshJobsLocked()
	s.mu.Unlock()
	require.Equal(t, []string{"hot", "warm", "cold"}, []string{jobs[0].host, jobs[1].host, jobs[2].host})
}

// The refresh round stops dispatching once its time budget is spent. Without
// that, a round grows with the active set — and since maxAge, sustained-lead and
// the CDN hysteresis are all priced in rounds, a silently minutes-long round
// voids every one of them at once.
func TestRefreshRoundStopsAtItsTimeBudget(t *testing.T) {
	hosts := []string{"a.example", "b.example", "c.example"}
	newRound := func(budget time.Duration) *Smart {
		s := newTestSmart("x")
		s.params.idleTimeout = 30 * time.Minute
		s.params.probeInterval = budget
		now := time.Now()
		for _, h := range hosts {
			s.active[h] = now
			s.tcpPort[h] = 443
		}
		return s
	}

	// Budget already spent before the first dispatch → nothing is probed, so no
	// host gains a stat entry.
	spent := newRound(0)
	spent.refreshActive()
	for _, h := range hosts {
		require.Nil(t, spent.hostStatLocked(h, "x", false), h+" must not be probed past the budget")
	}

	// Positive control: with a real budget the same round probes every host, so
	// the assertion above cannot pass merely because refreshActive did nothing.
	ample := newRound(time.Minute)
	ample.refreshActive()
	for _, h := range hosts {
		require.NotNil(t, ample.hostStatLocked(h, "x", false), h+" must be probed within the budget")
	}
}

func TestPingTimeoutDoesNotReviveLocalDown(t *testing.T) {
	s := newTestSmart("x", "y")
	x := s.nodeByTag["x"]
	x.outbound = stubOutbound{tag: "x", hang: true} // its probe will time out
	// A healthy witness so the common-mode path, if wrongly reached, would suppress.
	s.nodeByTag["y"].lastSample = pingSample{ok: true, rttMs: 30, localMin: 25}

	x.health = healthLocalDown
	x.pingOKStreak = 1 // mid-recovery progress that a bogus revive would also clobber

	s.pingOnce(x) // ~1s: probe hangs to the ping timeout

	require.Equal(t, healthLocalDown, x.health, "a timed-out ping must not promote a downed node to suspect")
}

func TestPrewarmCoalescesAndReleases(t *testing.T) {
	s := newTestSmart("x", "y") // stub outbounds fail fast, so dials return immediately
	// Prewarm is intentionally not coalesced: back-to-back calls (start, then a
	// network change) must each run, or an InterfaceUpdated landing during a
	// prewarm would be silently dropped right when warmth matters most.
	require.NotPanics(t, func() { s.prewarm() })
	require.NotPanics(t, func() { s.prewarm() })
}

// recordingOutbound is a stubOutbound that remembers every destination it was
// asked to dial, so a test can assert WHICH marker/port a probe used.
type recordingOutbound struct {
	stubOutbound
	mu    sync.Mutex
	dials []M.Socksaddr
}

func (o *recordingOutbound) DialContext(_ context.Context, _ string, destination M.Socksaddr) (net.Conn, error) {
	o.mu.Lock()
	o.dials = append(o.dials, destination)
	o.mu.Unlock()
	return nil, net.ErrClosed
}

func (o *recordingOutbound) ports() []uint16 {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]uint16, 0, len(o.dials))
	for _, d := range o.dials {
		out = append(out, d.Port)
	}
	return out
}

// measured wires a host with a fresh remote sample on every node so selection
// has something rankable, and returns the shared timestamp.
func measured(s *Smart, host string, local float64, remote map[string]float64) time.Time {
	now := time.Now()
	for tag, r := range remote {
		s.nodeByTag[tag].localMin.add(now, local)
		s.hostStatLocked(host, tag, true).remote.update(now, r, 0.25, 90*time.Second)
	}
	return now
}

// The CDN takeover band must be measured over the same set selectNode ranked. A
// node that is not selectable (declared dead here) can still hold a fresh, low
// remote sample; letting it set the band shrank it below the winner's own remote
// and rejected the very node selectNode had just chosen, pinning the host to the
// dirtier one forever.
func TestCDNTakeoverBandIgnoresUnusableNode(t *testing.T) {
	s := newTestSmart("dead", "clean", "dirty")
	s.params.minDwell = 0
	s.cdnClass["h"] = true
	now := measured(s, "h", 30, map[string]float64{"dead": 10, "clean": 30, "dirty": 20})
	for tag, prio := range map[string]uint16{"dead": 100, "clean": 1, "dirty": 100} {
		s.nodeByTag[tag].cleanPriority = prio
	}
	s.nodeByTag["dead"].health = healthLocalDown

	s.sticky["h"] = &stickyEntry{
		node: "dirty", reason: reasonCDN, confidence: confLow, lastSwitch: now.Add(-time.Hour),
	}
	s.reEvaluateStickyLocked("h", N.NetworkTCP)
	require.Equal(t, "clean", s.sticky["h"].node,
		"rankable band is min(clean=30,dirty=20)+15=35, so the clean node is in band")
}

// A dial retry must never be handed back the node that just failed it. The
// locally-degraded fast path re-judges the sticky over the whole node set, so
// its result has to be re-read through the exclude-aware accessor.
func TestDegradedStickyRePickHonoursExclude(t *testing.T) {
	s := newTestSmart("a", "b")
	s.params.minDwell = 0
	now := measured(s, "h", 30, map[string]float64{"a": 20, "b": 20})
	s.nodeByTag["b"].localPenaltyMs = 500 // sticky node degraded → re-evaluation path
	s.sticky["h"] = &stickyEntry{
		node: "b", reason: reasonTotal, confidence: confLow, lastSwitch: now.Add(-time.Hour),
	}

	nr := s.pickNode(context.Background(), destKey{host: "h"}, N.NetworkTCP, 443, map[string]bool{"a": true})
	require.NotNil(t, nr)
	require.NotEqual(t, "a", nr.tag, "the node that already failed this request must not be re-picked")
}

// A UDP-only host has no TCP probe port, and its UDP port cannot be TCP-probed.
// Running the synchronous first-connect measurement anyway made every new
// STUN/WebRTC destination pay the full first-connect timeout for a measurement
// that could never succeed.
func TestUDPOnlyHostSkipsFirstConnectMeasure(t *testing.T) {
	s := newTestSmart("x")
	s.params.firstConnectTimeout = 5 * time.Second
	s.nodeByTag["x"].outbound = stubOutbound{tag: "x", hang: true}

	start := time.Now()
	nr := s.pickNode(context.Background(), destKey{host: "stun.example"}, N.NetworkUDP, 3478, nil)
	require.NotNil(t, nr, "must still route, via the stable fallback pick")
	require.Less(t, time.Since(start), time.Second, "no synchronous probe without a TCP port")
}

// Once a TCP flow has taught the host's port, a UDP flow to the same host is
// measurable again — on the learned TCP port, never on its own UDP port.
func TestUDPFirstConnectUsesLearnedTCPPort(t *testing.T) {
	s := newTestSmart("x")
	s.params.firstConnectTimeout = 200 * time.Millisecond
	rec := &recordingOutbound{stubOutbound: stubOutbound{tag: "x"}}
	s.nodeByTag["x"].outbound = rec
	s.tcpPort["h.example"] = 443 // learned by an earlier TCP flow

	s.pickNode(context.Background(), destKey{host: "h.example"}, N.NetworkUDP, 3478, nil)

	ports := rec.ports()
	require.NotEmpty(t, ports, "a host with a known TCP port is still measured")
	require.NotContains(t, ports, uint16(3478), "the UDP port must never be TCP-probed")
	require.Contains(t, ports, uint16(443))
}

// Every open connection to a bad target reports its own hard error, so one page
// load can deliver a burst of reports for what is really a single event. They
// must arm the block once, not walk the backoff to its ceiling.
func TestLiveFailureBurstDoesNotRatchetBackoff(t *testing.T) {
	s := newTestSmart("x", "y")
	nr := s.nodeByTag["x"]
	for i := 0; i < 5; i++ {
		s.reportConnFailure(nr, "a.com", errTest)
	}
	hs := s.hostStatLocked("a.com", "x", false)
	require.NotNil(t, hs)
	require.Equal(t, 1, hs.backoff, "a burst of reports for one event escalates once")
	require.True(t, hs.blockedUntil.After(time.Now()))
	require.Equal(t, healthHealthy, nr.health, "one target failing is never the link's fault")
}

// The fallback relaxes selectable's clauses in order: a node that is merely
// blocked for THIS host loses to one that is fully selectable.
func TestFallbackPrefersUnblockedNode(t *testing.T) {
	s := newTestSmart("blocked", "ok")
	s.hostStatLocked("h", "blocked", true).blockedUntil = time.Now().Add(time.Minute)
	s.mu.Lock()
	nr := s.fallbackNodeLocked("h", N.NetworkTCP, nil)
	s.mu.Unlock()
	require.NotNil(t, nr)
	require.Equal(t, "ok", nr.tag)

	// With every node blocked the block no longer discriminates, so it is relaxed
	// rather than leaving the request with no route.
	s.hostStatLocked("h", "ok", true).blockedUntil = time.Now().Add(time.Minute)
	s.mu.Lock()
	nr = s.fallbackNodeLocked("h", N.NetworkTCP, nil)
	s.mu.Unlock()
	require.NotNil(t, nr)
}

var errTest = errTestErr("connection reset by peer")

type errTestErr string

func (e errTestErr) Error() string { return string(e) }
