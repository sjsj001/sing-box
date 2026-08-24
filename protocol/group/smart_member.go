package group

// The state a group and a member carry between decisions: which member a group
// is using, what each member's path has been measured at, and how much each has
// carried. The decision core in smart_decision.go turns these into a choice;
// nothing here talks to the network.

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing/common"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type smartGroup struct {
	tag           string
	bonus         time.Duration
	selectFastest bool
	members       []*smartMember
	selected      atomic.Pointer[smartMember]
	// onMemberChange is how a member swap reaches the audit trail. It is a
	// callback rather than a back-reference so the group stays something the
	// decision tests can build on its own.
	onMemberChange func(group string, from string, to string, reason string)
	// reasons counts what this group carried, by what put it here. The
	// breakdown is the useful half: a group carrying everything because it keeps
	// winning races reads very differently from one carrying everything because
	// the rest keep failing over onto it.
	reasons [reasonCount]counter
}

// Why a request went where it did, as an index rather than the string the logs
// carry, so counting one costs an atomic add rather than a map write on every
// connection.
const (
	usageScore = iota
	usageSticky
	usageFallback
	usageRaced
	usageFailover
	usageRideAlong
	usageOther
	reasonCount
)

var usageReasonNames = [reasonCount]string{
	usageScore: reasonScore, usageSticky: reasonSticky, usageFallback: reasonFallback,
	usageRaced: reasonRaced, usageFailover: reasonFailover, usageRideAlong: reasonRideAlong,
	usageOther: "other",
}

// counter is a running total that can also be read as what has happened since
// it was last read. Both are wanted: the total is what the process has carried,
// and the difference is what a window carried — and a trail written in windows
// can be added up across a restart, which one written in totals cannot.
type counter struct {
	total    atomic.Int64
	reported atomic.Int64
}

func (c *counter) Add(delta int64) {
	c.total.Add(delta)
}

func (c *counter) delta() int64 {
	total := c.total.Load()
	return total - c.reported.Swap(total)
}

func reasonIndex(reason string) int {
	switch reason {
	case reasonScore:
		return usageScore
	case reasonSticky:
		return usageSticky
	case reasonFallback:
		return usageFallback
	case reasonRaced:
		return usageRaced
	case reasonFailover:
		return usageFailover
	case reasonRideAlong:
		return usageRideAlong
	default:
		return usageOther
	}
}

type smartMember struct {
	tag      string
	outbound adapter.Outbound
	access   sync.Mutex
	window   rollingMin
	// setupWindow is the rolling minimum of the establishment costs that
	// actually contained a handshake. It is what bounds establishment, kept
	// apart from window because the two measure different things: window is how
	// far away the proxy is, setupWindow is what reaching it from cold costs.
	setupWindow rollingMin
	// roundTripWindow is the rolling minimum of the round trips observed here.
	// It is the ceiling on how far away this member can be: local is the round
	// trip minus the proxy's own span, and a span is never negative.
	roundTripWindow rollingMin
	// chainWindow is what the interior of this member's chain costs — the part
	// of its span that is neither this client's leg nor the innermost connect.
	// Zero on a proxy that dials destinations itself; on a relay it is the leg
	// that nothing else here measures.
	//
	// A cost rather than a minimum, deliberately: see rollingCost. It is also
	// the one window that survives resetPath, for the reason given there.
	chainWindow rollingCost
	// chained records whether this member has been measured to have an interior
	// worth watching, which decides what its heartbeat has to reach. Sticky
	// across the classification's own hysteresis so a member sitting on the
	// line does not alternate between two kinds of probe.
	chained atomic.Bool
	healthy atomic.Bool
	// lastUsed is when this member last completed a measurement for traffic —
	// a race probe or a connection the caller is holding. It is what lets the
	// heartbeat skip busy members, so a heartbeat's own measurement is
	// deliberately not counted here; see smartMember.recordProbe.
	lastUsed atomic.Int64
	// lastSetup is the most recent connection-establishment cost, in
	// nanoseconds. It is what a race hands back to a group whose pool went cold.
	lastSetup atomic.Int64
	// requestsTCP and requestsUDP count what this member actually carried, as
	// distinct from what it was measured against. Probes and heartbeats are
	// excluded: they are this outbound's own traffic, and counting them would
	// make an idle member look busy.
	requestsTCP counter
	requestsUDP counter
	// probing is set while a confirming probe of this member is in flight, so a
	// node dying under load produces one probe rather than one per connection
	// that noticed. See Smart.confirmDown.
	probing atomic.Bool
	// unsupported keeps the "this server has no smart extension" warning to one
	// line rather than one per heartbeat, forever.
	unsupported sync.Once
}

// supportsUDP reports whether this member can carry UDP at all — naive only
// does when UDP-over-TCP is enabled on it.
func (m *smartMember) supportsUDP() bool {
	return m.outbound != nil && common.Contains(m.outbound.Network(), N.NetworkUDP)
}

func (m *smartMember) setup() time.Duration {
	return time.Duration(m.lastSetup.Load())
}

// warmupDials is how many connections it takes to leave none of this member's
// pools cold.
//
// naive hands each new stream to the next pool in rotation, so the count is the
// member's own concurrency and nothing else: fewer leaves some pools cold and
// the handshake lands on whichever probe draws one, later leaves the extra
// dials landing back on pools that are already warm.
func (m *smartMember) warmupDials() int {
	pooled, isPooled := common.Cast[naive.PooledOutbound](m.outbound)
	if !isPooled {
		return 1
	}
	dials := pooled.Pools()
	if dials < 1 {
		return 1
	}
	return min(dials, warmupDialsMax)
}

// local is this client to the innermost proxy: the near hop plus whatever the
// chain costs in between. It is what the documentation has always called
// local, and what a ranking needs — the round trip with the one leg that
// belongs to the destination taken out.
//
// An unmeasured interior counts as none. Refusing to rank a member for it
// would drop it from current, selectGroup and fallbackGroup at once, which is
// a far worse answer than treating a member nothing has crossed as the single
// hop it appears to be.
func (m *smartMember) local() (time.Duration, bool) {
	m.access.Lock()
	defer m.access.Unlock()
	near, ok := m.window.value()
	if !ok {
		return 0, false
	}
	chain, _ := m.chainWindow.value()
	return near + chain, true
}

// near is the leg to the first proxy on its own, which is what the heartbeat
// address measures and what the locally-timed handshake can bound. Separate
// from local because the two stop being the same thing on a relay.
func (m *smartMember) near() (time.Duration, bool) {
	m.access.Lock()
	defer m.access.Unlock()
	return m.window.value()
}

// chain is what the interior of this member's chain costs. Absent rather than
// zero when nothing has crossed it: a member nothing has measured this way is
// not a member known to have no interior.
func (m *smartMember) chain() (time.Duration, bool) {
	m.access.Lock()
	defer m.access.Unlock()
	return m.chainWindow.value()
}

// chainProbeFloor and chainProbeCeiling classify a member by the interior it
// has been measured to have, which decides what its heartbeat has to reach.
// The gap between them is hysteresis: a member sitting on the line would
// otherwise alternate between two kinds of probe, and the kind it gets is what
// produces the samples the classification reads.
//
// The floor comes from measurement. Members that dial destinations themselves
// put the middle of their interior at half a millisecond, the ninetieth
// percentile at 1.3ms and the worst of two hours at 4.4ms; twenty is more than
// four times that worst case. It also sits below the point where an interior
// starts changing which member a group picks, so nothing that could move a
// selection goes unmeasured.
//
// Those numbers describe an interior with the exit's name resolution taken out
// of it, which is what the wire now reports separately. Against an exit too old
// to report it the interior carries its lookups, four cold ones in a window of
// eight are enough to cross this floor, and such a member is classified as a
// relay until they age out — a bounded misreading whose only consequence is
// which address its heartbeat asks for.
const (
	chainProbeFloor   = 20 * time.Millisecond
	chainProbeCeiling = 10 * time.Millisecond
)

// relayed reports whether this member has an interior worth watching. Sticky:
// once a member has been seen to have one, only measuring a small one takes it
// back, and a member with no measurement at all keeps whatever it was.
func (m *smartMember) relayed() bool {
	chain, measured := m.chain()
	if !measured {
		return m.chained.Load()
	}
	if chain > chainProbeFloor {
		m.chained.Store(true)
	} else if chain < chainProbeCeiling {
		m.chained.Store(false)
	}
	return m.chained.Load()
}

// resetPath forgets everything measured about the path to this member.
//
// Called when the network underneath changed: every rolling window here is a
// minimum, and a minimum never recovers from a floor that no longer exists. The
// concrete failure is the establishment bound — 3× the cheapest handshake ever
// seen — surviving a move from a fast network to a slow one: every cold
// handshake on the new network overruns the old bound, each overrun condemns
// the member and fails the connection that paid for it, and the samples that
// would raise the bound are exactly the ones being cut short. Warm reconnects
// sit below coldSetupFloor and never enter the window, so nothing corrects it.
// Forgetting returns the member to the cold state, where the bound is not
// derived at all until the new network has been measured.
func (m *smartMember) resetPath() {
	m.lastSetup.Store(0)
	m.access.Lock()
	defer m.access.Unlock()
	m.window = rollingMin{}
	m.setupWindow = rollingMin{}
	m.roundTripWindow = rollingMin{}
	// chainWindow deliberately survives, and it is the only one that does.
	// Every window above is a minimum, and the reason they are dropped is that
	// a minimum never recovers from a floor that no longer exists. This one is
	// neither: it is a mean, and what it measures — the first proxy onwards —
	// is on the far side of the change. Clearing it would also put every relay
	// back on the heartbeat address after each network switch, which is the
	// blind state this measurement exists to leave.
}

// readyTimeout bounds how long establishing the connection to this member may
// take before the leg is called broken.
//
// It is derived from the establishment cost observed for this member rather
// than from its round trip. The round trip is deliberately kept as a rolling
// *minimum* — the floor of what the path can do — and a small multiple of a
// floor is not a bound on a path that jitters: on a lossy link the typical round
// trip is already several times the minimum, so a bound built from it fires on
// members that are perfectly healthy, and firing takes the whole node out of
// service. The setup cost is measured on this side, cannot be misreported, and
// is the very quantity being bounded.
func (m *smartMember) readyTimeout() (time.Duration, bool) {
	m.access.Lock()
	setup, hasSetup := m.setupWindow.value()
	// The near window, deliberately, not local(). What this bounds is the
	// connection to the first proxy coming up, and nothing beyond that proxy is
	// inside it — a relay with a slow interior would otherwise buy itself a
	// wider blackhole budget for the one leg the interior has nothing to do
	// with.
	near, hasNear := m.window.value()
	m.access.Unlock()
	switch {
	case hasSetup:
		return setup*readyTimeoutFactor + readyTimeoutFloor, true
	case hasNear:
		// Nothing has paid for a handshake yet — every stream so far landed on a
		// warm pool. The round trip at least scales with distance, which is
		// better than a fixed number.
		return near*readyTimeoutFactor + readyTimeoutFloor, true
	default:
		return 0, false
	}
}

// record banks a measurement and says so when the floor had to override what
// the proxy reported.
func (s *Smart) record(member *smartMember, measurement naive.ConnMeasurement) {
	reported, floor, corrected := member.record(measurement)
	s.reportCorrection(member, reported, floor, corrected)
}

// recordProbe is record for a heartbeat's own measurement. See
// smartMember.recordProbe for why the two cannot be the same call.
func (s *Smart) recordProbe(member *smartMember, destination M.Socksaddr, measurement naive.ConnMeasurement) {
	// The heartbeat address never leaves the proxy, so it has nothing to say
	// about what lies beyond it. Anything else the probe was pointed at does.
	reported, floor, corrected := member.recordProbe(measurement, !naive.IsProbe(destination))
	s.reportCorrection(member, reported, floor, corrected)
}

func (s *Smart) reportCorrection(member *smartMember, reported time.Duration, floor time.Duration, corrected bool) {
	if !corrected {
		return
	}
	s.logger.Debug("member ", member.tag, " reported local=", short(reported),
		", raised to ", short(floor), ": its own handshake and round trip put it no closer")
}

// localFloorLocked is the closest this member could honestly be, from the two
// quantities the proxy has no say in. access must be held.
//
// The handshake gives the estimate: TCP plus TLS costs several round trips, so
// a fraction of the cheapest one ever seen is a conservative distance. On its
// own that estimate is not safe, because a handshake can be slow for reasons
// that have nothing to do with distance — a burst of them competing for the
// same cores will do it — and a starved one reads as a node much further away
// than it is. Seen in production: eight members warming at once produced
// handshakes from 78ms to 1.5s in an order that tracked scheduling rather than
// geography, and the member that drew the longest was recorded at twice its
// real distance and lost every race for the rest of the run.
//
// The round trip bounds it. Local is the round trip minus the span the proxy
// claimed, and a span is never negative, so local never exceeds the round trip
// that contained it — which is timed on this side. A floor above the smallest
// round trip ever observed is therefore provably too high, whatever the
// handshake suggested.
func (m *smartMember) localFloorLocked() (time.Duration, bool) {
	setup, hasSetup := m.setupWindow.value()
	if !hasSetup {
		return 0, false
	}
	floor := setup / localFloorDivisor
	if roundTrip, hasRoundTrip := m.roundTripWindow.value(); hasRoundTrip && floor > roundTrip {
		floor = roundTrip
	}
	return floor, true
}

// record banks a measurement, reporting the reading it rejected when the floor
// had to correct one. Silent correction is what let a floor built from a
// contended handshake go unnoticed while it took a node out of service.
func (m *smartMember) record(measurement naive.ConnMeasurement) (reported time.Duration, floor time.Duration, corrected bool) {
	// Traffic always crosses the whole chain, so what it says about the
	// interior is always worth keeping.
	return m.recordMeasurement(measurement, true, true)
}

// recordProbe banks a heartbeat's measurement. Everything a measurement teaches
// is kept — health, the windows, the setup cost — except that lastUsed is left
// alone, because a heartbeat is not the member being used.
//
// lastUsed is what heartbeatRound reads to skip members that carried traffic
// recently. Stamped by the heartbeat itself, it suppresses the round after a
// probe about half the time: the interval is 60s with 20s of jitter, so an
// idle member ends up probed at around 90s instead of 60s — cold for exactly
// the failover that reaches for it. The request counters already exclude
// probes for the same reason: this outbound's own traffic must not make a
// member look busy. Every probe path shares this — warmup, the probe round
// after a network change, and the confirming probe all land here too.
func (m *smartMember) recordProbe(measurement naive.ConnMeasurement, traversed bool) (reported time.Duration, floor time.Duration, corrected bool) {
	return m.recordMeasurement(measurement, false, traversed)
}

func (m *smartMember) recordMeasurement(measurement naive.ConnMeasurement, used bool, traversed bool) (reported time.Duration, floor time.Duration, corrected bool) {
	// Reaching the proxy at all proves it is alive, whatever it chose to report.
	m.lastSetup.Store(int64(measurement.Setup))
	m.healthy.Store(true)
	if used {
		m.lastUsed.Store(time.Now().UnixNano())
	}
	m.access.Lock()
	defer m.access.Unlock()
	if measurement.Setup >= coldSetupFloor {
		m.setupWindow.add(measurement.Setup)
	}
	if measurement.RoundTrip > 0 {
		m.roundTripWindow.add(measurement.RoundTrip)
	}
	if !measurement.HasSpan {
		// A proxy that did not say what its own work cost leaves a round trip
		// that cannot be split. Recording it as the client-to-proxy leg would
		// file the distance to the destination against the node itself, and the
		// node is then judged on destinations it happened to be asked for.
		return 0, 0, false
	}
	if traversed {
		// Only a measurement that actually crossed the chain says anything
		// about its interior. The heartbeat address does not: the first proxy
		// answers it itself and reports a span of zero, which computes an
		// interior of zero — and zero here is a claim, not an absence. Left
		// unguarded, every heartbeat would push one into this window and eight
		// of them would flush a relay's interior out of sight entirely.
		if chain, ok := measurement.ChainSpan(); ok {
			m.chainWindow.add(chain)
		}
	}
	local := measurement.Local()
	bound, hasBound := m.localFloorLocked()
	if hasBound && local < bound {
		// The proxy's account puts it closer than it can honestly be. Either its
		// clock is wrong or it is claiming more of the round trip as its own
		// work than it spent, and the two are indistinguishable from here — so
		// the leg is recorded at the closest it can be rather than dropped,
		// which would take a working node out of the ranking on a suspicion.
		m.window.add(bound)
		return local, bound, true
	}
	m.window.add(local)
	return local, 0, false
}

// current reports the member a group uses, and records it.
//
// Exactly one member is used at a time, never a rotation: two connections to the
// same destination leaving from different addresses is precisely what breaks
// logged-in sessions and trips risk checks. For the same reason the choice is
// held in place by the same hysteresis that governs the groups themselves —
// members of one group still have different exit addresses, so letting the two
// nearest ones trade back and forth on a millisecond of jitter would reintroduce
// exactly the problem the group was supposed to remove.
func (g *smartGroup) current() *smartMember {
	if !g.selectFastest {
		// Configured order, and it only moves when a member fails.
		return g.record(g.firstHealthy(), reasonFailover)
	}

	var (
		best      *smartMember
		bestLocal time.Duration
	)
	for _, member := range g.members {
		if !member.healthy.Load() {
			continue
		}
		local, measured := member.local()
		if !measured {
			continue
		}
		if best == nil || local < bestLocal {
			best, bestLocal = member, local
		}
	}
	if best == nil {
		// Nothing measured yet: fall back rather than refuse to dial.
		return g.record(g.firstHealthy(), reasonFallback)
	}

	incumbent := g.selected.Load()
	if incumbent != nil && incumbent != best {
		if !incumbent.healthy.Load() {
			// Not a change of mind. The member in use was condemned and this is
			// what is left, which is a failover however it is reached — and the
			// hysteresis below deliberately does not apply, because there is
			// nothing to hold on to. Recording it as a score change puts "the
			// other one measured faster" in the trail for what was an outage,
			// and that is the reading that sends an audit looking in the wrong
			// place: it did that here before this line existed.
			return g.record(best, reasonFailover)
		}
		if local, measured := incumbent.local(); measured && bestLocal > local-switchHysteresis {
			return incumbent
		}
	}
	return g.record(best, reasonScore)
}

// firstHealthy returns the first member believed to be up, or — when none is —
// the first member anyway.
//
// Offering a member that is believed to be down looks wrong until you follow
// what happens next: dialing it either works, which marks it healthy again, or
// fails, which schedules an immediate confirming probe. Either way the group
// recovers on the next request. Returning nothing instead would make recovery
// wait for the next heartbeat round, so a blip that briefly knocked out every
// member would keep failing traffic long after the network came back.
func (g *smartGroup) firstHealthy() *smartMember {
	for _, member := range g.members {
		if member.healthy.Load() {
			return member
		}
	}
	if len(g.members) > 0 {
		return g.members[0]
	}
	return nil
}

// record notes the member the group is using, announcing it when that is a
// change.
//
// A member change moves every destination on this group to a different exit
// address without any of them changing group, so it is exactly as visible to a
// site as a group change and exactly as invisible in a log that only records
// group choices.
func (g *smartGroup) record(member *smartMember, reason string) *smartMember {
	if member == nil {
		return nil
	}
	previous := g.selected.Swap(member)
	if previous != nil && previous != member && g.onMemberChange != nil {
		g.onMemberChange(g.tag, previous.tag, member.tag, reason)
	}
	return member
}
