package group

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

func TestACostEstimatorSeesLossThatAMinimumCannot(t *testing.T) {
	t.Parallel()
	// The case that decides which estimator this leg gets. A hop dropping one
	// packet in four is, at its best, indistinguishable from a perfect one, so
	// a minimum reports the clean samples and reports them for ever. What a
	// ranking has to compare is what the next connection will pay.
	clean, lossy := ms(1), ms(680)
	sequence := []time.Duration{clean, clean, lossy, clean, clean, lossy, clean, clean}

	var floor rollingMin
	var cost rollingCost
	for _, sample := range sequence {
		floor.add(sample)
		cost.add(sample)
	}

	seenByMin, _ := floor.value()
	seenByCost, _ := cost.value()
	require.Equal(t, clean, seenByMin, "a minimum cannot see loss at all")
	require.Greater(t, seenByCost, ms(90),
		"the mean of what was actually paid does, even with the worst sample set aside")
}

func TestTheWorstSampleIsSetAsideExactlyOnce(t *testing.T) {
	t.Parallel()
	// One cold session per window is the artefact this is shaped around, so one
	// sample is discarded. A second one is real and has to be seen.
	var one rollingCost
	for _, sample := range []time.Duration{ms(1), ms(1), ms(1), ms(1), ms(1), ms(1), ms(1), ms(400)} {
		one.add(sample)
	}
	value, _ := one.value()
	require.Equal(t, ms(1), value, "a lone cold handshake is set aside")

	var two rollingCost
	for _, sample := range []time.Duration{ms(1), ms(1), ms(1), ms(1), ms(1), ms(1), ms(400), ms(400)} {
		two.add(sample)
	}
	value, _ = two.value()
	require.Greater(t, value, ms(50), "a second one is the path, not an artefact")
}

func TestACostWindowTooShortToTrimReportsNothing(t *testing.T) {
	t.Parallel()
	// Deliberately not the rule remoteWindow follows. That window holds repeated
	// readings of one destination, so its newest is a fair guess at the next
	// one. This window is fed by whichever destinations were dialed, and its
	// first samples are not a random pair: heartbeats are answered by the proxy
	// and never cross the interior, so the first thing to write here is a
	// member's first real connection — exactly when the relay's session to its
	// exit and the exit's resolver are both cold. Reported untrimmed, one such
	// sample lands in local() for the whole group and moves a selection that
	// switchHysteresis then holds after the window has recovered.
	var window rollingCost
	_, ok := window.value()
	require.False(t, ok, "an empty window stands for nothing")

	window.add(ms(5))
	_, ok = window.value()
	require.False(t, ok, "one sample is not a distribution, and nothing can be set aside")

	window.add(ms(300))
	_, ok = window.value()
	require.False(t, ok, "nor are two")

	window.add(ms(5))
	value, ok := window.value()
	require.True(t, ok)
	require.Equal(t, ms(5), value, "at the quorum the worst of the three is the one set aside")
}

func TestOneColdInteriorCannotMoveASelectionOnItsOwn(t *testing.T) {
	t.Parallel()
	// The regression this window's quorum exists for. Two members a hair apart,
	// the faster one meets a cold exit resolver on its first real connection.
	// Counted, that member loses the group; and because the incumbent then only
	// has to stay within switchHysteresis, it keeps it after the window has
	// recovered — one cold sample, one permanent move.
	quick, slow := newMember("quick", ms(150)), newMember("slow", ms(160))
	group := &smartGroup{tag: "us", selectFastest: true, members: []*smartMember{quick, slow}}
	require.Equal(t, quick, group.current(), "the faster member starts with it")

	cold := naive.ConnMeasurement{
		RoundTrip: ms(210), ServerSpan: ms(60), RemoteDial: 0,
		HasSpan: true, HasRemote: true,
	}
	quick.record(cold)
	local, _ := quick.local()
	require.Equal(t, ms(150), local, "one crossing says nothing about the interior yet")
	require.Equal(t, quick, group.current(), "so it cannot cost the member the group")
}

func TestTheHeartbeatAddressNeverTeachesTheChainWindow(t *testing.T) {
	t.Parallel()
	// The most dangerous single line in this change. The first proxy answers
	// the heartbeat address itself and reports a span of zero, which computes
	// an interior of zero — and zero is a claim, not an absence. Left
	// unguarded, eight heartbeats would flush a relay's interior out of sight.
	member := newMember("relay", 0)

	relayed := naive.ConnMeasurement{
		RoundTrip: ms(300), ServerSpan: ms(170), RemoteDial: ms(8),
		HasSpan: true, HasRemote: true,
	}
	for range medianQuorum {
		member.record(relayed)
	}
	chain, measured := member.chain()
	require.True(t, measured)
	require.Equal(t, ms(162), chain, "traffic crosses the interior and reports it")

	// The proxy's own address: rtt is the near hop, and the interior it claims
	// is zero because it never dialed.
	answered := naive.ConnMeasurement{RoundTrip: ms(130), HasSpan: true, HasRemote: true}
	for range localWindow * 2 {
		member.recordProbe(answered, false)
	}
	chain, measured = member.chain()
	require.True(t, measured)
	require.Equal(t, ms(162), chain,
		"a probe that never left the proxy must not be able to report on what is beyond it")

	// A probe that did go somewhere real teaches it, as traffic does.
	member.recordProbe(naive.ConnMeasurement{
		RoundTrip: ms(320), ServerSpan: ms(190), RemoteDial: ms(10),
		HasSpan: true, HasRemote: true,
	}, true)
	chain, _ = member.chain()
	require.Equal(t, ms(162), chain,
		"four samples, the largest set aside, and the rest are the three at 162")
}

func TestAColdLookupAtTheExitIsNotTheMembersInterior(t *testing.T) {
	t.Parallel()
	// Opening a page full of hosts nobody has resolved yet used to move a
	// single-hop member. The interior is span minus connect, and the exit takes
	// its lookup out of the connect but not out of the span, so every cold
	// resolution landed in the node's own leg — four of them in a window of
	// eight put it past the point where it changes which member a group uses,
	// and past the floor that decides what the member's heartbeat has to reach.
	//
	// Reported on the wire, the lookup goes to the destination leg instead.
	measured := newMember("direct", 0)
	blind := newMember("direct-old-exit", 0)
	for i := range localWindow {
		cold := i < localWindow/2
		var resolve time.Duration
		if cold {
			resolve = ms(50)
		}
		// The same connection, seen by an exit that reports its lookup and by
		// one too old to: the round trip and the span are identical, only the
		// destination leg differs by what was moved onto it.
		span := 1790*time.Microsecond + resolve
		measured.record(naive.ConnMeasurement{
			RoundTrip:  155710*time.Microsecond + span,
			ServerSpan: span,
			RemoteDial: 1040*time.Microsecond + resolve,
			HasSpan:    true, HasRemote: true,
		})
		blind.record(naive.ConnMeasurement{
			RoundTrip:  155710*time.Microsecond + span,
			ServerSpan: span,
			RemoteDial: 1040 * time.Microsecond,
			HasSpan:    true, HasRemote: true,
		})
	}

	chain, ok := measured.chain()
	require.True(t, ok)
	require.Equal(t, 750*time.Microsecond, chain,
		"the interior is the exit's own routing, and it did not change")
	require.False(t, measured.relayed(), "so nothing here looks like a relay")
	local, _ := measured.local()
	require.Equal(t, 156460*time.Microsecond, local)

	// What it used to do, which is also what an exit too old to report the
	// lookup still does — stated so the cost of that half-upgraded pairing is
	// visible rather than assumed away.
	blindChain, _ := blind.chain()
	require.Greater(t, blindChain, chainProbeFloor,
		"charged to the node, four cold lookups cross the floor on their own")
	require.True(t, blind.relayed())
}

func TestAMemberIsClassifiedByTheInteriorItWasMeasuredToHave(t *testing.T) {
	t.Parallel()
	// What decides whether this member's heartbeat has to reach somewhere real.
	direct := newMember("direct", 0)
	require.False(t, direct.relayed(), "nothing measured yet, and nothing claimed")

	// The interior a proxy that dials destinations itself has: its own routing
	// and name resolution.
	for range 4 {
		direct.record(naive.ConnMeasurement{
			RoundTrip: ms(158), ServerSpan: 1790 * time.Microsecond, RemoteDial: 1040 * time.Microsecond,
			HasSpan: true, HasRemote: true,
		})
	}
	require.False(t, direct.relayed(), "half a millisecond is not an interior worth probing for")

	relay := newMember("relay", 0)
	for range 4 {
		relay.record(naive.ConnMeasurement{
			RoundTrip: ms(300), ServerSpan: ms(170), RemoteDial: ms(8),
			HasSpan: true, HasRemote: true,
		})
	}
	require.True(t, relay.relayed())

	// Sticky through the gap: a member that drops between the two thresholds
	// keeps what it was, so it does not alternate between two kinds of probe.
	for range localWindow {
		relay.record(naive.ConnMeasurement{
			RoundTrip: ms(150), ServerSpan: ms(15), RemoteDial: 0,
			HasSpan: true, HasRemote: true,
		})
	}
	require.True(t, relay.relayed(), "15ms is inside the hysteresis gap")

	for range localWindow {
		relay.record(naive.ConnMeasurement{
			RoundTrip: ms(150), ServerSpan: ms(2), RemoteDial: ms(1),
			HasSpan: true, HasRemote: true,
		})
	}
	require.False(t, relay.relayed(), "measured small enough for long enough, it is not a relay")
}

func TestANetworkChangeKeepsWhatWasLearnedBeyondTheProxy(t *testing.T) {
	t.Parallel()
	// resetPath drops every window that is a minimum, because a minimum never
	// recovers from a floor that no longer exists. The interior is neither a
	// minimum nor on this side of the change — and it is what decides whether
	// this member's heartbeat can see that leg at all, so dropping it would put
	// every relay back on the blind probe after each network switch.
	member := newMember("relay", 0)
	for range 4 {
		member.record(naive.ConnMeasurement{
			RoundTrip: ms(300), ServerSpan: ms(170), RemoteDial: ms(8),
			HasSpan: true, HasRemote: true,
		})
	}
	require.True(t, member.relayed())

	member.resetPath()

	_, hasNear := member.near()
	require.False(t, hasNear, "the client's own leg is measured again from scratch")
	chain, hasChain := member.chain()
	require.True(t, hasChain, "what lies beyond the proxy did not change with our network")
	require.Equal(t, ms(162), chain)
	require.True(t, member.relayed(), "so the member is still probed in a way that can see it")
}

func TestAGroupLeavesARelayWhoseInteriorWentBad(t *testing.T) {
	t.Parallel()
	// The production failure, in the arithmetic that produced it. A relay and a
	// direct member of one group, chosen by whichever is nearer. The relay's
	// own leg is genuinely shorter — that is why it was configured — and the
	// interior between it and the exit is what went from under a millisecond to
	// a sixth of a second.
	relay := newMember("relay", 0)
	direct := newMember("direct", 0)
	group := &smartGroup{tag: "us", selectFastest: true, members: []*smartMember{relay, direct}}

	healthy := func() {
		// near 132ms, interior 8.6ms — the relay is nearer, and it should win.
		relay.record(naive.ConnMeasurement{
			RoundTrip: 140600 * time.Microsecond, ServerSpan: 22600 * time.Microsecond,
			RemoteDial: ms(14), HasSpan: true, HasRemote: true,
		})
		// near 155.7ms, interior 0.75ms.
		direct.record(naive.ConnMeasurement{
			RoundTrip: 157500 * time.Microsecond, ServerSpan: 1790 * time.Microsecond,
			RemoteDial: 1040 * time.Microsecond, HasSpan: true, HasRemote: true,
		})
	}
	for range localWindow {
		healthy()
	}
	require.Equal(t, "relay", group.current().tag,
		"while its interior is small the relay really is the shorter path, and keeps its place")

	// The interior degrades to 162ms. Nothing about the relay's own leg changed
	// — which is exactly why this was invisible: local read 132ms throughout.
	for range localWindow {
		relay.record(naive.ConnMeasurement{
			RoundTrip: 307900 * time.Microsecond, ServerSpan: 175900 * time.Microsecond,
			RemoteDial: ms(14), HasSpan: true, HasRemote: true,
		})
	}

	near, _ := relay.near()
	require.Equal(t, ms(132), near, "its own leg is untouched, as it was in production")
	local, _ := relay.local()
	require.Greater(t, local, ms(280), "but the path through it is not")
	require.Equal(t, "direct", group.current().tag,
		"so the group leaves it, which is what never happened")
}

func TestADirectMemberIsUnmovedByTheChange(t *testing.T) {
	t.Parallel()
	// The other half of the requirement. A member that dials destinations
	// itself has an interior too — its own routing and name resolution — and if
	// counting it moved anything, this would be a change to every deployment
	// rather than a fix for chained ones.
	direct := newMember("direct", 0)
	for range localWindow {
		direct.record(naive.ConnMeasurement{
			RoundTrip: 157500 * time.Microsecond, ServerSpan: 1790 * time.Microsecond,
			RemoteDial: 1040 * time.Microsecond, HasSpan: true, HasRemote: true,
		})
	}
	near, _ := direct.near()
	local, _ := direct.local()
	require.Equal(t, 155710*time.Microsecond, near)
	require.Equal(t, 156460*time.Microsecond, local)
	require.Less(t, local-near, switchHysteresis,
		"the difference has to be far below what moves a selection, or this is a change for everyone")
}

func TestABlindProbeBudgetDoesNotKillAChainedMember(t *testing.T) {
	t.Parallel()
	// These two have to ship together. The heartbeat address is answered by the
	// first proxy, so its budget is the near hop; a probe that goes somewhere
	// real crosses the whole chain and needs the time that takes. Budgeting the
	// second from the first is how a member whose interior had grown to three
	// seconds gets condemned every round for missing a 3.06s deadline.
	smart := &Smart{ctx: context.Background(), logger: logger.NOP()}
	relay := newMember("relay", 0)
	for range localWindow {
		relay.record(naive.ConnMeasurement{
			RoundTrip: ms(3150), ServerSpan: ms(3018), RemoteDial: ms(14),
			HasSpan: true, HasRemote: true,
		})
	}

	warm := smart.probeTimeout(relay, naive.ProbeDestination())
	crossing := smart.probeTimeout(relay, reachabilityDestinations[0])
	require.Equal(t, ms(132)*probeTimeoutFactor+probeTimeoutFloor, warm,
		"the address the proxy answers is bounded by the leg to the proxy")
	require.Greater(t, crossing, ms(3150),
		"and the one that crosses the chain has to allow what crossing it costs")
	require.True(t, relay.relayed(), "a member with an interior this size is probed for it")
}

func TestOldSnapshotsDoNotStampedeAfterAnUpgrade(t *testing.T) {
	// The stored leg changed meaning, so it changed key with it. Read as a path,
	// an old near hop would be compared against a live one that now includes the
	// interior — a doubling on every settled connection, three in a row is a
	// confirmed anomaly, and every destination on a relay would re-race within
	// minutes of the upgrade. Absent reads as no measurement, and observeScore
	// returns before it can conclude anything.
	var decoded destinationEntry
	require.NoError(t, json.Unmarshal(
		[]byte(`{"remote":{"us":[14000000]},"local":{"us":132000000},"selected":"us","raced_at":"2026-08-09T12:00:00Z"}`),
		&decoded))
	_, ok := decoded.pathFor("us")
	require.False(t, ok, "a leg stored under the old meaning is not read as the new one")
	_, _, ok = decoded.legsFor("us")
	require.False(t, ok, "so the drift check has nothing to compare and stays quiet")

	remote, ok := decoded.remoteFor("us")
	require.True(t, ok, "while everything the upgrade must not throw away survives")
	require.Equal(t, ms(14), remote)
	require.Equal(t, "us", decoded.selected())
}

func TestInteriorDriftIsNotChargedToTheDestinationLeg(t *testing.T) {
	t.Parallel()
	// The gap a trimmed mean leaves: an interior that drops packets without its
	// base latency changing keeps most samples where they were, so the window
	// needs two bad ones before it moves. Every affected connection pays in
	// full though, one at a time, and comparing per connection is what sees it.
	//
	// Where the confirmation is written down then matters. The destination leg
	// did not move, and writing this against it would file a verdict about the
	// exit for something that happened before the exit.
	fixture := newRaceFixture(t, 0, map[string][2]time.Duration{"a": {ms(29), ms(5)}})
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)
	group := fixture.smart.groups[0]
	member := group.members[0]

	for range localWindow {
		member.record(naive.ConnMeasurement{
			RoundTrip: ms(40), ServerSpan: ms(11), RemoteDial: ms(5),
			HasSpan: true, HasRemote: true,
		})
	}
	held, _ := member.chain()
	require.Equal(t, ms(6), held)

	fixture.smart.cache.store(key, &destinationEntry{
		Remote:   map[string]remoteWindow{"a": {ms(5)}},
		Path:     map[string]time.Duration{"a": ms(35)},
		Selected: "a",
		RacedAt:  time.Now(),
	})

	// Connections whose interior has collapsed, the destination leg unchanged.
	// Recorded then observed, in that order, which is what a settled connection
	// does.
	drifted := naive.ConnMeasurement{
		RoundTrip: ms(700), ServerSpan: ms(671), RemoteDial: ms(5),
		HasSpan: true, HasRemote: true,
	}
	for range anomalyStreak {
		fixture.smart.record(member, drifted)
		fixture.smart.observeScore(key, destination, group, member, drifted)
	}

	after, _ := member.chain()
	require.Greater(t, after, ms(100),
		"the readings that made the streak are in the member's own window already")

	entry := fixture.smart.cache.load(key)
	remote, _ := entry.remoteFor("a")
	require.Equal(t, ms(5), remote,
		"and nothing is charged to a destination leg that never moved")

	require.Eventually(t, func() bool {
		return fixture.outbounds["a"].settledConns() == 1
	}, time.Second, time.Millisecond, "the refresh the streak asked for still runs")
}

func TestASmallInteriorCannotSwallowADestinationThatMoved(t *testing.T) {
	t.Parallel()
	// A multiple on its own cannot carry the attribution. A member that dials
	// destinations itself has an interior of half a millisecond at the middle
	// and 1.3 at the ninetieth percentile, so two ordinary samples of the same
	// healthy member are already a multiple apart — and on that scale the ratio
	// fires on noise and hands a real destination move to the wrong leg, which
	// throws the one live reading allowed to touch the snapshot away.
	//
	// Observed without recording, to hold the member's window still: what is
	// under test is where the confirmation is filed, not how the window fills.
	fixture := newRaceFixture(t, 0, map[string][2]time.Duration{"a": {ms(29), ms(5)}})
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)
	group := fixture.smart.groups[0]
	member := group.members[0]

	for range localWindow {
		member.record(naive.ConnMeasurement{
			RoundTrip: ms(40), ServerSpan: ms(11), RemoteDial: ms(5),
			HasSpan: true, HasRemote: true,
		})
	}
	held, _ := member.chain()
	require.Equal(t, ms(6), held)

	fixture.smart.cache.store(key, &destinationEntry{
		Remote:   map[string]remoteWindow{"a": {ms(5)}},
		Path:     map[string]time.Duration{"a": ms(35)},
		Selected: "a",
		RacedAt:  time.Now(),
	})

	// The destination moved from 5ms to 400ms. The interior went from 6ms to
	// 13ms, which is past anomalyFactor and nowhere near enough to move a
	// selection — so it is not what changed this one.
	moved := naive.ConnMeasurement{
		RoundTrip: ms(443), ServerSpan: ms(413), RemoteDial: ms(400),
		HasSpan: true, HasRemote: true,
	}
	live, ok := moved.ChainSpan()
	require.True(t, ok)
	require.Greater(t, live, held*anomalyFactor, "the ratio alone would call this the interior")
	require.Less(t, live-held, switchHysteresis, "while the move itself cannot decide anything")

	for range anomalyStreak {
		fixture.smart.observeScore(key, destination, group, member, moved)
	}

	entry := fixture.smart.cache.load(key)
	remote, _ := entry.remoteFor("a")
	require.Equal(t, ms(400), remote,
		"the destination leg moved and the confirmation belongs to it")
}

func TestDriftIsWeighedAgainstTheMemberThatCarriedTheConnection(t *testing.T) {
	t.Parallel()
	// The testimony arriving here is one connection's, and the window it has to
	// be weighed against is that connection's member — not whichever member the
	// group would hand out now. A heartbeat condemning the carrier on another
	// goroutine makes an election fail over mid-settle, and the comparison would
	// then read one member's live interior against a sibling's window, at which
	// point the attribution reverses and the evidence is filed against whichever
	// leg did not move.
	fixture := newRaceFixture(t, 0, map[string][2]time.Duration{"a": {ms(29), ms(5)}})
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)
	group := fixture.smart.groups[0]
	carrier := group.members[0]

	sibling := newMember("a-2", ms(20))
	sibling.outbound = fixture.outbounds["a"]
	group.members = append(group.members, sibling)
	group.selectFastest = true

	for range localWindow {
		carrier.record(naive.ConnMeasurement{
			RoundTrip: ms(140), ServerSpan: ms(105), RemoteDial: ms(5),
			HasSpan: true, HasRemote: true,
		})
		sibling.record(naive.ConnMeasurement{
			RoundTrip: ms(40), ServerSpan: ms(11), RemoteDial: ms(5),
			HasSpan: true, HasRemote: true,
		})
	}
	carrierChain, _ := carrier.chain()
	siblingChain, _ := sibling.chain()
	require.Equal(t, ms(100), carrierChain)
	require.Equal(t, ms(6), siblingChain)
	require.Equal(t, sibling, group.current(),
		"the group would hand out the sibling, which is the whole point")

	fixture.smart.cache.store(key, &destinationEntry{
		Remote:   map[string]remoteWindow{"a": {ms(5)}},
		Path:     map[string]time.Duration{"a": ms(35)},
		Selected: "a",
		RacedAt:  time.Now(),
	})

	// The destination moved. The carrier's interior is exactly where it has been
	// all along — but it is a hundred milliseconds, so read against the
	// sibling's six it looks like the interior is what moved.
	moved := naive.ConnMeasurement{
		RoundTrip: ms(529), ServerSpan: ms(500), RemoteDial: ms(400),
		HasSpan: true, HasRemote: true,
	}
	for range anomalyStreak {
		fixture.smart.observeScore(key, destination, group, carrier, moved)
	}

	entry := fixture.smart.cache.load(key)
	remote, _ := entry.remoteFor("a")
	require.Equal(t, ms(400), remote,
		"weighed against the carrier, its interior did not move and the destination did")
}

func TestOurOwnUplinkGoingBadIsNotBlamedOnAnyMember(t *testing.T) {
	t.Parallel()
	// The property that makes the attribution safe, and it is structural rather
	// than tuned: the interior is span minus connect, so this client's uplink
	// congesting raises the round trip and the near hop together and leaves the
	// interior exactly where it was. There is no path by which our network
	// going bad counts against a member's chain.
	fixture := newRaceFixture(t, 0, map[string][2]time.Duration{"a": {ms(29), ms(5)}})
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)
	group := fixture.smart.groups[0]
	member := group.members[0]

	for range localWindow {
		member.record(naive.ConnMeasurement{
			RoundTrip: ms(40), ServerSpan: ms(11), RemoteDial: ms(5),
			HasSpan: true, HasRemote: true,
		})
	}
	held, _ := member.chain()

	fixture.smart.cache.store(key, &destinationEntry{
		Remote:   map[string]remoteWindow{"a": {ms(5)}},
		Path:     map[string]time.Duration{"a": ms(35)},
		Selected: "a",
		RacedAt:  time.Now(),
	})

	// Our own leg collapses; everything beyond the proxy is untouched.
	congested := naive.ConnMeasurement{
		RoundTrip: ms(900), ServerSpan: ms(11), RemoteDial: ms(5),
		HasSpan: true, HasRemote: true,
	}
	for range anomalyStreak {
		fixture.smart.record(member, congested)
		fixture.smart.observeScore(key, destination, group, member, congested)
	}

	after, _ := member.chain()
	require.Equal(t, held, after,
		"our uplink says nothing about the leg between the proxy and its exit")
}
