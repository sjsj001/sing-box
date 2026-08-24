package group

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/stretchr/testify/require"
)

// measured is a reading from a proxy that reported its own work, which is what
// makes the round trip splittable into legs.
func measured(local time.Duration) naive.ConnMeasurement {
	return naive.ConnMeasurement{RoundTrip: local, HasSpan: true, HasRemote: true}
}

func newMember(tag string, local time.Duration) *smartMember {
	member := &smartMember{tag: tag}
	member.healthy.Store(true)
	if local > 0 {
		member.record(measured(local))
	}
	return member
}

func TestGroupFirstFollowsConfiguredOrder(t *testing.T) {
	t.Parallel()
	// "first" means the operator picked one; measurements must not override it.
	fast := newMember("second", ms(10))
	preferred := newMember("first", ms(200))
	group := &smartGroup{tag: "g", members: []*smartMember{preferred, fast}}
	require.Equal(t, "first", group.current().tag)
}

func TestGroupFirstMovesOnlyOnFailure(t *testing.T) {
	t.Parallel()
	preferred := newMember("first", ms(200))
	backup := newMember("second", ms(10))
	group := &smartGroup{tag: "g", members: []*smartMember{preferred, backup}}
	require.Equal(t, "first", group.current().tag)

	preferred.healthy.Store(false)
	require.Equal(t, "second", group.current().tag)

	preferred.healthy.Store(true)
	require.Equal(t, "first", group.current().tag, "it must come back once it recovers")
}

func TestGroupFastestPicksTheNearestMember(t *testing.T) {
	t.Parallel()
	near := newMember("near", ms(29))
	far := newMember("far", ms(80))
	group := &smartGroup{tag: "g", selectFastest: true, members: []*smartMember{far, near}}
	require.Equal(t, "near", group.current().tag)
}

func TestGroupFastestHoldsItsChoiceWithinHysteresis(t *testing.T) {
	t.Parallel()
	// Members of one group still leave from different addresses, so trading
	// between two near-equal ones would undo the point of grouping them.
	incumbent := newMember("incumbent", ms(60))
	challenger := newMember("challenger", ms(80))
	group := &smartGroup{tag: "g", selectFastest: true, members: []*smartMember{incumbent, challenger}}
	require.Equal(t, "incumbent", group.current().tag)

	// The challenger pulls ahead, but only by 14ms.
	challenger.record(measured(ms(46)))
	require.Equal(t, "incumbent", group.current().tag)
}

func TestGroupFastestYieldsBeyondHysteresis(t *testing.T) {
	t.Parallel()
	incumbent := newMember("incumbent", ms(60))
	challenger := newMember("challenger", ms(80))
	group := &smartGroup{tag: "g", selectFastest: true, members: []*smartMember{incumbent, challenger}}
	require.Equal(t, "incumbent", group.current().tag)

	// 16ms better: worth the move.
	challenger.record(measured(ms(44)))
	require.Equal(t, "challenger", group.current().tag)
}

func TestGroupFastestDropsAnUnhealthyIncumbentAtOnce(t *testing.T) {
	t.Parallel()
	// Hysteresis is about noise, not about failure: a member that is gone must
	// be left immediately, however good its last measurement was.
	incumbent := newMember("incumbent", ms(29))
	backup := newMember("backup", ms(200))
	group := &smartGroup{tag: "g", selectFastest: true, members: []*smartMember{incumbent, backup}}
	require.Equal(t, "incumbent", group.current().tag)

	incumbent.healthy.Store(false)
	require.Equal(t, "backup", group.current().tag)
}

func TestGroupFastestCallsAnOutageAFailover(t *testing.T) {
	t.Parallel()
	// The backup is slower, so nothing about this move is a score. Recording it
	// as one puts "the other member measured faster" in the trail for an outage,
	// and an audit reading that goes looking for a latency change that never
	// happened — which is how a real one was misread.
	incumbent := newMember("incumbent", ms(29))
	backup := newMember("backup", ms(200))
	var reasons []string
	group := &smartGroup{
		tag: "g", selectFastest: true, members: []*smartMember{incumbent, backup},
		onMemberChange: func(_ string, _ string, _ string, reason string) {
			reasons = append(reasons, reason)
		},
	}
	require.Equal(t, "incumbent", group.current().tag)

	incumbent.healthy.Store(false)
	require.Equal(t, "backup", group.current().tag)
	require.Equal(t, []string{reasonFailover}, reasons)

	// And coming back is a score change, because that is what it is.
	incumbent.healthy.Store(true)
	require.Equal(t, "incumbent", group.current().tag)
	require.Equal(t, []string{reasonFailover, reasonScore}, reasons)
}

func TestGroupIsStableUnderMemberJitter(t *testing.T) {
	t.Parallel()
	a := newMember("a", ms(60))
	b := newMember("b", ms(62))
	group := &smartGroup{tag: "g", selectFastest: true, members: []*smartMember{a, b}}
	chosen := group.current().tag
	for i := range 500 {
		jitter := ms(i%9 - 4)
		a.record(measured(ms(60) + jitter))
		b.record(measured(ms(62) - jitter))
		require.Equal(t, chosen, group.current().tag, "member choice moved on jitter at iteration %d", i)
	}
}

func TestSilentProxyDoesNotBecomeALocalMeasurement(t *testing.T) {
	t.Parallel()
	// A proxy that answers without reporting what its own work cost leaves the
	// round trip unsplittable. Recording it as the client-to-proxy leg would
	// file the distance to the destination against the node, so the node would
	// be ranked by whichever destinations it happened to be asked for.
	member := &smartMember{tag: "silent"}
	member.record(naive.ConnMeasurement{RoundTrip: ms(400)})
	_, hasLocal := member.local()
	require.False(t, hasLocal, "an unsplittable round trip must not become a local reading")
	require.True(t, member.healthy.Load(), "but reaching the proxy still proves it is alive")

	member.record(measured(ms(30)))
	local, ok := member.local()
	require.True(t, ok)
	require.Equal(t, ms(30), local)
}

func TestGroupWithNothingHealthyStillOffersAMemberToTry(t *testing.T) {
	t.Parallel()
	// Offering a member believed to be down looks wrong until you follow what
	// happens next: dialing it either works, which marks it healthy, or fails,
	// which schedules an immediate confirming probe. Returning nothing instead
	// would make recovery wait for the next heartbeat round, so a blip that
	// briefly knocked out every member would keep failing traffic long after the
	// network came back.
	member := newMember("only", ms(29))
	group := &smartGroup{tag: "g", selectFastest: true, members: []*smartMember{member}}
	member.healthy.Store(false)
	require.Equal(t, "only", group.current().tag)

	// A healthy member is still preferred over one believed to be down.
	healthy := newMember("healthy", ms(200))
	group.members = append(group.members, healthy)
	require.Equal(t, "healthy", group.current().tag)
}

func TestGroupWithNoMembersHasNothingToOffer(t *testing.T) {
	t.Parallel()
	require.Nil(t, (&smartGroup{tag: "empty"}).current())
}

func TestAbandonedProbeDoesNotCondemnItsMember(t *testing.T) {
	t.Parallel()
	// A race cancels everyone it did not pick. That says nothing about those
	// members, and treating it as a fault takes healthy nodes out of service
	// every time they merely come second — which then distorts the next race,
	// because the members it excluded were never asked.
	smart := &Smart{logger: logger.NOP(), cache: newSmartCache(context.Background(), "smart")}
	group := &smartGroup{tag: "g"}
	member := newMember("loser", ms(30))

	cancelled := fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, context.Canceled)
	require.True(t, errors.Is(cancelled, context.Canceled),
		"the original cause must survive wrapping, or the guard below never fires")

	smart.reportFailure("apple.com", M.ParseSocksaddr("apple.com:443"), group, member, cancelled)
	require.True(t, member.healthy.Load(), "being abandoned is not a failure")

	smart.reportFailure("apple.com", M.ParseSocksaddr("apple.com:443"), group, member,
		fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, context.DeadlineExceeded))
	require.True(t, member.healthy.Load(), "a caller's own deadline is not a failure either")

	// A real transport failure is not shrugged off — but it is verified rather
	// than believed: the report requests a probe and the probe's verdict does
	// the condemning. See TestABlackholedLegCondemnsOnSightAndAnInstantFailureDoesNot
	// for the split, and TestOneOutageIsOneConfirmingProbe for the probe.
	smart.reportFailure("apple.com", M.ParseSocksaddr("apple.com:443"), group, member,
		fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, net.ErrClosed))
	require.True(t, member.healthy.Load(), "an instant failure awaits the probe's verdict")
}

func TestTheDerivedEstablishmentBoundStillCondemnsAMember(t *testing.T) {
	t.Parallel()
	// The bound awaitReady sets is also a deadline, and it arrives wrapped in
	// exactly the same way as a caller who lost interest. Only the sentinel
	// separates them, and getting it wrong either condemns healthy members or
	// leaves a blackholed leg in service.
	smart := &Smart{logger: logger.NOP(), cache: newSmartCache(context.Background(), "smart")}
	group := &smartGroup{tag: "g"}
	member := newMember("blackholed", ms(30))

	timedOut := fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, errLegTimedOut)
	require.True(t, errors.Is(timedOut, context.DeadlineExceeded),
		"the sentinel must read as a deadline, or nothing about this case is interesting")

	smart.reportFailure("apple.com", M.ParseSocksaddr("apple.com:443"), group, member, timedOut)
	require.False(t, member.healthy.Load(), "a leg that never came up is a verdict about the member")
}

func newSmartFromOptions(t *testing.T, options option.SmartOutboundOptions) (*Smart, error) {
	t.Helper()
	outbound, err := NewSmart(context.Background(), nil, logger.NOP(), "smart", options)
	if err != nil {
		return nil, err
	}
	return outbound.(*Smart), nil
}

func TestAlphaOfZeroIsASettingRatherThanAnAbsence(t *testing.T) {
	t.Parallel()
	// Ranking on the destination leg alone is a legitimate thing to ask for, and
	// with a bare float it is indistinguishable from not having asked at all.
	zero := 0.0
	smart, err := newSmartFromOptions(t, option.SmartOutboundOptions{
		Alpha:  &zero,
		Groups: []option.SmartGroupOptions{{Tag: "hk", Outbounds: []string{"hk-1"}}},
	})
	require.NoError(t, err)
	require.Equal(t, 0.0, smart.alpha)

	smart, err = newSmartFromOptions(t, option.SmartOutboundOptions{
		Groups: []option.SmartGroupOptions{{Tag: "hk", Outbounds: []string{"hk-1"}}},
	})
	require.NoError(t, err)
	require.Equal(t, defaultAlpha, smart.alpha, "omitting it still means the default")

	tooLarge := 1.5
	_, err = newSmartFromOptions(t, option.SmartOutboundOptions{
		Alpha:  &tooLarge,
		Groups: []option.SmartGroupOptions{{Tag: "hk", Outbounds: []string{"hk-1"}}},
	})
	require.Error(t, err)
}

func TestAmbiguousGroupingIsRejected(t *testing.T) {
	t.Parallel()
	// Snapshots are filed per group tag, so two groups sharing one overwrite
	// each other's measurements.
	_, err := newSmartFromOptions(t, option.SmartOutboundOptions{
		Groups: []option.SmartGroupOptions{
			{Tag: "hk", Outbounds: []string{"hk-1"}},
			{Tag: "hk", Outbounds: []string{"hk-2"}},
		},
	})
	require.ErrorContains(t, err, "duplicate group tag")

	// A group asserts that its members share a destination leg, so one outbound
	// in two groups asserts something contradictory — and gives that node two
	// health states and two measurement windows that drift apart.
	_, err = newSmartFromOptions(t, option.SmartOutboundOptions{
		Groups: []option.SmartGroupOptions{
			{Tag: "hk", Outbounds: []string{"shared"}},
			{Tag: "jp", Outbounds: []string{"shared"}},
		},
	})
	require.ErrorContains(t, err, "is in both group")
}

func TestNowNamesSomethingAllAlsoNames(t *testing.T) {
	t.Parallel()
	// A dashboard marks the entry in All that equals Now. A Now of
	// "group/member" matches nothing, and the panel shows no selection at all.
	member := newMember("hk-1", ms(29))
	smart := &Smart{
		alpha:  testAlpha,
		groups: []*smartGroup{{tag: "hk", members: []*smartMember{member}}},
	}
	require.Contains(t, smart.All(), smart.Now())
}

func TestAStarvedHandshakeDoesNotMakeANodeLookFurtherAway(t *testing.T) {
	t.Parallel()
	// The production failure, digit for digit. Eight members warmed at once and
	// their handshakes came back in an order that tracked scheduling rather than
	// geography — 78ms for the nearest, 747ms for a node 80ms away. A floor of
	// setup/5 then put that node at 149.4ms, nearly twice its real distance, and
	// it lost every race for the rest of the run. Losing meant it was never
	// dialed again, so no cheaper handshake ever arrived to correct it.
	//
	// The round trip is the bound: local is the round trip minus the proxy's own
	// span, and a span is never negative.
	starved := &smartMember{tag: "out-jp-ddps"}
	starved.record(naive.ConnMeasurement{
		Setup:     746801 * time.Microsecond,
		RoundTrip: 79714 * time.Microsecond, // a heartbeat: span is zero
		HasSpan:   true, HasRemote: true,
	})

	local, measured := starved.local()
	require.True(t, measured)
	require.Equal(t, 79714*time.Microsecond, local,
		"a handshake starved of CPU says nothing about how far away the node is")

	// Unclamped this would have been setup/5, which is what shipped.
	require.Greater(t, 746801*time.Microsecond/localFloorDivisor, local,
		"the case is only interesting while the handshake estimate exceeds the round trip")

	// The group is then ranked on its real distance, so it is not pruned against
	// a nearer one the moment that one answers.
	near := newMember("out-hk-r0", 28727*time.Microsecond)
	starvedBound := staticBound(testAlpha, local, 50*time.Millisecond)
	nearScore := score(testAlpha, mustLocal(t, near), 900*time.Microsecond, 0)
	require.Less(t, starvedBound, nearScore,
		"with a 50ms bonus it has to survive long enough to answer")
}

func TestTheFloorStillCatchesAProxyThatUnderstatesItsWork(t *testing.T) {
	t.Parallel()
	// Bounding the floor by the round trip must not disarm it. A proxy claiming
	// almost the whole round trip as its own work still gets pulled back, up to
	// the closest the round trip allows.
	member := &smartMember{tag: "liar"}
	member.record(naive.ConnMeasurement{
		Setup: 240 * time.Millisecond, RoundTrip: 200 * time.Millisecond,
		ServerSpan: 199 * time.Millisecond, HasSpan: true,
	})
	local, measured := member.local()
	require.True(t, measured)
	require.Equal(t, 240*time.Millisecond/localFloorDivisor, local,
		"48ms is what its own handshake allows, and 48 < the 200ms round trip")

	reported, floor, corrected := (&smartMember{tag: "liar2"}).record(naive.ConnMeasurement{
		Setup: 240 * time.Millisecond, RoundTrip: 200 * time.Millisecond,
		ServerSpan: 199 * time.Millisecond, HasSpan: true,
	})
	require.True(t, corrected, "a correction must be reported, not applied silently")
	require.Equal(t, time.Millisecond, reported)
	require.Equal(t, 48*time.Millisecond, floor)
}

func mustLocal(t *testing.T, member *smartMember) time.Duration {
	t.Helper()
	local, measured := member.local()
	require.True(t, measured)
	return local
}

func TestTheEstablishmentBoundComesFromTheHandshakeNotTheFloorOfTheRoundTrip(t *testing.T) {
	t.Parallel()
	// The round trip is kept as a rolling minimum — the floor of what the path
	// can do — so a small multiple of it is not a bound on a path that jitters.
	// Firing takes the whole node out of service, so the difference matters.
	member := &smartMember{tag: "m"}
	_, derived := member.readyTimeout()
	require.False(t, derived, "nothing measured yet means nothing to derive a bound from")

	// Warm streams only: no handshake has been paid for, so the round trip is
	// all there is.
	member.record(naive.ConnMeasurement{
		Setup: time.Millisecond, RoundTrip: ms(30), HasSpan: true,
	})
	budget, derived := member.readyTimeout()
	require.True(t, derived)
	require.Equal(t, ms(30)*readyTimeoutFactor+readyTimeoutFloor, budget)

	// One cold stream, and the bound switches to what establishing actually
	// costs — which on this member is four times what its round trip suggested.
	member.record(naive.ConnMeasurement{
		Setup: ms(120), RoundTrip: ms(150), ServerSpan: ms(120), HasSpan: true,
	})
	budget, derived = member.readyTimeout()
	require.True(t, derived)
	require.Equal(t, ms(120)*readyTimeoutFactor+readyTimeoutFloor, budget)
}

func TestWarmingCoversEveryPoolExactlyOnce(t *testing.T) {
	t.Parallel()
	// naive hands each stream to the next pool in rotation, so the number of
	// dials that leaves none of them cold is the member's own concurrency:
	// fewer leaves pools cold, more lands back on ones already warm.
	member := &smartMember{tag: "m"}
	require.Equal(t, 1, member.warmupDials(), "an outbound with no pools still needs one dial")

	member.outbound = pooledOutbound{pools: 4}
	require.Equal(t, 4, member.warmupDials())

	member.outbound = pooledOutbound{pools: 1}
	require.Equal(t, 1, member.warmupDials(), "the default must not open three useless connections")

	member.outbound = pooledOutbound{pools: 64}
	require.Equal(t, warmupDialsMax, member.warmupDials(), "and an extreme one is capped")
}

type pooledOutbound struct {
	adapter.Outbound
	pools int
}

func (o pooledOutbound) Pools() int { return o.pools }

func TestHeartbeatsCarryNoPeriodToLockOnto(t *testing.T) {
	t.Parallel()
	// The contents are encrypted; the timing is not. A small exchange on an
	// otherwise silent connection exactly every sixty seconds is a periodicity
	// anyone watching the flow can pick out without decrypting a byte.
	seen := make(map[time.Duration]bool)
	for range 200 {
		interval := nextHeartbeat()
		require.GreaterOrEqual(t, interval, heartbeatInterval-heartbeatJitter)
		require.Less(t, interval, heartbeatInterval+heartbeatJitter)
		seen[interval] = true
	}
	require.Greater(t, len(seen), 100, "the interval has to actually move")
}

func TestAProxyCannotClaimToBeCloserThanItsOwnHandshake(t *testing.T) {
	t.Parallel()
	// Both legs of the ranking are the proxy's word: report the span as the
	// whole round trip and the connect as zero, and the node presents itself as
	// sitting next to the client and next to every destination at once. The
	// handshake is the part it cannot touch, because this side timed it.
	member := &smartMember{tag: "liar"}
	member.record(naive.ConnMeasurement{
		Setup: ms(240), RoundTrip: ms(200), ServerSpan: ms(200), HasSpan: true,
	})

	local, measured := member.local()
	require.True(t, measured)
	require.Equal(t, ms(240)/localFloorDivisor, local,
		"a 240ms handshake cannot belong to a node claiming to be 0ms away")

	// An honest node is left alone: 60ms against a floor of 48ms.
	honest := &smartMember{tag: "honest"}
	honest.record(naive.ConnMeasurement{
		Setup: ms(240), RoundTrip: ms(200), ServerSpan: ms(140), HasSpan: true,
	})
	local, measured = honest.local()
	require.True(t, measured)
	require.Equal(t, ms(60), local, "the floor must never correct a plausible reading")
}

func TestAnImpossibleReportIsNotAMeasurement(t *testing.T) {
	t.Parallel()
	// A span cannot outlast the round trip that bracketed it, and a connect
	// cannot outlast the span that contains it. One impossible field makes the
	// whole account untrustworthy.
	overrun := naive.ConnMeasurement{
		RoundTrip: ms(100), ServerSpan: ms(400), HasSpan: true,
		RemoteDial: 0, HasRemote: true,
	}.Validated()
	require.False(t, overrun.HasSpan)
	require.False(t, overrun.HasRemote)

	fine := naive.ConnMeasurement{
		RoundTrip: ms(100), ServerSpan: ms(40), HasSpan: true,
		RemoteDial: ms(30), HasRemote: true,
	}.Validated()
	require.True(t, fine.HasSpan)
	require.True(t, fine.HasRemote)
}

func TestAnUnderstatedSpanTakesItsLocalLegDownWithIt(t *testing.T) {
	t.Parallel()
	// Measured against a relay whose session to its next hop was already up: it
	// reported 29µs of its own work and a 57.8ms connect inside it. Keeping the
	// span and dropping only the connect is the wrong half to keep — local is
	// the round trip minus the span, so a span understated to nothing hands the
	// node the whole distance to the destination as though it were its own.
	relay := naive.ConnMeasurement{
		RoundTrip:  196005 * time.Microsecond,
		ServerSpan: 29 * time.Microsecond,
		RemoteDial: 57773 * time.Microsecond,
		HasSpan:    true, HasRemote: true,
	}
	require.Equal(t, 195976*time.Microsecond, relay.Local(),
		"unvalidated, the node's own leg reads as the whole round trip")

	validated := relay.Validated()
	require.False(t, validated.HasRemote)
	require.False(t, validated.HasSpan,
		"the span is what makes local meaningful, and this one is provably wrong")

	// And nothing from it reaches the member's estimate of how far away it is.
	member := &smartMember{tag: "relay"}
	member.record(validated)
	_, hasLocal := member.local()
	require.False(t, hasLocal)
	require.True(t, member.healthy.Load(), "but reaching the proxy still proves it is alive")

	// The heartbeat, which the proxy answers itself, is unaffected: nothing was
	// dialed, so a zero span is the truth rather than an understatement.
	member.record(naive.ConnMeasurement{
		RoundTrip: 133218 * time.Microsecond,
		HasSpan:   true, HasRemote: true,
	}.Validated())
	local, hasLocal := member.local()
	require.True(t, hasLocal)
	require.Equal(t, 133218*time.Microsecond, local)
}

func TestGroupsAreStillOfferedBeforeAnythingIsMeasured(t *testing.T) {
	t.Parallel()
	// The listeners open before the warmup round lands. Dropping unmeasured
	// groups leaves nothing to race in that window, and every connection in it
	// fails outright with "no usable group" rather than merely being routed on
	// incomplete information.
	cold := &smartMember{tag: "cold-1"}
	cold.healthy.Store(true)
	smart := &Smart{
		alpha:  testAlpha,
		groups: []*smartGroup{{tag: "cold", members: []*smartMember{cold}}},
	}

	candidates := smart.candidates(nil, time.Now())
	require.Len(t, candidates, 1, "a group with nothing measured must still be offered")
	require.False(t, candidates[0].hasLocal)
	require.Equal(t, "cold", fallbackGroup(testAlpha, candidates),
		"with nothing to rank by, configured order beats refusing to dial")
	require.Equal(t, "", chosenGroup(testAlpha, candidates, ""),
		"but it must not be *scored*, which would mean scoring a guess")
}

func TestASnapshotCarriesAGroupUntilItIsMeasuredAgain(t *testing.T) {
	t.Parallel()
	// Straight after a restart nothing has been measured live, but the snapshot
	// kept what each leg cost. Using it is what lets a restart route from its
	// cache instead of re-racing every destination it had already decided.
	cold := &smartMember{tag: "cold-1"}
	cold.healthy.Store(true)
	smart := &Smart{
		alpha:  testAlpha,
		groups: []*smartGroup{{tag: "cold", members: []*smartMember{cold}}},
	}
	entry := &destinationEntry{
		Remote:  map[string]remoteWindow{"cold": {ms(2)}},
		Path:    map[string]time.Duration{"cold": ms(29)},
		RacedAt: time.Now(),
	}

	candidates := smart.candidates(entry, time.Now())
	require.Len(t, candidates, 1)
	require.True(t, candidates[0].hasLocal)
	require.Equal(t, ms(29), candidates[0].local)
	require.Equal(t, "cold", chosenGroup(testAlpha, candidates, ""))
}

func TestUDPSkipsAGroupWhoseMemberCannotCarryIt(t *testing.T) {
	t.Parallel()
	// The group is left out rather than switched to a sibling that can: swapping
	// would send one destination out of two different addresses depending on the
	// protocol, which is the split grouping exists to prevent.
	tcpOnly := newMember("tcp-only", ms(29))
	tcpOnly.outbound = fakeOutbound{networks: []string{N.NetworkTCP}}
	capable := newMember("both", ms(80))
	capable.outbound = fakeOutbound{networks: []string{N.NetworkTCP, N.NetworkUDP}}

	require.False(t, tcpOnly.supportsUDP())
	require.True(t, capable.supportsUDP())

	smart := &Smart{
		alpha: testAlpha,
		groups: []*smartGroup{
			{tag: "near", members: []*smartMember{tcpOnly}},
			{tag: "far", members: []*smartMember{capable}},
		},
	}
	require.Equal(t, []string{N.NetworkTCP, N.NetworkUDP}, smart.Network())

	entry := &destinationEntry{Remote: map[string]remoteWindow{"near": {ms(1)}, "far": {ms(1)}}}
	now := time.Now()
	require.Len(t, smart.candidates(entry, now), 2, "both groups can carry TCP")

	udp := smart.usableCandidates(entry, now, N.NetworkUDP)
	require.Len(t, udp, 1)
	require.Equal(t, "far", udp[0].tag)
}

func TestNetworkOmitsUDPWhenNoMemberCarriesIt(t *testing.T) {
	t.Parallel()
	// Reported up front so a rule needing UDP fails to match here, rather than
	// matching and then failing at dial time.
	member := newMember("tcp-only", ms(29))
	member.outbound = fakeOutbound{networks: []string{N.NetworkTCP}}
	smart := &Smart{groups: []*smartGroup{{tag: "g", members: []*smartMember{member}}}}
	require.Equal(t, []string{N.NetworkTCP}, smart.Network())
}

type fakeOutbound struct {
	adapter.Outbound
	networks []string
}

func (o fakeOutbound) Network() []string { return o.networks }

func TestANetworkChangeForgetsWhatTheOldPathMeasured(t *testing.T) {
	t.Parallel()
	// Every window here is a rolling minimum, and a minimum never recovers from
	// a floor that no longer exists. The bound on establishing the leg is three
	// times the cheapest handshake ever seen: carried from a fast network onto a
	// slow one it fires on every cold handshake, and each firing condemns the
	// member and fails the connection that paid for it — while the samples that
	// would raise the bound are the ones being cut short.
	member := newMember("a-1", ms(12))
	member.record(naive.ConnMeasurement{
		Setup: ms(60), RoundTrip: ms(70), ServerSpan: ms(58), RemoteDial: ms(58),
		HasSpan: true, HasRemote: true,
	})
	budget, derived := member.readyTimeout()
	require.True(t, derived)
	require.Equal(t, ms(60)*readyTimeoutFactor+readyTimeoutFloor, budget)

	member.resetPath()

	_, derived = member.readyTimeout()
	require.False(t, derived, "with the path gone there is nothing left to derive a bound from")
	_, measured := member.local()
	require.False(t, measured, "and nothing left to rank the member on until it is measured again")
	require.Zero(t, member.setup())
}

func TestAHeartbeatDoesNotMakeAnIdleMemberLookBusy(t *testing.T) {
	t.Parallel()
	// The heartbeat skips members that carried traffic within the interval, and
	// reads that from lastUsed — so a heartbeat that stamps lastUsed suppresses
	// the round after itself about half the time. An idle member then drifts to
	// around 90s between probes rather than the 60s the interval names, and its
	// pool is cold for the failover that reaches for it. Probes are this
	// outbound's own traffic; the request counters already exclude them for the
	// same reason.
	member := newMember("a-1", 0)
	member.outbound = &answeringOutbound{measurement: measured(ms(10))}
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		groups: []*smartGroup{{tag: "a", members: []*smartMember{member}}},
	}

	// The real heartbeat path, not a stand-in for it: this is the call whose own
	// bookkeeping used to decide whether the next round would run.
	smart.probeMember(context.Background(), member, keepWarm(member))
	require.True(t, member.healthy.Load(), "the probe answered")
	local, banked := member.local()
	require.True(t, banked, "everything else the probe taught is kept — only the stamp is not")
	require.Equal(t, ms(10), local)
	require.Len(t, smart.dueForHeartbeat(time.Now()), 1,
		"an idle member is due one interval after its own heartbeat, not two")

	// Real traffic is the thing that proves what a heartbeat would check.
	smart.record(member, measured(ms(10)))
	require.Empty(t, smart.dueForHeartbeat(time.Now()),
		"a member that just carried traffic has nothing left for a heartbeat to prove")

	// And once the interval has passed, it is due again.
	member.lastUsed.Store(time.Now().Add(-2 * heartbeatInterval).UnixNano())
	require.Len(t, smart.dueForHeartbeat(time.Now()), 1)
}

func TestAnUnhealthyMemberIsProbedNoMatterHowRecentlyItWasSeen(t *testing.T) {
	t.Parallel()
	// The skip exists to save traffic on members that are demonstrably fine.
	// A member marked down is the opposite case: the heartbeat is the only thing
	// that can find out it came back.
	member := newMember("a-1", 0)
	member.outbound = &answeringOutbound{measurement: measured(ms(10))}
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		groups: []*smartGroup{{tag: "a", members: []*smartMember{member}}},
	}
	smart.record(member, measured(ms(10)))
	require.Empty(t, smart.dueForHeartbeat(time.Now()))

	member.healthy.Store(false)
	require.Len(t, smart.dueForHeartbeat(time.Now()), 1,
		"an unhealthy member is probed however recently it was used")
}
