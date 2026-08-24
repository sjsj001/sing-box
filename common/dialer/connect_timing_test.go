package dialer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDirectHopReportsTheDialWithResolutionTakenOut(t *testing.T) {
	t.Parallel()
	// A cold resolver cache can cost more than a second, and left in it would be
	// attributed to the path to the destination — making a perfectly good exit
	// look unusable for as long as the measurement is cached.
	_, timing := WithConnectTiming(context.Background())
	timing.RecordDNS(30 * time.Millisecond)
	timing.RecordDial(50 * time.Millisecond)

	total, connect, hasConnect, ok := timing.Spans()
	require.True(t, ok)
	require.True(t, hasConnect)
	require.Equal(t, 20*time.Millisecond, connect)
	require.GreaterOrEqual(t, total, 50*time.Millisecond)
}

func TestARelayTotalCoversTheWaitForItsNextHop(t *testing.T) {
	t.Parallel()
	// The shape seen in production: a relay whose session to the next hop is
	// already up. Opening a stream on it returns in microseconds, and everything
	// the hop actually spends getting the destination reachable happens
	// afterwards, while it waits for the next hop to answer.
	//
	// Reporting only the dial made total smaller than the connect it is supposed
	// to contain — which is impossible on its face, and which a client reads as
	// the hop being infinitely close to both ends at once.
	_, timing := WithConnectTiming(context.Background())
	timing.MarkRelay()
	timing.RecordDial(30 * time.Microsecond) // a stream on a warm session

	// The wait for the next hop's answer, which is where the real cost is.
	time.Sleep(20 * time.Millisecond)
	timing.RecordInnerAck(15 * time.Millisecond)

	total, connect, hasConnect, ok := timing.Spans()
	require.True(t, ok)
	require.True(t, hasConnect)
	require.Equal(t, 15*time.Millisecond, connect)
	require.GreaterOrEqual(t, total, connect,
		"a hop cannot have spent less than the connect its own span contains")
	require.GreaterOrEqual(t, total, 20*time.Millisecond)
}

func TestTheTotalIsStampedOnceAndKept(t *testing.T) {
	t.Parallel()
	// It is subtracted from the client's round trip, so two calls reporting two
	// different numbers would mean the split depended on who asked.
	_, timing := WithConnectTiming(context.Background())
	timing.RecordDial(time.Millisecond)

	first, _, _, ok := timing.Spans()
	require.True(t, ok)
	time.Sleep(10 * time.Millisecond)
	second, _, _, _ := timing.Spans()
	require.Equal(t, first, second)
}

func TestARelayWhoseNextHopSaidNothingReportsNoConnect(t *testing.T) {
	t.Parallel()
	// On a relay the only honest connect time is the one the next hop reports.
	// Absent means unknown; a small number would mean "very close", and that lie
	// would win every comparison it took part in.
	_, timing := WithConnectTiming(context.Background())
	timing.MarkRelay()
	timing.RecordDial(40 * time.Millisecond)

	total, _, hasConnect, ok := timing.Spans()
	require.True(t, ok)
	require.False(t, hasConnect)
	require.GreaterOrEqual(t, total, 40*time.Millisecond)
}

func TestNothingIsReportedBeforeADialCompletes(t *testing.T) {
	t.Parallel()
	_, timing := WithConnectTiming(context.Background())
	_, _, _, ok := timing.Spans()
	require.False(t, ok)

	var absent *ConnectTiming
	_, _, _, ok = absent.Spans()
	require.False(t, ok, "every method has to tolerate a nil receiver")
}

func TestATimingBuiltWithoutAStartFallsBackToTheDial(t *testing.T) {
	t.Parallel()
	// Otherwise the zero time turns into a total of several decades, which the
	// client would read as a proxy that has been working on this since 1970.
	timing := &ConnectTiming{}
	timing.RecordDial(40 * time.Millisecond)

	total, _, _, ok := timing.Spans()
	require.True(t, ok)
	require.Equal(t, 40*time.Millisecond, total)
}

func TestResolutionIsReportedSoTheClientCanChargeItToTheDestination(t *testing.T) {
	t.Parallel()
	ctx, timing := WithConnectTiming(context.Background())
	require.NotNil(t, ConnectTimingFromContext(ctx))
	timing.RecordDNS(50 * time.Millisecond)
	timing.RecordDial(60 * time.Millisecond)

	_, connect, hasConnect, ok := timing.Spans()
	require.True(t, ok)
	require.True(t, hasConnect)
	resolve, hasResolve := timing.Resolution()
	require.True(t, hasResolve)
	require.Equal(t, 50*time.Millisecond, resolve)
	require.Equal(t, 60*time.Millisecond, connect+resolve,
		"the invariant the client relies on: what was found plus what was reached is the dial")
}

func TestAnAddressThatNeededNoLookupSaysSoRatherThanSayingNothing(t *testing.T) {
	t.Parallel()
	// Zero is a claim here, and a useful one: it is what an IP literal or a warm
	// cache produces, and it tells the client there is nothing to move off the
	// node's leg. Absent would leave the client guessing.
	_, timing := WithConnectTiming(context.Background())
	timing.RecordDial(20 * time.Millisecond)

	resolve, hasResolve := timing.Resolution()
	require.True(t, hasResolve)
	require.Zero(t, resolve)
}

func TestARelayPassesTheInnerResolutionThroughAndNeverItsOwn(t *testing.T) {
	t.Parallel()
	// A relay's own lookup found a proxy, not a destination, so it belongs in
	// this hop's interior and must not be handed to the client as the
	// destination's. Only what the next hop reported passes through.
	_, timing := WithConnectTiming(context.Background())
	timing.RecordDNS(40 * time.Millisecond)
	timing.RecordDial(45 * time.Millisecond)
	timing.RecordInnerAck(15 * time.Millisecond)
	timing.RecordInnerResolution(3 * time.Millisecond)

	resolve, hasResolve := timing.Resolution()
	require.True(t, hasResolve)
	require.Equal(t, 3*time.Millisecond, resolve)
}

func TestARelayWhoseNextHopIsTooOldToReportOneReportsNothing(t *testing.T) {
	t.Parallel()
	// Absent rather than this hop's own, and rather than zero: a next hop that
	// predates the field made no claim, and inventing one would move time off
	// the destination leg that was never on it.
	_, timing := WithConnectTiming(context.Background())
	timing.RecordDNS(40 * time.Millisecond)
	timing.RecordDial(45 * time.Millisecond)
	timing.RecordInnerAck(15 * time.Millisecond)

	_, hasResolve := timing.Resolution()
	require.False(t, hasResolve)
}
