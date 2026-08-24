package naive

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestThePathDividesIntoThreeLegs(t *testing.T) {
	t.Parallel()
	// The identity the whole relay-aware ranking rests on. It holds without
	// anything being added to the protocol, because each hop's span already
	// contains the wait for the hop beyond it and the innermost connect is
	// passed up unchanged.
	relay := ConnMeasurement{
		RoundTrip:  3150 * time.Millisecond,
		ServerSpan: 3018 * time.Millisecond,
		RemoteDial: 14 * time.Millisecond,
		HasSpan:    true,
		HasRemote:  true,
	}
	chain, ok := relay.ChainSpan()
	require.True(t, ok)
	require.Equal(t, 132*time.Millisecond, relay.NearHop())
	require.Equal(t, 3004*time.Millisecond, chain)
	require.Equal(t, relay.RoundTrip, relay.NearHop()+chain+relay.RemoteDial,
		"the three legs have to add back up to what the client timed")

	// A member that dials the destination itself has an interior too — its own
	// routing and name resolution — and it is small. This is the sample the
	// classification threshold was set against.
	direct := ConnMeasurement{
		RoundTrip:  157500 * time.Microsecond,
		ServerSpan: 1790 * time.Microsecond,
		RemoteDial: 1040 * time.Microsecond,
		HasSpan:    true,
		HasRemote:  true,
	}
	chain, ok = direct.ChainSpan()
	require.True(t, ok)
	require.Equal(t, 750*time.Microsecond, chain)
	require.Equal(t, direct.RoundTrip, direct.NearHop()+chain+direct.RemoteDial)
}

func TestAnUnmeasuredInteriorIsAbsentRatherThanZero(t *testing.T) {
	t.Parallel()
	// Zero would say the chain has no interior, which is exactly what a relay
	// looks like when nothing measures it — the reading this exists to stop.
	silent := ConnMeasurement{
		RoundTrip:  200 * time.Millisecond,
		ServerSpan: 50 * time.Millisecond,
		HasSpan:    true,
	}
	_, ok := silent.ChainSpan()
	require.False(t, ok, "a hop that reported no connect leaves the interior unknown")

	// And the heartbeat address, which the proxy answers without dialing,
	// honestly reports an interior of nothing.
	answered := ConnMeasurement{
		RoundTrip: 128 * time.Millisecond,
		HasSpan:   true,
		HasRemote: true,
	}
	chain, ok := answered.ChainSpan()
	require.True(t, ok)
	require.Zero(t, chain)
}

func TestFindingTheDestinationCountsAgainstTheDestination(t *testing.T) {
	t.Parallel()
	// The exit's own lookup used to land in the chain's interior, because the
	// interior is span minus connect and the exit takes its resolution out of
	// the connect but not out of the span. That charged a node for something
	// that belongs to whichever destination it happened to be asked for.
	//
	// Reported separately, it joins the destination leg, and the identity that
	// everything else rests on is unchanged — which is the reason it is moved
	// rather than simply removed.
	cold := ConnMeasurement{
		RoundTrip:  207500 * time.Microsecond,
		ServerSpan: 51790 * time.Microsecond,
		RemoteDial: 1040*time.Microsecond + 50*time.Millisecond,
		HasSpan:    true,
		HasRemote:  true,
	}
	chain, ok := cold.ChainSpan()
	require.True(t, ok)
	require.Equal(t, 155710*time.Microsecond, cold.NearHop())
	require.Equal(t, 750*time.Microsecond, chain,
		"the interior is the exit's routing, and a cold lookup is not that")
	require.Equal(t, cold.RoundTrip, cold.NearHop()+chain+cold.RemoteDial,
		"the three legs still add back up to what the client timed")

	// The same connection as an older exit reports it: no resolution field, so
	// the lookup stays where it was and the interior carries it.
	blind := cold
	blind.RemoteDial = 1040 * time.Microsecond
	blindChain, ok := blind.ChainSpan()
	require.True(t, ok)
	require.Equal(t, 50750*time.Microsecond, blindChain,
		"an exit that does not report it leaves things exactly as they were")
}
