package naive

import (
	"math"
	"strconv"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

func TestConnectAckRoundTrips(t *testing.T) {
	t.Parallel()
	for _, ack := range []ConnectAck{
		{Total: 200 * time.Millisecond},
		{Total: 200 * time.Millisecond, Connect: 20 * time.Millisecond, HasConnect: true},
		{Total: 0, Connect: 0, HasConnect: true},
	} {
		parsed, ok := ParseConnectAck(FormatConnectAck(ack))
		require.True(t, ok)
		require.Equal(t, ack, parsed)
	}
}

func TestAbsentAckIsNotAMeasurementOfZero(t *testing.T) {
	t.Parallel()
	// The difference that matters: a server that measured nothing must not be
	// read as a server that measured zero, which would rank as a destination
	// sitting right next to the exit.
	_, ok := ParseConnectAck("")
	require.False(t, ok)

	ack, ok := ParseConnectAck("5000")
	require.True(t, ok)
	require.False(t, ack.HasConnect, "a hop that reported no connect time must say so")
	require.Equal(t, 5*time.Millisecond, ack.Total)
}

func TestAckRejectsValuesThatWouldOverflowIntoANegativeDuration(t *testing.T) {
	t.Parallel()
	// Scaling microseconds up to a Duration overflows int64 well before
	// ParseInt's own limit, and the result comes out negative — which reads as
	// an impossibly close destination and would capture every routing decision
	// it took part in.
	micros := int64(math.MaxInt64)
	overflow := strconv.FormatInt(micros, 10)
	// Computed at run time: as a constant expression the compiler rejects it,
	// which is exactly the check that is missing when the value arrives off the
	// wire and is scaled without a bound.
	require.Negative(t, time.Duration(micros)*time.Microsecond,
		"the value under test must really overflow")

	_, ok := ParseConnectAck(overflow)
	require.False(t, ok, "an overflowing total must be rejected")

	_, ok = ParseConnectAck("5000;d=" + overflow)
	require.False(t, ok, "an overflowing connect time must be rejected")
}

func TestAckRejectsMalformedInput(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"abc", "-1", "5000;d=-1", "5000;d=abc", ";d=1"} {
		_, ok := ParseConnectAck(value)
		require.False(t, ok, "value %q must be rejected", value)
	}
}

func TestAckIgnoresUnknownParameters(t *testing.T) {
	t.Parallel()
	// Room to add fields later without older clients misreading the value.
	ack, ok := ParseConnectAck("5000;x=1;d=2000")
	require.True(t, ok)
	require.Equal(t, 5*time.Millisecond, ack.Total)
	require.Equal(t, 2*time.Millisecond, ack.Connect)
	require.True(t, ack.HasConnect)
}

func TestProbeDestinationIsFreshEveryTime(t *testing.T) {
	t.Parallel()
	// A constant destination arriving on a timer is a clean thing to notice from
	// outside, so each heartbeat carries a new name and only the family is fixed.
	seen := make(map[string]bool)
	for range 64 {
		destination := ProbeDestination()
		require.True(t, IsProbe(destination), "the server has to still recognise it")
		require.False(t, seen[destination.Fqdn], "a repeated name defeats the point")
		seen[destination.Fqdn] = true
	}
}

func TestOnlyProbesAreRecognisedAsProbes(t *testing.T) {
	t.Parallel()
	// A real destination must never be answered locally instead of dialed.
	for _, host := range []string{
		"example.com", "probe.arpa.example.com", "arpa", "", "1.1.1.1",
	} {
		require.False(t, IsProbe(M.ParseSocksaddr(host+":443")), host)
	}
}

func TestConnectAckCarriesTheResolutionItTookOutOfTheConnect(t *testing.T) {
	t.Parallel()
	for _, ack := range []ConnectAck{
		{Total: 200 * time.Millisecond, Connect: 20 * time.Millisecond, HasConnect: true,
			Resolve: 50 * time.Millisecond, HasResolve: true},
		// Zero is a claim, not an absence: the destination was already known.
		{Total: 200 * time.Millisecond, Connect: 20 * time.Millisecond, HasConnect: true,
			Resolve: 0, HasResolve: true},
	} {
		parsed, ok := ParseConnectAck(FormatConnectAck(ack))
		require.True(t, ok)
		require.Equal(t, ack, parsed)
	}

	// A hop that predates the field says nothing either way, which is not the
	// same as saying the lookup cost nothing.
	older, ok := ParseConnectAck("200000;d=20000")
	require.True(t, ok)
	require.False(t, older.HasResolve)
}

func TestAckSkipsFieldsThisBuildDoesNotKnow(t *testing.T) {
	t.Parallel()
	// What makes either side able to add a field without waiting for the other,
	// and the reason the resolution span could be added at all.
	ack, ok := ParseConnectAck("200000;d=20000;z=1;n=50000;zz=whatever")
	require.True(t, ok)
	require.Equal(t, 20*time.Millisecond, ack.Connect)
	require.Equal(t, 50*time.Millisecond, ack.Resolve)
	require.True(t, ack.HasResolve)
}

func TestAMalformedResolutionMakesTheWholeValueUntrustworthy(t *testing.T) {
	t.Parallel()
	// Held to the connect's standard rather than skipped, because the client
	// moves this span from the node's leg onto the destination's — the two legs
	// that are ranked against each other.
	_, ok := ParseConnectAck("200000;d=20000;n=notanumber")
	require.False(t, ok)

	micros := int64(math.MaxInt64)
	_, ok = ParseConnectAck("200000;d=20000;n=" + strconv.FormatInt(micros, 10))
	require.False(t, ok, "an overflowing resolution must be rejected")
}

func TestALookupWithoutAConnectIsNotADestinationLeg(t *testing.T) {
	t.Parallel()
	// A header carrying a resolution and no connect is malformed — the two are
	// reported together or not at all — and the client must not end up holding
	// a destination leg it was never told. Absent stays absent.
	ack, ok := ParseConnectAck("200000;n=50000")
	require.True(t, ok, "an unknown-shaped ack is still a readable total")
	require.False(t, ack.HasConnect)
	require.True(t, ack.HasResolve)
}

func TestTheDestinationLegIsTheConnectPlusFindingIt(t *testing.T) {
	t.Parallel()
	// The one arithmetic decision the whole resolve field exists to make, and
	// it lives here rather than beside its caller so that the default build
	// tests it: the caller is behind an outbound build tag, so a test written
	// there never runs in CI and a silent revert to charging the node would
	// stay green.
	ack, ok := ParseConnectAck("51790;d=1040;n=50000")
	require.True(t, ok)
	leg, measured := ack.DestinationLeg()
	require.True(t, measured)
	require.Equal(t, 51040*time.Microsecond, leg,
		"finding the destination is part of reaching it")

	// An exit too old to report a lookup leaves it wherever it fell.
	older, ok := ParseConnectAck("51790;d=1040")
	require.True(t, ok)
	leg, measured = older.DestinationLeg()
	require.True(t, measured)
	require.Equal(t, 1040*time.Microsecond, leg)

	// A resolution with no connect beside it describes no leg, and must not
	// become one: the pair is what carries the invariant.
	orphan, ok := ParseConnectAck("51790;n=50000")
	require.True(t, ok)
	_, measured = orphan.DestinationLeg()
	require.False(t, measured)
}
