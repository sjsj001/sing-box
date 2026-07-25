package group

import (
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"

	"github.com/stretchr/testify/require"
)

func TestKeyOfDomain(t *testing.T) {
	// Keyed on the FULL host, not eTLD+1: sibling hosts under one domain must be
	// able to land on different nodes (the leaseweb per-location requirement).
	k := keyOf(&adapter.InboundContext{Destination: M.Socksaddr{Fqdn: "www.apple.com", Port: 443}})
	require.Equal(t, "www.apple.com", k.host)
	a := keyOf(&adapter.InboundContext{Destination: M.Socksaddr{Fqdn: "hk.leaseweb.net", Port: 443}})
	b := keyOf(&adapter.InboundContext{Destination: M.Socksaddr{Fqdn: "sg.leaseweb.net", Port: 443}})
	require.NotEqual(t, a.host, b.host, "same domain, different hosts must not share a bucket")
}

func TestKeyOfIP(t *testing.T) {
	k := keyOf(&adapter.InboundContext{Destination: M.ParseSocksaddr("1.1.1.1:443")})
	require.Equal(t, "1.1.1.1", k.host)
}

func TestKeyOfSniffAndDomainFallback(t *testing.T) {
	// A non-domain destination falls back to the sniffed host, then Domain.
	k := keyOf(&adapter.InboundContext{Destination: M.ParseSocksaddr("1.1.1.1:443"), SniffHost: "sniffed.example"})
	require.Equal(t, "sniffed.example", k.host)
	k = keyOf(&adapter.InboundContext{Destination: M.ParseSocksaddr("1.1.1.1:443"), Domain: "meta.example"})
	require.Equal(t, "meta.example", k.host)
}

func TestKeyOfEmpty(t *testing.T) {
	require.Empty(t, keyOf(nil).host)
	require.Empty(t, keyOf(&adapter.InboundContext{}).host)
}

func TestRollingMinIgnoresSpikes(t *testing.T) {
	base := time.Unix(0, 0)
	r := newRollingMin(90 * time.Second)
	r.add(base, 30)
	r.add(base.Add(time.Second), 1000) // bufferbloat spike
	r.add(base.Add(2*time.Second), 35)
	m, ok := r.min(base.Add(3 * time.Second))
	require.True(t, ok)
	require.Equal(t, 30.0, m) // spike ignored
}

func TestRollingMinEvictsWindow(t *testing.T) {
	base := time.Unix(0, 0)
	r := newRollingMin(10 * time.Second)
	r.add(base, 30)
	r.add(base.Add(20*time.Second), 200) // old 30 falls out of window
	m, ok := r.min(base.Add(20 * time.Second))
	require.True(t, ok)
	require.Equal(t, 200.0, m)
}

func TestEWMAFreshStale(t *testing.T) {
	base := time.Unix(0, 0)
	var e ewma
	require.Equal(t, sampleMissing, e.state(base, 90*time.Second))
	e.update(base, 100, 0.25, 90*time.Second)
	require.Equal(t, sampleFresh, e.state(base.Add(time.Second), 90*time.Second))
	require.Equal(t, sampleStale, e.state(base.Add(200*time.Second), 90*time.Second))
}

func TestEWMAConverges(t *testing.T) {
	base := time.Unix(0, 0)
	var e ewma
	e.update(base, 100, 0.25, 90*time.Second)
	e.update(base, 200, 0.25, 90*time.Second)
	require.InDelta(t, 125.0, e.value, 0.001) // 0.25*200 + 0.75*100
}

func TestIsOutlier(t *testing.T) {
	require.True(t, isOutlier(1000, 40)) // RTO-style spike
	require.False(t, isOutlier(60, 40))  // normal jitter
	require.False(t, isOutlier(120, 40)) // 3x boundary not exceeded strictly? 3*40=120, not > 120
	require.True(t, isOutlier(900, 40))  // > 40+800
}

func TestObserveRemoteLoneOutlierSkipped(t *testing.T) {
	base := time.Unix(0, 0)
	hs := &hostNodeStat{}
	// Establish a baseline.
	require.False(t, hs.observeRemote(base, 100, 0.25, 90*time.Second))
	require.InDelta(t, 100.0, hs.remote.value, 0.001)
	// A lone outlier (>3× and >+800) is skipped: EWMA and freshness unchanged.
	before := hs.remote.updated
	require.False(t, hs.observeRemote(base.Add(time.Second), 1000, 0.25, 90*time.Second))
	require.InDelta(t, 100.0, hs.remote.value, 0.001) // not absorbed
	require.Equal(t, before, hs.remote.updated)       // stayed at the old timestamp
	require.Equal(t, 1, hs.outlierCount)
}

func TestObserveRemoteTwoOutliersAcceptedSevere(t *testing.T) {
	base := time.Unix(0, 0)
	hs := &hostNodeStat{}
	hs.observeRemote(base, 100, 0.25, 90*time.Second)
	// First outlier skipped.
	require.False(t, hs.observeRemote(base.Add(time.Second), 1000, 0.25, 90*time.Second))
	// Second consecutive outlier: accepted as real, severe degradation. EWMA
	// moves up and freshness advances so the node cannot go stale-and-pinned.
	require.True(t, hs.observeRemote(base.Add(2*time.Second), 1000, 0.25, 90*time.Second))
	require.InDelta(t, 325.0, hs.remote.value, 0.001) // 0.25*1000 + 0.75*100
	require.Equal(t, base.Add(2*time.Second), hs.remote.updated)
	require.Equal(t, 0, hs.outlierCount) // reset after acceptance
}

func TestObserveRemoteOutlierThenNormalResets(t *testing.T) {
	base := time.Unix(0, 0)
	hs := &hostNodeStat{}
	hs.observeRemote(base, 100, 0.25, 90*time.Second)
	hs.observeRemote(base.Add(time.Second), 1000, 0.25, 90*time.Second) // lone outlier → count 1
	require.Equal(t, 1, hs.outlierCount)
	// A normal sample resets the streak so a later single outlier is skipped
	// again rather than treated as the "second in a row".
	require.False(t, hs.observeRemote(base.Add(2*time.Second), 110, 0.25, 90*time.Second))
	require.Equal(t, 0, hs.outlierCount)
}

func TestObserveRemoteModerateJumpNotSevere(t *testing.T) {
	base := time.Unix(0, 0)
	hs := &hostNodeStat{}
	hs.observeRemote(base, 100, 0.25, 90*time.Second)
	// 100→500 is a real slowdown but below the outlier threshold (max(300,900)),
	// so it is absorbed by the EWMA at the normal pace — not a severe reroute.
	require.False(t, hs.observeRemote(base.Add(time.Second), 500, 0.25, 90*time.Second))
	require.InDelta(t, 200.0, hs.remote.value, 0.001) // 0.25*500 + 0.75*100
}

func TestObserveRemoteFirstSampleNeverSevere(t *testing.T) {
	base := time.Unix(0, 0)
	hs := &hostNodeStat{}
	// The very first sample has no prior value to be an outlier of.
	require.False(t, hs.observeRemote(base, 5000, 0.25, 90*time.Second))
	require.InDelta(t, 5000.0, hs.remote.value, 0.001)
}

func TestLocalCongestedLiveWitness(t *testing.T) {
	// An alive-but-slowed node (ping ok, inflation > 200) → congested.
	others := []pingSample{{ok: true, rttMs: 350, localMin: 100}} // inflation 250
	require.True(t, localCongested(others, 200))
	// Same inflation but the witness timed out (ok=false) → NOT congested:
	// a dead node's non-return is not evidence of congestion (blocker fix).
	others = []pingSample{{ok: false, rttMs: 0, localMin: 100}}
	require.False(t, localCongested(others, 200))
	// Alive but not inflated enough → not congestion.
	others = []pingSample{{ok: true, rttMs: 250, localMin: 100}} // inflation 150 < 200
	require.False(t, localCongested(others, 200))
}

func TestLocalCongestedMultiDeath(t *testing.T) {
	// Two nodes both timed out (same-fate death) → no live witness → NOT
	// congested → the caller judges each dead (they can't vouch for each other).
	others := []pingSample{
		{ok: false, localMin: 30},
		{ok: false, localMin: 200},
	}
	require.False(t, localCongested(others, 200))
}

func TestLocalCongestedAsymmetricBaseline(t *testing.T) {
	// +150ms additive bufferbloat: on a 200ms baseline that's only 1.75x (a
	// multiplicative test would miss it), but the absolute increment is 150 <
	// 200 threshold so this alone isn't witness; a 30ms node at +250 is.
	require.False(t, localCongested([]pingSample{{ok: true, rttMs: 350, localMin: 200}}, 200))
	require.True(t, localCongested([]pingSample{{ok: true, rttMs: 300, localMin: 30}}, 200))
}

// A value older than maxAge is no longer evidence — rankable already refuses to
// rank it. Blending a fresh reading against it drags the new estimate toward a
// number nothing else will accept, and leaves it there for several rounds; after
// a suspend or a deferred refresh, the first sample back should be believed.
func TestEWMAReplacesStaleValueInsteadOfBlending(t *testing.T) {
	const maxAge = 90 * time.Second
	base := time.Unix(1000, 0)
	var e ewma
	e.update(base, 500, 0.25, maxAge)

	e.update(base.Add(10*time.Minute), 20, 0.25, maxAge)
	require.InDelta(t, 20.0, e.value, 0.001, "a sample after the gap replaces the stale estimate")

	// Inside the window it still averages, as before.
	e.update(base.Add(10*time.Minute+time.Second), 40, 0.25, maxAge)
	require.InDelta(t, 25.0, e.value, 0.001, "fresh samples still blend")
}

// "3x the previous estimate" says nothing when the previous estimate predates
// the gap. Judged against a stale value, a correct sample after a route change
// was skipped, and the next one then read as a confirmed SECOND outlier — a
// severe verdict that drops the host's sticky, off two good measurements.
func TestObserveRemoteIgnoresStaleBaselineForOutlier(t *testing.T) {
	const maxAge = 90 * time.Second
	base := time.Unix(1000, 0)
	hs := &hostNodeStat{}
	hs.observeRemote(base, 20, 0.25, maxAge)

	late := base.Add(10 * time.Minute)
	require.False(t, hs.observeRemote(late, 900, 0.25, maxAge), "not an outlier, just the first sample after a gap")
	require.InDelta(t, 900.0, hs.remote.value, 0.001, "and it must be recorded, not skipped")
	require.Equal(t, 0, hs.outlierCount)
	require.False(t, hs.observeRemote(late.Add(time.Second), 900, 0.25, maxAge), "so the next one is not a second outlier")
}
