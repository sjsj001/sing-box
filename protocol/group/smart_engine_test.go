package group

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func defaultSelectParams() selectParams {
	return selectParams{toleranceMs: 20, cdnBandMs: 15, switchPct: 15}
}

func fresh(tag string, local, remote float64) nodeCandidate {
	return nodeCandidate{
		tag: tag, localMin: local, remoteEWMA: remote,
		remoteFresh: true, usable: true, cleanPriority: 100,
	}
}

func TestSelectMinTotal(t *testing.T) {
	// req.3: with totals clearly apart, pick the lowest total.
	cands := []nodeCandidate{
		fresh("a", 50, 100), // total 150
		fresh("b", 30, 90),  // total 120  <- winner
		fresh("c", 80, 200), // total 280
	}
	tag, reason, ok := selectNode(cands, false, defaultSelectParams())
	require.True(t, ok)
	require.Equal(t, "b", tag)
	require.Equal(t, reasonTotal, reason)
}

func TestSelectToleranceLowRemote(t *testing.T) {
	// req.2: totals within tolerance (20ms) → prefer the lower-remote node,
	// even though it is not the outright min-total.
	cands := []nodeCandidate{
		fresh("lowtotal", 20, 130),  // total 150 (min), remote 130
		fresh("lowremote", 40, 125), // total 165 (>150 but <=170), remote 125 <- should win
	}
	tag, reason, ok := selectNode(cands, false, defaultSelectParams())
	require.True(t, ok)
	require.Equal(t, "lowremote", tag)
	require.Equal(t, reasonRemote, reason)
}

func TestSelectToleranceOutOfBandKeepsTotal(t *testing.T) {
	// A lower-remote node just outside the tolerance band must not win.
	cands := []nodeCandidate{
		fresh("lowtotal", 20, 130),  // total 150
		fresh("lowremote", 80, 100), // total 180 (>170), remote 100
	}
	tag, reason, ok := selectNode(cands, false, defaultSelectParams())
	require.True(t, ok)
	require.Equal(t, "lowtotal", tag)
	require.Equal(t, reasonTotal, reason)
}

func TestSelectCDNPrefersCleanInBand(t *testing.T) {
	// req.1: on a CDN, within the remote band pick the cleanest node even
	// though its total is higher.
	dirty := fresh("dirty", 10, 20) // total 30, remote 20, clean 100
	clean := fresh("clean", 60, 22) // total 82, remote 22 (within min+15), clean 5
	clean.cleanPriority = 5
	tag, reason, ok := selectNode([]nodeCandidate{dirty, clean}, true, defaultSelectParams())
	require.True(t, ok)
	require.Equal(t, "clean", tag)
	require.Equal(t, reasonCDN, reason)
}

func TestSelectCDNCleanOutOfBandRejected(t *testing.T) {
	// A cleaner node whose remote is outside the CDN band is not eligible;
	// the in-band node wins instead (guards the geo-correctness edge).
	dirty := fresh("dirty", 10, 20) // remote 20
	clean := fresh("clean", 60, 60) // remote 60 (> 20+15), clean 5
	clean.cleanPriority = 5
	tag, _, ok := selectNode([]nodeCandidate{dirty, clean}, true, defaultSelectParams())
	require.True(t, ok)
	require.Equal(t, "dirty", tag)
}

func TestRegionPreferSuppressesBackup(t *testing.T) {
	primary := fresh("hk-primary", 10, 200) // worse total
	primary.region, primary.regionMode, primary.isPrimary = "hk", "prefer", true
	backup := fresh("hk-backup", 10, 50) // better total but must be suppressed
	backup.region, backup.regionMode = "hk", "prefer"
	tag, _, ok := selectNode([]nodeCandidate{primary, backup}, false, defaultSelectParams())
	require.True(t, ok)
	require.Equal(t, "hk-primary", tag)
}

func TestRegionPreferUsesBackupWhenPrimaryDown(t *testing.T) {
	primary := fresh("hk-primary", 10, 200)
	primary.region, primary.regionMode, primary.isPrimary = "hk", "prefer", true
	primary.usable = false // primary not alive → backup promoted
	backup := fresh("hk-backup", 10, 50)
	backup.region, backup.regionMode = "hk", "prefer"
	tag, _, ok := selectNode([]nodeCandidate{primary, backup}, false, defaultSelectParams())
	require.True(t, ok)
	require.Equal(t, "hk-backup", tag)
}

func TestNoFreshCandidatesFallsThrough(t *testing.T) {
	// needs-probe (non-fresh) nodes are excluded from ranking → ok=false so
	// the caller uses single-dial priority.
	a := fresh("a", 10, 20)
	a.remoteFresh = false
	b := fresh("b", 10, 20)
	b.remoteFresh = false
	_, _, ok := selectNode([]nodeCandidate{a, b}, false, defaultSelectParams())
	require.False(t, ok)
}

func TestFilteredCandidatesExcluded(t *testing.T) {
	down := fresh("down", 1, 1)
	down.usable = false
	blocked := fresh("blocked", 1, 1)
	blocked.usable = false
	good := fresh("good", 100, 100)
	tag, _, ok := selectNode([]nodeCandidate{down, blocked, good}, false, defaultSelectParams())
	require.True(t, ok)
	require.Equal(t, "good", tag)
}

func TestMarginMs(t *testing.T) {
	// Below the absolute floor, the tolerance dominates; above, the percentage.
	require.Equal(t, 20.0, marginMs(100, 15, 20)) // 15%·100=15 < 20 → 20
	require.Equal(t, 30.0, marginMs(200, 15, 20)) // 15%·200=30 > 20 → 30
}

func TestChallengerBeatsStickyTotal(t *testing.T) {
	p := defaultSelectParams()
	sticky := fresh("s", 100, 100) // total 200
	// Needs to beat by margin_ms(200)=max(30,20)=30 → below 170.
	weak := fresh("w", 90, 100)   // total 190, not enough
	strong := fresh("x", 60, 100) // total 160, enough
	require.False(t, challengerBeatsSticky(sticky, weak, reasonTotal, 0, p))
	require.True(t, challengerBeatsSticky(sticky, strong, reasonTotal, 0, p))
}

func TestChallengerBeatsStickyCDNCleaner(t *testing.T) {
	p := defaultSelectParams()
	sticky := fresh("s", 10, 20)
	sticky.cleanPriority = 100
	challenger := fresh("c", 60, 25) // within band (bandLimit 35), strictly cleaner
	challenger.cleanPriority = 5
	require.True(t, challengerBeatsSticky(sticky, challenger, reasonCDN, 35, p))
	// Same challenger but out of band → cannot take over.
	require.False(t, challengerBeatsSticky(sticky, challenger, reasonCDN, 24, p))
}

func TestClassifyCDNQuorum(t *testing.T) {
	p := cdnParams{enterMs: 25, exitMs: 35, quorum: 2}
	require.True(t, classifyCDN([]float64{10, 15, 200}, false, p))   // 2 below 25
	require.False(t, classifyCDN([]float64{10, 200, 300}, false, p)) // only 1 below 25
}

func TestClassifyCDNHysteresis(t *testing.T) {
	p := cdnParams{enterMs: 25, exitMs: 35, quorum: 2}
	// 30ms samples: don't enter (>25) but, once CDN, don't leave (<35).
	require.False(t, classifyCDN([]float64{30, 30, 200}, false, p))
	require.True(t, classifyCDN([]float64{30, 30, 200}, true, p))
}

func TestClassifyCDNUniformityRejectsFarSource(t *testing.T) {
	p := cdnParams{enterMs: 25, exitMs: 35, quorum: 2}
	// A far single source: uniform (tight cluster) but absolutely large → not CDN.
	require.False(t, classifyCDN([]float64{190, 200, 210}, false, p))
	// Genuinely small & uniform → CDN via uniformity branch.
	require.True(t, classifyCDN([]float64{40, 42, 44}, false, p))
}

func TestClassifyCDNNoDataKeepsPrev(t *testing.T) {
	p := cdnParams{enterMs: 25, exitMs: 35, quorum: 2}
	require.True(t, classifyCDN(nil, true, p))
	require.False(t, classifyCDN(nil, false, p))
}

func TestAllAliveFresh(t *testing.T) {
	// All alive nodes fresh → true.
	require.True(t, allAliveFresh([]nodeCandidate{fresh("a", 1, 1), fresh("b", 1, 1)}))
	// One alive node not yet measured → false (incomplete).
	partial := fresh("b", 1, 1)
	partial.remoteFresh = false
	require.False(t, allAliveFresh([]nodeCandidate{fresh("a", 1, 1), partial}))
	// A non-fresh node that is unusable doesn't count against completeness.
	down := fresh("c", 1, 1)
	down.remoteFresh = false
	down.usable = false
	require.True(t, allAliveFresh([]nodeCandidate{fresh("a", 1, 1), down}))
	// No alive nodes → false.
	require.False(t, allAliveFresh(nil))
}

func TestClampBias(t *testing.T) {
	require.Equal(t, 0, clampBias(0))
	require.Equal(t, 50, clampBias(50))
	require.Equal(t, -50, clampBias(-50))
	require.Equal(t, 100, clampBias(100))
	require.Equal(t, -100, clampBias(-100))
	require.Equal(t, 100, clampBias(1000))   // over → clamped
	require.Equal(t, -100, clampBias(-1000)) // under → clamped
}

func TestBackoffEscalation(t *testing.T) {
	require.Equal(t, 30*time.Second, backoffDur(1))
	require.Equal(t, 2*time.Minute, backoffDur(2))
	require.Equal(t, 10*time.Minute, backoffDur(3))
	require.Equal(t, 10*time.Minute, backoffDur(9)) // capped
	now := time.Unix(0, 0)
	hs := &hostNodeStat{}
	hs.block(now) // level 1 → 30s
	require.Equal(t, 1, hs.backoff)
	require.Equal(t, now.Add(30*time.Second), hs.blockedUntil)

	// A further failure while the block is still armed is the SAME event still
	// being reported (every open connection to a bad target reports once), so it
	// must not ratchet — otherwise one page load walks the backoff to its
	// 10-minute ceiling.
	hs.block(now.Add(time.Second))
	require.Equal(t, 1, hs.backoff)
	require.Equal(t, now.Add(30*time.Second), hs.blockedUntil)

	// Failing again AFTER the block expired is genuine "failed again once we let
	// it back in" — that escalates.
	later := now.Add(31 * time.Second)
	hs.block(later) // level 2 → 2m
	require.Equal(t, 2, hs.backoff)
	require.Equal(t, later.Add(2*time.Minute), hs.blockedUntil)

	hs.clearBlock() // probe success: lift + decay
	require.True(t, hs.blockedUntil.IsZero())
	require.Equal(t, 1, hs.backoff)
}
