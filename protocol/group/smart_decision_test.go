package group

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testAlpha = 0.7

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

// The numbers below are the ones measured against the real nodes, so the tests
// assert the behaviour that actually ships rather than a convenient fiction.
//
//	hk local 29ms   jp local 80ms   sg local 64ms   de local 182ms
var (
	hk = candidate{tag: "hk", local: ms(29), hasLocal: true}
	jp = candidate{tag: "jp", local: ms(80), hasLocal: true}
	sg = candidate{tag: "sg", local: ms(64), hasLocal: true}
	de = candidate{tag: "de", local: ms(182), hasLocal: true}
)

// chosenGroup drops the reason, which most of these cases do not assert on.
func chosenGroup(alpha float64, candidates []candidate, incumbent string) string {
	chosen, _ := selectGroup(alpha, candidates, incumbent)
	return chosen
}

func withRemote(c candidate, remote time.Duration) candidate {
	c.remote, c.hasRemote = remote, true
	return c
}

func withBonus(c candidate, bonus time.Duration) candidate {
	c.bonus = bonus
	return c
}

// --- scoring: requirements 1, 2 and 3 -------------------------------------

func TestSelectsLowestTotalWhenNothingIsPreferred(t *testing.T) {
	t.Parallel()
	// A Singapore destination: sg is nearest to it and wins on plain merit.
	chosen := chosenGroup(testAlpha, []candidate{
		withRemote(hk, ms(37)),
		withRemote(jp, ms(74)),
		withRemote(sg, ms(2)),
		withRemote(de, ms(182)),
	}, "")
	require.Equal(t, "sg", chosen)
}

func TestPrefersShorterRemoteWhenTotalsMatch(t *testing.T) {
	t.Parallel()
	// Identical totals of 110ms, but A's destination leg is 10ms against B's
	// 90ms. Discounting local is what makes A win, with no tie-break rule.
	a := candidate{tag: "a", local: ms(100), hasLocal: true, remote: ms(10), hasRemote: true}
	b := candidate{tag: "b", local: ms(20), hasLocal: true, remote: ms(90), hasRemote: true}
	require.Equal(t, a.local+a.remote, b.local+b.remote)
	require.Equal(t, "a", chosenGroup(testAlpha, []candidate{a, b}, ""))
}

func TestAlphaOfOneRanksByTotal(t *testing.T) {
	t.Parallel()
	a := candidate{tag: "a", local: ms(100), hasLocal: true, remote: ms(10), hasRemote: true}
	b := candidate{tag: "b", local: ms(20), hasLocal: true, remote: ms(89), hasRemote: true}
	// b has the lower total, and with alpha=1 that is all that counts.
	require.Equal(t, "b", chosenGroup(1.0, []candidate{a, b}, ""))
	// The same pair with the local leg discounted goes the other way.
	require.Equal(t, "a", chosenGroup(testAlpha, []candidate{a, b}, ""))
}

func TestBonusDecidesWhenRemoteIsSmall(t *testing.T) {
	t.Parallel()
	// An anycast destination: every node sees ~1ms, so the configured
	// preference is the only thing left to separate them.
	candidates := []candidate{
		withRemote(hk, ms(1)),
		withRemote(withBonus(jp, ms(40)), ms(1)),
		withRemote(sg, ms(2)),
		withRemote(de, ms(1)),
	}
	require.Equal(t, "jp", chosenGroup(testAlpha, candidates, ""))
}

func TestBonusIsDrownedOutWhenRemoteIsLarge(t *testing.T) {
	t.Parallel()
	// A Hong Kong destination. jp still carries its 40ms preference, but hk is
	// genuinely 48ms closer to the destination, so the preference loses.
	candidates := []candidate{
		withRemote(hk, ms(1)),
		withRemote(withBonus(jp, ms(40)), ms(49)),
		withRemote(sg, ms(33)),
		withRemote(de, ms(199)),
	}
	require.Equal(t, "hk", chosenGroup(testAlpha, candidates, ""))
}

func TestBonusNeverOverridesAFarDestination(t *testing.T) {
	t.Parallel()
	// Even a very large preference must not drag traffic to a node whose
	// destination leg is 180ms worse.
	candidates := []candidate{
		withRemote(sg, ms(2)),
		withRemote(withBonus(de, ms(107)), ms(182)),
	}
	require.Equal(t, "sg", chosenGroup(testAlpha, candidates, ""))
}

// --- stability: requirement 9 ---------------------------------------------

func TestSelectionReportsWhyItChose(t *testing.T) {
	t.Parallel()
	// The reason travels into the log so a surprising route can be explained
	// without reconstructing the measurements by hand.
	incumbent := candidate{tag: "in", local: 0, hasLocal: true, remote: ms(100), hasRemote: true}
	challenger := candidate{tag: "ch", local: 0, hasLocal: true, remote: ms(86), hasRemote: true}

	chosen, reason := selectGroup(testAlpha, []candidate{incumbent, challenger}, "in")
	require.Equal(t, "in", chosen)
	require.Equal(t, reasonSticky, reason, "held in place, not chosen on merit")

	chosen, reason = selectGroup(testAlpha, []candidate{incumbent, challenger}, "")
	require.Equal(t, "ch", chosen)
	require.Equal(t, reasonScore, reason)
}

func TestIncumbentKeepsItsPlaceWithinHysteresis(t *testing.T) {
	t.Parallel()
	// Challenger is 14ms better: not enough.
	incumbent := candidate{tag: "in", local: 0, hasLocal: true, remote: ms(100), hasRemote: true}
	challenger := candidate{tag: "ch", local: 0, hasLocal: true, remote: ms(86), hasRemote: true}
	require.Equal(t, "in", chosenGroup(testAlpha, []candidate{incumbent, challenger}, "in"))
}

func TestIncumbentYieldsBeyondHysteresis(t *testing.T) {
	t.Parallel()
	// Challenger is 16ms better: enough.
	incumbent := candidate{tag: "in", local: 0, hasLocal: true, remote: ms(100), hasRemote: true}
	challenger := candidate{tag: "ch", local: 0, hasLocal: true, remote: ms(84), hasRemote: true}
	require.Equal(t, "ch", chosenGroup(testAlpha, []candidate{incumbent, challenger}, "in"))
}

func TestSelectionIsStableUnderLocalJitter(t *testing.T) {
	t.Parallel()
	// Two groups within a few ms of each other, jittered a thousand times. The
	// choice must never move: a site reached from an alternating exit address
	// is the failure this whole mechanism exists to prevent.
	chosen := ""
	for i := range 1000 {
		jitter := ms(i%11 - 5)
		candidates := []candidate{
			{tag: "a", local: ms(60) + jitter, hasLocal: true, remote: ms(20), hasRemote: true},
			{tag: "b", local: ms(62) - jitter, hasLocal: true, remote: ms(20), hasRemote: true},
		}
		next := chosenGroup(testAlpha, candidates, chosen)
		if chosen == "" {
			chosen = next
			continue
		}
		require.Equal(t, chosen, next, "selection moved on jitter alone at iteration %d", i)
	}
}

func TestUnmeasuredGroupsAreNotSelectable(t *testing.T) {
	t.Parallel()
	// hk has no measurement for this destination; ranking it would be guessing.
	require.Equal(t, "sg", chosenGroup(testAlpha, []candidate{hk, withRemote(sg, ms(2))}, ""))
	require.Equal(t, "", chosenGroup(testAlpha, []candidate{hk, sg}, ""))
}

func TestFallbackRanksByStaticBound(t *testing.T) {
	t.Parallel()
	// Knowing nothing about the destination, the preference still applies:
	// jp's bound is 56-40=16 against hk's 20.3.
	require.Equal(t, "jp", fallbackGroup(testAlpha, []candidate{hk, withBonus(jp, ms(40)), sg, de}))
	require.Equal(t, "hk", fallbackGroup(testAlpha, []candidate{hk, jp, sg, de}))
}

// --- racing: requirements 5 and 8 -----------------------------------------

func TestRacePrunesGroupsThatCannotWin(t *testing.T) {
	t.Parallel()
	// hk answers first on an anycast destination with a score of ~21ms. sg's
	// static bound is 44.8ms and de's is 127.4ms, so neither can catch up even
	// with a zero destination leg — the race stops without waiting for them.
	r := newRace(testAlpha, []candidate{hk, jp, sg, de})
	r.observe("hk", ms(1))
	viable := r.viable()
	require.Len(t, viable, 0, "no group should still be worth waiting for")
	require.True(t, r.settled())
	winner, ok := r.winner()
	require.True(t, ok)
	require.Equal(t, "hk", winner)
}

func TestRaceKeepsWaitingForAPreferredGroup(t *testing.T) {
	t.Parallel()
	// Same first result, but jp now carries a 40ms preference, which drops its
	// bound to 16ms — below hk's 21ms score, so it stays in the race.
	r := newRace(testAlpha, []candidate{hk, withBonus(jp, ms(40)), sg, de})
	r.observe("hk", ms(1))
	viable := r.viable()
	require.Len(t, viable, 1)
	require.Equal(t, "jp", viable[0].tag)
	require.False(t, r.settled())
}

func TestRaceDeadlineMatchesTheDerivedBound(t *testing.T) {
	t.Parallel()
	r := newRace(testAlpha, []candidate{hk, withBonus(jp, ms(40)), sg, de})
	r.observe("hk", ms(1))
	// best + bonus + local*(1-alpha) = 21.3 + 40 + 24 = 85.3ms
	best := score(testAlpha, ms(29), ms(1), 0)
	expected := best + ms(40) + time.Duration(0.3*float64(ms(80)))
	bound, hasBound := r.waitUntil(0)
	require.True(t, hasBound)
	require.Equal(t, expected, bound)
}

func TestRaceAcceptsAPreferredGroupJustInsideTheDeadline(t *testing.T) {
	t.Parallel()
	r := newRace(testAlpha, []candidate{hk, withBonus(jp, ms(40))})
	r.observe("hk", ms(1))
	deadline, _ := r.waitUntil(0)
	// A group answering at the deadline has remote = deadline - local, and by
	// construction that is exactly the point where it ties. One millisecond
	// sooner and it wins.
	r.observe("jp", deadline-ms(80)-time.Millisecond)
	winner, ok := r.winner()
	require.True(t, ok)
	require.Equal(t, "jp", winner)
}

func TestRaceRejectsAPreferredGroupJustOutsideTheDeadline(t *testing.T) {
	t.Parallel()
	r := newRace(testAlpha, []candidate{hk, withBonus(jp, ms(40))})
	r.observe("hk", ms(1))
	deadline, _ := r.waitUntil(0)
	r.observe("jp", deadline-ms(80)+time.Millisecond)
	winner, _ := r.winner()
	require.Equal(t, "hk", winner, "a result past the deadline must not win")
}

func TestRaceGivesBackHandshakeTimeToAColdGroup(t *testing.T) {
	t.Parallel()
	// The failure this prevents, seen on the real nodes: at first contact every
	// pool is cold, and the most distant group pays the largest TLS handshake.
	// Its answer therefore arrives late for a reason that says nothing about its
	// path, and without the allowance it is pruned before it can answer at all.
	cold := sg
	cold.setupAllowance = ms(130)
	warm := newRace(testAlpha, []candidate{hk, sg})
	warm.observe("hk", ms(100))
	withAllowance := newRace(testAlpha, []candidate{hk, cold})
	withAllowance.observe("hk", ms(100))

	warmBound, _ := warm.waitUntil(0)
	allowedBound, _ := withAllowance.waitUntil(0)
	require.Less(t, warmBound, raceHardTimeout, "the case must stay inside the cap")
	require.Equal(t, ms(130), allowedBound-warmBound,
		"the deadline should move out by exactly the handshake already paid")
}

func TestRaceIsBoundedWhenNobodyAnswers(t *testing.T) {
	t.Parallel()
	// Nothing has answered, so there is no bound to chase a better answer
	// against: the driver falls back to the dial budget. Bounding here instead
	// would fail every destination whose connect is slower than the cap.
	r := newRace(testAlpha, []candidate{hk, jp, sg, de})
	require.False(t, r.settled())
	_, hasBound := r.waitUntil(0)
	require.False(t, hasBound, "an unanswered race must not be cut off by the improvement cap")
}

func TestSlowDestinationIsNotCutOffBeforeItAnswers(t *testing.T) {
	t.Parallel()
	// A destination 800ms away is slow, not broken. Until something answers
	// there is nothing to improve on, so nothing may expire.
	r := newRace(testAlpha, []candidate{hk, sg})
	_, hasBound := r.waitUntil(0)
	require.False(t, hasBound)

	// Once one group answers, the cap applies again and the race stops chasing.
	r.observe("hk", ms(800))
	bound, hasBound := r.waitUntil(0)
	require.True(t, hasBound)
	require.LessOrEqual(t, bound, raceHardTimeout)
}

func TestRaceDeadlineIsCappedByTheHardTimeout(t *testing.T) {
	t.Parallel()
	// A slow destination pushes the derived deadline past the cap; the cap wins.
	r := newRace(testAlpha, []candidate{hk, withBonus(de, ms(107))})
	r.observe("hk", ms(400))
	bound, hasBound := r.waitUntil(0)
	require.True(t, hasBound)
	require.Equal(t, raceHardTimeout, bound)
}

func TestASlowFirstAnswerDoesNotSpendTheWholeChaseBudget(t *testing.T) {
	t.Parallel()
	// Measured in production: a destination 1030ms from the nearest group, so the
	// first answer arrived at 1082ms with a score of 1062ms. The cap bounds
	// chasing a *better* answer, but applied from the start of the race it had
	// already elapsed — so every group still in the running was dropped in the
	// same instant the first one answered, including one whose bound was 5.2ms.
	//
	// The destination was then routed through the group that happened to be
	// slowest to it, and the snapshot froze that for hours.
	slow := candidate{tag: "hk", local: 29600 * time.Microsecond, hasLocal: true}
	fast := candidate{tag: "jp", local: 79000 * time.Microsecond, hasLocal: true, bonus: 50 * time.Millisecond}
	r := newRace(testAlpha, []candidate{slow, fast})

	const firstAnswerAt = 1082 * time.Millisecond
	r.observe("hk", 1030*time.Millisecond)
	require.Len(t, r.viable(), 1, "jp can still win: its bound is far below that score")

	bound, hasBound := r.waitUntil(firstAnswerAt)
	require.True(t, hasBound)
	require.Greater(t, bound, firstAnswerAt,
		"a deadline already in the past abandons every group that could still win")
	// jp answers at local+remote; anything under this and it takes the race.
	require.Greater(t, bound, 1100*time.Millisecond)

	// The cap still bites, just measured from the answer rather than the start.
	require.LessOrEqual(t, bound, firstAnswerAt+raceHardTimeout)
}

func TestTheChaseCapIsUnchangedWhenTheFirstAnswerIsPrompt(t *testing.T) {
	t.Parallel()
	// The ordinary case has to be exactly as before: an answer at 32ms leaves the
	// derived deadline well inside the cap, so nothing about it moves.
	r := newRace(testAlpha, []candidate{hk, withBonus(jp, ms(40))})
	r.observe("hk", ms(1))
	fromStart, _ := r.waitUntil(0)
	fromAnswer, _ := r.waitUntil(32 * time.Millisecond)
	require.Equal(t, fromStart, fromAnswer)
	require.Less(t, fromStart, raceHardTimeout)
}

func TestRaceIgnoresFailedGroups(t *testing.T) {
	t.Parallel()
	r := newRace(testAlpha, []candidate{hk, withBonus(jp, ms(40))})
	r.observe("hk", ms(1))
	require.False(t, r.settled())
	r.fail("jp")
	require.True(t, r.settled(), "a failed group must not hold up the race")
	winner, _ := r.winner()
	require.Equal(t, "hk", winner)
}

func TestRaceWithNoSurvivorsHasNoWinner(t *testing.T) {
	t.Parallel()
	r := newRace(testAlpha, []candidate{hk, jp})
	r.fail("hk")
	r.fail("jp")
	require.True(t, r.settled())
	_, ok := r.winner()
	require.False(t, ok)
}

// --- measurement handling --------------------------------------------------

func TestRollingMinRejectsColdOutliers(t *testing.T) {
	t.Parallel()
	// The shape seen on the wire: a cold isolation pool, or a cold hop inside a
	// proxy chain, reports a local several times the true value. It must never
	// become the estimate.
	var window rollingMin
	for _, sample := range []time.Duration{ms(471), ms(452), ms(171), ms(172), ms(170), ms(171)} {
		window.add(sample)
	}
	value, ok := window.value()
	require.True(t, ok)
	require.Equal(t, ms(170), value)
}

func TestRollingMinForgetsBeyondItsWindow(t *testing.T) {
	t.Parallel()
	// A path that genuinely degrades must be followed, not anchored forever to
	// one lucky sample.
	var window rollingMin
	window.add(ms(10))
	for range localWindow {
		window.add(ms(200))
	}
	value, ok := window.value()
	require.True(t, ok)
	require.Equal(t, ms(200), value)
}

func TestRollingMinIsEmptyBeforeAnySample(t *testing.T) {
	t.Parallel()
	var window rollingMin
	_, ok := window.value()
	require.False(t, ok)
}

func TestAnUnmeasuredGroupGetsItsChanceAfterASlowFirstAnswer(t *testing.T) {
	t.Parallel()
	// The cap bounds chasing a *better* answer, so it runs from the answer in
	// hand. Anchored to the start of the race instead, a group with nothing
	// measured — every group in the window before warmup lands — is dropped the
	// instant a first answer arrives later than the cap itself, which is exactly
	// the slow destination where a challenger is most worth waiting for.
	state := newRace(testAlpha, []candidate{
		{tag: "measured", local: ms(10), hasLocal: true},
		{tag: "cold"},
	})
	state.observe("measured", ms(80))

	firstAnswerAt := 600 * time.Millisecond
	bound, hasBound := state.waitUntil(firstAnswerAt)
	require.True(t, hasBound)
	require.Greater(t, bound, firstAnswerAt,
		"a group with nothing measured must still have time left to answer")
	require.Equal(t, firstAnswerAt+raceHardTimeout, bound)
}
