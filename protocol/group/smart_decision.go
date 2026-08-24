package group

import (
	"time"
)

// The decision core of the smart group. Everything here is pure: it takes
// measurements and configuration and returns a choice, with no network, no
// clock of its own and no I/O. The parts that talk to the world live in
// smart.go and feed this.
//
// The model has exactly two measured quantities, because the naive extension
// reports them separately:
//
//	local  — client to proxy, a property of the *node*, near-constant across
//	         destinations, and across a proxy chain it covers every hop up to
//	         the innermost proxy.
//	remote — innermost proxy to destination, a property of the *destination*,
//	         and the thing that collapses to ~1ms for CDN and anycast targets.
//
// and one configured quantity, bonus, which expresses the one thing latency
// cannot measure: whether a node's exit address is a good one to be seen from.

const (
	// raceHardTimeout bounds how long a race keeps chasing a better answer once
	// it has one. It deliberately does not bound establishing the first answer —
	// see race.waitUntil.
	raceHardTimeout = 500 * time.Millisecond

	// raceDialTimeout bounds establishing a connection at all. It is generous
	// because a slow destination is still a working one, and a broken proxy is
	// already caught far sooner by the per-member ready bound.
	raceDialTimeout = 10 * time.Second

	// switchHysteresis is how much better a challenger must be before the
	// selection moves. Without it two near-equal groups trade places on
	// measurement noise, and the same site is reached from a different exit
	// address every few seconds.
	switchHysteresis = 15 * time.Millisecond

	// raceAnswerGrace is how long a race's background half keeps listening for
	// the groups its deadline cut off — see Smart.collectStragglers. It buys
	// verdict completeness, never caller latency: the caller left with its
	// tunnel when the deadline said to.
	raceAnswerGrace = 100 * time.Millisecond

	// localWindow is how many local samples the rolling minimum keeps.
	localWindow = 8
)

// score ranks a group for one destination.
//
//	score = alpha*local + remote - bonus
//
// alpha discounts the client-to-proxy leg, which is what makes a shorter remote
// leg win when totals are close — the continuous form of "break ties by remote",
// chosen over a threshold because a threshold flips back and forth at its
// boundary. bonus is a hand-set credit in milliseconds for a node worth paying
// latency to use; it decides the outcome when remote is small (CDN, anycast) and
// is drowned out when remote is large, which is exactly the intended shape and
// needs no CDN detection anywhere.
func score(alpha float64, local time.Duration, remote time.Duration, bonus time.Duration) time.Duration {
	return time.Duration(alpha*float64(local)) + remote - bonus
}

// staticBound is the best score a group could reach for *any* destination, since
// remote is never negative. It is known before a race starts, from local and
// bonus alone, and lets a race drop a group the moment some other group scores
// below it — no waiting, no measurement.
func staticBound(alpha float64, local time.Duration, bonus time.Duration) time.Duration {
	return time.Duration(alpha*float64(local)) - bonus
}

// candidate is one group as the decision core sees it.
type candidate struct {
	tag string
	// member is which of the group's outbounds these measurements belong to. The
	// decision never reads it — a group is chosen, not a member — but a race
	// decided while a group was failing over is decided against a different node
	// from the one it usually runs, and a trail that does not say which cannot
	// explain the result afterwards.
	member string
	// local is the client-to-proxy leg, and hasLocal distinguishes "measured as
	// zero" from "never measured" — the same distinction hasRemote draws, and
	// for the same reason: a group whose local is unknown must be allowed to
	// race, but must not be *scored* as though it were sitting next to the
	// client.
	local    time.Duration
	hasLocal bool
	// chain is how much of local is the interior of a proxy chain, carried for
	// the trail alone — the decision never reads it, because it is already
	// inside local. It is here because without it a switch record cannot say
	// which leg moved, and "the round trip was seconds while the score was
	// milliseconds" is the shape of failure this model exists to make visible.
	chain    time.Duration
	hasChain bool
	bonus    time.Duration
	// remote is the cached measurement for the destination being decided, and
	// hasRemote distinguishes "measured as zero" from "never measured".
	remote    time.Duration
	hasRemote bool
	// setupAllowance is the connection-establishment cost last seen for this
	// group. A group whose pool has gone cold answers that much later without
	// its path being any slower, so a race must hand the time back before
	// judging it late — otherwise the most distant node, which pays the largest
	// handshake, is disqualified for the one reason that says nothing about it.
	setupAllowance time.Duration
}

func (c candidate) score(alpha float64) time.Duration {
	return score(alpha, c.local, c.remote, c.bonus)
}

func (c candidate) staticBound(alpha float64) time.Duration {
	return staticBound(alpha, c.local, c.bonus)
}

// Why a group was picked, carried into the log so a surprising route can be
// explained without reconstructing the measurements by hand.
const (
	reasonScore     = "score"      // lowest score outright
	reasonSticky    = "sticky"     // challenger was better, but not by enough to move
	reasonFallback  = "fallback"   // nothing measured this destination yet
	reasonRaced     = "raced"      // decided by a race just now
	reasonFailover  = "failover"   // the previous choice would not carry the connection
	reasonRideAlong = "ride-along" // UDP following the decision made for the same host over TCP
)

// selectGroup picks the group for a destination from cached measurements.
//
// incumbent is whichever group is currently selected for this destination; it
// keeps its place unless a challenger beats it by more than switchHysteresis.
// Groups without a measurement for this destination are not selectable — there
// is nothing to rank them by — and an empty result means the caller must race.
func selectGroup(alpha float64, candidates []candidate, incumbent string) (string, string) {
	var (
		best           string
		bestScore      time.Duration
		incumbentScore time.Duration
		hasIncumbent   bool
	)
	for _, candidate := range candidates {
		if !candidate.hasRemote || !candidate.hasLocal {
			continue
		}
		candidateScore := candidate.score(alpha)
		if candidate.tag == incumbent {
			incumbentScore, hasIncumbent = candidateScore, true
		}
		if best == "" || candidateScore < bestScore {
			best, bestScore = candidate.tag, candidateScore
		}
	}
	if hasIncumbent && best != incumbent && bestScore > incumbentScore-switchHysteresis {
		return incumbent, reasonSticky
	}
	return best, reasonScore
}

// fallbackGroup is the choice when no destination measurement exists at all —
// no cache entry, and the race has not produced a result yet. Ranking by static
// bound is the best that can be done without knowing anything about the
// destination, and it is also the order a race should be read in.
func fallbackGroup(alpha float64, candidates []candidate) string {
	var (
		best      string
		bestBound time.Duration
	)
	for _, candidate := range candidates {
		if !candidate.hasLocal {
			// Its static bound would be just -bonus, which would rank a group
			// nothing is known about ahead of one known to be near.
			continue
		}
		bound := candidate.staticBound(alpha)
		if best == "" || bound < bestBound {
			best, bestBound = candidate.tag, bound
		}
	}
	if best == "" && len(candidates) > 0 {
		// Not one group has been measured yet — the first moments after a start,
		// before the warmup round has landed. Configured order is not a ranking,
		// but it beats refusing to dial.
		return candidates[0].tag
	}
	return best
}

// race decides how long to keep waiting on groups that have not answered yet.
//
// It is pure bookkeeping driven by the caller: results and failures go in,
// and it reports whether waiting can stop and who won. All durations are
// relative to the start of the race, so tests drive it with plain numbers.
type race struct {
	alpha      float64
	candidates []candidate
	pending    map[string]bool
	results    map[string]time.Duration
	best       string
	bestScore  time.Duration
	hasBest    bool
}

func newRace(alpha float64, candidates []candidate) *race {
	pending := make(map[string]bool, len(candidates))
	for _, candidate := range candidates {
		pending[candidate.tag] = true
	}
	return &race{
		alpha:      alpha,
		candidates: candidates,
		pending:    pending,
		results:    make(map[string]time.Duration, len(candidates)),
	}
}

// refreshLocal replaces what was known about a group's client-to-proxy leg with
// what this round measured.
//
// Same-round measurements are the comparable ones, and for a member that has
// never carried anything they are the only ones there are — without this, a
// group that entered the race unmeasured would be scored as though it sat next
// to the client and would win every race it was in.
func (r *race) refreshLocal(tag string, local time.Duration) {
	for index := range r.candidates {
		if r.candidates[index].tag == tag {
			r.candidates[index].local = local
			r.candidates[index].hasLocal = true
			return
		}
	}
}

// observe records that a group answered with a destination dial duration.
func (r *race) observe(tag string, remote time.Duration) {
	if !r.pending[tag] {
		return
	}
	delete(r.pending, tag)
	r.results[tag] = remote
	for _, candidate := range r.candidates {
		if candidate.tag != tag {
			continue
		}
		candidateScore := score(r.alpha, candidate.local, remote, candidate.bonus)
		if !r.hasBest || candidateScore < r.bestScore {
			r.best, r.bestScore, r.hasBest = tag, candidateScore, true
		}
		return
	}
}

// fail records that a group will not answer.
func (r *race) fail(tag string) {
	delete(r.pending, tag)
}

// failAllPending gives up on everything still in flight.
func (r *race) failAllPending() {
	clear(r.pending)
}

// viable reports the groups still worth waiting for: still pending, and with a
// static bound good enough that they could still win.
func (r *race) viable() []candidate {
	var viable []candidate
	for _, candidate := range r.candidates {
		if !r.pending[candidate.tag] {
			continue
		}
		if r.hasBest && candidate.hasLocal && candidate.staticBound(r.alpha) >= r.bestScore {
			// Cannot win even with remote=0: no reason to wait for it. A group
			// whose local is unknown has no bound to test, so it is never pruned
			// — pruning it would be pruning on a guess, and the guess would
			// always be that an unmeasured group is the best one.
			continue
		}
		viable = append(viable, candidate)
	}
	return viable
}

// waitUntil reports how long from the start of the race it is worth waiting for
// something better than the answer already in hand, and false when there is no
// answer yet.
//
// The distinction matters more than it looks. The cap exists so a race does not
// sit through every straggler once it has a usable result; applying it before
// any result arrives turns it into a connection timeout, and every destination
// whose connect is slower than the cap would fail outright the first time it was
// used — a slow destination is still a working one.
//
// A group that has not answered by time T has local+remote > T, so with local
// known its remote exceeds T-local and its score exceeds
// alpha*local + (T - local) - bonus. It can therefore still win only while
//
//	T < best + bonus + local*(1-alpha)
//
// which is tight under its model — an answer arriving at local + remote — and
// grows with bonus, so a node declared worth waiting for is waited for exactly
// as much longer as that declaration says. What the model leaves out is that
// an answer actually arrives at local + span, DNS included; the race's
// background half covers that gap, at no cost to the caller waiting here. See
// Smart.collectStragglers.
// firstAnswerAt is when the answer now in hand arrived, measured from the start
// of the race. The cap is applied from there rather than from the start because
// it bounds chasing a *better* answer, and there is nothing to chase until one
// exists: a destination whose first answer takes longer than the cap would
// otherwise arrive to find the whole budget already spent, and every group still
// in the running — including ones about to answer with a far better score —
// dropped in the same instant.
func (r *race) waitUntil(firstAnswerAt time.Duration) (time.Duration, bool) {
	if !r.hasBest {
		return 0, false
	}
	var latest time.Duration
	for _, candidate := range r.viable() {
		if !candidate.hasLocal {
			// The bound is derived from local; without it there is nothing to
			// derive. Waiting the full cap is the honest answer, and it is what
			// gives a member that has never been used its one chance to answer.
			// The cap, like every chase bound, runs from the answer in hand —
			// anchored to the start of the race it goes negative the moment a
			// first answer arrives later than the cap itself, and the one
			// chance evaporates exactly on the slow destinations that need it.
			latest = firstAnswerAt + raceHardTimeout
			break
		}
		deadline := r.bestScore + candidate.bonus +
			time.Duration((1-r.alpha)*float64(candidate.local)) +
			candidate.setupAllowance
		if deadline > latest {
			latest = deadline
		}
	}
	if cap := firstAnswerAt + raceHardTimeout; latest > cap {
		return cap, true
	}
	return latest, true
}

// settled reports whether the race can stop: everyone answered, or everyone left
// has been pruned.
func (r *race) settled() bool {
	return len(r.viable()) == 0
}

func (r *race) winner() (string, bool) {
	return r.best, r.hasBest
}

// rollingMin keeps the minimum of the last n samples.
//
// A minimum rather than an average because the quantity being estimated is what
// the path is capable of: queueing, load and cold TLS handshakes only ever push
// a sample upwards, so the floor is the stable estimate and a single outlier
// cannot drag it. It is also what keeps a cold isolation pool — or a cold hop
// inside a proxy chain, which is not otherwise visible — out of the ranking.
type rollingMin struct {
	samples [localWindow]time.Duration
	next    int
	count   int
}

func (r *rollingMin) add(sample time.Duration) {
	r.samples[r.next] = sample
	r.next = (r.next + 1) % localWindow
	if r.count < localWindow {
		r.count++
	}
}

func (r *rollingMin) value() (time.Duration, bool) {
	if r.count == 0 {
		return 0, false
	}
	best := r.samples[0]
	for i := 1; i < r.count; i++ {
		if r.samples[i] < best {
			best = r.samples[i]
		}
	}
	return best, true
}

// rollingCost keeps what the last n samples cost on average, with the single
// worst one set aside.
//
// A cost estimator, not a capability one — which is the whole reason it cannot
// be the rollingMin sitting above it. That one answers "how good can this path
// be", and for the leg this estimates the answer is a lie: a hop dropping one
// packet in four is, at its best, indistinguishable from a perfect one, so with
// a window of eight the minimum lands on a clean sample 99.998% of the time and
// deepening the window only makes it blinder. What a ranking has to compare is
// what the next hundred connections will pay, and that is a mean.
//
// The median is the wrong middle for the same case: a quarter of samples lost
// leaves three in four untouched, so the middle one never moves.
//
// Exactly one sample is set aside, because exactly one artefact appears at most
// once per window — the relay's own cold session to its exit. A member measured
// to have an interior is probed against a real address every round, which keeps
// that session warm, so a cold one is what a process start or a wake produces
// and nothing else.
//
// The exit's own cold resolver was the other candidate, and it is not one any
// more: an exit reports what its lookup cost and the client puts that on the
// destination leg, so it never reaches this window. Against an exit too old to
// report it, it does — and then it is not once per window either, which is the
// case this trimming cannot cover and does not claim to.
type rollingCost struct {
	samples [localWindow]time.Duration
	next    int
	count   int
}

func (r *rollingCost) add(sample time.Duration) {
	r.samples[r.next] = sample
	r.next = (r.next + 1) % localWindow
	if r.count < localWindow {
		r.count++
	}
}

func (r *rollingCost) value() (time.Duration, bool) {
	if r.count < medianQuorum {
		// Below three readings there is no distribution to trim. remoteWindow
		// reports its newest sample at this point; this one has to report
		// nothing at all, and the difference is where the samples come from.
		//
		// A destination window holds repeated readings of one destination, so
		// its newest is a fair guess at what the next connection meets. This
		// window is fed by whichever destinations happened to be dialed, and
		// its first samples are not a random pair: the proxy answers its own
		// probe address without dialing, so nothing a heartbeat measures ever
		// reaches here. The first thing that writes to this window is a
		// member's first real connection — the one moment the relay's session
		// to its exit and the exit's own resolver are both cold. Reported
		// untrimmed, that single sample lands in local() for the whole group,
		// and a selection it moves is then held by switchHysteresis long after
		// the window itself has recovered.
		//
		// Absent is the honest answer and every reader already has one: local
		// counts an unmeasured interior as none, and relayed keeps whatever it
		// last decided.
		return 0, false
	}
	var sum, worst time.Duration
	for i := 0; i < r.count; i++ {
		sum += r.samples[i]
		if r.samples[i] > worst {
			worst = r.samples[i]
		}
	}
	return (sum - worst) / time.Duration(r.count-1), true
}
