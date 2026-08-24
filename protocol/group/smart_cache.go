package group

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/bbolt"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/experimental/cachefile"
	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing/common"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/contrab/freelru"
	"github.com/sagernet/sing/contrab/maphash"
	"github.com/sagernet/sing/service"
)

// What was decided for each destination, held in memory and written to disk.
//
// Persistence is the point: without it every restart re-races everything, and a
// user watching their traffic leave from a different country after each restart
// has no reason to trust any of it.

const (
	// snapshotTTL is how long a race snapshot stays authoritative. Only the
	// groups that were not selected go stale: the selected one has its
	// destination leg re-reported by every real connection. Refreshing is
	// non-blocking, so a long TTL costs nothing but staleness, and destination
	// infrastructure moves on the order of hours at most.
	snapshotTTL = 6 * time.Hour

	// destinationCacheSize bounds how many destinations are remembered.
	//
	// It wants to sit past whatever snapshotRetention's week produces, because
	// evicting below that throws away decisions that were still going to be
	// used — and the one evicted is the destination not reached recently, which
	// is exactly the one whose next request would otherwise have been answered
	// from the cache rather than by racing every group again. One real client
	// reached six hundred destinations in a day.
	//
	// Four times that costs nothing measurable. The LRU is sharded and its fixed
	// overhead dominates at these sizes — 198KiB either side of the change — and
	// an entry is only built when a destination is actually decided, so what is
	// held is what is used. The hourly state dump is likewise a line per entry
	// that exists, not per slot.
	//
	// Sharding means the usable figure is a few percent under the nominal one, a
	// shard filling before its neighbours: 16384 holds a measured 15843.
	destinationCacheSize = 16384

	// snapshotRetention is when a snapshot stops being kept at all, as opposed
	// to merely being refreshed on next use. An expired snapshot is still a real
	// measurement and is used while a refresh runs behind it, so expiry is not a
	// reason to drop one — but a destination not reached for a week is not
	// coming back, and what is left on disk is then a record of somewhere the
	// user went once rather than anything the routing will consult.
	snapshotRetention = 7 * 24 * time.Hour

	// refreshBackoff is how long a failed refresh holds off the next one. The
	// stale snapshot stays in use meanwhile, which is the right answer: it is a
	// real measurement, and a destination that is not answering right now is not
	// going to be measured better by asking again immediately.
	refreshBackoff = time.Minute

	// snapshotUseLimit is how many requests a snapshot may answer before it is
	// due for re-verification, alongside the time-based lifetime.
	//
	// Time alone leaves a hole the busiest destinations fall into: the groups
	// that were not selected are only re-measured by a race, so however much
	// traffic a snapshot routes, its losing legs stay exactly as old as the
	// round that wrote them. Measured once in production: a location service
	// polling every 26 seconds rode a snapshot whose alternative leg was one
	// outlier sample for almost five hours — 789 requests, each paying ~155ms
	// over the exit that had long since recovered, with nothing due to look at
	// it before the lifetime ran out.
	//
	// The value is an amortisation bound rather than a tuning knob: a refresh
	// costs one dial per group, plus one retake for each group whose exit had to
	// look a hostname up, so verifying every 128 requests keeps the measurement
	// overhead at a few percent of the traffic that consumed the answer (five
	// groups / 128 ≈ 4%, or about 7% for a hostname, where a refresh finds the
	// incumbent's exit warm and every challenger's cold). An idle destination
	// never reaches the
	// limit and keeps the pure time-based lifetime; a busy one converts a
	// fraction of its own traffic into keeping its answer honest.
	snapshotUseLimit = 128

	// remoteWindowDepth is how many race samples a destination leg keeps.
	//
	// Deep enough that the median holds steady against two discordant samples
	// in a row — an anycast destination routinely hands two rounds two
	// different answers — and shallow enough that a genuine change of regime,
	// arriving one sample per round, takes over in three. Replayed against a
	// day of production races, five halved the switch churn of the one
	// afflicted destination while leaving 99.5% of decisions untouched; three
	// let single outliers through, eight dulled regime changes for no further
	// churn reduction.
	remoteWindowDepth = 5

	// destinationFailureCooldown is how long a group stays out of the running
	// for one destination after the proxy reported it unreachable. It is scoped
	// to the destination on purpose: a node that cannot reach one address is
	// usually fine for everything else, and dropping the whole node for it is
	// how a working proxy gets thrown away.
	destinationFailureCooldown = 5 * time.Minute

	// destinationFailureCooldownCap bounds how far the cooldown doubles when a
	// group keeps being told the same destination is unreachable.
	//
	// Some destinations are not coming back for reasons no amount of retrying
	// touches: an exit with no IPv6 route, asked for an IPv6 literal, answers
	// that it cannot reach it every single time. On a flat cooldown that is a
	// full round of doomed dials every five minutes for as long as something
	// keeps asking — measured on one host as seven thousand error lines in a
	// day, which cost more than the dials did: they buried a real outage that
	// happened in the middle of them.
	//
	// Doubling from the base turns a permanent condition into a background
	// hum, while the cap keeps a destination that does recover from waiting
	// long for anyone to notice. The count resets the moment the group reaches
	// the destination again, so a transient failure never accumulates.
	destinationFailureCooldownCap = time.Hour
)

// smartBucket holds one sub-bucket per outbound, and — until the first start
// that clears them — the rows written before they were separated.
//
// Snapshots have to be separated by outbound: a destination key is a digest of
// the host alone, so two smart outbounds in one process file the same
// destination under the same key. Sharing a key space means each start hands one
// of them the other's row — naming groups it cannot score, for a destination it
// then cannot re-race until that row expires. Measured on one host: of 1730
// stored snapshots, 11 belonged to a single-group outbound and had overwritten
// the five-group one's view of those destinations.
//
// The separation is a sub-bucket rather than a bucket per outbound because the
// cache file owns its own root: anything at the root not on cachefile's list is
// deleted at every start, so per-outbound root buckets would be written, wiped
// on the next start, and the persistence they exist for would be silently gone.
// Only this name is on that list, so the separation has to live under it.
var smartBucket = []byte("smart")

// legacySmartKeys lists the rows written before the separation: plain keys
// sitting directly in the bucket above, beside the sub-buckets that replaced
// them.
//
// They are not inherited. Which outbound each belongs to can be narrowed from
// the groups it names, but not settled: an outbound's group set may contain
// another's, so the same row can be claimed by both, and the one configuration
// where any of this matters is exactly the one where two of them were writing
// over each other. Claiming by exact group-set equality would settle it, but
// only by making every reader agree on when the rows may finally be dropped —
// and a first reader that deletes what it adopted takes the second reader's
// rows with it. The cost of dropping them instead is one race per destination,
// once, which is what this package already charges for any snapshot written by
// a version that formatted them differently.
//
// They are dropped rather than left because nothing else would ever remove
// them: the compaction that bounds this file only walks the rows an outbound
// owns.
func legacySmartKeys(bucket *bbolt.Bucket) [][]byte {
	var rows [][]byte
	_ = bucket.ForEach(func(key, value []byte) error {
		if value != nil {
			// A sub-bucket reads as a nil value. Everything else at this level
			// predates them.
			rows = append(rows, append([]byte(nil), key...))
		}
		return nil
	})
	return rows
}

// remoteWindow holds the last few race samples of one group's destination leg,
// oldest first.
//
// It exists because one sample was letting luck route hours of traffic. The
// destination leg of an anycast host genuinely differs between rounds — the
// same proxy dials 0.5ms one race and 150ms the next — and a single stored
// sample froze whichever answer the round happened to catch for the whole
// snapshot lifetime. Selection then rode outliers both ways: a group won on
// one lucky sample and disappointed every connection after, or lost on one
// unlucky sample and sat out hours it would have won.
//
// Windows are treated as immutable: extend copies, nothing writes in place.
// That is what lets carryIncumbent hand a window from one entry to the next
// without the two entries' locks having to know about each other.
type remoteWindow []time.Duration

// medianQuorum is the fewest samples a median is taken from. Below it the
// newest sample stands for the window on its own.
//
// A median needs three readings before it means anything. Two are not a
// middle: the lower of them is just the smaller, so a window of two would
// report whichever round went better and keep reporting it while the newer,
// worse reading sat unused — turning one lucky sample into two rounds of
// authority rather than damping it. Measured over a day of production, most
// legs never get past two samples, so that shallow case is not an edge: it
// was the common one, and it was strictly optimistic.
const medianQuorum = 3

// value reports the leg this window stands for: the median of its samples
// once there are enough of them to have a middle, and the newest sample
// before that.
//
// The median is the point of keeping a window at all — past the quorum, no
// single round's luck decides the value, and a change of regime takes over as
// soon as it holds the majority. Below the quorum the newest reading is the
// honest answer: with one or two samples there is no history to outvote
// anything with, and the most recent measurement is the best estimate of what
// the next connection will meet.
//
// The lower of the two middles on even counts, rather than their mean, keeps
// a value the window actually measured: the mean of [5ms, 286ms] is 145ms of
// compromise neither round saw.
func (w remoteWindow) value() (time.Duration, bool) {
	if len(w) == 0 {
		return 0, false
	}
	if len(w) < medianQuorum {
		return w[len(w)-1], true
	}
	sorted := make([]time.Duration, len(w))
	copy(sorted, w)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[(len(sorted)-1)/2], true
}

// extend returns a new window with the sample appended, keeping the newest
// remoteWindowDepth samples. Always a fresh allocation — see the immutability
// note on the type.
func (w remoteWindow) extend(sample time.Duration) remoteWindow {
	start := 0
	if len(w) >= remoteWindowDepth {
		start = len(w) - remoteWindowDepth + 1
	}
	next := make(remoteWindow, 0, remoteWindowDepth)
	next = append(next, w[start:]...)
	return append(next, sample)
}

// UnmarshalJSON accepts the window's own array form and the single number an
// earlier version stored, read as a window of one sample.
//
// Dropping the old rows would be the designed degradation for a format change
// — but this field is most of what the cache is worth, and a change that
// discards it re-races every destination on upgrade: the restart churn
// persistence exists to prevent, delivered by an update.
func (w *remoteWindow) UnmarshalJSON(data []byte) error {
	var samples []time.Duration
	if err := json.Unmarshal(data, &samples); err == nil {
		*w = samples
		return nil
	}
	var single time.Duration
	if err := json.Unmarshal(data, &single); err != nil {
		return err
	}
	*w = remoteWindow{single}
	return nil
}

// destinationEntry is one race snapshot: what each group's destination leg cost,
// measured in the same round.
//
// Only race measurements go in here. Feeding it the destination legs observed on
// real traffic would look like free data but is a trap: only the selected group
// carries traffic, so only its number would improve — a group that won once with
// a cold resolver cache behind it would keep the credit for warming up while
// every challenger stayed frozen at its cold value, and nothing could ever take
// the selection back. Same-round measurements are the only comparable ones.
type destinationEntry struct {
	// Remote maps group tag to its window of destination dial samples.
	Remote map[string]remoteWindow `json:"remote"`
	// Path maps group tag to this client's leg to the innermost proxy when the
	// snapshot was taken — the near hop plus whatever the chain cost in
	// between. It is kept so the score a group had then can be recomputed and
	// compared against the score it has now, which is what makes a node that
	// has become slow visible rather than only a destination that has moved.
	//
	// The JSON key changed with the meaning, and that is the point. Rows
	// written before this held only the near hop, and reading one as a path
	// would put a relay's stored leg back where the ranking could not see its
	// interior — but worse, legsFor feeds observeScore, which every settled
	// connection consults: an old near hop compared against a live path is a
	// doubling, three of them in a row is a confirmed anomaly, and every
	// destination whose selected group is a relay would be re-raced within
	// minutes of an upgrade. An absent key reads as no measurement, which
	// makes observeScore return before it can conclude anything, and costs one
	// round of carry.
	Path map[string]time.Duration `json:"path,omitempty"`
	// Selected is the group in use, kept so hysteresis survives a restart and
	// the same destination keeps leaving from the same address.
	Selected string `json:"selected,omitempty"`
	// Carried names the groups whose legs came from the previous snapshot
	// rather than from the round that wrote this one. Exactly one group is ever
	// in here — the one in use — and only for one round: see carryIncumbent.
	Carried map[string]bool `json:"carried,omitempty"`
	// Members maps group tag to the member that stood for the group in the round
	// that wrote this snapshot — the losers included, and the groups that never
	// answered most of all.
	//
	// A group is judged against whichever of its nodes was carrying it at the
	// time, so a round run while a group was failed over onto its backup reached
	// a verdict about the backup. The verdict then outlives the failover by
	// hours: the group recovers in minutes, and nothing about a group that did
	// not answer is stored for the drift check to notice. Keeping who was asked
	// makes that detectable — see Smart.racedAgainstOtherMembers.
	Members map[string]string `json:"members,omitempty"`
	// RacedAt is when the snapshot was taken.
	RacedAt time.Time `json:"raced_at"`
	// unreachable maps group tag to when its cooldown for this destination
	// expires. It is deliberately not persisted: a proxy that could not reach
	// something five minutes ago deserves a fresh chance after a restart.
	unreachable map[string]time.Time
	// refusals counts how many cooldowns in a row a group has earned for this
	// destination, which is what the next one is doubled from. Cleared as soon
	// as the group reaches the destination, and not persisted for the same
	// reason unreachable is not.
	refusals map[string]int
	// attempted is when a refresh was last tried, as opposed to RacedAt which is
	// when one last succeeded. A race that finds nothing leaves RacedAt where it
	// was, so without this the entry stays expired and every connection to the
	// destination starts another refresh — each one running its full dial budget
	// against a destination that is not answering.
	attempted time.Time
	// requestsTCP and requestsUDP count what this destination carried since the
	// last usage window. Not persisted: a count that survived a restart would be
	// added to the windows after it and counted twice.
	requestsTCP counter
	requestsUDP counter
	// served counts every request this snapshot has answered, against
	// snapshotUseLimit. Not persisted, and deliberately not carried onto the
	// entry that replaces this one: a fresh round has re-earned its authority.
	// After a restart the count starts over, which at worst delays one early
	// refresh by the limit — the time-based lifetime still stands behind it.
	served atomic.Int64
	// anomalies is how many destination-leg readings in a row have disagreed
	// with what this snapshot recorded. It lives on the entry rather than in a
	// map beside the cache so that it is bounded by the same eviction: a
	// destination that collected a reading or two and was never reached again
	// otherwise keeps its counter for the life of the process, and nothing ever
	// comes back to clear it.
	anomalies int
	// access guards the mutable fields. One entry is shared by every connection
	// to its destination, and those are concurrent by definition — a single page
	// load opens several at once.
	access sync.Mutex
}

func (e *destinationEntry) selected() string {
	e.access.Lock()
	defer e.access.Unlock()
	return e.Selected
}

// selectGroup records the group in use, reporting what it replaced and whether
// that was a change. The previous tag comes back because a move is only worth
// anything to an audit alongside what it moved from.
func (e *destinationEntry) selectGroup(tag string) (previous string, changed bool) {
	e.access.Lock()
	defer e.access.Unlock()
	if e.Selected == tag {
		return tag, false
	}
	previous, e.Selected = e.Selected, tag
	return previous, true
}

// remoteFor reports the cached destination leg for a group — the median of its
// window, not any single round's answer.
func (e *destinationEntry) remoteFor(tag string) (time.Duration, bool) {
	e.access.Lock()
	defer e.access.Unlock()
	return e.Remote[tag].value()
}

// pathFor reports the leg to the innermost proxy a group had when the snapshot was
// taken, for the window after a restart where nothing has been measured live.
func (e *destinationEntry) pathFor(tag string) (time.Duration, bool) {
	e.access.Lock()
	defer e.access.Unlock()
	local, found := e.Path[tag]
	return local, found
}

// legsFor reports both legs a group had when the snapshot was taken.
func (e *destinationEntry) legsFor(tag string) (local time.Duration, remote time.Duration, ok bool) {
	e.access.Lock()
	defer e.access.Unlock()
	remote, hasRemote := e.Remote[tag].value()
	local, hasLocal := e.Path[tag]
	return local, remote, hasRemote && hasLocal
}

// windowFor reads one group's window, for the snapshot write that extends it.
// Nil-tolerant because the first round of a destination has no prior entry.
func (e *destinationEntry) windowFor(tag string) remoteWindow {
	if e == nil {
		return nil
	}
	e.access.Lock()
	defer e.access.Unlock()
	return e.Remote[tag]
}

// retire drops the window's most optimistic sample and records this one as
// the newest reading, for a sample confirmed to be worse than anything held.
// A sample that is not worse than the best reading changes nothing.
//
// Displacing rather than appending is what makes confirmed evidence count at
// every depth. Appending alone could not: the median sits at index (len-1)/2,
// so on an odd-length window a larger sample slides in above the middle and
// leaves it exactly where it was, and below the quorum it is the newest
// sample that speaks, which an append reaches but a plain replacement does
// not. Doing both — out with the best reading, in as the latest — moves the
// value in either regime.
//
// Appending was also unsound in the other direction. Degradation is confirmed
// on the score, which carries both legs, so a group whose *local* leg
// collapsed arrives here with a destination sample smaller than the one on
// record; appended, it became the new minimum and made the group look better,
// the exact opposite of what the confirmation meant. Guarding on the best
// reading held rules that out, and leaves the operation monotone: the window
// loses its smallest sample and gains a larger one, so the value it reports
// can only rise.
//
// What it asserts is narrow and true: several consecutive readings at a
// multiple of the record prove the best sample in this window is no longer
// achievable.
func (w remoteWindow) retire(sample time.Duration) remoteWindow {
	if len(w) == 0 {
		return w
	}
	lowest := 0
	for i, held := range w {
		if held < w[lowest] {
			lowest = i
		}
	}
	if sample <= w[lowest] {
		return w
	}
	next := make(remoteWindow, 0, len(w))
	next = append(next, w[:lowest]...)
	next = append(next, w[lowest+1:]...)
	return append(next, sample)
}

// amendRemote replaces the newest reading in a group's window with one taken
// under conditions the reading it replaces could not offer.
//
// It exists for exactly one of those: the exit's name resolution. A race is the
// first thing to reach a destination through most of these groups, so the group
// carrying the traffic answers with a resolver that is warm for it and every
// challenger answers with one that is not — a difference of tens of
// milliseconds that is a fact about who was used last, not about the path.
// Frozen into a snapshot it becomes a standing handicap that renews itself at
// every refresh, which is the one mechanism meant to correct such things.
//
// Replacing rather than appending, because the two readings are the same
// measurement taken twice and the window has depth for distinct rounds, not for
// retakes. The window itself is immutable — readers hold it — so the amendment
// is a fresh copy, as extend and retire are.
func (e *destinationEntry) amendRemote(tag string, sample time.Duration) {
	e.access.Lock()
	defer e.access.Unlock()
	window, measured := e.Remote[tag]
	if !measured || len(window) == 0 {
		return
	}
	amended := make(remoteWindow, len(window))
	copy(amended, window)
	amended[len(amended)-1] = sample
	e.Remote[tag] = amended
}

// noteDegradation records one confirmed live sample against a group's window.
//
// Live measurements are otherwise kept out of the snapshot on purpose — only
// the selected group has traffic, and crediting it with its own warm numbers
// would entrench it (see the type comment above). A confirmed anomaly is the
// one reading allowed across that line: it is several consecutive samples at
// a multiple of the record, so it may only count against the group, never for
// it — see retire, which is what enforces that. Without it, the refresh the
// anomaly triggers judges the group on a window still innocent of the very
// evidence that forced the round: a race the degraded group sits out keeps
// its old value through the carry, and the switch waits a full round longer
// than the observations justify.
func (e *destinationEntry) noteDegradation(tag string, sample time.Duration) {
	e.access.Lock()
	defer e.access.Unlock()
	window, measured := e.Remote[tag]
	if !measured {
		return
	}
	e.Remote[tag] = window.retire(sample)
}

func (e *destinationEntry) racedAt() time.Time {
	return e.RacedAt
}

func (e *destinationEntry) measured() bool {
	e.access.Lock()
	defer e.access.Unlock()
	return len(e.Remote) > 0
}

func (e *destinationEntry) expired(destination string, now time.Time) bool {
	fresh := now.Sub(e.RacedAt) <= snapshotLifetime(destination)
	if fresh && e.served.Load() < snapshotUseLimit {
		return false
	}
	return e.attemptDue(now)
}

// countAnomaly records one reading that disagreed with this snapshot and
// reports how many have now done so in a row.
func (e *destinationEntry) countAnomaly() int {
	e.access.Lock()
	defer e.access.Unlock()
	e.anomalies++
	return e.anomalies
}

// resetAnomalies clears the run, which one agreeing reading is enough to do.
func (e *destinationEntry) resetAnomalies() {
	e.access.Lock()
	defer e.access.Unlock()
	e.anomalies = 0
}

// racedBy names the member that stood for a group when this snapshot was
// decided, or nothing when the round predates the field or left the group out.
func (e *destinationEntry) racedBy(tag string) string {
	e.access.Lock()
	defer e.access.Unlock()
	return e.Members[tag]
}

// racedAgainstOthers reports the groups now carried by a different member from
// the one that stood for them when this snapshot was decided.
func (e *destinationEntry) racedAgainstOthers(candidates []candidate) []string {
	e.access.Lock()
	defer e.access.Unlock()
	var drifted []string
	for _, c := range candidates {
		if raced, known := e.Members[c.tag]; known && raced != c.member {
			drifted = append(drifted, raced)
		}
	}
	return drifted
}

// attemptDue reports whether enough time has passed since the last round to be
// worth running another. It guards both a refresh of a stale snapshot and a
// first measurement that has never succeeded — the second is the one that
// matters, because a destination with no snapshot at all otherwise starts a
// round on every single connection to it.
func (e *destinationEntry) attemptDue(now time.Time) bool {
	e.access.Lock()
	defer e.access.Unlock()
	return now.Sub(e.attempted) > refreshBackoff
}

// noteAttempt records that a refresh ran, whether or not it produced anything.
func (e *destinationEntry) noteAttempt(now time.Time) {
	e.access.Lock()
	e.attempted = now
	e.access.Unlock()
}

// snapshotLifetime spreads the nominal lifetime by a quarter either way.
//
// Destinations tend to be first seen in bursts — one page load reaches a dozen
// hosts within a second — and a fixed lifetime makes that whole burst expire
// together, so six hours later they all race at the same moment. The spread is
// derived from the destination rather than drawn at random so it survives a
// restart: a destination that was due to refresh late still refreshes late.
func snapshotLifetime(destination string) time.Duration {
	hasher := fnv.New64a()
	hasher.Write([]byte(destination))
	spread := time.Duration(hasher.Sum64() % uint64(snapshotTTL/2))
	return snapshotTTL*3/4 + spread
}

func (e *destinationEntry) blocked(tag string, now time.Time) bool {
	e.access.Lock()
	defer e.access.Unlock()
	until, found := e.unreachable[tag]
	return found && now.Before(until)
}

func (e *destinationEntry) block(tag string, now time.Time) {
	e.access.Lock()
	defer e.access.Unlock()
	if e.unreachable == nil {
		e.unreachable = make(map[string]time.Time)
	}
	if e.refusals == nil {
		e.refusals = make(map[string]int)
	}
	e.refusals[tag]++
	e.unreachable[tag] = now.Add(cooldownAfter(e.refusals[tag]))
}

// reached clears what a group earned for refusing this destination, because it
// has just carried it. Called for the groups a round measured, which is the
// only evidence that the refusal is over.
func (e *destinationEntry) reached(tag string) {
	e.access.Lock()
	defer e.access.Unlock()
	delete(e.refusals, tag)
	delete(e.unreachable, tag)
}

// cooldownAfter reports how long the nth consecutive refusal keeps a group off
// a destination: the base doubled n-1 times, capped.
func cooldownAfter(refusals int) time.Duration {
	cooldown := destinationFailureCooldown
	for range refusals - 1 {
		cooldown *= 2
		if cooldown >= destinationFailureCooldownCap {
			return destinationFailureCooldownCap
		}
	}
	return cooldown
}

// carryIncumbent keeps the group in use in the snapshot when the round that
// produced this one did not re-measure it.
//
// A race stores only what answered, so a round the incumbent sat out leaves it
// with no destination leg — and a group with no leg cannot be scored, so
// hysteresis has nothing to hold and the selection moves unconditionally. On a
// destination whose reachability varies per exit, every round is then won by
// whichever group happened to answer, and the same host is reached from a
// different address each time. That is the exact failure grouping exists to
// prevent, and it was measured: one host cycling through all five exits in
// twenty minutes.
//
// Three things keep the stale value from outstaying its welcome. Only the
// incumbent is carried, so nothing else is ranked on old numbers. It is not
// carried if it failed this round, because that is evidence rather than
// silence. And it is carried for one round only — a group that stays quiet
// through two loses its place, which is what stops a value surviving
// indefinitely on a destination nobody can reach.
func (e *destinationEntry) carryIncumbent(previous *destinationEntry, tag string, failure error) {
	if previous == nil || tag == "" {
		return
	}
	if errors.Is(failure, naive.ErrDestinationUnreachable) {
		// The proxy answered, and what it said was that it could not reach this
		// destination. That is a measurement of a sort, and carrying the old one
		// over the top of it would hide the only real news in the round.
		//
		// Every other failure is about the leg to the proxy — a pooled
		// connection that died, a reset, a refused dial — and says nothing
		// whatever about this destination. Retiring the group's leg for one of
		// those costs it its eligibility for the whole snapshot lifetime, since
		// a group with no measurement cannot be selected: seen on
		// lh3.googleusercontent.com, where one "connection closed" during a
		// refresh moved a CDN scoring -8ms onto a group scoring 22ms and left
		// it there for six hours.
		return
	}
	if _, measured := e.Remote[tag]; measured {
		return
	}
	previous.access.Lock()
	defer previous.access.Unlock()
	if previous.Carried[tag] {
		// Already living on a carried value; a second silent round retires it.
		return
	}
	// The whole window moves, not one sample: silence says nothing about the
	// destination, so the incumbent keeps exactly the history it had. Safe to
	// share — windows are immutable, see remoteWindow.
	remote, hasRemote := previous.Remote[tag]
	local, hasLocal := previous.Path[tag]
	if !hasRemote || len(remote) == 0 || !hasLocal {
		return
	}
	if e.Remote == nil {
		e.Remote = make(map[string]remoteWindow, 1)
	}
	if e.Path == nil {
		e.Path = make(map[string]time.Duration, 1)
	}
	e.Remote[tag], e.Path[tag] = remote, local
	e.Carried = map[string]bool{tag: true}
}

// carried reports whether a group's legs in this snapshot were measured or
// inherited, so an audit does not read one as the other.
func (e *destinationEntry) carried(tag string) bool {
	e.access.Lock()
	defer e.access.Unlock()
	return e.Carried[tag]
}

// carryCooldowns copies the live cooldowns onto the snapshot that is about to
// replace this one.
//
// The copy is deep, and taken under this entry's lock. Handing the map itself
// over would leave two entries — each with its own mutex — writing the same map,
// which is not a data race that stays quiet: Go aborts the process with
// "concurrent map writes". Losing a cooldown recorded in the instant between the
// copy and the swap costs one retry against a destination the group just failed,
// which is the cheap side of the trade.
func (e *destinationEntry) carryCooldowns(previous *destinationEntry) {
	if previous == nil {
		return
	}
	previous.access.Lock()
	defer previous.access.Unlock()
	if len(previous.unreachable) == 0 && len(previous.refusals) == 0 {
		return
	}
	e.unreachable = make(map[string]time.Time, len(previous.unreachable))
	for tag, until := range previous.unreachable {
		e.unreachable[tag] = until
	}
	// The run of refusals moves with the cooldown it sets the length of.
	// Leaving it behind would reset every escalation on the next round, which
	// for a destination nothing can reach is every five minutes for ever.
	e.refusals = make(map[string]int, len(previous.refusals))
	for tag, count := range previous.refusals {
		e.refusals[tag] = count
	}
}

// count records that this destination carried one request.
func (e *destinationEntry) count(network string) {
	if e == nil {
		return
	}
	e.served.Add(1)
	if network == N.NetworkUDP {
		e.requestsUDP.total.Add(1)
	} else {
		e.requestsTCP.total.Add(1)
	}
}

// carryCounts moves what the previous entry has not yet reported onto the entry
// replacing it.
//
// A refresh builds a new entry rather than editing the old one, so without this
// a destination's count would restart every time it was re-raced — and the
// busiest destinations are re-raced the most, which would make the counts
// smallest exactly where they matter.
//
// It takes the outstanding amount with delta rather than copying both halves
// across, because copying is not atomic: a window ending between reading the
// total and reading the reported figure hands the new entry a gap it will
// report a second time. delta is the one operation that claims the outstanding
// count and marks it claimed together, so whichever of the two runs first, the
// other sees nothing left. What is lost instead is anything counted against the
// old entry between here and the store below, which is a handful of requests on
// a diagnostic — the cheap side of the trade.
func (e *destinationEntry) carryCounts(previous *destinationEntry) {
	if previous == nil {
		return
	}
	e.requestsTCP.total.Store(previous.requestsTCP.delta())
	e.requestsUDP.total.Store(previous.requestsUDP.delta())
}

// marshal renders the entry for storage under its own lock, so a flush cannot
// read fields a dial is midway through changing.
func (e *destinationEntry) marshal() ([]byte, error) {
	e.access.Lock()
	defer e.access.Unlock()
	type stored destinationEntry
	return json.Marshal((*stored)(e))
}

// smartCache holds the race snapshots, in memory and on disk.
//
// Persistence is what makes a decision survive a restart. Without it every
// restart re-races every destination, and a user watching their traffic leave
// from a different country after each restart has no reason to trust any of it.
type smartCache struct {
	access  sync.Mutex
	entries *freelru.Cache[string, *destinationEntry]
	// The cache file service is resolved on first use, not at construction:
	// outbounds are built before it is registered. Only the service is cached —
	// its database handle is read through it on every use, because the handle
	// is not stable: it is nil until the service starts, and the service
	// replaces it wholesale when it recovers from a corrupt file. A handle
	// captured once would either latch that nil forever or keep writing into a
	// database that recovery has closed.
	ctx       context.Context
	resolve   sync.Once
	cacheFile *cachefile.CacheFile
	// owner is this outbound's sub-bucket under smartBucket — what makes a
	// snapshot belong to one outbound rather than to the process.
	owner []byte
	// dirty tracks the destinations changed since the last flush, so a flush
	// writes what moved rather than walking the whole cache.
	dirty map[string]bool
}

func newSmartCache(ctx context.Context, tag string) *smartCache {
	return &smartCache{
		entries: common.Must1(freelru.New[string, *destinationEntry](
			destinationCacheSize, maphash.NewHasher[string]().Hash32, true)),
		owner: []byte(tag),
		dirty: make(map[string]bool),
		ctx:   ctx,
	}
}

// rows is this outbound's snapshots, or nil when none have been written yet.
func (c *smartCache) ownBucket(tx *bbolt.Tx) *bbolt.Bucket {
	bucket := tx.Bucket(smartBucket)
	if bucket == nil {
		return nil
	}
	return bucket.Bucket(c.owner)
}

func (c *smartCache) db() *bbolt.DB {
	if !c.persistent() {
		return nil
	}
	return c.cacheFile.DB
}

// persistent reports whether a cache file service exists at all. It answers a
// different question from db(): the service exists from construction, its
// database only from its start — and an entry changed in the gap between the
// two still wants its dirty mark, so the flush that runs after the database
// appears writes it.
func (c *smartCache) persistent() bool {
	c.resolve.Do(func() {
		c.cacheFile, _ = service.FromContext[adapter.CacheFile](c.ctx).(*cachefile.CacheFile)
	})
	return c.cacheFile != nil
}

// load reads a snapshot. Everything is in memory: the whole bucket is read once
// at startup, so a destination being used for the first time never waits on a
// disk read — and never holds every other decision behind one.
func (c *smartCache) load(destination string) *destinationEntry {
	c.access.Lock()
	defer c.access.Unlock()
	entry, _ := c.entries.Get(destination)
	return entry
}

// keys lists what is remembered, for a caller that wants to walk the whole
// table rather than look one destination up.
func (c *smartCache) keys() []string {
	c.access.Lock()
	defer c.access.Unlock()
	return c.entries.Keys()
}

func (c *smartCache) store(destination string, entry *destinationEntry) {
	persistent := c.persistent()
	c.access.Lock()
	defer c.access.Unlock()
	c.entries.Add(destination, entry)
	if persistent {
		c.dirty[destination] = true
	}
}

// touch marks an entry changed in place, for updates that do not replace it.
//
// Both markers check for a cache file first: without one nothing ever flushes,
// so a mark is never cleared, and the dirty set grows by one key per
// destination ever visited for the life of the process — the same unbounded
// growth the anomaly counter was once rebuilt to avoid.
func (c *smartCache) touch(destination string) {
	if !c.persistent() {
		return
	}
	c.access.Lock()
	defer c.access.Unlock()
	c.dirty[destination] = true
}

// preload reads the whole bucket into memory and compacts it.
//
// Nothing is deleted as decisions are made, so without this the file would grow
// for every destination ever visited and be read back in full at every start.
// Keeping the newest and dropping the rest bounds both.
func (c *smartCache) preload() {
	db := c.db()
	if db == nil {
		return
	}
	c.tidy(db)
	now := time.Now()
	stored := make(map[string]*destinationEntry)
	var abandoned []string
	_ = db.View(func(tx *bbolt.Tx) error {
		bucket := c.ownBucket(tx)
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(key, value []byte) error {
			var decoded destinationEntry
			if json.Unmarshal(value, &decoded) != nil {
				// Written by another version, or truncated. Dropping it costs
				// one race; interpreting it costs a wrong route.
				abandoned = append(abandoned, string(key))
				return nil
			}
			if now.Sub(decoded.RacedAt) > snapshotRetention {
				abandoned = append(abandoned, string(key))
				return nil
			}
			stored[string(key)] = &decoded
			return nil
		})
	})

	dropped := surplusEntries(stored, destinationCacheSize)
	c.access.Lock()
	for destination, entry := range stored {
		if dropped[destination] {
			continue
		}
		if _, live := c.entries.Get(destination); live {
			// The listeners accept before this runs, so a destination can be
			// raced while the bucket is still being read. The live entry is the
			// fresher one — the disk copy predates the very flush that wrote
			// it — and overwriting it here would resurrect last run's decision
			// over one made seconds ago, then flush the stale copy back over
			// the fresh row under the entry's still-standing dirty mark.
			continue
		}
		c.entries.Add(destination, entry)
	}
	for _, destination := range abandoned {
		if dropped == nil {
			dropped = make(map[string]bool, len(abandoned))
		}
		dropped[destination] = true
	}
	// The rows to delete were judged from the pre-scan view of the bucket. A
	// destination decided while the scan ran has a fresher row that view never
	// saw, and its dirty mark was cleared by its own flush — deleting on the
	// stale judgement would silently discard it until the next touch.
	for destination := range dropped {
		if c.dirty[destination] {
			delete(dropped, destination)
			continue
		}
		if _, live := c.entries.Get(destination); live {
			delete(dropped, destination)
		}
	}
	c.access.Unlock()

	if len(dropped) == 0 {
		return
	}
	_ = db.Batch(func(tx *bbolt.Tx) error {
		bucket := c.ownBucket(tx)
		if bucket == nil {
			return nil
		}
		for destination := range dropped {
			if err := bucket.Delete([]byte(destination)); err != nil {
				return err
			}
		}
		return nil
	})
}

// tidy clears what no outbound will read again: the rows written before
// snapshots were separated, and the sub-buckets of outbounds that no longer
// exist.
//
// Failures are dropped: a cache file that cannot be tidied must never stop the
// snapshots that can be read from being read, and the next start tries again.
func (c *smartCache) tidy(db *bbolt.DB) {
	horizon := time.Now().Add(-snapshotRetention)
	_ = db.Batch(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(smartBucket)
		if bucket == nil {
			return nil
		}
		// Both lists are taken before anything is deleted: bbolt does not
		// promise a cursor survives a write under it.
		legacy := legacySmartKeys(bucket)
		abandoned := abandonedOwners(bucket, c.owner, horizon)
		for _, key := range legacy {
			if err := bucket.Delete(key); err != nil {
				return err
			}
		}
		for _, owner := range abandoned {
			if err := bucket.DeleteBucket(owner); err != nil {
				return err
			}
		}
		return nil
	})
}

// abandonedOwners names the sub-buckets holding nothing any outbound would
// still act on.
//
// An outbound that was renamed or removed leaves its sub-bucket behind, and no
// other outbound's compaction reaches it — the same "nothing else ever will"
// that makes the pre-separation rows worth dropping.
//
// Age settles it rather than the list of configured outbounds, which this could
// reach for: a sub-bucket whose newest snapshot is past snapshotRetention holds
// only what that constant already calls a record of somewhere the user went
// once, so dropping it takes nothing preload would have kept. Asking who exists
// would name orphans sooner, at the price of a rule whose blast radius is the
// whole file if the answer is ever incomplete — during a reload, or for a
// caller that built this without the manager. Age cannot be wrong in that
// direction.
//
// For the same reason it does not distinguish a renamed outbound from a live
// one that has been idle a week: both are dropped, by whichever outbound tidies
// first, and both lose only rows that preload discards on sight.
func abandonedOwners(bucket *bbolt.Bucket, own []byte, horizon time.Time) [][]byte {
	var abandoned [][]byte
	_ = bucket.ForEachBucket(func(key []byte) error {
		if bytes.Equal(key, own) {
			return nil
		}
		if owned := bucket.Bucket(key); owned == nil || !allOlderThan(owned, horizon) {
			return nil
		}
		abandoned = append(abandoned, append([]byte(nil), key...))
		return nil
	})
	return abandoned
}

// allOlderThan reports whether every snapshot in one sub-bucket predates the
// horizon, stopping at the first that does not — which on a live outbound is
// almost always the first row read.
func allOlderThan(bucket *bbolt.Bucket, horizon time.Time) bool {
	older := true
	_ = bucket.ForEach(func(_, value []byte) error {
		if !older {
			return nil
		}
		var decoded destinationEntry
		if json.Unmarshal(value, &decoded) != nil {
			// Unreadable says nothing about when it was written, so it cannot
			// be the evidence that condemns the sub-bucket around it.
			older = false
			return nil
		}
		if decoded.RacedAt.After(horizon) {
			older = false
		}
		return nil
	})
	return older
}

// surplusEntries picks which destinations to forget when more were stored than
// are kept, dropping the least recently raced first.
func surplusEntries(stored map[string]*destinationEntry, limit int) map[string]bool {
	if len(stored) <= limit {
		return nil
	}
	destinations := make([]string, 0, len(stored))
	for destination := range stored {
		destinations = append(destinations, destination)
	}
	sort.Slice(destinations, func(i, j int) bool {
		return stored[destinations[i]].RacedAt.After(stored[destinations[j]].RacedAt)
	})
	dropped := make(map[string]bool, len(destinations)-limit)
	for _, destination := range destinations[limit:] {
		dropped[destination] = true
	}
	return dropped
}

// flush writes the destinations that changed since the last call.
func (c *smartCache) flush() error {
	db := c.db()
	if db == nil {
		return nil
	}
	c.access.Lock()
	pending := make(map[string]*destinationEntry, len(c.dirty))
	for destination := range c.dirty {
		if entry, found := c.entries.Get(destination); found {
			pending[destination] = entry
		}
	}
	c.dirty = make(map[string]bool)
	c.access.Unlock()

	if len(pending) == 0 {
		return nil
	}
	err := db.Batch(func(tx *bbolt.Tx) error {
		parent, createErr := tx.CreateBucketIfNotExists(smartBucket)
		if createErr != nil {
			return createErr
		}
		bucket, createErr := parent.CreateBucketIfNotExists(c.owner)
		if createErr != nil {
			return createErr
		}
		for destination, entry := range pending {
			encoded, marshalErr := entry.marshal()
			if marshalErr != nil {
				continue
			}
			if putErr := bucket.Put([]byte(destination), encoded); putErr != nil {
				return putErr
			}
		}
		return nil
	})
	if err != nil {
		// The marks were cleared before the write on the assumption it would
		// land. Putting them back is what stops a transient failure — a database
		// closing under a shutdown, a full disk — from silently discarding every
		// decision made since the last successful flush.
		c.access.Lock()
		for destination := range pending {
			c.dirty[destination] = true
		}
		c.access.Unlock()
	}
	return err
}
