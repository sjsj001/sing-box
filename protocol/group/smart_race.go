package group

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// Measuring a destination nobody has measured yet: one round of probes across
// every group, read in the order the decision core says is worth waiting for.
//
// A probe carries no payload, so the destination sees a connection and nothing
// more, and the winner's tunnel is the one the caller gets — there is no second
// dial.

type probeOutcome struct {
	tag         string
	conn        net.Conn
	measurement naive.ConnMeasurement
	// leg is this client to the innermost proxy: the near hop this round
	// measured, plus what the member's interior has been costing. Computed here
	// because only the prober holds the member.
	leg    time.Duration
	hasLeg bool
	// member is the node this outcome was measured through. Carried out of the
	// probe because a group hands out whichever member it would use now, and
	// after a failover that is a different node from the one that raced.
	member *smartMember
	err    error
}

// probe opens a tunnel through one group and waits for the proxy's answer.
//
// The tunnel carries no payload, so the destination sees a connection and
// nothing else — which is what makes it safe to open several at once. The
// winner's tunnel is already established, so the payload goes onto it directly
// with no second dial.
//
// The send is unconditional and the channel is sized to hold exactly one
// outcome per probe, so delivery never blocks and never depends on anyone still
// listening. That is what makes the count in race exact, and the count is what
// closes the tunnels of probes that answered after the race had walked away.
func (s *Smart) probe(ctx context.Context, key string, group *smartGroup, destination M.Socksaddr, results chan<- probeOutcome) {
	outcome := probeOutcome{tag: group.tag}
	defer func() { results <- outcome }()

	member := group.current()
	if member == nil {
		outcome.err = E.New("no usable member")
		return
	}
	outcome.member = member
	conn, err := member.outbound.DialContext(ctx, N.NetworkTCP, destination)
	if err != nil {
		outcome.err = err
		s.reportFailure(key, destination, group, member, err)
		return
	}
	outcome.conn = conn

	measured, isMeasured := common.Cast[naive.MeasuredConn](conn)
	if !isMeasured {
		outcome.err = E.New("outbound ", member.tag, " does not report measurements")
		return
	}
	if err = s.awaitReady(ctx, member, conn); err != nil {
		outcome.err = err
		s.reportFailure(key, destination, group, member, err)
		return
	}
	measurement, err := measured.Measure(ctx)
	if err != nil {
		outcome.err = err
		s.reportFailure(key, destination, group, member, err)
		return
	}
	s.record(member, measurement)
	if !measurement.HasRemote {
		// Nothing measured the destination leg, so this group cannot be ranked
		// for it. Dropping out is the honest outcome: a missing measurement read
		// as zero would win every race it entered.
		outcome.err = E.New("member ", member.tag, " reported no destination timing")
		return
	}
	outcome.measurement = measurement
	if measurement.HasSpan {
		// The near hop from this round, the interior from the member's window.
		// The two are taken differently on purpose: refreshLocal exists to keep
		// a race comparing same-round measurements, and what it guards against
		// — a cold isolation pool distorting one probe — is on the client's
		// side of the path, which is the near hop. The interior is a property
		// of the member, and a race is the first thing to reach a destination,
		// which is exactly when the relay's own session to its exit is most
		// likely to be cold. Judging a member on that one cold handshake, in
		// the round that decides where this destination lives for hours, is the
		// mistake the window was built to avoid.
		chain, measured := member.chain()
		if !measured {
			// Nothing banked yet, and a race is the wrong round to call that
			// zero: a member's first crossings are exactly when it has no
			// window, and reading an unmeasured interior as none lets a relay
			// enter as though it were a single hop and win a destination it
			// cannot serve. This round's own crossing is the conservative
			// answer — it is what this connection actually paid — and it no
			// longer carries the exit's name resolution, which is the reason
			// a live sample was too noisy to use before.
			chain, _ = measurement.ChainSpan()
		}
		outcome.leg, outcome.hasLeg = measurement.NearHop()+chain, true
	}
}

// errLegTimedOut marks the derived establishment bound firing, as opposed to
// the caller losing interest. It stays a context.DeadlineExceeded — it is one,
// and code above should be able to read it as one — so the only thing telling
// the two apart is the sentinel. See Smart.reportFailure, where exactly one of
// them is a verdict about the member.
var errLegTimedOut = fmt.Errorf("connection to the proxy did not come up in time (%w)", context.DeadlineExceeded)

// awaitReady waits for the connection to the proxy to come up, bounded by what
// that leg is known to cost.
//
// The bound is derived rather than fixed because the thing being guarded against
// is a blackholed route, which otherwise fails only when TCP gives up — long
// after every new connection has queued behind it. It covers only establishment:
// how long the proxy then takes to answer depends on the destination, and
// bounding that by the same number would call every distant destination a broken
// proxy.
func (s *Smart) awaitReady(ctx context.Context, member *smartMember, conn net.Conn) error {
	ready, isReady := common.Cast[naive.MeasuredConn](conn)
	if !isReady {
		return nil
	}
	budget, derived := member.readyTimeout()
	if !derived {
		// Wrapped like every other failure of this leg. Leaving it bare is not
		// harmless: on a relay the error travels through RemoteAck into
		// statusForError, and an unclassified error must not be at risk of
		// reading as a verdict about the destination — 502 is consumed
		// downstream as a replay licence.
		if err := ready.WaitReady(ctx); err != nil {
			return fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, err)
		}
		return nil
	}
	bounded, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	err := ready.WaitReady(bounded)
	if err == nil {
		return nil
	}
	if ctx.Err() == nil && bounded.Err() != nil {
		// The bound this outbound set, not one the caller imposed.
		err = errLegTimedOut
	}
	return fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, err)
}

// raceReport renders what every group brought to a race: the two legs it
// measured, the credit it was given, the bound it was judged against, and — for
// the ones that dropped out — why.
//
// It exists because the interesting failures are not visible in the outcome. A
// group that is pruned, or that reports a timing the client will not believe,
// leaves nothing in the log but an absence, and working out which of the two
// happened means recomputing the scores by hand from a dozen scattered lines.
//
// A Stringer rather than a formatted string: the logger checks the level before
// formatting, so above debug this costs one interface value and nothing else.
type raceReport struct {
	alpha      float64
	candidates []candidate
	remote     map[string]time.Duration
	// chain is the interior that went into each group's local, and near is what
	// was left. Both are here because a relay whose next hop had gone bad was
	// invisible in this line — the leg it degraded is in neither local nor
	// remote, and reconstructing it meant subtracting three logged numbers by
	// hand.
	//
	// The interior is the member's window rather than this round's sample, which
	// is what the ranking used: printing the live one instead left a row whose
	// own numbers did not add up, since local minus a live interior is not the
	// near hop.
	chain     map[string]time.Duration
	near      map[string]time.Duration
	failures  map[string]error
	winner    string
	hasWinner bool
	elapsed   time.Duration
}

func (r raceReport) String() string {
	var out strings.Builder
	if r.hasWinner {
		out.WriteString("won=" + r.winner)
	} else {
		out.WriteString("won=none")
	}
	out.WriteString(" in=" + short(r.elapsed))
	for _, c := range r.candidates {
		out.WriteString(" | " + c.tag)
		if c.hasLocal {
			out.WriteString(" local=" + short(c.local))
		} else {
			out.WriteString(" local=?")
		}
		if hop, measured := r.near[c.tag]; measured {
			out.WriteString(" near=" + short(hop))
		}
		if c.bonus > 0 {
			out.WriteString(" bonus=" + short(c.bonus))
		}
		if chain, reported := r.chain[c.tag]; reported && chain > 0 {
			out.WriteString(" chain=" + short(chain))
		}
		if remote, answered := r.remote[c.tag]; answered {
			out.WriteString(" remote=" + short(remote))
			out.WriteString(" score=" + short(score(r.alpha, c.local, remote, c.bonus)))
			continue
		}
		if c.hasLocal {
			// What it was judged against: the best score any destination could
			// give it. A bound above the winner's score is why it was dropped
			// without being waited for.
			out.WriteString(" bound=" + short(c.staticBound(r.alpha)))
		}
		if err, failed := r.failures[c.tag]; failed {
			out.WriteString(" failed=" + err.Error())
		} else {
			out.WriteString(" unanswered")
		}
	}
	return out.String()
}

// short trims a duration to something a log line can carry.
func short(d time.Duration) string {
	if d > time.Millisecond || d < -time.Millisecond {
		return d.Round(100 * time.Microsecond).String()
	}
	return d.Round(time.Microsecond).String()
}

// race measures every group against a destination and returns the winner's
// already-open tunnel.
//
// The caller is not made to wait for the whole story. It gets the best tunnel
// on the schedule the deadline math sets; groups still unanswered then are
// collected for a further grace in the background, and the snapshot is written
// from whoever answered at all — see collectStragglers for why the two
// schedules have to be different. run, when not nil, is the single-flight
// claim, released once the last write has landed.
func (s *Smart) race(ctx context.Context, key string, destination M.Socksaddr, run *raceRun) (string, net.Conn, error) {
	now := time.Now()
	entry := s.cache.load(key)
	// The selection this destination entered the race with. Both snapshot
	// writes anchor to it: anchoring the second to the first would let a
	// provisional winner pose as an incumbent it never was.
	anchor := incumbentOf(entry)
	// The two lists are built together and stay in step. A candidate with no
	// probe behind it would sit in the race's pending set with nothing that can
	// ever resolve it, and the race would wait out its whole budget for an
	// answer that was never going to come.
	var (
		candidates []candidate
		racing     []*smartGroup
	)
	for _, c := range s.candidates(entry, now) {
		group := s.group(c.tag)
		if group == nil {
			continue
		}
		candidates = append(candidates, c)
		racing = append(racing, group)
	}
	if len(racing) == 0 {
		s.settleRace(key, run)
		return "", nil, errNoUsableGroup(destination)
	}

	// Derived from the outbound's lifetime, not the caller's: a probe outlives
	// the caller by design — an answer arriving after the caller settled, or
	// even after it hung up, is still the measurement the next six hours route
	// on. The caller's departure is watched separately below.
	raceCtx, cancel := context.WithTimeout(s.ctx, raceDialTimeout)

	results := make(chan probeOutcome, len(racing))
	for _, group := range racing {
		go s.probe(raceCtx, key, group, destination, results)
	}
	// Every probe delivers exactly one outcome, so what the loop below does not
	// read is exactly what a drainer has to close.
	answered := 0

	started := time.Now()
	state := newRace(s.alpha, candidates)
	conns := make(map[string]net.Conn, len(candidates))
	remote := make(map[string]time.Duration, len(candidates))
	chain := make(map[string]time.Duration, len(candidates))
	near := make(map[string]time.Duration, len(candidates))
	local := make(map[string]time.Duration, len(candidates))
	legs := make(map[string]racedLeg, len(candidates))

	// Starts at the dial budget and is shortened only once something has
	// answered, so a race is never cut off before it has anything to show.
	timer := time.NewTimer(raceDialTimeout)
	defer timer.Stop()

	failures := make(map[string]error, len(candidates))
	// When the answer now in hand arrived. The cap on chasing a better one runs
	// from here, not from the start of the race.
	var firstAnswerAt time.Duration

	// handOff gives the round to its background half. All that differs between
	// the branches that let go is how many probes are still out and how long to
	// keep listening for them.
	handOff := func(pending int, grace time.Duration) {
		go s.collectStragglers(raceStragglers{
			key:         key,
			destination: destination,
			run:         run,
			cancel:      cancel,
			results:     results,
			pending:     pending,
			state:       state,
			remote:      remote,
			chain:       chain,
			near:        near,
			local:       local,
			legs:        legs,
			failures:    failures,
			anchor:      anchor,
			started:     started,
			entry:       entry,
			grace:       grace,
		})
	}

	for !state.settled() {
		select {
		case outcome := <-results:
			answered++
			if outcome.err != nil {
				failures[outcome.tag] = outcome.err
				state.fail(outcome.tag)
				if outcome.conn != nil {
					outcome.conn.Close()
				}
				break
			}
			conns[outcome.tag] = outcome.conn
			legs[outcome.tag] = racedLeg{member: outcome.member, resolve: outcome.measurement.Resolve}
			remote[outcome.tag] = outcome.measurement.RemoteDial
			if outcome.hasLeg {
				local[outcome.tag] = outcome.leg
				near[outcome.tag] = outcome.measurement.NearHop()
				chain[outcome.tag] = outcome.leg - outcome.measurement.NearHop()
				state.refreshLocal(outcome.tag, outcome.leg)
			}
			state.observe(outcome.tag, outcome.measurement.RemoteDial)
			if firstAnswerAt == 0 && state.hasBest {
				firstAnswerAt = time.Since(started)
			}
		case <-timer.C:
			// Either the chase for something better ran out, or nothing answered
			// within the dial budget. Whatever is in hand is the answer.
			state.failAllPending()
		case <-raceCtx.Done():
			state.failAllPending()
		case <-ctx.Done():
			if !state.hasBest && answered < len(racing) {
				// The caller is gone and nothing has answered yet. The probes
				// are left to finish on their own budget — what they measure
				// still decides the next six hours — so the round is handed to
				// the background half whole. The pending set still names every
				// probe in flight, and the grace is what is left of the dial
				// budget, which the probes cannot outlive: when raceCtx expires
				// they deliver failures and the collector settles early.
				// Cancelling here instead used to discard the whole round and
				// stamp a failed-race backoff on a destination that was
				// answering, because one caller hung up first.
				s.logger.DebugContext(ctx, "race ", destination, " abandoned by caller; collecting in background")
				if run != nil {
					run.decide()
				}
				handOff(len(racing)-answered, raceDialTimeout-time.Since(started))
				return "", nil, ctx.Err()
			}
			// An answer is in hand: the caller leaves with it, and the groups
			// still unanswered go through the same grace as any other race.
			state.failAllPending()
		}
		if state.settled() {
			break
		}
		bound, hasBound := state.waitUntil(firstAnswerAt)
		if !hasBound {
			// Nothing has answered yet; keep waiting on the dial budget.
			continue
		}
		remaining := bound - time.Since(started)
		if remaining <= 0 {
			state.failAllPending()
			break
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(remaining)
	}

	winner, hasWinner := state.winner()
	// Close everything that did not win. Their measurements are already banked,
	// and the destination only ever carried a connection attempt from them.
	for tag, conn := range conns {
		if tag != winner || !hasWinner {
			conn.Close()
		}
	}

	pending := len(racing) - answered
	if hasWinner && pending > 0 {
		// Someone has not answered, and the deadline math has already said
		// waiting longer cannot help this caller. It can still help the
		// verdict — see collectStragglers — so the caller leaves now with the
		// best tunnel there is, and the snapshot is written when the grace runs
		// out. The maps handed to the collector stay live; the provisional
		// snapshot gets copies, because an entry shares its maps with every
		// reader it has.
		provisional := raceReport{
			alpha:      s.alpha,
			candidates: state.candidates,
			remote:     cloneDurations(remote),
			chain:      cloneDurations(chain),
			near:       cloneDurations(near),
			failures:   failures,
			winner:     winner,
			hasWinner:  true,
			elapsed:    time.Since(started),
		}
		s.logger.DebugContext(ctx, "race ", destination, " (", pending, " still answering) ", provisional)
		// The loop above forced itself settled by clearing the pending set, and
		// observe refuses a tag it is not waiting for. Put back exactly the
		// ones that never delivered an outcome, so their answers still count.
		for _, c := range state.candidates {
			if _, answeredTag := remote[c.tag]; answeredTag {
				continue
			}
			if _, failed := failures[c.tag]; failed {
				continue
			}
			state.pending[c.tag] = true
		}
		s.storeSnapshot(key, destination, snapshotBasis{replacing: entry, carryFrom: entry}, provisional, cloneDurations(local), anchor)
		if run != nil {
			run.decide()
		}
		handOff(pending, raceAnswerGrace)
		return winner, conns[winner], nil
	}

	report := raceReport{
		alpha:      s.alpha,
		candidates: state.candidates,
		remote:     remote,
		chain:      chain,
		near:       near,
		failures:   failures,
		winner:     winner,
		hasWinner:  hasWinner,
		elapsed:    time.Since(started),
	}
	s.logger.DebugContext(ctx, "race ", destination, " ", report)
	s.auditRace(key, destination.String(), report)
	// The probes this race walked away from still answer — cancelling only makes
	// them answer sooner, it does not stop the ones that already have a tunnel
	// open. Nobody is left reading, so their connections have to be collected
	// here or they leak, one per abandoned probe, against a destination that is
	// about to be used a lot.
	//
	// It has to outlive this function: the whole point is that those probes have
	// not finished yet. The context is cancelled here, which is what bounds how
	// long the drainer waits.
	cancel()
	if pending > 0 {
		go drainProbes(results, pending)
	}
	if len(remote) > 0 {
		s.storeSnapshot(key, destination, snapshotBasis{replacing: entry, carryFrom: entry}, report, local, anchor)
		go s.amendColdResolutions(key, destination, s.cache.load(key), legs)
	} else {
		// Nothing to store, but the attempt still counts. A destination that has
		// never produced a measurement has no snapshot at all, so without a
		// record of having tried, every single connection to it starts another
		// round — one full set of probes each, against something that is not
		// answering. Seen in production: twenty-one rounds for one IPv6-only
		// host, none of which could ever have succeeded.
		s.noteFailedRace(key, entry)
	}
	s.settleRace(key, run)
	if !hasWinner {
		return "", nil, E.New("no group could reach ", destination)
	}
	return winner, conns[winner], nil
}

func cloneDurations(m map[string]time.Duration) map[string]time.Duration {
	cloned := make(map[string]time.Duration, len(m))
	for k, v := range m {
		cloned[k] = v
	}
	return cloned
}

// raceStragglers is everything a race hands to its background half.
type raceStragglers struct {
	key         string
	destination M.Socksaddr
	run         *raceRun
	cancel      context.CancelFunc
	results     chan probeOutcome
	pending     int
	state       *race
	remote      map[string]time.Duration
	chain       map[string]time.Duration
	near        map[string]time.Duration
	local       map[string]time.Duration
	legs        map[string]racedLeg
	failures    map[string]error
	anchor      string
	started     time.Time
	// entry is the snapshot from before the round started. The final write
	// judges the carry against it — the provisional written in between carries
	// this round's own mark, not a previous round's.
	entry *destinationEntry
	// grace is how long to keep listening for the groups that had not answered
	// when the caller left.
	grace time.Duration
}

// collectStragglers waits out the grace for the groups that had not answered
// when the caller left, then writes the snapshot from everyone who answered at
// all.
//
// This is the half that makes the deadline honest. The deadline models an
// answer as arriving at local + remote and is tight under that model; a real
// answer arrives at local + span, and the difference — the proxy's own DNS,
// resolved before it dials and charged to nobody — measured one to three
// milliseconds warm and around fifty cold. Cold is the first time anyone asks
// for a destination, which is exactly when races run, so the tight deadline
// cut off precisely the strong challenger it was derived to wait for. Seen
// twice in one night's trail: a group bounded at -10ms cut 0.4ms and 16ms
// before its answer arrived, parking an apple.com destination on a group
// scoring 246ms for six hours, ineligible even to be reconsidered because a
// group that never answers leaves no measurement.
//
// Waiting here costs the caller nothing — it already has its tunnel — so the
// grace can be generous where the deadline could not. If the full story names
// a different winner, the snapshot says so and the destination's next
// connection uses it: the same route-now, correct-later shape as every other
// background refresh.
func (s *Smart) collectStragglers(c raceStragglers) {
	grace := time.NewTimer(c.grace)
	defer grace.Stop()
collecting:
	for c.pending > 0 {
		select {
		case outcome := <-c.results:
			c.pending--
			if outcome.err != nil {
				c.failures[outcome.tag] = outcome.err
				c.state.fail(outcome.tag)
				if outcome.conn != nil {
					outcome.conn.Close()
				}
				continue
			}
			// The measurement is what was wanted; the winner has long been
			// handed out, so the tunnel itself is surplus.
			outcome.conn.Close()
			c.legs[outcome.tag] = racedLeg{member: outcome.member, resolve: outcome.measurement.Resolve}
			c.remote[outcome.tag] = outcome.measurement.RemoteDial
			if outcome.hasLeg {
				c.local[outcome.tag] = outcome.leg
				c.near[outcome.tag] = outcome.measurement.NearHop()
				c.chain[outcome.tag] = outcome.leg - outcome.measurement.NearHop()
				c.state.refreshLocal(outcome.tag, outcome.leg)
			}
			c.state.observe(outcome.tag, outcome.measurement.RemoteDial)
		case <-grace.C:
			break collecting
		}
	}
	c.cancel()
	if c.pending > 0 {
		go drainProbes(c.results, c.pending)
	}
	winner, hasWinner := c.state.winner()
	report := raceReport{
		alpha:      s.alpha,
		candidates: c.state.candidates,
		remote:     c.remote,
		chain:      c.chain,
		near:       c.near,
		failures:   c.failures,
		winner:     winner,
		hasWinner:  hasWinner,
		elapsed:    time.Since(c.started),
	}
	s.logger.Debug("race ", c.destination, " settled ", report)
	s.auditRace(c.key, c.destination.String(), report)
	if len(c.remote) > 0 {
		// What this write replaces is whatever the cache holds now — the
		// provisional snapshot, when this round wrote one — so the counts and
		// cooldowns accrued while the stragglers were answering carry forward.
		// The carry is still judged against c.entry, the snapshot from before
		// the round; see snapshotBasis for why the two must stay apart.
		s.storeSnapshot(c.key, c.destination, snapshotBasis{replacing: s.cache.load(c.key), carryFrom: c.entry}, report, c.local, c.anchor)
		go s.amendColdResolutions(c.key, c.destination, s.cache.load(c.key), c.legs)
	} else {
		// A round the caller abandoned before anything answered, and nothing
		// answered in the grace either. The stale snapshot, if any, stays; the
		// attempt still counts, so the next connection waits out the backoff
		// rather than starting a round per connection.
		s.noteFailedRace(c.key, s.cache.load(c.key))
	}
	s.settleRace(c.key, c.run)
}

// drainProbes closes the connections of the probes that answered too late to be
// read by the race that started them.
func drainProbes(results <-chan probeOutcome, pending int) {
	for range pending {
		if outcome := <-results; outcome.conn != nil {
			outcome.conn.Close()
		}
	}
}

// raceRun is one round of probes in flight, so that everything else asking for
// the same destination can wait for it instead of starting its own.
//
// It settles in two steps. decided closes when there is a snapshot worth
// reading — the provisional one, when a race keeps collecting in the
// background — and is what a waiting connection needs. done closes when the
// round is over entirely, and is what keeps a second round from starting while
// the first is still writing.
type raceRun struct {
	done       chan struct{}
	decided    chan struct{}
	decideOnce sync.Once
	// release gives back the slot this round was admitted on, and is called
	// when the round ends rather than when the caller leaves. The two are not
	// the same moment: a round hands its tail to the background half, and the
	// probes it is still collecting are dials against the destination — the
	// very thing the bound exists to count. Released on the caller's return
	// instead, a client that opens connections to new destinations and cancels
	// them keeps every slot free while the tails pile up unbounded.
	release func()
}

func (r *raceRun) decide() {
	r.decideOnce.Do(func() { close(r.decided) })
}

// claimRace registers a round of probes for a destination, reporting the run
// already in flight when there is one.
func (s *Smart) claimRace(key string) (*raceRun, bool) {
	run := &raceRun{done: make(chan struct{}), decided: make(chan struct{})}
	existing, running := s.races.LoadOrStore(key, run)
	if running {
		return existing.(*raceRun), false
	}
	return run, true
}

func (s *Smart) releaseRace(key string, run *raceRun) {
	run.decide()
	s.races.Delete(key)
	if run.release != nil {
		run.release()
	}
	close(run.done)
}

// settleRace releases the claim a race was run under, tolerating the runs
// tests start without one.
func (s *Smart) settleRace(key string, run *raceRun) {
	if run == nil {
		return
	}
	s.releaseRace(key, run)
}

// raceOrWait is the first-contact path: one round of probes per destination, and
// everything else that wants the same destination waits for it.
//
// Without the waiting half, a page load opening eight connections to a host
// nobody has visited starts eight rounds, and each round touches the
// destination once per group. What the destination sees is then dozens of
// connections from every exit address at once, torn down again immediately —
// which is the pattern grouping exists to avoid, arriving at the one moment the
// group has not been chosen yet.
func (s *Smart) raceOrWait(ctx context.Context, key string, destination M.Socksaddr) (net.Conn, error) {
	run, leading := s.claimRace(key)
	if leading {
		select {
		case s.raceBound() <- struct{}{}:
			run.release = func() { <-s.raceSlots }
		default:
			// Too many destinations racing already — the storm case; see
			// raceConcurrency. Handing this caller the static-ranking dial keeps
			// its latency flat, and the destination races on a later connection
			// instead of inside the storm.
			s.releaseRace(key, run)
			s.logger.DebugContext(ctx, "deferring race for ", destination, ": too many in flight")
			return s.dialWithoutRacing(ctx, key, destination)
		}
		winner, conn, err := s.race(ctx, key, destination, run)
		if err != nil {
			return nil, err
		}
		// The winner's tunnel goes straight to the caller, so a raced request is
		// counted here — dialWithFailover never sees it.
		if group := s.group(winner); group != nil {
			s.countRequest(key, group, group.current(), reasonRaced, N.NetworkTCP)
		}
		s.logger.InfoContext(ctx, destination, " via ", winner, " by=", reasonRaced)
		return conn, nil
	}

	select {
	case <-run.decided:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// The round has written a snapshot — final or provisional — so the normal
	// cached path applies. It may still have found nothing: every group failed,
	// or the leader's caller walked away mid-round. Falling through to a plain
	// dial is then better than starting the round again and multiplying the
	// probes after all.
	return s.dialWithoutRacing(ctx, key, destination)
}

// dialWithoutRacing picks a group from whatever is already known and dials it.
//
// It is the path for a destination that must not be raced right now: one a
// round has just finished with, and one a round has recently failed on. Both
// would otherwise start another round per connection.
func (s *Smart) dialWithoutRacing(ctx context.Context, key string, destination M.Socksaddr) (net.Conn, error) {
	entry := s.cache.load(key)
	candidates := s.candidates(entry, time.Now())
	if len(candidates) == 0 {
		return nil, errNoUsableGroup(destination)
	}
	var selected, reason string
	if entry != nil {
		selected, reason = selectGroup(s.alpha, candidates, entry.selected())
	}
	if selected == "" {
		selected, reason = fallbackGroup(s.alpha, candidates), reasonFallback
	}
	if selected == "" {
		return nil, errNoUsableGroup(destination)
	}
	return s.dialWithFailover(ctx, destination, key, selected, reason)
}

// raceDetached refreshes a stale snapshot without anyone waiting on it.
func (s *Smart) raceDetached(key string, destination M.Socksaddr) {
	run, leading := s.claimRace(key)
	if !leading {
		// Already being measured; another round would only duplicate the probes.
		return
	}
	select {
	case s.raceBound() <- struct{}{}:
		run.release = func() { <-s.raceSlots }
	default:
		// Nobody is waiting on a refresh, so under pressure it is the first
		// thing to give way. The snapshot stays; whatever wanted it refreshed
		// will want that again.
		s.releaseRace(key, run)
		return
	}
	_, conn, err := s.race(s.ctx, key, destination, run)
	if err != nil {
		s.logger.Debug("refresh race for ", destination, ": ", err)
		return
	}
	conn.Close()
}

// raceBound hands out the shared race bound, creating it if this outbound was
// assembled without NewSmart — same reasoning as Smart.slots.
func (s *Smart) raceBound() chan struct{} {
	s.raceSlotsOnce.Do(func() {
		if s.raceSlots == nil {
			s.raceSlots = make(chan struct{}, raceConcurrency)
		}
	})
	return s.raceSlots
}

// noteFailedRace remembers that a round produced nothing, so the next
// connection waits rather than starting another one.
//
// An entry with no measurements is not usable for routing — dialTCP still falls
// through to a race — but it is enough to carry the backoff, and it costs one
// slot in a cache that is bounded anyway.
func (s *Smart) noteFailedRace(key string, entry *destinationEntry) {
	if entry == nil {
		entry = &destinationEntry{}
		s.cache.store(key, entry)
	}
	entry.noteAttempt(time.Now())
}

// snapshotBasis names the two prior snapshots one write builds on. replacing is
// what the new snapshot displaces in the cache — the cooldowns and unreported
// counts move over from it. carryFrom is the snapshot of the round *before*
// this one, which is what the carry decision must be judged against: a race
// that writes twice — provisional, then final — passes the provisional as
// replacing, and judging the carry against it too would read this round's own
// carry mark as a previous silent round and retire the incumbent's leg after
// one round of silence instead of two. The fields only differ between those
// two writes; everywhere else one entry fills both.
type snapshotBasis struct {
	replacing *destinationEntry
	carryFrom *destinationEntry
}

// storeSnapshot writes one round's verdict over what basis.replacing holds.
func (s *Smart) storeSnapshot(key string, destination M.Socksaddr, basis snapshotBasis, report raceReport, local map[string]time.Duration, incumbent string) {
	selected := report.winner
	if incumbent != "" {
		// Keep the hysteresis anchored across refreshes. The anchor is the
		// selection from before the round started, passed in rather than read
		// from the cache: by the time a race's background half writes, the
		// cache holds the provisional snapshot, and a provisional winner is
		// not an incumbent.
		selected = incumbent
	}
	members := make(map[string]string, len(report.candidates))
	for _, c := range report.candidates {
		if c.member != "" {
			members[c.tag] = c.member
		}
	}
	// Each answering group's window is the round before's window plus this
	// round's sample. The base is carryFrom, never replacing, for the same
	// reason the carry judgement uses it: a race writes twice — provisional,
	// then final — and extending the provisional would count the early
	// answerers' samples twice in one round. Anchored to carryFrom, the final
	// write recomputes the same extension and lands the round's sample exactly
	// once. Groups that did not answer this round are dropped, not carried
	// forward wholesale: a window kept through silence would let a group stay
	// eligible for hours on history alone, which is the cold-credit trap the
	// entry's type comment describes — the one deliberate exception remains
	// carryIncumbent, below.
	remote := make(map[string]remoteWindow, len(report.remote))
	for tag, sample := range report.remote {
		remote[tag] = basis.carryFrom.windowFor(tag).extend(sample)
	}
	updated := &destinationEntry{
		Remote:   remote,
		Path:     local,
		Selected: selected,
		Members:  members,
		RacedAt:  time.Now(),
	}
	updated.carryIncumbent(basis.carryFrom, selected, report.failures[selected])
	updated.carryCooldowns(basis.replacing)
	updated.carryCounts(basis.replacing)
	// Answering with a destination leg is the proof that whatever the group was
	// refused for is over, so the cooldown carried above is lifted for exactly
	// the groups this round measured — and the run of refusals behind it with
	// it, or a destination that came back would serve its longest escalation
	// before anyone noticed.
	for tag := range report.remote {
		updated.reached(tag)
	}
	s.cache.store(key, updated)
	// A race is rare and its result is the whole point of running one. Waiting
	// for the next timer tick to persist it means an early restart throws the
	// decision away and races everything again.
	if err := s.cache.flush(); err != nil {
		s.logger.Debug("persist snapshot for ", destination, ": ", err)
	}
}

// observeScore watches how the group in use is actually performing.
//
// The measurement never feeds the ranking — same-round measurements are the only
// comparable ones — but the snapshot is frozen between races, so without this
// nothing that changes afterwards is visible until the lifetime runs out.
//
// It compares scores rather than only the destination leg, because the two ways
// a decision goes stale need catching equally. A destination moving shows up in
// the destination leg; a node becoming slow shows up in the client leg, and that
// case is worse than it sounds: the groups a race pruned hold no snapshot entry,
// so they are not even eligible to replace the incumbent, and the traffic would
// sit on a node it has outgrown until the snapshot expired.
//
// The destination is threaded through rather than rebuilt from the key: the key
// deliberately drops the port, so parsing it back yields port 0 and the refresh
// this triggers would dial somewhere that cannot answer.
// The member is passed in rather than asked of the group. What arrives here is
// one connection's testimony, and it has to be weighed against the windows of
// the member that carried it — which is not necessarily the one the group would
// hand out now. A heartbeat condemning that member on another goroutine makes
// current() fail over mid-settle, and the comparison would then read one
// member's live interior against a sibling's window: attribution reverses, and
// the evidence is filed against whichever leg did not move. Asking also had a
// side effect, current() being an election rather than a getter.
func (s *Smart) observeScore(key string, destination M.Socksaddr, group *smartGroup, member *smartMember, measurement naive.ConnMeasurement) {
	entry := s.cache.load(key)
	if entry == nil {
		return
	}
	recordedLocal, recordedRemote, ok := entry.legsFor(group.tag)
	if !ok {
		return
	}
	heldChain, _ := member.chain()
	// This connection on its own, not the windows. The windows are what the
	// ranking uses and they are deliberately steady; this is the check that has
	// to see a leg the windows can miss. An interior that drops packets without
	// its base latency changing leaves most samples where they were, so a
	// trimmed mean needs two bad ones in a window before it moves — while every
	// affected connection pays in full, one at a time, which is what arrives
	// here. The streak below is what keeps that from being twitchy.
	liveChain, hasLiveChain := measurement.ChainSpan()
	if !hasLiveChain {
		liveChain = heldChain
	}
	if _, hasNear := member.near(); !hasNear {
		return
	}
	if measurement.HasResolve && measurement.Resolve > warmResolverBias {
		// This connection paid for a lookup, and what it is being compared
		// against deliberately did not: a destination reading is retaken once
		// the exit has the name, so the record stands for the warm case. Read
		// as drift, a destination sparse enough for its exit's cache to lapse
		// between connections would re-race itself every few connections.
		//
		// No reset either, because this is not evidence that nothing moved. A
		// destination whose lookups lapse regularly would otherwise have every
		// streak broken by one of them and could never be found to have moved
		// at all.
		return
	}
	// Compared without the bonus. The bonus is a standing credit, identical in
	// both terms, and subtracting it from each can drive the pair negative or
	// through zero — at which point the ratio below stops meaning anything and a
	// group with a large enough bonus is never checked for drift again.
	was := score(s.alpha, recordedLocal, recordedRemote, 0)
	now := score(s.alpha, measurement.NearHop()+liveChain, measurement.RemoteDial, 0)
	if was <= 0 || now < was*anomalyFactor {
		entry.resetAnomalies()
		return
	}
	if entry.countAnomaly() < anomalyStreak {
		return
	}
	entry.resetAnomalies()
	// Where the confirmed evidence is written down depends on which leg moved,
	// and the split is clean for a reason worth stating: the interior is
	// span minus connect, so this client's own uplink congesting raises the
	// round trip and the near hop together and leaves the interior untouched.
	// There is no path by which our network going bad retires every member's
	// chain — that falls out of measuring the three legs separately.
	//
	// A multiple alone cannot carry the split, which is why there is a floor
	// beside it. A member that dials destinations itself has an interior of
	// half a millisecond at the middle and 1.3 at the ninetieth percentile —
	// the ratio between two ordinary samples of the same healthy member is
	// already past anomalyFactor, so on that scale the multiple fires on noise
	// and hands a real destination move to the wrong leg. Requiring the
	// interior to have moved by more than switchHysteresis as well says the
	// same thing the rest of the file says with that constant: below it,
	// nothing about this leg is large enough to change a decision, so it cannot
	// be what changed this one.
	interiorMoved := hasLiveChain && heldChain > 0 &&
		liveChain > heldChain*anomalyFactor && liveChain-heldChain > switchHysteresis
	if !interiorMoved {
		// The destination leg is what moved, so that is where the evidence is
		// written: the one live reading allowed to touch the snapshot, and the
		// refresh depends on it, because a degraded group is exactly the one
		// likely to sit that refresh out.
		//
		// When the interior is what moved, nothing is written here and nothing
		// needs to be. The snapshot's destination leg did not change, and the
		// member's own interior window has already seen every one of these
		// readings — each was recorded on its way past, one connection at a
		// time, which is what a streak is made of. That is the difference from
		// the destination leg, which only races ever write.
		entry.noteDegradation(group.tag, measurement.RemoteDial)
	}
	s.cache.touch(key)
	s.logger.Info("re-racing ", destination, ": ", group.tag, " scored ", was, " when measured, ", now, " now")
	go s.raceDetached(key, destination)
}

const (
	// anomalyFactor and anomalyStreak decide when a destination is considered to
	// have moved. Requiring a sustained multiple rather than a single sample
	// keeps ordinary jitter from triggering a race.
	anomalyFactor = 2
	anomalyStreak = 3
)
