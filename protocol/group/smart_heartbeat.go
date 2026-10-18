package group

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing/common"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// warmup reaches every member once before serving traffic, which is what
// produces the first measurements and opens each member's first pool.
//
// It used to make a second pass to open the rest of a member's pools. With
// streams placed on the least busy pool rather than in rotation, those dials
// landed on pools already open — an adaptive member has one at start anyway —
// and what they did open the server closed after two idle minutes.
//
// It does go round again for a member whose interior is still unmeasured,
// up to medianQuorum rounds, since that is what it takes to count. Left to
// the heartbeat, which comes round a minute later, every relay read as
// adjacent for the first minute or two, and UDP to a host with no TCP
// snapshot went to the nearest leg — on a core whose legs are all loopback,
// any group. A member the round condemned is left to its rechecks: against
// a dead one each round costs two cold probe budgets.
func (s *Smart) warmup(ctx context.Context) {
	var members []*smartMember
	for _, group := range s.groups {
		members = append(members, group.members...)
	}
	for range medianQuorum {
		s.probeMembers(ctx, members)
		members = slices.DeleteFunc(members, func(member *smartMember) bool {
			_, measured := member.chain()
			return measured || !member.healthy.Load()
		})
		if len(members) == 0 || ctx.Err() != nil {
			return
		}
	}
}

// probeMembers dials the given members, a few at a time.
//
// The bound is the point: all of them at once is dozens of TLS handshakes
// competing for the same cores, and the handshake time that comes back then
// measures the contention rather than the distance. Since the handshake is what
// bounds how close a member may claim to be, a starved one is filed as further
// away than it is — see smartMember.localFloorLocked.
func (s *Smart) probeMembers(ctx context.Context, members []*smartMember) {
	s.probeEach(ctx, members, probeTarget)
}

// probeTarget is what a routine heartbeat has to reach, from what this member
// has been measured to be.
//
// The default is the proxy's own probe address, which it answers itself
// without dialing: nothing leaves the proxy, so an idle member can be kept warm
// and measured as often as we like without being visible to anyone downstream,
// and the round trip that comes back is purely the leg to the proxy — which is
// what makes it a clean measurement of that leg.
//
// A member with no interior has nothing a real address could tell us that the
// proxy's own cannot, so it keeps that one and keeps emitting nothing. A
// member with an interior has two things only a real dial can settle: what
// that interior currently costs, and whether it is there at all — a relay
// whose own upstream has died answers the proxy address perfectly, every time.
//
// Two more cases need the real address for the same reason. A member that
// refused real traffic since its last heartbeat may have lost its egress, and
// a member that is down must not be let back in by the probe address — which
// is what an interface change used to do, probing every member this way.
//
// So does a member whose interior has not been measured yet, which is every
// member after a start: the chain window lives in memory only. Unmeasured, a
// relay reads as adjacent, and on a core whose members all dial a leg on
// loopback that sent the fallback and UDP with no snapshot to the furthest
// group until it had won three races. Three rounds settle it, and warmup runs
// all three; a member found to have no interior then goes back to the probe
// address.
func probeTarget(member *smartMember) M.Socksaddr {
	if member.relayed() || member.refused.Swap(false) || !member.healthy.Load() {
		return reachabilityDestinations[0]
	}
	if _, measured := member.chain(); !measured {
		return reachabilityDestinations[0]
	}
	return naive.ProbeDestination()
}

// confirmMembers probes members whose reachability is in doubt, which is a
// different question from the one a heartbeat asks — see proveReachable.
func (s *Smart) confirmMembers(ctx context.Context, members []*smartMember) {
	s.probeEach(ctx, members, proveReachable)
}

// proveReachable asks for somewhere real, because there is a failure the
// proxy's own probe address cannot see. A proxy whose own upstream is broken
// still answers that address perfectly — it never dials for it — so it stays
// healthy through a heartbeat while refusing every destination anyone actually
// wants. Measured on a relay whose next hop had stalled: real traffic failing
// in the same second as a heartbeat coming back in 128ms, over and over, with
// the member never once condemned.
//
// The address is an IP literal so a broken resolver at the proxy cannot answer
// the question the wrong way, and anycast so it is near wherever the exit is.
// Reaching it is not proof that everything works, and it does not need to be:
// this only ever runs alongside a real failure that already happened, so it is
// a second, independent reading rather than a lone canary.
func proveReachable(*smartMember) M.Socksaddr { return reachabilityDestinations[0] }

// reachabilityDestinations are what a probe dials when the question needs the
// whole chain crossed, and there are two of them, run by different operators,
// on purpose.
//
// One address cannot carry a verdict about a member. Whatever an exit does to a
// particular address — a provider firewall rule, a null route, a ruleset that
// filters one vendor — it does every round, so a member judged on a single
// address is either condemned for the life of the process or, if that refusal
// is forgiven, never condemnable at all. Neither reading is about the member.
// Two independent addresses separate the two cases: one refused while the other
// answers is a verdict about the address, both refused is a verdict about the
// exit.
//
// Both are IP literals so a broken resolver at the proxy cannot answer the
// question the wrong way, and both are anycast so they are near wherever the
// exit is. Port 443 because we never send a payload and close immediately: at
// 443 that is indistinguishable from the interrupted TLS attempts every device
// makes by the hundred, where on 80 a bare connect that says nothing stands out.
//
// The residue, stated rather than hidden: an exit filtering both is condemned
// while healthy. That is a far smaller target than filtering either one, and it
// fails in the direction that keeps traffic off a node we cannot confirm.
var reachabilityDestinations = [2]M.Socksaddr{
	M.ParseSocksaddr("1.1.1.1:443"),
	M.ParseSocksaddr("9.9.9.9:443"),
}

// alternateReachability is the other real address to cross-check a refusal
// against, when the one that failed was a real address at all.
//
// A failure against the proxy's own probe address has nothing beyond the proxy
// inside it, so it is already a reading about the member and there is nothing
// to cross-check it with.
func alternateReachability(destination M.Socksaddr) (M.Socksaddr, bool) {
	switch destination.String() {
	case reachabilityDestinations[0].String():
		return reachabilityDestinations[1], true
	case reachabilityDestinations[1].String():
		return reachabilityDestinations[0], true
	}
	return M.Socksaddr{}, false
}

func (s *Smart) probeEach(ctx context.Context, members []*smartMember, destination func(*smartMember) M.Socksaddr) {
	slots := s.slots()
	var wg sync.WaitGroup
	for _, member := range members {
		if member.outbound == nil {
			// Nothing to dial, so it would take a slot to do nothing with it.
			// Filtered here rather than in each caller, since every one of them
			// has to.
			continue
		}
		wg.Add(1)
		go func(member *smartMember) {
			defer wg.Done()
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			// Released only on the path that took a slot; the branch above
			// returns before this is registered.
			defer func() { <-slots }()
			if ctx.Err() != nil {
				// A free slot and a cancelled round can be ready together, and
				// select picks between them at random.
				return
			}
			s.probeMember(ctx, member, destination(member))
		}(member)
	}
	wg.Wait()
}

// slots hands out the shared probe bound, creating it if this outbound was
// assembled without NewSmart.
//
// The alternative is a nil channel, and a send on one of those blocks for ever
// rather than failing — the kind of mistake that costs a hang instead of an
// error, and that only the paths nothing exercises would hit.
func (s *Smart) slots() chan struct{} {
	s.probeSlotsOnce.Do(func() {
		if s.probeSlots == nil {
			s.probeSlots = make(chan struct{}, probeConcurrency)
		}
	})
	return s.probeSlots
}

// loop runs the two periodic jobs: keeping idle members measured and reachable,
// and getting decisions onto disk.
func (s *Smart) loop(ctx context.Context) {
	heartbeat := time.NewTimer(nextHeartbeat())
	defer heartbeat.Stop()
	flush := time.NewTicker(cacheFlushInterval)
	defer flush.Stop()
	state := time.NewTicker(auditStateInterval)
	defer state.Stop()
	usage := time.NewTicker(auditUsageInterval)
	defer usage.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			s.heartbeatRound(ctx)
			heartbeat.Reset(nextHeartbeat())
		case <-flush.C:
			if err := s.cache.flush(); err != nil {
				s.logger.Debug("flush smart cache: ", err)
			}
		case <-state.C:
			// Periodically as well as at shutdown, so a process that is killed
			// still leaves the trail able to answer what it thought at the time.
			s.auditState()
		case <-usage.C:
			s.auditUsage()
		}
	}
}

// nextHeartbeat scatters the interval so idle connections carry no period.
//
// What crosses the wire is encrypted, but its timing is not: a small request
// and a small response arriving on an otherwise silent connection exactly every
// sixty seconds is something anyone watching the flow can pick out without
// decrypting a byte. Nothing here is time-critical, so the moment is free to
// move.
func nextHeartbeat() time.Duration {
	return heartbeatInterval - heartbeatJitter + rand.N(2*heartbeatJitter)
}

// heartbeatRound pings the members that need it.
//
// A member that carried real traffic within heartbeatSkipWindow is skipped: it
// has just proved both things a heartbeat checks. What is left is the idle members — and
// those are exactly the ones a failover will reach for, and the ones whose
// connection pool would otherwise have gone cold. The traffic therefore scales
// with idleness, not with the size of the configuration.
func (s *Smart) heartbeatRound(ctx context.Context) {
	if s.paused() {
		// The device is asleep and the network under it is torn down. A probe
		// now wakes the radio to collect a timeout, and markDown reads that
		// timeout as the member having died — a wake would then start from
		// everything condemned at once. The members' health simply stays as it
		// was until the device is back.
		return
	}
	// Bounded for the same reason warmup is: after a network change every pool
	// is cold at once, and a round that measures its own contention poisons the
	// estimates it exists to keep fresh.
	//
	// Split by what each member's probe has to settle. A healthy idle one is
	// only being kept warm, and the probe address does that without emitting
	// anything. A member that is down has to be *let back in*, and reaching the
	// proxy is not enough to earn that: a proxy whose upstream is broken
	// answers its own probe address every time, so a heartbeat against it would
	// lift a condemnation that is still true — and, because any successful
	// measurement clears the flag, it would do so within a minute, every
	// minute, for as long as the outage lasted.
	warm, down := split(s.dueForHeartbeat(time.Now()), func(member *smartMember) bool {
		return member.healthy.Load()
	})
	s.probeMembers(ctx, warm)
	s.confirmMembers(ctx, down)
}

// split partitions members by a predicate, keeping the order of each side.
func split(members []*smartMember, yes func(*smartMember) bool) (matched, rest []*smartMember) {
	for _, member := range members {
		if yes(member) {
			matched = append(matched, member)
		} else {
			rest = append(rest, member)
		}
	}
	return matched, rest
}

// dueForHeartbeat is who a round would probe: every member that has not carried
// traffic within heartbeatSkipWindow, plus every member currently marked down —
// for those the heartbeat is the only thing that can discover they came back.
func (s *Smart) dueForHeartbeat(now time.Time) []*smartMember {
	deadline := now.Add(-heartbeatSkipWindow).UnixNano()
	var due []*smartMember
	for _, group := range s.groups {
		for _, member := range group.members {
			if member.outbound == nil {
				continue
			}
			if member.healthy.Load() && member.lastUsed.Load() > deadline {
				continue
			}
			due = append(due, member)
		}
	}
	return due
}

// probeMember measures one member against the address the caller chose, and
// settles its health on the answer — see probeTarget and proveReachable for what
// the choice decides.
func (s *Smart) probeMember(ctx context.Context, member *smartMember, destination M.Socksaddr) {
	if err := s.probeVerdict(ctx, member, destination, 0); err != nil {
		s.markDown(member, err)
	}
}

// probeVerdict is the failure a probe of member ends in, nil when the member
// answered. Each dial gets budget, or one derived from the member when that is
// zero; see probeOnce.
func (s *Smart) probeVerdict(ctx context.Context, member *smartMember, destination M.Socksaddr, budget time.Duration) error {
	if member.outbound == nil {
		return nil
	}
	err := s.probeOnce(ctx, member, destination, budget)
	if errors.Is(err, net.ErrClosed) && ctx.Err() == nil {
		// The session the probe was on went away under it. On a network
		// change the member drops its own sessions just after this round
		// starts whenever this group comes first in the configuration, and
		// the streams in flight get one of two answers: ERR_ABORTED, which is
		// naive.ErrClosedLocally, or ERR_CONNECTION_CLOSED for a stream placed
		// on a session already draining — three to five of eight resets
		// condemned a healthy member that way. The second is also what a
		// server's FIN reads as, so it is not excused, only tried again: the
		// attempt lands on a fresh session, and its outcome is judged below
		// like any other. net.ErrClosed is what every "session gone" answer
		// matches, ErrClosedLocally included; a refusal or a timeout does not,
		// so a node that is dead is not dialed twice for it.
		err = s.probeOnce(ctx, member, destination, budget)
	}
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, naive.ErrClosedLocally) {
		// Shutting down, or the round was called off by an interface change,
		// or the tunnel was closed from this side again — see above.
		// Not a reading about anything, and no reason to spend a second dial
		// establishing that. A real address may have been asked because the
		// member refused traffic, and probeTarget consumed that evidence: put
		// it back, or a burst of interface changes erases it and the member
		// must refuse again before anything checks its egress.
		if _, real := alternateReachability(destination); real {
			member.refused.Store(true)
		}
		return err
	}
	if errors.Is(err, naive.ErrNextHopUnreachable) && errors.Is(err, naive.ErrProxyAnswered) {
		// The proxy answered, and what it said was that its own next hop is
		// gone. That refusal is already about the member — it is the one leg no
		// choice of destination can route around — so a second address has
		// nothing to add.
		//
		// Both halves of the condition are load-bearing. Everything that is not
		// a 502 is filed under this error, silence included, so without the
		// second half a probe that simply timed out would be read as the proxy
		// having spoken — and a blackholed address is precisely the case the
		// cross-check below exists for, and precisely the one that repeats
		// every round.
		return err
	}
	alternate, hasAlternate := alternateReachability(destination)
	if !hasAlternate {
		return err
	}
	// Everything else is ambiguous from here: a refusal, a timeout and a
	// blackhole look the same whether the exit cannot reach anything or cannot
	// reach this one address. Asking a second, independent one is what tells
	// them apart, and it only costs a dial in the case that was already a
	// failure.
	if second := s.probeOnce(ctx, member, alternate, budget); second != nil {
		if errors.Is(second, context.Canceled) || errors.Is(second, naive.ErrClosedLocally) {
			// Called off before the second opinion came in; keep the
			// evidence that sent this probe to real addresses, as above.
			member.refused.Store(true)
		}
		return second
	}
	s.logger.Debug("member ", member.tag, " could not reach ", destination,
		" but reached ", alternate, ": ", err)
	return nil
}

// probeOnce measures one member against one address, banking what comes back.
//
// The budget is derived from the member's own latency unless the caller gives
// one: a fixed budget either starves the distant nodes, whose cold handshake
// alone can exceed it, or waits far too long on the near ones.
func (s *Smart) probeOnce(ctx context.Context, member *smartMember, destination M.Socksaddr, budget time.Duration) error {
	derived := budget == 0
	if derived {
		budget = s.probeTimeout(member, destination)
	}
	probeCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	conn, err := member.outbound.DialContext(naive.WithProbe(probeCtx), N.NetworkTCP, destination)
	if err != nil {
		return err
	}
	defer conn.Close()

	measured, isMeasured := common.Cast[naive.MeasuredConn](conn)
	if !isMeasured {
		return nil
	}
	measurement, err := measured.Measure(probeCtx)
	if err != nil {
		return err
	}
	if !measurement.HasSpan {
		// A server that understands the extension always reports a span, for
		// its own probe address and for anywhere else. One that does not is a
		// stock naiveproxy: it tunnels perfectly well, but it cannot be ranked,
		// and every heartbeat against it will keep looking like a fault. Saying
		// so once is the difference between a diagnosable configuration and a
		// node that is mysteriously always unreachable.
		member.unsupported.Do(func() {
			s.logger.Warn("member ", member.tag,
				" does not report connect timings; the server does not support the smart extension")
		})
	}
	if !derived {
		// Given long enough to wait out a queue, so what it timed is the
		// queue: banked, ten seconds of it would sit in the chain window as
		// distance, and the window drops only its worst sample of eight.
		return nil
	}
	s.recordProbe(member, destination, measurement)
	return nil
}

// probeTimeout budgets a heartbeat: a cold connection costs a handshake on top
// of the round trip, and both scale with how far away the member is.
func (s *Smart) probeTimeout(member *smartMember, destination M.Socksaddr) time.Duration {
	// Budgeted for the legs this probe actually crosses. The heartbeat address
	// is answered by the first proxy, so charging a relay's interior to it
	// would widen exactly the blackhole bound this exists to keep tight; a
	// probe that goes somewhere real crosses the whole chain and has to be
	// allowed the time that takes. Getting this wrong is not academic — a
	// member whose interior had grown to three seconds would be given a budget
	// derived from its 132ms near hop and condemned every round for missing it.
	var (
		leg      time.Duration
		measured bool
	)
	if naive.IsProbe(destination) {
		leg, measured = member.near()
	} else {
		leg, measured = member.local()
	}
	if !measured {
		return probeTimeoutCold
	}
	local := leg
	timeout := local*probeTimeoutFactor + probeTimeoutFloor
	if timeout < probeTimeoutFloor {
		return probeTimeoutFloor
	}
	return timeout
}

func (s *Smart) markDown(member *smartMember, err error) {
	if calledOff(err) {
		return
	}
	// A refusal about one address is deliberately *not* filtered out here.
	// It was, briefly, and the cure was worse: forgiving every 502 means an
	// exit whose egress is gone answers "cannot reach it" for every destination
	// in the world and stays healthy for the life of the process, holding the
	// traffic of a group whose other members are fine. Which of the two a
	// refusal is cannot be told from the refusal — it is told by asking a second
	// independent address, which probeMember has already done by the time
	// anything reaches here. See reachabilityDestinations.
	if s.congested(member, err) {
		return
	}
	s.condemn(member, err)
}

// calledOff reports a probe failure that is no reading of the member.
//
// Nobody was waiting for the answer, by the two routes that produce that: the
// probe was called off, or its tunnel was closed from this side. markDown is
// the second of the two doors a condemnation comes through — reportFailure is
// the other, and it has to draw the same line or the one that does not becomes
// the way a healthy node still gets taken out of service.
//
// Nor is a stream the proxy refused unprocessed: its session was closing for
// idleness as the probe arrived, which an idle member's heartbeat lands on now
// and then. The next round measures it again.
func calledOff(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, naive.ErrClosedLocally) || errors.Is(err, naive.ErrStreamRefused)
}

// congested reports — and says, once per episode — that a probe went
// unanswered on a member that is still delivering, which is a member too busy
// to answer in time rather than a dead one.
//
// Seen in production: one large download saturated the path through a
// group's only member, new streams on it waited thirty seconds for an answer,
// and the confirming probe ran out of its two-second budget. The member was
// condemned, the download moved to the next group and saturated that one, and
// then the next — three countries taken out one after another by a node that
// had been delivering the whole time. Moving the load cures nothing there: the
// queue is on whatever path carries it.
//
// Only silence is excused: the probe's own budget running out, or the bound on
// establishing the leg. A proxy that answered — refused, 502, 503 — or a leg
// that broke said something about the member, and is judged as before. And
// only for congestionGrace after the last payload arrived: a member that has
// really died stops delivering too, and the first probe after that condemns
// it. Nor for ever while it keeps delivering: each episode starts one probe
// that is given as long as a connection would wait, see outwait.
func (s *Smart) congested(member *smartMember, err error) bool {
	if !member.healthy.Load() || !unanswered(err) || !member.delivering(time.Now()) {
		return false
	}
	if member.congested.CompareAndSwap(false, true) {
		s.logger.Info("member ", member.tag, " is congested, not condemning: ", err)
		s.auditHealth(member, "congested", err)
	}
	// Every excuse, not only an episode's first: a long probe that was called
	// off ended without a verdict and left the episode open, and nothing else
	// would start another. outwaiting keeps it to one in flight.
	s.outwait(member)
	return true
}

// outwait bounds the congestion excuse: one probe per episode, given
// settleTimeout per address — what a connection waits for its CONNECT to be
// answered — and condemning the member if it still hears nothing.
//
// The excuse alone had no end. A member whose old streams keep delivering
// while its new ones are never answered — an exit whose connection table is
// full, a provider dropping new SYNs — was excused on every probe for as long
// as one download lasted, and every new connection on it waited out the whole
// settle. Real bottleneck queues measured one to fifteen seconds; a member
// that answers nothing in thirty has already failed every connection placed
// on it in that time.
//
// Its failures go straight to condemn rather than through markDown, whose
// excuse is for a probe that ran out of a budget shorter than the queue. This
// one has outwaited any queue, so silence now is not congestion; and it went
// through probeVerdict, so the second address was asked with the same time to
// answer, and an exit that drops one of them is not condemned for it. A late
// answer ends the episode without being banked; see probeOnce. Nothing is done
// when the member has been condemned since it began, or condemned and let back
// in: what it heard is older than that verdict.
//
// recordProbe and a late answer both end an episode, so a download that keeps
// the path full starts one of these each time a probe is excused again.
func (s *Smart) outwait(member *smartMember) {
	if member.outbound == nil || !member.outwaiting.CompareAndSwap(false, true) {
		return
	}
	downAt := member.downAt.Load()
	go func() {
		defer member.outwaiting.Store(false)
		err := s.probeVerdict(s.ctx, member, reachabilityDestinations[0], settleTimeout)
		// An answer heard meanwhile ended the episode; what this probe heard is
		// older than it.
		if s.closed.Load() || !member.healthy.Load() || member.downAt.Load() != downAt || !member.congested.Load() {
			return
		}
		if err == nil {
			member.congested.Store(false)
			return
		}
		if !calledOff(err) {
			s.condemn(member, err)
		}
	}()
}

// unanswered reports a failure that is silence: a deadline ran out with no
// status from the proxy. Both shapes it arrives in wrap context.DeadlineExceeded
// — a probe's budget expiring inside Measure, and errLegTimedOut — and neither
// carries ErrProxyAnswered, which every status does.
func unanswered(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, naive.ErrProxyAnswered)
}

// condemn takes a member out of service. markDown and reportFailure both come
// here, so a condemnation is recorded the same way whichever door it came
// through — including for the flap hold, which needs every one of them.
func (s *Smart) condemn(member *smartMember, err error) {
	if !member.healthy.CompareAndSwap(true, false) {
		return
	}
	member.previousDownAt.Store(member.downAt.Load())
	downAt := time.Now().UnixNano()
	member.downAt.Store(downAt)
	member.congested.Store(false)
	if s.ctx.Err() == nil && !s.closed.Load() {
		s.scheduleRechecks(member)
	}
	if member.heldDown(time.Unix(0, downAt), s.flapHold()) {
		// The rechecks inside the hold are skipped by it; this is the one at
		// its end.
		time.AfterFunc(s.flapHold(), func() {
			if !s.closed.Load() && !member.healthy.Load() && member.downAt.Load() == downAt {
				s.confirmDown(member)
			}
		})
	}
	member.udp.Interrupt(true)
	s.logger.Warn("member ", member.tag, " is unreachable: ", err)
	s.auditHealth(member, "down", err)
}
