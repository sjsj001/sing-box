package group

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing/common"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// warmup opens several connections to every member before serving traffic.
//
// naive spreads streams over a handful of isolated pools, so a single dial
// leaves the rest cold — and a cold pool costs a full TLS handshake. That
// handshake lands on whichever probe drew the cold pool, delaying its answer
// without making its path any slower, which is exactly the way a race can reach
// the wrong conclusion. Paying for the handshakes up front removes the
// distortion, and removes the first real request's cold start with it.
// It runs in two passes. The first reaches every member once, which is what
// produces the measurements; the second fills the remaining pools, and only for
// members that answered. Warming the pools of a member that is not there costs
// the full cold probe budget per dial and buys nothing, and with the dials
// bounded that wait is serial — on a start with no network it is the difference
// between tens of seconds and minutes.
func (s *Smart) warmup(ctx context.Context) {
	var first, rest []*smartMember
	for _, group := range s.groups {
		for _, member := range group.members {
			if member.outbound == nil {
				continue
			}
			first = append(first, member)
		}
	}
	s.probeMembers(ctx, first)

	for _, member := range first {
		if !member.healthy.Load() {
			continue
		}
		for range member.warmupDials() - 1 {
			rest = append(rest, member)
		}
	}
	s.probeMembers(ctx, rest)
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
// A member with no interior has nothing a real address could tell us that the
// proxy's own cannot, so it keeps that one and keeps emitting nothing. A
// member with an interior has two things only a real dial can settle: what
// that interior currently costs, and whether it is there at all — a relay
// whose own upstream has died answers the proxy address perfectly, every time.
func probeTarget(member *smartMember) M.Socksaddr {
	if member.relayed() {
		return reachabilityDestinations[0]
	}
	return naive.ProbeDestination()
}

// confirmMembers probes members whose reachability is in doubt, which is a
// different question from the one a heartbeat asks — see proveReachable.
func (s *Smart) confirmMembers(ctx context.Context, members []*smartMember) {
	s.probeEach(ctx, members, proveReachable)
}

// keepWarm and proveReachable are what a probe has to reach, and the choice
// says what the probe is for.
//
// keepWarm asks the proxy's own probe address, which it answers itself without
// dialing: nothing leaves the proxy, so an idle member can be kept warm and
// measured as often as we like without being visible to anyone downstream, and
// the round trip that comes back is purely the leg to the proxy — which is what
// makes it a clean measurement of that leg.
//
// proveReachable asks for somewhere real, because there is a failure the first
// question cannot see. A proxy whose own upstream is broken still answers its
// probe address perfectly — it never dials for it — so it stays healthy through
// a heartbeat while refusing every destination anyone actually wants. Measured
// on a relay whose next hop had stalled: real traffic failing in the same
// second as a heartbeat coming back in 128ms, over and over, with the member
// never once condemned.
//
// The address is an IP literal so a broken resolver at the proxy cannot answer
// the question the wrong way, and anycast so it is near wherever the exit is.
// Reaching it is not proof that everything works, and it does not need to be:
// this only ever runs alongside a real failure that already happened, so it is
// a second, independent reading rather than a lone canary.
func keepWarm(*smartMember) M.Socksaddr { return naive.ProbeDestination() }

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
// A member that carried real traffic within the interval is skipped: it has just
// proved both things a heartbeat checks. What is left is the idle members — and
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
// traffic within the interval, plus every member currently marked down — for
// those the heartbeat is the only thing that can discover they came back.
func (s *Smart) dueForHeartbeat(now time.Time) []*smartMember {
	deadline := now.Add(-heartbeatInterval).UnixNano()
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
// settles its health on the answer — see keepWarm and proveReachable for what
// the choice decides.
func (s *Smart) probeMember(ctx context.Context, member *smartMember, destination M.Socksaddr) {
	if member.outbound == nil {
		return
	}
	err := s.probeOnce(ctx, member, destination)
	if err == nil {
		return
	}
	if errors.Is(err, context.Canceled) {
		// Shutting down. Not a reading about anything, and no reason to spend a
		// second dial establishing that.
		return
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
		s.markDown(member, err)
		return
	}
	alternate, hasAlternate := alternateReachability(destination)
	if !hasAlternate {
		s.markDown(member, err)
		return
	}
	// Everything else is ambiguous from here: a refusal, a timeout and a
	// blackhole look the same whether the exit cannot reach anything or cannot
	// reach this one address. Asking a second, independent one is what tells
	// them apart, and it only costs a dial in the case that was already a
	// failure.
	if second := s.probeOnce(ctx, member, alternate); second != nil {
		s.markDown(member, second)
		return
	}
	s.logger.Debug("member ", member.tag, " could not reach ", destination,
		" but reached ", alternate, ": ", err)
}

// probeOnce measures one member against one address, banking what comes back.
func (s *Smart) probeOnce(ctx context.Context, member *smartMember, destination M.Socksaddr) error {
	// Derived from the member's own latency: a fixed budget either starves the
	// distant nodes, whose cold handshake alone can exceed it, or waits far too
	// long on the near ones.
	probeCtx, cancel := context.WithTimeout(ctx, s.probeTimeout(member, destination))
	defer cancel()

	conn, err := member.outbound.DialContext(probeCtx, N.NetworkTCP, destination)
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
	if !member.healthy.Load() {
		s.logger.Info("member ", member.tag, " is reachable again")
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
	if errors.Is(err, context.Canceled) || errors.Is(err, naive.ErrClosedLocally) {
		// Nobody was waiting for the answer, by the two routes that produce
		// that: the probe was called off, or its tunnel was closed from this
		// side. Neither is a reading of the member. This is the second of the
		// two doors a condemnation comes through — reportFailure is the other,
		// and it has to draw the same line or the one that does not becomes
		// the way a healthy node still gets taken out of service.
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
	if member.healthy.CompareAndSwap(true, false) {
		s.logger.Warn("member ", member.tag, " is unreachable: ", err)
	}
}
