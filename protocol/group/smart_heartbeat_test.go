package group

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/uot"
	"github.com/stretchr/testify/require"
)

// noInterior gives member what three heartbeats on a real address leave for
// one that dials destinations itself: an interior measured, and found absent.
func noInterior(member *smartMember) *smartMember {
	for range medianQuorum {
		member.chainWindow.add(0)
	}
	return member
}

func TestMarkDownIgnoresAProbeThisEndGaveUpOn(t *testing.T) {
	t.Parallel()
	// A condemnation arrives through two doors — reportFailure for a caller's
	// dial, markDown for a probe — and a line drawn at only one of them is the
	// door a healthy node still gets taken out of service through. Both a
	// cancelled probe and one whose tunnel this side closed say nothing about
	// the member.
	smart := &Smart{ctx: context.Background(), logger: logger.NOP()}
	member := &smartMember{tag: "out-us-dmit"}
	member.healthy.Store(true)

	smart.markDown(member, fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, context.Canceled))
	require.True(t, member.healthy.Load())
	smart.markDown(member, fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, naive.ErrClosedLocally))
	require.True(t, member.healthy.Load(), "this end closed it; nothing was measured")

	// The verdict the probe exists to reach still lands.
	smart.markDown(member, E.Cause(naive.ErrNextHopUnreachable, "connection refused"))
	require.False(t, member.healthy.Load())
}

// strandedOutbound is a proxy that is up and answers its own probe address
// perfectly, but cannot reach anywhere else — a relay whose next hop has
// stalled, which is what this looked like in production.
type strandedOutbound struct {
	adapter.Outbound
	dialed chan M.Socksaddr
}

func (o *strandedOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *strandedOutbound) DialContext(_ context.Context, _ string, destination M.Socksaddr) (net.Conn, error) {
	select {
	case o.dialed <- destination:
	default:
	}
	if naive.IsProbe(destination) {
		// Answered by the proxy itself, without dialing: it never leaves, so a
		// broken upstream cannot make it fail.
		return &answeringConn{measurement: answering(ms(20), 0)}, nil
	}
	// What a relay that cannot reach its own next hop answers: 503, which the
	// client reads as the hop being broken rather than the destination. A 502
	// would be the opposite claim — the exit reporting on the destination —
	// and it is deliberately not what this models. See statusForError.
	//
	// Carrying ErrProxyAnswered because a status came back, which is what
	// classifyHandshakeError does and what separates this from silence.
	return nil, fmt.Errorf("%w: %w: unexpected response status: 503",
		naive.ErrNextHopUnreachable, naive.ErrProxyAnswered)
}

func TestAProxyThatCanReachNothingIsCondemned(t *testing.T) {
	t.Parallel()
	// The failure the probe address cannot see. A proxy whose own upstream is
	// broken answers that address every time — it never dials for it — so a
	// heartbeat against it comes back healthy while every destination anyone
	// wants is refused. Measured on a relay: real traffic failing in the same
	// second as a heartbeat returning in 128ms, and the member never condemned.
	out := &strandedOutbound{dialed: make(chan M.Socksaddr, 8)}
	member := noInterior(&smartMember{tag: "stranded", outbound: out})
	member.healthy.Store(true)
	group := &smartGroup{tag: "us", members: []*smartMember{member}}
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{group},
	}

	// A heartbeat still says it is fine, and that reading is not wrong — it is
	// answering a different question.
	smart.probeMembers(context.Background(), []*smartMember{member})
	require.True(t, member.healthy.Load(),
		"the probe address is answered by the proxy, so it cannot fail this way")

	// The confirming probe asks for somewhere real, and that settles it.
	smart.confirmMembers(context.Background(), []*smartMember{member})
	require.False(t, member.healthy.Load(),
		"a proxy that refuses a real destination while its own probe passes must be condemned")
}

func TestARealFailureIsConfirmedAgainstARealDestination(t *testing.T) {
	t.Parallel()
	// reportFailure verifies an instant failure before condemning, and the
	// verdict is only worth anything if the probe tests what failed: a dial to
	// a destination. Against the proxy's own probe address it tests the one
	// leg that was never in doubt.
	out := &strandedOutbound{dialed: make(chan M.Socksaddr, 8)}
	member := noInterior(&smartMember{tag: "stranded", outbound: out})
	member.healthy.Store(true)
	group := &smartGroup{tag: "us", members: []*smartMember{member}}
	group.selected.Store(member)
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{group},
	}

	smart.reportFailure("3f2a", M.ParseSocksaddr("example.com:443"), group, member,
		E.Cause(naive.ErrNextHopUnreachable, "use of closed network connection"))

	require.Eventually(t, func() bool { return !member.healthy.Load() }, time.Second, time.Millisecond,
		"the confirming probe's verdict must condemn a proxy that reaches nothing")

	var asked []M.Socksaddr
	for len(out.dialed) > 0 {
		asked = append(asked, <-out.dialed)
	}
	require.NotEmpty(t, asked)
	for _, destination := range asked {
		require.False(t, naive.IsProbe(destination),
			"the confirmation must go somewhere real, not to the address the proxy answers itself")
	}
}

func TestACondemnedMemberIsNotLetBackInByTheProbeAddressAlone(t *testing.T) {
	t.Parallel()
	// Any successful measurement clears the health flag, so if the heartbeat
	// kept asking for the probe address a condemned member would be let back in
	// within a minute — every minute, for as long as the outage lasted, each
	// time sending traffic straight back into it.
	out := &strandedOutbound{dialed: make(chan M.Socksaddr, 8)}
	member := noInterior(&smartMember{tag: "stranded", outbound: out})
	group := &smartGroup{tag: "us", members: []*smartMember{member}}
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{group},
	}
	member.healthy.Store(false)

	require.Len(t, smart.dueForHeartbeat(time.Now()), 1, "a member that is down is due")
	smart.heartbeatRound(context.Background())

	require.False(t, member.healthy.Load(),
		"readmission has to be earned against a real destination, not the one the proxy answers itself")
}

func TestAHealthyIdleMemberIsKeptWarmWithoutEmittingTraffic(t *testing.T) {
	t.Parallel()
	// The other half of the rule: the routine heartbeat must stay on the probe
	// address. It is what makes the measurement purely the leg to the proxy,
	// and what keeps an idle member from putting a fixed address on the wire
	// every minute for anyone downstream to notice.
	out := &strandedOutbound{dialed: make(chan M.Socksaddr, 8)}
	member := noInterior(&smartMember{tag: "warm", outbound: out})
	member.healthy.Store(true)
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{{tag: "us", members: []*smartMember{member}}},
	}

	smart.heartbeatRound(context.Background())

	require.NotEmpty(t, out.dialed)
	for len(out.dialed) > 0 {
		require.True(t, naive.IsProbe(<-out.dialed),
			"a healthy member is only being kept warm; nothing needs to leave the proxy for that")
	}
	require.True(t, member.healthy.Load())
}

// filteringOutbound is a proxy that works, but whose exit refuses some set of
// addresses — a firewall rule at the provider, an exit that blocks one vendor,
// or, when the set is everything, an exit whose egress is simply gone.
type filteringOutbound struct {
	adapter.Outbound
	blocked  []M.Socksaddr
	blockAll bool
	dialed   chan M.Socksaddr
}

func (o *filteringOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *filteringOutbound) DialContext(_ context.Context, _ string, destination M.Socksaddr) (net.Conn, error) {
	if o.dialed != nil {
		select {
		case o.dialed <- destination:
		default:
		}
	}
	if naive.IsProbe(destination) {
		// Answered by the proxy itself, so nothing the exit does reaches it.
		return &answeringConn{measurement: answering(ms(20), 0)}, nil
	}
	refused := o.blockAll
	for _, address := range o.blocked {
		if destination.String() == address.String() {
			refused = true
		}
	}
	if refused {
		return nil, fmt.Errorf("%w: %w: unexpected response status: 502",
			naive.ErrDestinationUnreachable, naive.ErrProxyAnswered)
	}
	return &answeringConn{measurement: answering(ms(20), ms(5))}, nil
}

func TestAFilteredProbeAddressDoesNotCondemnTheMember(t *testing.T) {
	t.Parallel()
	// The probe asks for somewhere real so it can see a proxy that has lost its
	// upstream. The cost of asking for somewhere real is that the answer can be
	// about that somewhere: an exit that filters one address refuses it every
	// round. Condemning the member for that is a verdict about the wrong thing,
	// and it does not lapse — a condemned member is probed with the same
	// address, refused again, and skipped by current() in between, for the life
	// of the process. The second address is what settles it.
	out := &filteringOutbound{
		blocked: []M.Socksaddr{reachabilityDestinations[0]},
		dialed:  make(chan M.Socksaddr, 8),
	}
	member := &smartMember{tag: "filtered", outbound: out}
	member.healthy.Store(true)
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{{tag: "us", members: []*smartMember{member}}},
	}

	for range 3 {
		smart.confirmMembers(context.Background(), []*smartMember{member})
	}
	require.True(t, member.healthy.Load(),
		"one address was refused and the other answered, which says nothing about the member")

	var reached bool
	for len(out.dialed) > 0 {
		if (<-out.dialed).String() == reachabilityDestinations[1].String() {
			reached = true
		}
	}
	require.True(t, reached, "the refusal has to be cross-checked against the other address")

	// A proxy that cannot be reached at all is still condemned, so the guard
	// has not simply switched the check off.
	stranded := &smartMember{tag: "stranded", outbound: &strandedOutbound{dialed: make(chan M.Socksaddr, 4)}}
	stranded.healthy.Store(true)
	smart.confirmMembers(context.Background(), []*smartMember{stranded})
	require.False(t, stranded.healthy.Load())
}

func TestAnExitThatReachesNothingIsCondemned(t *testing.T) {
	t.Parallel()
	// The other half of the same question, and the one forgiving every refusal
	// used to get wrong. An exit whose egress is gone answers "cannot reach it"
	// for every address there is, including its own probe address, which it
	// answers perfectly because it never dials for that one. Left uncondemned it
	// keeps being handed the traffic of a group whose other members are fine.
	out := &filteringOutbound{blockAll: true}
	member := &smartMember{tag: "stranded-exit", outbound: out}
	member.healthy.Store(true)
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{{tag: "us", members: []*smartMember{member}}},
	}

	smart.confirmMembers(context.Background(), []*smartMember{member})
	require.False(t, member.healthy.Load(),
		"both independent addresses were refused, which is a verdict about the exit")
}

// blackholingConn is a tunnel to an address the exit silently drops: the proxy
// is reachable, the CONNECT goes out, and nothing ever comes back. What the
// client is left holding is its own deadline, not anything the proxy said.
type blackholingConn struct {
	answeringConn
}

func (c *blackholingConn) Measure(ctx context.Context) (naive.ConnMeasurement, error) {
	if _, bounded := ctx.Deadline(); !bounded {
		// Against a real blackhole this probe would never return.
		panic("a probe must be measured under a deadline")
	}
	// The error the client is left with once its deadline runs out, returned
	// straight away: waiting the deadline out for real costs the probe timeout
	// floor and adds nothing the classifier can see. The shape is the one the
	// real classifier produces: everything that is not a 502 is filed under
	// ErrNextHopUnreachable, silence included, and no cronet HandshakeError is
	// attached because there was no status to attach.
	return naive.ConnMeasurement{}, fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, context.DeadlineExceeded)
}

// blackholingOutbound is a working proxy behind an exit that null-routes one
// address — a provider filter that drops instead of refusing.
type blackholingOutbound struct {
	adapter.Outbound
	blocked M.Socksaddr
	dialed  chan M.Socksaddr
}

func (o *blackholingOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *blackholingOutbound) DialContext(_ context.Context, _ string, destination M.Socksaddr) (net.Conn, error) {
	select {
	case o.dialed <- destination:
	default:
	}
	if destination.String() == o.blocked.String() {
		return &blackholingConn{}, nil
	}
	return &answeringConn{measurement: answering(ms(20), ms(5))}, nil
}

func TestASilentlyDroppedProbeAddressIsCrossCheckedToo(t *testing.T) {
	t.Parallel()
	// The failure a refused address and a dropped one do not share. A refusal
	// arrives as a status the proxy sent; a drop arrives as our own deadline,
	// and everything that is not a 502 — silence included — is filed under the
	// same error as "my next hop is gone". Read as the proxy having spoken, a
	// null-routed address condemns a working member every round, with the down
	// half of the heartbeat asking the same address again.
	out := &blackholingOutbound{
		blocked: reachabilityDestinations[0],
		dialed:  make(chan M.Socksaddr, 8),
	}
	member := &smartMember{tag: "blackholed", outbound: out}
	member.healthy.Store(true)
	member.record(measured(ms(20)))
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{{tag: "us", members: []*smartMember{member}}},
	}

	smart.confirmMembers(context.Background(), []*smartMember{member})
	require.True(t, member.healthy.Load(),
		"one address went into a hole and the other answered, which says nothing about the member")

	var asked []string
	for len(out.dialed) > 0 {
		asked = append(asked, (<-out.dialed).String())
	}
	require.Contains(t, asked, reachabilityDestinations[1].String(),
		"silence has to be cross-checked, exactly as a refusal is")
}

func TestAProxyThatSaysItsNextHopIsGoneIsNotCrossChecked(t *testing.T) {
	t.Parallel()
	// The other side of the same line. A 503 is a statement the proxy made
	// about itself, and no choice of destination routes around the leg it is
	// about, so spending a second probe on it would only delay the verdict.
	out := &strandedOutbound{dialed: make(chan M.Socksaddr, 8)}
	member := noInterior(&smartMember{tag: "stranded", outbound: out})
	member.healthy.Store(true)
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{{tag: "us", members: []*smartMember{member}}},
	}

	smart.confirmMembers(context.Background(), []*smartMember{member})
	require.False(t, member.healthy.Load())

	var asked []string
	for len(out.dialed) > 0 {
		asked = append(asked, (<-out.dialed).String())
	}
	require.NotContains(t, asked, reachabilityDestinations[1].String(),
		"an answer about the member itself needs no second opinion")
}

// egressOutbound is a direct exit whose own reach the test decides: its probe
// address always answers, since the proxy answers that itself, and a real
// destination answers only when reach says so — otherwise the exit refuses it
// with a 502.
type egressOutbound struct {
	adapter.Outbound
	reach  func(M.Socksaddr) bool
	dialed chan M.Socksaddr
}

func (o *egressOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *egressOutbound) DialContext(_ context.Context, _ string, destination M.Socksaddr) (net.Conn, error) {
	select {
	case o.dialed <- destination:
	default:
	}
	if naive.IsProbe(destination) || o.reach(destination) {
		return &answeringConn{measurement: answering(ms(130), ms(1))}, nil
	}
	return &refusingConn{}, nil
}

func egressFixture(reach func(M.Socksaddr) bool) (*Smart, *smartGroup, *smartMember, *egressOutbound) {
	out := &egressOutbound{reach: reach, dialed: make(chan M.Socksaddr, 8)}
	main := noInterior(newMember("main", ms(130)))
	main.outbound = out
	// Idle for longer than the interval, which is what a member refusing
	// everything becomes: a 502 is never recorded as use.
	main.lastUsed.Store(time.Now().Add(-2 * heartbeatInterval).UnixNano())
	spare := newMember("spare", ms(140))
	spare.outbound = &answeringOutbound{measurement: answering(ms(140), ms(1))}
	group := &smartGroup{tag: "us", members: []*smartMember{main, spare}}
	s := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		alpha:  testAlpha,
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{group},
	}
	return s, group, main, out
}

var refused502 = fmt.Errorf("%w: %w: unexpected response status: 502",
	naive.ErrDestinationUnreachable, naive.ErrProxyAnswered)

func TestAnExitThatRefusesEverythingIsCondemnedByTheNextHeartbeat(t *testing.T) {
	t.Parallel()
	// A 502 is filed against the destination. An exit whose egress is gone
	// answers 502 for every destination, and its probe address keeps
	// answering — so a heartbeat on that address kept it healthy for ever,
	// holding the group off its working spare.
	s, group, main, _ := egressFixture(func(M.Socksaddr) bool { return false })
	s.reportFailure("k", M.ParseSocksaddr("example.com:443"), group, main, refused502)

	s.heartbeatRound(context.Background())
	require.False(t, main.healthy.Load())
	require.Equal(t, "spare", group.current().tag)
}

func TestARefusalOfOneDestinationDoesNotCondemnAnExitThatReachesOthers(t *testing.T) {
	t.Parallel()
	s, group, main, out := egressFixture(func(destination M.Socksaddr) bool {
		return destination == reachabilityDestinations[0]
	})
	s.reportFailure("k", M.ParseSocksaddr("example.com:443"), group, main, refused502)

	s.heartbeatRound(context.Background())
	require.Equal(t, reachabilityDestinations[0], <-out.dialed, "the refusal is checked against a real address")
	require.True(t, main.healthy.Load())
	require.Equal(t, "main", group.current().tag)

	main.lastUsed.Store(time.Now().Add(-2 * heartbeatInterval).UnixNano())
	s.heartbeatRound(context.Background())
	require.True(t, naive.IsProbe(<-out.dialed), "one check per refusal, then back to the probe address")
}

func TestRefusalRunsProbeAMemberOncePerInterval(t *testing.T) {
	t.Parallel()
	// A port scan through a relay is one refused destination after another,
	// and every third spent a confirming probe on the exit.
	s, group, main, out := egressFixture(func(destination M.Socksaddr) bool {
		return slices.Contains(reachabilityDestinations[:], destination)
	})
	refused := 0
	refuseRun := func() {
		for range refusalBurst {
			refused++
			s.reportFailure(fmt.Sprint("k", refused), M.ParseSocksaddr(fmt.Sprintf("host%d.example:443", refused)), group, main, refused502)
		}
	}
	// confirmDown raises probing before it returns, so a run that probed is
	// waited for here and one that did not finds nothing to wait for.
	probes := func() int {
		require.Eventually(t, func() bool { return !main.probing.Load() }, 5*time.Second, time.Millisecond)
		var n int
		for len(out.dialed) > 0 {
			if slices.Contains(reachabilityDestinations[:], <-out.dialed) {
				n++
			}
		}
		return n
	}

	refuseRun()
	require.NotZero(t, probes())
	refuseRun()
	require.Zero(t, probes(), "within 30s of the last")
	main.refusalProbedAt.Add(-int64(refusalProbeInterval))
	refuseRun()
	require.NotZero(t, probes(), "30s on")
	require.True(t, main.healthy.Load())
}

func TestAnInterfaceChangeDoesNotReadmitAMemberThatIsDown(t *testing.T) {
	t.Parallel()
	// The change probes every member to measure the new network. On the probe
	// address that lifted a condemnation the heartbeat was careful to keep.
	out := &strandedOutbound{dialed: make(chan M.Socksaddr, 8)}
	member := noInterior(&smartMember{tag: "stranded", outbound: out})
	group := &smartGroup{tag: "us", members: []*smartMember{member}}
	s := &Smart{ctx: context.Background(), logger: logger.NOP(), cache: newSmartCache(context.Background(), "smart"), groups: []*smartGroup{group}}

	s.InterfaceUpdated(context.Background())
	require.Equal(t, reachabilityDestinations[0], <-out.dialed)
	require.False(t, member.healthy.Load())
}

// stalledProbeOutbound holds every dial until the test opens the gate or the
// dial is called off, and counts the probes that got through.
type stalledProbeOutbound struct {
	adapter.Outbound
	gate     chan struct{}
	answered atomic.Int32
}

func (o *stalledProbeOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *stalledProbeOutbound) DialContext(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
	select {
	case <-o.gate:
	case <-ctx.Done():
	}
	if ctx.Err() != nil {
		// Both can be ready at once; a real dial that was called off fails.
		return nil, ctx.Err()
	}
	o.answered.Add(1)
	return &answeringConn{measurement: answering(ms(30), 0)}, nil
}

func TestInterfaceChangesInABurstProbeEachMemberOnce(t *testing.T) {
	t.Parallel()
	// Each change used to start a round of its own while the last was still
	// queued behind the probe slots, so a burst of five probed every member
	// five times over, against networks already gone.
	out := &stalledProbeOutbound{gate: make(chan struct{})}
	var members []*smartMember
	for i := range 10 {
		member := newMember(fmt.Sprint("m", i), ms(30))
		member.outbound = out
		members = append(members, member)
	}
	s := &Smart{
		ctx: context.Background(), logger: logger.NOP(),
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{{tag: "g", members: members}},
	}
	for range 5 {
		s.InterfaceUpdated(context.Background())
	}
	close(out.gate)

	require.Eventually(t, func() bool { return out.answered.Load() >= 10 }, 5*time.Second, time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	require.EqualValues(t, 10, out.answered.Load())
}

func TestARefusalOutlivesAProbeRoundThatWasCalledOff(t *testing.T) {
	t.Parallel()
	// probeTarget consumes the evidence when it picks the real address. An
	// interface change cancelling that probe used to leave the member looking
	// as if it had never refused anything.
	member := newMember("main", ms(130))
	member.outbound = &stalledProbeOutbound{gate: make(chan struct{})}
	member.refused.Store(true)
	s := &Smart{ctx: context.Background(), logger: logger.NOP(), cache: newSmartCache(context.Background(), "smart")}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	destination := probeTarget(member)
	require.Equal(t, reachabilityDestinations[0], destination)
	s.probeMember(ctx, member, destination)
	require.True(t, member.refused.Load())
	require.True(t, member.healthy.Load())
}

// secondOpinionCancellingOutbound refuses the first real address, and the
// round is called off while it asks the second.
type secondOpinionCancellingOutbound struct {
	adapter.Outbound
	cancel context.CancelFunc
}

func (o *secondOpinionCancellingOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *secondOpinionCancellingOutbound) DialContext(ctx context.Context, _ string, destination M.Socksaddr) (net.Conn, error) {
	if destination == reachabilityDestinations[0] {
		return &refusingConn{}, nil
	}
	o.cancel()
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestARefusalOutlivesASecondOpinionThatWasCalledOff(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	member := newMember("main", ms(130))
	member.outbound = &secondOpinionCancellingOutbound{cancel: cancel}
	member.refused.Store(true)
	s := &Smart{ctx: context.Background(), logger: logger.NOP(), cache: newSmartCache(context.Background(), "smart")}

	s.probeMember(ctx, member, probeTarget(member))
	require.True(t, member.refused.Load())
	require.True(t, member.healthy.Load())
}

// abortedConn is a tunnel this process closed while its CONNECT was still
// waiting: the member's own CloseAllConnections failing it with ERR_ABORTED,
// in the shape the naive outbound hands that up.
type abortedConn struct{ refusingConn }

func (c *abortedConn) Measure(context.Context) (naive.ConnMeasurement, error) {
	return naive.ConnMeasurement{}, fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, naive.ErrClosedLocally)
}

// abortingOutbound aborts its first aborts dials under the probe and answers
// every one after, counting them all.
type abortingOutbound struct {
	adapter.Outbound
	aborts int32
	dials  atomic.Int32
}

func (o *abortingOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *abortingOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	if o.dials.Add(1) <= o.aborts {
		return &abortedConn{}, nil
	}
	return &answeringConn{measurement: answering(ms(30), 0)}, nil
}

func TestAProbeItsMemberAbortedIsRetriedOnTheNewSession(t *testing.T) {
	t.Parallel()
	// A network change calls the outbounds in configuration order. With the
	// group listed first, its probes are in flight when each member drops its
	// sessions, and a member measured on the probe address has no second
	// address to ask: it was condemned for a whole heartbeat, and even
	// excused, it would go unmeasured on the network the round was for.
	out := &abortingOutbound{aborts: 1}
	member := &smartMember{tag: "direct", outbound: out}
	member.healthy.Store(true)
	s := &Smart{ctx: context.Background(), logger: logger.NOP(), cache: newSmartCache(context.Background(), "smart")}

	s.probeMembers(context.Background(), []*smartMember{member})
	require.True(t, member.healthy.Load())
	require.EqualValues(t, 2, out.dials.Load(),
		"the abort says the old sessions are gone, so another dial lands on a new one")
	_, measured := member.near()
	require.True(t, measured, "the round meant to measure the new network still does")
}

func TestARefusalOutlivesAProbeThisEndKeptClosing(t *testing.T) {
	t.Parallel()
	// The evidence TestARefusalOutlivesAProbeRoundThatWasCalledOff keeps, lost
	// by the other route to "nobody is waiting": a tunnel closed from this side
	// is no more a reading than a cancelled one, and cross-checking it against
	// a second address spends a dial on nothing.
	out := &abortingOutbound{aborts: math.MaxInt32}
	member := newMember("main", ms(130))
	member.outbound = out
	member.refused.Store(true)
	s := &Smart{ctx: context.Background(), logger: logger.NOP(), cache: newSmartCache(context.Background(), "smart")}

	destination := probeTarget(member)
	require.Equal(t, reachabilityDestinations[0], destination)
	s.probeMember(context.Background(), member, destination)
	require.True(t, member.refused.Load())
	require.True(t, member.healthy.Load())
	require.EqualValues(t, 2, out.dials.Load(), "one retry, and no second opinion on something that is not a reading")
}

// reloadingOutbound refuses its first refusals dials and answers every one
// after, counting them all: a relay whose listener is closed for the moment it
// takes to reload.
type reloadingOutbound struct {
	adapter.Outbound
	refusals int32
	dials    atomic.Int32
}

func (o *reloadingOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *reloadingOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	if o.dials.Add(1) <= o.refusals {
		return nil, E.Cause(naive.ErrNextHopUnreachable, "connection refused")
	}
	return &answeringConn{measurement: answering(ms(30), 0)}, nil
}

func TestAMemberCondemnedByItsConfirmingProbeIsRecheckedBeforeTheHeartbeat(t *testing.T) {
	t.Parallel()
	// A relay being reloaded refuses dials for the gap between closing its
	// listener and opening it again, and the GOAWAY it sent makes the client
	// dial in exactly that gap. The confirming probe lands there too, and the
	// member was condemned until the heartbeat: 40–80 seconds on a node that
	// was back before the verdict was logged, on every reload.
	out := &reloadingOutbound{refusals: 2}
	member := &smartMember{tag: "hk-1", outbound: out}
	member.healthy.Store(true)
	group := &smartGroup{tag: "hk", members: []*smartMember{member}}
	group.selected.Store(member)
	s := &Smart{
		ctx:          context.Background(),
		logger:       logger.NOP(),
		cache:        newSmartCache(context.Background(), "smart"),
		groups:       []*smartGroup{group},
		recheckAfter: 500 * time.Millisecond,
	}

	s.reportFailure("3f2a", M.ParseSocksaddr("example.com:443"), group, member,
		E.Cause(naive.ErrNextHopUnreachable, "connection refused"))

	require.Eventually(t, func() bool { return !member.healthy.Load() }, time.Second, time.Millisecond,
		"the verdict still lands at once; the recheck only lifts it")
	require.EqualValues(t, 2, out.dials.Load(), "both real addresses refused, nothing more yet")
	require.Eventually(t, func() bool { return member.healthy.Load() }, 5*time.Second, time.Millisecond,
		"the recheck finds the relay back and lets the member in without waiting for a heartbeat")
	require.EqualValues(t, 3, out.dials.Load())
}

func TestAMemberThatStaysDownIsRecheckedThreeTimes(t *testing.T) {
	t.Parallel()
	// The rechecks cover the first minute, not the outage: a node that is
	// really dead costs three extra pairs of dials, at 5, 15 and 45 seconds,
	// and then goes back to the heartbeat — or every condemned member would be
	// dialed every few seconds for as long as the outage lasted.
	out := &reloadingOutbound{refusals: math.MaxInt32}
	member := &smartMember{tag: "hk-1", outbound: out}
	member.healthy.Store(true)
	group := &smartGroup{tag: "hk", members: []*smartMember{member}}
	group.selected.Store(member)
	// A recheck that comes due while the probe before it still holds probing
	// is folded into that probe and dials nothing; 20ms was too little room
	// for that under -race.
	s := &Smart{
		ctx:          context.Background(),
		logger:       logger.NOP(),
		cache:        newSmartCache(context.Background(), "smart"),
		groups:       []*smartGroup{group},
		recheckAfter: 100 * time.Millisecond,
	}

	s.reportFailure("3f2a", M.ParseSocksaddr("example.com:443"), group, member,
		E.Cause(naive.ErrNextHopUnreachable, "connection refused"))

	require.Eventually(t, func() bool { return out.dials.Load() == 8 }, 5*time.Second, time.Millisecond,
		"the probe's pair of dials, then a pair for each of the three rechecks")
	time.Sleep(100 * time.Millisecond)
	require.EqualValues(t, 8, out.dials.Load(), "no fourth look before the heartbeat")
	require.False(t, member.healthy.Load())
}

func TestAMemberTheHeartbeatCondemnedIsRecheckedToo(t *testing.T) {
	t.Parallel()
	// Condemned by the heartbeat, or on a blackhole, rather than by a
	// confirming probe: a route that flapped for nine seconds kept its member
	// out for fifty more, until the next heartbeat round.
	out := &reloadingOutbound{refusals: 0}
	member := &smartMember{tag: "hk-1", outbound: out}
	member.healthy.Store(true)
	group := &smartGroup{tag: "hk", members: []*smartMember{member}}
	group.selected.Store(member)
	s := &Smart{
		ctx:          context.Background(),
		logger:       logger.NOP(),
		cache:        newSmartCache(context.Background(), "smart"),
		groups:       []*smartGroup{group},
		recheckAfter: 20 * time.Millisecond,
	}
	t.Cleanup(func() { s.closed.Store(true) })

	s.markDown(member, E.Cause(naive.ErrNextHopUnreachable, "connection refused"))
	require.False(t, member.healthy.Load())
	require.Eventually(t, member.healthy.Load, 5*time.Second, time.Millisecond,
		"the first recheck finds it back, with no heartbeat running")
}

func TestTheRechecksStopOnceTheMemberIsBack(t *testing.T) {
	t.Parallel()
	// A node back after ten seconds — longer than a reload — used to wait out
	// the heartbeat on its backup. The second recheck finds it, and the third
	// has nothing left to do.
	out := &reloadingOutbound{refusals: 4}
	member := &smartMember{tag: "hk-1", outbound: out}
	member.healthy.Store(true)
	group := &smartGroup{tag: "hk", members: []*smartMember{member}}
	group.selected.Store(member)
	s := &Smart{
		ctx:          context.Background(),
		logger:       logger.NOP(),
		cache:        newSmartCache(context.Background(), "smart"),
		groups:       []*smartGroup{group},
		recheckAfter: 20 * time.Millisecond,
	}

	s.reportFailure("3f2a", M.ParseSocksaddr("example.com:443"), group, member,
		E.Cause(naive.ErrNextHopUnreachable, "connection refused"))

	require.Eventually(t, func() bool { return out.dials.Load() == 5 && member.healthy.Load() },
		5*time.Second, time.Millisecond, "the probe's pair, the first recheck's pair, one dial that answered")
	// Past where the third recheck lands: it finds the member up and dials
	// nothing.
	time.Sleep(10 * s.recheckAfter)
	require.EqualValues(t, 5, out.dials.Load())
}

// flappingFixture is a group whose member was condemned once a minute ago and
// let back in, beside another group that is up, so the hold has somewhere for
// the traffic to be in the meantime.
func flappingFixture(out adapter.Outbound) (*Smart, *smartGroup, *smartMember) {
	member := &smartMember{tag: "hk-1", outbound: out}
	member.healthy.Store(true)
	member.downAt.Store(time.Now().Add(-time.Minute).UnixNano())
	group := &smartGroup{tag: "hk", members: []*smartMember{member}}
	group.selected.Store(member)
	spare := newMember("jp-1", ms(50))
	// recheckAfter: see TestAMemberThatStaysDownIsRecheckedThreeTimes.
	s := &Smart{
		ctx:          context.Background(),
		logger:       logger.NOP(),
		cache:        newSmartCache(context.Background(), "smart"),
		groups:       []*smartGroup{group, {tag: "jp", members: []*smartMember{spare}}},
		recheckAfter: 100 * time.Millisecond,
	}
	return s, group, member
}

func TestAMemberCondemnedTwiceInTenMinutesServesAHold(t *testing.T) {
	t.Parallel()
	// Every readmission moves each destination on the group to a different
	// exit address and back again. A node that keeps dropping out did that to
	// them every few minutes, so the second condemnation in a window holds it
	// out for a minute, whatever answers in the meantime.
	s, _, member := flappingFixture(nil)

	s.markDown(member, E.New("connection refused"))
	s.record(member, answering(ms(30), ms(1)))
	s.recordProbe(member, reachabilityDestinations[0], answering(ms(30), ms(1)))
	require.False(t, member.healthy.Load(), "neither traffic nor a probe lets it in during the hold")

	member.downAt.Store(time.Now().Add(-s.flapHold()).UnixNano())
	s.recordProbe(member, reachabilityDestinations[0], answering(ms(30), ms(1)))
	require.True(t, member.healthy.Load(), "the first answer after the hold does")
}

func TestOneCondemnationInTenMinutesIsNotHeld(t *testing.T) {
	t.Parallel()
	s, _, member := flappingFixture(nil)
	member.downAt.Store(time.Now().Add(-flapWindow - time.Second).UnixNano())

	s.markDown(member, E.New("connection refused"))
	s.record(member, answering(ms(30), ms(1)))
	require.True(t, member.healthy.Load(), "the last one was outside the window, so one answer is enough")
}

func TestAHoldIsLiftedWhenNothingElseIsUp(t *testing.T) {
	t.Parallel()
	// With nothing else up, traffic reaches the held member anyway, and a group
	// whose members are all down hands out its first one: held, an answering
	// member would sit behind a dead sibling for the whole hold.
	s, _, member := flappingFixture(nil)
	s.groups[1].members[0].healthy.Store(false)

	s.markDown(member, E.New("connection refused"))
	s.record(member, answering(ms(30), ms(1)))
	require.True(t, member.healthy.Load())
}

func TestAHeldMemberIsRecheckedWhenItsHoldEnds(t *testing.T) {
	t.Parallel()
	// The rechecks inside the hold could not let it in, so there is one, at
	// the end — without it the hold would also cost the heartbeat after it.
	out := &reloadingOutbound{refusals: 2}
	s, group, member := flappingFixture(out)

	before := member.downAt.Load()
	s.reportFailure("3f2a", M.ParseSocksaddr("example.com:443"), group, member,
		E.Cause(naive.ErrNextHopUnreachable, "connection refused"))
	require.Eventually(t, func() bool { return member.downAt.Load() != before }, time.Second, time.Millisecond)
	condemned := time.Unix(0, member.downAt.Load())
	require.Eventually(t, func() bool { return out.dials.Load() == 3 && member.healthy.Load() },
		5*time.Second, time.Millisecond, "the probe at the end of the hold lets it in")
	require.GreaterOrEqual(t, time.Since(condemned), s.flapHold())
	require.EqualValues(t, 3, out.dials.Load(),
		"the probe's pair and the one at the end of the hold; none of the rechecks inside it")
}

func TestAHeldMemberIsStillRecheckedWhenNothingElseIsUp(t *testing.T) {
	t.Parallel()
	// With nothing else up the hold does not apply, so neither does skipping
	// the rechecks inside it: they are what finds the member first.
	out := &reloadingOutbound{refusals: math.MaxInt32}
	s, group, member := flappingFixture(out)
	s.groups[1].members[0].healthy.Store(false)

	s.reportFailure("3f2a", M.ParseSocksaddr("example.com:443"), group, member,
		E.Cause(naive.ErrNextHopUnreachable, "connection refused"))
	require.Eventually(t, func() bool { return out.dials.Load() == 10 }, 5*time.Second, time.Millisecond,
		"the probe, the three rechecks and the one at the end of the hold, a pair each")
	time.Sleep(100 * time.Millisecond)
	require.EqualValues(t, 10, out.dials.Load())
}

// closedConn is a probe whose session was closed under it from either end:
// ERR_CONNECTION_CLOSED, which cronet matches to net.ErrClosed and which is
// not this end's doing by its label.
type closedConn struct{ refusingConn }

func (c *closedConn) Measure(context.Context) (naive.ConnMeasurement, error) {
	return naive.ConnMeasurement{}, fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, net.ErrClosed)
}

// closingOutbound answers its first closes dials with a closed session and
// every one after, counting them all.
type closingOutbound struct {
	adapter.Outbound
	closes int32
	dials  atomic.Int32
}

func (o *closingOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *closingOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	if o.dials.Add(1) <= o.closes {
		return &closedConn{}, nil
	}
	return &answeringConn{measurement: answering(ms(30), 0)}, nil
}

func TestAProbeOnASessionThatClosedUnderItIsRetriedOnce(t *testing.T) {
	t.Parallel()
	// On a network change the streams in flight get one of two answers:
	// ERR_ABORTED, which is ErrClosedLocally and was retried, or
	// ERR_CONNECTION_CLOSED for a stream placed on a session already draining
	// — and three to five of eight resets condemned a healthy member on the
	// second, measured on the probe address with no second address to ask.
	out := &closingOutbound{closes: 1}
	member := &smartMember{tag: "direct", outbound: out}
	member.healthy.Store(true)
	s := &Smart{ctx: context.Background(), logger: logger.NOP(), cache: newSmartCache(context.Background(), "smart")}

	s.probeMembers(context.Background(), []*smartMember{member})
	require.True(t, member.healthy.Load(), "the session was gone; the retry on a fresh one is the reading")
	require.EqualValues(t, 2, out.dials.Load())
	_, measured := member.near()
	require.True(t, measured)
}

func TestAMemberWhoseSessionsKeepClosingIsStillCondemned(t *testing.T) {
	t.Parallel()
	// ERR_CONNECTION_CLOSED is also what a server's FIN reads as, so it is
	// tried again and not excused: a member whose fresh session closes too is
	// condemned on that second reading.
	out := &closingOutbound{closes: math.MaxInt32}
	member := noInterior(&smartMember{tag: "direct", outbound: out})
	member.healthy.Store(true)
	s := &Smart{ctx: context.Background(), logger: logger.NOP(), cache: newSmartCache(context.Background(), "smart")}

	s.probeMembers(context.Background(), []*smartMember{member})
	require.False(t, member.healthy.Load())
	require.EqualValues(t, 2, out.dials.Load(), "one retry, then the verdict")
}

func TestARecheckDueAfterCloseDialsNothing(t *testing.T) {
	t.Parallel()
	// s.ctx is the box's, and an embedder may close this outbound without
	// cancelling it first: the recheck then fired against members whose
	// outbounds were closed too.
	out := &reloadingOutbound{refusals: math.MaxInt32}
	member := &smartMember{tag: "hk-1", outbound: out}
	member.healthy.Store(true)
	group := &smartGroup{tag: "hk", members: []*smartMember{member}}
	group.selected.Store(member)
	s := &Smart{
		ctx:          context.Background(),
		logger:       logger.NOP(),
		cache:        newSmartCache(context.Background(), "smart"),
		groups:       []*smartGroup{group},
		recheckAfter: 500 * time.Millisecond,
	}

	s.reportFailure("3f2a", M.ParseSocksaddr("example.com:443"), group, member,
		E.Cause(naive.ErrNextHopUnreachable, "connection refused"))
	require.Eventually(t, func() bool { return !member.healthy.Load() }, time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return !member.probing.Load() }, time.Second, time.Millisecond)
	dials := out.dials.Load()
	require.NoError(t, s.Close())
	time.Sleep(800 * time.Millisecond)
	require.Equal(t, dials, out.dials.Load(),
		"dialed after Close: the recheck came due after it, unless this test was too slow to close within recheckAfter")
}

// silentOutbound reaches its proxy and gets no answer within a heartbeat's
// budget: what a path saturated by a bulk download looks like to a new stream,
// and equally what a dead exit looks like; only whether payload is still
// arriving tells the two apart.
//
// queue is how long a new stream waits behind that download before the proxy
// answers it, with verdict, or as a tunnel when verdict is nil; zero is never.
// A probe whose budget is at least the queue hears the answer at once, after
// gate opens when there is one: the budget is read off the deadline rather
// than waited out. dials counts the probes on a heartbeat's budget, outwaits
// those on outwait's.
type silentOutbound struct {
	adapter.Outbound
	queue    time.Duration
	verdict  error
	gate     chan struct{}
	dials    atomic.Int32
	outwaits atomic.Int32
}

func (o *silentOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *silentOutbound) DialContext(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
	deadline, _ := ctx.Deadline()
	budget := time.Until(deadline)
	if budget > probeTimeoutCold {
		o.outwaits.Add(1)
		if o.gate != nil {
			select {
			case <-o.gate:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	} else {
		o.dials.Add(1)
	}
	switch {
	case o.queue == 0 || budget < o.queue:
		return &blackholingConn{}, nil
	case o.verdict != nil:
		return &verdictConn{err: o.verdict}, nil
	default:
		return &answeringConn{measurement: answering(ms(30), ms(1))}, nil
	}
}

// verdictConn is a tunnel the proxy answered with err.
type verdictConn struct {
	answeringConn
	err error
}

func (c *verdictConn) Measure(context.Context) (naive.ConnMeasurement, error) {
	return naive.ConnMeasurement{}, c.err
}

func TestAProbeLeftUnansweredDoesNotCondemnAMemberThatIsDelivering(t *testing.T) {
	t.Parallel()
	// Seen in production: one download saturated the path through a group's
	// only member, the confirming probe ran out of its budget, and the member
	// was condemned — so the download moved to the next group and saturated
	// that, and then the next. The member had been delivering all along.
	lines := newRecordingLogger()
	// New streams wait ten seconds behind the download, and outwait's probe
	// is held until the two below are in: its answer ends the episode.
	out := &silentOutbound{queue: 10 * time.Second, gate: make(chan struct{})}
	member := newMember("out-hk-hkt", ms(30))
	member.outbound = out
	member.noteDelivered()
	s := &Smart{
		ctx: context.Background(), logger: lines,
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{{tag: "hk", members: []*smartMember{member}}},
	}

	s.confirmMembers(context.Background(), []*smartMember{member})
	s.confirmMembers(context.Background(), []*smartMember{member})
	require.True(t, member.healthy.Load())
	require.EqualValues(t, 4, out.dials.Load(), "both addresses were still asked, twice")
	require.Len(t, lines.matching("member out-hk-hkt is congested, not condemning: "), 1,
		"once for the episode, not once per probe")
	close(out.gate)
	require.Eventually(t, func() bool { return !member.outwaiting.Load() }, time.Second, time.Millisecond)
	require.True(t, member.healthy.Load(), "answered once it had waited out the queue")

	// A member that has really died stops delivering too. Data that stopped
	// when a probe began is a whole probe budget old when its silence is
	// judged — two seconds at the least — and that is past the grace: the
	// probe has watched the stop.
	require.Less(t, congestionGrace, probeTimeoutFloor)
	member.deliveredAt.Store(time.Now().Add(-probeTimeoutFloor).UnixNano())
	s.confirmMembers(context.Background(), []*smartMember{member})
	require.False(t, member.healthy.Load())
}

func TestOnlySilenceIsExcusedByDelivery(t *testing.T) {
	t.Parallel()
	// A proxy that answered said something about the member, and delivering
	// says nothing against it: 502 from both real addresses, or a 503 about
	// its own next hop, condemns as before.
	refusing, _, refusingMember, _ := egressFixture(func(M.Socksaddr) bool { return false })
	refusingMember.noteDelivered()
	refusing.confirmMembers(context.Background(), []*smartMember{refusingMember})
	require.False(t, refusingMember.healthy.Load())

	stranded := &smartMember{tag: "stranded", outbound: &strandedOutbound{dialed: make(chan M.Socksaddr, 8)}}
	stranded.healthy.Store(true)
	stranded.noteDelivered()
	s := &Smart{ctx: context.Background(), logger: logger.NOP(), cache: newSmartCache(context.Background(), "smart")}
	s.confirmMembers(context.Background(), []*smartMember{stranded})
	require.False(t, stranded.healthy.Load())

	broken := newMember("broken", ms(30))
	broken.noteDelivered()
	s.markDown(broken, E.Cause(naive.ErrNextHopUnreachable, "connection refused"))
	require.False(t, broken.healthy.Load(), "nor does a leg that broke")
}

func TestACongestedMemberIsRecordedInTheTrail(t *testing.T) {
	t.Parallel()
	smart, path := auditedSmart(t)
	member := newMember("out-hk", ms(30))
	member.noteDelivered()
	smart.groups = []*smartGroup{{tag: "hk", members: []*smartMember{member}}}
	silence := fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, context.DeadlineExceeded)

	smart.markDown(member, silence)
	smart.markDown(member, silence)
	smart.recordProbe(member, reachabilityDestinations[0], answering(ms(30), ms(1)))
	smart.markDown(member, silence)
	require.NoError(t, smart.audit.Close())

	records := readTrail(t, path)
	require.Len(t, records, 2, "one per episode; an answered probe ends one")
	for _, record := range records {
		require.Equal(t, auditTypeHealth, record.Type)
		require.Equal(t, "congested", record.To)
		require.Equal(t, "out-hk", record.Groups[0].Member)
		require.Contains(t, record.Groups[0].Failed, context.DeadlineExceeded.Error())
	}
	require.True(t, member.healthy.Load())
}

func TestAnExitThatRefusesThreeDestinationsInARowIsCheckedAtOnce(t *testing.T) {
	t.Parallel()
	// An exit whose egress is gone answers 502 for everything, and the
	// heartbeat that would ask a real address is up to eighty seconds away —
	// each new destination in between paying a round trip to be refused.
	s, group, main, out := egressFixture(func(M.Socksaddr) bool { return false })
	s.reportFailure("k1", M.ParseSocksaddr("a.example:443"), group, main, refused502)
	s.reportFailure("k2", M.ParseSocksaddr("b.example:443"), group, main, refused502)
	s.reportFailure("k2", M.ParseSocksaddr("b.example:80"), group, main, refused502)
	require.Empty(t, out.dialed, "two destinations, whatever the ports, prove nothing yet")

	s.reportFailure("k3", M.ParseSocksaddr("c.example:443"), group, main, refused502)
	require.Eventually(t, func() bool { return !main.healthy.Load() }, time.Second, time.Millisecond,
		"the third asks the real addresses, and both refusing condemns it")
	require.Equal(t, "spare", group.current().tag)
}

func TestARefusalRunIsResetByAnythingGettingThrough(t *testing.T) {
	t.Parallel()
	// An exit that cannot reach some destinations and carries everything else
	// is not one that lost its egress.
	s, group, main, out := egressFixture(func(M.Socksaddr) bool { return false })
	s.reportFailure("k1", M.ParseSocksaddr("a.example:443"), group, main, refused502)
	s.reportFailure("k2", M.ParseSocksaddr("b.example:443"), group, main, refused502)
	s.record(main, answering(ms(130), ms(1)))
	s.reportFailure("k3", M.ParseSocksaddr("c.example:443"), group, main, refused502)
	s.reportFailure("k4", M.ParseSocksaddr("d.example:443"), group, main, refused502)
	require.Empty(t, out.dialed)
	require.True(t, main.healthy.Load())
}

// localOutage builds hk[hk1], jp[jp1], us[us1, us2]. hk1, jp1 and us1 carried
// traffic through two short outages of this end's uplink five minutes apart
// and were condemned in both; us2 is the idle backup of a "first" group, which
// only the heartbeat probes, and the second outage fell between two rounds —
// unless backupAlsoDown.
func localOutage(backupAlsoDown bool) (*Smart, []*smartMember) {
	hk1, jp1 := newMember("hk1", ms(30)), newMember("jp1", ms(50))
	us1, us2 := newMember("us1", ms(150)), newMember("us2", ms(160))
	s := &Smart{
		ctx: context.Background(), logger: logger.NOP(), alpha: testAlpha,
		cache: newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{
			{tag: "hk", members: []*smartMember{hk1}},
			{tag: "jp", members: []*smartMember{jp1}},
			{tag: "us", members: []*smartMember{us1, us2}},
		},
	}
	condemned := []*smartMember{hk1, jp1, us1}
	if backupAlsoDown {
		condemned = append(condemned, us2)
	}
	outage := E.Cause(naive.ErrNextHopUnreachable, "network is unreachable")
	for _, m := range condemned {
		m.downAt.Store(time.Now().Add(-5 * time.Minute).UnixNano())
		s.condemn(m, outage)
	}
	return s, condemned
}

func candidateTags(s *Smart) []string {
	var tags []string
	for _, c := range s.candidates(nil, 0, time.Now()) {
		tags = append(tags, c.tag)
	}
	return tags
}

func TestALocalOutageIsNotHeldAsFlapping(t *testing.T) {
	t.Parallel()
	// The uplink is back and every condemned member answers. Held as
	// flapping, all of them stayed out behind the backup the outage missed.
	s, condemned := localOutage(false)
	for _, m := range condemned {
		s.recordProbe(m, reachabilityDestinations[0], answering(ms(30), ms(1)))
	}
	require.Equal(t, []string{"hk", "jp", "us"}, candidateTags(s),
		"the outage was this end's, not the members'; every group should be back")
}

func TestAfterALocalOutageEveryMemberThatAnswersIsLetIn(t *testing.T) {
	t.Parallel()
	// With every member down the first to answer was let in, and that alone
	// held every other one out.
	s, condemned := localOutage(true)
	for _, m := range condemned {
		s.recordProbe(m, reachabilityDestinations[0], answering(ms(30), ms(1)))
	}
	require.Equal(t, []string{"hk", "jp", "us"}, candidateTags(s))
}

func TestAMemberFlappingAloneIsStillHeld(t *testing.T) {
	t.Parallel()
	// Another group's member condemned long before or after is no common
	// cause: the hold stands.
	s, _, member := flappingFixture(nil)
	s.groups[1].members[0].downAt.Store(time.Now().Add(-2 * flapCommonCause).UnixNano())
	s.markDown(member, E.New("connection refused"))
	s.recordProbe(member, reachabilityDestinations[0], answering(ms(30), ms(1)))
	require.False(t, member.healthy.Load())
}

func TestAHoldFromTheHeartbeatAlsoEndsWithAProbe(t *testing.T) {
	t.Parallel()
	// Condemned for the second time by the heartbeat rather than a confirming
	// probe: only the confirming probe scheduled the probe at the end of the
	// hold, so this one waited out the next heartbeat, 40-80s in production.
	s, _, member := flappingFixture(&answeringOutbound{measurement: answering(ms(30), ms(1))})
	s.recheckAfter = 20 * time.Millisecond // hold 240ms
	s.markDown(member, E.Cause(naive.ErrNextHopUnreachable, "connection refused"))
	require.False(t, member.healthy.Load())
	require.Eventually(t, member.healthy.Load, 4*s.flapHold(), time.Millisecond,
		"the hold ended; a probe should have let it back in")
}

func TestANetworkChangeForgetsTheFlapHistory(t *testing.T) {
	t.Parallel()
	// A phone on a bad Wi-Fi: the member was condemned twice there, then the
	// phone moved to cellular. Both condemnations were about the old path.
	s, _, member := flappingFixture(nil)
	s.markDown(member, E.Cause(naive.ErrNextHopUnreachable, "connection refused"))
	s.InterfaceUpdated(context.Background())
	s.recordProbe(member, reachabilityDestinations[0], answering(ms(30), ms(1)))
	require.True(t, member.healthy.Load(), "the new network's first answer should let it in")
}

// congestedFixture is one delivering member behind out.
func congestedFixture(out adapter.Outbound) (*Smart, *smartMember) {
	member := newMember("hk1", ms(30))
	member.outbound = out
	member.noteDelivered()
	s := &Smart{
		ctx: context.Background(), logger: logger.NOP(),
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{{tag: "hk", members: []*smartMember{member}}},
	}
	return s, member
}

func TestTheCongestionExcuseEndsWithAProbeThatOutwaitsTheQueue(t *testing.T) {
	t.Parallel()
	// A member whose old streams keep delivering while its new ones are
	// never answered — an exit with a full connection table — was excused on
	// every probe for as long as the download lasted: forty rounds, ten
	// minutes at one per fifteen seconds, and still healthy.
	out := &silentOutbound{}
	s, member := congestedFixture(out)
	for range 40 {
		member.noteDelivered()
		s.confirmMembers(context.Background(), []*smartMember{member})
	}
	require.Eventually(t, func() bool { return !member.healthy.Load() }, time.Second, time.Millisecond,
		"thirty seconds per address with no answer is not congestion")
	require.EqualValues(t, 2, out.outwaits.Load(), "one long probe, both addresses")
}

func TestTheLongProbeCondemnsAnExitRefusingBothAddressesBehindTheQueue(t *testing.T) {
	t.Parallel()
	// The relay dials for five seconds before it answers 502, past a
	// heartbeat's budget, so every routine probe hears silence and is excused.
	out := &silentOutbound{queue: 5 * time.Second, verdict: refused502}
	s, member := congestedFixture(out)
	s.confirmMembers(context.Background(), []*smartMember{member})
	require.Eventually(t, func() bool { return !member.healthy.Load() }, time.Second, time.Millisecond)
	require.EqualValues(t, 2, out.outwaits.Load(), "both addresses refused")
}

func TestALongProbeOutlivedByItsEpisodeCondemnsNothing(t *testing.T) {
	t.Parallel()
	// The heartbeat heard the member answer while the long probe was still
	// out, which ended the episode; the long probe then came back refused
	// on both addresses, an exit whose reach to them differs from moment to
	// moment. What it heard is older than that answer.
	out := &silentOutbound{queue: 5 * time.Second, verdict: refused502, gate: make(chan struct{})}
	s, member := congestedFixture(out)
	s.confirmMembers(context.Background(), []*smartMember{member})
	require.Eventually(t, func() bool { return out.outwaits.Load() == 1 }, time.Second, time.Millisecond)
	s.recordProbe(member, reachabilityDestinations[0], answering(ms(30), ms(1)))
	require.False(t, member.congested.Load(), "the answer ended the episode")
	close(out.gate)
	require.Eventually(t, func() bool { return !member.outwaiting.Load() }, time.Second, time.Millisecond)
	require.True(t, member.healthy.Load())
}

// selfAnsweringOutbound answers the proxy's own probe address at once, as a
// relay does without dialing, and is silentOutbound for everything else.
type selfAnsweringOutbound struct{ *silentOutbound }

func (o *selfAnsweringOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if naive.IsProbe(destination) {
		return &answeringConn{measurement: answering(ms(30), ms(1))}, nil
	}
	return o.silentOutbound.DialContext(ctx, network, destination)
}

func TestAHeartbeatThatNeverLeavesTheProxyDoesNotEndACongestionEpisode(t *testing.T) {
	t.Parallel()
	// An exit that has stopped answering new connections while its old ones
	// deliver: the long probe hears nothing, but the heartbeat, aimed at the
	// proxy's own address for a member with no interior, was answered in the
	// meantime and ended the episode, so the verdict was dropped (end to end:
	// two of three runs never condemned the member in five minutes).
	out := &selfAnsweringOutbound{&silentOutbound{gate: make(chan struct{})}}
	s, member := congestedFixture(out)
	for range medianQuorum {
		s.recordProbe(member, reachabilityDestinations[0], answering(ms(30), ms(1)))
	}
	require.True(t, naive.IsProbe(probeTarget(member)))
	member.noteDelivered()
	s.confirmMembers(context.Background(), []*smartMember{member})
	require.True(t, member.congested.Load())
	require.Eventually(t, func() bool { return out.outwaits.Load() == 1 }, time.Second, time.Millisecond)
	member.noteDelivered()
	s.probeMembers(context.Background(), []*smartMember{member})
	close(out.gate)
	require.Eventually(t, func() bool { return !member.outwaiting.Load() }, time.Second, time.Millisecond)
	require.False(t, member.healthy.Load(), "the proxy answering itself says nothing about its exit")
}

// calledOffOnceOutbound calls the first long probe (and probeVerdict's retry
// of it) off from this end; later ones hear nothing.
type calledOffOnceOutbound struct {
	*silentOutbound
	long atomic.Int32
}

func (o *calledOffOnceOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if deadline, _ := ctx.Deadline(); time.Until(deadline) > probeTimeoutCold && o.long.Add(1) <= 2 {
		o.outwaits.Add(1)
		return &verdictConn{err: naive.ErrClosedLocally}, nil
	}
	return o.silentOutbound.DialContext(ctx, network, destination)
}

func TestALongProbeCalledOffLeavesTheNextExcuseItsOwn(t *testing.T) {
	t.Parallel()
	out := &calledOffOnceOutbound{silentOutbound: &silentOutbound{}}
	s, member := congestedFixture(out)
	member.noteDelivered()
	s.confirmMembers(context.Background(), []*smartMember{member})
	require.Eventually(t, func() bool { return out.long.Load() >= 2 && !member.outwaiting.Load() }, time.Second, time.Millisecond)
	require.True(t, member.healthy.Load(), "called off is no verdict")
	member.noteDelivered()
	s.confirmMembers(context.Background(), []*smartMember{member})
	require.Eventually(t, func() bool { return !member.healthy.Load() }, time.Second, time.Millisecond,
		"the second excuse started no long probe")
}

// nullRoutingOutbound is silentOutbound behind an exit that drops one address,
// taking settle to do it, and recording the budget each address was given.
type nullRoutingOutbound struct {
	*silentOutbound
	dropped M.Socksaddr
	settle  time.Duration
	access  sync.Mutex
	budgets map[M.Socksaddr]time.Duration
}

func (o *nullRoutingOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	deadline, _ := ctx.Deadline()
	o.access.Lock()
	o.budgets[destination] = time.Until(deadline)
	o.access.Unlock()
	if destination == o.dropped {
		time.Sleep(o.settle)
		return &blackholingConn{}, nil
	}
	return o.silentOutbound.DialContext(ctx, network, destination)
}

func TestTheLongProbeGivesTheSecondAddressItsOwnTime(t *testing.T) {
	t.Parallel()
	// Behind a ten-second queue, on an exit that drops 1.1.1.1: the first
	// address says nothing however long it is given. Had the two shared one
	// budget, the second would get what the first left — nothing on a real
	// blackhole — and a member still delivering would be condemned on one
	// address, which is what two addresses exist to prevent.
	out := &nullRoutingOutbound{
		silentOutbound: &silentOutbound{queue: 10 * time.Second},
		dropped:        reachabilityDestinations[0],
		settle:         200 * time.Millisecond,
		budgets:        make(map[M.Socksaddr]time.Duration),
	}
	s, member := congestedFixture(out)
	s.confirmMembers(context.Background(), []*smartMember{member})
	require.Eventually(t, func() bool { return !member.outwaiting.Load() && !member.congested.Load() },
		time.Second, time.Millisecond, "the second address answered once the queue let it through")
	require.True(t, member.healthy.Load())
	out.access.Lock()
	defer out.access.Unlock()
	require.Greater(t, out.budgets[reachabilityDestinations[1]], settleTimeout-out.settle/2,
		"the second address is given as long as the first was")
}

func TestALateAnswerToTheLongProbeIsNotAMeasurement(t *testing.T) {
	t.Parallel()
	// Ten seconds of queue are not ten seconds of distance, and the chain
	// window drops only its worst sample of eight.
	out := &silentOutbound{queue: 10 * time.Second, gate: make(chan struct{})}
	s, member := congestedFixture(out)
	member.record(measured(ms(30))) // two interior readings: one more and the chain counts
	s.confirmMembers(context.Background(), []*smartMember{member})
	require.Eventually(t, func() bool { return out.outwaits.Load() == 1 }, time.Second, time.Millisecond)

	// A new episode while the long probe is out starts no second one.
	s.recordProbe(member, naive.ProbeDestination(), answering(ms(30), 0))
	member.noteDelivered()
	s.confirmMembers(context.Background(), []*smartMember{member})
	require.True(t, member.congested.Load())

	close(out.gate)
	require.Eventually(t, func() bool { return !member.outwaiting.Load() && !member.congested.Load() },
		time.Second, time.Millisecond, "the answer ends the episode")
	require.EqualValues(t, 1, out.outwaits.Load())
	require.True(t, member.healthy.Load())
	_, hasChain := member.chain()
	require.False(t, hasChain, "and is not banked")
}

func TestTheLongProbesVerdictIsDroppedOnceTheMemberWasJudgedAgain(t *testing.T) {
	t.Parallel()
	// Condemned for a refusal and let back in while the long probe waited:
	// the silence it heard is older than the answer that let it in.
	out := &silentOutbound{gate: make(chan struct{})}
	s, member := congestedFixture(out)
	s.confirmMembers(context.Background(), []*smartMember{member})
	require.Eventually(t, func() bool { return out.outwaits.Load() == 1 }, time.Second, time.Millisecond)

	s.markDown(member, E.Cause(naive.ErrNextHopUnreachable, "connection refused"))
	require.False(t, member.healthy.Load())
	s.recordProbe(member, reachabilityDestinations[0], answering(ms(30), ms(1)))
	require.True(t, member.healthy.Load())

	close(out.gate)
	require.Eventually(t, func() bool { return !member.outwaiting.Load() }, time.Second, time.Millisecond)
	require.True(t, member.healthy.Load())
}

// datagramOutbound carries UDP over a real uot.Conn on a pipe, and answers a
// TCP probe at once.
type datagramOutbound struct {
	adapter.Outbound
	conn *uot.Conn
}

func (o *datagramOutbound) Network() []string { return []string{N.NetworkTCP, N.NetworkUDP} }

func (o *datagramOutbound) DialContext(_ context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
	if network == N.NetworkUDP {
		return o.conn, nil
	}
	return &answeringConn{measurement: answering(ms(30), ms(1))}, nil
}

func (o *datagramOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return o.conn, nil
}

// datagramFixture is a delivering-capable member whose UDP flows run over a
// pipe; send puts one datagram on the far end.
func datagramFixture(t *testing.T) (s *Smart, member *smartMember, destination M.Socksaddr, send func()) {
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close(); server.Close() })
	destination = M.ParseSocksaddr("203.0.113.9:443")
	member = newMember("hk1", ms(30))
	member.outbound = &datagramOutbound{conn: uot.NewConn(client, uot.Request{IsConnect: true, Destination: destination})}
	s = &Smart{
		ctx: context.Background(), logger: logger.NOP(), alpha: testAlpha,
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{{tag: "hk", members: []*smartMember{member}}},
	}
	send = func() {
		go func() {
			payload := []byte("quic payload")
			_ = binary.Write(server, binary.BigEndian, uint16(len(payload)))
			_, _ = server.Write(payload)
		}()
	}
	return s, member, destination, send
}

var probeSilence = fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, context.DeadlineExceeded)

func TestDatagramsArrivingCountAsDelivering(t *testing.T) {
	t.Parallel()
	// A QUIC download over udp_over_tcp saturating the member's path: the
	// flow keeps reading, and new CONNECTs go unanswered behind it. Read as
	// silence from a member delivering nothing, the probe condemned it, and the
	// condemnation cut the download.
	s, member, destination, send := datagramFixture(t)
	flow, err := s.ListenPacket(context.Background(), destination)
	require.NoError(t, err)
	waiter, created := bufio.CreatePacketReadWaiter(bufio.NewPacketConn(flow))
	require.True(t, created, "the copy loop still reads through uot's own read waiter")
	waiter.InitializeReadWaiter(N.ReadWaitOptions{})
	send()
	buffer, _, err := waiter.WaitReadPacket()
	require.NoError(t, err)
	buffer.Release()
	require.True(t, member.delivering(time.Now()))

	s.markDown(member, probeSilence)
	require.True(t, member.healthy.Load(), "congested, not dead")
	send()
	buffer, _, err = waiter.WaitReadPacket()
	require.NoError(t, err, "and the download it was carrying is not cut")
	buffer.Release()
}

func TestDatagramsOnAConnectedFlowCountAsDelivering(t *testing.T) {
	t.Parallel()
	// The same over DialContext, which the router reads as a stream.
	s, member, destination, send := datagramFixture(t)
	flow, err := s.DialContext(context.Background(), N.NetworkUDP, destination)
	require.NoError(t, err)
	send()
	n, err := flow.Read(make([]byte, 64))
	require.NoError(t, err)
	require.Positive(t, n)
	require.True(t, member.delivering(time.Now()))
}

func TestAMemberIsProbedBeforeTheRelayDropsItsIdleSession(t *testing.T) {
	t.Parallel()
	// Rounds come up to heartbeatInterval+heartbeatJitter apart, and the
	// relay (naive inbound IdleTimeout) closes a session idle for 120s; the
	// pool trusts one for 110s. Skipped for traffic 59s before a round, a
	// member could sit silent for 139s, and the first request after a pause
	// paid for a new session.
	member := newMember("a-1", ms(10))
	member.outbound = &answeringOutbound{measurement: measured(ms(10))}
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		groups: []*smartGroup{{tag: "a", members: []*smartMember{member}}},
	}
	now := time.Now()
	worstNextRound := heartbeatInterval + heartbeatJitter
	for idle := time.Second; idle < 2*heartbeatInterval; idle += time.Second {
		member.lastUsed.Store(now.Add(-idle).UnixNano())
		if len(smart.dueForHeartbeat(now)) == 0 {
			require.LessOrEqual(t, idle+worstNextRound, 110*time.Second,
				"skipped %s after its last use, it could sit silent until the relay drops its session", idle)
		}
	}
	member.lastUsed.Store(now.Add(-10 * time.Second).UnixNano())
	require.Empty(t, smart.dueForHeartbeat(now), "a member busy a moment ago still needs no heartbeat")
}
