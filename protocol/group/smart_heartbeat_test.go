package group

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/protocol/naive"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/stretchr/testify/require"
)

func TestMarkDownIgnoresAProbeThisEndGaveUpOn(t *testing.T) {
	t.Parallel()
	// A condemnation arrives through two doors — reportFailure for a caller's
	// dial, markDown for a probe — and a line drawn at only one of them is the
	// door a healthy node still gets taken out of service through. Both a
	// cancelled probe and one whose tunnel this side closed say nothing about
	// the member.
	smart := &Smart{logger: logger.NOP()}
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
	member := &smartMember{tag: "stranded", outbound: out}
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
	member := &smartMember{tag: "stranded", outbound: out}
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
	member := &smartMember{tag: "stranded", outbound: out}
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
	member := &smartMember{tag: "warm", outbound: out}
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
	<-ctx.Done()
	// The shape the real classifier produces: everything that is not a 502 is
	// filed under ErrNextHopUnreachable, silence included, and no cronet
	// HandshakeError is attached because there was no status to attach.
	return naive.ConnMeasurement{}, fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, ctx.Err())
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
	member := &smartMember{tag: "stranded", outbound: out}
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
