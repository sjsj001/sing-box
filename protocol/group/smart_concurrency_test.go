package group

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
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

// A snapshot is shared by every connection to the same destination, and those
// run concurrently by definition — one page load opens eight at once. Anything
// mutated on it has to be safe for that.
func TestDestinationEntryIsSafeUnderConcurrentUse(t *testing.T) {
	t.Parallel()
	entry := &destinationEntry{
		Remote:  map[string]remoteWindow{"hk": {time.Millisecond}, "jp": {time.Millisecond}},
		RacedAt: time.Now(),
	}
	tags := []string{"hk", "jp", "sg", "de"}

	var wg sync.WaitGroup
	for i := range 64 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tag := tags[i%len(tags)]
			now := time.Now()
			switch i % 4 {
			case 0:
				entry.block(tag, now)
			case 1:
				entry.blocked(tag, now)
			case 2:
				entry.selected()
			default:
				entry.selectGroup(tag)
			}
		}(i)
	}
	wg.Wait()
}

// settleConn is a member connection that fails to answer, and counts how often
// it is asked.
type settleConn struct {
	net.Conn
	measures atomic.Int32
}

func (c *settleConn) Read([]byte) (int, error)        { return 0, io.EOF }
func (c *settleConn) Write(p []byte) (int, error)     { return len(p), nil }
func (c *settleConn) Close() error                    { return nil }
func (c *settleConn) WaitReady(context.Context) error { return nil }

func (c *settleConn) Measure(context.Context) (naive.ConnMeasurement, error) {
	c.measures.Add(1)
	// Not a definite failure, so settling stops here instead of failing over —
	// which keeps this test about single-flight rather than about routing.
	return naive.ConnMeasurement{}, io.ErrUnexpectedEOF
}

func TestSettleRunsOnceUnderConcurrentReadAndWrite(t *testing.T) {
	t.Parallel()
	// The two copy directions run at the same time and both reach settle. If it
	// is not single-flight, both perform the failover: two connections are
	// dialed, one is silently leaked, and the swap happens twice.
	stub := &settleConn{}
	member := newMember("m", ms(30))
	conn := &replayConn{
		Conn:        stub,
		smart:       &Smart{ctx: context.Background(), logger: logger.NOP()},
		destination: M.ParseSocksaddr("example.com:443"),
		key:         "example.com",
		group:       &smartGroup{tag: "g", members: []*smartMember{member}},
		member:      member,
		exclude:     map[string]bool{"g": true},
	}

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = conn.settle()
		}()
	}
	wg.Wait()

	require.Equal(t, int32(1), stub.measures.Load(),
		"settle must run once however many callers reach it")
}

func TestLateProbeConnectionsAreClosed(t *testing.T) {
	t.Parallel()
	// A race stops as soon as the outcome is decided, but the probes it walked
	// away from may already have answered into the buffer. Nobody reads those,
	// so unless they are drained their tunnels stay open — one leaked
	// connection per race, against a destination that is about to be used a lot.
	results := make(chan probeOutcome, 4)
	var closed atomic.Int32
	for range 3 {
		results <- probeOutcome{tag: "late", conn: &countingConn{closed: &closed}}
	}
	drainProbes(results, 3)
	require.Equal(t, int32(3), closed.Load())
}

func TestProbesAnsweringAfterTheRaceAreStillClosed(t *testing.T) {
	t.Parallel()
	// The ones that matter are not sitting in the buffer when the race walks
	// away — they are still dialing, and they answer afterwards. A drain that
	// only takes what is already buffered collects none of them.
	results := make(chan probeOutcome, 2)
	var closed atomic.Int32

	done := make(chan struct{})
	go func() {
		defer close(done)
		drainProbes(results, 2)
	}()

	for range 2 {
		go func() {
			time.Sleep(10 * time.Millisecond)
			results <- probeOutcome{tag: "late", conn: &countingConn{closed: &closed}}
		}()
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the drain must wait for the probes it is responsible for")
	}
	require.Equal(t, int32(2), closed.Load())
}

// crowdingOutbound records the most dials it ever had open at once.
type crowdingOutbound struct {
	adapter.Outbound
	hold     time.Duration
	dials    atomic.Int32
	inFlight atomic.Int32
	peak     atomic.Int32
}

func (o *crowdingOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *crowdingOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	o.dials.Add(1)
	open := o.inFlight.Add(1)
	defer o.inFlight.Add(-1)
	for {
		peak := o.peak.Load()
		if open <= peak || o.peak.CompareAndSwap(peak, open) {
			break
		}
	}
	time.Sleep(o.hold)
	return &answeringConn{measurement: answering(ms(30), 0)}, nil
}

func TestProbesAreBoundedSoTheyDoNotMeasureTheirOwnContention(t *testing.T) {
	t.Parallel()
	// Unbounded, every pool of every member opens at the same moment: on a small
	// router that is dozens of TLS handshakes competing for the same cores, and
	// the handshake that comes back then measures the contention rather than the
	// distance. Since the handshake bounds how close a member may claim to be, a
	// starved one is filed as further away than it is.
	out := &crowdingOutbound{hold: 15 * time.Millisecond}
	var members []*smartMember
	for range 16 {
		member := &smartMember{tag: "m", outbound: out}
		member.healthy.Store(true)
		members = append(members, member)
	}
	smart := &Smart{ctx: context.Background(), logger: logger.NOP()}

	smart.probeMembers(context.Background(), members)

	require.Equal(t, int32(16), out.dials.Load(), "every member still gets dialed")
	require.LessOrEqual(t, out.peak.Load(), int32(probeConcurrency),
		"but never more than the bound at once")
}

func TestProbingAnOutboundBuiltWithoutItsConstructorDoesNotHang(t *testing.T) {
	t.Parallel()
	// The bound lives in a channel, and a send on a nil one blocks for ever
	// rather than failing — a hang instead of an error, on exactly the paths
	// nothing exercises.
	out := &crowdingOutbound{}
	member := &smartMember{tag: "m", outbound: out}
	member.healthy.Store(true)
	smart := &Smart{ctx: context.Background(), logger: logger.NOP()}
	require.Nil(t, smart.probeSlots, "the case is only interesting while it is unset")

	done := make(chan struct{})
	go func() {
		defer close(done)
		smart.probeMembers(context.Background(), []*smartMember{member})
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("probing blocked on an uninitialised bound")
	}
	require.Equal(t, int32(1), out.dials.Load())
}

func TestARaceReportSaysWhyAGroupDroppedOut(t *testing.T) {
	t.Parallel()
	// This line is the diagnosis. Without it, telling a pruned group from one
	// whose timings were rejected means recomputing every score by hand from a
	// dozen scattered lines — which is how the starved-handshake failure stayed
	// hidden through two investigations.
	report := raceReport{
		alpha: testAlpha,
		candidates: []candidate{
			{tag: "hk", local: 28727 * time.Microsecond, hasLocal: true},
			{tag: "jp", local: 149360 * time.Microsecond, hasLocal: true, bonus: 50 * time.Millisecond},
			{tag: "us", local: 133218 * time.Microsecond, hasLocal: true},
			{tag: "eu"},
		},
		remote:    map[string]time.Duration{"hk": 900 * time.Microsecond},
		failures:  map[string]error{"us": errors.New("reported no destination timing")},
		winner:    "hk",
		hasWinner: true,
		elapsed:   50 * time.Millisecond,
	}
	line := report.String()

	require.Contains(t, line, "won=hk")
	// The winner shows both legs and the score they produced.
	require.Contains(t, line, "hk local=28.7ms remote=900µs score=21ms")
	// A group that was waited for and never answered shows the bound it was
	// judged against — 0.7×149.36 − 50 = 54.6ms, well above the winner's 21ms,
	// which is the whole explanation.
	require.Contains(t, line, "jp local=149.4ms bonus=50ms bound=54.6ms unanswered")
	// One that answered with something unusable says so instead.
	require.Contains(t, line, "us local=133.2ms bound=93.3ms failed=reported no destination timing")
	// And one that has never been measured is not dressed up as though it had.
	require.Contains(t, line, "eu local=? unanswered")
}

type countingConn struct {
	net.Conn
	closed *atomic.Int32

	access        sync.Mutex
	received      []byte
	readDeadline  time.Time
	writeDeadline time.Time
}

func (c *countingConn) SetDeadline(t time.Time) error {
	c.access.Lock()
	defer c.access.Unlock()
	c.readDeadline, c.writeDeadline = t, t
	return nil
}

func (c *countingConn) SetReadDeadline(t time.Time) error {
	c.access.Lock()
	defer c.access.Unlock()
	c.readDeadline = t
	return nil
}

func (c *countingConn) SetWriteDeadline(t time.Time) error {
	c.access.Lock()
	defer c.access.Unlock()
	c.writeDeadline = t
	return nil
}

func (c *countingConn) deadlines() (read time.Time, write time.Time) {
	c.access.Lock()
	defer c.access.Unlock()
	return c.readDeadline, c.writeDeadline
}

func (c *countingConn) Close() error {
	c.closed.Add(1)
	return nil
}

func (c *countingConn) Read([]byte) (int, error) { return 0, io.EOF }

func (c *countingConn) Write(p []byte) (int, error) {
	c.access.Lock()
	defer c.access.Unlock()
	c.received = append(c.received, p...)
	return len(p), nil
}

func (c *countingConn) text() string {
	c.access.Lock()
	defer c.access.Unlock()
	return string(c.received)
}

// refusingConn is a tunnel the proxy refuses, on a schedule the test controls.
type refusingConn struct {
	net.Conn
	refuseAt chan struct{}
}

func (c *refusingConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *refusingConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *refusingConn) Close() error                     { return nil }
func (c *refusingConn) WaitReady(context.Context) error  { return nil }
func (c *refusingConn) SetDeadline(time.Time) error      { return nil }
func (c *refusingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *refusingConn) SetWriteDeadline(time.Time) error { return nil }

func (c *refusingConn) Measure(ctx context.Context) (naive.ConnMeasurement, error) {
	if c.refuseAt != nil {
		// The real one blocks on the proxy's answer under the caller's context,
		// so this one has to as well — a stub that ignores cancellation cannot
		// be used to test that cancellation arrives.
		select {
		case <-c.refuseAt:
		case <-ctx.Done():
			return naive.ConnMeasurement{}, ctx.Err()
		}
	}
	return naive.ConnMeasurement{},
		fmt.Errorf("%w: %w", naive.ErrDestinationUnreachable, net.ErrClosed)
}

// gatedOutbound hands out a prepared connection, optionally not until the test
// says so — which is how a test gets inside the window where the tunnel has been
// condemned but its replacement is not up yet.
type gatedOutbound struct {
	adapter.Outbound
	gate chan struct{}
	conn net.Conn
}

func (o *gatedOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *gatedOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	if o.gate != nil {
		<-o.gate
	}
	return o.conn, nil
}

// failoverFixture is a smart outbound with a condemned connection on group "a"
// and one usable group "b" to move it to.
func failoverFixture(t *testing.T, dialGate chan struct{}, refuseAt chan struct{}) (*replayConn, *countingConn) {
	t.Helper()
	var closed atomic.Int32
	replacement := &countingConn{closed: &closed}

	condemned := newMember("a-1", ms(30))
	condemned.outbound = &gatedOutbound{}
	spare := newMember("b-1", ms(40))
	spare.outbound = &gatedOutbound{gate: dialGate, conn: replacement}

	groupA := &smartGroup{tag: "a", members: []*smartMember{condemned}}
	groupB := &smartGroup{tag: "b", members: []*smartMember{spare}}
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		alpha:  testAlpha,
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{groupA, groupB},
	}
	return &replayConn{
		Conn:        &refusingConn{refuseAt: refuseAt},
		smart:       smart,
		destination: M.ParseSocksaddr("example.com:443"),
		key:         "example.com",
		group:       groupA,
		member:      condemned,
		exclude:     map[string]bool{"a": true},
	}, replacement
}

func TestWritesDuringAFailoverReachTheReplacement(t *testing.T) {
	t.Parallel()
	// The window is real: the tunnel has been condemned, the replacement is
	// still being dialed, and a write arriving now used to go onto the doomed
	// tunnel, be reported as sent, and never be replayed.
	dialGate := make(chan struct{})
	conn, replacement := failoverFixture(t, dialGate, nil)

	settled := make(chan error, 1)
	go func() { settled <- conn.settle() }()

	// Wait for the settle to be inside the dial, which is where the flag it sets
	// starts diverting writes into the replay buffer.
	require.Eventually(t, func() bool {
		conn.access.Lock()
		defer conn.access.Unlock()
		return conn.failingOver
	}, time.Second, time.Millisecond)

	written := make(chan error, 1)
	go func() {
		_, err := conn.Write([]byte("world"))
		written <- err
	}()
	// The write must not be answered before the bytes have somewhere to go.
	select {
	case err := <-written:
		t.Fatalf("write reported before the replacement existed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(dialGate)
	require.NoError(t, <-settled)
	require.NoError(t, <-written)
	require.Equal(t, "world", replacement.text(),
		"a write that raced the failover must be replayed onto the new tunnel")
}

func TestClosingDuringAFailoverDoesNotLeakTheReplacement(t *testing.T) {
	t.Parallel()
	// Close takes the tunnel being abandoned. Without a record that it happened,
	// the failover still in flight installs a replacement that nothing will ever
	// close.
	dialGate := make(chan struct{})
	conn, replacement := failoverFixture(t, dialGate, nil)

	go func() { _ = conn.settle() }()
	require.Eventually(t, func() bool {
		conn.access.Lock()
		defer conn.access.Unlock()
		return conn.failingOver
	}, time.Second, time.Millisecond)

	require.NoError(t, conn.Close())
	close(dialGate)

	require.Eventually(t, func() bool {
		return replacement.closed.Load() == 1
	}, time.Second, time.Millisecond,
		"a replacement dialed for a connection nobody owns any more must be closed")
}

func TestSettleHonoursTheCallersReadDeadline(t *testing.T) {
	t.Parallel()
	// The wait for the proxy's answer happens inside the first Read. Running it
	// on the outbound's lifetime context makes SetReadDeadline silently do
	// nothing, and leaves a proxy that never answers holding the read open.
	conn, _ := failoverFixture(t, nil, make(chan struct{})) // never refuses, never answers
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(50*time.Millisecond)))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = conn.Read(make([]byte, 1))
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the read ignored the deadline the caller set")
	}
}

func TestOneOutageIsOneConfirmingProbe(t *testing.T) {
	t.Parallel()
	// Every connection in flight to a node that has just died fails at roughly
	// the same moment. Without collapsing them, one outage becomes one probe per
	// failed connection — all of them to a node already known to be down, each
	// holding a dial open for its whole budget, at the moment the outage is
	// already generating load.
	out := &crowdingOutbound{hold: 200 * time.Millisecond}
	member := &smartMember{tag: "out-jp-rfc", outbound: out}
	member.healthy.Store(true)
	group := &smartGroup{tag: "jp", members: []*smartMember{member}}
	group.selected.Store(member)
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{group},
	}

	broken := E.Cause(naive.ErrNextHopUnreachable, "connection closed")
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			smart.reportFailure("3f2a", M.ParseSocksaddr("apple.com:443"), group, member, broken)
		}()
	}
	wg.Wait()
	require.True(t, member.healthy.Load(),
		"an instant failure is verified before it condemns; the probe's verdict does that")

	// Asserted while that probe is still running, which is when the storm would
	// have been in flight.
	require.Eventually(t, func() bool { return out.dials.Load() > 0 }, time.Second, time.Millisecond)
	require.Equal(t, int32(1), out.dials.Load(), "thirty-two failures, one probe")
}

// resetByNetwork stands in for cronet's error type, which reports several
// genuine network failures as matching net.ErrClosed. See naive.ErrClosedLocally.
type resetByNetwork struct{}

func (resetByNetwork) Error() string        { return "connection reset" }
func (resetByNetwork) Is(target error) bool { return target == net.ErrClosed }

func TestATunnelThisEndClosedDoesNotSpendAProbe(t *testing.T) {
	t.Parallel()
	// A client that opens a connection speculatively and drops it before the
	// proxy has answered produces a failed dial that is entirely this side's
	// doing. On a preconnect host that is the ordinary case rather than the
	// exception — 51 of 63 dials to one CDN name on a live deployment — and each
	// one bought a confirming probe to establish that a node this process had
	// hung up on was still there.
	out := &crowdingOutbound{hold: 200 * time.Millisecond}
	member := &smartMember{tag: "out-us-dmit", outbound: out}
	member.healthy.Store(true)
	group := &smartGroup{tag: "us", members: []*smartMember{member}}
	group.selected.Store(member)
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{group},
	}

	// Wrapped as it actually arrives. The classification stays on it — a local
	// close still has to reach a relay as a hop-level failure, or the status it
	// is answered with reads downstream as a licence to replay the payload — so
	// only the cause underneath says who closed it.
	abandoned := fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, naive.ErrClosedLocally)
	for range 8 {
		smart.reportFailure("3f2a", M.ParseSocksaddr("apple.com:443"), group, member, abandoned)
	}
	require.True(t, member.healthy.Load())
	require.Never(t, func() bool { return out.dials.Load() > 0 },
		100*time.Millisecond, 10*time.Millisecond,
		"nothing was learned about the member, so there is nothing to confirm")

	// The trap the sentinel exists to avoid, per naive.ErrClosedLocally: judged
	// on net.ErrClosed this would have been excused too, and a proxy the network
	// has just reset is exactly what the confirming probe is for.
	reset := fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, resetByNetwork{})
	require.True(t, errors.Is(reset, net.ErrClosed), "the trap only exists if this holds")
	require.False(t, errors.Is(reset, naive.ErrClosedLocally))
	smart.reportFailure("3f2a", M.ParseSocksaddr("apple.com:443"), group, member, reset)
	require.Eventually(t, func() bool { return out.dials.Load() > 0 },
		time.Second, time.Millisecond, "a reset connection still has to be verified")
}

func TestABlackholedLegCondemnsOnSightAndAnInstantFailureDoesNot(t *testing.T) {
	t.Parallel()
	// The split follows from what retrying costs, not from how bad the error
	// looks. A blackholed leg makes every attempt pay the whole timeout, so
	// waiting for a probe's verdict means every connection in the meantime hangs
	// for the full budget. An instant failure costs one round trip to pay again
	// — and over one real day, ten of fifteen condemnations were a single dead
	// pooled connection ("use of closed network connection") on a node that was
	// fine, each condemnation flapping the group onto its backup and poisoning
	// whatever raced in the window.
	out := &crowdingOutbound{hold: 200 * time.Millisecond}
	member := &smartMember{tag: "out-jp-rfc", outbound: out}
	member.healthy.Store(true)
	group := &smartGroup{tag: "jp", members: []*smartMember{member}}
	group.selected.Store(member)
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{group},
	}

	stale := E.Cause(naive.ErrNextHopUnreachable, "use of closed network connection")
	smart.reportFailure("3f2a", M.ParseSocksaddr("apple.com:443"), group, member, stale)
	require.True(t, member.healthy.Load(), "a stale pooled connection is not a dead node")

	blackholed := fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, errLegTimedOut)
	smart.reportFailure("3f2a", M.ParseSocksaddr("apple.com:443"), group, member, blackholed)
	require.False(t, member.healthy.Load(), "a blackholed leg is condemned before the probe returns")
}

func TestAMemberIsProbedAgainOnTheNextFailure(t *testing.T) {
	t.Parallel()
	// Collapsing a burst must not become "probed once and then left to the
	// heartbeat". A group with nothing healthy still offers a member to try, and
	// until the verdict is in every connection reaching for it pays the timeout
	// again — so once the probe is done, the next failure starts another.
	out := &crowdingOutbound{}
	member := &smartMember{tag: "out-jp-rfc", outbound: out}
	member.healthy.Store(true)
	group := &smartGroup{tag: "jp", members: []*smartMember{member}}
	group.selected.Store(member)
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{group},
	}
	broken := E.Cause(naive.ErrNextHopUnreachable, "connection closed")

	smart.reportFailure("3f2a", M.ParseSocksaddr("apple.com:443"), group, member, broken)
	require.Eventually(t, func() bool { return out.dials.Load() == 1 }, time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return !member.probing.Load() }, time.Second, time.Millisecond)

	// Still down, and asked again: it gets probed again.
	smart.reportFailure("3f2a", M.ParseSocksaddr("apple.com:443"), group, member, broken)
	require.Eventually(t, func() bool { return out.dials.Load() == 2 }, time.Second, time.Millisecond)
}

func TestAReadDeadlineDoesNotBecomeAWriteDeadlineAcrossAFailover(t *testing.T) {
	t.Parallel()
	// The caller set a read deadline; the swap must hand the replacement exactly
	// that. Applied as SetDeadline it also installs a write deadline the caller
	// never set — and once that instant passes, every write on the replacement
	// fails with a timeout on a connection the caller believes has none.
	conn, replacement := failoverFixture(t, nil, nil)
	deadline := time.Now().Add(time.Minute)
	require.NoError(t, conn.SetReadDeadline(deadline))

	// The read settles, swaps to the replacement, and then reads it — which
	// yields the stub's EOF. The EOF is proof the swap happened: a failed settle
	// surfaces the condemnation error instead.
	_, err := conn.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "the failover swap is expected to succeed")

	read, write := replacement.deadlines()
	require.Equal(t, deadline, read, "the read deadline belongs to the connection")
	require.True(t, write.IsZero(), "no write deadline was ever set")
}

func TestADeadlineIsNotTakenAsProofTheDestinationGotNothing(t *testing.T) {
	t.Parallel()
	// Only a failure that proves the destination received nothing may be
	// replayed, or a request arrives twice. The handshake classifier wraps every
	// non-502 failure as "next hop unreachable", including a settle bounded by
	// the caller's own read deadline running out while a slow destination was
	// still being dialed — the proxy may deliver moments later. definiteFailure
	// is the class check; the proof itself is provenUndelivered's job.
	// Built exactly as awaitReady and the handshake classifier build it.
	wrapped := func(err error) error {
		return fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, err)
	}
	require.False(t, definiteFailure(wrapped(context.DeadlineExceeded)),
		"a deadline expiring says nothing about what the destination received")
	require.False(t, definiteFailure(wrapped(context.Canceled)))

	// The derived bound on establishing the leg is this outbound's own verdict
	// rather than the caller losing interest, so its class qualifies.
	require.True(t, definiteFailure(wrapped(errLegTimedOut)))
	// And the proxy saying so outright is unambiguous.
	require.True(t, definiteFailure(fmt.Errorf("%w: %w", naive.ErrDestinationUnreachable, E.New("502"))))
}

// brokenTunnelConn is a tunnel whose stream breaks after establishment, and
// that can testify whether it carried payload — exactly what the real tunnel
// reports through naive.PayloadCarrier. With writeSucceeds it models early
// data going onto the wire before the proxy's answer; without it, a write
// still blocked on the establishment gate, which fails without carrying.
//
// It deliberately collapses the concurrency into single-threaded calls, which
// keeps the tests about the replay decision. The interleaving itself — a
// write parked inside the tunnel while the settle condemns it — is
// gatedTunnelConn's job, below.
type brokenTunnelConn struct {
	net.Conn
	writeSucceeds bool
	carried       atomic.Bool
	closedCount   atomic.Int32
}

func (c *brokenTunnelConn) Read([]byte) (int, error) { return 0, io.EOF }

func (c *brokenTunnelConn) Write(p []byte) (int, error) {
	if !c.writeSucceeds {
		return 0, net.ErrClosed
	}
	if len(p) > 0 {
		c.carried.Store(true)
	}
	return len(p), nil
}

func (c *brokenTunnelConn) Close() error                     { c.closedCount.Add(1); return nil }
func (c *brokenTunnelConn) WaitReady(context.Context) error  { return nil }
func (c *brokenTunnelConn) CarriedPayload() bool             { return c.carried.Load() }
func (c *brokenTunnelConn) SetDeadline(time.Time) error      { return nil }
func (c *brokenTunnelConn) SetReadDeadline(time.Time) error  { return nil }
func (c *brokenTunnelConn) SetWriteDeadline(time.Time) error { return nil }

func (c *brokenTunnelConn) Measure(context.Context) (naive.ConnMeasurement, error) {
	// How a stream that broke before the CONNECT answer surfaces: the
	// classifier's catch-all wrap around the transport error.
	return naive.ConnMeasurement{}, fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, io.ErrUnexpectedEOF)
}

// brokenTunnelFixture is failoverFixture with the condemned tunnel supplied by
// the test, which is how a test controls what the tunnel says it carried. The
// condemned member has no outbound, so the confirming probe the failure report
// starts has nothing to dial and the test stays about the replay decision.
func brokenTunnelFixture(t *testing.T, tunnel net.Conn) (*replayConn, *countingConn) {
	t.Helper()
	var closed atomic.Int32
	replacement := &countingConn{closed: &closed}

	condemned := newMember("a-1", ms(30))
	spare := newMember("b-1", ms(40))
	spare.outbound = &gatedOutbound{conn: replacement}

	groupA := &smartGroup{tag: "a", members: []*smartMember{condemned}}
	groupB := &smartGroup{tag: "b", members: []*smartMember{spare}}
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		alpha:  testAlpha,
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{groupA, groupB},
	}
	return &replayConn{
		Conn:        tunnel,
		smart:       smart,
		destination: M.ParseSocksaddr("example.com:443"),
		key:         "example.com",
		group:       groupA,
		member:      condemned,
		exclude:     map[string]bool{"a": true},
	}, replacement
}

func TestATunnelThatCarriedPayloadIsNotReplayed(t *testing.T) {
	t.Parallel()
	// The opening write goes onto the tunnel as early data, before the proxy has
	// answered the CONNECT. A stream that came up, carried that write and then
	// broke arrives here wearing the same "next hop unreachable" wrap as one
	// that never came up — but the proxy may already have forwarded the payload,
	// and replaying it would deliver the request twice. The tunnel's own
	// testimony is what tells the two apart.
	tunnel := &brokenTunnelConn{writeSucceeds: true}
	conn, replacement := brokenTunnelFixture(t, tunnel)

	_, writeErr := conn.Write([]byte("POST /order"))
	require.NoError(t, writeErr)

	err := conn.settle()
	require.ErrorIs(t, err, naive.ErrNextHopUnreachable,
		"a tunnel that carried payload must surface the failure, not hide it behind a replay")
	require.GreaterOrEqual(t, tunnel.closedCount.Load(), int32(1),
		"the condemned tunnel is closed before it is judged")
	require.Empty(t, replacement.text(), "nothing may be replayed onto another group")

	_, readErr := conn.Read(make([]byte, 1))
	require.ErrorIs(t, readErr, naive.ErrNextHopUnreachable,
		"the caller gets the failure and retries itself; no swap happened")
}

func TestATunnelThatCarriedNothingIsReplayed(t *testing.T) {
	t.Parallel()
	// The same failure on a tunnel that never carried a byte is proof the
	// destination saw nothing, and the buffered opening write moves to another
	// group. The write fails on the establishment gate here, exactly like a
	// write still blocked on a leg that never came up.
	tunnel := &brokenTunnelConn{writeSucceeds: false}
	conn, replacement := brokenTunnelFixture(t, tunnel)

	written, writeErr := conn.Write([]byte("hello"))
	require.NoError(t, writeErr, "the failover inside the write is the retry")
	require.Equal(t, len("hello"), written)

	require.Equal(t, "hello", replacement.text(),
		"the opening write is replayed onto the replacement")
	require.GreaterOrEqual(t, tunnel.closedCount.Load(), int32(1),
		"the condemned tunnel is closed before the replacement is dialed")
}

// gatedTunnelConn models the real tunnel's write path faithfully: Write blocks
// on an establishment gate, the carried record and Close share one mutex, and
// a write that passes the gate re-checks closed under that mutex — so once
// Close has returned, CarriedPayload is final. This is the invariant
// provenUndelivered leans on, reproduced honestly enough to race against.
type gatedTunnelConn struct {
	net.Conn
	// measureRefused releases Measure's failure — the test's handle on when
	// the settle is allowed to condemn the tunnel.
	measureRefused chan struct{}

	mu      sync.Mutex
	carried bool
	closed  bool
	gate    chan struct{}
	closeCh chan struct{}

	waiting     atomic.Int32
	closedCount atomic.Int32
}

func newGatedTunnelConn() *gatedTunnelConn {
	return &gatedTunnelConn{
		measureRefused: make(chan struct{}),
		gate:           make(chan struct{}),
		closeCh:        make(chan struct{}),
	}
}

func (c *gatedTunnelConn) Read([]byte) (int, error) { return 0, io.EOF }

func (c *gatedTunnelConn) Write(p []byte) (int, error) {
	c.waiting.Add(1)
	defer c.waiting.Add(-1)
	select {
	case <-c.gate:
	case <-c.closeCh:
		return 0, net.ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	if len(p) > 0 {
		c.carried = true
	}
	return len(p), nil
}

func (c *gatedTunnelConn) Close() error {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		close(c.closeCh)
	}
	c.mu.Unlock()
	c.closedCount.Add(1)
	return nil
}

func (c *gatedTunnelConn) CarriedPayload() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.carried
}

func (c *gatedTunnelConn) WaitReady(context.Context) error  { return nil }
func (c *gatedTunnelConn) SetDeadline(time.Time) error      { return nil }
func (c *gatedTunnelConn) SetReadDeadline(time.Time) error  { return nil }
func (c *gatedTunnelConn) SetWriteDeadline(time.Time) error { return nil }

func (c *gatedTunnelConn) Measure(ctx context.Context) (naive.ConnMeasurement, error) {
	select {
	case <-c.measureRefused:
	case <-ctx.Done():
		return naive.ConnMeasurement{}, ctx.Err()
	}
	return naive.ConnMeasurement{}, fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, io.ErrUnexpectedEOF)
}

func TestAWriteBlockedOnTheGateIsReleasedByTheSettleAndReplayedOnce(t *testing.T) {
	t.Parallel()
	// The interleaving the whole judgement order exists for: the opening write
	// is parked inside the tunnel's Write, blocked on the establishment gate,
	// while the settle fails on the read side. The settle closes the tunnel
	// before judging it — the parked write comes back with ErrClosed instead
	// of slipping onto the wire, the tunnel testifies it carried nothing, and
	// the buffered bytes reach the replacement exactly once.
	tunnel := newGatedTunnelConn()
	conn, replacement := brokenTunnelFixture(t, tunnel)

	go func() { _, _ = conn.Read(make([]byte, 1)) }()
	written := make(chan error, 1)
	go func() {
		_, err := conn.Write([]byte("hello"))
		written <- err
	}()

	// The write must be inside the tunnel, parked on the gate, before the
	// settle is allowed to fail — that is what makes this the interleaving
	// rather than a write that already returned.
	require.Eventually(t, func() bool { return tunnel.waiting.Load() == 1 },
		time.Second, time.Millisecond)
	close(tunnel.measureRefused)

	require.NoError(t, <-written, "the failover inside the settle is the retry")
	require.Equal(t, "hello", replacement.text(),
		"the buffered write reaches the replacement exactly once")
	require.False(t, tunnel.CarriedPayload(),
		"the settle's close must beat the parked write to the stream")
	require.GreaterOrEqual(t, tunnel.closedCount.Load(), int32(1))
}

func TestAGateRacingTheSettleNeverDoublesTheWrite(t *testing.T) {
	t.Parallel()
	// Whichever side wins — the gate releasing the write onto the condemned
	// tunnel, or the settle closing it first — the bytes must end up on at
	// most one tunnel. Which one is timing; both is the double delivery this
	// whole mechanism exists to rule out.
	for range 50 {
		tunnel := newGatedTunnelConn()
		conn, replacement := brokenTunnelFixture(t, tunnel)

		go func() { _, _ = conn.Read(make([]byte, 1)) }()
		written := make(chan struct{})
		go func() {
			_, _ = conn.Write([]byte("x"))
			close(written)
		}()
		go close(tunnel.gate)
		close(tunnel.measureRefused)
		<-written

		carried := tunnel.CarriedPayload()
		flushed := replacement.text() != ""
		require.False(t, carried && flushed,
			"the write landed on the condemned tunnel and was still replayed")
	}
}

func TestReplayRequiresProofTheTunnelCarriedNothing(t *testing.T) {
	t.Parallel()
	// provenUndelivered is the proof half of the replay decision: the error's
	// class claims non-delivery, the tunnel's testimony confirms or denies it.
	wrapped := func(err error) error {
		return fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, err)
	}
	carried := &brokenTunnelConn{writeSucceeds: true}
	_, _ = carried.Write([]byte("x"))
	require.False(t, provenUndelivered(wrapped(io.ErrUnexpectedEOF), carried),
		"payload on the wire means non-delivery cannot be proven")
	// Even the establishment bound is not believed over the tunnel's testimony:
	// the bound can fire and the leg come up a moment later, releasing a write.
	require.False(t, provenUndelivered(wrapped(errLegTimedOut), carried))

	idle := &brokenTunnelConn{}
	require.True(t, provenUndelivered(wrapped(io.ErrUnexpectedEOF), idle),
		"a tunnel that carried nothing proves the destination saw nothing")

	// The proxy answering "destination unreachable" is proof by itself:
	// whatever the tunnel carried died at the proxy.
	require.True(t, provenUndelivered(fmt.Errorf("%w: %w", naive.ErrDestinationUnreachable, E.New("502")), carried))

	// A tunnel that cannot testify: only the establishment bound — which fires
	// before writes can pass the gate — still counts as proof.
	require.True(t, provenUndelivered(wrapped(errLegTimedOut), &refusingConn{}))
	require.False(t, provenUndelivered(wrapped(io.ErrUnexpectedEOF), &refusingConn{}))
}
