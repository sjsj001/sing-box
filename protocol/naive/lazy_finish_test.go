package naive

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing/common/logger"
	"github.com/stretchr/testify/require"
)

// afterFinish is a writeResponse that behaves the way net/http does with an
// HTTP/2 ResponseWriter: using it after the handler returned is not an error,
// it is a panic.
type afterFinish struct {
	access   sync.Mutex
	finished bool
	writes   int
}

func (w *afterFinish) write(int, ConnectAck, bool) error {
	w.access.Lock()
	defer w.access.Unlock()
	if w.finished {
		panic("Header called after Handler finished")
	}
	w.writes++
	return nil
}

func (w *afterFinish) handlerReturned() {
	w.access.Lock()
	defer w.access.Unlock()
	w.finished = true
}

func newTestResponder(writer *afterFinish) *responder {
	_, timing := dialer.WithConnectTiming(context.Background())
	return newResponder(context.Background(), logger.NOP(), timing, writer.write)
}

func TestALateAnswerIsDroppedRatherThanWritten(t *testing.T) {
	t.Parallel()
	// The relay case: the answer comes from a goroutine waiting on the next
	// hop, and on a request that has already ended it arrives after the
	// handler returned. Writing then took the whole process down.
	writer := &afterFinish{}
	r := newTestResponder(writer)

	r.finish()
	writer.handlerReturned()

	require.NotPanics(t, func() {
		require.Error(t, r.settle(http.StatusOK, ConnectAck{}, false),
			"a response with nowhere to go is an error, not a write")
	})
	require.Zero(t, writer.writes)
}

func TestAnAnswerInTimeIsStillWritten(t *testing.T) {
	t.Parallel()
	// The guard must not cost the ordinary case its response.
	writer := &afterFinish{}
	r := newTestResponder(writer)

	require.NoError(t, r.settle(http.StatusOK, ConnectAck{}, false))
	require.Equal(t, 1, writer.writes)

	// And finish over the top of a settled responder changes nothing.
	r.finish()
	writer.handlerReturned()
	require.Equal(t, 1, writer.writes)
}

func TestFinishWaitsOutAWriteAlreadyUnderWay(t *testing.T) {
	t.Parallel()
	// The window the flag alone would not close: a write that began before the
	// handler returned must complete before it does, or it straddles the
	// return and panics anyway.
	writer := &afterFinish{}
	started := make(chan struct{})
	release := make(chan struct{})
	_, timing := dialer.WithConnectTiming(context.Background())
	r := newResponder(context.Background(), logger.NOP(), timing,
		func(statusCode int, ack ConnectAck, hasAck bool) error {
			close(started)
			<-release
			return writer.write(statusCode, ack, hasAck)
		})

	go func() { _ = r.settle(http.StatusOK, ConnectAck{}, false) }()
	<-started

	finished := make(chan struct{})
	go func() {
		r.finish()
		writer.handlerReturned()
		close(finished)
	}()

	select {
	case <-finished:
		t.Fatal("finish returned while a write was still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("finish never returned")
	}
	require.Equal(t, 1, writer.writes)
}

func TestSettlingAndFinishingConcurrentlyNeverWritesLate(t *testing.T) {
	t.Parallel()
	// Both orders happen in production, and the losing one must be the write.
	for range 200 {
		writer := &afterFinish{}
		r := newTestResponder(writer)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = r.settle(http.StatusOK, ConnectAck{}, false)
		}()
		go func() {
			defer wg.Done()
			r.finish()
			writer.handlerReturned()
		}()
		wg.Wait()
		require.LessOrEqual(t, writer.writes, 1)
	}
}

func TestAConnectionServedInProcessAnswersInsteadOfHanging(t *testing.T) {
	t.Parallel()
	// Every path that dials reports a handshake before data can flow, but the
	// paths that serve a connection inside this process never dial and never
	// report: a hijacked DNS query, a dns outbound. Held against a report that
	// cannot come, the first write back blocks until the request times out and
	// the client gets nothing at all — for every DNS query through the tunnel.
	writer := &afterFinish{}
	r := newTestResponder(writer)

	done := make(chan error, 1)
	go func() { done <- r.awaitServed() }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("a write on a connection nothing will settle must answer, not hang")
	}
	require.Equal(t, 1, writer.writes)
}

func TestARelayStillWaitsForItsNextHopsAnswer(t *testing.T) {
	t.Parallel()
	// The one path that is legitimately unsettled while data could move: the
	// answer is being resolved from the next hop. A write must wait for the
	// real status rather than settling a plain 200 over it.
	writer := &afterFinish{}
	r := newTestResponder(writer)
	r.resolving.Store(true)

	done := make(chan error, 1)
	go func() { done <- r.awaitServed() }()

	select {
	case <-done:
		t.Fatal("a relay's write must not settle the answer out from under the resolver")
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, r.settle(http.StatusServiceUnavailable, ConnectAck{}, false))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("the write must be released once the next hop answered")
	}
	require.Equal(t, 1, writer.writes, "and exactly one answer reaches the client")
}

func TestAnAckCarriesWhatTheLookupCost(t *testing.T) {
	t.Parallel()
	// The server half of the wiring. Without this the resolution never reaches
	// the wire and the client silently goes back to charging it to the node.
	_, timing := dialer.WithConnectTiming(context.Background())
	timing.RecordDNS(50 * time.Millisecond)
	timing.RecordDial(60 * time.Millisecond)
	r := newResponder(context.Background(), logger.NOP(), timing,
		func(int, ConnectAck, bool) error { return nil })

	ack, ok := r.spans()
	require.True(t, ok)
	require.True(t, ack.HasConnect)
	require.Equal(t, 10*time.Millisecond, ack.Connect)
	require.True(t, ack.HasResolve, "the lookup has to reach the wire")
	require.Equal(t, 50*time.Millisecond, ack.Resolve)
	require.Contains(t, FormatConnectAck(ack), ";n=50000")
}

func TestARelayPassesTheInnerLookupThroughToItsClient(t *testing.T) {
	t.Parallel()
	// The relay half. A relay's own lookup found a proxy, so only what the next
	// hop reported may be handed on as the destination's.
	_, timing := dialer.WithConnectTiming(context.Background())
	timing.RecordDNS(40 * time.Millisecond)
	timing.RecordDial(45 * time.Millisecond)
	r := newResponder(context.Background(), logger.NOP(), timing,
		func(int, ConnectAck, bool) error { return nil })

	inner, innerOK := ParseConnectAck("120000;d=8000;n=3000")
	require.True(t, innerOK)
	timing.RecordInnerAck(inner.Connect)
	require.True(t, inner.HasResolve)
	timing.RecordInnerResolution(inner.Resolve)

	ack, ok := r.spans()
	require.True(t, ok)
	require.Equal(t, 8*time.Millisecond, ack.Connect)
	require.Equal(t, 3*time.Millisecond, ack.Resolve,
		"the next hop's lookup, never this hop's own")
}

// A 502 is consumed downstream as a replay licence: proof that any early
// payload died at a proxy, so the client may send the request again through
// another group. Granting it for an error nobody classified sends one request
// twice, and nothing else would notice.
func TestOnlyAVerdictAboutTheDestinationLicensesAReplay(t *testing.T) {
	t.Parallel()
	require.Equal(t, http.StatusBadGateway, statusForError(fmt.Errorf("%w: refused", ErrDestinationUnreachable)))
	require.Equal(t, http.StatusServiceUnavailable, statusForError(fmt.Errorf("%w: down", ErrNextHopUnreachable)))
	require.Equal(t, http.StatusBadGateway, statusForError(errors.New("connection refused")),
		"unclassified only from this hop's own dial toward the destination, where it is a verdict about it")
}

type answeredReporter struct{ err error }

func (r answeredReporter) RemoteAck(context.Context) (ConnectAck, bool, error) {
	return ConnectAck{}, false, r.err
}

func TestARelayLicensesAReplayOnlyWhenTheNextHopDid(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		err    error
		status int
	}{
		"next hop refused the destination": {fmt.Errorf("%w: 502", ErrDestinationUnreachable), http.StatusBadGateway},
		"next hop is down":                 {fmt.Errorf("%w: 503", ErrNextHopUnreachable), http.StatusServiceUnavailable},
		// The payload may still be on one of this hop's legs.
		"conversation broke off": {errors.New("stream reset"), http.StatusServiceUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var status int
			_, timing := dialer.WithConnectTiming(context.Background())
			r := newResponder(context.Background(), logger.NOP(), timing,
				func(code int, _ ConnectAck, _ bool) error { status = code; return nil })
			r.settleFromNextHop(answeredReporter{test.err})
			require.Equal(t, test.status, status)
		})
	}
}

// gatedReporter is a next hop that answers once released — the way a real one
// answers when its status arrives, or when the copy loop closes the conn.
type gatedReporter struct {
	net.Conn
	release chan struct{}
	err     error
}

func (r *gatedReporter) RemoteAck(ctx context.Context) (ConnectAck, bool, error) {
	select {
	case <-r.release:
		return ConnectAck{}, false, r.err
	case <-ctx.Done():
		return ConnectAck{}, false, ctx.Err()
	}
}

// relayResponder is a responder whose answer is being resolved from a next hop
// that has not answered yet. It records the one status written.
func relayResponder(t *testing.T, ctx context.Context, nextHop error) (*responder, *gatedReporter, *int, *int) {
	t.Helper()
	var status, writes int
	_, timing := dialer.WithConnectTiming(context.Background())
	r := newResponder(ctx, logger.NOP(), timing, func(code int, _ ConnectAck, _ bool) error {
		status = code
		writes++
		return nil
	})
	reporter := &gatedReporter{release: make(chan struct{}), err: nextHop}
	require.NoError(t, r.success(reporter))
	return r, reporter, &status, &writes
}

// On a relay the copy loop sees the next hop's failure too, raw, and reported
// it as a handshake failure that raced the resolver: whichever settled first
// answered, and the raw one answered 502 — a verdict about the destination,
// which cooled its port for the whole group at the client, five times in six,
// while the member stayed up. The resolver classifies, and the raw report has
// to wait for it: a next hop that is down is a 503, and one that answered 502
// itself still a 502.
func TestACopyLoopFailureOnARelayIsAnsweredByTheResolver(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		nextHop error
		status  int
	}{
		"next hop is down":                 {fmt.Errorf("%w: connection refused", ErrNextHopUnreachable), http.StatusServiceUnavailable},
		"next hop refused the destination": {fmt.Errorf("%w: 502", ErrDestinationUnreachable), http.StatusBadGateway},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r, reporter, status, writes := relayResponder(t, context.Background(), test.nextHop)

			failed := make(chan error, 1)
			go func() { failed <- r.failure(errors.New("connection refused")) }()
			select {
			case <-failed:
				t.Fatal("the raw failure answered before the resolver")
			case <-time.After(50 * time.Millisecond):
			}
			require.Zero(t, *writes)

			close(reporter.release)
			select {
			case err := <-failed:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("the failure must return once the resolver answered")
			}
			require.Equal(t, test.status, *status)
			require.Equal(t, 1, *writes)
		})
	}
}

// A probe is a CONNECT with no payload, and on a relay whose next hop is down
// its handler returned before the resolver had answered: finish raised the
// flag, the resolver's 503 was dropped, and net/http answered the handler's
// silence with a 200. The client read a relay with no way out as reachable,
// three times in four. The handler has to wait for the resolver.
func TestFinishWaitsForTheResolversAnswer(t *testing.T) {
	t.Parallel()
	r, reporter, status, writes := relayResponder(t, context.Background(), fmt.Errorf("%w: connection refused", ErrNextHopUnreachable))

	finished := make(chan struct{})
	go func() {
		r.finish()
		close(finished)
	}()
	select {
	case <-finished:
		t.Fatal("finish returned before the resolver answered")
	case <-time.After(50 * time.Millisecond):
	}

	close(reporter.release)
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("finish must return once the resolver answered")
	}
	require.Equal(t, http.StatusServiceUnavailable, *status)
	require.Equal(t, 1, *writes)
}

// The wait must not outlive the client: one that hung up cancels the request
// context, and a handler held for a resolver nobody is listening to would be
// a goroutine per abandoned request. The resolver sees the same cancellation
// and may still answer first; that write is legal, and not the point here.
func TestFinishLetsAClientThatHungUpGo(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	r, _, _, _ := relayResponder(t, ctx, fmt.Errorf("%w: connection refused", ErrNextHopUnreachable))
	cancel()

	finished := make(chan struct{})
	go func() {
		r.finish()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("finish must not wait on a resolver after the client hung up")
	}
}

// measuredNextHop is a next hop that answers after delay, over a stream whose
// own setup took setup of that delay.
type measuredNextHop struct {
	delay time.Duration
	setup time.Duration
}

func (h measuredNextHop) RemoteAck(context.Context) (ConnectAck, bool, error) {
	time.Sleep(h.delay)
	return ConnectAck{Total: 20 * time.Millisecond, Connect: 8 * time.Millisecond, HasConnect: true}, true, nil
}

func (h measuredNextHop) Measure(context.Context) (ConnMeasurement, error) {
	return ConnMeasurement{Setup: h.setup}, nil
}

func (measuredNextHop) WaitReady(context.Context) error { return nil }

func relaySpan(t *testing.T, nextHop measuredNextHop) ConnectAck {
	t.Helper()
	_, timing := dialer.WithConnectTiming(context.Background())
	timing.RecordDial(time.Millisecond)
	var sent ConnectAck
	r := newResponder(context.Background(), logger.NOP(), timing, func(_ int, ack ConnectAck, hasAck bool) error {
		require.True(t, hasAck)
		sent = ack
		return nil
	})
	r.settleFromNextHop(nextHop)
	return sent
}

// A relay whose stream to the next hop waited for a new session reported the
// handshake inside its span, and the client put it in the chain window, where
// a burst of cold streams moved a group off its exit. The span leaves it out,
// so a cold stream reports the chain a warm one does.
func TestARelayLeavesTheNextHopsHandshakeOutOfItsSpan(t *testing.T) {
	t.Parallel()
	const handshake = 500 * time.Millisecond
	warm := make(chan ConnectAck, 1)
	go func() { warm <- relaySpan(t, measuredNextHop{delay: 50 * time.Millisecond}) }()
	cold := relaySpan(t, measuredNextHop{delay: 50*time.Millisecond + handshake, setup: handshake})
	warmAck := <-warm
	require.InDelta(t, float64(warmAck.Total), float64(cold.Total), float64(150*time.Millisecond),
		"cold %v, warm %v", cold.Total, warmAck.Total)
	require.Equal(t, 8*time.Millisecond, cold.Connect)

	// A setup that cannot have fit is not taken out: below the destination
	// leg the client would drop the whole report.
	bogus := relaySpan(t, measuredNextHop{delay: 50 * time.Millisecond, setup: time.Hour})
	require.GreaterOrEqual(t, bogus.Total, 50*time.Millisecond)
}
