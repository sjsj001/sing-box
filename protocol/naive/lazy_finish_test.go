package naive

import (
	"context"
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
