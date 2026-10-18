package listener

import (
	"context"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/proxyproto"
	"github.com/sagernet/sing-box/log"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

// scriptedListener answers Accept from a script: an error, or a connection.
type scriptedListener struct {
	net.Listener
	script   []any
	accepted []time.Time
	shutdown *Listener
}

func (l *scriptedListener) Accept() (net.Conn, error) {
	l.accepted = append(l.accepted, time.Now())
	if len(l.script) == 0 {
		l.shutdown.shutdown.Store(true)
		return nil, net.ErrClosed
	}
	next := l.script[0]
	l.script = l.script[1:]
	if err, isError := next.(error); isError {
		return nil, err
	}
	return next.(net.Conn), nil
}

func (l *scriptedListener) Close() error { return nil }

type closingHandler struct{ got chan net.Conn }

func (h closingHandler) NewConnection(_ context.Context, conn net.Conn, _ adapter.InboundContext, _ N.CloseHandlerFunc) {
	conn.Close()
	h.got <- conn
}

// Out of descriptors, Linux keeps the connection queued and fails the next
// accept at once: without a backoff the loop spun, writing half a million
// error lines a second. The delay doubles from 5ms, and an accept that
// succeeds resets it, as in net/http.
func TestAnAcceptOutOfDescriptorsBacksOff(t *testing.T) {
	exhausted := &net.OpError{Op: "accept", Err: syscall.EMFILE}
	require.True(t, exhausted.Temporary())
	server, client := net.Pipe()
	defer client.Close()
	handler := closingHandler{got: make(chan net.Conn, 1)}
	l := &Listener{ctx: context.Background(), logger: log.NewNOPFactory().Logger(), disableLog: true, connHandler: handler}
	fake := &scriptedListener{script: []any{exhausted, exhausted, exhausted, server, exhausted}, shutdown: l}
	l.tcpListener = fake

	l.loopTCPIn()

	require.Equal(t, server, <-handler.got, "the connection after the failures is still served")
	require.Len(t, fake.accepted, 6)
	gaps := make([]time.Duration, 0, 5)
	for i := 1; i < len(fake.accepted); i++ {
		gaps = append(gaps, fake.accepted[i].Sub(fake.accepted[i-1]))
	}
	require.GreaterOrEqual(t, gaps[0], 5*time.Millisecond)
	require.GreaterOrEqual(t, gaps[1], 10*time.Millisecond)
	require.GreaterOrEqual(t, gaps[2], 20*time.Millisecond)
	require.Less(t, gaps[3], 20*time.Millisecond, "a successful accept is not delayed")
	require.GreaterOrEqual(t, gaps[4], 5*time.Millisecond)
	require.Less(t, gaps[4], 40*time.Millisecond, "the delay starts over after a success, not at the next doubling")
}

// closedListener fails every Accept as closed, as a listener does once the
// branch for a lasting error has closed it, without a shutdown under way.
type closedListener struct {
	net.Listener
	accepts int
}

func (l *closedListener) Accept() (net.Conn, error) {
	l.accepts++
	if l.accepts > 100 {
		panic("still accepting a closed listener")
	}
	return nil, net.ErrClosed
}

func (l *closedListener) Close() error { return nil }

// The branch for a lasting error closes the listener and continues; the next
// Accept then fails as closed, and with no shutdown flagged the loop went
// round again at once, logging "tcp listener closed" without end.
func TestAClosedListenerEndsTheLoop(t *testing.T) {
	l := &Listener{ctx: context.Background(), logger: log.NewNOPFactory().Logger(), disableLog: true}
	fake := &closedListener{}
	l.tcpListener = fake
	l.loopTCPIn()
	require.Equal(t, 1, fake.accepts)
}

// A bad PROXY header reports itself temporary, but it is one client's: waiting
// on it would let a client sending garbage slow every accept.
func TestABadProxyHeaderDoesNotSlowAccepting(t *testing.T) {
	badHeader := &proxyproto.Error{}
	l := &Listener{ctx: context.Background(), logger: log.NewNOPFactory().Logger(), disableLog: true}
	fake := &scriptedListener{script: []any{badHeader, badHeader, badHeader}, shutdown: l}
	l.tcpListener = fake
	l.loopTCPIn()
	require.Len(t, fake.accepted, 4)
	require.Less(t, fake.accepted[3].Sub(fake.accepted[0]), 5*time.Millisecond)
}
