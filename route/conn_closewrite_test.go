package route

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

type errorRecorder struct {
	log.ContextLogger
	access sync.Mutex
	lines  []string
}

func (l *errorRecorder) ErrorContext(_ context.Context, args ...any) {
	l.access.Lock()
	defer l.access.Unlock()
	l.lines = append(l.lines, fmt.Sprint(args...))
}

// noHalfClose stands in for what smart's replayConn reports when its tunnel
// cannot half-close: a wrapper the copy loop's unwrapping stops at.
type noHalfClose struct{ net.Conn }

func (noHalfClose) CloseWrite() error { return errors.ErrUnsupported }

type fixedDialer struct {
	N.Dialer
	conn net.Conn
}

func (d *fixedDialer) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return d.conn, nil
}

// Behind a selector, the outbound is an interrupt.Conn, which the copy loop
// unwraps, over a connection whose CloseWrite reports that it cannot
// half-close. A client ending its connection there is how a connection ends,
// and was logged as an error.
func TestAClientEndingAConnectionBehindAGroupIsNotAnError(t *testing.T) {
	logger := &errorRecorder{ContextLogger: log.NewNOPFactory().Logger()}
	manager := NewConnectionManager(logger)
	remote, far := net.Pipe()
	defer far.Close()
	go func() {
		buffer := make([]byte, 64)
		for {
			if _, err := far.Read(buffer); err != nil {
				return
			}
		}
	}()
	client, inbound := net.Pipe()
	outbound := interrupt.NewGroup().NewConn(noHalfClose{remote}, true, false)
	closed := make(chan struct{})
	var metadata adapter.InboundContext
	metadata.Destination = M.ParseSocksaddrHostPort("127.0.0.1", 443)
	manager.NewConnection(context.Background(), &fixedDialer{conn: outbound}, inbound, metadata, func(error) { close(closed) })
	_, err := client.Write([]byte("request"))
	require.NoError(t, err)
	require.NoError(t, client.Close())
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the connection was not closed")
	}
	logger.access.Lock()
	defer logger.access.Unlock()
	require.Empty(t, logger.lines)
}
