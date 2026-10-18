package route

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// noDeadlinePacketConn is what a naive HTTP/2 stream carrying UoT looks like
// to route: reads block until Close, and read deadlines are refused.
type noDeadlinePacketConn struct {
	N.PacketConn
	closed chan struct{}
}

func (c *noDeadlinePacketConn) ReadPacket(*buf.Buffer) (M.Socksaddr, error) {
	<-c.closed
	return M.Socksaddr{}, net.ErrClosed
}

func (c *noDeadlinePacketConn) WritePacket(buffer *buf.Buffer, _ M.Socksaddr) error {
	buffer.Release()
	return nil
}

func (c *noDeadlinePacketConn) Close() error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return nil
}

func (c *noDeadlinePacketConn) SetReadDeadline(time.Time) error { return os.ErrInvalid }

type udpListenDialer struct{ N.Dialer }

func (udpListenDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return net.ListenPacket("udp", "127.0.0.1:0")
}

func TestAUDPSessionWithoutReadDeadlinesEndsAtItsTimeout(t *testing.T) {
	t.Parallel()
	manager := NewConnectionManager(log.NewNOPFactory().NewLogger("route"))
	conn := &noDeadlinePacketConn{closed: make(chan struct{})}
	ended := make(chan error, 1)
	manager.NewPacketConnection(context.Background(), udpListenDialer{}, conn, adapter.InboundContext{
		Destination: M.ParseSocksaddr("127.0.0.1:53"),
		UDPTimeout:  100 * time.Millisecond,
	}, func(err error) { ended <- err })
	select {
	case <-ended:
	case <-time.After(2 * time.Second):
		t.Fatal("the session outlived its 100ms udp_timeout by two seconds")
	}
	select {
	case <-conn.closed:
	default:
		t.Fatal("the inbound side was left open")
	}
}
