package snell

import (
	"context"
	"crypto/rand"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/route"
	"github.com/sagernet/sing-snell/snellv4"
	E "github.com/sagernet/sing/common/exceptions"
	F "github.com/sagernet/sing/common/format"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

// echoOnceRouter stands in for route: echo one read, close (a zero chunk on a
// reuse stream), wait for the client's EOF, then report the stream done.
type echoOnceRouter struct{ adapter.ConnectionRouterEx }

func (echoOnceRouter) RouteConnectionEx(_ context.Context, conn net.Conn, _ adapter.InboundContext, onClose N.CloseHandlerFunc) {
	go func() {
		b := make([]byte, 64)
		n, err := conn.Read(b)
		if err == nil {
			_, err = conn.Write(b[:n])
		}
		conn.Close()
		_, _ = io.Copy(io.Discard, conn)
		onClose(err)
	}()
}

type tcpDialer struct{ addr string }

func (d tcpDialer) DialContext(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
	var dialer net.Dialer
	return dialer.DialContext(ctx, "tcp", d.addr)
}

func (tcpDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}

func serveSnell(t *testing.T, inbound *Inbound) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go inbound.NewConnection(context.Background(), conn, adapter.InboundContext{}, func(error) {})
		}
	}()
	return listener.Addr().String()
}

func reuseRoundTrip(t *testing.T, client *snellv4.Client, message string) {
	t.Helper()
	conn, err := client.DialContext(context.Background(), M.ParseSocksaddr("example.com:443"))
	require.NoError(t, err)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Write([]byte(message))
	require.NoError(t, err)
	reply := make([]byte, len(message))
	_, err = io.ReadFull(conn, reply)
	require.NoError(t, err)
	require.Equal(t, message, string(reply))
	_, err = conn.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "the server's zero chunk, after which Close returns the session to the pool")
}

func TestAReloadClosesAnIdleReuseSession(t *testing.T) {
	t.Parallel()
	// Between two streams a reuse session waits inside the service call for
	// the next request, where nothing in route tracks it. A reload cancels
	// the box and closes what route tracks; the session stayed open, served
	// by the old box, and failed the client's next request with its context.
	inbound := newV5InboundForHandshake(t)
	inbound.router = echoOnceRouter{}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	boxCtx, cancelBox := context.WithCancel(context.Background())
	ended := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		inbound.NewConnection(boxCtx, conn, adapter.InboundContext{}, func(error) {})
		close(ended)
	}()
	client, err := snellv4.NewClient(snellv4.ClientOptions{
		PSK: []byte("test-password"), Reuse: true, Dialer: tcpDialer{listener.Addr().String()}, Server: M.ParseSocksaddr(listener.Addr().String()),
	})
	require.NoError(t, err)
	defer client.Close()
	reuseRoundTrip(t, client, "first")
	// Lets the server finish the stream and wait for the next request. A
	// cancel landing earlier is caught by the service's own wait on the
	// stream, which watches ctx, and would pass without the fix.
	time.Sleep(100 * time.Millisecond)
	cancelBox()
	select {
	case <-ended:
	case <-time.After(time.Second):
		t.Fatal("one second after the reload the idle session is still open, served by the old box")
	}
}

// routeUDPRouter routes a packet connection the way route does, through a real
// ConnectionManager with a UDP idle timeout and a real UDP socket.
type routeUDPRouter struct {
	adapter.ConnectionRouterEx
	manager    *route.ConnectionManager
	udpTimeout time.Duration
}

func (r *routeUDPRouter) RoutePacketConnectionEx(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	metadata.UDPTimeout = r.udpTimeout
	go r.manager.NewPacketConnection(ctx, udpListenDialer{}, conn, metadata, onClose)
}

type udpListenDialer struct{}

func (udpListenDialer) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, net.ErrClosed
}

func (udpListenDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return net.ListenPacket("udp", "127.0.0.1:0")
}

func startUDPEcho(t *testing.T) (*net.UDPAddr, *atomic.Int32) {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	var received atomic.Int32
	go func() {
		b := make([]byte, 2048)
		for {
			n, addr, err := conn.ReadFrom(b)
			if err != nil {
				return
			}
			received.Add(1)
			_, _ = conn.WriteTo(b[:n], addr)
		}
	}()
	return conn.LocalAddr().(*net.UDPAddr), &received
}

func TestAUoTSessionThatRouteEndsClosesItsConnection(t *testing.T) {
	t.Parallel()
	// route ends a UDP session after its idle timeout, 10s for port 53. The
	// service's Close then kept the TCP stream open for five minutes, reading
	// and dropping what the client sent: a DNS client asked again into
	// nothing and waited out its own timeout before it dialed anew.
	require.Equal(t, C.ProtocolDNS, C.PortProtocols[53])
	require.Equal(t, 10*time.Second, C.ProtocolTimeouts[C.ProtocolDNS])
	inbound := newV5InboundForHandshake(t)
	inbound.handshakeTimeout = 5 * time.Second
	inbound.router = &routeUDPRouter{manager: route.NewConnectionManager(log.NewNOPFactory().NewLogger("route")), udpTimeout: 200 * time.Millisecond}
	addr := serveSnell(t, inbound)
	echo, received := startUDPEcho(t)
	client, err := snellv4.NewClient(snellv4.ClientOptions{PSK: []byte("test-password")})
	require.NoError(t, err)
	raw, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	packetConn, err := client.DialPacketConn(raw)
	require.NoError(t, err)
	defer packetConn.Close()
	query := func(message string, wait time.Duration) error {
		if _, err := packetConn.WriteTo([]byte(message), echo); err != nil {
			return err
		}
		_ = packetConn.SetReadDeadline(time.Now().Add(wait))
		reply := make([]byte, 64)
		n, _, err := packetConn.ReadFrom(reply)
		if err == nil && string(reply[:n]) != message {
			return E.New("wrong reply ", string(reply[:n]))
		}
		return err
	}
	require.NoError(t, query("first", time.Second))
	// Past twice the timeout: TimeoutPacketConn re-arms once for a reply that
	// arrived after the read began.
	time.Sleep(600 * time.Millisecond)
	start := time.Now()
	err = query("second", 2*time.Second)
	t.Logf("query after the idle gap: %v after %v", err, time.Since(start).Round(time.Millisecond))
	require.Error(t, err, "route has ended the session")
	require.False(t, E.IsTimeout(err), "the client must learn the session is over, not wait into a stream the server only drains")
	require.Equal(t, int32(1), received.Load())
}

func TestAProbeCannotTellARequestHeaderByItsLength(t *testing.T) {
	t.Parallel()
	// Random bytes short of a request header (16 salt + 23 header) waited for
	// the handshake deadline; 39 or more failed authentication and were reset
	// at once. Every length now waits for the deadline.
	inbound := newV5InboundForHandshake(t)
	inbound.handshakeTimeout = 300 * time.Millisecond
	addr := serveSnell(t, inbound)
	for _, length := range []int{1, 16, 38, 39, 40, 200, 2000} {
		t.Run(F.ToString(length, "_bytes"), func(t *testing.T) {
			t.Parallel()
			conn, err := net.Dial("tcp", addr)
			require.NoError(t, err)
			defer conn.Close()
			probe := make([]byte, length)
			_, _ = rand.Read(probe)
			start := time.Now()
			_, err = conn.Write(probe)
			require.NoError(t, err)
			_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			_, err = conn.Read(make([]byte, 1))
			elapsed := time.Since(start)
			require.Error(t, err)
			require.False(t, E.IsTimeout(err), "the server closes it, at its deadline")
			require.Greater(t, elapsed, inbound.handshakeTimeout*2/3, "closed after %v: %v", elapsed, err)
		})
	}
}
