package route

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/database64128/tfo-go/v2"
	"github.com/stretchr/testify/require"
)

// fastOpenDialer returns what the default dialer does with tcp_fast_open on: a
// connection that dials on its first write, wrapped in trackedConn.
type fastOpenDialer struct {
	N.Dialer
	target  M.Socksaddr
	manager *ConnectionManager
}

func (d *fastOpenDialer) DialContext(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
	// Fallback, so a platform without fast open still dials as production
	// would there: the half-close test's write is the first, which fast opens.
	conn, err := dialer.DialSlowContext(&tfo.Dialer{Fallback: true}, ctx, network, d.target)
	if err != nil {
		return nil, err
	}
	return d.manager.TrackConn(conn), nil
}

// greetingServer speaks first, as SMTP, IMAP and POP3 do.
func greetingServer(t *testing.T) M.Socksaddr {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = conn.Write([]byte("220 ready\r\n"))
				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()
	return M.SocksaddrFromNet(listener.Addr())
}

// Over a fast open connection, nothing is dialed until something is written.
// The copy loop writes first when the destination says so, but trackedConn hid
// the deprecated interface that said it, and a client waiting for a greeting
// waited on a connection never made.
func TestAServerFirstProtocolIsGreetedOverFastOpen(t *testing.T) {
	manager := NewConnectionManager(log.NewNOPFactory().Logger())
	client, inbound := net.Pipe()
	defer client.Close()
	var metadata adapter.InboundContext
	metadata.Destination = M.ParseSocksaddrHostPort("127.0.0.1", 25)
	manager.NewConnection(context.Background(), &fastOpenDialer{target: greetingServer(t), manager: manager}, inbound, metadata, nil)

	require.NoError(t, client.SetReadDeadline(time.Now().Add(5*time.Second)))
	greeting := make([]byte, 64)
	n, err := client.Read(greeting)
	require.NoError(t, err)
	require.Equal(t, "220 ready\r\n", string(greeting[:n]))
}

func TestAFastOpenConnectionHalfClosesThroughTheTracker(t *testing.T) {
	manager := NewConnectionManager(log.NewNOPFactory().Logger())
	conn, err := (&fastOpenDialer{target: greetingServer(t), manager: manager}).DialContext(context.Background(), N.NetworkTCP, M.Socksaddr{})
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Write([]byte("QUIT\r\n"))
	require.NoError(t, err)
	// What the copy loop does: unwrap through the tracker, then half-close.
	closer, isCloser := N.UnwrapWriter(conn).(N.WriteCloser)
	require.True(t, isCloser, "the dialed connection is found beneath the tracker")
	require.NoError(t, closer.CloseWrite())
	// The server hangs up only once it reads the end of the request, so the
	// greeting ending in EOF shows both halves: the write side closed, the read
	// side still open.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	greeting, err := io.ReadAll(conn)
	require.NoError(t, err)
	require.Equal(t, "220 ready\r\n", string(greeting))
}

// answerAfterEOFServer answers only once the request has ended, as a server
// that reads a whole upload before replying does.
func answerAfterEOFServer(t *testing.T) M.Socksaddr {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(io.Discard, conn)
				_, _ = conn.Write([]byte("pong"))
			}()
		}
	}()
	return M.SocksaddrFromNet(listener.Addr())
}

// End to end through the copy loop: a client that ends its request over a fast
// open connection still gets the answer, so the half-close reached the dialed
// connection beneath the tracker rather than closing it outright.
func TestAClientHalfCloseOverFastOpenKeepsTheAnswer(t *testing.T) {
	manager := NewConnectionManager(log.NewNOPFactory().Logger())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	client, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer client.Close()
	inbound, err := listener.Accept()
	require.NoError(t, err)
	var metadata adapter.InboundContext
	metadata.Destination = M.ParseSocksaddrHostPort("127.0.0.1", 25)
	go manager.NewConnection(context.Background(), &fastOpenDialer{target: answerAfterEOFServer(t), manager: manager}, inbound, metadata, nil)

	_, err = client.Write([]byte("ping"))
	require.NoError(t, err)
	require.NoError(t, client.(*net.TCPConn).CloseWrite())
	require.NoError(t, client.SetReadDeadline(time.Now().Add(5*time.Second)))
	answer, err := io.ReadAll(client)
	require.NoError(t, err)
	require.Equal(t, "pong", string(answer))
}
