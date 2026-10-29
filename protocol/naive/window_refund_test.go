package naive

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/listener"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/route"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// gatedRouter holds each tunnel unread, as a destination that has stopped
// reading would, until release closes. Then it reads the tunnel to its end and
// says so on drained.
type gatedRouter struct {
	adapter.ConnectionRouterEx
	release <-chan struct{}
	drained chan<- struct{}
}

func (r gatedRouter) RouteConnectionEx(_ context.Context, conn net.Conn, _ adapter.InboundContext, onClose N.CloseHandlerFunc) {
	go func() {
		<-r.release
		_, err := io.Copy(io.Discard, conn)
		r.drained <- struct{}{}
		conn.Close()
		onClose(err)
	}()
}

// stallRouter routes each tunnel the way the router does, through
// route.ConnectionManager, to a destination that never reads. The
// destinations close with the test.
type stallRouter struct {
	adapter.ConnectionRouterEx
	t       *testing.T
	manager *route.ConnectionManager
}

func (r stallRouter) RouteConnectionEx(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	r.manager.NewConnection(ctx, r, conn, metadata, onClose)
}

func (r stallRouter) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	conn, destination := net.Pipe()
	r.t.Cleanup(func() { destination.Close() })
	return conn, nil
}

func (stallRouter) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, os.ErrInvalid
}

// resetTunnelsGiveBack fills a connection's 16MiB upload budget with two
// authenticated tunnels through router on path, resets both, runs afterReset, and
// returns the connection window once the server has given back what it will.
func resetTunnelsGiveBack(t *testing.T, path h2Path, router adapter.ConnectionRouterEx, afterReset func()) int64 {
	const budget = 16 << 20 // MaxUploadBufferPerConnection; half of it per stream
	inbound, _ := fallbackInbound(t)
	inbound.router = router
	inbound.listener = listener.New(listener.Options{}) // only for its options; never started
	conn := dialH2(t, inbound, path)
	framer := h2Preface(t, conn)
	framer.ReadMetaHeaders = hpack.NewDecoder(4096, nil)

	var writing sync.Mutex
	write := func(frame func() error) {
		t.Helper()
		writing.Lock()
		defer writing.Unlock()
		require.NoError(t, frame())
	}
	var granted atomic.Int64 // connection window granted past the first 65535
	statuses := make(chan string, 2)
	pinged := make(chan struct{}, 1)
	go func() {
		for {
			frame, err := framer.ReadFrame()
			if err != nil {
				return
			}
			switch frame := frame.(type) {
			case *http2.SettingsFrame:
				if !frame.IsAck() {
					writing.Lock()
					_ = framer.WriteSettingsAck()
					writing.Unlock()
				}
			case *http2.WindowUpdateFrame:
				if frame.StreamID == 0 {
					granted.Add(int64(frame.Increment))
				}
			case *http2.MetaHeadersFrame:
				statuses <- frame.PseudoValue("status")
			case *http2.PingFrame:
				if frame.IsAck() {
					pinged <- struct{}{}
				}
			}
		}
	}()
	var sent int64
	window := func() int64 { return 65535 + granted.Load() - sent }
	waitForBudget := func(failure string) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); window() < budget; time.Sleep(10 * time.Millisecond) {
			require.True(t, time.Now().Before(deadline), "%s: window %d of %d", failure, window(), budget)
		}
	}

	var block bytes.Buffer
	encoder := hpack.NewEncoder(&block)
	// Zeros get past the padding reader: eight empty padded reads, then raw
	// bytes. A frame may be no larger than MaxReadFrameSize.
	chunk := make([]byte, 16<<10)
	waitForBudget("the server never opened the connection window")
	for _, id := range []uint32{1, 3} {
		block.Reset()
		for _, field := range []hpack.HeaderField{
			{Name: ":method", Value: http.MethodConnect},
			{Name: ":authority", Value: "example.com:443"},
			{Name: "padding", Value: "x"},
			{Name: "proxy-authorization", Value: "Basic dXNlcjpzZWNyZXQ="},
		} {
			require.NoError(t, encoder.WriteField(field))
		}
		write(func() error {
			return framer.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: block.Bytes(), EndHeaders: true})
		})
		select {
		case status := <-statuses:
			require.Equal(t, "200", status)
		case <-time.After(5 * time.Second):
			t.Fatal("the tunnel was never answered")
		}
		for left := budget / 2; left > 0; left -= len(chunk) {
			write(func() error { return framer.WriteData(id, false, chunk) })
		}
		sent += budget / 2
	}
	write(func() error { return framer.WriteRSTStream(1, http2.ErrCodeCancel) })
	write(func() error { return framer.WriteRSTStream(3, http2.ErrCodeCancel) })
	if afterReset != nil {
		afterReset()
	}
	waitForBudget("the reset tunnels' upload never came back")
	// Whatever the server gives back has reached its serve loop by now, so it
	// is queued ahead of this PING's ack.
	write(func() error { return framer.WritePing(false, [8]byte{}) })
	select {
	case <-pinged:
	case <-time.After(5 * time.Second):
		t.Fatal("the PING was never acknowledged")
	}
	t.Logf("sent %d, window back to %d", sent, window())
	return window()
}

// R8-13 lets one connection buffer 16MiB of upload. What a tunnel had not read
// when its client reset it must come back to the connection, and only once:
// x/net v0.57 gave it back at the reset and again as the handler read it
// (GO-2026-6612), so the window grew past what the server would buffer. The
// fixed h2 server — x/net v0.60, net/http's from go1.26.9 — gives it back only
// as the handler reads or closes the body, or returns, so the inbound closes
// the body of a tunnel whose stream is reset.
func TestResetStreamRefundsConnectionWindow(t *testing.T) {
	const budget = 16 << 20
	everyPath(t, func(t *testing.T, path h2Path) {
		t.Run("destination drains after the reset", func(t *testing.T) {
			release := make(chan struct{})
			drained := make(chan struct{}, 2)
			window := resetTunnelsGiveBack(t, path, gatedRouter{release: release, drained: drained}, func() {
				close(release)
				for range 2 {
					select {
					case <-drained:
					case <-time.After(5 * time.Second):
						t.Fatal("a reset tunnel was never read to its end")
					}
				}
			})
			require.EqualValues(t, budget, window, "the reset tunnels' upload came back more than once")
		})
		// route.ConnectionManager's copy does not watch the stream's context,
		// so with the destination stuck nothing but the inbound closing the
		// body on the reset gives the upload back.
		t.Run("destination never reads", func(t *testing.T) {
			window := resetTunnelsGiveBack(t, path, stallRouter{t: t, manager: route.NewConnectionManager(context.Background(), log.NewNOPFactory().Logger())}, nil)
			require.EqualValues(t, budget, window, "the reset tunnels' upload came back more than once")
		})
	})
}
