package naive

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/listener"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/route"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio/deadline"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

// blockedSizeReader records the size of the read in progress, -1 between
// reads.
type blockedSizeReader struct {
	io.Reader
	blocked atomic.Int64
	reads   atomic.Int64
}

func newBlockedSizeReader(reader io.Reader) *blockedSizeReader {
	r := &blockedSizeReader{Reader: reader}
	r.blocked.Store(-1)
	return r
}

func (r *blockedSizeReader) Read(p []byte) (int, error) {
	r.reads.Add(1)
	r.blocked.Store(int64(len(p)))
	n, err := r.Reader.Read(p)
	r.blocked.Store(-1)
	return n, err
}

// quietRead waits until the copy sits in one read, and returns its size.
func (r *blockedSizeReader) quietRead(t *testing.T) int64 {
	t.Helper()
	var size int64
	require.Eventually(t, func() bool {
		reads, blocked := r.reads.Load(), r.blocked.Load()
		time.Sleep(20 * time.Millisecond)
		size = blocked
		return blocked >= 0 && r.reads.Load() == reads && r.blocked.Load() == blocked
	}, 5*time.Second, time.Millisecond)
	return size
}

// tunnelRouter routes a tunnel the way route.Router does, a deadline reader in
// front and then route.ConnectionManager, to remote.
type tunnelRouter struct {
	adapter.ConnectionRouterEx
	manager *route.ConnectionManager
	remote  net.Conn
}

func (r tunnelRouter) RouteConnectionEx(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	if deadline.NeedAdditionalReadDeadline(conn) {
		conn = deadline.NewConn(conn)
	}
	r.manager.NewConnection(ctx, r, conn, metadata, onClose)
}

func (r tunnelRouter) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return r.remote, nil
}

func (tunnelRouter) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}

// statusWriter is an HTTP/2 or HTTP/3 ResponseWriter that reports the status
// it is given.
type statusWriter struct {
	header http.Header
	status chan int
}

func (w *statusWriter) Header() http.Header         { return w.header }
func (w *statusWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *statusWriter) WriteHeader(code int)        { w.status <- code }
func (w *statusWriter) Flush()                      {}

// quicBody returns at once from an empty Read, as quic-go's request body does.
type quicBody struct{ io.Reader }

func (b quicBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return b.Reader.Read(p)
}

// An idle tunnel's upload waits holding no buffer on HTTP/2 and a 16 KiB one
// on HTTP/3: inside the padded first frames, after them, after a burst that
// ended exactly on the full buffer, and after an upload past 512 KB grew the
// full size to 64 KiB. On HTTP/3 a read that filled the 16 KiB is followed by
// a full one. Every byte still arrives in order, and a smart client
// is answered with the dial's timing, which reaches the responder through the
// wrapper's Upstream.
func TestAnIdleTunnelUploadWaitsOnASmallBuffer(t *testing.T) {
	for _, protoMajor := range []int{2, 3} {
		for _, c := range []struct{ frames, uploaded int }{{3, 0}, {paddingCount, 1}, {paddingCount, idleReadBuffer}, {paddingCount, 32 << 10}, {paddingCount, 600 << 10}} {
			upload, client := io.Pipe()
			var body *blockedSizeReader
			if protoMajor == 2 {
				body = newBlockedSizeReader(upload)
			} else {
				body = newBlockedSizeReader(quicBody{upload})
			}
			local, remote := net.Pipe()
			received := make(chan []byte, 1)
			go func() {
				data, _ := io.ReadAll(local)
				received <- data
			}()
			inbound, _ := fallbackInbound(t)
			inbound.router = tunnelRouter{manager: route.NewConnectionManager(log.NewNOPFactory().Logger()), remote: remote}
			inbound.listener = listener.New(listener.Options{}) // only for its options; never started
			header := http.Header{}
			header.Set("Padding", "x")
			header.Set("Proxy-Authorization", "Basic dXNlcjpzZWNyZXQ=")
			header.Set(headerSmart, "1")
			request := &http.Request{
				Method:     http.MethodConnect,
				ProtoMajor: protoMajor,
				Header:     header,
				Host:       "example.com:443",
				URL:        &url.URL{Host: "example.com:443"},
				RemoteAddr: "127.0.0.1:1",
				Body:       io.NopCloser(body),
			}
			writer := &statusWriter{header: http.Header{}, status: make(chan int, 2)}
			served := make(chan struct{})
			go func() {
				inbound.ServeHTTP(writer, request)
				close(served)
			}()
			select {
			case status := <-writer.status:
				require.Equal(t, http.StatusOK, status)
				require.NotEmpty(t, writer.header.Get(ConnectAckHeader), "answered without the dial reported")
			case <-time.After(5 * time.Second):
				t.Fatal("the smart client was never answered")
			}

			var padding paddingConn
			for range c.frames {
				_, err := padding.writeWithPadding(client, []byte("x"))
				require.NoError(t, err)
			}
			if c.uploaded > 0 {
				_, err := client.Write(bytes.Repeat([]byte("y"), c.uploaded))
				require.NoError(t, err)
			}
			blocked := body.quietRead(t)
			switch {
			case protoMajor == 2:
				require.Zero(t, blocked, "%+v", c)
			case c.uploaded == idleReadBuffer:
				require.Equal(t, int64(buf.BufferSize), blocked, "a read that filled the small buffer is followed by a full one")
			default:
				require.LessOrEqual(t, blocked, int64(idleReadBuffer), "%+v", c)
			}
			client.Close()
			require.Equal(t, append(bytes.Repeat([]byte("x"), c.frames), bytes.Repeat([]byte("y"), c.uploaded)...), <-received)
			select {
			case <-served:
			case <-time.After(5 * time.Second):
				t.Fatal("the handler outlived its tunnel")
			}
		}
	}
}

// startH2CUpload posts a request over net/http's own HTTP/2, whose body is
// what the returned writer writes, to a server running handler.
func startH2CUpload(t *testing.T, handler http.HandlerFunc) *io.PipeWriter {
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	server := &http.Server{Protocols: &protocols, Handler: handler}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	transport := &http.Transport{Protocols: &protocols}
	t.Cleanup(transport.CloseIdleConnections)
	reader, writer := io.Pipe()
	t.Cleanup(func() { writer.Close() })
	go func() {
		response, err := (&http.Client{Transport: transport}).Post("http://"+listener.Addr().String(), "application/octet-stream", reader)
		if err == nil {
			response.Body.Close()
		}
	}()
	return writer
}

// waitReadable rests on this: an empty Read on the HTTP/2 server's request
// body blocks until data arrives, and takes none of it. TLS h2 and h2c are the
// same server. Should a Go release change it, the relay would go back to
// holding a full buffer per idle tunnel without anything else failing.
func TestHTTP2BodyEmptyReadWaitsAndTakesNothing(t *testing.T) {
	type result struct {
		n   int
		err error
	}
	started := make(chan struct{})
	emptyDone := make(chan result, 1)
	got := make(chan string, 1)
	upload := startH2CUpload(t, func(w http.ResponseWriter, r *http.Request) {
		close(started)
		n, err := r.Body.Read(nil)
		emptyDone <- result{n, err}
		data, _ := io.ReadAll(r.Body)
		got <- string(data)
	})

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler never started")
	}
	// Only from inside the handler does the read's silence prove anything.
	select {
	case r := <-emptyDone:
		t.Fatalf("the empty read returned before any data: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}
	_, err := upload.Write([]byte("hello"))
	require.NoError(t, err)
	select {
	case r := <-emptyDone:
		require.Equal(t, result{}, r)
	case <-time.After(2 * time.Second):
		t.Fatal("the empty read did not return once data arrived")
	}
	upload.Close()
	require.Equal(t, "hello", <-got, "the empty read took data")
}

// A body that ends with nothing sent ends an empty Read with io.EOF, which the
// copy takes as the tunnel's upload ending.
func TestHTTP2BodyEmptyReadEndsWithTheStream(t *testing.T) {
	started := make(chan struct{})
	emptyDone := make(chan error, 1)
	upload := startH2CUpload(t, func(w http.ResponseWriter, r *http.Request) {
		close(started)
		_, err := r.Body.Read(nil)
		emptyDone <- err
	})
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler never started")
	}
	upload.Close()
	select {
	case err := <-emptyDone:
		require.Equal(t, io.EOF, err)
	case <-time.After(2 * time.Second):
		t.Fatal("the empty read did not return at the end of the stream")
	}
}
