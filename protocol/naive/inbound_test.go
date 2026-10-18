package naive

import (
	"bytes"
	"context"
	stdtls "crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/listener"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// A reload closes this inbound and starts another in the same process. Unless
// Close tells the client to go, it keeps sending CONNECTs to the closed
// instance — each one a 502 — on a session that never idles out. Closing the
// http.Server would cut the session before the GOAWAY. Under go1.27 the plain
// and lazy cases are what h2c could not pass: it served each session on a
// copy of the http.Server that Shutdown never reached.
func TestAReloadEndsTheSessionsTheOldInstanceHeld(t *testing.T) {
	everyPath(t, func(t *testing.T, path h2Path) {
		inbound, _ := fallbackInbound(t)
		inbound.listener = listener.New(listener.Options{}) // only for Close; never started
		conn := dialH2(t, inbound, path)
		framer := h2Preface(t, conn)
		var block bytes.Buffer
		encoder := hpack.NewEncoder(&block)
		for _, field := range []hpack.HeaderField{
			{Name: ":method", Value: http.MethodHead},
			{Name: ":scheme", Value: "https"},
			{Name: ":authority", Value: "example.com"},
			{Name: ":path", Value: "/"},
		} {
			require.NoError(t, encoder.WriteField(field))
		}
		require.NoError(t, framer.WriteHeaders(http2.HeadersFrameParam{
			StreamID: 1, BlockFragment: block.Bytes(), EndStream: true, EndHeaders: true,
		}))
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
		for answered := false; !answered; {
			frame, err := framer.ReadFrame()
			require.NoError(t, err)
			if settings, ok := frame.(*http2.SettingsFrame); ok && !settings.IsAck() {
				require.NoError(t, framer.WriteSettingsAck())
			}
			headers, ok := frame.(*http2.HeadersFrame)
			answered = ok && headers.StreamID == 1 && headers.StreamEnded()
		}

		require.NoError(t, inbound.Close())

		// The server closes a GOAWAY'd session about a second after its last
		// stream ends; the deadline only tells that apart from never.
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
		var toldToGo bool
		for {
			frame, err := framer.ReadFrame()
			if err != nil {
				var netErr net.Error
				require.False(t, errors.As(err, &netErr) && netErr.Timeout(), "the session outlived the instance: %v", err)
				break
			}
			if goAway, isGoAway := frame.(*http2.GoAwayFrame); isGoAway {
				// The h2 server turns a GOAWAY under way into FLOW_CONTROL once
				// it sees a flow control error; a plain close must not.
				require.Equal(t, http2.ErrCodeNo, goAway.ErrCode)
				toldToGo = true
			}
		}
		require.True(t, toldToGo, "the session was dropped without a GOAWAY")
	})
}

// echoRouter sends whatever a tunnel carries straight back through it.
type echoRouter struct{ adapter.ConnectionRouterEx }

func (echoRouter) RouteConnectionEx(_ context.Context, conn net.Conn, _ adapter.InboundContext, onClose N.CloseHandlerFunc) {
	go func() {
		_, err := io.Copy(conn, conn)
		conn.Close()
		if onClose != nil {
			onClose(err)
		}
	}()
}

// WebTimeout bounds requests that are not tunnels; a tunnel may sit idle for
// hours. No test let one idle past it before, so nothing caught a timeout
// reaching an h2 stream: HTTP/2 applies the http.Server's ReadTimeout and
// WriteTimeout to every stream (ServeHTTP clears only the read one), and a
// deadline left on the connection would cut every tunnel on it.
func TestAnH2TunnelOutlivesWebTimeout(t *testing.T) {
	shortenWebTimeout(t)
	everyPath(t, func(t *testing.T, path h2Path) {
		inbound, _ := fallbackInbound(t)
		inbound.router = echoRouter{}
		inbound.listener = listener.New(listener.Options{})
		var protocols http.Protocols
		transport := &http.Transport{Protocols: &protocols}
		url := "http://"
		if path != plainPath {
			protocols.SetHTTP2(true)
			transport.TLSClientConfig = &stdtls.Config{InsecureSkipVerify: true}
			url = "https://"
		} else {
			protocols.SetUnencryptedHTTP2(true)
		}
		url += serveOn(t, inbound, path)
		defer transport.CloseIdleConnections()
		upload, uploadWriter := io.Pipe()
		defer uploadWriter.Close()
		request, err := http.NewRequest(http.MethodConnect, url, upload)
		require.NoError(t, err)
		request.Host = "example.com:443"
		request.Header.Set("Padding", "x")
		request.Header.Set("Proxy-Authorization", "Basic dXNlcjpzZWNyZXQ=")
		response, err := transport.RoundTrip(request)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, 2, response.ProtoMajor)

		var client paddingConn
		ping := func() {
			t.Helper()
			_, err := client.writeWithPadding(uploadWriter, []byte("ping"))
			require.NoError(t, err)
			read := make(chan error, 1)
			go func() {
				reply := make([]byte, 4)
				_, err := io.ReadFull(readerFunc(func(p []byte) (int, error) {
					return client.readWithPadding(response.Body, p)
				}), reply)
				if err == nil && string(reply) != "ping" {
					err = errors.New("echoed " + string(reply))
				}
				read <- err
			}()
			select {
			case err := <-read:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("no echo through the tunnel")
			}
		}
		ping()
		time.Sleep(4 * WebTimeout)
		ping()
	})
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

// gatedConn holds its second write — the client's Finished — until released,
// and says when it got there: the server has then sent its own flight and is
// waiting inside the handshake.
type gatedConn struct {
	net.Conn
	writes  int
	reached chan struct{}
	gate    chan struct{}
}

func (c *gatedConn) Write(p []byte) (int, error) {
	if c.writes++; c.writes == 2 {
		close(c.reached)
		<-c.gate
	}
	return c.Conn.Write(p)
}

// A connection still in its TLS handshake when Close ran came out of it as a
// session nothing told to go: no GOAWAY, served until the client left. The h2
// server records it only after the handshake and its preface, and nothing on
// the way checks for shutting down. Plain text takes the same way in, without
// the handshake to hold it at: there the preface is what comes after Close.
func TestAConnectionHandshakingThroughCloseIsNotServed(t *testing.T) {
	everyPath(t, func(t *testing.T, path h2Path) {
		inbound, _ := fallbackInbound(t)
		inbound.listener = listener.New(listener.Options{}) // only for Close; never started
		if path == plainPath {
			tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			server := inbound.newHTTPServer()
			inbound.httpServer = server
			accepted := make(chan struct{}, 1)
			connState := server.ConnState
			server.ConnState = func(conn net.Conn, state http.ConnState) {
				if state == http.StateNew {
					accepted <- struct{}{}
				}
				connState(conn, state)
			}
			go server.Serve(tcpListener)
			t.Cleanup(func() { server.Close() })
			conn, err := net.Dial("tcp", tcpListener.Addr().String())
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })
			<-accepted
			require.NoError(t, inbound.Close())

			h2Preface(t, conn)
			requireClosedByServer(t, conn)
			return
		}
		raw, err := net.Dial("tcp", serveOn(t, inbound, path))
		require.NoError(t, err)
		t.Cleanup(func() { raw.Close() })
		gated := &gatedConn{Conn: raw, reached: make(chan struct{}), gate: make(chan struct{})}
		conn := stdtls.Client(gated, &stdtls.Config{InsecureSkipVerify: true, NextProtos: []string{"h2"}})
		handshake := make(chan error, 1)
		go func() { handshake <- conn.Handshake() }()
		<-gated.reached
		require.NoError(t, inbound.Close())
		close(gated.gate)
		require.NoError(t, <-handshake)

		h2Preface(t, conn)
		requireClosedByServer(t, conn)
	})
}
