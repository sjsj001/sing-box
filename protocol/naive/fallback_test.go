package naive

import (
	"bufio"
	"bytes"
	"context"
	stdtls "crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/auth"
	"github.com/sagernet/sing/common/logger"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// fallbackInbound is an inbound with one user and a fallback site holding a
// page, which is what makes a stalled write possible at all.
func fallbackInbound(t *testing.T) (*Inbound, chan struct{}) {
	t.Helper()
	root := t.TempDir()
	page := bytes.Repeat([]byte("decoy "), 4096)
	require.NoError(t, os.WriteFile(filepath.Join(root, "index.html"), page, 0o644))
	served := make(chan struct{}, 16)
	files := http.FileServer(http.Dir(root))
	return &Inbound{
		ctx:           context.Background(),
		logger:        log.NewNOPFactory().Logger(),
		authenticator: auth.NewAuthenticator([]auth.User{{Username: "user", Password: "secret"}}),
		fallbackHandler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			files.ServeHTTP(writer, request)
			served <- struct{}{}
		}),
	}, served
}

func shortenWebTimeout(t *testing.T) {
	previous := WebTimeout
	WebTimeout = 200 * time.Millisecond
	t.Cleanup(func() { WebTimeout = previous })
}

func TestAnUnauthenticatedConnectIsRefusedLikeAWebServer(t *testing.T) {
	inbound, _ := fallbackInbound(t)
	for name, header := range map[string]http.Header{
		"no padding":     {},
		"wrong password": {"Padding": {"x"}, "Proxy-Authorization": {"Basic dXNlcjp3cm9uZw=="}},
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodConnect, "http://example.com:443", nil)
			request.Header = header
			recorder := httptest.NewRecorder()
			inbound.ServeHTTP(recorder, request)
			require.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
			require.Equal(t, "GET, HEAD", recorder.Header().Get("Allow"))
			require.Zero(t, recorder.Body.Len(), "a proxy's refusal must not be the decoy page")
		})
	}
}

func serveInbound(t *testing.T, inbound *Inbound) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := inbound.newHTTPServer()
	inbound.httpServer = server
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	return listener.Addr().String()
}

// A client that never opens its receive window cannot be sent the page. Before
// the write deadline its request parked the handler, and the upload buffer
// behind it, for the life of the connection — without authenticating.
func TestAFallbackPageAClientWillNotReceiveIsAbandoned(t *testing.T) {
	shortenWebTimeout(t)
	inbound, served := fallbackInbound(t)
	address := serveInbound(t, inbound)

	conn, err := net.Dial("tcp", address)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Write([]byte(http2.ClientPreface))
	require.NoError(t, err)
	framer := http2.NewFramer(conn, conn)
	require.NoError(t, framer.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 0}))
	var block bytes.Buffer
	encoder := hpack.NewEncoder(&block)
	for _, field := range []hpack.HeaderField{
		{Name: ":method", Value: http.MethodGet},
		{Name: ":scheme", Value: "https"},
		{Name: ":authority", Value: "example.com"},
		{Name: ":path", Value: "/"},
	} {
		require.NoError(t, encoder.WriteField(field))
	}
	require.NoError(t, framer.WriteHeaders(http2.HeadersFrameParam{
		StreamID: 1, BlockFragment: block.Bytes(), EndStream: true, EndHeaders: true,
	}))

	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatal("the fallback handler is still parked on a write the client will never accept")
	}
}

// h2Path is a way h2 reaches the server: by prior knowledge in plain text, by
// ALPN over standard TLS, and by prior knowledge inside the TLS of the lazy
// path reality and kTLS take.
type h2Path int

const (
	plainPath h2Path = iota
	tlsPath
	lazyPath
)

// serveOn serves the inbound as Start does for path.
func serveOn(t *testing.T, inbound *Inbound, path h2Path) string {
	t.Helper()
	switch path {
	case plainPath:
		return serveInbound(t, inbound)
	case lazyPath:
		// Anything but a *tls.STDServerConfig takes the lazy path.
		inbound.tlsConfig = struct{ tls.ServerConfig }{testTLSConfig(t)}
	}
	return serveInboundTLS(t, inbound)
}

// dialH2 serves the inbound on path and connects to it, with h2 negotiated
// where there is anything to negotiate.
func dialH2(t *testing.T, inbound *Inbound, path h2Path) net.Conn {
	t.Helper()
	address := serveOn(t, inbound, path)
	var conn net.Conn
	var err error
	if path == plainPath {
		conn, err = net.Dial("tcp", address)
	} else {
		conn, err = stdtls.Dial("tcp", address, &stdtls.Config{InsecureSkipVerify: true, NextProtos: []string{"h2"}})
	}
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

// everyPath runs f over each way h2 reaches the server.
func everyPath(t *testing.T, f func(t *testing.T, path h2Path)) {
	t.Run("plain", func(t *testing.T) { f(t, plainPath) })
	t.Run("tls", func(t *testing.T) { f(t, tlsPath) })
	t.Run("lazy", func(t *testing.T) { f(t, lazyPath) })
}

// h2Preface opens h2 on conn and says nothing more.
func h2Preface(t *testing.T, conn net.Conn) *http2.Framer {
	t.Helper()
	_, err := conn.Write([]byte(http2.ClientPreface))
	require.NoError(t, err)
	framer := http2.NewFramer(conn, conn)
	require.NoError(t, framer.WriteSettings())
	return framer
}

// The h2 server allocates the length a frame header declares before reading a
// byte of the frame. With the default cap of a megabyte, nine bytes bought that
// megabyte, unauthenticated, until the connection idled out.
func TestAFrameTooLargeToReadIsRefusedRatherThanAwaited(t *testing.T) {
	everyPath(t, func(t *testing.T, path h2Path) {
		inbound, _ := fallbackInbound(t)
		conn := dialH2(t, inbound, path)
		h2Preface(t, conn)
		// A header declaring one byte past the protocol's minimum frame size,
		// of an unknown type on stream 0, and nothing after it.
		const length = 16<<10 + 1
		_, err := conn.Write([]byte{length >> 16, length >> 8 & 0xff, length & 0xff, 0xff, 0, 0, 0, 0, 0})
		require.NoError(t, err)
		requireClosedByServer(t, conn)
	})
}

// A header block is held until it ends, up to the header-list cap. At the
// default megabyte, a block that never ended held that much, unauthenticated,
// until the connection idled out.
func TestAHeaderBlockThatWillNotFitIsRefusedRatherThanAwaited(t *testing.T) {
	everyPath(t, func(t *testing.T, path h2Path) {
		inbound, _ := fallbackInbound(t)
		conn := dialH2(t, inbound, path)
		framer := h2Preface(t, conn)
		// 256KiB of fields, far under a megabyte, and never END_HEADERS.
		for i := range 16 {
			var block bytes.Buffer
			encoder := hpack.NewEncoder(&block)
			require.NoError(t, encoder.WriteField(hpack.HeaderField{Name: fmt.Sprintf("x-pad-%d", i), Value: strings.Repeat("a", 16<<10)}))
			if i == 0 {
				err := framer.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: block.Bytes()})
				require.NoError(t, err)
			} else if framer.WriteContinuation(1, false, block.Bytes()) != nil {
				break // the server has already hung up
			}
		}
		requireClosedByServer(t, conn)
	})
}

func TestAnHTTP1ConnectionThatNeverSpeaksIsClosed(t *testing.T) {
	shortenWebTimeout(t)
	inbound, _ := fallbackInbound(t)
	conn, err := net.Dial("tcp", serveInbound(t, inbound))
	require.NoError(t, err)
	defer conn.Close()
	requireClosedByServer(t, conn)
}

// Nothing else fails if these go: without IdleTimeout an unauthenticated client
// that fetched the page once holds its keep-alive connection forever, and
// without ReadHeaderTimeout one that never sends a request does.
func TestTheFallbackServerBoundsIdleAndSilentConnections(t *testing.T) {
	server := (&Inbound{ctx: context.Background()}).newHTTPServer()
	require.Positive(t, server.IdleTimeout)
	require.Positive(t, server.ReadHeaderTimeout)
}

// testTLSConfig is a standard TLS config as an inbound with no alpn set
// starts with.
func testTLSConfig(t *testing.T) tls.ServerConfig {
	t.Helper()
	key, certificate, err := tls.GenerateCertificate(nil, nil, time.Now, "example.com", time.Now().Add(time.Hour))
	require.NoError(t, err)
	config, err := tls.NewServer(context.Background(), log.NewNOPFactory().Logger(), option.InboundTLSOptions{
		Enabled:     true,
		Certificate: []string{string(certificate)},
		Key:         []string{string(key)},
	})
	require.NoError(t, err)
	require.NoError(t, config.Start())
	return config
}

// serveInboundTLS serves the inbound over TLS as Start does, with a standard
// config unless the inbound already has one.
func serveInboundTLS(t *testing.T, inbound *Inbound) string {
	t.Helper()
	if inbound.tlsConfig == nil {
		inbound.tlsConfig = testTLSConfig(t)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := inbound.newHTTPServer()
	inbound.httpServer = server
	go server.Serve(inbound.tlsListener(listener))
	t.Cleanup(func() { server.Close() })
	return listener.Addr().String()
}

func requireClosedByServer(t *testing.T, conn net.Conn) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	for {
		_, err := conn.Read(make([]byte, 1))
		if err == nil {
			continue
		}
		var netErr net.Error
		require.False(t, errors.As(err, &netErr) && netErr.Timeout(), "the server never closed it: %v", err)
		return
	}
}

// requireSettingsFirst reads what a Go h2 server sends before anything else.
func requireSettingsFirst(t *testing.T, conn net.Conn) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	head := make([]byte, 9)
	_, err := io.ReadFull(conn, head)
	require.NoError(t, err, "nothing from the server")
	require.Equal(t, byte(http2.FrameSettings), head[3], "the first frame is not SETTINGS: %q", head)
}

// ReadHeaderTimeout bounds a client that never says hello and one that
// completes the handshake and then says nothing. On the standard path net/http
// gives the handshake that bound itself; on the lazy path (reality, kTLS) the
// handshake runs inside the first read, under the deadline net/http sets for
// the request head, which crypto/tls neither clears nor moves. A client silent
// after negotiating h2 on the standard path is the h2 server's instead: it is
// sent SETTINGS at once and closed after its fixed 10s, as by any Go server.
func TestATLSConnectionThatNeverSpeaksIsClosed(t *testing.T) {
	shortenWebTimeout(t)
	for _, lazy := range []bool{false, true} {
		name := "standard"
		if lazy {
			name = "lazy"
		}
		t.Run(name, func(t *testing.T) {
			inbound, _ := fallbackInbound(t)
			if lazy {
				inbound.tlsConfig = struct{ tls.ServerConfig }{testTLSConfig(t)}
			}
			address := serveInboundTLS(t, inbound)

			t.Run("no hello", func(t *testing.T) {
				conn, err := net.Dial("tcp", address)
				require.NoError(t, err)
				defer conn.Close()
				requireClosedByServer(t, conn)
			})
			for _, protocol := range []string{"http/1.1", "h2"} {
				t.Run("silent after "+protocol, func(t *testing.T) {
					conn, err := stdtls.Dial("tcp", address, &stdtls.Config{InsecureSkipVerify: true, NextProtos: []string{protocol}})
					require.NoError(t, err)
					defer conn.Close()
					require.Equal(t, protocol, conn.ConnectionState().NegotiatedProtocol)
					if protocol == "h2" && !lazy {
						requireSettingsFirst(t, conn)
						return
					}
					requireClosedByServer(t, conn)
				})
			}
		})
	}
}

// What a single handshake used to give the proxy away by: with TLS outside
// net/http and h2c inside it, a connection that negotiated h2 was answered in
// HTTP/1, one that negotiated http/1.1 was spoken h2 to, and an h2c upgrade
// inside TLS was taken. A Go server serves what ALPN chose.
func TestTheProtocolSpokenIsTheOneNegotiated(t *testing.T) {
	inbound, _ := fallbackInbound(t)
	address := serveInboundTLS(t, inbound)
	dial := func(t *testing.T, protocol string) net.Conn {
		conn, err := stdtls.Dial("tcp", address, &stdtls.Config{InsecureSkipVerify: true, NextProtos: []string{protocol}})
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		require.Equal(t, protocol, conn.ConnectionState().NegotiatedProtocol)
		return conn
	}
	t.Run("HTTP/1 after h2", func(t *testing.T) {
		conn := dial(t, "h2")
		_, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"))
		require.NoError(t, err)
		// The h2 server queues its SETTINGS and then reads the preface; a bad one ends
		// the connection, on one CPU often before the SETTINGS are out, as on
		// any Go server. What the lazy path did instead was answer in HTTP/1.
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
		reply, err := io.ReadAll(conn)
		var netErr net.Error
		require.False(t, errors.As(err, &netErr) && netErr.Timeout(), "neither answered nor closed: %v", err)
		require.False(t, bytes.HasPrefix(reply, []byte("HTTP/")), "answered in HTTP/1: %q", reply)
	})
	t.Run("h2 after http/1.1", func(t *testing.T) {
		conn := dial(t, "http/1.1")
		_, err := conn.Write([]byte(http2.ClientPreface))
		require.NoError(t, err)
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
		head := make([]byte, 9)
		_, err = io.ReadFull(conn, head)
		require.NoError(t, err)
		require.Equal(t, "HTTP/1.1 ", string(head))
	})
	t.Run("h2c upgrade", func(t *testing.T) {
		conn := dial(t, "http/1.1")
		_, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: example.com\r\n" +
			"Connection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: AAMAAABkAAQAAP__\r\n\r\n"))
		require.NoError(t, err)
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
		response, err := http.ReadResponse(bufio.NewReader(conn), nil)
		require.NoError(t, err)
		response.Body.Close()
		require.Equal(t, http.StatusOK, response.StatusCode)
	})
}

type debugRecorder struct {
	logger.ContextLogger
	lines chan string
}

func (r debugRecorder) Debug(args ...any) {
	select {
	case r.lines <- fmt.Sprint(args...):
	default:
	}
}

// A Go server answers plain HTTP on its TLS port with a 400; this one used to
// say nothing. net/http also logs every failed handshake, which on a public
// port is every scanner: that belongs at Debug, not on stderr.
func TestPlainHTTPOnTheTLSPortIsAnswered400(t *testing.T) {
	inbound, _ := fallbackInbound(t)
	lines := make(chan string, 16)
	inbound.logger = debugRecorder{inbound.logger, lines}
	conn, err := net.Dial("tcp", serveInboundTLS(t, inbound))
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Write([]byte("GET / HTTP/1.0\r\n\r\n"))
	require.NoError(t, err)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	response, err := io.ReadAll(conn)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(response), "HTTP/1.0 400 "), "%q", response)
	select {
	case line := <-lines:
		require.Contains(t, line, "TLS handshake error")
	case <-time.After(5 * time.Second):
		t.Fatal("the failed handshake was not logged at Debug")
	}
}

// The fallback site is the one thing a stranger can read without a password,
// and this process can read every file on the machine. So the site ends at its
// root — no link out, no dotfiles, no listing — and saying no must look like a
// plain miss, not a 500 only this server would give.
func TestTheFallbackSiteStaysInsideItsRoot(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "html")
	for _, dir := range []string{".git", ".well-known", "sub", "empty", "trap/index.html"} {
		require.NoError(t, os.MkdirAll(filepath.Join(root, dir), 0o755))
	}
	for name, content := range map[string]string{
		"index.html": "decoy", "sub/index.html": "sub", "sub/a.txt": "inside",
		".env": "SECRET", ".git/config": "SECRET", ".well-known/x": "challenge",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(root, name), []byte(content), 0o644))
	}
	require.NoError(t, os.WriteFile(filepath.Join(base, "outside.txt"), []byte("SECRET"), 0o600))
	require.NoError(t, os.Symlink("../outside.txt", filepath.Join(root, "out.txt")))
	require.NoError(t, os.Symlink("..", filepath.Join(root, "up")))
	require.NoError(t, os.Symlink("sub/a.txt", filepath.Join(root, "in.txt")))
	require.NoError(t, os.Symlink("sub", filepath.Join(root, "insub")))
	// The deployment cost: an absolute link is refused even when it points
	// inside, so a site must link relatively.
	require.NoError(t, os.Symlink(filepath.Join(root, "sub", "a.txt"), filepath.Join(root, "abs.txt")))

	get := func(site, target string, header ...string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodGet, target, nil)
		for i := 0; i+1 < len(header); i += 2 {
			request.Header.Set(header[i], header[i+1])
		}
		recorder := httptest.NewRecorder()
		http.FileServerFS(webRoot(site)).ServeHTTP(recorder, request)
		return recorder
	}
	for target, code := range map[string]int{
		"/": 200, "/index.html": 301, "/sub": 301, "/sub/": 200, "/sub/a.txt": 200,
		"/in.txt": 200, "/insub/a.txt": 200, "/.well-known/x": 200,
		"/out.txt": 404, "/up/outside.txt": 404, "/up/": 404, "/abs.txt": 404,
		"/.env": 404, "/.git/config": 404, "/.git/": 404,
		"/empty": 404, "/empty/": 404, "/trap/": 404,
	} {
		response := get(root, target)
		require.Equal(t, code, response.Code, target)
		require.NotContains(t, response.Body.String(), "SECRET", target)
	}
	require.Equal(t, "inside", get(root, "/in.txt").Body.String())

	ranged := get(root, "/sub/a.txt", "Range", "bytes=1-2")
	require.Equal(t, http.StatusPartialContent, ranged.Code)
	require.Equal(t, "ns", ranged.Body.String())
	modified := ranged.Header().Get("Last-Modified")
	require.NotEmpty(t, modified)
	require.Equal(t, http.StatusNotModified, get(root, "/sub/a.txt", "If-Modified-Since", modified).Code)

	require.Equal(t, http.StatusNotFound, get(filepath.Join(base, "missing"), "/").Code,
		"a site that is not there yet is a miss, not a failure")
}
