package quic

import (
	"context"
	stdtls "crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"testing"
	"time"

	"github.com/sagernet/quic-go"
	"github.com/sagernet/quic-go/http3"
	"github.com/sagernet/sing-box/common/listener"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/json/badoption"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

// serveH3 starts the naive inbound's HTTP/3 server as production configures it.
func serveH3(t *testing.T) string {
	t.Helper()
	return serveH3With(t, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}))
}

func serveH3With(t *testing.T, handler http.Handler) string {
	t.Helper()
	return serveH3In(t, context.Background(), handler)
}

func serveH3In(t *testing.T, ctx context.Context, handler http.Handler) string {
	t.Helper()
	logger := log.NewNOPFactory().Logger()
	key, certificate, err := tls.GenerateCertificate(nil, nil, time.Now, "example.com", time.Now().Add(time.Hour))
	require.NoError(t, err)
	config, err := tls.NewServer(ctx, logger, option.InboundTLSOptions{
		Enabled:     true,
		Certificate: []string{string(certificate)},
		Key:         []string{string(key)},
	})
	require.NoError(t, err)
	require.NoError(t, config.Start())
	udp := listener.New(listener.Options{
		Context: ctx,
		Logger:  logger,
		Network: []string{N.NetworkUDP},
		Listen:  option.ListenOptions{Listen: common.Ptr(badoption.Addr(netip.MustParseAddr("127.0.0.1")))},
	})
	server, err := naive.ConfigureHTTP3ListenerFunc(ctx, logger, udp, handler, config, option.NaiveInboundOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { server.Close() })
	return server.(interface{ Addr() net.Addr }).Addr().String()
}

func dialH3(t *testing.T, address string) *quic.Conn {
	t.Helper()
	conn, err := quic.DialAddr(context.Background(), address,
		&stdtls.Config{InsecureSkipVerify: true, NextProtos: []string{http3.NextProtoH3}}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.CloseWithError(0, "") })
	return conn
}

// http3 allocates the length a HEADERS frame declares before reading a byte of
// it. At the default cap of a megabyte, five bytes on a stream bought that
// megabyte, unauthenticated, for as long as the client kept the stream open;
// at 16KiB, a connection's 1024 streams would still buy 16MiB.
func TestAHeadThatWillNotFitIsRefusedRatherThanAwaited(t *testing.T) {
	conn := dialH3(t, serveH3(t))
	stream, err := conn.OpenStream()
	require.NoError(t, err)
	// A HEADERS frame (type 0x01) declaring one byte past the 4KiB cap, and
	// nothing after it.
	_, err = stream.Write([]byte{0x01, 0x50, 0x01})
	require.NoError(t, err)

	require.NoError(t, stream.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, err = io.ReadAll(stream)
	var netErr net.Error
	require.False(t, err != nil && errors.As(err, &netErr) && netErr.Timeout(),
		"the server is still waiting for a head past the cap: %v", err)
}

func TestOneConnectionCannotOpenUnboundedStreams(t *testing.T) {
	conn := dialH3(t, serveH3(t))
	for range 1024 {
		_, err := conn.OpenStream()
		require.NoError(t, err)
	}
	_, err := conn.OpenStream()
	require.Error(t, err, "past the per-connection limit")
}

// Each unidirectional stream a client opens holds a goroutine waiting for its
// type until the connection ends: none can be given a read deadline, since the
// control stream must stay open. An h3 client needs three.
func TestOneConnectionCannotOpenUnboundedUniStreams(t *testing.T) {
	conn := dialH3(t, serveH3(t))
	for range 8 {
		_, err := conn.OpenUniStream()
		require.NoError(t, err)
	}
	_, err := conn.OpenUniStream()
	require.Error(t, err, "past the per-connection limit")
}

func shortenWebTimeout(t *testing.T) {
	previous := naive.WebTimeout
	naive.WebTimeout = 200 * time.Millisecond
	t.Cleanup(func() { naive.WebTimeout = previous })
}

// A client that opened a request stream and never finished its head held the
// stream, and the goroutine reading it, for as long as it kept the connection
// alive. HTTP/1 has bounded reading a head since ReadHeaderTimeout.
func TestARequestHeadThatNeverArrivesIsGivenUpOn(t *testing.T) {
	shortenWebTimeout(t)
	conn := dialH3(t, serveH3(t))
	stream, err := conn.OpenStream()
	require.NoError(t, err)
	// A HEADERS frame type and nothing after it.
	_, err = stream.Write([]byte{0x01})
	require.NoError(t, err)

	require.NoError(t, stream.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, err = io.ReadAll(stream)
	var netErr net.Error
	require.False(t, err != nil && errors.As(err, &netErr) && netErr.Timeout(),
		"the server is still waiting for the head: %v", err)
}

// A tunnel clears the bound once it authenticates, as ServeHTTP does, and its
// body may then sit idle as long as it likes.
func TestABodyIsNotCutOnceTheBoundIsCleared(t *testing.T) {
	shortenWebTimeout(t)
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_ = http.NewResponseController(writer).SetReadDeadline(time.Time{})
		writer.WriteHeader(http.StatusOK)
		writer.(http.Flusher).Flush()
		body, _ := io.ReadAll(request.Body)
		_, _ = writer.Write(body)
	})
	transport := &http3.Transport{TLSClientConfig: &stdtls.Config{InsecureSkipVerify: true}}
	defer transport.Close()
	reader, writer := io.Pipe()
	request, err := http.NewRequest(http.MethodPost, "https://"+serveH3With(t, handler)+"/", reader)
	require.NoError(t, err)
	response, err := transport.RoundTrip(request)
	require.NoError(t, err)
	defer response.Body.Close()

	time.Sleep(3 * naive.WebTimeout)
	_, err = writer.Write([]byte("late"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, "late", string(body))
}

// A connection accepted after Close took its list is told at once, not left
// for the socket to take down silently.
func TestAConnectionAcceptedDuringCloseIsToldToo(t *testing.T) {
	conn := dialH3(t, serveH3(t))
	live := &liveConns{conns: make(map[*quic.Conn]struct{})}
	live.closeAll()
	live.add(conn)
	select {
	case <-conn.Context().Done():
	case <-time.After(5 * time.Second):
		t.Fatal("added after closeAll and left open")
	}
}

// A reload from the command line cancels the box's context before Close, and
// the accept loop's exit closed the socket under the connections, which
// learnt of it only from the idle timeout.
func TestCancellingTheContextTellsEveryClientAtOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := dialH3(t, serveH3In(t, ctx, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusMethodNotAllowed)
	})))
	cancel()
	select {
	case <-conn.Context().Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the client was not told the server went away")
	}
}
