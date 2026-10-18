package quic

import (
	"bufio"
	"context"
	stdtls "crypto/tls"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"testing"
	"time"

	"github.com/sagernet/quic-go"
	"github.com/sagernet/quic-go/http3"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/auth"
	"github.com/sagernet/sing/common/json/badoption"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

func freePort(t *testing.T) uint16 {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return uint16(port)
}

// startInbound starts a naive inbound as a config would, on TCP and UDP, with
// ECH on. It returns the address both listen on, the ECH config list, and the
// scope whose Close closes the inbound as a box does.
func startInbound(t *testing.T, router adapter.Router) (string, []byte, *adapter.Scope) {
	t.Helper()
	key, cert, err := tls.GenerateCertificate(nil, nil, time.Now, "example.com", time.Now().Add(time.Hour))
	require.NoError(t, err)
	echConfig, echKey, err := tls.ECHKeygenDefault("not.example.org")
	require.NoError(t, err)
	port := freePort(t)
	options := option.NaiveInboundOptions{
		ListenOptions: option.ListenOptions{Listen: common.Ptr(badoption.Addr(netip.MustParseAddr("127.0.0.1"))), ListenPort: port},
		Users:         []auth.User{{Username: "user", Password: "secret"}},
	}
	options.TLS = &option.InboundTLSOptions{
		Enabled: true, ServerName: "example.com",
		Certificate: []string{string(cert)}, Key: []string{string(key)},
		ECH: &option.InboundECHOptions{Enabled: true, Key: []string{echKey}},
	}
	in, err := naive.NewInbound(context.Background(), router, log.NewNOPFactory().Logger(), "naive-in", options)
	require.NoError(t, err)
	scope := adapter.NewScope(context.Background(), log.NewNOPFactory().Logger())
	require.NoError(t, in.Start(adapter.StartStateStart, scope))
	block, _ := pem.Decode([]byte(echConfig))
	require.NotNil(t, block)
	return fmt.Sprintf("127.0.0.1:%d", port), block.Bytes, scope
}

// crypto/tls decrypts an ECH hello with the keys of the config the connection
// was made with, before it asks GetConfigForClient for another: the TCP
// listener has to make it with the inbound's own config.
func TestECHIsAcceptedOnTheTCPPath(t *testing.T) {
	address, echList, in := startInbound(t, nil)
	defer in.Close()
	conn, err := stdtls.Dial("tcp", address, &stdtls.Config{
		ServerName: "example.com", InsecureSkipVerify: true,
		NextProtos:                     []string{"h2"},
		EncryptedClientHelloConfigList: echList,
		MinVersion:                     stdtls.VersionTLS13,
	})
	require.NoError(t, err)
	defer conn.Close()
	state := conn.ConnectionState()
	require.True(t, state.ECHAccepted, "ECH not accepted")
	require.Equal(t, "h2", state.NegotiatedProtocol)
	// A Go server sends SETTINGS right after the handshake.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	head := make([]byte, 9)
	_, err = io.ReadFull(conn, head)
	require.NoError(t, err, "no SETTINGS from the server")
	require.Equal(t, byte(4), head[3], "first frame is not SETTINGS")
}

// TCP and QUIC used to share one ALPN list, so TCP took h3 and QUIC took h2
// and http/1.1; a Go or Caddy server refuses both.
func TestEachTransportNegotiatesOnlyItsOwnProtocols(t *testing.T) {
	address, _, in := startInbound(t, nil)
	defer in.Close()
	_, err := stdtls.Dial("tcp", address, &stdtls.Config{ServerName: "example.com", InsecureSkipVerify: true, NextProtos: []string{"h3"}})
	require.ErrorContains(t, err, "no application protocol")
	for _, protocol := range []string{"h2", "http/1.1"} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = quic.DialAddr(ctx, address, &stdtls.Config{ServerName: "example.com", InsecureSkipVerify: true, NextProtos: []string{protocol}}, nil)
		cancel()
		require.ErrorContains(t, err, "no application protocol", protocol)
	}
}

// Shutdown has closed the TCP listener by the time Close closes it again.
// Reported, that failed every stop.
func TestCloseReturnsNoError(t *testing.T) {
	_, _, in := startInbound(t, nil)
	require.NoError(t, in.Close())
}

// The heartbeat address is answered without a dial, so this needs no router:
// it is the CONNECT handling itself, on each protocol a client may arrive by.
func TestAnAuthenticatedConnectIsAnsweredOnEveryProtocol(t *testing.T) {
	address, _, in := startInbound(t, nil)
	defer in.Close()
	clientTLS := &stdtls.Config{ServerName: "example.com", InsecureSkipVerify: true}
	connect := func(t *testing.T, transport http.RoundTripper) *http.Response {
		request, err := http.NewRequest(http.MethodConnect, "https://"+address, nil)
		require.NoError(t, err)
		request.Host = naive.ProbeDestination().String()
		request.Header.Set("Padding", "x")
		request.Header.Set("Proxy-Authorization", "Basic dXNlcjpzZWNyZXQ=")
		response, err := transport.RoundTrip(request)
		require.NoError(t, err)
		response.Body.Close()
		return response
	}
	t.Run("h2", func(t *testing.T) {
		var protocols http.Protocols
		protocols.SetHTTP2(true)
		transport := &http.Transport{Protocols: &protocols, TLSClientConfig: clientTLS}
		defer transport.CloseIdleConnections()
		response := connect(t, transport)
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, 2, response.ProtoMajor)
	})
	t.Run("http/1.1", func(t *testing.T) {
		conn, err := stdtls.Dial("tcp", address, &stdtls.Config{ServerName: "example.com", InsecureSkipVerify: true, NextProtos: []string{"http/1.1"}})
		require.NoError(t, err)
		defer conn.Close()
		host := naive.ProbeDestination().String()
		_, err = fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\nPadding: x\r\nProxy-Authorization: Basic dXNlcjpzZWNyZXQ=\r\n\r\n", host, host)
		require.NoError(t, err)
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
		response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodConnect})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode)
	})
	t.Run("h3", func(t *testing.T) {
		transport := &http3.Transport{TLSClientConfig: clientTLS}
		defer transport.Close()
		response := connect(t, transport)
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, 3, response.ProtoMajor)
	})
}

// echoRouter sends whatever a tunnel carries straight back through it.
type echoRouter struct{ adapter.Router }

func (echoRouter) RouteConnectionEx(_ context.Context, conn net.Conn, _ adapter.InboundContext, onClose N.CloseHandlerFunc) {
	go func() {
		_, err := io.Copy(conn, conn)
		conn.Close()
		if onClose != nil {
			onClose(err)
		}
	}()
}

// readPadded reads n bytes of data from naive's first, padded frames: a
// two-byte data length, a padding length, the data, then the padding.
func readPadded(reader io.Reader, n int) (string, error) {
	var data []byte
	for len(data) < n {
		head := make([]byte, 3)
		if _, err := io.ReadFull(reader, head); err != nil {
			return "", err
		}
		frame := make([]byte, int(binary.BigEndian.Uint16(head))+int(head[2]))
		if _, err := io.ReadFull(reader, frame); err != nil {
			return "", err
		}
		data = append(data, frame[:len(frame)-int(head[2])]...)
	}
	return string(data), nil
}

// serveConn gives each HTTP/3 request stream WebTimeout to deliver its head,
// and ServeHTTP clears that once the request authenticates as a tunnel, whose
// body may then sit idle for hours. The test of the bound clears it in its own
// handler, so nothing failed without the line in ServeHTTP.
func TestAnH3TunnelOutlivesWebTimeout(t *testing.T) {
	shortenWebTimeout(t)
	address, _, in := startInbound(t, echoRouter{})
	defer in.Close()
	transport := &http3.Transport{TLSClientConfig: &stdtls.Config{ServerName: "example.com", InsecureSkipVerify: true}}
	defer transport.Close()
	upload, uploadWriter := io.Pipe()
	defer uploadWriter.Close()
	request, err := http.NewRequest(http.MethodConnect, "https://"+address, upload)
	require.NoError(t, err)
	request.Host = "example.com:443"
	request.Header.Set("Padding", "x")
	request.Header.Set("Proxy-Authorization", "Basic dXNlcjpzZWNyZXQ=")
	response, err := transport.RoundTrip(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, 3, response.ProtoMajor)

	ping := func() {
		t.Helper()
		_, err := uploadWriter.Write([]byte{0, 4, 0, 'p', 'i', 'n', 'g'})
		require.NoError(t, err)
		read := make(chan error, 1)
		go func() {
			reply, err := readPadded(response.Body, 4)
			if err == nil && reply != "ping" {
				err = errors.New("echoed " + reply)
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
	time.Sleep(3 * naive.WebTimeout)
	ping()
}

// Closing the UDP socket alone drops every QUIC connection with no
// CONNECTION_CLOSE, and the instance reloaded onto the same port drops the
// client's packets as unknown: the client found out from its own loss
// detection, some six seconds later, and every request in between hung and
// failed. A remote application error, H3_NO_ERROR, is what proves the
// CONNECTION_CLOSE arrived; an idle timeout would be neither remote nor this
// quick.
func TestCloseTellsEveryClientAtOnce(t *testing.T) {
	address, _, in := startInbound(t, nil)
	conn := dialH3(t, address)
	require.NoError(t, in.Close())
	select {
	case <-conn.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("the client was left to find out by itself")
	}
	cause := context.Cause(conn.Context())
	var applicationError *quic.ApplicationError
	var transportError *quic.TransportError
	switch {
	case errors.As(cause, &applicationError):
		require.True(t, applicationError.Remote, "the server has to say so, not the client's own timeout")
		require.Equal(t, quic.ApplicationErrorCode(http3.ErrCodeNoError), applicationError.ErrorCode)
	case errors.As(cause, &transportError):
		// Before the server has confirmed the handshake, an application
		// close travels as this transport error (RFC 9000 §10.2.3).
		require.True(t, transportError.Remote, "the server has to say so, not the client's own timeout")
		require.Equal(t, quic.ApplicationErrorErrorCode, transportError.ErrorCode)
	default:
		t.Fatalf("closed by %T: %v", cause, cause)
	}
}
