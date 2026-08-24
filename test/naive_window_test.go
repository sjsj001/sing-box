//go:build with_naive_outbound

package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/netip"
	"os"
	"testing"
	"time"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/byteformats"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/protocol/socks"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
)

// stream_receive_window spent its whole life parsed but never handed to
// cronet, so every deployment ran on the 128MB default no matter what the
// config said. This test believes nothing but the wire: whatever the option
// says must show up in the client's h2 preface — SETTINGS carries half the
// value as the per-stream window, and the connection-level WINDOW_UPDATE tops
// the session up to the full value.
func TestNaiveOutboundReceiveWindow(t *testing.T) {
	const window uint64 = 4 * 1024 * 1024

	caPem, certPem, keyPem := createSelfSignedCertificate(t, "example.org")
	caPemContent, err := os.ReadFile(caPem)
	require.NoError(t, err)
	keyPair, err := tls.LoadX509KeyPair(certPem, keyPem)
	require.NoError(t, err)

	// Not a naive server: a TLS listener that speaks just enough h2 to read
	// the client's opening frames and report what windows they announced.
	sniffer, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{keyPair},
		NextProtos:   []string{"h2"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { sniffer.Close() })
	snifferPort := uint16(sniffer.Addr().(*net.TCPAddr).Port)

	type preface struct {
		streamWindow     uint32
		sessionIncrement uint32
	}
	prefaceCh := make(chan preface, 1)
	// The client is a real Chromium network stack: it may open spare sockets,
	// abandon one attempt and dial again. Serve every connection it makes and
	// let the first one that completes an h2 preface decide the test; the
	// stragglers just error out on their own deadlines.
	go func() {
		for {
			conn, acceptErr := sniffer.Accept()
			if acceptErr != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(15 * time.Second))
				framer := http2.NewFramer(conn, conn)
				// The server half of the preface, so the client has no reason
				// to sit on its own half.
				if writeErr := framer.WriteSettings(); writeErr != nil {
					return
				}
				magic := make([]byte, len(http2.ClientPreface))
				if _, readErr := io.ReadFull(conn, magic); readErr != nil {
					return
				}
				var got preface
				var haveStream, haveSession bool
				for !haveStream || !haveSession {
					frame, readErr := framer.ReadFrame()
					if readErr != nil {
						return
					}
					switch frame := frame.(type) {
					case *http2.SettingsFrame:
						if value, ok := frame.Value(http2.SettingInitialWindowSize); ok {
							got.streamWindow = value
							haveStream = true
						}
					case *http2.WindowUpdateFrame:
						if frame.StreamID == 0 {
							got.sessionIncrement = frame.Increment
							haveSession = true
						}
					}
				}
				select {
				case prefaceCh <- got:
				default:
				}
			}(conn)
		}
	}()

	var receiveWindow byteformats.MemoryBytes
	require.NoError(t, json.Unmarshal([]byte(`"4mb"`), &receiveWindow))
	require.Equal(t, window, receiveWindow.Value())

	startInstance(t, option.Options{
		Inbounds: []option.Inbound{
			{
				Type: C.TypeMixed,
				Tag:  "mixed-in",
				Options: &option.HTTPMixedInboundOptions{
					ListenOptions: option.ListenOptions{
						Listen:     common.Ptr(badoption.Addr(netip.IPv4Unspecified())),
						ListenPort: clientPort,
					},
				},
			},
		},
		Outbounds: []option.Outbound{
			{
				Type: C.TypeDirect,
			},
			{
				Type: C.TypeNaive,
				Tag:  "naive-out",
				Options: &option.NaiveOutboundOptions{
					ServerOptions: option.ServerOptions{
						Server:     "127.0.0.1",
						ServerPort: snifferPort,
					},
					Username:      "sekai",
					Password:      "password",
					ReceiveWindow: &receiveWindow,
					OutboundTLSOptionsContainer: option.OutboundTLSOptionsContainer{
						TLS: &option.OutboundTLSOptions{
							Enabled:     true,
							ServerName:  "example.org",
							Certificate: []string{string(caPemContent)},
						},
					},
				},
			},
		},
		Route: &option.RouteOptions{
			Rules: []option.Rule{
				{
					Type: C.RuleTypeDefault,
					DefaultOptions: option.DefaultRule{
						RawDefaultRule: option.RawDefaultRule{
							Inbound: []string{"mixed-in"},
						},
						RuleAction: option.RuleAction{
							Action: C.RuleActionTypeRoute,
							RouteOptions: option.RouteActionOptions{
								Outbound: "naive-out",
							},
						},
					},
				},
			},
		},
	})

	// Any dial through the box makes cronet bring its h2 session up against
	// the sniffer. The CONNECT itself never completes and is not the point —
	// but the socks conn must stay open until the preface is captured: the
	// mixed inbound answers before the tunnel exists, and closing early
	// cancels the upstream dial mid-handshake.
	dialCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() {
		dialer := socks.NewClient(N.SystemDialer, M.ParseSocksaddrHostPort("127.0.0.1", clientPort), socks.Version5, "", "")
		conn, dialErr := dialer.DialContext(dialCtx, "tcp", M.ParseSocksaddrHostPort("127.0.0.1", testPort))
		if dialErr == nil {
			defer conn.Close()
			<-dialCtx.Done()
		}
	}()

	select {
	case got := <-prefaceCh:
		require.Equal(t, uint32(window/2), got.streamWindow,
			"SETTINGS INITIAL_WINDOW_SIZE must be half the configured session window")
		require.Equal(t, uint32(window-65535), got.sessionIncrement,
			"connection WINDOW_UPDATE must raise the session window to the configured value")
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the client's h2 preface")
	}
}
