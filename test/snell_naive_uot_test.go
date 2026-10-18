//go:build with_naive_outbound

package main

import (
	"net"
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/sagernet/sing-box"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-snell/snellv4"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/auth"
	E "github.com/sagernet/sing/common/exceptions"
	F "github.com/sagernet/sing/common/format"
	"github.com/sagernet/sing/common/json/badoption"
	"github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

// The production path for a client's UDP: snell v5 UoT into the core, naive
// UoT out to a relay, the relay's route to the destination. Whatever ends the
// session past the snell inbound has to end the snell session with it, or the
// client keeps sending into a stream the core only drains.
func TestSnellUoTEndsWithTheSessionBehindIt(t *testing.T) {
	t.Run("core idle timeout", func(t *testing.T) {
		// Stands in for the 10s route gives port 53 at the core.
		_, query := startSnellNaiveUoTChain(t, 300*time.Millisecond, 0)
		require.NoError(t, query("first", 5*time.Second))
		// Past twice the timeout: TimeoutPacketConn re-arms once for a reply
		// that arrived after the read began.
		time.Sleep(900 * time.Millisecond)
		requireSessionEnded(t, query)
	})
	t.Run("relay idle timeout", func(t *testing.T) {
		// The relay's UoT connection is an HTTP/2 stream without read
		// deadlines, so its udp_timeout runs on route's timer instead.
		_, query := startSnellNaiveUoTChain(t, 0, 300*time.Millisecond)
		require.NoError(t, query("first", 5*time.Second))
		time.Sleep(900 * time.Millisecond)
		requireSessionEnded(t, query)
	})
	t.Run("relay closes", func(t *testing.T) {
		// A relay reload or restart: its streams are reset under the core.
		relay, query := startSnellNaiveUoTChain(t, 0, 0)
		require.NoError(t, query("first", 5*time.Second))
		require.NoError(t, relay.Close())
		requireSessionEnded(t, query)
	})
}

func requireSessionEnded(t *testing.T, query func(string, time.Duration) error) {
	t.Helper()
	start := time.Now()
	err := query("second", 3*time.Second)
	t.Logf("query after the session ended: %v after %v", err, time.Since(start).Round(time.Millisecond))
	require.Error(t, err)
	require.False(t, E.IsTimeout(err), "the client must learn the session is over, not wait into a stream the core only drains")
}

func startSnellNaiveUoTChain(t *testing.T, coreUDPTimeout, relayUDPTimeout time.Duration) (*box.Box, func(string, time.Duration) error) {
	t.Helper()
	caPem, certPem, keyPem := createSelfSignedCertificate(t, "example.org")
	caPemContent, err := os.ReadFile(caPem)
	require.NoError(t, err)
	ports := snellFreePorts(t, 2)
	snellPort, naivePort := ports[0], ports[1]
	relay := startInstance(t, option.Options{
		Inbounds: []option.Inbound{{
			Type: C.TypeNaive,
			Tag:  "naive-in",
			Options: &option.NaiveInboundOptions{
				ListenOptions: option.ListenOptions{
					Listen:     common.Ptr(badoption.Addr(netip.AddrFrom4([4]byte{127, 0, 0, 1}))),
					ListenPort: naivePort,
				},
				Users:   []auth.User{{Username: "sekai", Password: "password"}},
				Network: network.NetworkTCP,
				InboundTLSOptionsContainer: option.InboundTLSOptionsContainer{
					TLS: &option.InboundTLSOptions{
						Enabled:         true,
						ServerName:      "example.org",
						CertificatePath: certPem,
						KeyPath:         keyPem,
					},
				},
			},
		}},
		Outbounds: []option.Outbound{{Type: C.TypeDirect, Tag: "direct"}},
		Route: &option.RouteOptions{Rules: []option.Rule{{
			Type: C.RuleTypeDefault,
			DefaultOptions: option.DefaultRule{
				RawDefaultRule: option.RawDefaultRule{Inbound: []string{"naive-in"}},
				RuleAction: option.RuleAction{
					Action: C.RuleActionTypeRoute,
					RouteOptions: option.RouteActionOptions{
						Outbound: "direct",
						RawRouteOptionsActionOptions: option.RawRouteOptionsActionOptions{
							UDPTimeout: badoption.Duration(relayUDPTimeout),
						},
					},
				},
			},
		}}},
	})
	startInstance(t, option.Options{
		Inbounds: []option.Inbound{{
			Type: C.TypeSnell,
			Tag:  "snell-in",
			Options: &option.SnellInboundOptions{
				AbstractSnellInboundOptions: option.AbstractSnellInboundOptions{
					ListenOptions: option.ListenOptions{
						Listen:     common.Ptr(badoption.Addr(netip.AddrFrom4([4]byte{127, 0, 0, 1}))),
						ListenPort: snellPort,
					},
					PSK: snellSharedPSK,
				},
				Version: 5,
			},
		}},
		Outbounds: []option.Outbound{
			{Type: C.TypeDirect, Tag: "direct"},
			{
				Type: C.TypeNaive,
				Tag:  "naive-out",
				Options: &option.NaiveOutboundOptions{
					ServerOptions: option.ServerOptions{Server: "127.0.0.1", ServerPort: naivePort},
					Username:      "sekai",
					Password:      "password",
					UDPOverTCP:    &option.UDPOverTCPOptions{Enabled: true},
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
		Route: &option.RouteOptions{Rules: []option.Rule{{
			Type: C.RuleTypeDefault,
			DefaultOptions: option.DefaultRule{
				RawDefaultRule: option.RawDefaultRule{Inbound: []string{"snell-in"}},
				RuleAction: option.RuleAction{
					Action: C.RuleActionTypeRoute,
					RouteOptions: option.RouteActionOptions{
						Outbound: "naive-out",
						RawRouteOptionsActionOptions: option.RawRouteOptionsActionOptions{
							UDPTimeout: badoption.Duration(coreUDPTimeout),
						},
					},
				},
			},
		}}},
	})

	echo, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { echo.Close() })
	go func() {
		b := make([]byte, 2048)
		for {
			n, addr, err := echo.ReadFrom(b)
			if err != nil {
				return
			}
			_, _ = echo.WriteTo(b[:n], addr)
		}
	}()
	client, err := snellv4.NewClient(snellv4.ClientOptions{PSK: []byte(snellSharedPSK)})
	require.NoError(t, err)
	raw, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", F.ToString(snellPort)))
	require.NoError(t, err)
	packetConn, err := client.DialPacketConn(raw)
	require.NoError(t, err)
	t.Cleanup(func() { packetConn.Close() })
	return relay, func(message string, wait time.Duration) error {
		if _, err := packetConn.WriteTo([]byte(message), echo.LocalAddr()); err != nil {
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
}
