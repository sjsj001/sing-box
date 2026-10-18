//go:build with_naive_outbound

package main

import (
	"io"
	"net/netip"
	"os"
	"testing"
	"time"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/auth"
	"github.com/sagernet/sing/common/json/badoption"
	"github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

// The HTTP/3 server gives a request stream WebTimeout to deliver its head, and
// the naive inbound clears that once a CONNECT authenticates. Without the
// clearing a tunnel whose upload sat idle that long was cut.
func TestNaiveSelfQUICTunnelOutlivesTheHeadBound(t *testing.T) {
	previous := naive.WebTimeout
	naive.WebTimeout = 200 * time.Millisecond
	t.Cleanup(func() { naive.WebTimeout = previous })
	startNaiveEcho(t)
	caPem, certPem, keyPem := createSelfSignedCertificate(t, "example.org")
	caPemContent, err := os.ReadFile(caPem)
	require.NoError(t, err)
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
			{
				Type: C.TypeNaive,
				Tag:  "naive-in",
				Options: &option.NaiveInboundOptions{
					ListenOptions: option.ListenOptions{
						Listen:     common.Ptr(badoption.Addr(netip.IPv4Unspecified())),
						ListenPort: serverPort,
					},
					Users: []auth.User{
						{
							Username: "sekai",
							Password: "password",
						},
					},
					Network: network.NetworkUDP,
					InboundTLSOptionsContainer: option.InboundTLSOptionsContainer{
						TLS: &option.InboundTLSOptions{
							Enabled:         true,
							ServerName:      "example.org",
							CertificatePath: certPem,
							KeyPath:         keyPem,
						},
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
						ServerPort: serverPort,
					},
					Username: "sekai",
					Password: "password",
					QUIC:     true,
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
	conn := naiveExchange(t)
	defer conn.Close()
	time.Sleep(5 * naive.WebTimeout)
	_, err = conn.Write([]byte("pong"))
	require.NoError(t, err)
	reply := make([]byte, 4)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = io.ReadFull(conn, reply)
	require.NoError(t, err, "the tunnel was cut while its upload sat idle")
	require.Equal(t, "pong", string(reply))
}
