//go:build with_naive_outbound

package main

import (
	"context"
	"io"
	"net"
	"net/netip"
	"os"
	"strconv"
	"testing"
	"time"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/group"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/auth"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/common/network"
	N "github.com/sagernet/sing/common/network"
	socks "github.com/sagernet/sing/protocol/socks"

	"github.com/stretchr/testify/require"
)

// smartAssertTCP drives one echo exchange through the socks proxy, using a
// loop-accepting echo server so it is robust to the smart group's first-connect
// probe (a bare TCP connect the server makes to the target before the real
// connection). testPingPongWithConn's single Accept() would be consumed by the
// probe, so the smart tests use this instead.
func smartAssertTCP(t *testing.T, clientPort, testPort uint16) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(int(testPort)))
	require.NoError(t, err)
	defer l.Close()
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(conn, conn); _ = conn.Close() }()
		}
	}()
	dialer := socks.NewClient(N.SystemDialer, M.ParseSocksaddrHostPort("127.0.0.1", clientPort), socks.Version5, "", "")
	conn, err := dialer.DialContext(context.Background(), "tcp", M.ParseSocksaddrHostPort("127.0.0.1", testPort))
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(15*time.Second)))
	_, err = conn.Write([]byte("hello"))
	require.NoError(t, err)
	buf := make([]byte, 5)
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	require.Equal(t, "hello", string(buf))
}

func smartNaiveServer(t *testing.T, tag string, port uint16, certPem, keyPem string) option.Inbound {
	return option.Inbound{
		Type: C.TypeNaive,
		Tag:  tag,
		Options: &option.NaiveInboundOptions{
			ListenOptions: option.ListenOptions{
				Listen:     common.Ptr(badoption.Addr(netip.IPv4Unspecified())),
				ListenPort: port,
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
	}
}

func smartNaiveClient(tag string, port uint16, caPem string) option.Outbound {
	return option.Outbound{
		Type: C.TypeNaive,
		Tag:  tag,
		Options: &option.NaiveOutboundOptions{
			ServerOptions: option.ServerOptions{Server: "127.0.0.1", ServerPort: port},
			Username:      "sekai",
			Password:      "password",
			OutboundTLSOptionsContainer: option.OutboundTLSOptionsContainer{
				TLS: &option.OutboundTLSOptions{
					Enabled:     true,
					ServerName:  "example.org",
					Certificate: []string{caPem},
				},
			},
		},
	}
}

// TestSmartSelfBasic drives traffic through a smart group over two real naive
// tunnels (two in-process naive servers), validating member resolution, the
// engine selection + single-dial fallback, DialContext, and per-node interrupt
// end to end. Short ping/probe intervals make the measurement loops run during
// the test, exercising the probe protocol (probe.go server handler +
// smart_probe.go client) — a hang or leak there is caught by goleak.
func TestSmartSelfBasic(t *testing.T) {
	caPem, certPem, keyPem := createSelfSignedCertificate(t, "example.org")
	caPemContent, err := os.ReadFile(caPem)
	require.NoError(t, err)
	caStr := string(caPemContent)

	instance := startInstance(t, option.Options{
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
			smartNaiveServer(t, "naive-in-1", serverPort, certPem, keyPem),
			smartNaiveServer(t, "naive-in-2", otherPort, certPem, keyPem),
		},
		Outbounds: []option.Outbound{
			{Type: C.TypeDirect, Tag: "direct-out"},
			smartNaiveClient("naive-out-1", serverPort, caStr),
			smartNaiveClient("naive-out-2", otherPort, caStr),
			{
				Type: C.TypeSmart,
				Tag:  "smart",
				Options: &option.SmartOutboundOptions{
					GroupCommonOption: option.GroupCommonOption{
						Outbounds: []string{"naive-out-1", "naive-out-2"},
					},
					PingInterval: badoption.Duration(time.Second),
					Nodes: []option.SmartNodeOptions{
						{Tag: "naive-out-1", CleanPriority: 1},
						{Tag: "naive-out-2", CleanPriority: 100},
					},
				},
			},
		},
		Route: smartRoute(),
	})

	// Immediate traffic (before any probe data) must flow via single-dial fallback.
	smartAssertTCP(t, clientPort, testPort)

	// The ping probe must round-trip through the real naive tunnels: a node only
	// becomes healthy with a measured baseline if probePing dialed the magic
	// authority and the server's handleSmartProbe answered.
	smartOut, ok := instance.Outbound().Outbound("smart")
	require.True(t, ok)
	sm := smartOut.(*group.Smart)
	require.Eventually(t, func() bool {
		nodes := sm.DebugNodes()
		if len(nodes) != 2 {
			return false
		}
		for _, n := range nodes {
			if !n.Healthy || n.BaselineMs < 0 {
				return false
			}
		}
		return true
	}, 6*time.Second, 200*time.Millisecond, "both nodes should be healthy with a measured local baseline")

	smartAssertTCP(t, clientPort, testPort)
}

// TestSmartSelfFailover includes a dead member (naive outbound to a closed
// port). The smart group must mark it localDown via the ping/handshake-verdict
// path while keeping the live members healthy, and traffic must keep flowing.
func TestSmartSelfFailover(t *testing.T) {
	caPem, certPem, keyPem := createSelfSignedCertificate(t, "example.org")
	caPemContent, err := os.ReadFile(caPem)
	require.NoError(t, err)
	caStr := string(caPemContent)

	const deadPort = 10099 // nothing listens here

	instance := startInstance(t, option.Options{
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
			smartNaiveServer(t, "naive-in-1", serverPort, certPem, keyPem),
			smartNaiveServer(t, "naive-in-2", otherPort, certPem, keyPem),
		},
		Outbounds: []option.Outbound{
			{Type: C.TypeDirect, Tag: "direct-out"},
			smartNaiveClient("live-1", serverPort, caStr),
			smartNaiveClient("live-2", otherPort, caStr),
			smartNaiveClient("dead", deadPort, caStr),
			{
				Type: C.TypeSmart,
				Tag:  "smart",
				Options: &option.SmartOutboundOptions{
					GroupCommonOption: option.GroupCommonOption{
						Outbounds: []string{"live-1", "live-2", "dead"},
					},
					PingInterval: badoption.Duration(time.Second),
				},
			},
		},
		Route: smartRoute(),
	})

	smartOut, ok := instance.Outbound().Outbound("smart")
	require.True(t, ok)
	sm := smartOut.(*group.Smart)

	// The dead member must be judged localDown while the live members stay up.
	require.Eventually(t, func() bool {
		var deadDown, liveUp int
		for _, n := range sm.DebugNodes() {
			if n.Tag == "dead" && !n.Healthy {
				deadDown++
			}
			if (n.Tag == "live-1" || n.Tag == "live-2") && n.Healthy && n.BaselineMs >= 0 {
				liveUp++
			}
		}
		return deadDown == 1 && liveUp == 2
	}, 15*time.Second, 300*time.Millisecond, "dead member localDown, both live members healthy")

	// Traffic keeps flowing (routed around the dead member).
	smartAssertTCP(t, clientPort, testPort)
}

func smartRoute() *option.RouteOptions {
	return &option.RouteOptions{
		Rules: []option.Rule{
			{
				Type: C.RuleTypeDefault,
				DefaultOptions: option.DefaultRule{
					RawDefaultRule: option.RawDefaultRule{Inbound: []string{"mixed-in"}},
					RuleAction: option.RuleAction{
						Action:       C.RuleActionTypeRoute,
						RouteOptions: option.RouteActionOptions{Outbound: "smart"},
					},
				},
			},
		},
	}
}
