//go:build with_naive_outbound

package main

import (
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"testing"
	"time"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/auth"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/protocol/socks"

	"github.com/stretchr/testify/require"
)

// With no insecure_concurrency the pools are adaptive: one connection while
// one stream is enough, more when concurrent demand stacks past the spread
// threshold, back to one when the demand goes. The h2 session count in the
// netlog is the ground truth that pool bookkeeping became real connections.
func TestNaiveSelfAdaptivePools(t *testing.T) {
	caPem, certPem, keyPem := createSelfSignedCertificate(t, "example.org")
	caPemContent, err := os.ReadFile(caPem)
	require.NoError(t, err)

	startNaiveEcho(t)

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
			{
				Type: C.TypeNaive,
				Tag:  "naive-in",
				Options: &option.NaiveInboundOptions{
					ListenOptions: option.ListenOptions{
						Listen:     common.Ptr(badoption.Addr(netip.IPv4Unspecified())),
						ListenPort: serverPort,
					},
					Users:   []auth.User{{Username: "sekai", Password: "password"}},
					Network: N.NetworkTCP,
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
			{Type: C.TypeDirect},
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
					// insecure_concurrency deliberately unset: adaptive.
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

	naiveOut, ok := instance.Outbound().Outbound("naive-out")
	require.True(t, ok)
	naiveOutbound := naiveOut.(*naive.Outbound)

	netLogPath := "/tmp/naive_adaptive_netlog.json"
	require.True(t, naiveOutbound.Client().Engine().StartNetLogToFile(netLogPath, true))
	defer naiveOutbound.Client().Engine().StopNetLog()

	// One stream at a time is one pool, however many times it runs. It stays
	// open through the burst below so the arithmetic is deterministic —
	// releases are asynchronous and a test must not race one.
	single := naiveExchange(t)
	require.Equal(t, 1, naiveOutbound.Pools(), "a lone stream must not grow the pool set")

	// Eight more concurrent streams make nine, all to one destination: they
	// spread two per connection, so the set must grow to exactly five —
	// demand-shaped, nothing more.
	held := holdConcurrentStreams(t, 8)
	require.Equal(t, 5, naiveOutbound.Pools(), "nine same-destination streams should ride exactly five pools")
	single.Close()
	for _, conn := range held {
		conn.Close()
	}

	// Demand gone, bookkeeping follows — the connections themselves are the
	// server idle timer's job, not this test's.
	require.Eventually(t, func() bool { return naiveOutbound.Pools() == 1 },
		5*time.Second, 50*time.Millisecond, "pool count should fall back to one after the burst drains")

	naiveOutbound.Client().Engine().StopNetLog()
	// Five pools mean at least five sessions; a parallel connect job may
	// leave one transient extra, which is Chromium's business, not a failure.
	sessions := netlogH2SessionCount(t, netLogPath)
	require.GreaterOrEqual(t, sessions, 5,
		"adaptive growth should have opened five h2 sessions. NetLog: %s", netLogPath)
	require.LessOrEqual(t, sessions, 6,
		"more sessions than pools plus one transient connect race. NetLog: %s", netLogPath)
}

// startNaiveEcho serves a plain echo on testPort for streams to hold open.
func startNaiveEcho(t *testing.T) {
	echo, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", testPort))
	require.NoError(t, err)
	t.Cleanup(func() { echo.Close() })
	go func() {
		for {
			conn, acceptErr := echo.Accept()
			if acceptErr != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				io.Copy(conn, conn)
			}(conn)
		}
	}()
}

// naiveExchange opens one tunnel through the box's socks inbound to the echo
// server, proves it carries data both ways, and hands the conn back open — an
// active stream the pool bookkeeping must account for.
func naiveExchange(t *testing.T) net.Conn {
	dialer := socks.NewClient(N.SystemDialer, M.ParseSocksaddrHostPort("127.0.0.1", clientPort), socks.Version5, "", "")
	conn, err := dialer.DialContext(globalCtx, "tcp", M.ParseSocksaddrHostPort("127.0.0.1", testPort))
	require.NoError(t, err)
	_, err = conn.Write([]byte("ping"))
	require.NoError(t, err)
	reply := make([]byte, 4)
	_, err = io.ReadFull(conn, reply)
	require.NoError(t, err)
	return conn
}

// holdConcurrentStreams opens n tunnels at once and returns them all open.
func holdConcurrentStreams(t *testing.T, n int) []net.Conn {
	var held []net.Conn
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn := naiveExchange(t)
			mu.Lock()
			held = append(held, conn)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return held
}
