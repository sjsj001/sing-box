//go:build with_naive_outbound

package main

import (
	"context"
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"testing"
	"time"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/auth"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/protocol/socks"

	"github.com/stretchr/testify/require"
)

func TestNaiveSelfCloseWrite(t *testing.T)     { testNaiveSelfCloseWrite(t, false) }
func TestNaiveSelfQUICCloseWrite(t *testing.T) { testNaiveSelfCloseWrite(t, true) }

// A client that ends its request and then waits for the answer — anything that
// frames a request by end of input — used to get nothing back: the tunnel had
// no way to end only the request side, so it was closed outright.
func testNaiveSelfCloseWrite(t *testing.T, quic bool) {
	listener, err := net.Listen("tcp", M.ParseSocksaddrHostPort("127.0.0.1", testPort).String())
	require.NoError(t, err)
	var served sync.WaitGroup
	served.Add(1)
	go func() {
		defer served.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			served.Add(1)
			go func() {
				defer served.Done()
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				// The answer exists only once the request has ended.
				request, _ := io.ReadAll(conn)
				if string(request) == "request" {
					_, _ = conn.Write([]byte("response"))
				}
			}()
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		served.Wait()
	})

	instance := startInstance(t, naiveSelfOptions(t, quic))
	destination := M.ParseSocksaddrHostPort("127.0.0.1", testPort)

	t.Run("through the proxy", func(t *testing.T) {
		dialer := socks.NewClient(N.SystemDialer, M.ParseSocksaddrHostPort("127.0.0.1", clientPort), socks.Version5, "", "")
		conn, err := dialer.DialContext(context.Background(), N.NetworkTCP, destination)
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.Write([]byte("request"))
		require.NoError(t, err)
		require.NoError(t, conn.(*net.TCPConn).CloseWrite())
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(10*time.Second)))
		response, err := io.ReadAll(conn)
		require.NoError(t, err)
		require.Equal(t, "response", string(response))
	})

	t.Run("on the tunnel", func(t *testing.T) {
		naiveOut, loaded := instance.Outbound().Outbound("naive-out")
		require.True(t, loaded)
		conn, err := naiveOut.DialContext(context.Background(), N.NetworkTCP, destination)
		require.NoError(t, err)
		defer conn.Close()
		closer, isCloser := conn.(N.WriteCloser)
		require.True(t, isCloser, "the copy loop half-closes only what asserts as a WriteCloser")
		_, err = conn.Write([]byte("request"))
		require.NoError(t, err)
		require.NoError(t, closer.CloseWrite())
		// Data after end of stream makes cronet fail the whole stream, so a
		// late write must be refused here and the answer still arrive.
		_, err = conn.Write([]byte("late"))
		require.Error(t, err)
		require.NoError(t, closer.CloseWrite(), "a second CloseWrite is a no-op")
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(10*time.Second)))
		response, err := io.ReadAll(conn)
		require.NoError(t, err)
		require.Equal(t, "response", string(response))
	})
}

func naiveSelfOptions(t *testing.T, quic bool) option.Options {
	caPem, certPem, keyPem := createSelfSignedCertificate(t, "example.org")
	caPemContent, err := os.ReadFile(caPem)
	require.NoError(t, err)
	inboundNetwork := N.NetworkTCP
	if quic {
		inboundNetwork = N.NetworkUDP
	}
	return option.Options{
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
					Network: option.NetworkList(inboundNetwork),
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
			// A selector wraps what it dials in interrupt.Conn, which hid
			// CloseWrite as route's trackedConn did on the server.
			{
				Type:    C.TypeSelector,
				Tag:     "select",
				Options: &option.SelectorOutboundOptions{GroupCommonOption: option.GroupCommonOption{Outbounds: []string{"naive-out"}}},
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
					QUIC:     quic,
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
								Outbound: "select",
							},
						},
					},
				},
			},
		},
	}
}
