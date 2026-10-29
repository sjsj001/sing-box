//go:build with_naive_outbound

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/sagernet/sing-box/protocol/naive"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/stretchr/testify/require"
)

// Exercise real CONNECT refusals and recovery over both supported transports.
func TestSmartDestinationReboot(t *testing.T) {
	for _, quic := range []bool{false, true} {
		t.Run(fmt.Sprint("quic=", quic), func(t *testing.T) {
			ca, cert, key := createSelfSignedCertificate(t, "example.org")
			caPEM, err := os.ReadFile(ca)
			require.NoError(t, err)
			caJSON, err := json.Marshal(string(caPEM))
			require.NoError(t, err)
			transport := "tcp"
			if quic {
				transport = "udp"
			}
			var outbounds []string
			for _, tag := range []string{"a", "b"} {
				port := c26Port(t)
				startC26Box(t, fmt.Sprintf(`{"log":{"level":"error"},"inbounds":[{
					"type":"naive","tag":"in","listen":"127.0.0.1","listen_port":%d,"network":%q,
					"users":[{"username":"test","password":"test"}],
					"tls":{"enabled":true,"server_name":"example.org","certificate_path":%q,"key_path":%q}}],
					"outbounds":[{"type":"direct","tag":"direct"}],"route":{"final":"direct"}}`, port, transport, cert, key))
				outbounds = append(outbounds, fmt.Sprintf(`{"type":"naive","tag":%q,"server":"127.0.0.1","server_port":%d,"quic":%v,
					"username":"test","password":"test","tls":{"enabled":true,"server_name":"example.org","certificate":[%s]}}`, tag, port, quic, caJSON))
			}
			client := startC26Box(t, fmt.Sprintf(`{"log":{"level":"error"},"outbounds":[%s,%s,
				{"type":"smart","tag":"smart","groups":[{"tag":"a","outbounds":["a"]},{"tag":"b","outbounds":["b"]}]}],
				"route":{"final":"smart"}}`, outbounds[0], outbounds[1]))
			smart, loaded := client.Outbound().Outbound("smart")
			require.True(t, loaded)
			address := fmt.Sprintf("127.0.0.1:%d", c26Port(t))
			startTarget := func() net.Listener {
				listener, err := net.Listen("tcp", address)
				require.NoError(t, err)
				t.Cleanup(func() { listener.Close() })
				go func() {
					for {
						conn, err := listener.Accept()
						if err != nil {
							return
						}
						go func() { defer conn.Close(); io.Copy(conn, conn) }()
					}
				}()
				return listener
			}
			ping := func() error {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				conn, err := smart.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr(address))
				if err != nil {
					return err
				}
				defer conn.Close()
				if err = conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
					return err
				}
				if _, err = conn.Write([]byte("ping")); err != nil {
					return err
				}
				var response [4]byte
				_, err = io.ReadFull(conn, response[:])
				if err == nil && string(response[:]) != "ping" {
					return fmt.Errorf("unexpected echo: %q", response)
				}
				return err
			}
			target := startTarget()
			require.NoError(t, ping())
			require.NoError(t, target.Close())
			require.ErrorIs(t, ping(), naive.ErrDestinationUnreachable)
			startTarget()
			require.Eventually(t, func() bool { return ping() == nil }, 4*time.Second, 100*time.Millisecond,
				"the restarted target must recover without waiting for the five-minute route cooldown")
			require.NoError(t, ping(), "recovery must also lift the cached refusal for subsequent requests")
		})
	}
}
