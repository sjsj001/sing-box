//go:build linux

package naive

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing/common/bufio"

	"github.com/stretchr/testify/require"
)

func loopbackPair(t testing.TB) (server, client net.Conn) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	client, err = net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	server, err = listener.Accept()
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	return server, client
}

func TestWrapPacedReadsTCPInfo(t *testing.T) {
	server, client := loopbackPair(t)
	require.Equal(t, server, wrapPaced(server, 0))
	behindProxy, isPaced := wrapPaced(&bufio.AddrConn{Conn: server}, testTarget).(*pacedConn)
	require.True(t, isPaced, "reached through Upstream, as with proxy_protocol")

	paced := wrapPaced(server, testTarget).(*pacedConn)
	require.Same(t, behindProxy.path, paced.path, "one path per peer address")
	go io.Copy(io.Discard, client)
	_, err := paced.Write(make([]byte, 1<<20))
	require.NoError(t, err)
	read := func() (paceInfo, error) {
		paced.access.Lock()
		defer paced.access.Unlock()
		return paced.info()
	}
	require.Eventually(t, func() bool {
		info, err := read()
		return err == nil && info.rate > 0 && info.minRTT > 0 && info.minRTT <= info.rtt
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, paced.Close())
	require.NotContains(t, paced.path.members, paced)
	_, err = read()
	require.Error(t, err, "a closed socket opens the gate")
}

// BenchmarkPacedWrite writes TLS-record-sized chunks over loopback, a path
// without a queue, where the gate must cost nothing.
func BenchmarkPacedWrite(b *testing.B) {
	for _, name := range []string{"raw", "paced"} {
		b.Run(name, func(b *testing.B) {
			server, client := loopbackPair(b)
			go io.Copy(io.Discard, client)
			conn := server
			var reads int
			if name == "paced" {
				paced := wrapPaced(server, testTarget).(*pacedConn)
				info := paced.info
				paced.info = func() (paceInfo, error) {
					reads++
					return info()
				}
				conn = paced
			}
			record := make([]byte, testRecord)
			b.SetBytes(testRecord)
			b.ResetTimer()
			for range b.N {
				if _, err := conn.Write(record); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(reads)/float64(b.N), "tcpinfo/op")
		})
	}
}
