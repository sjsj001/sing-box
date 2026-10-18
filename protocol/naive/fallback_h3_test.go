//go:build with_quic

package naive

import (
	"bytes"
	"context"
	stdtls "crypto/tls"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/sagernet/quic-go"
	"github.com/sagernet/quic-go/http3"
	"github.com/sagernet/sing-box/common/tls"

	"github.com/stretchr/testify/require"
)

// The h3 server shares the handler, and an h3 client can withhold stream
// credit just as an h2 one withholds its window. This fails if a quic-go
// update drops the writer's SetWriteDeadline, whose error the handler ignores.
func TestAnH3FallbackPageAClientWillNotReceiveIsAbandoned(t *testing.T) {
	shortenWebTimeout(t)
	inbound, _ := fallbackInbound(t)
	served := make(chan error, 1)
	inbound.fallbackHandler = http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, err := writer.Write(bytes.Repeat([]byte("x"), 1<<20))
		served <- err
	})

	certificate, err := tls.GenerateKeyPair(nil, nil, time.Now, "example.com")
	require.NoError(t, err)
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	server := &http3.Server{Handler: inbound, TLSConfig: http3.ConfigureTLSConfig(&stdtls.Config{Certificates: []stdtls.Certificate{*certificate}})}
	go server.Serve(udp)
	t.Cleanup(func() { server.Close() })

	transport := &http3.Transport{
		TLSClientConfig: &stdtls.Config{InsecureSkipVerify: true},
		QUICConfig: &quic.Config{
			InitialStreamReceiveWindow:     16 << 10,
			MaxStreamReceiveWindow:         16 << 10,
			InitialConnectionReceiveWindow: 64 << 10,
			MaxConnectionReceiveWindow:     64 << 10,
			KeepAlivePeriod:                time.Second,
		},
	}
	defer transport.Close()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+udp.LocalAddr().String()+"/", nil)
	require.NoError(t, err)
	response, err := transport.RoundTrip(request)
	require.NoError(t, err)
	defer response.Body.Close() // never read, so the window stays shut

	select {
	case err := <-served:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the h3 fallback handler is still parked on a write the client will never accept")
	}
}
