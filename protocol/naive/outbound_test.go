//go:build with_naive_outbound

package naive

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/cronet-go"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

func TestOutboundRejectsCertificateServerName(t *testing.T) {
	_, err := NewOutbound(context.Background(), nil, log.NewNOPFactory().Logger(), "", option.NaiveOutboundOptions{
		OutboundTLSOptionsContainer: option.OutboundTLSOptionsContainer{
			TLS: &option.OutboundTLSOptions{
				Enabled:               true,
				CertificateServerName: "certificate.example",
			},
		},
	})
	require.ErrorContains(t, err, "certificate_server_name is not supported on naive outbound")
}

// ackingConn is a proxy connection whose handshake has completed and whose
// response header is whatever the test wants to have arrived.
type ackingConn struct {
	cronet.NaiveConn
	timing cronet.ConnTiming
	header string
}

func (c *ackingConn) HandshakeContext(context.Context) error { return nil }
func (c *ackingConn) Timing() (cronet.ConnTiming, bool)      { return c.timing, true }

func (c *ackingConn) ResponseHeader(key string) (string, bool) {
	if key != ConnectAckHeader {
		return "", false
	}
	return c.header, true
}

func TestTheLookupTheExitReportedJoinsTheDestinationLeg(t *testing.T) {
	// The client half of the wiring, and the one place the decision is actually
	// made. Without it the resolution stays in the chain's interior — charged to
	// the node for whichever destination it was asked for — which is the whole
	// reason the field exists.
	conn := &dialConn{NaiveConn: &ackingConn{
		timing: cronet.ConnTiming{RoundTrip: 207500 * time.Microsecond},
		header: "51790;d=1040;n=50000",
	}}

	measurement, err := conn.Measure(context.Background())
	require.NoError(t, err)
	require.True(t, measurement.HasRemote)
	require.Equal(t, 51040*time.Microsecond, measurement.RemoteDial,
		"reaching the destination is the connect plus finding it")

	chain, ok := measurement.ChainSpan()
	require.True(t, ok)
	require.Equal(t, 750*time.Microsecond, chain,
		"what is left for the node is its own routing, not the lookup")
	require.Equal(t, measurement.RoundTrip,
		measurement.NearHop()+chain+measurement.RemoteDial,
		"and the three legs still add up to what the client timed")

	// An exit too old to report it leaves the lookup where it was.
	older := &dialConn{NaiveConn: &ackingConn{
		timing: cronet.ConnTiming{RoundTrip: 207500 * time.Microsecond},
		header: "51790;d=1040",
	}}
	olderMeasurement, err := older.Measure(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1040*time.Microsecond, olderMeasurement.RemoteDial)
	olderChain, ok := olderMeasurement.ChainSpan()
	require.True(t, ok)
	require.Equal(t, 50750*time.Microsecond, olderChain)
}

// countingLogger records how many debug lines were written, and takes the rest
// of the interface from the no-op so overriding one method is all it costs.
type countingLogger struct {
	logger.ContextLogger
	debug atomic.Int32
}

func (l *countingLogger) DebugContext(context.Context, ...any) { l.debug.Add(1) }

// measuringConn is a proxy connection whose measurement fails with whatever the
// test wants to have gone wrong.
type measuringConn struct {
	net.Conn
	err error
}

func (c *measuringConn) Measure(context.Context) (ConnMeasurement, error) {
	return ConnMeasurement{}, c.err
}
func (c *measuringConn) WaitReady(context.Context) error { return c.err }

func TestAMeasurementNobodyIsWaitingForIsNotReported(t *testing.T) {
	t.Parallel()
	// Every dial is measured twice — once by whoever ranks the path, once by
	// this detached line — so a tunnel closed from this side produces two
	// entries beside the real verdict. During one production outage that read
	// as the verdict having been swallowed, and an audit reported a bug that
	// was not there. The ranking draws this line in two places already; this is
	// the third.
	for _, test := range []struct {
		name     string
		err      error
		reported bool
	}{
		{"closed by this end", ErrClosedLocally, false},
		{"caller stopped waiting", context.Canceled, false},
		{"wrapped local close", fmt.Errorf("%w: %w", ErrNextHopUnreachable, ErrClosedLocally), false},
		{"the proxy refused", fmt.Errorf("%w: %w", ErrNextHopUnreachable, errors.New("connection reset")), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			counted := &countingLogger{ContextLogger: logger.NOP()}
			logMeasurement(context.Background(), counted, &measuringConn{err: test.err},
				M.ParseSocksaddr("example.com:443"))
			// Eventually for the line that must appear, Never for the one that
			// must not: the write happens on a detached goroutine, so a poll
			// that merely has not seen it yet is indistinguishable from silence
			// on the first tick — which is a test that passes either way.
			written := func() bool { return counted.debug.Load() > 0 }
			if test.reported {
				require.Eventually(t, written, time.Second, time.Millisecond)
				return
			}
			require.Never(t, written, 200*time.Millisecond, 5*time.Millisecond)
		})
	}
}
