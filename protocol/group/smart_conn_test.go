package group

import (
	"errors"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIsHardConnErrorTransportOnly pins the invariant the whole abrupt-death
// failover rests on (G1/G17): only a client↔proxy transport failure is a "hard"
// error. Target-level / stream-level outcomes (the naive server already sent 200
// before dialing the target, so a dead target surfaces as EOF / a stream error)
// and benign closes must NOT be classified as hard, or reportConnFailure would
// wrongly down a healthy proxy because the *target* was dead.
func TestIsHardConnErrorTransportOnly(t *testing.T) {
	// Hard (client↔proxy transport is gone) — via errno, independent of text.
	require.True(t, isHardConnError(syscall.ECONNREFUSED))
	require.True(t, isHardConnError(syscall.ECONNRESET))
	require.True(t, isHardConnError(syscall.EHOSTUNREACH))
	require.True(t, isHardConnError(syscall.ENETUNREACH))
	require.True(t, isHardConnError(&net.OpError{Op: "read", Err: syscall.ECONNRESET}))

	// Not hard: benign close / EOF / stream-level / target-level errors. These are
	// how a dead *target* surfaces through the tunnel; downing the node on them
	// would let one dead destination fell a healthy proxy.
	require.False(t, isHardConnError(nil))
	require.False(t, isHardConnError(io.EOF))
	require.False(t, isHardConnError(net.ErrClosed))
	require.False(t, isHardConnError(errors.New("http2: stream closed")))
	require.False(t, isHardConnError(errors.New("http2 protocol error")))
	require.False(t, isHardConnError(syscall.ETIMEDOUT)) // timeout handled by the verdict path, not here
	require.False(t, isHardConnError(errors.New("i/o timeout")))
}

// TestReportConnFailureNotTriggeredByStreamError confirms the failoverConn
// wrapper only reports transport-hard errors upward: a stream/target error read
// through the wrapper is passed through without reporting (G1).
func TestReportConnFailureNotTriggeredByStreamError(t *testing.T) {
	reported := 0
	fc := &failoverConn{
		Conn:   errConn{err: errors.New("http2 protocol error")},
		report: func(error) { reported++ },
	}
	_, err := fc.Read(make([]byte, 1))
	require.Error(t, err)
	require.Equal(t, 0, reported, "stream/target error must not report a node failure")

	// A transport-hard error does report, exactly once.
	fc2 := &failoverConn{
		Conn:   errConn{err: syscall.ECONNRESET},
		report: func(error) { reported++ },
	}
	_, _ = fc2.Read(make([]byte, 1))
	_, _ = fc2.Read(make([]byte, 1))
	require.Equal(t, 1, reported, "hard error reports exactly once")
}

// errConn is a net.Conn whose Read/Write always fail with a fixed error.
type errConn struct {
	net.Conn
	err error
}

func (c errConn) Read([]byte) (int, error)  { return 0, c.err }
func (c errConn) Write([]byte) (int, error) { return 0, c.err }
