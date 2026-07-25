package group

import (
	"errors"
	"net"
	"strings"
	"sync"
	"syscall"
)

// failoverConn wraps a member connection and reports the first hard connection
// error (refused/reset/no-route) back to the group, so an abruptly dead node —
// whose failure surfaces only on read/write because naive dials optimistically
// (early data) — is marked down immediately and its stickies dropped. The next
// request then recovers without waiting for the ping loop (§4/§7).
type failoverConn struct {
	net.Conn
	report func(error)
	once   sync.Once
}

func (c *failoverConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if isHardConnError(err) {
		c.once.Do(func() { c.report(err) })
	}
	return n, err
}

func (c *failoverConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if isHardConnError(err) {
		c.once.Do(func() { c.report(err) })
	}
	return n, err
}

// Upstream lets sing's connection helpers see through the wrapper.
func (c *failoverConn) Upstream() any { return c.Conn }

// isHardConnError reports whether err indicates the proxy path is unreachable
// (as opposed to a normal close/EOF or a benign timeout). It matches on errno
// semantics first — the cronet-go fork's NetError implements errors.Is against
// these syscall errnos, so this catches "address unreachable" (net_error -109),
// which the old string set missed, and stays correct across Chromium error-text
// changes. A string fallback covers wrappers that don't expose an errno (G17).
// Timeouts and normal closes are deliberately excluded: they are not hard
// failures and are handled by the suspect/verdict path.
func isHardConnError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, syscall.EHOSTUNREACH) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "connection refused") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "no route to host") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "network is unreachable") ||
		strings.Contains(s, "address unreachable")
}
