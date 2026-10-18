//go:build !linux

package naive

import (
	"net"
	"time"
)

// wrapPaced gates only on Linux, where pacedConn reads TCP_INFO.
func wrapPaced(conn net.Conn, _ time.Duration) net.Conn {
	return conn
}
