package naive

import (
	"context"
	"net"
	"sync"

	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing/common"
)

// lazyListener replaces aTLS.NewListener: sing's LazyConn reassigns its embedded
// Conn during the handshake, so its promoted Close races with http.Server.Close.
// Here the raw conn is never reassigned and the TLS conn lives in an atomic.
type lazyListener struct {
	net.Listener
	config tls.ServerConfig
}

func (l *lazyListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &lazyConn{Conn: conn, config: l.config}, nil
}

type lazyConn struct {
	net.Conn // raw conn, never reassigned
	config   tls.ServerConfig
	access   sync.Mutex
	tlsConn  common.TypedValue[tls.Conn]
}

func (c *lazyConn) handshake() (net.Conn, error) {
	if conn := c.tlsConn.Load(); conn != nil {
		return conn, nil
	}
	c.access.Lock()
	defer c.access.Unlock()
	if conn := c.tlsConn.Load(); conn != nil {
		return conn, nil
	}
	conn, err := tls.ServerHandshake(context.Background(), c.Conn, c.config)
	if err != nil {
		return nil, err
	}
	c.tlsConn.Store(conn)
	return conn, nil
}

func (c *lazyConn) Read(p []byte) (int, error) {
	conn, err := c.handshake()
	if err != nil {
		return 0, err
	}
	return conn.Read(p)
}

func (c *lazyConn) Write(p []byte) (int, error) {
	conn, err := c.handshake()
	if err != nil {
		return 0, err
	}
	return conn.Write(p)
}

// Close during a handshake closes the raw conn, which aborts the handshake.
func (c *lazyConn) Close() error {
	if conn := c.tlsConn.Load(); conn != nil {
		return conn.Close()
	}
	return c.Conn.Close()
}
