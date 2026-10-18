package naive

import (
	"context"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/logger"
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

// lazyConn must not gain a ConnectionState method: net/http would take it for
// TLS, skip the h2 preface it serves the lazy path by, and answer every h2
// client as a bad HTTP/1 request.
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

// tlsListener wraps the TCP listener in the inbound's TLS, and readies
// httpServer for what comes out of it.
//
// A standard config gets stdListener, which hands net/http a *tls.Conn as
// any Go server does: net/http runs the handshake, answers plain HTTP with
// a 400, and serves h2 or HTTP/1 by what ALPN chose, h2 starting with its
// SETTINGS. h2 by prior knowledge stays off inside TLS — a proxy keeping TLS
// outside its HTTP server and h2 by prior knowledge inside it was told apart
// by a single handshake.
// reality and kTLS do not hand back a *tls.Conn and keep the lazy path, where
// net/http sees plain text and h2 comes by prior knowledge whatever ALPN chose.
func (n *Inbound) tlsListener(tcpListener net.Listener) net.Listener {
	if len(n.tlsConfig.NextProtos()) == 0 {
		n.tlsConfig.SetNextProtos([]string{"h2", "http/1.1"})
	} else if !common.Contains(n.tlsConfig.NextProtos(), "h2") {
		n.tlsConfig.SetNextProtos(append([]string{"h2"}, n.tlsConfig.NextProtos()...))
	}
	if std, isSTD := n.tlsConfig.(*tls.STDServerConfig); isSTD {
		n.httpServer.Protocols.SetUnencryptedHTTP2(false)
		n.httpServer.Protocols.SetHTTP2(true)
		return &stdListener{Listener: tcpListener, config: std, sendQueueTarget: n.options.SendQueueTarget.Build()}
	}
	return &lazyListener{Listener: tcpListener, config: n.tlsConfig}
}

// stdListener makes each connection with the config's own *crypto/tls.Config
// as it is at accept time, so ECH keys, certificate reloads, ALPN and
// reject_unknown_sni all apply. A fresh Config whose GetConfigForClient
// returns it would lose ECH: crypto/tls decrypts the ECH hello with the
// outer config's keys before it asks for another.
// The raw conn under each is paced at sendQueueTarget; see pacedConn.
type stdListener struct {
	net.Listener
	config          *tls.STDServerConfig
	sendQueueTarget time.Duration
}

func (l *stdListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return l.config.Server(wrapPaced(conn, l.sendQueueTarget))
}

// debugWriter takes what net/http logs — a line for every failed TLS
// handshake, which on a public port is every scanner — off stderr and into
// the inbound's logger at Debug. A recovered panic in a handler and an Accept
// failing in a loop (out of descriptors) used to reach the journal through
// stderr and still do, as errors.
type debugWriter struct{ logger logger.Logger }

func (w debugWriter) Write(p []byte) (int, error) {
	line := strings.TrimRight(string(p), "\n")
	if strings.HasPrefix(line, "http: panic serving") ||
		strings.HasPrefix(line, "http2: panic serving") ||
		strings.HasPrefix(line, "http: Accept error") {
		w.logger.Error(line)
	} else {
		w.logger.Debug(line)
	}
	return len(p), nil
}
