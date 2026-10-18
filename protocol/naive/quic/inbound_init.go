package quic

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/sagernet/quic-go"
	"github.com/sagernet/quic-go/congestion"
	"github.com/sagernet/quic-go/http3"
	"github.com/sagernet/sing-box/common/listener"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing-quic"
	congestion_meta1 "github.com/sagernet/sing-quic/congestion_meta1"
	congestion_meta2 "github.com/sagernet/sing-quic/congestion_meta2"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/common/ntp"
)

func init() {
	naive.ConfigureHTTP3ListenerFunc = func(ctx context.Context, logger logger.Logger, listener *listener.Listener, handler http.Handler, tlsConfig tls.ServerConfig, options option.NaiveInboundOptions) (io.Closer, error) {
		err := qtls.ConfigureHTTP3(tlsConfig)
		if err != nil {
			return nil, err
		}
		// QUIC negotiates h3 alone, as any h3 server does. ConfigureHTTP3
		// drops the config http3.ConfigureTLSConfig returns, so h3 used to be
		// added to the config TCP shares instead — and then TCP took h3, and
		// QUIC h2 and http/1.1.
		if _, isQUIC := tlsConfig.(qtls.ServerConfig); !isQUIC {
			tlsConfig = h3OnlyConfig{tlsConfig}
		} else if !common.Contains(tlsConfig.NextProtos(), http3.NextProtoH3) {
			tlsConfig.SetNextProtos(append(append([]string{}, tlsConfig.NextProtos()...), http3.NextProtoH3))
		}

		udpConn, err := listener.ListenUDP()
		if err != nil {
			return nil, err
		}

		var congestionControl func(conn *quic.Conn) congestion.CongestionControl
		timeFunc := ntp.TimeFuncFromContext(ctx)
		if timeFunc == nil {
			timeFunc = time.Now
		}
		switch options.QUICCongestionControl {
		case "", "bbr":
			congestionControl = func(conn *quic.Conn) congestion.CongestionControl {
				return congestion_meta2.NewBbrSenderWithProfile(conn.InitialPacketSize(), congestion_meta2.ProfileStandard)
			}
		case "cubic":
			congestionControl = func(conn *quic.Conn) congestion.CongestionControl {
				return congestion_meta1.NewCubicSender(
					congestion_meta1.DefaultClock{TimeFunc: timeFunc},
					conn.InitialPacketSize(),
					false,
				)
			}
		case "reno":
			congestionControl = func(conn *quic.Conn) congestion.CongestionControl {
				return congestion_meta1.NewCubicSender(
					congestion_meta1.DefaultClock{TimeFunc: timeFunc},
					conn.InitialPacketSize(),
					true,
				)
			}
		default:
			return nil, E.New("unknown quic congestion control: ", options.QUICCongestionControl)
		}

		quicListener, err := qtls.ListenEarly(udpConn, tlsConfig, &quic.Config{
			// Per connection, before anyone has authenticated. Unbounded, each
			// stream could pin a parked handler. What bounds an unauthenticated
			// stream now is serveConn's read deadline on its head and
			// MaxHeaderBytes: a goroutine and at most 4KiB for WebTimeout, and
			// another connection costs an attacker next to nothing. 256 only
			// capped honest clients: Chromium puts every tunnel to one server
			// on one QUIC session and queues past the limit, and a cronet
			// outbound got about 255 tunnels at once through an h3 relay, the
			// rest waiting for one to end.
			MaxIncomingStreams: 1024,
			Allow0RTT:          true,
			DisablePathManager: true,

			// The default is 100. An h3 client needs three — control and
			// the two QPACK streams — and each one opened unauthenticated
			// holds a goroutine until the connection ends: a unidirectional
			// stream cannot be given a read deadline, since the control
			// stream must stay open.
			MaxIncomingUniStreams: 8,
		})
		if err != nil {
			udpConn.Close()
			return nil, err
		}

		h3Server := &http3.Server{
			Handler: handler,
			// The default is 1MiB, and http3 allocates the length a HEADERS
			// frame declares before reading it: five bytes on a stream bought
			// a megabyte for as long as the client held the stream open. A
			// naive CONNECT head is well under a kilobyte. Across all 1024
			// streams of a connection this still pins up to 4MiB.
			MaxHeaderBytes: 4 << 10,
			ConnContext: func(ctx context.Context, conn *quic.Conn) context.Context {
				conn.SetCongestionControl(congestionControl(conn))
				return log.ContextWithNewID(ctx)
			},
		}

		live := &liveConns{EarlyListener: quicListener, conns: make(map[*quic.Conn]struct{})}
		go func() {
			// The CLI cancels the box's context before it calls Close on a
			// reload, and this loop's exit is then what closes the socket:
			// the connections are told first, or they are destroyed silently
			// and their clients wait out the idle timeout.
			defer func() {
				live.closeAll()
				udpConn.Close()
			}()
			for {
				conn, err := quicListener.Accept(ctx)
				if err != nil {
					if !E.IsClosedOrCanceled(err) {
						logger.Error("http3 server closed: ", err)
					}
					return
				}
				live.add(conn)
				go serveConn(h3Server, conn)
			}
		}()

		return live, nil
	}
	naive.WrapError = qtls.WrapError
}

// serveConn is http3's own connection loop, ServeListener's, with one thing
// added: a request stream may take WebTimeout to deliver its head. Without it
// a client that only completed the QUIC handshake could open streams and
// send nothing, each holding a goroutine and its stream for as long as the
// connection lived; HTTP/1 has had the same bound since ReadHeaderTimeout.
// ServeHTTP clears it once a request authenticates as a tunnel.
//
// What the loop leaves out — GOAWAY on a graceful Close, the connection count
// it waits on — only serves http3.Server.Close, which nothing here calls: the
// listener is closed instead, after liveConns has told each connection.
func serveConn(server *http3.Server, conn *quic.Conn) {
	raw, err := server.NewRawServerConn(conn)
	if err != nil {
		conn.CloseWithError(quic.ApplicationErrorCode(http3.ErrCodeInternalError), "")
		return
	}
	go func() {
		for {
			stream, acceptErr := conn.AcceptUniStream(conn.Context())
			if acceptErr != nil {
				return
			}
			go raw.HandleUnidirectionalStream(stream)
		}
	}()
	for {
		stream, acceptErr := conn.AcceptStream(conn.Context())
		if acceptErr != nil {
			return
		}
		_ = stream.SetReadDeadline(time.Now().Add(naive.WebTimeout))
		go raw.HandleRequestStream(stream)
	}
}

// h3OnlyConfig is what qtls.ListenEarly reads a standard config through:
// h3 as the only protocol, the TCP listener's config left alone.
type h3OnlyConfig struct{ tls.ServerConfig }

func (c h3OnlyConfig) STDConfig() (*tls.STDConfig, error) {
	config, err := c.ServerConfig.STDConfig()
	if err != nil {
		return nil, err
	}
	return http3.ConfigureTLSConfig(config), nil
}

// liveConns is the listener with every connection the accept loop handed to
// serveConn and has not ended, so that Close can tell each one it is over.
// Closing the transport destroys them with no CONNECTION_CLOSE, and a client
// keeps sending on a connection the instance reloaded onto the same port does
// not know — which drops the packets, having no stateless reset key — until
// its own loss detection gives up: every request of the next six seconds hung
// and failed, on a relay that was back within milliseconds.
type liveConns struct {
	qtls.EarlyListener
	access sync.Mutex
	conns  map[*quic.Conn]struct{}
	closed bool
}

// Close tells the connections before it closes the listener: closing the
// listener ends the accept loop, whose exit closes the socket the
// CONNECTION_CLOSEs leave through.
func (l *liveConns) Close() error {
	l.closeAll()
	return l.EarlyListener.Close()
}

func (l *liveConns) add(conn *quic.Conn) {
	l.access.Lock()
	if l.closed {
		// Accepted after closeAll took its list: told now, or it would be
		// destroyed silently with the socket like any connection never told.
		l.access.Unlock()
		conn.CloseWithError(quic.ApplicationErrorCode(http3.ErrCodeNoError), "")
		return
	}
	l.conns[conn] = struct{}{}
	l.access.Unlock()
	context.AfterFunc(conn.Context(), func() {
		l.access.Lock()
		delete(l.conns, conn)
		l.access.Unlock()
	})
}

// closeAll sends each connection a CONNECTION_CLOSE and waits for it to go
// out. H3_NO_ERROR: the server is going away, which is no fault of the client's.
// Outside the lock, because CloseWithError returns once the connection has
// ended, and ending it is what removes it.
func (l *liveConns) closeAll() {
	l.access.Lock()
	l.closed = true
	conns := make([]*quic.Conn, 0, len(l.conns))
	for conn := range l.conns {
		conns = append(conns, conn)
	}
	l.access.Unlock()
	for _, conn := range conns {
		conn.CloseWithError(quic.ApplicationErrorCode(http3.ErrCodeNoError), "")
	}
}
