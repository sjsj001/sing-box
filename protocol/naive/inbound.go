package naive

import (
	"context"
	"errors"
	"io"
	stdlog "log"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/common/listener"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/common/uot"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/auth"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	sHttp "github.com/sagernet/sing/protocol/http"
)

var (
	ConfigureHTTP3ListenerFunc func(ctx context.Context, logger logger.Logger, listener *listener.Listener, handler http.Handler, tlsConfig tls.ServerConfig, options option.NaiveInboundOptions) (io.Closer, error)
	WrapError                  func(error) error
)

func RegisterInbound(registry *inbound.Registry) {
	inbound.Register[option.NaiveInboundOptions](registry, C.TypeNaive, NewInbound)
}

type Inbound struct {
	inbound.Adapter
	ctx              context.Context
	router           adapter.ConnectionRouterEx
	logger           logger.ContextLogger
	options          option.NaiveInboundOptions
	listener         *listener.Listener
	network          []string
	networkIsDefault bool
	authenticator    *auth.Authenticator
	tlsConfig        tls.ServerConfig
	httpServer       *http.Server
	closing          atomic.Bool
	h3Server         io.Closer
	fallbackHandler  http.Handler // 静态文件服务
}

func NewInbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.NaiveInboundOptions) (adapter.Inbound, error) {
	inbound := &Inbound{
		Adapter: inbound.NewAdapter(C.TypeNaive, tag),
		ctx:     ctx,
		router:  uot.NewRouter(router, logger),
		logger:  logger,
		options: options,
		listener: listener.New(listener.Options{
			Context: ctx,
			Logger:  logger,
			Listen:  options.ListenOptions,
		}),
		networkIsDefault: options.Network == "",
		network:          options.Network.Build(),
		authenticator:    auth.NewAuthenticator(options.Users),
	}

	// 初始化 fallback 静态文件服务器（/var/www/html）
	fileServer := http.FileServerFS(webRoot("/var/www/html"))
	inbound.fallbackHandler = fileServer

	if common.Contains(inbound.network, N.NetworkUDP) {
		if options.TLS == nil || !options.TLS.Enabled {
			return nil, E.New("TLS is required for QUIC server")
		}
	}
	if len(options.Users) == 0 {
		return nil, E.New("missing users")
	}
	for _, user := range options.Users {
		// Either one may be empty, as naiveproxy allows, but not both: that
		// user is "Basic Og==", which anyone can send — an open proxy.
		if user.Username == "" && user.Password == "" {
			return nil, E.New("missing username and password")
		}
	}
	if options.TLS != nil {
		tlsConfig, err := tls.NewServer(ctx, logger, common.PtrValueOrDefault(options.TLS))
		if err != nil {
			return nil, err
		}
		inbound.tlsConfig = tlsConfig
	}
	return inbound, nil
}

func (n *Inbound) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	// One cleanup, not one per resource: the scope runs cleanups in reverse,
	// and Close needs its own order — see there.
	scope.Add(n.Close)
	if n.tlsConfig != nil {
		err := n.tlsConfig.Start()
		if err != nil {
			return E.Cause(err, "create TLS config")
		}
	}
	if common.Contains(n.network, N.NetworkTCP) {
		tcpListener, err := n.listener.ListenTCP()
		if err != nil {
			return err
		}
		n.httpServer = n.newHTTPServer()
		listener := net.Listener(tcpListener)
		if n.tlsConfig != nil {
			listener = n.tlsListener(tcpListener)
		}
		go func() {
			sErr := n.httpServer.Serve(listener)
			if sErr != nil && !errors.Is(sErr, http.ErrServerClosed) {
				n.logger.Error("http server serve error: ", sErr)
			}
		}()
	}

	if common.Contains(n.network, N.NetworkUDP) {
		http3Server, err := ConfigureHTTP3ListenerFunc(n.ctx, n.logger, n.listener, n, n.tlsConfig, n.options)
		if err == nil {
			n.h3Server = http3Server
		} else if len(n.network) > 1 {
			n.logger.Warn(E.Cause(err, "naive http3 disabled"))
		} else {
			return err
		}
	}

	return nil
}

// WebTimeout bounds what a request that is not a tunnel may hold: reading a
// request head — on an HTTP/1 connection, and on an HTTP/3 request stream —
// and writing the fallback site. Tunnels
// never meet it — net/http clears the connection's deadlines once a request
// head is read, or once it has the h2 preface, and only the fallback handler
// sets a write deadline. On HTTP/3
// the bound is a read deadline on the request stream, which ServeHTTP clears
// once the request authenticates as a tunnel.
// It also bounds the TLS handshake. On the standard TLS path net/http gives
// the handshake ReadHeaderTimeout, so handshake_timeout has no effect there.
// On the lazy path (reality, kTLS) the handshake runs inside the first read,
// so it must stay above the default handshake cap of 15s, and a
// handshake_timeout set above WebTimeout is cut to WebTimeout.
// A variable so tests can shorten it.
var WebTimeout = 30 * time.Second

// newHTTPServer serves HTTP/1 and net/http's own HTTP/2. As made here it
// takes h2 by prior knowledge, which is what the plain path and the lazy TLS
// path need — lazyConn is not a *tls.Conn, so net/http sees no ALPN there;
// tlsListener turns that off and h2 by ALPN on for the standard TLS path.
func (n *Inbound) newHTTPServer() *http.Server {
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	server := &http.Server{
		Handler: n,
		BaseContext: func(listener net.Listener) context.Context {
			return n.ctx
		},
		Protocols: &protocols,
		HTTP2: &http.HTTP2Config{
			// Upload direction (client → proxy). CONNECT upload today is
			// interactive-sized, so these bound what one connection can
			// pin in server memory instead of the 128/256MB they used to
			// allow; raise them if bulk upload ever becomes a workload.
			MaxReceiveBufferPerStream:     8 * 1024 * 1024,
			MaxReceiveBufferPerConnection: 16 * 1024 * 1024,
			// The default is 1MiB, and the h2 server allocates the length a
			// frame header declares before reading the frame: nine bytes
			// after the preface bought a megabyte, unauthenticated, until
			// IdleTimeout. 16KiB is the protocol's minimum, so every client
			// already sends within it; Chromium never sends a larger frame.
			MaxReadFrameSize: 16 << 10,
		},
		// A connection still on its way into h2 when Close runs — in its TLS
		// handshake, or yet to send its preface — is recorded by the h2
		// server after Shutdown told the recorded ones to GOAWAY, and nothing
		// would end it. The h2 server reports a connection active right after
		// recording it, once it has the preface. Close raises the flag before
		// Shutdown, so one that finds it down here was recorded in time for
		// the GOAWAY; one that finds it up is closed instead of served. An
		// HTTP/1 connection whose next request arrives after Close is closed
		// the same way, rather than served by this instance.
		ConnState: func(conn net.Conn, state http.ConnState) {
			if state == http.StateActive && n.closing.Load() {
				conn.Close()
			}
		},
		// ReadHeaderTimeout bounds an HTTP/1 request head and, on the plain
		// and lazy paths, the wait for the h2 preface; IdleTimeout an HTTP/1
		// keep-alive connection and an h2 connection with zero open streams
		// — a parked client pool slot. The h2 timer only runs while no
		// stream is open, so a long-lived CONNECT or UoT session is never
		// cut, however silent it is. Without them a connection that never
		// sends a request, or idles after fetching the fallback page, is held
		// open forever.
		ReadHeaderTimeout: WebTimeout,
		IdleTimeout:       120 * time.Second,
		// Bounds an HTTP/1 request head and an h2 header list. At the 1MiB
		// default an unauthenticated h2 header block that never ended held
		// a megabyte until IdleTimeout. A CONNECT head is well under a
		// kilobyte.
		MaxHeaderBytes: 64 << 10,
		ErrorLog:       stdlog.New(debugWriter{n.logger}, "", 0),
		// Never WriteTimeout or ReadTimeout: HTTP/2 applies them to every
		// stream, which would cut long-lived tunnels.
	}
	keepRoundRobinWriteScheduler(server)
	return server
}

func (n *Inbound) Close() error {
	// A reload closes this instance and starts another in the same process.
	// Without a GOAWAY, a client's h2 session stays with this instance, whose
	// router is gone, so every CONNECT on it is answered 502 — and the
	// session never idles out while the client keeps using it. Shutdown with
	// an expired context sends the GOAWAY without waiting: existing streams
	// may finish, the session closes after them, and the client opens its
	// next one against the new instance. httpServer.Close is not called
	// after it: net/http tracks the h2 connections it serves, and would cut
	// them before the GOAWAY went out. An HTTP/1 connection still
	// writing the fallback page ends at its WebTimeout write deadline instead.
	// A UDP-only inbound has no httpServer. closing goes up before Shutdown
	// for the connections still on their way in; see ConnState.
	//
	// The h3 server goes before the listener for the same reason: its Close
	// tells each QUIC connection it is over, and the CONNECTION_CLOSE leaves
	// through the UDP socket closeListener closes. Closed in the other order,
	// a client kept sending on a connection the next instance did not know,
	// and every request of its next six seconds hung and then failed.
	n.closing.Store(true)
	if n.httpServer != nil {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = n.httpServer.Shutdown(ctx)
	}
	return common.Close(
		n.h3Server,
		common.Closer(n.closeListener),
		n.tlsConfig,
	)
}

// closeListener closes the TCP and UDP sockets. Shutdown has already closed
// the TCP one, which is an error here and nothing to report: returned, it
// made every Close fail. A listener whose Close does nothing would not do:
// Shutdown waits for Serve to return, and Serve returns only once its
// listener is closed.
func (n *Inbound) closeListener() error {
	err := n.listener.Close()
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

// ServeHTTP is a thin shell so that an HTTP/2 or HTTP/3 tunnel's handler,
// which lasts as long as the tunnel, waits on a shallow stack. A parked
// goroutine's stack is halved only while less than a quarter of it is in use:
// parked under serveHTTP's and newConnection's frames, about 4 KB with the
// metadata, every idle tunnel's handler kept 16 KiB. Up here it keeps 4 KiB.
func (n *Inbound) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if wait := n.serveHTTP(writer, request); wait != nil {
		wait()
	}
}

// serveHTTP returns what an HTTP/2 or HTTP/3 tunnel's handler waits for
// before it returns, and nil for anything else.
func (n *Inbound) serveHTTP(writer http.ResponseWriter, request *http.Request) (wait func()) {
	ctx := log.ContextWithNewID(request.Context())

	// 检查是否是 CONNECT 请求
	if request.Method != "CONNECT" {
		n.handleFallback(writer, request)
		return nil
	}

	// 检查 Padding 头
	if request.Header.Get("Padding") == "" {
		n.handleFallback(writer, request)
		return nil
	}

	// 检查认证
	userName, password, authOk := sHttp.ParseBasicAuth(request.Header.Get("Proxy-Authorization"))
	if authOk {
		authOk = n.authenticator.Verify(userName, password)
	}
	if !authOk {
		n.handleFallback(writer, request)
		return nil
	}
	// The HTTP/3 server bounds reading a request stream by WebTimeout, so a
	// client cannot hold one open by sending nothing. This is a tunnel now,
	// whose body may rightly sit idle for hours. The error is ignored: the
	// other protocols have nothing set here to clear.
	_ = http.NewResponseController(writer).SetReadDeadline(time.Time{})

	// 认证成功，正常处理
	// Smart-aware clients get the response held back until the destination dial
	// has settled, so it can carry the dial duration and report a status when
	// the dial fails. Everyone else keeps the original immediate reply.
	deferResponse := request.Header.Get(headerSmart) != ""
	flusher := writer.(http.Flusher)

	hostPort := request.Header.Get("-connect-authority")
	if hostPort == "" {
		hostPort = request.URL.Host
		if hostPort == "" {
			hostPort = request.Host
		}
	}
	source := sHttp.SourceAddress(request)
	destination := M.ParseSocksaddr(hostPort).Unwrap()

	if IsProbe(destination) {
		// Answered here, with nothing dialed: the round trip a client measures
		// from this is purely its own leg to this proxy.
		writer.Header().Set("Padding", generatePaddingHeader())
		writer.Header().Set(ConnectAckHeader, FormatConnectAck(ConnectAck{HasConnect: true}))
		writer.WriteHeader(http.StatusOK)
		flusher.Flush()
		return nil
	}

	if hijacker, isHijacker := writer.(http.Hijacker); isHijacker {
		if !deferResponse {
			writer.Header().Set("Padding", generatePaddingHeader())
			writer.WriteHeader(http.StatusOK)
			flusher.Flush()
		}
		conn, reader, err := hijacker.Hijack()
		if err != nil {
			n.badRequest(ctx, request, E.New("hijack failed"))
			return nil
		}
		if cacheLen := reader.Reader.Buffered(); cacheLen > 0 {
			cache := buf.NewSize(cacheLen)
			_, err = cache.ReadFullFrom(reader.Reader, cacheLen)
			if err != nil {
				cache.Release()
				conn.Close()
				n.badRequest(ctx, request, E.Cause(err, "read cache"))
				return nil
			}
			conn = bufio.NewCachedConn(conn, cache)
		}
		var lazyResponder *responder
		if deferResponse {
			var timing *dialer.ConnectTiming
			ctx, timing = dialer.WithConnectTiming(ctx)
			lazyResponder = newResponder(ctx, n.logger, timing, hijackedResponseWriter(conn, request.ProtoMajor, request.ProtoMinor))
		}
		n.newConnection(ctx, &naiveConn{Conn: conn, responder: lazyResponder}, userName, source, destination, nil)
	} else {
		var lazyResponder *responder
		if deferResponse {
			var timing *dialer.ConnectTiming
			ctx, timing = dialer.WithConnectTiming(ctx)
			lazyResponder = newResponder(ctx, n.logger, timing, h2ResponseWriter(writer, flusher))
		} else {
			writer.Header().Set("Padding", generatePaddingHeader())
			writer.WriteHeader(http.StatusOK)
			flusher.Flush()
		}
		stopClose := func() bool { return false }
		if request.ProtoMajor == 2 {
			// The h2 server gives a reset stream's unread upload back to the
			// connection window only once its body is closed — net/http's as
			// x/net's since GO-2026-6612 — and the copy
			// route.ConnectionManager runs does not watch this context: a
			// tunnel whose destination had stopped reading kept its share of
			// the connection's upload budget until the destination moved.
			// The context also ends with the inbound's own, which a reload
			// cancels before Close: tunnels then end at the cancel instead of
			// in Close, the same way and with the same trace log.
			// HTTP/3 needs none of it: quic-go gives a reset stream's unread
			// data back to the connection when the reset arrives.
			stopClose = context.AfterFunc(request.Context(), func() { request.Body.Close() })
		}
		tunnel := newH2Tunnel(&naiveH2Conn{
			reader:         request.Body,
			writer:         writer,
			flusher:        flusher,
			remoteAddress:  source,
			emptyReadWaits: request.ProtoMajor == 2,
			responder:      lazyResponder,
		})
		done := make(chan struct{})
		n.newConnection(ctx, tunnel, userName, source, destination, N.OnceClose(func(error) {
			close(done)
		}))
		return func() {
			<-done
			tunnel.CloseWrapper()
			stopClose()
			// The writer belongs to this handler and dies with it, while the
			// response can still be resolved from a goroutine — see
			// responder.finish.
			lazyResponder.finish()
		}
	}
	return nil
}

// handleFallback answers anything that is not an authenticated naive CONNECT
// the way a plain web server would.
//
// Anything but GET and HEAD gets a bodiless 405, as nginx and Caddy give a
// CONNECT; a file server would answer it 200 with the page. For the page
// itself the write is bounded: a client that advertises a zero flow-control
// window would otherwise park the handler, and the upload buffer behind it,
// forever — without ever authenticating. The deadline is refreshed by each
// fallback request, so a keep-alive connection serving the page again later
// is unaffected. HTTP/1, HTTP/2 and HTTP/3 all support it; the error is
// ignored so that a writer which does not still serves the page.
func (n *Inbound) handleFallback(writer http.ResponseWriter, request *http.Request) {
	n.logger.DebugContext(request.Context(), "fallback to static files for ", request.RemoteAddr)
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		writer.Header().Set("Allow", "GET, HEAD")
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	request.Body.Close()
	_ = http.NewResponseController(writer).SetWriteDeadline(time.Now().Add(WebTimeout))
	n.fallbackHandler.ServeHTTP(writer, request)
}

func (n *Inbound) newConnection(ctx context.Context, conn net.Conn, userName string, source M.Socksaddr, destination M.Socksaddr, onClose N.CloseHandlerFunc) {
	if userName != "" {
		n.logger.InfoContext(ctx, "[", userName, "] inbound connection from ", source)
		n.logger.InfoContext(ctx, "[", userName, "] inbound connection to ", destination)
	} else {
		n.logger.InfoContext(ctx, "inbound connection from ", source)
		n.logger.InfoContext(ctx, "inbound connection to ", destination)
	}
	var metadata adapter.InboundContext
	metadata.Inbound = n.Tag()
	metadata.InboundType = n.Type()
	//nolint:staticcheck
	metadata.InboundDetour = n.listener.ListenOptions().Detour
	//nolint:staticcheck
	metadata.Source = source
	metadata.Destination = destination
	metadata.OriginDestination = M.SocksaddrFromNet(conn.LocalAddr()).Unwrap()
	metadata.User = userName
	n.router.RouteConnectionEx(ctx, conn, metadata, onClose)
}

func (n *Inbound) badRequest(ctx context.Context, request *http.Request, err error) {
	n.logger.ErrorContext(ctx, E.Cause(err, "process connection from ", request.RemoteAddr))
}
