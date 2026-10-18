package snell

import (
	"cmp"
	"context"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/common/listener"
	"github.com/sagernet/sing-box/common/uot"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	obfs "github.com/sagernet/sing-box/transport/simple-obfs"
	snellprotocol "github.com/sagernet/sing-snell"
	"github.com/sagernet/sing-snell/snellv5"
	"github.com/sagernet/sing-snell/snellv6"
	"github.com/sagernet/sing/common/auth"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	F "github.com/sagernet/sing/common/format"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func RegisterInbound(registry *inbound.Registry) {
	inbound.Register[option.SnellInboundOptions](registry, C.TypeSnell, NewInbound)
}

var _ adapter.TCPInjectableInbound = (*Inbound)(nil)

type Inbound struct {
	inbound.Adapter
	ctx        context.Context
	router     adapter.ConnectionRouterEx
	logger     logger.ContextLogger
	listener   *listener.Listener
	service    snellprotocol.Service
	users      []option.SnellUser
	version    int
	obfsMode   string
	udpNat     *quicProxyNATService
	quicAuth   *quicProxyAuthenticationService
	quicParser quicProxyInitParser
	// handshakeTimeout overrides C.TCPTimeout as the bound on a client sending
	// its request; zero means the constant. Set by tests.
	handshakeTimeout time.Duration
}

func NewInbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.SnellInboundOptions) (adapter.Inbound, error) {
	inbound := &Inbound{
		Adapter:  inbound.NewAdapter(C.TypeSnell, tag),
		ctx:      ctx,
		router:   uot.NewRouter(router, logger),
		logger:   logger,
		users:    options.Users,
		version:  options.Version,
		obfsMode: options.ObfsOptions.ObfsMode,
	}
	if err := validateSnellInboundObfs(options.Version, options.ObfsOptions.ObfsMode); err != nil {
		return nil, err
	}
	authentication := snellprotocol.MultiUserAuthenticationUserKey
	if options.MultiUserAuthentication == "psk" {
		authentication = snellprotocol.MultiUserAuthenticationPSK
	}
	var userList []int
	var keyList [][]byte
	if len(options.Users) > 0 {
		userList = make([]int, len(options.Users))
		keyList = make([][]byte, len(options.Users))
		for index, user := range options.Users {
			userList[index] = index
			if authentication == snellprotocol.MultiUserAuthenticationPSK {
				keyList[index] = []byte(user.PSK)
			} else {
				keyList[index] = []byte(user.UserKey)
			}
		}
	}
	var err error
	switch options.Version {
	case 5:
		serviceOptions := snellv5.ServiceOptions{
			PSK:                     []byte(options.PSK),
			Handler:                 inbound,
			MultiUserAuthentication: authentication,
		}
		inbound.service, err = newSnellV5Service(serviceOptions, userList, keyList)
	case 6:
		var mode snellv6.Mode
		mode, err = snellv6.ParseMode(options.V6Options.Mode)
		if err != nil {
			return nil, err
		}
		serviceOptions := snellv6.ServerOptions{
			PSK:                     []byte(options.PSK),
			Mode:                    mode,
			Handler:                 inbound,
			MultiUserAuthentication: authentication,
		}
		if len(options.Users) > 0 {
			var service *snellv6.MultiService[int]
			service, err = snellv6.NewMultiService[int](serviceOptions)
			if err != nil {
				return nil, err
			}
			err = service.UpdateUsers(userList, keyList)
			inbound.service = service
		} else {
			inbound.service, err = snellv6.NewService(serviceOptions)
		}
	case 0:
		return nil, E.New("snell: missing version")
	default:
		return nil, E.New("snell: unsupported version: ", options.Version)
	}
	if err != nil {
		return nil, err
	}
	var quicParser quicProxyInitParser
	if options.Version == 5 {
		var loaded bool
		quicParser, loaded = inbound.service.(quicProxyInitParser)
		if !loaded {
			return nil, E.New("snell: version 5 service does not support QUIC proxy init parsing")
		}
	} else {
		quicService, createErr := newSnellV5Service(snellv5.ServiceOptions{
			PSK:                     []byte(options.PSK),
			Handler:                 inbound,
			MultiUserAuthentication: authentication,
		}, userList, keyList)
		if createErr != nil {
			return nil, E.Cause(createErr, "create Snell v6 QUIC Proxy compatibility service")
		}
		var loaded bool
		quicParser, loaded = quicService.(quicProxyInitParser)
		if !loaded {
			return nil, E.New("snell: version 5 service does not support QUIC proxy init parsing")
		}
	}
	listenerOptions := listener.Options{
		Context:           ctx,
		Logger:            logger,
		Network:           []string{N.NetworkTCP, N.NetworkUDP},
		Listen:            options.ListenOptions,
		ConnectionHandler: inbound,
		PacketHandler:     (*inboundPacketHandler)(inbound),
	}
	inbound.quicParser = quicParser
	inbound.listener = listener.New(listenerOptions)
	return inbound, nil
}

func newSnellV5Service(options snellv5.ServiceOptions, userList []int, keyList [][]byte) (snellprotocol.Service, error) {
	if len(userList) == 0 {
		return snellv5.NewService(options)
	}
	service, err := snellv5.NewMultiService[int](options)
	if err != nil {
		return nil, err
	}
	if err = service.UpdateUsers(userList, keyList); err != nil {
		return nil, err
	}
	return service, nil
}

func (h *Inbound) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	switch stage {
	case adapter.StartStateInitialize:
		h.udpNat = newQUICProxyNATService((*inboundUDPHandler)(h), h.preparePacketConnection, snellprotocol.QUICProxySessionIdleTimeout)
		scope.Add(func() error { h.udpNat.Close(); return nil })
		h.quicAuth = newQUICProxyAuthenticationService(h.quicParser, h.udpNat, h.logger)
		scope.Add(func() error { h.quicAuth.Close(); return nil })
	case adapter.StartStateStart:
		if err := h.listener.Start(); err != nil {
			return err
		}
		scope.Add(h.listener.Close)
	}
	return nil
}

func (h *Inbound) NewConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	// Nothing else bounds the wait for the request: a client that connects and
	// sends nothing held this goroutine and its socket for as long as it kept
	// the connection open, which a scanner or a half-dead peer does for ever.
	// Set on the socket itself, below every wrapper, and lifted by the
	// callbacks that receive the parsed request — see clearHandshakeDeadline.
	handshakeDeadline := time.Now().Add(cmp.Or(h.handshakeTimeout, C.TCPTimeout))
	_ = conn.SetReadDeadline(handshakeDeadline)
	socket := conn
	if h.obfsMode == "http" {
		conn = obfs.NewHTTPObfsServer(conn)
	}
	// A reuse session lives inside the service call below, and between two
	// streams nothing in route tracks it: without this a reload left it open,
	// served by the old box, and the client's next request on it failed.
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	ctx = context.WithValue(ctx, (*tunnelConnKey)(nil), conn)
	err := h.service.NewConnection(adapter.WithContext(ctx, &metadata), conn, metadata.Source, onClose)
	if err == nil {
		return
	}
	if E.IsClosedOrCanceled(err) {
		h.logger.DebugContext(ctx, "connection closed: ", err)
	} else {
		h.logger.ErrorContext(ctx, E.Cause(err, "process connection from ", metadata.Source))
		// Random bytes short of a request header (16 salt + 23 header) wait
		// for the deadline, longer ones failed authentication at once: the
		// difference names the protocol to a prober. Holding a failed one to
		// the same deadline adds no reach to a client wanting to pin a
		// connection, which fewer bytes already pin as long.
		_ = socket.SetReadDeadline(handshakeDeadline)
		_, _ = io.Copy(io.Discard, socket)
	}
	N.CloseOnHandshakeFailure(conn, onClose, err)
}

func (h *Inbound) NewConnectionEx(ctx context.Context, conn net.Conn, source M.Socksaddr, destination M.Socksaddr, onClose N.CloseHandlerFunc) {
	clearHandshakeDeadline(conn)
	_, metadata := adapter.ExtendContext(ctx)
	if source.IsValid() {
		metadata.Source = source
	}
	if destination.IsValid() {
		metadata.Destination = destination
	}
	h.newConnection(ctx, conn, *metadata, onClose)
}

func (h *Inbound) NewPacketConnectionEx(ctx context.Context, conn N.PacketConn, source M.Socksaddr, destination M.Socksaddr, onClose N.CloseHandlerFunc) {
	clearHandshakeDeadline(conn)
	if tunnel, loaded := ctx.Value((*tunnelConnKey)(nil)).(net.Conn); loaded {
		conn = &closeTunnelPacketConn{PacketConn: conn, tunnel: tunnel}
	}
	_, metadata := adapter.ExtendContext(ctx)
	if source.IsValid() {
		metadata.Source = source
	}
	if destination.IsValid() {
		metadata.Destination = destination
	}
	h.newPacketConnection(ctx, conn, *metadata, onClose)
}

type tunnelConnKey struct{}

// closeTunnelPacketConn ends a UDP-over-TCP session by closing its TCP
// connection, so the client learns at once. The service's own Close keeps the
// stream open, discarding what the client sends, until the client closes or
// five minutes pass: after route ended the session (its UDP idle timeout, 10s
// for port 53, the relay closing the outbound's stream, a reload) the client
// sent into nothing meanwhile, and a DNS client waited out its own timeout
// before it dialed again.
type closeTunnelPacketConn struct {
	N.PacketConn
	tunnel net.Conn

	// Packets handed over one at a time, as the copy from a naive UoT stream
	// does, were a record and a write system call each. They are staged and
	// written as one batch of records while the previous batch is in flight.
	// A copy from a UDP socket reads in batches and reaches the service's
	// batch writer through CreatePacketBatchWriter instead; a session's copy
	// takes one path for its whole life, so the two never reorder. Packets
	// staged but not yet written when the session closes are lost.
	access   sync.Mutex
	cond     sync.Cond
	staged   []*buf.Buffer
	dests    []M.Socksaddr
	flushing bool
	err      error
	batch    N.PacketBatchWriter
}

const maxStagedPackets = 64

func (c *closeTunnelPacketConn) CreatePacketBatchWriter() (N.PacketBatchWriter, bool) {
	return bufio.CreatePacketBatchWriter(c.PacketConn)
}

func (c *closeTunnelPacketConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	c.access.Lock()
	defer c.access.Unlock()
	if c.cond.L == nil {
		c.cond.L = &c.access
	}
	for c.err == nil && len(c.staged) >= maxStagedPackets {
		c.cond.Wait()
	}
	if c.err != nil {
		buffer.Release()
		return c.err
	}
	c.staged = append(c.staged, buffer)
	c.dests = append(c.dests, destination)
	if !c.flushing {
		c.flushing = true
		go c.flush()
	}
	return nil
}

func (c *closeTunnelPacketConn) flush() {
	c.access.Lock()
	for len(c.staged) > 0 && c.err == nil {
		buffers, dests := c.staged, c.dests
		c.staged, c.dests = nil, nil
		c.cond.Broadcast()
		c.access.Unlock()
		var err error
		if len(buffers) == 1 {
			err = c.PacketConn.WritePacket(buffers[0], dests[0])
		} else {
			if c.batch == nil {
				c.batch = bufio.NewPacketBatchWriter(c.PacketConn)
			}
			err = c.batch.WritePacketBatch(buffers, dests)
		}
		c.access.Lock()
		if err != nil {
			c.err = err
		}
	}
	buf.ReleaseMulti(c.staged)
	c.staged, c.dests = nil, nil
	c.flushing = false
	c.cond.Broadcast()
	c.access.Unlock()
}

func (c *closeTunnelPacketConn) Close() error {
	return E.Errors(c.PacketConn.Close(), c.tunnel.Close())
}

func (c *closeTunnelPacketConn) Upstream() any { return c.PacketConn }

func (c *closeTunnelPacketConn) ReaderReplaceable() bool { return true }

// WriterReplaceable is false so the copy cannot reach past WritePacket to the
// service's own, which would skip the staging.
func (c *closeTunnelPacketConn) WriterReplaceable() bool { return false }

// clearHandshakeDeadline lifts the bound NewConnection put on the request, now
// that one has been parsed. The service calls back once per logical stream,
// so with reuse this runs again for every stream on the session — lifting a
// bound already lifted — and the idle wait between streams stays unbounded, as
// it was. The wrappers in between all hand deadlines to the socket. The QUIC
// proxy's packet connections come here too and never had a deadline to lift.
func clearHandshakeDeadline(conn interface{ SetReadDeadline(time.Time) error }) {
	_ = conn.SetReadDeadline(time.Time{})
}

func (h *Inbound) newConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	metadata.Inbound = h.Tag()
	metadata.InboundType = h.Type()
	if len(h.users) > 0 {
		userIndex, loaded := auth.UserFromContext[int](ctx)
		if !loaded {
			N.CloseOnHandshakeFailure(conn, onClose, os.ErrInvalid)
			return
		}
		user := h.users[userIndex].Name
		if user == "" {
			user = F.ToString(userIndex)
		} else {
			metadata.User = user
		}
		h.logger.InfoContext(ctx, "[", user, "] inbound connection to ", metadata.Destination)
	} else {
		h.logger.InfoContext(ctx, "inbound connection to ", metadata.Destination)
	}
	h.router.RouteConnectionEx(ctx, conn, metadata, onClose)
}

func (h *Inbound) newPacketConnection(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	metadata.Inbound = h.Tag()
	metadata.InboundType = h.Type()
	if len(h.users) > 0 {
		userIndex, loaded := auth.UserFromContext[int](ctx)
		if !loaded {
			N.CloseOnHandshakeFailure(conn, onClose, os.ErrInvalid)
			return
		}
		user := h.users[userIndex].Name
		if user == "" {
			user = F.ToString(userIndex)
		} else {
			metadata.User = user
		}
		h.logger.InfoContext(ctx, "[", user, "] inbound packet connection from ", metadata.Source)
	} else {
		h.logger.InfoContext(ctx, "inbound packet connection from ", metadata.Source)
	}
	h.router.RoutePacketConnectionEx(ctx, conn, metadata, onClose)
}

func validateSnellInboundObfs(version int, obfsMode string) error {
	if version != 5 || obfsMode == "" || obfsMode == "none" || obfsMode == "http" {
		return nil
	}
	if obfsMode == "tls" {
		return E.New("snell: TLS obfs is unsupported for version 5; use ShadowTLS instead")
	}
	return E.New("snell: version 5 only supports http obfs")
}

type inboundUDPHandler Inbound

func (h *inboundUDPHandler) NewPacketConnectionEx(ctx context.Context, conn N.PacketConn, source M.Socksaddr, destination M.Socksaddr, onClose N.CloseHandlerFunc) {
	(*Inbound)(h).NewPacketConnectionEx(ctx, conn, source, destination, onClose)
}

func (h *Inbound) preparePacketConnection(source M.Socksaddr, destination M.Socksaddr, session *snellprotocol.QUICProxySession) (context.Context, N.PacketWriter) {
	ctx := log.ContextWithNewID(h.ctx)
	ctx = session.Context(ctx)
	return ctx, &quicProxyResponseWriter{
		writer:     h.listener.PacketWriter(),
		clientAddr: source,
	}
}

type quicProxyResponseWriter struct {
	writer     N.PacketWriter
	clientAddr M.Socksaddr
}

func (w *quicProxyResponseWriter) WritePacket(buffer *buf.Buffer, _ M.Socksaddr) error {
	return w.writer.WritePacket(buffer, w.clientAddr)
}

type inboundPacketHandler Inbound

type quicProxyInitParser interface {
	ParseQUICProxyInit(data []byte) (*snellprotocol.QUICProxySession, []byte, error)
}

func (h *inboundPacketHandler) NewPacket(buffer *buf.Buffer, source M.Socksaddr) {
	defer buffer.Release()
	data := buffer.Bytes()
	if len(data) == 0 || h.udpNat == nil {
		return
	}
	if h.processSessionPacket(source, data) {
		return
	}
	if snellprotocol.IsQUICLooking(data[0]) {
		if h.quicAuth != nil && h.quicAuth.QueuePending(source, data) {
			return
		}
		if h.processSessionPacket(source, data) {
			return
		}
		h.logger.Debug("quic proxy: discard packet without session from ", source)
		return
	}
	if h.quicAuth != nil && h.quicAuth.Submit(source, data) {
		return
	}
	if h.processSessionPacket(source, data) {
		return
	}
	h.logger.Debug("quic proxy: discard init while authentication queue is full from ", source)
}

func (h *inboundPacketHandler) processSessionPacket(source M.Socksaddr, data []byte) bool {
	entry, loaded := h.udpNat.Session(source)
	if !loaded {
		return false
	}
	session := entry.session
	session.Touch()
	payload := data
	if !snellprotocol.IsQUICLooking(data[0]) {
		var err error
		payload, err = session.DecodeDuplicateInit(data)
		if err != nil {
			h.logger.Debug("quic proxy: reject duplicate init from ", source, ": ", err)
			return true
		}
	}
	h.udpNat.NewPacket(entry, payload)
	return true
}
