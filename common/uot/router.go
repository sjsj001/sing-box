package uot

import (
	stdbufio "bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/uot"
)

var _ adapter.ConnectionRouterEx = (*Router)(nil)

type Router struct {
	router adapter.ConnectionRouterEx
	logger logger.ContextLogger
}

func NewRouter(router adapter.ConnectionRouterEx, logger logger.ContextLogger) *Router {
	return &Router{router, logger}
}

func (r *Router) RouteConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext) error {
	switch metadata.Destination.Fqdn {
	case uot.MagicAddress:
		request, err := uot.ReadRequest(conn)
		if err != nil {
			return E.Cause(err, "read UoT request")
		}
		if request.IsConnect {
			r.logger.InfoContext(ctx, "inbound UoT connect connection to ", request.Destination)
		} else {
			r.logger.InfoContext(ctx, "inbound UoT connection to ", request.Destination)
		}
		metadata.Domain = metadata.Destination.Fqdn
		metadata.Destination = request.Destination
		return r.router.RoutePacketConnection(ctx, newServerConn(conn, *request), metadata)
	case uot.LegacyMagicAddress:
		r.logger.InfoContext(ctx, "inbound legacy UoT connection")
		metadata.Domain = metadata.Destination.Fqdn
		metadata.Destination = M.Socksaddr{Addr: netip.IPv4Unspecified()}
		return r.RoutePacketConnection(ctx, newServerConn(conn, uot.Request{}), metadata)
	}
	return r.router.RouteConnection(ctx, conn, metadata)
}

func (r *Router) RoutePacketConnection(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext) error {
	return r.router.RoutePacketConnection(ctx, conn, metadata)
}

func (r *Router) RouteConnectionEx(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	switch metadata.Destination.Fqdn {
	case uot.MagicAddress:
		request, err := uot.ReadRequest(conn)
		if err != nil {
			err = E.Cause(err, "UoT read request")
			r.logger.ErrorContext(ctx, "process connection from ", metadata.Source, ": ", err)
			N.CloseOnHandshakeFailure(conn, onClose, err)
			return
		}
		if request.IsConnect {
			r.logger.InfoContext(ctx, "inbound UoT connect connection to ", request.Destination)
		} else {
			r.logger.InfoContext(ctx, "inbound UoT connection to ", request.Destination)
		}
		metadata.Domain = metadata.Destination.Fqdn
		metadata.Destination = request.Destination
		r.router.RoutePacketConnectionEx(ctx, newServerConn(conn, *request), metadata, onClose)
		return
	case uot.LegacyMagicAddress:
		r.logger.InfoContext(ctx, "inbound legacy UoT connection")
		metadata.Domain = metadata.Destination.Fqdn
		metadata.Destination = M.Socksaddr{Addr: netip.IPv4Unspecified()}
		r.RoutePacketConnectionEx(ctx, newServerConn(conn, uot.Request{}), metadata, onClose)
		return
	}
	r.router.RouteConnectionEx(ctx, conn, metadata, onClose)
}

func (r *Router) RoutePacketConnectionEx(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	r.router.RoutePacketConnectionEx(ctx, conn, metadata, onClose)
}

// newServerConn is uot.NewConn for a stream that has no vectorised writer,
// such as a naive HTTP/2 stream behind HTTP2ConnWrapper or any stream under
// TLS. There sing's uot.Conn copies each packet next to its length and never
// releases the packet it was handed, so every datagram sent down the tunnel
// cost a fresh 64 KiB pool buffer; and every packet was its own stream write,
// which on HTTP/2 is a flush: a DATA frame, a TLS record and a round trip
// through the h2 server's goroutines. A stream that has a vectorised writer (a
// TCP socket) keeps sing's path, which has neither problem. The test is the
// one uot.NewConn makes, so the two never disagree about which path a stream
// takes.
func newServerConn(conn net.Conn, request uot.Request) N.PacketConn {
	if _, vectorised := bufio.CreateVectorisedWriter(N.UnwrapWriter(conn)); vectorised {
		return uot.NewConn(conn, request)
	}
	c := &serverConn{
		Conn:        uot.NewConn(&bufferedReadConn{conn, stdbufio.NewReaderSize(conn, readBufferSize)}, request),
		isConnect:   request.IsConnect,
		destination: request.Destination,
	}
	c.cond.L = &c.access
	return c
}

// serverConn frames the packets it is handed into a staging buffer, and a
// flusher goroutine writes what is staged as one stream write: packets that
// arrive while a write is in flight go out together in the next one. The
// flusher and the buffer exist only while there is something to write.
//
// A write returns once its frame is staged, where uot.Conn's returned once it
// was on the stream, so frames staged but not yet written when the session is
// closed are lost. That happens when the copy closes the session at once on
// an error from its source; they are datagrams, which may be lost anyway.
type serverConn struct {
	*uot.Conn
	isConnect   bool
	destination M.Socksaddr
	access      sync.Mutex
	cond        sync.Cond
	staged      []byte
	flushing    bool
	err         error
}

// stageLimit bounds what one stream write carries. Frames are appended only
// while the stage stays within it, so the stage never outgrows the pool
// buffer it starts in. A frame larger than the limit, which only a payload
// near 64 KiB makes, waits for an empty stage and takes a buffer of its own
// from the pool's largest tier (72 KiB), which goes back to the pool like the
// others.
const stageLimit = 64 * 1024

func (c *serverConn) Write(p []byte) (int, error) {
	return c.WriteTo(p, c.destination)
}

func (c *serverConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	err := c.WritePacket(buf.As(p), M.SocksaddrFromNet(addr))
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *serverConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	defer buffer.Release()
	if buffer.Len() > 0xffff {
		return E.Cause(io.ErrShortBuffer, "UoT write")
	}
	frameLen := 2 + buffer.Len()
	if !c.isConnect {
		frameLen += uot.AddrParser.AddrPortLen(destination)
	}
	c.access.Lock()
	defer c.access.Unlock()
	for c.err == nil && len(c.staged) > 0 && len(c.staged)+frameLen > stageLimit {
		c.cond.Wait()
	}
	if c.err != nil {
		return c.err
	}
	if c.staged == nil {
		c.staged = buf.Get(max(stageLimit, frameLen))[:0]
	}
	frame := bytes.NewBuffer(c.staged)
	if !c.isConnect {
		if err := uot.AddrParser.WriteAddrPort(frame, destination); err != nil {
			return err
		}
	}
	frame.Write(binary.BigEndian.AppendUint16(nil, uint16(buffer.Len())))
	frame.Write(buffer.Bytes())
	c.staged = frame.Bytes()
	if !c.flushing {
		c.flushing = true
		go c.flush()
	}
	return nil
}

func (c *serverConn) flush() {
	c.access.Lock()
	for c.staged != nil && c.err == nil {
		staged := c.staged
		c.staged = nil
		c.cond.Broadcast()
		c.access.Unlock()
		_, err := c.Conn.Conn.Write(staged)
		_ = buf.Put(staged[:cap(staged)])
		c.access.Lock()
		if err != nil {
			c.err = err
		}
	}
	if c.staged != nil {
		_ = buf.Put(c.staged[:cap(c.staged)])
		c.staged = nil
	}
	c.flushing = false
	c.cond.Broadcast()
	c.access.Unlock()
}

// bufferedReadConn serves the framing's small reads (address, length and
// payload: five per packet) from one read of the stream; each read of an
// HTTP/2 request body is a round trip to the h2 server's goroutine.
type bufferedReadConn struct {
	net.Conn
	reader *stdbufio.Reader
}

const readBufferSize = 4096

func (c *bufferedReadConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

// Upstream keeps the inbound's handshake reporter in reach: a naive relay
// answers a deferred CONNECT only when route finds it through this chain.
func (c *bufferedReadConn) Upstream() any {
	return c.Conn
}
