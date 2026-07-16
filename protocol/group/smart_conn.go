package group

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// smartEarlyFailWindow: a read/write error with zero bytes received within
// this window of dialing is treated as a dial-level failure. Early-data
// protocols (naive) surface CONNECT failures this way instead of at dial.
const smartEarlyFailWindow = 3 * time.Second

type smartConnCallbacks struct {
	onTotal     func(ms float64) // first-write→first-read duration
	onTTFB      func(ms float64) // application first-byte duration (TLS only)
	onEarlyFail func(err error)  // dial-equivalent failure
	// earlyFailWindow overrides smartEarlyFailWindow when positive: paths with
	// timing history are declared hung from their own expected duration
	// instead of the one-size-fits-all default.
	earlyFailWindow time.Duration
}

// smartLocalAbort reports errors caused by our own side tearing the
// connection down (user abort, interrupt group, context cancellation); they
// carry no signal about the member's path and must not count as failures.
// io.ErrClosedPipe is deliberately not matched: pipe-backed streams surface
// remote closes with it too.
func smartLocalAbort(err error) bool {
	return errors.Is(err, net.ErrClosed) ||
		errors.Is(err, context.Canceled)
}

// tlsRecordParser tracks TLS record framing (the 5-byte plaintext headers)
// across arbitrarily fragmented reads/writes without buffering payload.
type tlsRecordParser struct {
	hdr       [5]byte
	hdrN      int
	remaining int
}

// feed consumes b, invoking onRecord at each record boundary with the record
// type. Returns false when the stream cannot be TLS record framing.
func (p *tlsRecordParser) feed(b []byte, onRecord func(recordType byte)) bool {
	for len(b) > 0 {
		if p.remaining > 0 {
			skip := p.remaining
			if skip > len(b) {
				skip = len(b)
			}
			p.remaining -= skip
			b = b[skip:]
			continue
		}
		need := 5 - p.hdrN
		if need > len(b) {
			need = len(b)
		}
		copy(p.hdr[p.hdrN:], b[:need])
		p.hdrN += need
		b = b[need:]
		if p.hdrN < 5 {
			return true
		}
		p.hdrN = 0
		recordType := p.hdr[0]
		if recordType < 20 || recordType > 23 || p.hdr[1] != 3 {
			return false
		}
		length := int(p.hdr[3])<<8 | int(p.hdr[4])
		if length > 16384+2048 {
			return false
		}
		onRecord(recordType)
		p.remaining = length
	}
	return true
}

const (
	smartConnModeUnknown = iota
	smartConnModeTLS
	smartConnModePlain
)

// smartMeasureConn passively samples one TCP connection: total
// first-write→first-read duration (≈ baseline + legFactor × remote RTT) and,
// for TLS, the application-layer first-byte duration. Once sampling finishes
// it becomes a transparent passthrough (Upstream + Replaceable).
type smartMeasureConn struct {
	net.Conn
	cb         smartConnCallbacks
	dialedAt   time.Time
	failWindow time.Duration
	done       atomic.Bool

	mu                sync.Mutex
	hangTimer         *time.Timer
	firstWriteAt      time.Time
	firstReadAt       time.Time
	mode              int
	client            tlsRecordParser
	server            tlsRecordParser
	sawClientCCS      bool
	clientAppAfterCCS int
	tls13             bool
	// appRequestAt is when the client sent its post-handshake application
	// record (the request); TTFB = first server app record after it.
	appRequestAt  time.Time
	ttfbDone      bool
	totalReported bool
	earlyFailDone bool
}

func newSmartMeasureConn(conn net.Conn, cb smartConnCallbacks) *smartMeasureConn {
	failWindow := cb.earlyFailWindow
	if failWindow <= 0 {
		failWindow = smartEarlyFailWindow
	}
	c := &smartMeasureConn{Conn: conn, cb: cb, dialedAt: time.Now(), failWindow: failWindow}
	if cb.onEarlyFail != nil {
		// Silently dropped paths (firewalled member) hang instead of
		// erroring; a request that wrote data but saw no byte and no error
		// within the window counts as a dial-level failure too.
		c.hangTimer = time.AfterFunc(failWindow, c.onHangTimeout)
	}
	return c
}

func (c *smartMeasureConn) onHangTimeout() {
	c.mu.Lock()
	if c.earlyFailDone || c.done.Load() || c.firstWriteAt.IsZero() || !c.firstReadAt.IsZero() {
		c.mu.Unlock()
		return
	}
	c.earlyFailDone = true
	c.done.Store(true)
	cb := c.cb.onEarlyFail
	c.mu.Unlock()
	if cb != nil {
		cb(errSmartNoResponse)
	}
}

var errSmartNoResponse = errors.New("smart: no response within early-failure window")

func (c *smartMeasureConn) Write(p []byte) (int, error) {
	if c.done.Load() {
		return c.Conn.Write(p)
	}
	c.observeWrite(p)
	n, err := c.Conn.Write(p)
	if err != nil {
		c.reportEarlyFail(err)
	}
	return n, err
}

func (c *smartMeasureConn) Read(p []byte) (int, error) {
	if c.done.Load() {
		return c.Conn.Read(p)
	}
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.observeRead(p[:n])
	}
	if err != nil {
		c.reportEarlyFail(err)
	}
	return n, err
}

func (c *smartMeasureConn) observeWrite(p []byte) {
	if len(p) == 0 {
		return
	}
	var report func()
	c.mu.Lock()
	now := time.Now()
	if c.firstWriteAt.IsZero() {
		c.firstWriteAt = now
		if p[0] == 0x16 && len(p) >= 2 && p[1] == 3 {
			c.mode = smartConnModeTLS
		} else {
			c.mode = smartConnModePlain
		}
	}
	if c.mode == smartConnModeTLS && !c.ttfbDone {
		ok := c.client.feed(p, func(recordType byte) {
			switch recordType {
			case 20:
				c.sawClientCCS = true
			case 23:
				if c.firstReadAt.IsZero() || !c.sawClientCCS {
					// Early data (0-RTT) or unexpected framing: the first
					// application record cannot be attributed reliably.
					c.ttfbDone = true
					return
				}
				c.clientAppAfterCCS++
				need := 1
				if c.tls13 {
					need = 2
				}
				if c.clientAppAfterCCS == need && c.appRequestAt.IsZero() {
					c.appRequestAt = now
				}
			}
		})
		if !ok {
			c.ttfbDone = true
		}
	}
	report = c.maybeFinishLocked()
	c.mu.Unlock()
	if report != nil {
		report()
	}
}

func (c *smartMeasureConn) observeRead(p []byte) {
	var reports []func()
	c.mu.Lock()
	now := time.Now()
	if c.firstReadAt.IsZero() {
		c.firstReadAt = now
		if c.hangTimer != nil {
			c.hangTimer.Stop()
		}
		if !c.firstWriteAt.IsZero() && !c.totalReported {
			c.totalReported = true
			total := float64(now.Sub(c.firstWriteAt)) / float64(time.Millisecond)
			if cb := c.cb.onTotal; cb != nil {
				reports = append(reports, func() { cb(total) })
			}
		} else if c.firstWriteAt.IsZero() {
			// Server-first protocol: no meaningful client-anchored sample.
			c.totalReported = true
			c.ttfbDone = true
		}
	}
	if c.mode == smartConnModeTLS && !c.ttfbDone {
		ok := c.server.feed(p, func(recordType byte) {
			switch recordType {
			case 21:
				c.ttfbDone = true
			case 23:
				if !c.sawClientCCS {
					c.tls13 = true
					return
				}
				if !c.appRequestAt.IsZero() && !c.ttfbDone {
					c.ttfbDone = true
					ttfb := float64(now.Sub(c.appRequestAt)) / float64(time.Millisecond)
					if cb := c.cb.onTTFB; cb != nil {
						reports = append(reports, func() { cb(ttfb) })
					}
				}
			}
		})
		if !ok {
			c.ttfbDone = true
		}
	}
	if report := c.maybeFinishLocked(); report != nil {
		reports = append(reports, report)
	}
	c.mu.Unlock()
	for _, report := range reports {
		report()
	}
}

// maybeFinishLocked flips the passthrough flag once every sample this
// connection can produce has been produced or abandoned.
func (c *smartMeasureConn) maybeFinishLocked() func() {
	if !c.totalReported {
		return nil
	}
	if c.mode == smartConnModeTLS && !c.ttfbDone {
		return nil
	}
	c.done.Store(true)
	return nil
}

func (c *smartMeasureConn) reportEarlyFail(err error) {
	c.mu.Lock()
	if c.earlyFailDone || !c.firstReadAt.IsZero() || c.firstWriteAt.IsZero() ||
		time.Since(c.dialedAt) > c.failWindow {
		c.mu.Unlock()
		return
	}
	c.earlyFailDone = true
	c.done.Store(true)
	if c.hangTimer != nil {
		c.hangTimer.Stop()
	}
	cb := c.cb.onEarlyFail
	c.mu.Unlock()
	if cb != nil && !smartLocalAbort(err) {
		cb(err)
	}
}

func (c *smartMeasureConn) ReaderReplaceable() bool {
	return c.done.Load()
}

func (c *smartMeasureConn) WriterReplaceable() bool {
	return c.done.Load()
}

// NeedHandshakeForRead / NeedHandshakeForWrite mark the wrapper as upgradable
// to the copy pipeline: replaceability is evaluated once at copy setup, and
// without these the wrapper would sit in the path (blocking splice/zero-copy)
// for the connection's whole life instead of only until sampling finishes.
func (c *smartMeasureConn) NeedHandshakeForRead() bool {
	return !c.done.Load()
}

func (c *smartMeasureConn) NeedHandshakeForWrite() bool {
	return !c.done.Load()
}

func (c *smartMeasureConn) Upstream() any {
	return c.Conn
}

// smartMeasurePacketConn samples the first write→read round trip of a UDP
// flow; used for observation and failure detection only.
type smartMeasurePacketConn struct {
	net.PacketConn
	cb       smartConnCallbacks
	dialedAt time.Time
	done     atomic.Bool

	mu            sync.Mutex
	firstWriteAt  time.Time
	totalReported bool
	earlyFailDone bool
}

func newSmartMeasurePacketConn(conn net.PacketConn, cb smartConnCallbacks) *smartMeasurePacketConn {
	return &smartMeasurePacketConn{PacketConn: conn, cb: cb, dialedAt: time.Now()}
}

func (c *smartMeasurePacketConn) noteWrite() {
	if c.done.Load() {
		return
	}
	c.mu.Lock()
	if c.firstWriteAt.IsZero() {
		c.firstWriteAt = time.Now()
	}
	c.mu.Unlock()
}

func (c *smartMeasurePacketConn) noteRead() {
	if c.done.Load() {
		return
	}
	var report func()
	c.mu.Lock()
	if !c.totalReported && !c.firstWriteAt.IsZero() {
		c.totalReported = true
		total := float64(time.Since(c.firstWriteAt)) / float64(time.Millisecond)
		if cb := c.cb.onTotal; cb != nil {
			report = func() { cb(total) }
		}
	}
	c.done.Store(true)
	c.mu.Unlock()
	if report != nil {
		report()
	}
}

func (c *smartMeasurePacketConn) noteError(err error) {
	if c.done.Load() {
		return
	}
	c.mu.Lock()
	if c.earlyFailDone || c.totalReported || c.firstWriteAt.IsZero() ||
		time.Since(c.dialedAt) > smartEarlyFailWindow {
		c.mu.Unlock()
		return
	}
	c.earlyFailDone = true
	c.done.Store(true)
	cb := c.cb.onEarlyFail
	c.mu.Unlock()
	if cb != nil && !smartLocalAbort(err) {
		cb(err)
	}
}

func (c *smartMeasurePacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	c.noteWrite()
	n, err := c.PacketConn.WriteTo(p, addr)
	if err != nil {
		c.noteError(err)
	}
	return n, err
}

func (c *smartMeasurePacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(p)
	if err != nil {
		c.noteError(err)
	} else if n > 0 {
		c.noteRead()
	}
	return n, addr, err
}

func (c *smartMeasurePacketConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	c.noteWrite()
	if packetWriter, ok := c.PacketConn.(N.PacketWriter); ok {
		err := packetWriter.WritePacket(buffer, destination)
		if err != nil {
			c.noteError(err)
		}
		return err
	}
	defer buffer.Release()
	_, err := c.PacketConn.WriteTo(buffer.Bytes(), destination.UDPAddr())
	if err != nil {
		c.noteError(err)
	}
	return err
}

func (c *smartMeasurePacketConn) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	if packetReader, ok := c.PacketConn.(N.PacketReader); ok {
		destination, err := packetReader.ReadPacket(buffer)
		if err != nil {
			c.noteError(err)
		} else {
			c.noteRead()
		}
		return destination, err
	}
	_, addr, err := buffer.ReadPacketFrom(c.PacketConn)
	if err != nil {
		c.noteError(err)
		return M.Socksaddr{}, err
	}
	c.noteRead()
	return M.SocksaddrFromNet(addr).Unwrap(), nil
}

func (c *smartMeasurePacketConn) ReaderReplaceable() bool {
	return c.done.Load()
}

func (c *smartMeasurePacketConn) WriterReplaceable() bool {
	return c.done.Load()
}

func (c *smartMeasurePacketConn) NeedHandshakeForRead() bool {
	return !c.done.Load()
}

func (c *smartMeasurePacketConn) NeedHandshakeForWrite() bool {
	return !c.done.Load()
}

func (c *smartMeasurePacketConn) Upstream() any {
	return c.PacketConn
}
