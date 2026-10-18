package group

import (
	"context"
	"net"
	"os"
	"sync"
	"time"

	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"golang.org/x/crypto/cryptobyte"
)

// A race used to carry no payload: the winner's tunnel was handed to the
// caller and only then did the opening write go out, one round trip to the
// winner after the race. A TLS ClientHello that offers no early data is the
// one opening that can go out on every probe at once: it asks the destination
// for nothing but a handshake, a losing probe is closed before the client
// could answer its ServerHello, and the bytes that do ask for something only
// ever follow on the winner.
//
// The cost is what the destination sees. A payload-free round showed it one
// bare connection from each racing exit; a carried one shows it the same
// ClientHello from each, which links those exits to one client exactly
// instead of by timing. Carrying it to the first few groups only would keep
// the saving where it is smallest — a near group winning — and lose it where
// it is largest, so every racing group gets it.

type helloVerdict int

const (
	helloNotCarriable helloVerdict = iota
	helloIncomplete
	helloCarriable
)

// classifyHello judges the opening bytes of a connection. It accepts exactly
// one TLS record holding exactly one ClientHello with no early_data extension
// and nothing after it; n is the record's length.
func classifyHello(b []byte) (n int, verdict helloVerdict) {
	if len(b) > 0 && b[0] != 0x16 || len(b) > 1 && b[1] != 3 {
		return 0, helloNotCarriable
	}
	if len(b) < 5 {
		return 0, helloIncomplete
	}
	length := int(b[3])<<8 | int(b[4])
	if length > 1<<14 {
		return 0, helloNotCarriable
	}
	if len(b) < 5+length {
		return 0, helloIncomplete
	}
	record := cryptobyte.String(b[5:])
	var (
		msgType uint8
		body    cryptobyte.String
	)
	if !record.ReadUint8(&msgType) || msgType != 1 || !record.ReadUint24LengthPrefixed(&body) || !record.Empty() {
		return 0, helloNotCarriable
	}
	var sessionID, suites, compression cryptobyte.String
	if !body.Skip(2+32) || !body.ReadUint8LengthPrefixed(&sessionID) ||
		!body.ReadUint16LengthPrefixed(&suites) || !body.ReadUint8LengthPrefixed(&compression) {
		return 0, helloNotCarriable
	}
	if body.Empty() {
		return 5 + length, helloCarriable
	}
	var extensions cryptobyte.String
	if !body.ReadUint16LengthPrefixed(&extensions) || !body.Empty() {
		return 0, helloNotCarriable
	}
	for !extensions.Empty() {
		var (
			extension uint16
			data      cryptobyte.String
		)
		if !extensions.ReadUint16(&extension) || !extensions.ReadUint16LengthPrefixed(&data) {
			return 0, helloNotCarriable
		}
		// early_data: a ticket good for one use, which a losing probe would
		// spend and leave the winner a full handshake.
		if extension == 42 {
			return 0, helloNotCarriable
		}
	}
	return 5 + length, helloCarriable
}

// helloCarrier is where a race's probes and its caller meet. Each probe
// attaches its tunnel; the caller offers the hello once it has one. Whichever
// comes second writes it, so every probe gets it exactly once, and nothing is
// written once the race has decided.
type helloCarrier struct {
	access  sync.Mutex
	hello   []byte
	decided bool
	probes  []*carriedProbe
}

type carriedProbe struct {
	conn   net.Conn
	pushed bool
	done   chan struct{}
	err    error
}

func (r *raceRun) carrier() *helloCarrier {
	if r == nil {
		return nil
	}
	return r.carry
}

func (h *helloCarrier) attach(conn net.Conn) {
	if h == nil {
		return
	}
	probe := &carriedProbe{conn: conn, done: make(chan struct{})}
	h.access.Lock()
	if h.decided {
		h.access.Unlock()
		return
	}
	h.probes = append(h.probes, probe)
	hello := h.hello
	if hello != nil {
		probe.pushed = true
	}
	h.access.Unlock()
	if hello != nil {
		go probe.push(hello)
	}
}

func (h *helloCarrier) offer(hello []byte) {
	h.access.Lock()
	if h.decided || h.hello != nil {
		h.access.Unlock()
		return
	}
	h.hello = hello
	var targets []*carriedProbe
	for _, probe := range h.probes {
		if !probe.pushed {
			probe.pushed = true
			targets = append(targets, probe)
		}
	}
	h.access.Unlock()
	for _, probe := range targets {
		go probe.push(hello)
	}
}

// decide stops all further pushes and reports how much of the opening the
// winner already has, waiting for that write to finish so nothing can
// overtake it.
func (h *helloCarrier) decide(winner net.Conn) (carried int, err error) {
	h.access.Lock()
	h.decided = true
	var pushed *carriedProbe
	for _, probe := range h.probes {
		if probe.conn == winner && probe.pushed {
			pushed = probe
		}
	}
	hello := h.hello
	h.access.Unlock()
	if pushed == nil {
		return 0, nil
	}
	<-pushed.done
	if pushed.err != nil {
		return 0, pushed.err
	}
	return len(hello), nil
}

// push writes the hello off the probe's goroutine: a tunnel's first write
// waits for its leg to come up, and the probe must not wait on that.
func (p *carriedProbe) push(hello []byte) {
	_, p.err = p.conn.Write(hello)
	close(p.done)
}

// raceConn is what the caller holds while its destination is being raced. It
// takes writes from the start, and reads wait for the winner.
//
// Like replayConn it has no Upstream(): reaching past it would read a tunnel
// that does not exist yet.
type raceConn struct {
	carrier     *helloCarrier
	cancel      context.CancelFunc
	destination M.Socksaddr
	ready       chan struct{}

	access  sync.Mutex
	conn    net.Conn
	err     error
	pending []byte
	judged  bool
	flushed bool
	closed  bool

	readDeadline  time.Time
	writeDeadline time.Time
}

// raceCarrying races destination in the background and hands the caller a
// connection at once, so its opening write is in hand while the probes are
// still out.
func (s *Smart) raceCarrying(ctx context.Context, key string, destination M.Socksaddr, run *raceRun) *raceConn {
	run.carry = &helloCarrier{}
	// The connection outlives the dial; a dialer that cancels its context on
	// return must not read as the caller leaving. Close is that signal.
	raceCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	conn := &raceConn{carrier: run.carry, cancel: cancel, destination: destination, ready: make(chan struct{})}
	go func() {
		winner, member, tunnel, err := s.race(raceCtx, key, destination, run)
		var wrapped net.Conn
		if err == nil {
			// Counted against the member that raced: after a failover
			// meanwhile, the group's current one is another node.
			group := s.group(winner)
			s.countRequest(key, group, member, reasonRaced, N.NetworkTCP)
			s.logger.InfoContext(ctx, destination, " via ", winner, " by=", reasonRaced)
			// Already answered, so settled: nothing to replay. Wrapped all the
			// same, so reads stamp the member's deliveredAt, without which a
			// member carrying a raced download looked idle to Smart.congested,
			// and so RemoteAck, CloseWrite and the deadlines go where they do
			// on every other connection.
			wrapped = &replayConn{
				Conn: tunnel, smart: s, destination: destination, key: key,
				group: group, member: member, settled: true,
			}
		}
		conn.install(wrapped, tunnel, err)
	}()
	return conn
}

func (c *raceConn) install(winner net.Conn, tunnel net.Conn, err error) {
	carried, pushErr := c.carrier.decide(tunnel)
	if err == nil && pushErr != nil {
		err = E.Cause(pushErr, "carry the opening to the winner")
	}
	c.access.Lock()
	rest := c.pending[carried:]
	c.pending, c.flushed = nil, true
	closed := c.closed
	c.access.Unlock()
	if err == nil && closed {
		err = net.ErrClosed
	}
	if err == nil && len(rest) > 0 {
		_, err = winner.Write(rest)
	}
	if err != nil && winner != nil {
		winner.Close()
		winner = nil
	}
	c.access.Lock()
	c.conn, c.err = winner, err
	if winner != nil {
		if !c.readDeadline.IsZero() {
			_ = winner.SetReadDeadline(c.readDeadline)
		}
		if !c.writeDeadline.IsZero() {
			_ = winner.SetWriteDeadline(c.writeDeadline)
		}
	}
	closed = c.closed
	c.access.Unlock()
	close(c.ready)
	if closed && winner != nil {
		winner.Close()
	}
}

func (c *raceConn) wait(deadline time.Time) error {
	select {
	case <-c.ready:
		return c.err
	default:
	}
	if deadline.IsZero() {
		<-c.ready
		return c.err
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-c.ready:
		return c.err
	case <-timer.C:
		return os.ErrDeadlineExceeded
	}
}

func (c *raceConn) Write(p []byte) (int, error) {
	c.access.Lock()
	if !c.flushed && !c.closed && len(c.pending)+len(p) <= replayBufferLimit {
		c.pending = append(c.pending, p...)
		var hello []byte
		if !c.judged {
			switch n, verdict := classifyHello(c.pending); verdict {
			case helloCarriable:
				hello = c.pending[:n:n]
				c.judged = true
			case helloNotCarriable:
				c.judged = true
			}
		}
		c.access.Unlock()
		if hello != nil {
			c.carrier.offer(hello)
		}
		return len(p), nil
	}
	deadline := c.writeDeadline
	c.access.Unlock()
	if err := c.wait(deadline); err != nil {
		return 0, err
	}
	return c.current().Write(p)
}

func (c *raceConn) Read(p []byte) (int, error) {
	c.access.Lock()
	deadline := c.readDeadline
	c.access.Unlock()
	if err := c.wait(deadline); err != nil {
		return 0, err
	}
	return c.current().Read(p)
}

// CreateReadWaiter gives the copy loop the same small idle buffer a cached
// connection gets. The loop asks before the race has a winner, so the waiter
// reads through Read, which waits for one.
func (c *raceConn) CreateReadWaiter() (N.ReadWaiter, bool) {
	return &replayReadWaiter{conn: c}, true
}

func (c *raceConn) current() net.Conn {
	c.access.Lock()
	defer c.access.Unlock()
	return c.conn
}

func (c *raceConn) Close() error {
	c.access.Lock()
	c.closed = true
	conn := c.conn
	c.access.Unlock()
	c.cancel()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (c *raceConn) CloseWrite() error {
	if err := c.wait(time.Time{}); err != nil {
		return err
	}
	closer, isCloser := c.current().(N.WriteCloser)
	if !isCloser {
		return E.New("not a write closer")
	}
	return closer.CloseWrite()
}

func (c *raceConn) RemoteAck(ctx context.Context) (naive.ConnectAck, bool, error) {
	select {
	case <-c.ready:
	case <-ctx.Done():
		return naive.ConnectAck{}, false, ctx.Err()
	}
	if c.err != nil {
		return naive.ConnectAck{}, false, c.err
	}
	reporter, isReporter := common.Cast[naive.RemoteDialReporter](c.current())
	if !isReporter {
		return naive.ConnectAck{}, false, nil
	}
	return reporter.RemoteAck(ctx)
}

func (c *raceConn) LocalAddr() net.Addr {
	if conn := c.current(); conn != nil {
		return conn.LocalAddr()
	}
	return M.Socksaddr{}
}

func (c *raceConn) RemoteAddr() net.Addr {
	if conn := c.current(); conn != nil {
		return conn.RemoteAddr()
	}
	return c.destination
}

func (c *raceConn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

func (c *raceConn) SetReadDeadline(t time.Time) error {
	c.access.Lock()
	c.readDeadline = t
	conn := c.conn
	c.access.Unlock()
	if conn != nil {
		return conn.SetReadDeadline(t)
	}
	return nil
}

func (c *raceConn) SetWriteDeadline(t time.Time) error {
	c.access.Lock()
	c.writeDeadline = t
	conn := c.conn
	c.access.Unlock()
	if conn != nil {
		return conn.SetWriteDeadline(t)
	}
	return nil
}
