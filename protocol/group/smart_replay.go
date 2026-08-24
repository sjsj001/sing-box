package group

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing/common"
	M "github.com/sagernet/sing/common/metadata"
)

// A connection handed to the caller before the proxy has said whether it could
// reach the destination.
//
// Waiting for that answer would cost a round trip on every connection, and it
// is not needed: the payload can go onto the tunnel immediately, and if the
// answer turns out to be no, another group is dialed and the opening write is
// replayed onto it. The caller sees one continuous connection and never learns
// the first attempt happened.

// replayBufferLimit caps how much of the opening write is held for a possible
// replay. A TLS ClientHello or an HTTP request head fits easily; past that the
// connection is committed and the buffer is dropped.
const replayBufferLimit = 64 * 1024

// replayConn defers judgement on a connection until the proxy has answered.
//
// Writes pass through immediately and are copied aside; the first read waits for
// the answer, which it would have done anyway since nothing can come back before
// the destination is connected. If the answer says the tunnel never reached the
// destination, another group is dialed and the copied opening write is replayed
// onto it — the caller sees one continuous connection and never learns that the
// first attempt happened.
//
// It deliberately does not expose Upstream(). Doing so would let common.Cast
// reach past it to the tunnel underneath and copy directly onto that, and the
// decision this type exists to defer would be skipped along with the Read and
// Write that carry it. The cost is that the optimisations reached that way stay
// off for the life of the connection, which for a cronet-backed tunnel is small:
// it is not a file descriptor, so there was no splice to lose.
type replayConn struct {
	net.Conn
	smart       *Smart
	destination M.Socksaddr
	key         string
	group       *smartGroup
	member      *smartMember
	exclude     map[string]bool

	access   sync.Mutex
	settled  bool
	replay   []byte
	replayOK bool
	// failingOver is set once the tunnel has been condemned and before the
	// replacement is dialed. While it is set the buffer belongs to the settler:
	// a write arriving in that window must join the replay rather than go onto a
	// connection that is already being abandoned, or it is written to the doomed
	// tunnel, reported as sent, and never replayed.
	failingOver bool
	// closed records that the caller is done, so a failover that completes after
	// Close does not install a replacement nothing will ever close.
	closed bool
	// deadline is the last read deadline the caller set, remembered so the
	// settle wait honours it too. readOnly records which call set it: a deadline
	// carried onto the replacement must be applied with the method the caller
	// used, or SetReadDeadline turns into a write deadline the caller never set
	// — and once that instant passes, every write on the replacement fails.
	deadline time.Time
	readOnly bool
	// settleOnce keeps settling single-flight. Both copy directions reach it,
	// and without this both would perform the failover: two connections dialed,
	// one silently leaked, and the swap applied twice.
	settleOnce sync.Once
	settleErr  error
}

// bufferReplay copies p aside for a possible replay, reporting whether the
// connection is still replayable at all. The lock must be held.
func (c *replayConn) bufferReplay(p []byte) {
	if len(c.replay)+len(p) <= replayBufferLimit {
		c.replay = append(c.replay, p...)
		c.replayOK = true
		return
	}
	// Too much has been said to repeat it; the connection is committed.
	c.replayOK = false
}

func (c *replayConn) Write(p []byte) (int, error) {
	c.access.Lock()
	settled, failingOver := c.settled, c.failingOver
	if !settled {
		c.bufferReplay(p)
	}
	conn := c.Conn
	c.access.Unlock()

	if failingOver {
		// The settler owns the buffer and will flush it onto the replacement, so
		// this write has already been delivered by joining it. Waiting for the
		// settle keeps the caller's ordering honest: it must not be told the
		// bytes are gone before they are.
		if err := c.settle(); err != nil {
			return 0, err
		}
		return len(p), nil
	}

	written, err := conn.Write(p)
	if err == nil || settled {
		return written, err
	}
	// The write is usually the first thing to notice a proxy that has gone away,
	// and it runs concurrently with the read that would otherwise settle the
	// connection. Left alone it reports the error upwards and the whole thing is
	// torn down before the failover can happen, so it settles here as well. The
	// payload is already in the replay buffer, so settling *is* the retry.
	if settleErr := c.settle(); settleErr != nil {
		return written, err
	}
	c.access.Lock()
	replaced := c.Conn != conn
	c.access.Unlock()
	if !replaced {
		return written, err
	}
	return len(p), nil
}

func (c *replayConn) Read(p []byte) (int, error) {
	if err := c.settle(); err != nil {
		return 0, err
	}
	c.access.Lock()
	conn := c.Conn
	c.access.Unlock()
	return conn.Read(p)
}

// settle waits for the proxy's answer and, if it refused, moves the connection
// to another group. Concurrent callers all wait for the first one's outcome.
func (c *replayConn) settle() error {
	c.settleOnce.Do(func() { c.settleErr = c.doSettle() })
	return c.settleErr
}

func (c *replayConn) doSettle() error {
	c.access.Lock()
	if c.settled {
		c.access.Unlock()
		return nil
	}
	conn, member := c.Conn, c.member
	c.access.Unlock()

	measured, isMeasured := common.Cast[naive.MeasuredConn](conn)
	if !isMeasured {
		c.finish()
		return nil
	}

	// Bounded, and by the caller's own deadline when it set one. Without this
	// the wait runs on the outbound's lifetime context, which makes
	// SetReadDeadline silently do nothing on the first read of every connection
	// — and leaves a proxy that accepts the stream but never answers the CONNECT
	// holding that read open forever.
	ctx, cancel := c.settleContext()
	defer cancel()

	err := c.smart.awaitReady(ctx, member, conn)
	var measurement naive.ConnMeasurement
	if err == nil {
		measurement, err = measured.Measure(ctx)
	}
	if err == nil {
		c.smart.record(member, measurement)
		if measurement.HasRemote {
			c.smart.observeScore(c.key, c.destination, c.group, member, measurement)
		}
		c.finish()
		return nil
	}

	c.smart.reportFailure(c.key, c.destination, c.group, member, err)

	// A failed settle is remembered, so no caller will ever use this tunnel
	// again — and closing it right here, before anything acts on the failure,
	// is load-bearing twice over. It pins the tunnel's testimony: after Close
	// no write can reach the stream, so what CarriedPayload answers below is
	// what it will ever answer. And on a relay it pins the tunnel before the
	// failure is reported upstream through RemoteAck: a write still blocked on
	// the establishment gate must not be able to slip onto the wire after the
	// hop downstream has already acted on the report.
	conn.Close()

	c.access.Lock()
	replayable := (c.replayOK || len(c.replay) == 0) && !c.closed
	c.access.Unlock()
	replayable = replayable && definiteFailure(err) && provenUndelivered(err, conn)

	c.access.Lock()
	if c.closed {
		replayable = false
	}
	if replayable {
		// From here the tunnel is condemned: writes stop going onto it and start
		// joining the buffer the flush below owns.
		c.failingOver = true
	}
	c.access.Unlock()
	if !replayable {
		c.finish()
		return err
	}

	var exclude []string
	for tag := range c.exclude {
		exclude = append(exclude, tag)
	}
	replacement, dialErr := c.smart.replayOnAnotherGroup(ctx, c.destination, c.key, exclude...)
	if dialErr != nil {
		c.finish()
		return err
	}

	// The buffer is captured under the same lock that installs the replacement,
	// and only now — capturing it before the dial would drop every write that
	// arrived while it was in progress, having already reported them as sent.
	c.access.Lock()
	buffered, closed, canReplay := c.replay, c.closed, c.replayOK || len(c.replay) == 0
	if closed || !canReplay {
		// Either the caller gave up while the replacement was being dialed, or
		// the writes that arrived in that window pushed the buffer past what can
		// be repeated. Nothing may be handed back a connection it cannot use.
		c.settled, c.failingOver, c.replay = true, false, nil
		c.access.Unlock()
		replacement.Close()
		return err
	}
	if !c.deadline.IsZero() {
		// The caller set this before the swap; it belongs to the connection, not
		// to the tunnel that just failed.
		if c.readOnly {
			_ = replacement.SetReadDeadline(c.deadline)
		} else {
			_ = replacement.SetDeadline(c.deadline)
		}
	}
	if len(buffered) > 0 {
		if _, writeErr := replacement.Write(buffered); writeErr != nil {
			c.settled, c.failingOver, c.replay = true, false, nil
			c.access.Unlock()
			replacement.Close()
			return err
		}
	}
	c.Conn = replacement
	c.settled, c.failingOver, c.replay = true, false, nil
	c.access.Unlock()
	// The condemned tunnel was closed before it was judged, above.
	return nil
}

// finish marks the connection settled without a swap.
func (c *replayConn) finish() {
	c.access.Lock()
	c.settled, c.failingOver, c.replay = true, false, nil
	c.access.Unlock()
}

// settleTimeout bounds settling when the caller set no deadline of its own. It
// is generous because what is being waited for is the proxy dialing the
// destination, and a distant destination is slow rather than broken — the point
// is only that the wait ends.
const settleTimeout = 30 * time.Second

func (c *replayConn) settleContext() (context.Context, context.CancelFunc) {
	if deadline, hasDeadline := c.readDeadline(); hasDeadline {
		return context.WithDeadline(c.smart.ctx, deadline)
	}
	return context.WithTimeout(c.smart.ctx, settleTimeout)
}

func (c *replayConn) readDeadline() (time.Time, bool) {
	c.access.Lock()
	defer c.access.Unlock()
	return c.deadline, !c.deadline.IsZero()
}

// SetDeadline and SetReadDeadline are intercepted so the settle wait — which
// happens inside the caller's first Read — honours them. The value is passed
// through to the tunnel unchanged; recording it here only makes the part of the
// read that this type added obey the same bound as the rest of it.
func (c *replayConn) SetDeadline(t time.Time) error {
	return c.recordDeadline(t, false).SetDeadline(t)
}

func (c *replayConn) SetReadDeadline(t time.Time) error {
	return c.recordDeadline(t, true).SetReadDeadline(t)
}

func (c *replayConn) recordDeadline(t time.Time, readOnly bool) net.Conn {
	c.access.Lock()
	defer c.access.Unlock()
	c.deadline, c.readOnly = t, readOnly
	return c.Conn
}

// The remaining net.Conn methods are intercepted only to read the tunnel under
// the lock. Left to promotion they read the embedded field directly, and that
// read races the swap a failover performs — an interface value is two words,
// and a torn one is not a stale answer but a crash.
func (c *replayConn) current() net.Conn {
	c.access.Lock()
	defer c.access.Unlock()
	return c.Conn
}

func (c *replayConn) SetWriteDeadline(t time.Time) error { return c.current().SetWriteDeadline(t) }
func (c *replayConn) LocalAddr() net.Addr                { return c.current().LocalAddr() }
func (c *replayConn) RemoteAddr() net.Addr               { return c.current().RemoteAddr() }

// RemoteAck reports what the proxy answered, for a relay whose outbound is this
// smart group: without it the relay cannot pass the innermost connect time
// through, its own ack never carries one, and the next smart client inward
// drops every group behind this relay as unrankable — on exactly the chained
// topology the measurements exist for. Settling first is what makes the answer
// exist; when settling failed over, the answer is the replacement tunnel's,
// which is the tunnel the caller is now holding.
func (c *replayConn) RemoteAck(ctx context.Context) (naive.ConnectAck, bool, error) {
	if err := c.settle(); err != nil {
		return naive.ConnectAck{}, false, err
	}
	reporter, isReporter := common.Cast[naive.RemoteDialReporter](c.current())
	if !isReporter {
		return naive.ConnectAck{}, false, nil
	}
	return reporter.RemoteAck(ctx)
}

// definiteFailure reports whether a failure's class claims the destination
// received nothing. The claim alone is not proof — see provenUndelivered,
// which a replay additionally requires. Anything less certain than both risks
// a request arriving twice, which is a worse outcome than a connection the
// caller retries itself.
//
// A deadline expiring is exactly that less-certain case, and it arrives here
// wearing the ErrNextHopUnreachable wrap: awaitReady and the handshake
// classifier put it on every failure, including a settle bounded by the
// caller's own read deadline running out while a slow destination was still
// being dialed. The proxy may deliver the payload moments later, so replaying
// it risks the double arrival this predicate exists to rule out — and the
// failover would be dialed with a context that has already expired anyway. The
// one deadline exempt from this is errLegTimedOut, the derived bound on
// establishing the leg to the proxy: it is this outbound's own verdict rather
// than the caller losing interest.
func definiteFailure(err error) bool {
	if !errors.Is(err, errLegTimedOut) &&
		(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return false
	}
	return errors.Is(err, naive.ErrDestinationUnreachable) ||
		errors.Is(err, naive.ErrNextHopUnreachable)
}

// provenUndelivered reports whether the failure proves the destination
// received nothing, which is what makes replaying the opening write safe.
//
// The proof cannot come from the error's name alone. The tunnel carries the
// opening write as early data — it goes onto the stream as soon as the leg to
// the proxy is up, before the proxy has answered the CONNECT — and
// ErrNextHopUnreachable is the classifier's catch-all: it covers a stream
// that never came up and equally one that came up, carried that write, and
// then broke before the answer arrived. In the second case the proxy may
// already have forwarded the payload, and only the tunnel itself knows which
// case happened. It must be closed before it is asked: its answer is final
// once Close has returned, so no write still blocked on the establishment
// gate can slip onto the wire afterwards and invalidate it.
//
// The one wrap that is proof by itself is ErrDestinationUnreachable: the
// proxy answered that it could not reach the destination, so anything it was
// holding died there. Across a chain that status propagates from the hop that
// failed, so it stays proof on a relay too — but only because every hop is
// fail-closed about producing it: a 502 comes from an explicit
// destination-unreachable classification and never from a default branch (see
// statusForError and settleFromNextHop on the inbound side). A hop that
// answered 502 for an error it merely failed to classify would be handing out
// replay licences for payload still sitting on one of its own legs.
func provenUndelivered(err error, conn net.Conn) bool {
	if errors.Is(err, naive.ErrDestinationUnreachable) {
		return true
	}
	if carrier, isCarrier := common.Cast[naive.PayloadCarrier](conn); isCarrier {
		return !carrier.CarriedPayload()
	}
	// A tunnel that cannot testify. The one failure still provably before any
	// payload is the derived establishment bound: writes gate on the leg
	// coming up, and it never did. That reasoning is only as good as the
	// gating — a future MeasuredConn whose writes buffer locally instead of
	// gating on establishment must implement PayloadCarrier or lose replay,
	// not fall through to here.
	return errors.Is(err, errLegTimedOut)
}

func (c *replayConn) Close() error {
	c.access.Lock()
	conn := c.Conn
	// closed is what a failover still in flight checks before installing its
	// replacement. Without it, closing during a settle closes the tunnel being
	// abandoned and leaves the one dialed to replace it open with no owner.
	c.closed, c.settled, c.replay = true, true, nil
	c.access.Unlock()
	return conn.Close()
}
