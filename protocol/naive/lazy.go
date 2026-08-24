package naive

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
)

const (
	// headerSmart is sent by clients that understand a deferred CONNECT
	// response. Without it the inbound answers immediately, exactly as it always
	// has, so stock naiveproxy clients see no behaviour change at all.
	headerSmart = "-smart"
	// probeSuffix marks a destination the inbound answers itself rather than
	// dialing, so a client can measure and keep alive the client-to-proxy leg
	// without emitting any traffic beyond the proxy. The heartbeat matters most
	// for members carrying no traffic — exactly the ones a failover reaches for,
	// and exactly the ones whose connection pool would otherwise be cold.
	//
	// Only the suffix is fixed. A constant destination arriving on a timer is a
	// clean thing to notice from the outside, so the label in front of it is
	// fresh for every probe; what is left is a family of names rather than a beacon.
	probeSuffix = ".probe.arpa"
)

// ProbeDestination returns a fresh address for one heartbeat.
func ProbeDestination() M.Socksaddr {
	var label [8]byte
	_, _ = rand.Read(label[:])
	return M.Socksaddr{Fqdn: hex.EncodeToString(label[:]) + probeSuffix, Port: 443}
}

// IsProbe reports whether a destination is a heartbeat rather than somewhere to
// dial.
func IsProbe(destination M.Socksaddr) bool {
	return strings.HasSuffix(destination.Fqdn, probeSuffix)
}

// A CONNECT that cannot be established is answered with one of two statuses so a
// client can tell a destination-specific problem from a node-wide one:
// StatusBadGateway means the innermost proxy could not reach the destination,
// StatusServiceUnavailable means a hop could not reach its next hop. Blaming a
// whole node for one unreachable destination — or the reverse — is the
// difference between failing over usefully and thrashing.
var (
	ErrDestinationUnreachable = errors.New("destination unreachable")
	ErrNextHopUnreachable     = errors.New("next hop unreachable")

	// ErrProxyAnswered additionally marks a failure the proxy reported with a
	// status, as opposed to one that reached the client as silence.
	//
	// The two statuses above deliberately do not draw that line: for real
	// traffic a hop saying its next one is gone and a hop saying nothing are
	// the same failure, and collapsing them is what keeps an unclassified error
	// from ever reading as a verdict about the destination. A probe deciding
	// whether to *condemn a node* needs them apart, and this is the difference.
	// A status is something the proxy said about itself. Silence may equally be
	// about the single address that probe happened to ask for — an exit that
	// null-routes one address produces it every round, identically — and
	// telling those two apart is what a second address is for.
	ErrProxyAnswered = errors.New("the proxy answered")

	// ErrClosedLocally marks a tunnel that this process closed while something
	// was still waiting on it — a race dropping the groups it no longer needs, a
	// client hanging up on a connection it opened speculatively. It says nothing
	// about the proxy, which is the whole reason it is told apart.
	//
	// It is this package's own value rather than the transport's, because the
	// code that has to recognise it — the ranking in protocol/group — is built
	// whether or not the naive outbound is, and the transport is not. What
	// carries the transport's equivalent over is localClose, applied on the
	// three methods that ranking calls: Measure, RemoteAck and WaitReady.
	// Anything else reaching past those returns the transport's own value,
	// which does not match this one.
	//
	// It has to be a distinct sentinel rather than net.ErrClosed, which reaches
	// a caller from both directions: cronet's error type reports
	// ERR_CONNECTION_CLOSED, _RESET, _ABORTED and _SOCKET_NOT_CONNECTED as
	// matching that one, and those four are exactly the failures that do say
	// something about a node. Judging on it would excuse a proxy the network had
	// just reset. It still wraps net.ErrClosed, because sing tests for that
	// sentinel to keep a closed connection quiet everywhere else.
	ErrClosedLocally = fmt.Errorf("%w (closed by this end)", net.ErrClosed)
)

// RemoteDialReporter is implemented by outbound connections that can report the
// connect timing the next hop of a chain sent back.
type RemoteDialReporter interface {
	// RemoteAck blocks until the next hop has answered the CONNECT and returns
	// what it reported. It reports false when that hop measured nothing.
	RemoteAck(ctx context.Context) (ConnectAck, bool, error)
}

// statusForError translates a dial failure into the CONNECT status. The 502
// it can produce is consumed downstream as a replay licence — proof that any
// early payload died at a proxy — so it must never come out of a default for
// an error nobody classified. The plain fallthrough below is safe on exactly
// one condition: unclassified errors reach here only from this hop's own dial
// toward the destination (HandshakeFailure), where the failure is by
// construction a verdict about the destination and no payload has moved. The
// one path that handles someone else's errors — the next-hop conversation in
// settleFromNextHop — classifies before calling.
func statusForError(err error) int {
	if errors.Is(err, ErrNextHopUnreachable) {
		return http.StatusServiceUnavailable
	}
	return http.StatusBadGateway
}

// responder holds the CONNECT response back until the destination dial has
// settled. Deferring it lets the reply carry the dial duration and lets a failed
// dial return a status instead of a tunnel that silently goes nowhere. It costs
// nothing on the data path: the proxy cannot forward payload before the dial
// completes either way, so the reply was never the thing gating the first byte.
type responder struct {
	ctx           context.Context
	logger        logger.ContextLogger
	writeResponse func(statusCode int, ack ConnectAck, hasAck bool) error
	timing        *dialer.ConnectTiming
	once          sync.Once
	settled       atomic.Bool
	// resolving is set once success has handed the answer to a background
	// resolver — a relay waiting for its next hop to report. It tells a write
	// arriving before the settle apart from a write on a connection nothing will
	// ever settle: the first must wait for the real answer, the second is the
	// answer. See awaitServed.
	resolving atomic.Bool
	done      chan struct{}
	err       error
	// access guards writeResponse against finished, which has to be exact: on
	// HTTP/2 the write goes to the handler's ResponseWriter, and net/http
	// panics rather than erroring when one is touched after its handler
	// returned. A plain flag checked before the call would still let a write
	// that had already begun straddle the return.
	access   sync.Mutex
	finished bool
}

// errResponseAbandoned is what await reports when the request ended before the
// dial it was waiting on produced an answer. The tunnel is over either way; the
// caller needs to stop rather than write into it.
var errResponseAbandoned = errors.New("connect response abandoned: the request finished first")

func newResponder(ctx context.Context, logger logger.ContextLogger, timing *dialer.ConnectTiming, writeResponse func(int, ConnectAck, bool) error) *responder {
	return &responder{
		ctx:           ctx,
		logger:        logger,
		timing:        timing,
		writeResponse: writeResponse,
		done:          make(chan struct{}),
	}
}

// await blocks until the response has been written. Every write back to the
// client goes through it, which is what stops the deferred header from racing
// the response body.
func (r *responder) await() error {
	if r == nil {
		return nil
	}
	if r.settled.Load() {
		return r.err
	}
	select {
	case <-r.done:
		return r.err
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
}

// awaitServed is await for the write path. Every path that dials reports a
// handshake before any data can flow — the connection manager reports success
// strictly before it starts copying — but the paths that serve the connection
// inside this process never dial and never report: a hijacked DNS query, a dns
// outbound. For those, the first byte written back is itself the proof that the
// "dial" succeeded, so the response is settled here rather than held forever
// against a report that cannot come. No timing is claimed: nothing crossed the
// network, and a zero would read as "on top of the destination".
//
// A relay is the one path that is legitimately unsettled while data could move —
// its answer is being resolved from the next hop in the background — and
// resolving marks it; the write then waits for the real answer, exactly as
// before.
func (r *responder) awaitServed() error {
	if r == nil {
		return nil
	}
	if r.settled.Load() {
		return r.err
	}
	if r.resolving.Load() {
		return r.await()
	}
	return r.settle(http.StatusOK, ConnectAck{}, false)
}

func (r *responder) settle(statusCode int, ack ConnectAck, hasAck bool) error {
	r.once.Do(func() {
		r.access.Lock()
		if r.finished {
			// The request is over and the response has nowhere to go. Dropping
			// it is the whole point: on HTTP/2 writing here is not a failed
			// write, it is a panic that takes the process down.
			r.err = errResponseAbandoned
		} else {
			r.err = r.writeResponse(statusCode, ack, hasAck)
		}
		r.access.Unlock()
		r.settled.Store(true)
		close(r.done)
	})
	return r.err
}

// finish closes the window in which a response may still be written, and is
// called by the handler that owns the writer before it returns.
//
// It exists because a settle can be in flight at that moment and has no way to
// know. The deferred response is resolved from a goroutine when the outbound is
// itself a naive hop — the innermost dial duration is only known once that hop
// answers — so on a relay the answer can arrive after the request it belongs to
// has ended. Taking the lock waits out a write already under way; setting the
// flag under it stops every later one. Seen in production before this existed:
// a relay panicking on "Header called after Handler finished" roughly twenty
// times a day, each one a restart, and each restart a burst of connection
// refused at every client pointed at it.
//
// Only the HTTP/2 path needs it. A hijacked connection outlives its handler by
// design, and answering late on one is correct rather than fatal.
func (r *responder) finish() {
	if r == nil {
		return
	}
	r.access.Lock()
	r.finished = true
	r.access.Unlock()
	// Release anything still waiting on a response that can no longer come.
	r.settle(http.StatusServiceUnavailable, ConnectAck{}, false)
}

// spans reads what the dial cost from the instrumentation on the context.
func (r *responder) spans() (ConnectAck, bool) {
	total, connect, hasConnect, ok := r.timing.Spans()
	if !ok {
		return ConnectAck{}, false
	}
	ack := ConnectAck{Total: total, Connect: connect, HasConnect: hasConnect}
	if hasConnect {
		// Only alongside a connect: the pair is what carries the invariant, and
		// a resolution span next to an absent connect describes nothing a client
		// can put anywhere.
		ack.Resolve, ack.HasResolve = r.timing.Resolution()
	}
	return ack, true
}

// success answers the CONNECT after a successful dial. When the outbound is
// itself a naive hop the innermost duration is only known once that hop answers,
// so it is resolved in the background: HTTP/2 delivers the next hop's response
// headers strictly before any of its body data, which means the value is always
// in hand by the time there is something to write back, and the data path never
// waits on it.
func (r *responder) success(remoteConn net.Conn) error {
	if r == nil {
		return nil
	}
	if remoteConn != nil {
		if reporter, isReporter := common.Cast[RemoteDialReporter](remoteConn); isReporter {
			// Marked before the goroutine exists, so a write racing this call
			// waits for the next hop's answer instead of settling a plain 200
			// over it.
			r.resolving.Store(true)
			go r.settleFromNextHop(reporter)
			return nil
		}
	}
	ack, hasAck := r.spans()
	return r.settle(http.StatusOK, ack, hasAck)
}

func (r *responder) settleFromNextHop(reporter RemoteDialReporter) {
	remoteAck, hasRemoteAck, err := reporter.RemoteAck(r.ctx)
	if err != nil {
		if !errors.Is(err, ErrDestinationUnreachable) && !errors.Is(err, ErrNextHopUnreachable) {
			// An unclassified error out of the next-hop conversation is this
			// hop failing to finish that conversation, not a verdict about the
			// destination. Left bare it would fall through statusForError to
			// 502 — a replay licence the client acts on while the payload may
			// still be sitting on one of this hop's legs.
			err = fmt.Errorf("%w: %w", ErrNextHopUnreachable, err)
		}
		if settleErr := r.settle(statusForError(err), ConnectAck{}, false); settleErr != nil {
			r.logger.DebugContext(r.ctx, "write connect response: ", settleErr)
		}
		return
	}
	// Pass the next hop's connect time through unchanged: it is the only honest
	// one on a relay, where this hop's own dial measured a proxy session rather
	// than the distance to the destination.
	if hasRemoteAck && remoteAck.HasConnect {
		r.timing.RecordInnerAck(remoteAck.Connect)
		if remoteAck.HasResolve {
			r.timing.RecordInnerResolution(remoteAck.Resolve)
		}
	}
	ack, hasAck := r.spans()
	if settleErr := r.settle(http.StatusOK, ack, hasAck); settleErr != nil {
		r.logger.DebugContext(r.ctx, "write connect response: ", settleErr)
	}
}

func (r *responder) failure(err error) error {
	if r == nil {
		return nil
	}
	return r.settle(statusForError(err), ConnectAck{}, false)
}

func h2ResponseWriter(writer http.ResponseWriter, flusher http.Flusher) func(int, ConnectAck, bool) error {
	return func(statusCode int, ack ConnectAck, hasAck bool) error {
		header := writer.Header()
		header.Set("Padding", generatePaddingHeader())
		if statusCode == http.StatusOK && hasAck {
			header.Set(ConnectAckHeader, FormatConnectAck(ack))
		}
		writer.WriteHeader(statusCode)
		flusher.Flush()
		return nil
	}
}

func hijackedResponseWriter(conn net.Conn, protoMajor int, protoMinor int) func(int, ConnectAck, bool) error {
	return func(statusCode int, ack ConnectAck, hasAck bool) error {
		var response strings.Builder
		response.WriteString("HTTP/")
		response.WriteString(strconv.Itoa(protoMajor))
		response.WriteString(".")
		response.WriteString(strconv.Itoa(protoMinor))
		response.WriteString(" ")
		response.WriteString(strconv.Itoa(statusCode))
		response.WriteString(" ")
		response.WriteString(http.StatusText(statusCode))
		response.WriteString("\r\nPadding: ")
		response.WriteString(generatePaddingHeader())
		if statusCode == http.StatusOK && hasAck {
			response.WriteString("\r\n")
			response.WriteString(ConnectAckHeader)
			response.WriteString(": ")
			response.WriteString(FormatConnectAck(ack))
		}
		response.WriteString("\r\n\r\n")
		_, err := conn.Write([]byte(response.String()))
		return err
	}
}
