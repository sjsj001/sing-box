package group

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	E "github.com/sagernet/sing/common/exceptions"
)

// smartRaceBufLimit caps client bytes buffered before a winner is decided.
// A TLS ClientHello is well under this; hitting the cap without any server
// byte means something is deeply wrong and the race fails.
const smartRaceBufLimit = 64 * 1024

// smartRaceHead is the buffer for the winner-deciding first read.
const smartRaceHead = 32 * 1024

// smartRaceFlags lets measurement callbacks distinguish "lost the race and
// was closed" (not a failure) from a genuine path failure.
type smartRaceFlags struct {
	abandoned atomic.Bool
}

type smartRaceCandidate struct {
	tag   string
	flags *smartRaceFlags
	dial  func() (net.Conn, error)
	// delay postpones this candidate's dial (hedged request): it starts only
	// after the delay elapses or another candidate fails, and is skipped
	// entirely if a winner is decided first.
	delay time.Duration

	conn    net.Conn
	dialErr error
	written int
	failed  bool
}

// smartRaceConn implements cold-start racing under early-data dial
// semantics: the caller's first bytes (the ClientHello) are fanned out to
// every candidate; the first candidate to deliver a response byte wins and
// carries the connection; losers are closed.
type smartRaceConn struct {
	mu         sync.Mutex
	cond       *sync.Cond
	candidates []*smartRaceCandidate
	buf        []byte
	winner     *smartRaceCandidate
	winnerErr  error // winner's replay write failed; conn is write-dead
	head       []byte
	failed     int
	firstErr   error
	closed     bool
	wake       chan struct{} // closed on first failure/close: delayed candidates start early
	wakeOnce   sync.Once

	readDeadline  time.Time
	writeDeadline time.Time

	onWinner func(tag string)
}

func newSmartRaceConn(candidates []*smartRaceCandidate, onWinner func(tag string)) *smartRaceConn {
	race := &smartRaceConn{
		candidates: candidates,
		onWinner:   onWinner,
		wake:       make(chan struct{}),
	}
	race.cond = sync.NewCond(&race.mu)
	for _, candidate := range candidates {
		go race.runCandidate(candidate)
	}
	return race
}

func (r *smartRaceConn) wakeDelayed() {
	r.wakeOnce.Do(func() { close(r.wake) })
}

func (r *smartRaceConn) runCandidate(candidate *smartRaceCandidate) {
	if candidate.delay > 0 {
		timer := time.NewTimer(candidate.delay)
		select {
		case <-timer.C:
		case <-r.wake:
			timer.Stop()
		}
		r.mu.Lock()
		if r.closed || r.winner != nil {
			// The primary already delivered (or the conn is gone): the
			// hedge never fires.
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
	}
	conn, err := candidate.dial()
	r.mu.Lock()
	if r.closed || r.winner != nil {
		r.mu.Unlock()
		if conn != nil {
			candidate.flags.abandoned.Store(true)
			conn.Close()
		}
		return
	}
	if err != nil {
		candidate.dialErr = err
		r.noteFailureLocked(candidate, err)
		r.mu.Unlock()
		return
	}
	candidate.conn = conn
	if !r.readDeadline.IsZero() {
		conn.SetReadDeadline(r.readDeadline)
	}
	if !r.writeDeadline.IsZero() {
		conn.SetWriteDeadline(r.writeDeadline)
	}
	r.mu.Unlock()

	go r.pumpWrites(candidate)

	head := make([]byte, smartRaceHead)
	n, err := conn.Read(head)
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	if r.winner != nil {
		// Lost while blocked in Read (winner decided elsewhere closed us).
		r.mu.Unlock()
		return
	}
	// n > 0 with a non-nil error (data + EOF in one read) is still a win per
	// the io.Reader contract; the error resurfaces on the next direct Read.
	if n == 0 {
		r.noteFailureLocked(candidate, err)
		r.mu.Unlock()
		return
	}
	r.winner = candidate
	r.head = head[:n]
	var losers []net.Conn
	for _, other := range r.candidates {
		if other != candidate && other.conn != nil {
			other.flags.abandoned.Store(true)
			losers = append(losers, other.conn)
		}
	}
	r.cond.Broadcast()
	if r.onWinner != nil {
		go r.onWinner(candidate.tag)
	}
	r.mu.Unlock()
	// Loser conns may be arbitrary protocol stacks whose Close blocks; keep
	// that out of the critical section.
	for _, loser := range losers {
		loser.Close()
	}
}

// noteFailureLocked records a candidate's failure at most once: a broken conn
// can error in both its write pump and its head read, and double-counting
// would trip "all failed" while another candidate is still racing.
func (r *smartRaceConn) noteFailureLocked(candidate *smartRaceCandidate, err error) {
	if candidate.failed {
		return
	}
	candidate.failed = true
	r.failed++
	if r.firstErr == nil && err != nil {
		r.firstErr = err
	}
	// Any failure starts delayed (hedge) candidates immediately.
	r.wakeDelayed()
	if r.failed >= len(r.candidates) {
		r.cond.Broadcast()
	}
}

// pumpWrites replays buffered client bytes to one candidate and keeps it in
// sync until a winner is decided.
func (r *smartRaceConn) pumpWrites(candidate *smartRaceCandidate) {
	for {
		r.mu.Lock()
		for !r.closed && r.winner == nil && candidate.written >= len(r.buf) {
			r.cond.Wait()
		}
		if r.closed || (r.winner != nil && r.winner != candidate) {
			r.mu.Unlock()
			return
		}
		if r.winner == candidate && candidate.written >= len(r.buf) {
			r.cond.Broadcast() // drained; direct writes may proceed
			r.mu.Unlock()
			return
		}
		chunk := make([]byte, len(r.buf)-candidate.written)
		copy(chunk, r.buf[candidate.written:])
		r.mu.Unlock()

		if _, err := candidate.conn.Write(chunk); err != nil {
			r.mu.Lock()
			if !r.closed {
				if r.winner == nil {
					r.noteFailureLocked(candidate, err)
				} else if r.winner == candidate && r.winnerErr == nil {
					// The winner died mid-replay: without this, a Write
					// blocked on the drain wait would never wake.
					r.winnerErr = err
					r.cond.Broadcast()
				}
			}
			r.mu.Unlock()
			return
		}

		r.mu.Lock()
		candidate.written += len(chunk)
		r.mu.Unlock()
	}
}

func (r *smartRaceConn) allFailedLocked() bool {
	return r.failed >= len(r.candidates)
}

func (r *smartRaceConn) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, net.ErrClosed
	}
	if r.winner != nil {
		// Wait until the pump drained the buffer into the winner, then write
		// directly to preserve ordering.
		for r.winner.written < len(r.buf) && !r.closed && r.winnerErr == nil {
			r.cond.Wait()
		}
		if r.closed {
			return 0, net.ErrClosed
		}
		if r.winnerErr != nil {
			return 0, r.winnerErr
		}
		conn := r.winner.conn
		r.mu.Unlock()
		n, err := conn.Write(p)
		r.mu.Lock()
		return n, err
	}
	if r.allFailedLocked() {
		return 0, r.raceError()
	}
	if len(r.buf)+len(p) > smartRaceBufLimit {
		return 0, E.New("race buffer overflow before any response")
	}
	r.buf = append(r.buf, p...)
	r.cond.Broadcast()
	return len(p), nil
}

func (r *smartRaceConn) Read(p []byte) (int, error) {
	r.mu.Lock()
	for r.winner == nil && !r.allFailedLocked() && !r.closed {
		r.cond.Wait()
	}
	if r.closed {
		r.mu.Unlock()
		return 0, net.ErrClosed
	}
	if r.winner == nil {
		err := r.raceError()
		r.mu.Unlock()
		return 0, err
	}
	if len(r.head) > 0 {
		n := copy(p, r.head)
		r.head = r.head[n:]
		r.mu.Unlock()
		return n, nil
	}
	conn := r.winner.conn
	r.mu.Unlock()
	return conn.Read(p)
}

func (r *smartRaceConn) raceError() error {
	if r.firstErr != nil {
		return r.firstErr
	}
	return E.New("all race candidates failed")
}

func (r *smartRaceConn) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	winner := r.winner
	candidates := r.candidates
	r.wakeDelayed()
	r.cond.Broadcast()
	r.mu.Unlock()
	for _, candidate := range candidates {
		if candidate.conn != nil {
			if winner == nil || candidate != winner {
				candidate.flags.abandoned.Store(true)
			}
			candidate.conn.Close()
		}
	}
	return nil
}

func (r *smartRaceConn) LocalAddr() net.Addr {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.winner != nil {
		return r.winner.conn.LocalAddr()
	}
	for _, candidate := range r.candidates {
		if candidate.conn != nil {
			return candidate.conn.LocalAddr()
		}
	}
	return &net.TCPAddr{}
}

func (r *smartRaceConn) RemoteAddr() net.Addr {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.winner != nil {
		return r.winner.conn.RemoteAddr()
	}
	for _, candidate := range r.candidates {
		if candidate.conn != nil {
			return candidate.conn.RemoteAddr()
		}
	}
	return &net.TCPAddr{}
}

func (r *smartRaceConn) SetDeadline(t time.Time) error {
	r.SetReadDeadline(t)
	r.SetWriteDeadline(t)
	return nil
}

func (r *smartRaceConn) SetReadDeadline(t time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readDeadline = t
	for _, candidate := range r.candidates {
		if candidate.conn != nil {
			candidate.conn.SetReadDeadline(t)
		}
	}
	return nil
}

func (r *smartRaceConn) SetWriteDeadline(t time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writeDeadline = t
	for _, candidate := range r.candidates {
		if candidate.conn != nil {
			candidate.conn.SetWriteDeadline(t)
		}
	}
	return nil
}

// settledLocked reports whether the race is fully resolved: a live winner,
// the head buffer handed over, and the replay buffer drained into the winner.
// From that point every Read/Write goes straight to the winner connection.
func (r *smartRaceConn) settledLocked() bool {
	return !r.closed && r.winner != nil && r.winnerErr == nil &&
		len(r.head) == 0 && r.winner.written >= len(r.buf)
}

func (r *smartRaceConn) settled() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.settledLocked()
}

// ReaderReplaceable / WriterReplaceable / NeedHandshakeForRead /
// NeedHandshakeForWrite / Upstream let the copy pipeline swap the settled
// race out of the data path (restoring splice/zero-copy), mirroring
// smartMeasureConn: replaceability is evaluated once after the "handshake"
// (here: the race) completes.
func (r *smartRaceConn) ReaderReplaceable() bool {
	return r.settled()
}

func (r *smartRaceConn) WriterReplaceable() bool {
	return r.settled()
}

func (r *smartRaceConn) NeedHandshakeForRead() bool {
	return !r.settled()
}

func (r *smartRaceConn) NeedHandshakeForWrite() bool {
	return !r.settled()
}

func (r *smartRaceConn) Upstream() any {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.winner != nil {
		return r.winner.conn
	}
	return nil
}

var _ net.Conn = (*smartRaceConn)(nil)
