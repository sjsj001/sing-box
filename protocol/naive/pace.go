package naive

import (
	"net"
	"net/netip"
	"sync"
	"time"
)

const (
	// Writes under this size are never held: TLS records of HEADERS, control
	// frames and small responses. The answer to a CONNECT must not wait
	// behind a download's queue.
	paceSmallWrite = 4 << 10
	// The gate never holds the queue below this, whatever the delivery rate
	// says.
	paceFloor = 64 << 10
	// Once a path is found without a queue, writes in the next millisecond go
	// without reading TCP_INFO again; a queue that starts building is seen at
	// the first write after that. Without it, unlimited over 8 streams, a
	// prototype cost the relay 1.42s of CPU per GB against 1.09s ungated.
	paceFresh = time.Millisecond
	// A path's min RTT is the least that its connections reported within
	// pathReportFresh. pathTableSize bounds the paths kept: a public relay is
	// scanned from everywhere. A path with no connection left is dropped once
	// pathLinger has passed, when the room is needed.
	pathReportFresh = 10 * time.Second
	pathTableSize   = 512
	pathLinger      = 5 * time.Minute
)

// paceMaxWait is the longest one write is held. Past it the write goes, and
// the kernel's own blocking bounds the queue, as it did before the gate. 1s
// did not drain the queue a download builds while starting at 10Mbps; a
// prototype at 30s did visibly better at 50Mbps than at 1s. A variable so
// tests can shorten it.
var paceMaxWait = 10 * time.Second

// pacedConn holds the relay's bulk writes while its own send queue keeps the
// path's queueing delay above a target.
//
// One h2 session through a relay carries every stream of a client, and what
// one download may have in flight is bounded by the HTTP/2 flow-control
// windows, not by the relay's congestion control: naive clients advertise
// 128MB per session and 64MB per stream, BBR does not keep that out of the
// bottleneck queue, and every other stream of the session waits behind it.
// Measured in a Linux netns (35ms RTT, htb, a 2000-packet FIFO, 8 parallel
// downloads through a two-hop naive chain, a small request timed every 0.5s
// on the same path) with a 40ms target: at 10Mbps the small request took
// 175ms median and 0.51s worst against 10.8s and 14.7s without the gate, for
// 9 instead of 10Mbps; at 50Mbps 128–145ms and 0.37–1.2s worst against 0.7s
// and 8.8s, for 47 instead of 48; unlimited 1764 against 1888Mbps over 8
// streams, 485 against 485 over one.
//
// The baseline is the path's, not the connection's: every connection to the
// same peer address reports its tcpi_min_rtt, and the gate measures queueing
// against the least reported within pathReportFresh (see pacePath). A
// connection opened while the queue was already there never sees the empty
// path — one measured 204ms — and judged by its own min RTT it never held
// back, filled the queue, and kept it full for every other connection.
// There is no valve for a queue that is not ours: told from this end's
// TCP_INFO alone that guess went wrong (delivery rates summing to 75% above
// the link, a 13.9s srtt the FIFO cannot explain), and one connection
// opening on it set off the next. The rate is tcpi_delivery_rate as it
// stands; a two-second maximum of it read ACK batches as bandwidth, which
// raised the limit and misled the valve.
//
// Two ways it is known to fail, which is why it is off unless configured: a
// bottleneck shared with loss-based traffic it does not carry, where the gate
// yields (pushed down to about 2Mbps while a CUBIC iperf took 45), and a
// route that got longer, which reads as a queue until the kernel's min RTT
// window of five minutes lets go of the shorter one (35 → 135ms held a
// download at 28Mbps against 47 without the gate).
//
// Only the standard TLS path over TCP is gated (stdListener): the gate sits
// under crypto/tls, whose one raw Write per record makes a bulk write
// recognisable by its size. The lazy path (reality, kTLS), h2c without TLS
// and HTTP/3 are not gated.
//
// crypto/tls writes records under its own lock, but SetWriteDeadline comes
// from other goroutines, so the state is under access. Close needs nothing
// of its own beyond leaving the path: a sleep is at most 10ms, and TCP_INFO
// on a closed socket fails, which opens the gate onto a write that returns
// the close error.
type pacedConn struct {
	net.Conn
	target time.Duration
	path   *pacePath
	// info reads the socket's TCP_INFO, and fails once the socket is closed.
	info   func() (paceInfo, error)
	access sync.Mutex
	// openAt is when the gate last found the path without a queue; see
	// paceFresh.
	openAt        time.Time
	writeDeadline time.Time
}

func (c *pacedConn) Write(p []byte) (int, error) {
	if len(p) >= paceSmallWrite {
		c.wait(len(p))
	}
	return c.Conn.Write(p)
}

// wait returns once a write of n bytes may go, after paceMaxWait at most, or
// when the next sleep would reach the write deadline: the write then meets
// the deadline in the kernel as it would have without the gate, instead of
// the gate failing a write the socket could take.
func (c *pacedConn) wait(n int) {
	start := time.Now()
	for now := start; ; now = time.Now() {
		var sleep time.Duration
		c.access.Lock()
		if now.Sub(c.openAt) >= paceFresh {
			info, err := c.info()
			if err == nil {
				var clear bool
				sleep, clear = paceDecide(info, c.path.report(c, info, now), c.target, n)
				if clear {
					c.openAt = now
				} else {
					c.openAt = time.Time{}
				}
			}
		}
		deadline := c.writeDeadline
		c.access.Unlock()
		if sleep == 0 {
			return
		}
		wake := now.Add(sleep)
		if wake.Sub(start) > paceMaxWait || !deadline.IsZero() && !wake.Before(deadline) {
			return
		}
		time.Sleep(sleep)
	}
}

func (c *pacedConn) SetDeadline(t time.Time) error {
	c.setWriteDeadline(t)
	return c.Conn.SetDeadline(t)
}

func (c *pacedConn) SetWriteDeadline(t time.Time) error {
	c.setWriteDeadline(t)
	return c.Conn.SetWriteDeadline(t)
}

func (c *pacedConn) setWriteDeadline(t time.Time) {
	c.access.Lock()
	c.writeDeadline = t
	c.access.Unlock()
}

func (c *pacedConn) Close() error {
	c.path.leave(c, time.Now())
	return c.Conn.Close()
}

// Upstream lets sing's common.Cast reach the raw conn.
func (c *pacedConn) Upstream() any { return c.Conn }

// paceInfo is what the gate reads of TCP_INFO.
type paceInfo struct {
	rtt    time.Duration // tcpi_rtt, smoothed
	minRTT time.Duration // tcpi_min_rtt; ~0 until the first RTT sample
	rate   uint64        // tcpi_delivery_rate, bytes per second
	queued uint64        // tcpi_unacked × tcpi_snd_mss + tcpi_notsent_bytes
}

// paceDecide returns how long a write of n bytes waits before asking again,
// zero for at once. clear reports that the path has no queue to judge — no
// baseline or rate yet, or queueing within target — rather than a queue of
// ours still under its limit.
func paceDecide(info paceInfo, pathMin, target time.Duration, n int) (sleep time.Duration, clear bool) {
	if pathMin == 0 || info.rate == 0 || info.rtt-pathMin <= target {
		return 0, true
	}
	limit := max(paceFloor, uint64(float64(info.rate)*(pathMin+target).Seconds()))
	if info.queued+uint64(n) <= limit {
		return 0, false
	}
	return min(max(info.rtt/8, time.Millisecond), 10*time.Millisecond), false
}

// pacePath is what the connections to one peer address share: the min RTT
// each last reported.
type pacePath struct {
	access  sync.Mutex
	members map[*pacedConn]paceReport
	// emptyAt is when the last member left.
	emptyAt time.Time
}

type paceReport struct {
	minRTT time.Duration
	at     time.Time
}

// report records c's min RTT and returns the path's: the least reported
// within pathReportFresh by any member, c included. A min RTT of zero or
// above the smoothed RTT is no sample (the kernel starts it at ~0). A
// connection that has left the path is not let back in by a write racing its
// Close, and finds no baseline.
func (p *pacePath) report(c *pacedConn, info paceInfo, now time.Time) time.Duration {
	own := info.minRTT
	if own > info.rtt {
		own = 0
	}
	p.access.Lock()
	defer p.access.Unlock()
	if _, isMember := p.members[c]; !isMember {
		return 0
	}
	p.members[c] = paceReport{minRTT: own, at: now}
	// ponytail: a scan of the members per check; keep the minimum with its
	// expiry if a path ever carries hundreds of connections.
	var pathMin time.Duration
	for _, r := range p.members {
		if r.minRTT > 0 && now.Sub(r.at) < pathReportFresh && (pathMin == 0 || r.minRTT < pathMin) {
			pathMin = r.minRTT
		}
	}
	return pathMin
}

func (p *pacePath) leave(c *pacedConn, now time.Time) {
	p.access.Lock()
	delete(p.members, c)
	if len(p.members) == 0 {
		p.emptyAt = now
	}
	p.access.Unlock()
}

// pacePaths holds the paths by peer address.
type pacePaths struct {
	access sync.Mutex
	paths  map[netip.Addr]*pacePath
}

var pacePathsByPeer = &pacePaths{paths: make(map[netip.Addr]*pacePath)}

// join puts c on the path to peer. With pathTableSize paths kept and none of
// them empty for pathLinger, it returns nil and c goes ungated.
func (t *pacePaths) join(peer netip.Addr, c *pacedConn, now time.Time) *pacePath {
	t.access.Lock()
	defer t.access.Unlock()
	path := t.paths[peer]
	if path == nil {
		if len(t.paths) >= pathTableSize {
			for key, kept := range t.paths {
				kept.access.Lock()
				stale := len(kept.members) == 0 && now.Sub(kept.emptyAt) > pathLinger
				kept.access.Unlock()
				if stale {
					delete(t.paths, key)
				}
			}
			if len(t.paths) >= pathTableSize {
				return nil
			}
		}
		path = &pacePath{members: make(map[*pacedConn]paceReport)}
		t.paths[peer] = path
	}
	path.access.Lock()
	path.members[c] = paceReport{}
	path.access.Unlock()
	return path
}
