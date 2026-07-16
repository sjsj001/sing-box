package group

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// smartProbeMinInterval rate-limits probes per (target, member) pair.
const smartProbeMinInterval = 15 * time.Second

// smartProber serializes background probes: per-key singleflight, a global
// concurrency semaphore, and 0–500ms jitter so probe bursts never align.
type smartProber struct {
	ctx     context.Context
	timeout time.Duration
	sem     chan struct{}

	mu       sync.Mutex
	inflight map[string]bool
	lastRun  map[string]time.Time
}

func newSmartProber(ctx context.Context, concurrency int, timeout time.Duration) *smartProber {
	return &smartProber{
		ctx:      ctx,
		timeout:  timeout,
		sem:      make(chan struct{}, concurrency),
		inflight: make(map[string]bool),
		lastRun:  make(map[string]time.Time),
	}
}

// smartProberGCSize bounds the lastRun bookkeeping map: past this size, keys
// idle for many intervals are pruned (targets evicted from the LRU table
// would otherwise leak their prober keys forever).
const smartProberGCSize = 4096

// schedule runs fn in the background unless the same key is already inflight
// or ran within the minimum interval.
func (p *smartProber) schedule(key string, run func()) {
	p.mu.Lock()
	if p.inflight[key] || time.Since(p.lastRun[key]) < smartProbeMinInterval {
		p.mu.Unlock()
		return
	}
	if len(p.lastRun) > smartProberGCSize {
		cutoff := time.Now().Add(-10 * smartProbeMinInterval)
		for k, at := range p.lastRun {
			if at.Before(cutoff) {
				delete(p.lastRun, k)
			}
		}
	}
	p.inflight[key] = true
	p.mu.Unlock()
	go func() {
		defer func() {
			p.mu.Lock()
			delete(p.inflight, key)
			p.lastRun[key] = time.Now()
			p.mu.Unlock()
		}()
		select {
		case <-time.After(time.Duration(rand.Int63n(int64(500 * time.Millisecond)))):
		case <-p.ctx.Done():
			return
		}
		select {
		case p.sem <- struct{}{}:
			defer func() { <-p.sem }()
		case <-p.ctx.Done():
			return
		}
		run()
	}()
}

type smartProbeResult struct {
	totalMs float64 // first-write→first-read over the tunnel; same shape as passive samples
	dialMs  float64 // DialContext duration; ≈0 for early-data protocols
	// earlyData: the dialed connection exposes HandshakeContext — protocol
	// truth that the dial returned before any round trip (naive et al.),
	// making the leg-factor classification definitive without timing.
	earlyData bool
	// timingTrusted: dial/total timings reflect the real path. Plain-HTTP
	// probes are untrusted — ISP transparent proxies answer the port-80 SYN
	// locally (dial ≈ ms) while the response crosses the real path, faking
	// an early-data timing signature (observed live via 1.1.1.1:80).
	timingTrusted bool
}

// smartHandshakeConn is satisfied by early-data protocol connections (naive)
// that can block until the remote end confirms the tunnel is established.
type smartHandshakeConn interface {
	HandshakeContext(ctx context.Context) error
}

// probeTLS measures one member→target path by dialing through the member and
// timing ClientHello→first-response-byte, matching the passive sample shape.
// The handshake result itself is irrelevant (InsecureSkipVerify; even an
// alert proves reachability and yields valid timing).
func (p *smartProber) probeTLS(detour N.Dialer, host string, port uint16) (smartProbeResult, error) {
	ctx, cancel := context.WithTimeout(p.ctx, p.timeout)
	defer cancel()
	start := time.Now()
	conn, err := detour.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddrHostPort(host, port))
	if err != nil {
		return smartProbeResult{}, err
	}
	defer conn.Close()
	dialMs := float64(time.Since(start)) / float64(time.Millisecond)
	_, earlyData := common.Cast[smartHandshakeConn](conn)

	totalCh := make(chan float64, 1)
	measured := newSmartMeasureConn(conn, smartConnCallbacks{
		onTotal: func(ms float64) {
			select {
			case totalCh <- ms:
			default:
			}
		},
	})
	tlsConn := tls.Client(measured, &tls.Config{
		ServerName:         host,
		InsecureSkipVerify: true,
	})
	handshakeErr := tlsConn.HandshakeContext(ctx)
	select {
	case totalMs := <-totalCh:
		return smartProbeResult{totalMs: totalMs, dialMs: dialMs, earlyData: earlyData, timingTrusted: true}, nil
	default:
	}
	if handshakeErr != nil {
		return smartProbeResult{}, handshakeErr
	}
	return smartProbeResult{}, E.New("probe completed without timing sample")
}

// probeHTTP measures one member→target path over a plain-HTTP port with a
// minimal HEAD request, so the wire traffic looks ordinary (a ClientHello on
// port 80 would not). Timing shape matches probeTLS: first-write→first-read.
func (p *smartProber) probeHTTP(detour N.Dialer, host string, port uint16) (smartProbeResult, error) {
	ctx, cancel := context.WithTimeout(p.ctx, p.timeout)
	defer cancel()
	start := time.Now()
	conn, err := detour.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddrHostPort(host, port))
	if err != nil {
		return smartProbeResult{}, err
	}
	defer conn.Close()
	dialMs := float64(time.Since(start)) / float64(time.Millisecond)
	_, earlyData := common.Cast[smartHandshakeConn](conn)

	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
	}
	_, err = fmt.Fprintf(conn, "HEAD / HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", host)
	if err != nil {
		return smartProbeResult{}, err
	}
	requestAt := time.Now()
	buffer := make([]byte, 256)
	n, err := conn.Read(buffer)
	if n == 0 {
		if err == nil {
			err = E.New("probe completed without response")
		}
		return smartProbeResult{}, err
	}
	totalMs := float64(time.Since(requestAt)) / float64(time.Millisecond)
	return smartProbeResult{totalMs: totalMs, dialMs: dialMs, earlyData: earlyData}, nil
}

// probeAnchor measures the health-check endpoint with protocol-appropriate
// payload: TLS on 443, a plain HTTP HEAD elsewhere.
func (p *smartProber) probeAnchor(detour N.Dialer, host string, port uint16) (smartProbeResult, error) {
	if port == 443 {
		return p.probeTLS(detour, host, port)
	}
	return p.probeHTTP(detour, host, port)
}

// legFactorFor classifies a member's dial semantics from one health probe:
// early-data protocols return from dial almost immediately (the round trip
// happens after the first write, so samples span two legs → factor 2);
// blocking protocols spend the round trip in the dial itself (factor 1).
// HandshakeContext on the dialed conn is protocol truth and decides
// immediately. Timing decides only when the probe's timings are trusted
// (TLS anchors) and only at the extremes: anchor server processing pushes
// a blocking member's dial share into a wide ambiguous middle band, and
// untrusted (plain-HTTP) probes cannot vote at all — transparent proxies
// fake near-zero dials there (both observed live; issue 05).
func legFactorFor(result smartProbeResult) (factor float64, definitive bool) {
	if result.earlyData {
		return 2, true
	}
	if !result.timingTrusted || result.totalMs <= 0 {
		return 1, false
	}
	ratio := result.dialMs / result.totalMs
	switch {
	case ratio < 0.2:
		return 2, true
	case ratio >= 0.6:
		return 1, true
	default:
		return 1, false
	}
}

// probeAlive checks reachability of a non-TLS target port without producing
// a timing sample: HandshakeContext when the protocol supports it, otherwise
// a short read deadline that treats fast errors as failure.
func (p *smartProber) probeAlive(detour N.Dialer, host string, port uint16) error {
	ctx, cancel := context.WithTimeout(p.ctx, p.timeout)
	defer cancel()
	conn, err := detour.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddrHostPort(host, port))
	if err != nil {
		return err
	}
	defer conn.Close()
	// common.Cast walks Upstream(): group members hand back wrapped conns
	// (interrupt and friends) that hide HandshakeContext from a direct
	// type assertion.
	if handshakeConn, ok := common.Cast[smartHandshakeConn](conn); ok {
		return handshakeConn.HandshakeContext(ctx)
	}
	// No handshake surface: wait briefly for a server banner or an early
	// error. A timeout means no fast failure — treat the path as alive.
	conn.SetReadDeadline(time.Now().Add(time.Second))
	buffer := make([]byte, 1)
	_, err = conn.Read(buffer)
	if err == nil {
		return nil
	}
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return nil
	}
	return err
}

var _ net.Conn = (*smartMeasureConn)(nil)
