package group

import (
	"math"
	"time"
)

// smart_measure.go holds the measurement primitives: a rolling-minimum local
// baseline (bufferbloat-immune), an EWMA remote estimate with a freshness
// clock, outlier rejection, and the local congestion judgment. All are
// deterministic given an explicit `now`, so they unit-test directly.
// See tingly-riding-parrot-final.md §2, §3 (sample quality), §4 (judgment).

// rollingMin keeps the minimum sample over a sliding time window. The local
// baseline uses this so transient queue inflation (bufferbloat) cannot raise
// it; only a genuinely faster sample lowers it.
type rollingMin struct {
	window  time.Duration
	samples []tsVal
}

type tsVal struct {
	t time.Time
	v float64
}

func newRollingMin(window time.Duration) *rollingMin {
	return &rollingMin{window: window}
}

func (r *rollingMin) add(now time.Time, v float64) {
	r.evict(now)
	r.samples = append(r.samples, tsVal{now, v})
}

func (r *rollingMin) evict(now time.Time) {
	cutoff := now.Add(-r.window)
	i := 0
	for i < len(r.samples) && !r.samples[i].t.After(cutoff) {
		i++
	}
	if i > 0 {
		r.samples = r.samples[i:]
	}
}

// min returns the window minimum and whether any sample is present.
func (r *rollingMin) min(now time.Time) (float64, bool) {
	r.evict(now)
	if len(r.samples) == 0 {
		return 0, false
	}
	m := r.samples[0].v
	for _, s := range r.samples[1:] {
		if s.v < m {
			m = s.v
		}
	}
	return m, true
}

// ewma is an exponentially weighted moving average with a last-update timestamp
// used for the fresh/stale distinction (§0.3).
type ewma struct {
	value   float64
	has     bool
	updated time.Time
}

// update folds a sample into the average. A previous value older than maxAge is
// treated as absent rather than blended: stale is precisely the state in which
// the selector has already stopped trusting that value (rankable drops it), so
// averaging a fresh reading against it would drag the new estimate toward
// evidence nothing else will accept — and leave it there for several rounds.
// After a suspend, a long idle, or a deferred refresh round, the first sample
// back should be believed, not diluted.
func (e *ewma) update(now time.Time, sample, alpha float64, maxAge time.Duration) {
	if e.state(now, maxAge) != sampleFresh {
		e.value = sample
		e.has = true
	} else {
		e.value = alpha*sample + (1-alpha)*e.value
	}
	e.updated = now
}

// state classifies the sample against maxAge (= 2×ProbeInterval).
func (e *ewma) state(now time.Time, maxAge time.Duration) sampleState {
	if !e.has {
		return sampleMissing
	}
	if now.Sub(e.updated) < maxAge {
		return sampleFresh
	}
	return sampleStale
}

type sampleState uint8

const (
	sampleMissing sampleState = iota
	sampleFresh
	sampleStale
)

// isOutlier flags a remote sample as suspicious: a large jump likely from an
// RTO retransmit rather than a real latency change (§3 sample quality). Such a
// sample is not fed into the EWMA on first sight; two in a row are treated as
// real degradation.
func isOutlier(sample, ewmaVal float64) bool {
	return sample > math.Max(3*ewmaVal, ewmaVal+800)
}

// observeRemote records a fresh remote sample into the stat's EWMA with outlier
// gating (§3, G4). A lone outlier (likely an RTO retransmit) is skipped so it
// cannot poison the estimate; two consecutive outliers are accepted as genuine
// severe degradation, and severe=true tells the caller to reroute off the node
// now instead of waiting out the sustained-lead window. Accepting the sample
// (rather than skipping forever) also keeps the EWMA fresh, so a persistently
// degraded node cannot go stale and get pinned by the sticky fast path.
// The outlier test needs a FRESH baseline to be meaningful: "3× the previous
// estimate" says nothing when the previous estimate predates the gap. Judged
// against a stale value, a correct sample after a route change or a suspend gets
// skipped, and the next one is then read as a confirmed second outlier — a
// severe verdict that drops the sticky, from two perfectly good measurements.
func (hs *hostNodeStat) observeRemote(now time.Time, sample, alpha float64, maxAge time.Duration) (severe bool) {
	if hs.remote.state(now, maxAge) == sampleFresh && isOutlier(sample, hs.remote.value) {
		hs.outlierCount++
		if hs.outlierCount < 2 {
			return false // lone outlier: skipped, EWMA and freshness untouched
		}
		severe = true
	}
	hs.outlierCount = 0
	hs.remote.update(now, sample, alpha, maxAge)
	return severe
}

// pingSample is one node's current-round ping observation used by the
// congestion judgment: ok = ping returned this round, rttMs = its RTT,
// localMin = its rolling-min baseline.
type pingSample struct {
	ok       bool
	rttMs    float64
	localMin float64
}

// localCongested is the common-mode / differential judgment (§4). A witness
// must be "alive but slowed": its ping returned AND its ABSOLUTE inflation
// (current − rolling-min) exceeds queueDelayMs. Absolute (not multiplicative)
// so a shared additive bufferbloat is judged consistently across asymmetric
// baselines. Timed-out / dead / no-sample nodes are excluded as witnesses —
// their non-return is not "inflation", which is what keeps multiple
// simultaneous deaths from vouching for each other. Returns true → the suspect
// is a congestion victim; the caller suppresses (bounded) rather than killing.
func localCongested(others []pingSample, queueDelayMs float64) bool {
	for _, s := range others {
		if s.ok && s.rttMs-s.localMin > queueDelayMs {
			return true
		}
	}
	return false
}
