package group

// smart_engine.go holds the pure per-destination decision logic of the smart
// group: scoring, CDN classification, the selection flow, and the takeover
// (hysteresis) dimension test. Everything here is deterministic and free of
// network / clock dependencies so it can be unit-tested directly; the stateful
// wiring (probing, sticky bookkeeping, sustained-lead timing) lives elsewhere.
//
// See tingly-riding-parrot-final.md §0.3 (sample tri-state / score), §0.4
// (dimensions & margin), §3 (CDN classify + selection + takeover).

// stickyReason is the dimension on which the sticky node was originally chosen.
// Takeover must beat the sticky on this same dimension (§0.4).
type stickyReason uint8

const (
	reasonTotal  stickyReason = iota // min effective total
	reasonRemote                     // lower remote within tolerance band (req.2)
	reasonCDN                        // cleaner node within CDN band (req.1)
)

// String gives each selection dimension a stable log word, so status/switch
// lines read "by=cdn" rather than an opaque numeric code (G15).
func (r stickyReason) String() string {
	switch r {
	case reasonRemote:
		return "remote"
	case reasonCDN:
		return "cdn"
	default:
		return "total"
	}
}

// nodeCandidate is a per-node snapshot used for one selection. localMin and
// remoteEWMA are milliseconds; remoteFresh means the (node,host) remote sample
// is present and fresh (age < 2×ProbeInterval) — only fresh samples are
// comparable (§0.3), so non-fresh nodes are excluded from ranking.
type nodeCandidate struct {
	tag            string
	localMin       float64
	remoteEWMA     float64
	biasMs         int
	localPenaltyMs float64 // differential local-leg degradation penalty (§4, G5)
	remoteFresh    bool
	cleanPriority  uint16

	// usable is the single "may this node carry the request" verdict, filled by
	// Smart.selectable — alive, supports the network, not blocked for this host,
	// and not excluded by a retry. Deliberately ONE field rather than the three
	// separate flags it replaces: the engine only ever ANDs them, and three
	// parallel copies of the predicate is exactly how a node ended up selectable
	// on one path and not another (see Smart.selectable).
	usable bool

	// Region membership.
	region     string
	regionMode string // "prefer" | "equivalent" | ""
	isPrimary  bool
}

// total is the effective score = local_min + remote_ewma + bias_ms, plus any
// transient local-leg degradation penalty (§0.3, G5).
func (c nodeCandidate) total() float64 {
	return c.localMin + c.remoteEWMA + float64(c.biasMs) + c.localPenaltyMs
}

// remote is the proxy→target leg EWMA.
func (c nodeCandidate) remote() float64 { return c.remoteEWMA }

// selectParams are the resolved decision tunables in milliseconds.
type selectParams struct {
	toleranceMs float64 // req.2 close-total band + margin_ms floor
	cdnBandMs   float64 // CDN in-band width
	switchPct   uint16  // takeover margin percent
}

// cdnParams are the resolved CDN-classification tunables in milliseconds.
type cdnParams struct {
	enterMs float64
	exitMs  float64
	quorum  int
}

// marginMs is the takeover margin for a given base value: max(pct%·base,
// tolerance). Always returns milliseconds, so a challenger must beat the sticky
// by at least the larger of a relative and an absolute floor (§0.4).
func marginMs(base float64, pct uint16, tolMs float64) float64 {
	m := base * float64(pct) / 100
	if tolMs > m {
		return tolMs
	}
	return m
}

// classifyCDN decides whether a site is CDN/anycast from its fresh per-node
// remote samples, with enter/exit hysteresis (§3). Returns prevIsCDN when there
// is no data (caller keeps the previous, 60s-cached classification).
func classifyCDN(freshRemotes []float64, prevIsCDN bool, p cdnParams) bool {
	// Too few fresh samples to decide: keep the previous classification rather
	// than declassifying on sparse data (a transient probe miss shouldn't drop
	// a site out of CDN and flap the selection).
	if len(freshRemotes) < p.quorum {
		return prevIsCDN
	}
	threshold := p.enterMs
	if prevIsCDN {
		threshold = p.exitMs
	}
	// Quorum: enough nodes are all close to the target.
	count := 0
	for _, r := range freshRemotes {
		if r < threshold {
			count++
		}
	}
	if count >= p.quorum {
		return true
	}
	// Uniformity: ≥3 nodes, tightly clustered, and absolutely small — the
	// "absolutely small" guard rejects a single far source that is merely
	// uniformly slow from every node (§3).
	if len(freshRemotes) >= 3 {
		mn, mx, sum := freshRemotes[0], freshRemotes[0], 0.0
		for _, r := range freshRemotes {
			if r < mn {
				mn = r
			}
			if r > mx {
				mx = r
			}
			sum += r
		}
		mean := sum / float64(len(freshRemotes))
		if mn > 0 && mx/mn < 1.3 && mean < 60 {
			return true
		}
	}
	return false
}

// regionFilter applies region rules to the alive set: for a "prefer" region
// whose primary is alive, its non-primary members are dropped (§3 step 2).
// Operates on the liveness-filtered set, before the fresh-only restriction, so
// an alive-but-needs-probe primary still suppresses its backups.
func regionFilter(alive []nodeCandidate) []nodeCandidate {
	primaryAlive := make(map[string]bool)
	for _, c := range alive {
		if c.regionMode == "prefer" && c.isPrimary {
			primaryAlive[c.region] = true
		}
	}
	if len(primaryAlive) == 0 {
		return alive
	}
	out := alive[:0:0]
	for _, c := range alive {
		if c.regionMode == "prefer" && !c.isPrimary && primaryAlive[c.region] {
			continue
		}
		out = append(out, c)
	}
	return out
}

// rankable returns the candidates eligible for score ranking: usable, then
// region-filtered, then fresh-only (§3 steps 1-3).
func rankable(cands []nodeCandidate) []nodeCandidate {
	alive := make([]nodeCandidate, 0, len(cands))
	for _, c := range cands {
		if !c.usable {
			continue
		}
		alive = append(alive, c)
	}
	alive = regionFilter(alive)
	fresh := alive[:0:0]
	for _, c := range alive {
		if c.remoteFresh {
			fresh = append(fresh, c)
		}
	}
	return fresh
}

// selectNode runs the selection flow and returns the winner tag, the dimension
// it won on, and ok=false when there is no rankable (fresh) candidate — in
// which case the caller falls back to single-dial priority (§0.3).
func selectNode(cands []nodeCandidate, isCDN bool, p selectParams) (string, stickyReason, bool) {
	set := rankable(cands)
	if len(set) == 0 {
		return "", reasonTotal, false
	}
	if isCDN {
		tag := selectCDN(set, p)
		return tag, reasonCDN, true
	}
	tag, reason := selectByTotal(set, p)
	return tag, reason, true
}

// selectCDN picks, among candidates whose remote is within min+cdnBand, the one
// with the smallest clean_priority (cleanest), breaking ties by total (§3 CDN).
func selectCDN(set []nodeCandidate, p selectParams) string {
	minRemote := set[0].remote()
	for _, c := range set {
		if c.remote() < minRemote {
			minRemote = c.remote()
		}
	}
	limit := minRemote + p.cdnBandMs
	best := -1
	for i, c := range set {
		if c.remote() > limit {
			continue
		}
		if best < 0 ||
			c.cleanPriority < set[best].cleanPriority ||
			(c.cleanPriority == set[best].cleanPriority && c.total() < set[best].total()) {
			best = i
		}
	}
	return set[best].tag
}

// selectByTotal picks the minimum-total node, then within the tolerance band of
// that minimum prefers the lowest remote (req.2). reason=remote when a
// different (higher-total-but-lower-remote) node wins, else reason=total (§3).
func selectByTotal(set []nodeCandidate, p selectParams) (string, stickyReason) {
	minIdx := 0
	for i, c := range set {
		if c.total() < set[minIdx].total() {
			minIdx = i
		}
	}
	limit := set[minIdx].total() + p.toleranceMs
	winner := minIdx
	for i, c := range set {
		if c.total() <= limit && c.remote() < set[winner].remote() {
			winner = i
		}
	}
	if winner == minIdx {
		return set[minIdx].tag, reasonTotal
	}
	return set[winner].tag, reasonRemote
}

// challengerBeatsSticky is the instantaneous dimension test for takeover (§0.4).
// Both nodes must be fresh. bandLimitRemote is min(remote)+cdnBand over the
// current rankable set, used only for reason=cdn. The sustained-over-time
// (MinDwell) requirement is enforced by the caller.
func challengerBeatsSticky(sticky, challenger nodeCandidate, reason stickyReason, bandLimitRemote float64, p selectParams) bool {
	switch reason {
	case reasonRemote:
		return challenger.remote() < sticky.remote()-marginMs(sticky.remote(), p.switchPct, p.toleranceMs)
	case reasonCDN:
		if challenger.remote() > bandLimitRemote {
			return false
		}
		if challenger.cleanPriority < sticky.cleanPriority {
			return true
		}
		if challenger.cleanPriority > sticky.cleanPriority {
			return false
		}
		// Tie on clean_priority → fall back to total with margin, base =
		// sticky.total (§0.4), still restricted to the CDN band above.
		return challenger.total() < sticky.total()-marginMs(sticky.total(), p.switchPct, p.toleranceMs)
	default: // reasonTotal
		return challenger.total() < sticky.total()-marginMs(sticky.total(), p.switchPct, p.toleranceMs)
	}
}
