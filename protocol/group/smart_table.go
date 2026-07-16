package group

import (
	"container/list"
	"net"
	"net/netip"
	"sort"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"

	"golang.org/x/net/publicsuffix"
)

// smartWindowCap is the fixed sample window per (target, member) for total
// first-write→first-read durations; medians over it drive all decisions.
const smartWindowCap = 16

// smartTTFBWindowCap holds application-layer first-byte samples used only by
// origin-affinity evaluation.
const smartTTFBWindowCap = 64

type smartWindow struct {
	samples []float64
	next    int
}

func newSmartWindow(capacity int) *smartWindow {
	return &smartWindow{samples: make([]float64, 0, capacity)}
}

func (w *smartWindow) Push(v float64) {
	if len(w.samples) < cap(w.samples) {
		w.samples = append(w.samples, v)
		w.next = len(w.samples) % cap(w.samples)
		return
	}
	w.samples[w.next] = v
	w.next = (w.next + 1) % cap(w.samples)
}

func (w *smartWindow) Count() int {
	return len(w.samples)
}

func (w *smartWindow) Min() (float64, bool) {
	if len(w.samples) == 0 {
		return 0, false
	}
	minVal := w.samples[0]
	for _, v := range w.samples[1:] {
		if v < minVal {
			minVal = v
		}
	}
	return minVal, true
}

func (w *smartWindow) Values() []float64 {
	out := make([]float64, len(w.samples))
	copy(out, w.samples)
	return out
}

func (w *smartWindow) Median() (float64, bool) {
	return w.Quantile(0.5)
}

func (w *smartWindow) Quantile(q float64) (float64, bool) {
	if len(w.samples) == 0 {
		return 0, false
	}
	sorted := make([]float64, len(w.samples))
	copy(sorted, w.samples)
	sort.Float64s(sorted)
	idx := int(q * float64(len(sorted)-1))
	return sorted[idx], true
}

// smartTargetStats is the per-(target, member) record. All fields are guarded
// by the owning smartTarget mutex.
type smartTargetStats struct {
	window        *smartWindow
	ttfb          *smartWindow
	lastTTFB      time.Time
	lastSample    time.Time
	lastProbe     time.Time
	lastFail      time.Time
	consecFail    int
	spikeStreak   int
	cooldownUntil time.Time
	backoff       int
	needsRecovery bool
	lastRecovery  time.Time
}

func newSmartTargetStats() *smartTargetStats {
	return &smartTargetStats{
		window: newSmartWindow(smartWindowCap),
		ttfb:   newSmartWindow(smartTTFBWindowCap),
	}
}

// smartTarget is one learned-table entry, keyed either exactly (full FQDN /
// exact IP) or aggregate (eTLD+1 / IP prefix). Samples are dual-written to
// both levels; decisions read the exact entry, which inherits the aggregate
// entry's current member at creation.
type smartTarget struct {
	mu        sync.Mutex
	key       string
	aggregate *smartTarget // nil on aggregate entries themselves
	probeHost string
	port      uint16

	current    string
	currentAt  time.Time
	currentWhy string
	lastUsed   time.Time
	switches   int

	// all-members-failed throttle: new connections before this instant get a
	// fail-fast decision instead of another full retry chain
	downRetryAt time.Time

	// improvement channel state
	challenger    string
	challengerWhy string // smartReasonPreferred (threshold walk) or smartReasonImprove (latency)
	confirmStreak int

	// origin affinity state
	affinity       string
	affinityCand   string
	affinityStreak int
	revokeStreak   int
	ttfbEvalTick   int

	exploreN uint64

	members map[string]*smartTargetStats
}

// clearChallengerLocked drops any pending improvement/preference challenge;
// t.mu must be held.
func (t *smartTarget) clearChallengerLocked() {
	t.challenger = ""
	t.challengerWhy = ""
	t.confirmStreak = 0
}

func (t *smartTarget) stats(tag string) *smartTargetStats {
	s, ok := t.members[tag]
	if !ok {
		s = newSmartTargetStats()
		t.members[tag] = s
	}
	return s
}

// smartTable is an LRU-bounded map of smartTarget entries.
type smartTable struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	lru     *list.List // front = most recently used; values are *smartTarget
	max     int
	ttl     time.Duration // in-memory record expiry; 0 disables
}

func newSmartTable(maxTargets int, recordTTL time.Duration) *smartTable {
	return &smartTable{
		entries: make(map[string]*list.Element),
		lru:     list.New(),
		max:     maxTargets,
		ttl:     recordTTL,
	}
}

func (tb *smartTable) get(key string) *smartTarget {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.getLocked(key)
}

func (tb *smartTable) getLocked(key string) *smartTarget {
	if elem, ok := tb.entries[key]; ok {
		tb.lru.MoveToFront(elem)
		return elem.Value.(*smartTarget)
	}
	return nil
}

func (tb *smartTable) put(t *smartTarget) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.putLocked(t)
}

func (tb *smartTable) putLocked(t *smartTarget) {
	if elem, ok := tb.entries[t.key]; ok {
		elem.Value = t
		tb.lru.MoveToFront(elem)
		return
	}
	tb.entries[t.key] = tb.lru.PushFront(t)
	for tb.lru.Len() > tb.max {
		oldest := tb.lru.Back()
		if oldest == nil {
			break
		}
		tb.lru.Remove(oldest)
		delete(tb.entries, oldest.Value.(*smartTarget).key)
	}
}

func (tb *smartTable) len() int {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.lru.Len()
}

func (tb *smartTable) all() []*smartTarget {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	targets := make([]*smartTarget, 0, tb.lru.Len())
	for elem := tb.lru.Front(); elem != nil; elem = elem.Next() {
		targets = append(targets, elem.Value.(*smartTarget))
	}
	return targets
}

// ensure returns the exact-level entry for the given keys, creating both
// levels as needed. A newly created exact entry inherits the aggregate
// entry's sticky member so new subdomains start from the site-level prior.
// The whole lookup-or-create runs under one table lock so concurrent first
// dials to the same key always share a single entry (lock order is strictly
// tb.mu → target.mu; no path acquires them in reverse).
func (tb *smartTable) ensure(exactKey, aggKey, probeHost string, port uint16, now time.Time) *smartTarget {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	var agg *smartTarget
	if aggKey != "" && aggKey != exactKey {
		agg = tb.getLocked(aggKey)
		if agg == nil {
			agg = &smartTarget{
				key:       aggKey,
				probeHost: probeHost,
				port:      port,
				members:   make(map[string]*smartTargetStats),
			}
			tb.putLocked(agg)
		}
		agg.mu.Lock()
		tb.expireLocked(agg, now)
		agg.lastUsed = now
		agg.mu.Unlock()
	}
	exact := tb.getLocked(exactKey)
	if exact == nil {
		exact = &smartTarget{
			key:       exactKey,
			aggregate: agg,
			probeHost: probeHost,
			port:      port,
			members:   make(map[string]*smartTargetStats),
		}
		if agg != nil {
			agg.mu.Lock()
			exact.current = agg.current
			exact.currentAt = agg.currentAt
			exact.currentWhy = agg.currentWhy
			agg.mu.Unlock()
		}
		tb.putLocked(exact)
	} else if agg != nil {
		exact.mu.Lock()
		if exact.aggregate == nil {
			exact.aggregate = agg
		}
		exact.mu.Unlock()
	}
	exact.mu.Lock()
	tb.expireLocked(exact, now)
	exact.lastUsed = now
	exact.port = port
	exact.mu.Unlock()
	return exact
}

// expireLocked resets a record whose last use is beyond the TTL, so a
// long-running process applies the same expiry the snapshot loader does.
// t.mu must be held.
func (tb *smartTable) expireLocked(t *smartTarget, now time.Time) {
	if tb.ttl <= 0 || t.lastUsed.IsZero() || now.Sub(t.lastUsed) <= tb.ttl {
		return
	}
	t.current = ""
	t.currentAt = time.Time{}
	t.currentWhy = ""
	t.challenger = ""
	t.challengerWhy = ""
	t.confirmStreak = 0
	t.affinity = ""
	t.affinityCand = ""
	t.affinityStreak = 0
	t.revokeStreak = 0
	t.ttfbEvalTick = 0
	t.exploreN = 0
	t.downRetryAt = time.Time{}
	t.members = make(map[string]*smartTargetStats)
}

// smartKeys derives (exact key, aggregate key, probe host) from connection
// metadata. Domains aggregate to eTLD+1; bare IPs aggregate to /24 (v6 /48).
// Bare fake IPs carry no routable meaning and are excluded from the table.
func smartKeys(metadata *adapter.InboundContext) (exactKey string, aggKey string, probeHost string, ok bool) {
	if metadata == nil {
		return "", "", "", false
	}
	var host string
	if metadata.Destination.IsDomain() {
		host = metadata.Destination.Fqdn
	} else if metadata.SniffHost != "" {
		host = metadata.SniffHost
	} else {
		host = metadata.Domain
	}
	if host != "" && net.ParseIP(host) == nil {
		exactKey = host
		aggKey = host
		if etld, err := publicsuffix.EffectiveTLDPlusOne(host); err == nil {
			aggKey = etld
		}
		return exactKey, aggKey, host, true
	}
	var addr netip.Addr
	if len(metadata.DestinationAddresses) > 0 {
		addr = metadata.DestinationAddresses[0]
	} else {
		addr = metadata.Destination.Addr
	}
	if !addr.IsValid() || metadata.FakeIP {
		return "", "", "", false
	}
	exactKey = addr.String()
	prefixBits := 24
	if addr.Is6() {
		prefixBits = 48
	}
	if prefix, err := addr.Prefix(prefixBits); err == nil {
		aggKey = prefix.String()
	} else {
		aggKey = exactKey
	}
	return exactKey, aggKey, exactKey, true
}
