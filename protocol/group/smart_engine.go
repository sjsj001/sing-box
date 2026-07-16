package group

import (
	"sync"
	"sync/atomic"
	"time"

	N "github.com/sagernet/sing/common/network"
)

// smartConfig holds resolved smart-group parameters. All durations are
// pre-validated; zero values are not allowed here (defaults applied by the
// outbound constructor).
type smartConfig struct {
	anycastThreshold time.Duration
	tolerance        time.Duration
	improveRatio     float64
	improveMin       time.Duration
	dwell            time.Duration
	confirmations    int
	failLimit        int
	cooldownBase     time.Duration
	cooldownMax      time.Duration
	sampleTTL        time.Duration
	exploreInterval  int // 0 = exploration disabled
	maxTargets       int
	recordTTL        time.Duration
}

// smartMemberView is a point-in-time snapshot of one member's runtime state,
// supplied by the orchestrator so the engine stays free of I/O.
type smartMemberView struct {
	tag       string
	alive     bool
	udp       bool
	baseline  float64 // local→member floor in ms; 0 when unknown yet
	legFactor float64 // 2 for early-data protocols, 1 for blocking; 0 → 1
}

type smartMemberViews map[string]*smartMemberView

// smartDecision is the outcome of a per-dial selection.
type smartDecision struct {
	attempts  []string // candidate chain, tried in order
	reason    string   // sticky | preferred | affinity | latency | failover | coldstart | explore | fallback
	coldStart bool
}

const (
	smartReasonSticky     = "sticky"
	smartReasonPreferred  = "preferred"
	smartReasonAffinity   = "affinity"
	smartReasonLatency    = "latency"
	smartReasonFailover   = "failover"
	smartReasonColdStart  = "coldstart"
	smartReasonExplore    = "explore"
	smartReasonFallback   = "fallback"
	smartReasonImprove    = "improve"
	smartReasonTargetDown = "target-down"
)

// smartTargetDownWindow bounds the all-members-failed detection: when every
// attempted member failed within this window the target itself is considered
// down and no cooldown is recorded.
const smartTargetDownWindow = 30 * time.Second

// smartTargetDownRetryInterval rate-limits retries against a down target:
// between declarations, at most one connection per interval runs a full
// attempt chain; the rest fail fast without burning upstream dials.
const smartTargetDownRetryInterval = 3 * time.Second

// smartRelapseWindow: a member failing again on a target within this window
// of its last recovery keeps escalating the cooldown backoff instead of
// restarting from the base duration.
const smartRelapseWindow = 5 * time.Minute

// cooldownDuration doubles the base per accumulated backoff step, clamped to
// max; shared by the smart engine and the failover group.
func cooldownDuration(base, max time.Duration, backoff int) time.Duration {
	duration := base << (backoff - 1)
	if duration > max || duration <= 0 {
		duration = max
	}
	return duration
}

// smartSwitchGrace suppresses spike accounting right after a switch: the
// first connections through a fresh current carry one-time costs (session
// establishment, cold caches) that would otherwise read as degradation and
// re-demote the member the moment it takes over. Hard failures are still
// counted during the grace period.
const smartSwitchGrace = 15 * time.Second

// smartIdleSpikeGrace: multiplexed protocols (naive) tear down idle
// transport sessions; the first stream after an idle gap re-establishes one
// and its total inevitably reads as a spike. The first sample after this
// much idle time is treated as warm-up: it enters the window (medians are
// robust) but does not advance the spike streak.
const smartIdleSpikeGrace = 60 * time.Second

type smartEngine struct {
	cfg smartConfig
	// effective priority: preferred prefix + remaining declared order.
	order atomic.Pointer[[]string]

	emit         func(format string, args ...any)                // decision-event logging
	probeRequest func(t *smartTarget, tag string, reason string) // background probe scheduling

	winMu sync.Mutex
	wins  map[string]int64
}

func newSmartEngine(cfg smartConfig, order []string) *smartEngine {
	engine := &smartEngine{
		cfg:          cfg,
		wins:         make(map[string]int64),
		emit:         func(string, ...any) {},
		probeRequest: func(*smartTarget, string, string) {},
	}
	engine.setOrder(order)
	return engine
}

func (e *smartEngine) setOrder(order []string) {
	e.order.Store(&order)
}

func (e *smartEngine) orderNow() []string {
	return *e.order.Load()
}

func (e *smartEngine) recordWin(tag string) {
	e.winMu.Lock()
	e.wins[tag]++
	e.winMu.Unlock()
}

func (e *smartEngine) topWin() string {
	e.winMu.Lock()
	defer e.winMu.Unlock()
	var best string
	var bestN int64 = -1
	for _, tag := range e.orderNow() {
		if e.wins[tag] > bestN {
			best, bestN = tag, e.wins[tag]
		}
	}
	return best
}

// available reports whether a member may serve this target right now. A
// member whose cooldown expired but has not passed a recovery probe stays
// excluded; the engine requests that probe as a side effect of filtering.
func (e *smartEngine) available(t *smartTarget, view *smartMemberView, network string, now time.Time) bool {
	if view == nil || !view.alive {
		return false
	}
	if network == N.NetworkUDP && !view.udp {
		return false
	}
	stats, ok := t.members[view.tag]
	if !ok {
		return true
	}
	if stats.cooldownUntil.After(now) {
		return false
	}
	if stats.needsRecovery {
		e.probeRequest(t, view.tag, "recovery")
		return false
	}
	return true
}

func (e *smartEngine) fresh(stats *smartTargetStats, now time.Time) bool {
	return stats != nil && stats.window.Count() > 0 && now.Sub(stats.lastSample) <= e.cfg.sampleTTL
}

// remoteRTT derives the member→target latency estimate in ms from the stored
// total (first-write→first-read) median, the member baseline floor, and the
// protocol leg factor.
func (e *smartEngine) remoteRTT(t *smartTarget, view *smartMemberView, now time.Time) (float64, bool) {
	stats := t.members[view.tag]
	if !e.fresh(stats, now) {
		return 0, false
	}
	med, ok := stats.window.Median()
	if !ok {
		return 0, false
	}
	leg := view.legFactor
	if leg <= 0 {
		leg = 1
	}
	remote := med - view.baseline
	if remote < 0 {
		remote = 0
	}
	return remote / leg, true
}

// improveMarginMs is the advantage a challenger must show over the current
// member's remote RTT: max(improve_ratio × current, improve_min).
func (e *smartEngine) improveMarginMs(curRemote float64) float64 {
	margin := curRemote * e.cfg.improveRatio
	if improveMin := float64(e.cfg.improveMin) / float64(time.Millisecond); improveMin > margin {
		margin = improveMin
	}
	return margin
}

func (e *smartEngine) totalMedian(t *smartTarget, tag string, now time.Time) (float64, bool) {
	stats := t.members[tag]
	if !e.fresh(stats, now) {
		return 0, false
	}
	return stats.window.Median()
}

// thresholdWinner walks the effective priority order and returns the first
// available member whose fresh remote RTT clears the anycast threshold.
func (e *smartEngine) thresholdWinner(t *smartTarget, members smartMemberViews, network string, now time.Time) string {
	thresholdMs := float64(e.cfg.anycastThreshold) / float64(time.Millisecond)
	for _, tag := range e.orderNow() {
		view := members[tag]
		if !e.available(t, view, network, now) {
			continue
		}
		remote, ok := e.remoteRTT(t, view, now)
		if ok && remote <= thresholdMs {
			return tag
		}
	}
	return ""
}

// latencyWinner picks, among members with fresh data whose total duration
// lies within tolerance of the fastest, the one with the lowest remote RTT.
func (e *smartEngine) latencyWinner(t *smartTarget, members smartMemberViews, network string, now time.Time) string {
	toleranceMs := float64(e.cfg.tolerance) / float64(time.Millisecond)
	minTotal := -1.0
	for _, tag := range e.orderNow() {
		if !e.available(t, members[tag], network, now) {
			continue
		}
		total, ok := e.totalMedian(t, tag, now)
		if ok && (minTotal < 0 || total < minTotal) {
			minTotal = total
		}
	}
	if minTotal < 0 {
		return ""
	}
	var winner string
	bestRemote := -1.0
	for _, tag := range e.orderNow() {
		view := members[tag]
		if !e.available(t, view, network, now) {
			continue
		}
		total, ok := e.totalMedian(t, tag, now)
		if !ok || total > minTotal+toleranceMs {
			continue
		}
		remote, ok := e.remoteRTT(t, view, now)
		if !ok {
			continue
		}
		if bestRemote < 0 || remote < bestRemote {
			winner, bestRemote = tag, remote
		}
	}
	return winner
}

// failoverChain returns up to max additional candidates ordered by
// threshold-walk first, then remote RTT, excluding listed tags.
func (e *smartEngine) failoverChain(t *smartTarget, members smartMemberViews, network string, now time.Time, exclude []string, max int) []string {
	excluded := func(tag string) bool {
		for _, ex := range exclude {
			if ex == tag {
				return true
			}
		}
		return false
	}
	var chain []string
	appendTag := func(tag string) {
		if tag == "" || excluded(tag) {
			return
		}
		for _, c := range chain {
			if c == tag {
				return
			}
		}
		if len(chain) < max {
			chain = append(chain, tag)
		}
	}
	type scored struct {
		tag    string
		remote float64
	}
	var withData []scored
	for _, tag := range e.orderNow() {
		view := members[tag]
		if !e.available(t, view, network, now) {
			continue
		}
		if remote, ok := e.remoteRTT(t, view, now); ok {
			withData = append(withData, scored{tag, remote})
		}
	}
	for i := 0; i < len(withData); i++ {
		best := i
		for j := i + 1; j < len(withData); j++ {
			if withData[j].remote < withData[best].remote {
				best = j
			}
		}
		withData[i], withData[best] = withData[best], withData[i]
		appendTag(withData[i].tag)
	}
	for _, tag := range e.orderNow() {
		if e.available(t, members[tag], network, now) {
			appendTag(tag)
		}
	}
	return chain
}

// coldStartCandidates picks the racing pool for an unknown target: the top of
// the effective priority order, the member winning the most targets globally,
// and the next priority entry, deduplicated, at most three.
func (e *smartEngine) coldStartCandidates(t *smartTarget, members smartMemberViews, network string, now time.Time) []string {
	var candidates []string
	appendTag := func(tag string) {
		if tag == "" || len(candidates) >= 3 {
			return
		}
		for _, c := range candidates {
			if c == tag {
				return
			}
		}
		if e.available(t, members[tag], network, now) {
			candidates = append(candidates, tag)
		}
	}
	for _, tag := range e.orderNow() {
		if e.available(t, members[tag], network, now) {
			appendTag(tag)
			break
		}
	}
	appendTag(e.topWin())
	for _, tag := range e.orderNow() {
		appendTag(tag)
	}
	return candidates
}

// decide selects the candidate chain for one new connection. It must stay
// cheap: the common path is the sticky O(1) branch.
func (e *smartEngine) decide(t *smartTarget, members smartMemberViews, network string, now time.Time) smartDecision {
	t.mu.Lock()
	defer t.mu.Unlock()
	return e.decideLocked(t, members, network, now)
}

func (e *smartEngine) decideLocked(t *smartTarget, members smartMemberViews, network string, now time.Time) smartDecision {
	// Down-target throttle: fail fast between rate-limited retry slots.
	if now.Before(t.downRetryAt) {
		return smartDecision{reason: smartReasonTargetDown}
	}
	// UDP follows the TCP-learned state but never rewrites it: when the
	// sticky member cannot serve this flow, pick a substitute for the flow
	// only (spec: 成员不支持 UDP 时顺延).
	commit := network != N.NetworkUDP
	// Sticky gate.
	if t.current != "" && e.available(t, members[t.current], network, now) {
		// Exploration: threshold-rule targets occasionally lend one real
		// connection to a candidate that still lacks TTFB samples.
		if e.cfg.exploreInterval > 0 && network != N.NetworkUDP &&
			(t.currentWhy == smartReasonPreferred || t.currentWhy == smartReasonAffinity) {
			t.exploreN++
			if t.exploreN%uint64(e.cfg.exploreInterval) == 0 {
				if tag := e.exploreCandidate(t, members, network, now); tag != "" {
					return smartDecision{
						attempts: append([]string{tag}, t.current),
						reason:   smartReasonExplore,
					}
				}
			}
		}
		attempts := append([]string{t.current},
			e.failoverChain(t, members, network, now, []string{t.current}, 2)...)
		if stats := t.members[t.current]; stats != nil && now.Sub(stats.lastSample) > e.cfg.sampleTTL && stats.window.Count() > 0 {
			e.probeRequest(t, t.current, "refresh")
		}
		if commit {
			e.refreshStaleAlternativeLocked(t, members, network, now)
		}
		return smartDecision{attempts: attempts, reason: smartReasonSticky}
	}

	// Re-decision.
	if t.affinity != "" && e.available(t, members[t.affinity], network, now) {
		if commit {
			e.setCurrentLocked(t, t.affinity, smartReasonAffinity, now)
		}
		attempts := append([]string{t.affinity},
			e.failoverChain(t, members, network, now, []string{t.affinity}, 2)...)
		return smartDecision{attempts: attempts, reason: smartReasonAffinity}
	}
	if winner := e.thresholdWinner(t, members, network, now); winner != "" {
		if commit {
			e.setCurrentLocked(t, winner, smartReasonPreferred, now)
		}
		attempts := append([]string{winner},
			e.failoverChain(t, members, network, now, []string{winner}, 2)...)
		return smartDecision{attempts: attempts, reason: smartReasonPreferred}
	}
	if winner := e.latencyWinner(t, members, network, now); winner != "" {
		if commit {
			e.setCurrentLocked(t, winner, smartReasonLatency, now)
		}
		attempts := append([]string{winner},
			e.failoverChain(t, members, network, now, []string{winner}, 2)...)
		return smartDecision{attempts: attempts, reason: smartReasonLatency}
	}
	// No usable data: cold start.
	candidates := e.coldStartCandidates(t, members, network, now)
	if len(candidates) > 0 {
		return smartDecision{attempts: candidates, reason: smartReasonColdStart, coldStart: true}
	}
	// Everything dead or cooling: last-resort fallback, ignoring filters.
	for _, tag := range e.orderNow() {
		if view := members[tag]; view != nil && (network != N.NetworkUDP || view.udp) {
			return smartDecision{attempts: []string{tag}, reason: smartReasonFallback}
		}
	}
	return smartDecision{reason: smartReasonFallback}
}

func (e *smartEngine) exploreCandidate(t *smartTarget, members smartMemberViews, network string, now time.Time) string {
	var best string
	bestCount := smartTTFBWindowCap
	var stalest string
	var stalestAt time.Time
	for _, tag := range e.orderNow() {
		if tag == t.current || !e.available(t, members[tag], network, now) {
			continue
		}
		count := 0
		var lastTTFB time.Time
		if stats, ok := t.members[tag]; ok {
			count = stats.ttfb.Count()
			lastTTFB = stats.lastTTFB
		}
		if count < bestCount {
			best, bestCount = tag, count
		}
		// Full window: rotate by staleness, so the affinity comparison keeps
		// tracking reality instead of freezing at the first 64 samples
		// (spec R2-7).
		if count >= smartTTFBWindowCap && now.Sub(lastTTFB) > e.cfg.sampleTTL {
			if stalest == "" || lastTTFB.Before(stalestAt) {
				stalest, stalestAt = tag, lastTTFB
			}
		}
	}
	if best != "" {
		return best
	}
	return stalest
}

// refreshStaleAlternativeLocked keeps the improvement channels supplied with
// comparison data (spec R2-6): a sticky target probes its stalest non-current
// member once that member's samples and probes have both outlived the sample
// TTL. Without this, alternatives go stale minutes after cold start and the
// better-switch walk can never elect a member that has since improved. At
// most one probe per decision; the prober's per-key rate limit and global
// semaphore bound the aggregate cost.
func (e *smartEngine) refreshStaleAlternativeLocked(t *smartTarget, members smartMemberViews, network string, now time.Time) {
	var stalest string
	var stalestAt time.Time
	for _, tag := range e.orderNow() {
		if tag == t.current || !e.available(t, members[tag], network, now) {
			continue
		}
		var last time.Time
		if stats, ok := t.members[tag]; ok {
			last = stats.lastSample
			if stats.lastProbe.After(last) {
				last = stats.lastProbe
			}
		}
		// A member with no record at all (dropped from a snapshot, never
		// covered by cold start) is infinitely stale.
		if !last.IsZero() && now.Sub(last) <= e.cfg.sampleTTL {
			continue
		}
		if stalest == "" || last.Before(stalestAt) {
			stalest, stalestAt = tag, last
		}
	}
	if stalest != "" {
		e.probeRequest(t, stalest, "refresh-alt")
	}
}

// resampleOutliersLocked requests verification probes for members whose few
// samples look notably worse than the current member's median: a lone
// sample measured through a cold-start probe burst — even one only mildly
// inflated — would otherwise exclude the member from every walk for the
// whole sample TTL (observed live: a preferred member's single 112ms
// race-fill sample kept it just above the anycast threshold). At most two
// extra probes per (target, member) — count reaches 3 and the condition
// stops matching — with the prober's per-key rate limit underneath.
func (e *smartEngine) resampleOutliersLocked(t *smartTarget, members smartMemberViews, network string, now time.Time) {
	curStats := t.members[t.current]
	if curStats == nil {
		return
	}
	curMedian, ok := curStats.window.Median()
	if !ok || curMedian <= 0 {
		return
	}
	for _, tag := range e.orderNow() {
		if tag == t.current || !e.available(t, members[tag], network, now) {
			continue
		}
		stats := t.members[tag]
		if stats == nil || stats.window.Count() == 0 || stats.window.Count() > 2 {
			continue
		}
		median, _ := stats.window.Median()
		if median > 1.5*curMedian {
			e.probeRequest(t, tag, "resample")
		}
	}
}

// setCurrentLocked switches the sticky member; t.mu must be held.
func (e *smartEngine) setCurrentLocked(t *smartTarget, tag string, why string, now time.Time) {
	if t.current == tag {
		t.currentWhy = why
		return
	}
	previous := t.current
	t.current = tag
	t.currentAt = now
	t.currentWhy = why
	t.clearChallengerLocked()
	if previous != "" {
		t.switches++
		e.emit("target %s: switch %s -> %s (%s)", t.key, previous, tag, why)
	} else {
		e.emit("target %s: select %s (%s)", t.key, tag, why)
	}
	e.recordWin(tag)
	if agg := t.aggregate; agg != nil {
		agg.mu.Lock()
		agg.current = tag
		agg.currentAt = now
		agg.currentWhy = why
		agg.mu.Unlock()
	}
}

// onDialSuccess records a successful dial through tag for this target.
// It deliberately does NOT reset consecFail: for session-based protocols
// (naive et al.) a dial only creates a local stream, so alternating
// dial-success/early-failure must still accrue toward the fail limit. The
// reset happens in onPassiveSample, when a response byte proves the path.
func (e *smartEngine) onDialSuccess(t *smartTarget, tag string, now time.Time) {
	t.mu.Lock()
	stats := t.stats(tag)
	stats.needsRecovery = false
	if t.current == "" {
		e.setCurrentLocked(t, tag, smartReasonColdStart, now)
	}
	agg := t.aggregate
	t.mu.Unlock()
	if agg != nil {
		agg.mu.Lock()
		aggStats := agg.stats(tag)
		aggStats.needsRecovery = false
		agg.mu.Unlock()
	}
}

// onDialFailure records a failed dial and applies demotion/cooldown per the
// fast "worse" channel. Failures affect only this (member, target) pair.
func (e *smartEngine) onDialFailure(t *smartTarget, tag string, members smartMemberViews, network string, now time.Time) {
	t.mu.Lock()
	stats := t.stats(tag)
	stats.lastFail = now
	// Failures from connections that predate an active cooldown are already
	// accounted for; letting stragglers re-trigger would escalate backoff
	// from a single event.
	if stats.cooldownUntil.After(now) {
		agg := t.aggregate
		t.mu.Unlock()
		if agg != nil {
			agg.mu.Lock()
			agg.stats(tag).lastFail = now
			agg.mu.Unlock()
		}
		return
	}
	stats.consecFail++
	if stats.consecFail >= e.cfg.failLimit {
		if e.targetDownLocked(t, now) {
			t.downRetryAt = now.Add(smartTargetDownRetryInterval)
			e.emit("target %s: all members failing, treating target as down", t.key)
		} else {
			e.applyCooldownLocked(t, stats, tag, now)
			if t.current == tag && network != N.NetworkUDP {
				e.demoteLocked(t, tag, members, network, now)
			}
		}
	}
	agg := t.aggregate
	t.mu.Unlock()
	if agg != nil {
		agg.mu.Lock()
		aggStats := agg.stats(tag)
		aggStats.consecFail++
		aggStats.lastFail = now
		agg.mu.Unlock()
	}
}

// targetDownLocked reports whether every member that was ever attempted for
// this target failed recently — evidence the target itself is unreachable.
func (e *smartEngine) targetDownLocked(t *smartTarget, now time.Time) bool {
	attempted := 0
	failing := 0
	for _, stats := range t.members {
		if stats.window.Count() == 0 && stats.consecFail == 0 {
			continue
		}
		attempted++
		if !stats.lastFail.IsZero() && now.Sub(stats.lastFail) <= smartTargetDownWindow {
			failing++
		}
	}
	return attempted >= 2 && failing == attempted
}

func (e *smartEngine) applyCooldownLocked(t *smartTarget, stats *smartTargetStats, tag string, now time.Time) {
	if !stats.lastRecovery.IsZero() && now.Sub(stats.lastRecovery) > smartRelapseWindow {
		stats.backoff = 0
	}
	stats.backoff++
	duration := cooldownDuration(e.cfg.cooldownBase, e.cfg.cooldownMax, stats.backoff)
	stats.cooldownUntil = now.Add(duration)
	stats.needsRecovery = true
	stats.consecFail = 0
	stats.spikeStreak = 0
	e.emit("target %s: member %s cooling down for %s (backoff %d)", t.key, tag, duration, stats.backoff)
}

func (e *smartEngine) demoteLocked(t *smartTarget, from string, members smartMemberViews, network string, now time.Time) {
	if winner := e.thresholdWinner(t, members, network, now); winner != "" {
		e.setCurrentLocked(t, winner, smartReasonFailover, now)
		return
	}
	if winner := e.latencyWinner(t, members, network, now); winner != "" {
		e.setCurrentLocked(t, winner, smartReasonFailover, now)
		return
	}
	for _, tag := range e.orderNow() {
		if tag != from && e.available(t, members[tag], network, now) {
			e.setCurrentLocked(t, tag, smartReasonFailover, now)
			return
		}
	}
	t.current = ""
	t.currentWhy = ""
}

// onPassiveSample feeds one measured total duration from real traffic. A
// completed sample is the real success signal: it proves the member carried
// a response byte, so the failure streak resets here (not at dial time).
func (e *smartEngine) onPassiveSample(t *smartTarget, tag string, ms float64, members smartMemberViews, network string, now time.Time) {
	t.mu.Lock()
	stats := t.stats(tag)
	stats.consecFail = 0
	med, hasMed := stats.window.Median()
	// First sample after an idle gap: likely a rebuilt mux session, not path
	// degradation. It still enters the window below.
	idleWarmup := !stats.lastSample.IsZero() && now.Sub(stats.lastSample) > smartIdleSpikeGrace
	stats.window.Push(ms)
	stats.lastSample = now
	degraded := false
	if tag == t.current {
		isSpike := hasMed && stats.window.Count() >= 4 && ms > 2*med
		switch {
		case !isSpike:
			stats.spikeStreak = 0
		case idleWarmup || now.Sub(t.currentAt) <= smartSwitchGrace:
			// Ambiguous sample (cold session / switch transient): neither
			// counts toward demotion nor erases accumulated evidence.
		default:
			stats.spikeStreak++
			if stats.spikeStreak >= 2 {
				degraded = true
				e.emit("target %s: member %s latency spiked (%.0fms > 2x median %.0fms)", t.key, tag, ms, med)
				e.applyCooldownLocked(t, stats, tag, now)
				if network != N.NetworkUDP {
					e.demoteLocked(t, tag, members, network, now)
				}
			}
		}
	}
	if !degraded {
		e.checkChallengerLocked(t, members, network, now)
	}
	agg := t.aggregate
	t.mu.Unlock()
	if agg != nil {
		agg.mu.Lock()
		aggStats := agg.stats(tag)
		aggStats.consecFail = 0
		aggStats.window.Push(ms)
		aggStats.lastSample = now
		agg.mu.Unlock()
	}
}

// checkChallengerLocked looks for a candidate whose fresh remote RTT beats
// the current member by the improvement margin, and requests confirmation
// probes for it. Switching happens only in onProbeResult after the streak.
func (e *smartEngine) checkChallengerLocked(t *smartTarget, members smartMemberViews, network string, now time.Time) {
	if t.current == "" {
		return
	}
	curView := members[t.current]
	if curView == nil {
		return
	}
	curRemote, ok := e.remoteRTT(t, curView, now)
	if !ok {
		return
	}
	e.resampleOutliersLocked(t, members, network, now)
	// An established origin affinity overrides the threshold walk (spec §3.3
	// step 2): while the affinity member holds the target, neither the
	// preferred walk nor latency improvement may challenge it — only
	// revocation unpins. Without this the two channels oscillate forever.
	if t.affinity != "" && t.affinity == t.current {
		if t.challenger != "" {
			t.clearChallengerLocked()
		}
		return
	}
	// Threshold walk next. If the walk elects the current member it is
	// pinned; latency-based improvement must not fight it. If it elects a
	// more-preferred member, that member challenges regardless of latency
	// advantage (spec S3: anycast converges to preferred[0] even when it is
	// not the fastest), still gated by probe confirmation + dwell.
	if walkWinner := e.thresholdWinner(t, members, network, now); walkWinner != "" {
		if walkWinner == t.current {
			if t.challenger != "" {
				t.clearChallengerLocked()
			}
			return
		}
		if t.challenger != walkWinner || t.challengerWhy != smartReasonPreferred {
			t.challenger = walkWinner
			t.challengerWhy = smartReasonPreferred
			t.confirmStreak = 0
		}
		e.probeRequest(t, walkWinner, "confirm")
		return
	}
	margin := e.improveMarginMs(curRemote)
	var best string
	bestRemote := -1.0
	for _, tag := range e.orderNow() {
		if tag == t.current || !e.available(t, members[tag], network, now) {
			continue
		}
		remote, ok := e.remoteRTT(t, members[tag], now)
		if !ok {
			continue
		}
		if bestRemote < 0 || remote < bestRemote {
			best, bestRemote = tag, remote
		}
	}
	if best == "" || curRemote-bestRemote <= margin {
		if t.challenger != "" {
			t.clearChallengerLocked()
		}
		return
	}
	if t.challenger != best || t.challengerWhy != smartReasonImprove {
		t.challenger = best
		t.challengerWhy = smartReasonImprove
		t.confirmStreak = 0
	}
	e.probeRequest(t, best, "confirm")
}

// onProbeResult feeds one active probe outcome.
func (e *smartEngine) onProbeResult(t *smartTarget, tag string, ms float64, err error, members smartMemberViews, network string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	stats := t.stats(tag)
	stats.lastProbe = now
	if err != nil {
		stats.lastFail = now
		if stats.needsRecovery {
			// A failed recovery probe keeps escalating the cooldown — unless
			// the target itself is down, which is not the member's fault.
			if !e.targetDownLocked(t, now) {
				e.applyCooldownLocked(t, stats, tag, now)
			}
		} else {
			stats.consecFail++
			if stats.consecFail >= e.cfg.failLimit && !e.targetDownLocked(t, now) {
				e.applyCooldownLocked(t, stats, tag, now)
				if t.current == tag && network != N.NetworkUDP {
					e.demoteLocked(t, tag, members, network, now)
				}
			}
		}
		if t.challenger == tag {
			t.clearChallengerLocked()
		}
		return
	}
	// Alive-only probes (non-TLS ports) carry no timing; recording their 0
	// would fabricate an impossibly fast sample.
	if ms > 0 {
		stats.window.Push(ms)
		stats.lastSample = now
	}
	if stats.needsRecovery {
		// Backoff is deliberately kept: a relapse within smartRelapseWindow
		// escalates the next cooldown instead of restarting at the base.
		stats.needsRecovery = false
		stats.consecFail = 0
		stats.lastRecovery = now
		e.emit("target %s: member %s recovered", t.key, tag)
	}
	if t.challenger == tag && t.current != "" {
		qualified := false
		if t.challengerWhy == smartReasonPreferred {
			qualified = e.thresholdWinner(t, members, network, now) == tag
		} else {
			curView := members[t.current]
			challengerView := members[tag]
			if curView == nil || challengerView == nil {
				return
			}
			curRemote, okCur := e.remoteRTT(t, curView, now)
			chRemote, okCh := e.remoteRTT(t, challengerView, now)
			if !okCur || !okCh {
				return
			}
			qualified = curRemote-chRemote > e.improveMarginMs(curRemote)
		}
		if qualified {
			why := t.challengerWhy
			if why == "" {
				why = smartReasonImprove
			}
			t.confirmStreak++
			if t.confirmStreak >= e.cfg.confirmations && now.Sub(t.currentAt) >= e.cfg.dwell {
				e.setCurrentLocked(t, tag, why, now)
			} else if t.confirmStreak < e.cfg.confirmations {
				e.probeRequest(t, tag, "confirm")
			}
		} else {
			t.clearChallengerLocked()
		}
	}
}

// onTTFBSample feeds one application-layer first-byte duration and, every 16
// samples, re-evaluates origin affinity for threshold-rule targets.
func (e *smartEngine) onTTFBSample(t *smartTarget, tag string, ms float64, members smartMemberViews, network string, now time.Time) {
	t.mu.Lock()
	stats := t.stats(tag)
	stats.ttfb.Push(ms)
	stats.lastTTFB = now
	t.ttfbEvalTick++
	if t.ttfbEvalTick%16 == 0 {
		e.evaluateAffinityLocked(t, members, network, now)
	}
	agg := t.aggregate
	t.mu.Unlock()
	if agg != nil {
		agg.mu.Lock()
		aggStats := agg.stats(tag)
		aggStats.ttfb.Push(ms)
		aggStats.lastTTFB = now
		agg.mu.Unlock()
	}
}

const (
	smartAffinityMinSamples = 32
	smartAffinityMargin     = 0.7  // candidate must be ≤70% of current on median AND p90
	smartAffinityRevoke     = 0.85 // affinity revoked once gap shrinks past 15%
	smartAffinityStreak     = 2
)

func (e *smartEngine) evaluateAffinityLocked(t *smartTarget, members smartMemberViews, network string, now time.Time) {
	// Affinity is scoped to threshold-rule winners (spec §2.1 #8); a current
	// set by coldstart/failover/latency is a transient baseline that must not
	// anchor affinity conclusions.
	if t.current == "" || (t.currentWhy != smartReasonPreferred && t.currentWhy != smartReasonAffinity) {
		return
	}
	curStats := t.members[t.current]
	if curStats == nil || curStats.ttfb.Count() < smartAffinityMinSamples {
		return
	}
	curMed, _ := curStats.ttfb.Median()
	curP90, _ := curStats.ttfb.Quantile(0.9)

	if t.affinity != "" && t.affinity == t.current {
		// Revocation check: does any other member now come close?
		bestOtherMed := -1.0
		for tag, stats := range t.members {
			if tag == t.affinity || stats.ttfb.Count() < smartAffinityMinSamples {
				continue
			}
			med, _ := stats.ttfb.Median()
			if bestOtherMed < 0 || med < bestOtherMed {
				bestOtherMed = med
			}
		}
		if bestOtherMed >= 0 && curMed > smartAffinityRevoke*bestOtherMed {
			t.revokeStreak++
			if t.revokeStreak >= smartAffinityStreak {
				e.emit("target %s: origin affinity to %s revoked", t.key, t.affinity)
				t.affinity = ""
				t.revokeStreak = 0
				if winner := e.thresholdWinner(t, members, network, now); winner != "" && winner != t.current {
					e.setCurrentLocked(t, winner, smartReasonPreferred, now)
				}
			}
		} else {
			t.revokeStreak = 0
		}
		return
	}

	var best string
	bestMed := -1.0
	for _, tag := range e.orderNow() {
		if tag == t.current || !e.available(t, members[tag], network, now) {
			continue
		}
		stats := t.members[tag]
		if stats == nil || stats.ttfb.Count() < smartAffinityMinSamples {
			continue
		}
		med, _ := stats.ttfb.Median()
		p90, _ := stats.ttfb.Quantile(0.9)
		if med <= smartAffinityMargin*curMed && p90 <= smartAffinityMargin*curP90 {
			if bestMed < 0 || med < bestMed {
				best, bestMed = tag, med
			}
		}
	}
	if best == "" {
		t.affinityStreak = 0
		t.affinityCand = ""
		return
	}
	if t.affinityCand != best {
		t.affinityCand = best
		t.affinityStreak = 1
		return
	}
	t.affinityStreak++
	if t.affinityStreak >= smartAffinityStreak {
		t.affinity = best
		t.affinityStreak = 0
		t.affinityCand = ""
		e.emit("target %s: origin affinity established to %s (ttfb median %.0fms vs current %.0fms)", t.key, best, bestMed, curMed)
		e.setCurrentLocked(t, best, smartReasonAffinity, now)
	}
}
