package group

import (
	"context"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
)

func testSmartConfig() smartConfig {
	return smartConfig{
		anycastThreshold: 10 * time.Millisecond,
		tolerance:        30 * time.Millisecond,
		improveRatio:     0.25,
		improveMin:       30 * time.Millisecond,
		dwell:            5 * time.Minute,
		confirmations:    2,
		failLimit:        2,
		cooldownBase:     time.Minute,
		cooldownMax:      15 * time.Minute,
		sampleTTL:        15 * time.Minute,
		exploreInterval:  16,
		maxTargets:       1024,
		recordTTL:        14 * 24 * time.Hour,
	}
}

func testSmartEngine(order ...string) (*smartEngine, *[]string) {
	engine := newSmartEngine(testSmartConfig(), order)
	var probes []string
	engine.probeRequest = func(t *smartTarget, tag string, reason string) {
		probes = append(probes, tag+":"+reason)
	}
	return engine, &probes
}

func testSmartViews(tags ...string) smartMemberViews {
	views := make(smartMemberViews)
	for _, tag := range tags {
		views[tag] = &smartMemberView{tag: tag, alive: true, udp: true, baseline: 0, legFactor: 1}
	}
	return views
}

func testSmartTarget(key string) *smartTarget {
	return &smartTarget{key: key, probeHost: key, port: 443, members: make(map[string]*smartTargetStats)}
}

func pushSamples(t *smartTarget, tag string, ms float64, n int, now time.Time) {
	stats := t.stats(tag)
	for i := 0; i < n; i++ {
		stats.window.Push(ms)
	}
	stats.lastSample = now
}

func pushTTFB(t *smartTarget, tag string, ms float64, n int) {
	stats := t.stats(tag)
	for i := 0; i < n; i++ {
		stats.ttfb.Push(ms)
	}
}

func TestSmartWindow(t *testing.T) {
	w := newSmartWindow(4)
	if _, ok := w.Median(); ok {
		t.Fatal("median on empty window")
	}
	for _, v := range []float64{40, 10, 30, 20} {
		w.Push(v)
	}
	if med, _ := w.Median(); med != 20 {
		t.Fatalf("median = %v, want 20", med)
	}
	if minVal, _ := w.Min(); minVal != 10 {
		t.Fatalf("min = %v, want 10", minVal)
	}
	// Ring overwrite: push 4 more, old values gone.
	for _, v := range []float64{100, 100, 100, 100} {
		w.Push(v)
	}
	if med, _ := w.Median(); med != 100 {
		t.Fatalf("median after overwrite = %v, want 100", med)
	}
}

func TestSmartThresholdWalkAnycast(t *testing.T) {
	engine, _ := testSmartEngine("JP", "HK", "US")
	views := testSmartViews("JP", "HK", "US")
	target := testSmartTarget("cf.example")
	now := time.Now()
	// All members fast: preferred (JP) must win even though HK is lower.
	pushSamples(target, "JP", 5, 4, now)
	pushSamples(target, "HK", 3, 4, now)
	pushSamples(target, "US", 8, 4, now)
	decision := engine.decide(target, views, "tcp", now)
	if decision.reason != smartReasonPreferred || decision.attempts[0] != "JP" {
		t.Fatalf("got %s via %v, want preferred via JP", decision.reason, decision.attempts)
	}
}

func TestSmartThresholdWalkSingleQualifier(t *testing.T) {
	engine, _ := testSmartEngine("JP", "HK", "US")
	views := testSmartViews("JP", "HK", "US")
	target := testSmartTarget("us-site.example")
	now := time.Now()
	pushSamples(target, "JP", 150, 4, now)
	pushSamples(target, "HK", 120, 4, now)
	pushSamples(target, "US", 7, 4, now)
	decision := engine.decide(target, views, "tcp", now)
	if decision.attempts[0] != "US" {
		t.Fatalf("got %v, want US first", decision.attempts)
	}
}

func TestSmartLatencyFallback(t *testing.T) {
	engine, _ := testSmartEngine("JP", "HK", "US")
	views := testSmartViews("JP", "HK", "US")
	target := testSmartTarget("regional.example")
	now := time.Now()
	// Nobody clears 10ms; totals JP=50 HK=40 US=200, tolerance 30 → band
	// covers JP+HK; HK has the lower remote.
	pushSamples(target, "JP", 50, 4, now)
	pushSamples(target, "HK", 40, 4, now)
	pushSamples(target, "US", 200, 4, now)
	decision := engine.decide(target, views, "tcp", now)
	if decision.reason != smartReasonLatency || decision.attempts[0] != "HK" {
		t.Fatalf("got %s via %v, want latency via HK", decision.reason, decision.attempts)
	}
}

func TestSmartAntiFlap(t *testing.T) {
	engine, _ := testSmartEngine("JP", "HK")
	views := testSmartViews("JP", "HK")
	target := testSmartTarget("stable.example")
	now := time.Now()
	pushSamples(target, "JP", 35, 8, now)
	pushSamples(target, "HK", 35, 8, now)
	engine.decide(target, views, "tcp", now) // establishes current
	initial := target.current
	// Challenger oscillates 30/40ms for 20 rounds: inside the margin, so no
	// challenger, no switch.
	other := "HK"
	if initial == "HK" {
		other = "JP"
	}
	for i := 0; i < 20; i++ {
		ms := 30.0
		if i%2 == 1 {
			ms = 40.0
		}
		now = now.Add(time.Second)
		engine.onPassiveSample(target, other, ms, views, "tcp", now)
		engine.decide(target, views, "tcp", now)
	}
	if target.switches != 0 || target.current != initial {
		t.Fatalf("switches = %d current = %s, want 0 switches, current %s", target.switches, target.current, initial)
	}
}

func TestSmartImprovementSwitch(t *testing.T) {
	engine, probes := testSmartEngine("JP", "HK")
	views := testSmartViews("JP", "HK")
	target := testSmartTarget("improve.example")
	start := time.Now()
	pushSamples(target, "JP", 100, 8, start)
	engine.decide(target, views, "tcp", start)
	if target.current != "JP" {
		t.Fatalf("setup: current = %s", target.current)
	}
	// Fresh HK data at 40ms: advantage 60ms > max(25ms, 30ms) → challenger.
	engine.onPassiveSample(target, "HK", 40, views, "tcp", start.Add(time.Second))
	if target.challenger != "HK" {
		t.Fatalf("challenger = %q, want HK (probes: %v)", target.challenger, *probes)
	}
	// Within dwell: confirmations accumulate but no switch.
	engine.onProbeResult(target, "HK", 40, nil, views, "tcp", start.Add(2*time.Second))
	engine.onProbeResult(target, "HK", 40, nil, views, "tcp", start.Add(3*time.Second))
	if target.current != "JP" {
		t.Fatal("switched inside dwell period")
	}
	// Refresh JP so its data stays fresh past the dwell boundary.
	afterDwell := start.Add(6 * time.Minute)
	pushSamples(target, "JP", 100, 1, afterDwell)
	engine.onProbeResult(target, "HK", 40, nil, views, "tcp", afterDwell)
	if target.current != "HK" || target.currentWhy != smartReasonImprove {
		t.Fatalf("current = %s (%s), want HK (improve)", target.current, target.currentWhy)
	}
	if target.switches != 1 {
		t.Fatalf("switches = %d, want 1", target.switches)
	}
}

func TestSmartThresholdGuardBlocksImprovement(t *testing.T) {
	engine, _ := testSmartEngine("JP", "HK")
	views := testSmartViews("JP", "HK")
	target := testSmartTarget("anycast-guard.example")
	now := time.Now()
	pushSamples(target, "JP", 5, 8, now)
	engine.decide(target, views, "tcp", now)
	// HK at 1ms is "better" but JP still wins the threshold walk: pinned.
	engine.onPassiveSample(target, "HK", 1, views, "tcp", now.Add(time.Second))
	if target.challenger != "" {
		t.Fatalf("challenger = %q, want none (threshold guard)", target.challenger)
	}
}

func TestSmartFailureCooldownRecovery(t *testing.T) {
	engine, probes := testSmartEngine("JP", "HK")
	views := testSmartViews("JP", "HK")
	target := testSmartTarget("failover.example")
	now := time.Now()
	pushSamples(target, "JP", 20, 8, now)
	pushSamples(target, "HK", 60, 8, now)
	engine.decide(target, views, "tcp", now)
	if target.current != "JP" {
		t.Fatalf("setup: current = %s", target.current)
	}
	// Two consecutive failures → cooldown + demotion to HK.
	engine.onDialFailure(target, "JP", views, "tcp", now)
	engine.onDialFailure(target, "JP", views, "tcp", now.Add(time.Second))
	if target.current != "HK" {
		t.Fatalf("current = %s, want HK after demotion", target.current)
	}
	stats := target.members["JP"]
	if !stats.cooldownUntil.After(now) || !stats.needsRecovery {
		t.Fatal("JP not cooling down")
	}
	// After expiry JP still needs a recovery probe before re-entering.
	afterCooldown := stats.cooldownUntil.Add(time.Second)
	decision := engine.decide(target, views, "tcp", afterCooldown)
	if decision.attempts[0] == "JP" {
		t.Fatal("JP selected before recovery probe")
	}
	found := false
	for _, probe := range *probes {
		if probe == "JP:recovery" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no recovery probe requested: %v", *probes)
	}
	// Recovery probe succeeds → JP eligible again; backoff is retained so a
	// quick relapse escalates the cooldown instead of restarting at the base.
	engine.onProbeResult(target, "JP", 20, nil, views, "tcp", afterCooldown)
	if stats.needsRecovery {
		t.Fatal("JP not recovered after successful probe")
	}
	if stats.backoff != 1 {
		t.Fatalf("backoff = %d, want 1 retained after recovery", stats.backoff)
	}
	// Relapse within smartRelapseWindow escalates.
	relapse := afterCooldown.Add(time.Minute)
	engine.onDialFailure(target, "JP", views, "tcp", relapse)
	engine.onDialFailure(target, "JP", views, "tcp", relapse.Add(time.Second))
	if stats.backoff != 2 {
		t.Fatalf("backoff = %d, want 2 after relapse within window", stats.backoff)
	}
	// Recover again, then relapse long after the window: restart at base.
	recoveredAt := stats.cooldownUntil.Add(time.Second)
	engine.onProbeResult(target, "JP", 20, nil, views, "tcp", recoveredAt)
	late := recoveredAt.Add(smartRelapseWindow + time.Minute)
	engine.onDialFailure(target, "JP", views, "tcp", late)
	engine.onDialFailure(target, "JP", views, "tcp", late.Add(time.Second))
	if stats.backoff != 1 {
		t.Fatalf("backoff = %d, want 1 after relapse beyond window", stats.backoff)
	}
}

func TestSmartTargetDownNoCooldown(t *testing.T) {
	engine, _ := testSmartEngine("JP", "HK")
	views := testSmartViews("JP", "HK")
	target := testSmartTarget("dead.example")
	now := time.Now()
	pushSamples(target, "JP", 20, 4, now)
	pushSamples(target, "HK", 30, 4, now)
	engine.decide(target, views, "tcp", now)
	// Both members fail within the window: the target is down, nobody cools.
	engine.onDialFailure(target, "HK", views, "tcp", now)
	engine.onDialFailure(target, "JP", views, "tcp", now.Add(time.Second))
	engine.onDialFailure(target, "JP", views, "tcp", now.Add(2*time.Second))
	if target.members["JP"].cooldownUntil.After(now) {
		t.Fatal("JP cooled down despite target being down")
	}
}

func TestSmartAffinity(t *testing.T) {
	engine, _ := testSmartEngine("JP", "HK")
	views := testSmartViews("JP", "HK")
	target := testSmartTarget("origin-affine.example")
	now := time.Now()
	pushSamples(target, "JP", 5, 8, now)
	pushSamples(target, "HK", 6, 8, now)
	engine.decide(target, views, "tcp", now)
	if target.current != "JP" || target.currentWhy != smartReasonPreferred {
		t.Fatalf("setup: current = %s (%s)", target.current, target.currentWhy)
	}
	// HK's origin path is dramatically faster: median 100 vs 300.
	pushTTFB(target, "JP", 300, 40)
	pushTTFB(target, "HK", 100, 40)
	// Drive evaluations (every 16 ttfb samples).
	for i := 0; i < 64; i++ {
		tag := "JP"
		ms := 300.0
		if i%2 == 0 {
			tag = "HK"
			ms = 100.0
		}
		engine.onTTFBSample(target, tag, ms, views, "tcp", now)
	}
	if target.affinity != "HK" || target.current != "HK" {
		t.Fatalf("affinity = %q current = %s, want HK/HK", target.affinity, target.current)
	}
	// Gap dissolves: HK degrades to ~JP levels → revocation.
	for i := 0; i < 128; i++ {
		engine.onTTFBSample(target, "HK", 295, views, "tcp", now)
	}
	if target.affinity != "" {
		t.Fatalf("affinity = %q, want revoked", target.affinity)
	}
}

func TestSmartExplore(t *testing.T) {
	engine, _ := testSmartEngine("JP", "HK")
	engine.cfg.exploreInterval = 4
	views := testSmartViews("JP", "HK")
	target := testSmartTarget("explore.example")
	now := time.Now()
	pushSamples(target, "JP", 5, 8, now)
	pushSamples(target, "HK", 6, 8, now)
	engine.decide(target, views, "tcp", now)
	sawExplore := false
	for i := 0; i < 4; i++ {
		decision := engine.decide(target, views, "tcp", now)
		if decision.reason == smartReasonExplore {
			sawExplore = true
			if decision.attempts[0] != "HK" {
				t.Fatalf("explore went to %s, want HK", decision.attempts[0])
			}
		}
	}
	if !sawExplore {
		t.Fatal("no explore decision within interval")
	}
}

func TestSmartColdStart(t *testing.T) {
	engine, _ := testSmartEngine("JP", "HK", "US")
	views := testSmartViews("JP", "HK", "US")
	target := testSmartTarget("new.example")
	decision := engine.decide(target, views, "tcp", time.Now())
	if !decision.coldStart || len(decision.attempts) == 0 {
		t.Fatalf("decision = %+v, want cold start with candidates", decision)
	}
	if decision.attempts[0] != "JP" {
		t.Fatalf("first candidate = %s, want preferred JP", decision.attempts[0])
	}
}

func TestSmartKeys(t *testing.T) {
	metadata := &adapter.InboundContext{
		Destination: M.ParseSocksaddrHostPort("www.apple.com", 443),
	}
	exact, agg, host, ok := smartKeys(metadata)
	if !ok || exact != "www.apple.com" || agg != "apple.com" || host != "www.apple.com" {
		t.Fatalf("domain keys = %q/%q/%q ok=%v", exact, agg, host, ok)
	}
	metadata = &adapter.InboundContext{
		Destination: M.SocksaddrFrom(netip.MustParseAddr("1.2.3.4"), 443),
	}
	exact, agg, _, ok = smartKeys(metadata)
	if !ok || exact != "1.2.3.4" || agg != "1.2.3.0/24" {
		t.Fatalf("ip keys = %q/%q ok=%v", exact, agg, ok)
	}
	metadata = &adapter.InboundContext{
		Destination: M.SocksaddrFrom(netip.MustParseAddr("198.18.0.5"), 443),
		FakeIP:      true,
	}
	if _, _, _, ok = smartKeys(metadata); ok {
		t.Fatal("bare fake IP must be excluded")
	}
}

func TestSmartTableInheritance(t *testing.T) {
	table := newSmartTable(16, 0)
	now := time.Now()
	first := table.ensure("www.apple.com", "apple.com", "www.apple.com", 443, now)
	first.mu.Lock()
	first.current = "JP"
	first.mu.Unlock()
	if agg := table.get("apple.com"); agg == nil {
		t.Fatal("aggregate entry not created")
	} else {
		agg.mu.Lock()
		agg.current = "JP"
		agg.mu.Unlock()
	}
	second := table.ensure("store.apple.com", "apple.com", "store.apple.com", 443, now)
	if second.current != "JP" {
		t.Fatalf("new subdomain current = %q, want inherited JP", second.current)
	}
}

func TestSmartTableLRU(t *testing.T) {
	table := newSmartTable(2, 0)
	now := time.Now()
	table.ensure("a.example", "a.example", "a.example", 443, now)
	table.ensure("b.example", "b.example", "b.example", 443, now)
	table.ensure("c.example", "c.example", "c.example", 443, now)
	if table.get("a.example") != nil {
		t.Fatal("oldest entry not evicted")
	}
	if table.len() != 2 {
		t.Fatalf("table len = %d, want 2", table.len())
	}
}

func TestSmartPersistRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "smart-cache.json")
	now := time.Now()

	table := newSmartTable(64, 0)
	target := table.ensure("www.apple.com", "apple.com", "www.apple.com", 443, now)
	target.mu.Lock()
	target.current = "JP"
	target.currentWhy = smartReasonPreferred
	target.currentAt = now
	target.affinity = "HK"
	target.mu.Unlock()
	pushSamples(target, "JP", 42, 10, now)
	pushTTFB(target, "JP", 150, 20)
	// Entry from a member that no longer exists.
	pushSamples(target, "GONE", 10, 2, now)
	// Expired entry.
	expired := table.ensure("old.example", "old.example", "old.example", 443, now.Add(-30*24*time.Hour))
	expired.mu.Lock()
	expired.lastUsed = now.Add(-30 * 24 * time.Hour)
	expired.mu.Unlock()

	member := &smartMember{tag: "JP", baseline: newSmartWindow(smartWindowCap), legFactor: 2}
	member.baseline.Push(50)
	member.baseline.Push(48)

	snapshot := smartSnapshotFromTable(table, []*smartMember{member})
	if err := smartSaveSnapshot(path, snapshot); err != nil {
		t.Fatal(err)
	}

	loaded, err := smartLoadSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	fresh := newSmartTable(64, 0)
	loaded.apply(fresh, map[string]bool{"JP": true, "HK": true}, 14*24*time.Hour, now)

	restored := fresh.get("www.apple.com")
	if restored == nil {
		t.Fatal("target not restored")
	}
	if restored.current != "JP" || restored.affinity != "HK" {
		t.Fatalf("restored current=%q affinity=%q", restored.current, restored.affinity)
	}
	if restored.aggregate == nil || restored.aggregate.key != "apple.com" {
		t.Fatal("aggregate link not restored")
	}
	if _, hasGone := restored.members["GONE"]; hasGone {
		t.Fatal("unknown member tag survived restore")
	}
	if stats := restored.members["JP"]; stats == nil || stats.window.Count() == 0 {
		t.Fatal("samples not restored")
	} else if stats.window.Count() > smartSnapshotSampleCap {
		t.Fatalf("restored %d samples, cap is %d", stats.window.Count(), smartSnapshotSampleCap)
	}
	if fresh.get("old.example") != nil {
		t.Fatal("expired entry survived restore")
	}

	// Corrupt file → clean error.
	if err := os.WriteFile(path, []byte("{broken"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := smartLoadSnapshot(path); err == nil {
		t.Fatal("corrupt snapshot loaded without error")
	}
}

// --- measurement wrapper tests ---

func tlsRec(recordType byte, payloadLen int) []byte {
	record := make([]byte, 5+payloadLen)
	record[0] = recordType
	record[1] = 3
	record[2] = 3
	record[3] = byte(payloadLen >> 8)
	record[4] = byte(payloadLen)
	return record
}

type measureResult struct {
	totals chan float64
	ttfbs  chan float64
	fails  chan error
}

func newMeasureResult() *measureResult {
	return &measureResult{
		totals: make(chan float64, 4),
		ttfbs:  make(chan float64, 4),
		fails:  make(chan error, 4),
	}
}

func (r *measureResult) callbacks() smartConnCallbacks {
	return smartConnCallbacks{
		onTotal:     func(ms float64) { r.totals <- ms },
		onTTFB:      func(ms float64) { r.ttfbs <- ms },
		onEarlyFail: func(err error) { r.fails <- err },
	}
}

func mustReadN(t *testing.T, conn net.Conn, n int) {
	t.Helper()
	buffer := make([]byte, n)
	total := 0
	for total < n {
		read, err := conn.Read(buffer[total:])
		if err != nil {
			t.Fatalf("peer read: %v", err)
		}
		total += read
	}
}

func expectSample(t *testing.T, ch chan float64, what string) float64 {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatalf("no %s sample", what)
		return 0
	}
}

func expectNoSample(t *testing.T, ch chan float64, what string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("unexpected %s sample: %v", what, v)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSmartMeasureConnTLS12(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	result := newMeasureResult()
	measured := newSmartMeasureConn(client, result.callbacks())

	done := make(chan struct{})
	go func() {
		defer close(done)
		mustReadN(t, server, 105) // ClientHello
		server.Write(tlsRec(22, 80))
		mustReadN(t, server, 6+45) // CCS + Finished(22)
		server.Write(append(tlsRec(20, 1), tlsRec(22, 40)...))
		mustReadN(t, server, 55) // application data
		server.Write(tlsRec(23, 60))
	}()

	measured.Write(tlsRec(22, 100))
	buffer := make([]byte, 256)
	measured.Read(buffer) // ServerHello
	expectSample(t, result.totals, "total")
	measured.Write(tlsRec(20, 1))
	measured.Write(tlsRec(22, 40))
	measured.Read(buffer) // server CCS+Finished
	measured.Write(tlsRec(23, 50))
	measured.Read(buffer) // server application data
	if ttfb := expectSample(t, result.ttfbs, "ttfb"); ttfb < 0 {
		t.Fatalf("ttfb = %v", ttfb)
	}
	if !measured.ReaderReplaceable() {
		t.Fatal("wrapper not passthrough after sampling")
	}
	<-done
}

func TestSmartMeasureConnTLS13(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	result := newMeasureResult()
	measured := newSmartMeasureConn(client, result.callbacks())

	done := make(chan struct{})
	go func() {
		defer close(done)
		mustReadN(t, server, 105) // ClientHello
		// ServerHello + CCS + encrypted handshake disguised as 23.
		server.Write(append(append(tlsRec(22, 80), tlsRec(20, 1)...), tlsRec(23, 120)...))
		mustReadN(t, server, 6+45+55) // client CCS + Finished(23) + app(23)
		server.Write(tlsRec(23, 60))
	}()

	measured.Write(tlsRec(22, 100))
	buffer := make([]byte, 512)
	measured.Read(buffer)
	expectSample(t, result.totals, "total")
	measured.Write(tlsRec(20, 1))  // client CCS
	measured.Write(tlsRec(23, 40)) // Finished (disguised) — must NOT set appRequestAt
	measured.Write(tlsRec(23, 50)) // first true app data → appRequestAt
	measured.Read(buffer)          // server app data → t3
	expectSample(t, result.ttfbs, "ttfb")
	<-done
}

func TestSmartMeasureConnPlain(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	result := newMeasureResult()
	measured := newSmartMeasureConn(client, result.callbacks())

	go func() {
		mustReadN(t, server, 5)
		server.Write([]byte("world"))
	}()
	measured.Write([]byte("hello"))
	buffer := make([]byte, 16)
	measured.Read(buffer)
	expectSample(t, result.totals, "total")
	expectNoSample(t, result.ttfbs, "ttfb")
	if !measured.ReaderReplaceable() {
		t.Fatal("plain conn should be passthrough after first read")
	}
}

func TestSmartMeasureConnZeroRTT(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	result := newMeasureResult()
	measured := newSmartMeasureConn(client, result.callbacks())

	done := make(chan struct{})
	go func() {
		defer close(done)
		mustReadN(t, server, 105+55) // ClientHello + early data
		server.Write(tlsRec(22, 80))
		server.Write(tlsRec(23, 60))
	}()
	measured.Write(tlsRec(22, 100))
	measured.Write(tlsRec(23, 50)) // early data before any server byte
	buffer := make([]byte, 256)
	measured.Read(buffer)
	expectSample(t, result.totals, "total")
	measured.Read(buffer)
	expectNoSample(t, result.ttfbs, "ttfb")
	<-done
}

func TestSmartMeasureConnEarlyFail(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	result := newMeasureResult()
	measured := newSmartMeasureConn(client, result.callbacks())
	go func() {
		mustReadN(t, server, 105)
		server.Close() // zero-byte close before any response
	}()
	measured.Write(tlsRec(22, 100))
	buffer := make([]byte, 16)
	if _, err := measured.Read(buffer); err == nil {
		t.Fatal("read should fail")
	}
	select {
	case <-result.fails:
	case <-time.After(2 * time.Second):
		t.Fatal("early failure not reported")
	}
}

// --- race tests ---

func raceDialer(t *testing.T, delay time.Duration, response string, requestLen int) func() (net.Conn, error) {
	t.Helper()
	return func() (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			buffer := make([]byte, requestLen)
			total := 0
			for total < requestLen {
				n, err := server.Read(buffer[total:])
				if err != nil {
					return
				}
				total += n
			}
			time.Sleep(delay)
			server.Write([]byte(response))
		}()
		return client, nil
	}
}

func TestSmartRaceWinner(t *testing.T) {
	slowFlags := &smartRaceFlags{}
	fastFlags := &smartRaceFlags{}
	winnerCh := make(chan string, 1)
	race := newSmartRaceConn([]*smartRaceCandidate{
		{tag: "SLOW", flags: slowFlags, dial: raceDialer(t, 200*time.Millisecond, "slow-response", 5)},
		{tag: "FAST", flags: fastFlags, dial: raceDialer(t, 10*time.Millisecond, "fast-response", 5)},
	}, func(tag string) { winnerCh <- tag })
	defer race.Close()

	if _, err := race.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 64)
	n, err := race.Read(buffer)
	if err != nil {
		t.Fatal(err)
	}
	if string(buffer[:n]) != "fast-response" {
		t.Fatalf("read %q, want fast-response", buffer[:n])
	}
	select {
	case winner := <-winnerCh:
		if winner != "FAST" {
			t.Fatalf("winner = %s", winner)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no winner callback")
	}
	deadline := time.Now().Add(2 * time.Second)
	for !slowFlags.abandoned.Load() {
		if time.Now().After(deadline) {
			t.Fatal("loser not marked abandoned")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if fastFlags.abandoned.Load() {
		t.Fatal("winner marked abandoned")
	}
}

func TestSmartRaceAllFail(t *testing.T) {
	fail := func() (net.Conn, error) {
		return nil, os.ErrDeadlineExceeded
	}
	race := newSmartRaceConn([]*smartRaceCandidate{
		{tag: "A", flags: &smartRaceFlags{}, dial: fail},
		{tag: "B", flags: &smartRaceFlags{}, dial: fail},
	}, nil)
	defer race.Close()
	buffer := make([]byte, 8)
	if _, err := race.Read(buffer); err == nil {
		t.Fatal("read should fail when all candidates fail")
	}
}

func TestSmartUDPDoesNotRewriteSticky(t *testing.T) {
	engine, _ := testSmartEngine("JP", "HK")
	views := testSmartViews("JP", "HK")
	views["JP"].udp = false // sticky member lacks UDP support
	target := testSmartTarget("udp-defer.example")
	now := time.Now()
	pushSamples(target, "JP", 20, 8, now)
	pushSamples(target, "HK", 30, 8, now)
	engine.decide(target, views, "tcp", now)
	if target.current != "JP" {
		t.Fatalf("setup: current = %s", target.current)
	}
	decision := engine.decide(target, views, "udp", now)
	if len(decision.attempts) == 0 || decision.attempts[0] != "HK" {
		t.Fatalf("udp attempts = %v, want HK first", decision.attempts)
	}
	if target.current != "JP" {
		t.Fatalf("current = %s, want JP untouched by the UDP flow", target.current)
	}
}

func TestSmartTargetDownRateLimit(t *testing.T) {
	engine, _ := testSmartEngine("JP", "HK")
	views := testSmartViews("JP", "HK")
	target := testSmartTarget("down-limit.example")
	now := time.Now()
	pushSamples(target, "JP", 20, 4, now)
	pushSamples(target, "HK", 30, 4, now)
	engine.decide(target, views, "tcp", now)
	engine.onDialFailure(target, "HK", views, "tcp", now)
	engine.onDialFailure(target, "JP", views, "tcp", now.Add(time.Second))
	engine.onDialFailure(target, "JP", views, "tcp", now.Add(2*time.Second))
	// Target declared down: decisions within the retry interval fail fast.
	decision := engine.decide(target, views, "tcp", now.Add(3*time.Second))
	if decision.reason != smartReasonTargetDown || len(decision.attempts) != 0 {
		t.Fatalf("decision = %+v, want target-down fail-fast", decision)
	}
	// After the interval a retry chain is allowed again.
	retryAt := now.Add(2 * time.Second).Add(smartTargetDownRetryInterval + time.Second)
	decision = engine.decide(target, views, "tcp", retryAt)
	if len(decision.attempts) == 0 {
		t.Fatal("no retry allowed after the rate-limit interval")
	}
}

func TestSmartPreferredChallengeConvergence(t *testing.T) {
	// Race winner HK holds an anycast target; JP qualifies the threshold
	// walk, so it must challenge and take over after confirm + dwell even
	// though it is not faster (spec S3).
	cfg := testSmartConfig()
	cfg.dwell = time.Minute
	engine := newSmartEngine(cfg, []string{"JP", "HK"})
	engine.probeRequest = func(*smartTarget, string, string) {}
	views := testSmartViews("JP", "HK")
	target := testSmartTarget("anycast-converge.example")
	now := time.Now()
	engine.onDialSuccess(target, "HK", now) // race winner
	if target.current != "HK" {
		t.Fatalf("setup: current = %s", target.current)
	}
	pushSamples(target, "HK", 5, 8, now)
	pushSamples(target, "JP", 6, 8, now) // within threshold, slightly slower
	// A passive sample on the current member spots the walk mismatch.
	engine.onPassiveSample(target, "HK", 5, views, "tcp", now.Add(time.Second))
	if target.challenger != "JP" || target.challengerWhy != smartReasonPreferred {
		t.Fatalf("challenger = %q/%q, want JP/preferred", target.challenger, target.challengerWhy)
	}
	// Confirmations complete before dwell: no switch yet.
	engine.onProbeResult(target, "JP", 6, nil, views, "tcp", now.Add(2*time.Second))
	engine.onProbeResult(target, "JP", 6, nil, views, "tcp", now.Add(3*time.Second))
	if target.current != "HK" {
		t.Fatalf("switched before dwell: current = %s", target.current)
	}
	// After dwell the next confirmation completes the switch.
	engine.onProbeResult(target, "JP", 6, nil, views, "tcp", now.Add(cfg.dwell+time.Second))
	if target.current != "JP" || target.currentWhy != smartReasonPreferred {
		t.Fatalf("current = %s/%s, want JP/preferred", target.current, target.currentWhy)
	}
	if target.switches != 1 {
		t.Fatalf("switches = %d, want 1", target.switches)
	}
}

func TestSmartProbeAliveNoSample(t *testing.T) {
	engine, _ := testSmartEngine("JP", "HK")
	views := testSmartViews("JP", "HK")
	target := testSmartTarget("alive-probe.example")
	engine.onProbeResult(target, "JP", 0, nil, views, "tcp", time.Now())
	if target.members["JP"].window.Count() != 0 {
		t.Fatal("alive-only probe recorded a fabricated 0ms sample")
	}
}

func TestSmartRaceSingleCandidateDoubleFailure(t *testing.T) {
	// One candidate errors on both its write pump and its head read; the
	// double error must not trip "all failed" while the healthy candidate is
	// still racing.
	badDial := func() (net.Conn, error) {
		client, server := net.Pipe()
		server.Close()
		return client, nil
	}
	race := newSmartRaceConn([]*smartRaceCandidate{
		{tag: "BAD", flags: &smartRaceFlags{}, dial: badDial},
		{tag: "GOOD", flags: &smartRaceFlags{}, dial: raceDialer(t, 300*time.Millisecond, "late-response", 5)},
	}, nil)
	defer race.Close()
	if _, err := race.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 64)
	n, err := race.Read(buffer)
	if err != nil {
		t.Fatalf("read failed (%v), want the healthy candidate to win", err)
	}
	if string(buffer[:n]) != "late-response" {
		t.Fatalf("read %q, want late-response", buffer[:n])
	}
}

func TestSmartRaceWinnerWriteErrorUnblocks(t *testing.T) {
	// After a winner is decided, its conn dying mid-replay must wake blocked
	// writers with an error instead of hanging them until Close.
	client, server := net.Pipe()
	consumed := make(chan struct{})
	respond := make(chan struct{})
	race := newSmartRaceConn([]*smartRaceCandidate{
		{tag: "ONLY", flags: &smartRaceFlags{}, dial: func() (net.Conn, error) { return client, nil }},
	}, nil)
	defer race.Close()
	go func() {
		buffer := make([]byte, 5)
		io.ReadFull(server, buffer)
		close(consumed)
		<-respond
		server.Write([]byte("resp"))
		time.Sleep(100 * time.Millisecond) // leave "world" stuck in the pump
		server.Close()
	}()
	if _, err := race.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	<-consumed
	// Buffered pre-winner: the pump will block replaying it.
	if _, err := race.Write([]byte("world")); err != nil {
		t.Fatal(err)
	}
	close(respond)
	buffer := make([]byte, 16)
	if _, err := race.Read(buffer); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() {
		_, err := race.Write([]byte("!!!"))
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("write succeeded after the winner conn died mid-replay")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write hung after winner replay failure")
	}
}

func TestSmartTableEnsureConcurrent(t *testing.T) {
	table := newSmartTable(64, 0)
	now := time.Now()
	var group sync.WaitGroup
	results := make([]*smartTarget, 8)
	for i := range results {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			results[i] = table.ensure("www.example.com", "example.com", "www.example.com", 443, now)
		}(i)
	}
	group.Wait()
	for _, target := range results[1:] {
		if target != results[0] {
			t.Fatal("concurrent ensure produced distinct entries for one key")
		}
	}
	if agg := results[0].aggregate; agg == nil || agg != table.get("example.com") {
		t.Fatal("aggregate entry not shared")
	}
}

func TestSmartTableInMemoryTTL(t *testing.T) {
	table := newSmartTable(64, time.Hour)
	now := time.Now()
	target := table.ensure("ttl.example", "", "ttl.example", 443, now)
	target.mu.Lock()
	target.current = "JP"
	target.stats("JP").window.Push(10)
	target.mu.Unlock()
	same := table.ensure("ttl.example", "", "ttl.example", 443, now.Add(30*time.Minute))
	if same != target || same.current != "JP" {
		t.Fatal("entry expired before its TTL")
	}
	later := table.ensure("ttl.example", "", "ttl.example", 443, now.Add(2*time.Hour))
	if later.current != "" || len(later.members) != 0 {
		t.Fatal("stale entry not reset past the TTL")
	}
}

func TestSmartEarlyFailureAccrualAcrossDials(t *testing.T) {
	engine, _ := testSmartEngine("JP", "HK")
	views := testSmartViews("JP", "HK")
	target := testSmartTarget("session-proto.example")
	now := time.Now()
	pushSamples(target, "JP", 20, 8, now)
	pushSamples(target, "HK", 30, 8, now)
	engine.decide(target, views, "tcp", now)
	if target.current != "JP" {
		t.Fatalf("setup: current = %s", target.current)
	}
	// Session-based protocols "succeed" at every dial; the real failures
	// surface afterwards as early failures and must accrue to the limit.
	engine.onDialSuccess(target, "JP", now)
	engine.onDialFailure(target, "JP", views, "tcp", now.Add(time.Second))
	engine.onDialSuccess(target, "JP", now.Add(2*time.Second))
	engine.onDialFailure(target, "JP", views, "tcp", now.Add(3*time.Second))
	stats := target.members["JP"]
	if !stats.cooldownUntil.After(now.Add(3 * time.Second)) {
		t.Fatal("JP not cooled down: dial successes reset the failure streak")
	}
	if target.current != "HK" {
		t.Fatalf("current = %s, want HK after demotion", target.current)
	}
	// Straggler failures from pre-cooldown connections must not escalate.
	engine.onDialFailure(target, "JP", views, "tcp", now.Add(4*time.Second))
	engine.onDialFailure(target, "JP", views, "tcp", now.Add(5*time.Second))
	if stats.backoff != 1 {
		t.Fatalf("backoff = %d, want 1 (stragglers must not escalate)", stats.backoff)
	}
	// A completed passive sample is what clears the streak.
	engine.onDialFailure(target, "HK", views, "tcp", now.Add(6*time.Second))
	engine.onPassiveSample(target, "HK", 30, views, "tcp", now.Add(7*time.Second))
	engine.onDialFailure(target, "HK", views, "tcp", now.Add(8*time.Second))
	if target.members["HK"].cooldownUntil.After(now.Add(8 * time.Second)) {
		t.Fatal("HK cooled down although a sample reset the streak")
	}
}

func TestSmartAffinityPinsAgainstPreferredChallenge(t *testing.T) {
	// Once origin affinity holds a target, the preferred walk must not
	// challenge it back — the two channels previously oscillated forever
	// (observed live in the 30-minute soak).
	engine, _ := testSmartEngine("JP", "HK")
	views := testSmartViews("JP", "HK")
	target := testSmartTarget("affinity-pin.example")
	now := time.Now()
	pushSamples(target, "JP", 5, 8, now)
	pushSamples(target, "HK", 6, 8, now)
	engine.decide(target, views, "tcp", now)
	if target.current != "JP" || target.currentWhy != smartReasonPreferred {
		t.Fatalf("setup: current = %s/%s", target.current, target.currentWhy)
	}
	// HK's origin is much closer: affinity establishes and takes the target.
	pushTTFB(target, "JP", 100, smartAffinityMinSamples)
	pushTTFB(target, "HK", 40, smartAffinityMinSamples)
	for i := 0; i < 2*16; i++ {
		engine.onTTFBSample(target, "HK", 40, views, "tcp", now.Add(time.Second))
	}
	if target.current != "HK" || target.affinity != "HK" {
		t.Fatalf("setup: affinity not established (current=%s affinity=%s)", target.current, target.affinity)
	}
	// Passive samples on the affinity holder must not raise a preferred
	// challenger even though JP still wins the threshold walk.
	for i := 0; i < 5; i++ {
		engine.onPassiveSample(target, "HK", 6, views, "tcp", now.Add(time.Duration(2+i)*time.Second))
	}
	if target.challenger != "" {
		t.Fatalf("challenger = %q, want none while affinity holds", target.challenger)
	}
	if target.current != "HK" {
		t.Fatalf("current = %s, want HK pinned by affinity", target.current)
	}
}

func TestSmartSparseOutlierResample(t *testing.T) {
	// A member whose only sample was measured through a cold-start probe
	// burst must get a verification probe instead of 15 minutes of exclusion.
	engine, probes := testSmartEngine("JP", "SG")
	views := testSmartViews("JP", "SG")
	target := testSmartTarget("resample.example")
	now := time.Now()
	pushSamples(target, "JP", 90, 8, now)
	pushSamples(target, "SG", 3200, 1, now) // lone poisoned sample
	engine.decide(target, views, "tcp", now)
	engine.onPassiveSample(target, "JP", 90, views, "tcp", now.Add(time.Second))
	found := false
	for _, probe := range *probes {
		if probe == "SG:resample" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no resample probe for outlier member: %v", *probes)
	}
	// A mildly inflated sparse sample (>1.5× current) is verified too: this
	// is the case that silently pinned a preferred member above the anycast
	// threshold in integration.
	mild := testSmartTarget("resample-mild.example")
	pushSamples(mild, "JP", 50, 8, now)
	pushSamples(mild, "SG", 112, 1, now)
	engine.decide(mild, views, "tcp", now)
	*probes = nil
	engine.onPassiveSample(mild, "JP", 50, views, "tcp", now.Add(time.Second))
	found = false
	for _, probe := range *probes {
		if probe == "SG:resample" {
			found = true
		}
	}
	if !found {
		t.Fatalf("mildly inflated sparse sample not verified: %v", *probes)
	}
	// A healthy sparse member must not be re-probed.
	*probes = nil
	pushSamples(target, "SG", 95, 1, now.Add(2*time.Second))
	engine.onPassiveSample(target, "JP", 90, views, "tcp", now.Add(3*time.Second))
	for _, probe := range *probes {
		if probe == "SG:resample" {
			t.Fatalf("healthy member re-probed: %v", *probes)
		}
	}
}

func TestSmartIdleWarmupNoSpikeDemotion(t *testing.T) {
	// Multiplexed protocols rebuild transport sessions after idling; the
	// first sample after an idle gap reads as a spike but must not count
	// toward demotion. Sustained spikes afterwards still demote.
	engine, _ := testSmartEngine("JP", "HK")
	views := testSmartViews("JP", "HK")
	target := testSmartTarget("idle-mux.example")
	now := time.Now()
	pushSamples(target, "JP", 90, 8, now)
	pushSamples(target, "HK", 120, 8, now)
	engine.decide(target, views, "tcp", now)
	if target.current != "JP" {
		t.Fatalf("setup: current = %s", target.current)
	}
	later := now.Add(5 * time.Minute)                                               // past switch grace, past idle gap
	engine.onPassiveSample(target, "JP", 400, views, "tcp", later)                  // warm-up: no streak
	engine.onPassiveSample(target, "JP", 400, views, "tcp", later.Add(time.Second)) // streak 1
	stats := target.members["JP"]
	if stats.cooldownUntil.After(later) {
		t.Fatal("idle warm-up spike counted toward demotion")
	}
	if target.current != "JP" {
		t.Fatalf("current = %s, want JP still", target.current)
	}
	// A warm-up sample interleaved into a genuine streak must not erase the
	// accumulated evidence: streak stays at 1 through the idle gap...
	engine.onPassiveSample(target, "JP", 400, views, "tcp", later.Add(2*time.Minute))
	if stats.spikeStreak != 1 {
		t.Fatalf("spikeStreak = %d, want 1 preserved across warm-up", stats.spikeStreak)
	}
	// ...and the next hot spike completes the demotion.
	engine.onPassiveSample(target, "JP", 400, views, "tcp", later.Add(2*time.Minute+time.Second))
	if !stats.cooldownUntil.After(later) {
		t.Fatal("sustained spikes did not demote")
	}
	if target.current != "HK" {
		t.Fatalf("current = %s, want HK after demotion", target.current)
	}
}

func TestSmartLegFactor(t *testing.T) {
	// HandshakeContext is protocol truth: early-data regardless of timing.
	if got, definitive := legFactorFor(smartProbeResult{totalMs: 100, dialMs: 90, earlyData: true}); got != 2 || !definitive {
		t.Fatalf("earlyData conn = (%v, %v), want (2, definitive)", got, definitive)
	}
	// Timing decides only at the extremes: near-zero dial → early-data,
	// dial-dominated → blocking.
	if got, definitive := legFactorFor(smartProbeResult{totalMs: 100, dialMs: 0.5, timingTrusted: true}); got != 2 || !definitive {
		t.Fatalf("tiny dial = (%v, %v), want (2, definitive)", got, definitive)
	}
	if got, definitive := legFactorFor(smartProbeResult{totalMs: 100, dialMs: 95, timingTrusted: true}); got != 1 || !definitive {
		t.Fatalf("dial-dominated = (%v, %v), want (1, definitive)", got, definitive)
	}
	// The middle band (anchor server processing inflating the total) must
	// not reclassify: observed live as dial≈35ms/total≈70ms on 1.1.1.1:80.
	if _, definitive := legFactorFor(smartProbeResult{totalMs: 70, dialMs: 35, timingTrusted: true}); definitive {
		t.Fatal("ambiguous ratio classified as definitive")
	}
	if _, definitive := legFactorFor(smartProbeResult{}); definitive {
		t.Fatal("empty probe result classified as definitive")
	}
}

func TestSmartSWRRefresh(t *testing.T) {
	// A sticky decision whose samples went stale keeps serving and triggers
	// a background refresh probe (stale-while-revalidate).
	engine, probes := testSmartEngine("JP", "HK")
	views := testSmartViews("JP", "HK")
	target := testSmartTarget("swr.example")
	now := time.Now()
	pushSamples(target, "JP", 20, 8, now)
	pushSamples(target, "HK", 60, 8, now)
	engine.decide(target, views, "tcp", now)
	if target.current != "JP" {
		t.Fatalf("setup: current = %s", target.current)
	}
	stale := now.Add(16 * time.Minute) // past the 15m sample TTL
	decision := engine.decide(target, views, "tcp", stale)
	if decision.reason != smartReasonSticky || decision.attempts[0] != "JP" {
		t.Fatalf("stale decision = %s via %v, want sticky via JP", decision.reason, decision.attempts)
	}
	found := false
	for _, probe := range *probes {
		if probe == "JP:refresh" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no refresh probe on stale sticky decision: %v", *probes)
	}
}

func TestSmartProberSingleflight(t *testing.T) {
	// Concurrent schedules of the same key run at most once (inflight dedup +
	// the per-key minimum interval).
	prober := newSmartProber(context.Background(), 4, time.Second)
	var runs atomic.Int32
	for i := 0; i < 5; i++ {
		prober.schedule("dup|key", func() { runs.Add(1) })
	}
	deadline := time.Now().Add(2 * time.Second)
	for runs.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(600 * time.Millisecond) // let any duplicate (jitter ≤500ms) fire
	if got := runs.Load(); got != 1 {
		t.Fatalf("probe ran %d times, want exactly 1", got)
	}
}

func TestSmartMeasureConnHangEarlyFail(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	failures := make(chan error, 1)
	measured := newSmartMeasureConn(client, smartConnCallbacks{
		onEarlyFail: func(err error) { failures <- err },
	})
	defer measured.Close()
	go func() {
		buffer := make([]byte, 8)
		server.Read(buffer) // consume the request, never respond
	}()
	if _, err := measured.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-failures:
	case <-time.After(smartEarlyFailWindow + 2*time.Second):
		t.Fatal("zero-byte hang not reported as early failure")
	}
}

func TestSmartMeasureConnLocalAbort(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			defer conn.Close()
			buffer := make([]byte, 8)
			conn.Read(buffer)
			time.Sleep(500 * time.Millisecond)
		}
	}()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	failures := make(chan error, 1)
	measured := newSmartMeasureConn(conn, smartConnCallbacks{
		onEarlyFail: func(err error) { failures <- err },
	})
	if _, err := measured.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		measured.Close() // local abort while the read is blocked
	}()
	buffer := make([]byte, 8)
	if _, err := measured.Read(buffer); err == nil {
		t.Fatal("read should fail after local close")
	}
	select {
	case err := <-failures:
		t.Fatalf("local abort reported as early failure: %v", err)
	default:
	}
}

// fakeDialOutbound is a minimal adapter.Outbound whose dials are scripted by
// the test.
type fakeDialOutbound struct {
	outbound.Adapter
	dial func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error)
}

func (f *fakeDialOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return f.dial(ctx, network, destination)
}

func (f *fakeDialOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, os.ErrInvalid
}

// testSmartGroup hand-assembles a Smart instance with fake members, bypassing
// Start()/the outbound manager, for orchestration-level tests (dialHedged).
func testSmartGroup(tags ...string) *Smart {
	cfg := testSmartConfig()
	s := &Smart{
		ctx:    context.Background(),
		logger: log.NewNOPFactory().Logger(),
		cfg:    cfg,
		table:  newSmartTable(cfg.maxTargets, cfg.recordTTL),
		prober: newSmartProber(context.Background(), 8, time.Second),
	}
	s.engine = newSmartEngine(cfg, tags)
	members := make([]*smartMember, 0, len(tags))
	byTag := make(map[string]*smartMember, len(tags))
	for _, tag := range tags {
		member := &smartMember{
			tag:       tag,
			interrupt: interrupt.NewGroup(),
			udp:       true,
			baseline:  newSmartWindow(smartWindowCap),
			legFactor: 1,
			alive:     true,
		}
		members = append(members, member)
		byTag[tag] = member
	}
	s.memberSet.Store(&smartMemberSet{members: members, byTag: byTag, tags: tags, order: tags})
	return s
}

func (s *Smart) setTestDetour(tag string, dial func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error)) {
	member := s.memberSet.Load().byTag[tag]
	member.detour = &fakeDialOutbound{
		Adapter: outbound.NewAdapter("test", tag, []string{"tcp", "udp"}, nil),
		dial:    dial,
	}
}

func TestSmartAdaptiveWindows(t *testing.T) {
	s := &Smart{}
	now := time.Now()
	target := testSmartTarget("adaptive.example")
	// No history: default early-fail window (0 = keep 3s), 1s hedge delay.
	if got := s.expectedTotalMs(target, "A"); got != 0 {
		t.Fatalf("expectedTotalMs without data = %v, want 0", got)
	}
	if got := s.earlyFailWindow(target, "A"); got != 0 {
		t.Fatalf("earlyFailWindow without data = %v, want 0 (default)", got)
	}
	if got := s.hedgeDelay(target, "A"); got != smartHedgeNoDataDelay {
		t.Fatalf("hedgeDelay without data = %v, want %v", got, smartHedgeNoDataDelay)
	}
	// Mid-range median scales linearly: 3× / 1.5×.
	pushSamples(target, "A", 400, 8, now)
	if got := s.expectedTotalMs(target, "A"); got != 400 {
		t.Fatalf("expectedTotalMs = %v, want 400", got)
	}
	if got := s.earlyFailWindow(target, "A"); got != 1200*time.Millisecond {
		t.Fatalf("earlyFailWindow = %v, want 1.2s", got)
	}
	if got := s.hedgeDelay(target, "A"); got != 600*time.Millisecond {
		t.Fatalf("hedgeDelay = %v, want 600ms", got)
	}
	// Fast path clamps to the floors; slow path clamps to the ceilings.
	pushSamples(target, "B", 20, 8, now)
	if got := s.earlyFailWindow(target, "B"); got != smartEarlyFailMin {
		t.Fatalf("earlyFailWindow floor = %v, want %v", got, smartEarlyFailMin)
	}
	if got := s.hedgeDelay(target, "B"); got != smartHedgeMinDelay {
		t.Fatalf("hedgeDelay floor = %v, want %v", got, smartHedgeMinDelay)
	}
	pushSamples(target, "C", 5000, 8, now)
	if got := s.earlyFailWindow(target, "C"); got != smartEarlyFailWindow {
		t.Fatalf("earlyFailWindow ceiling = %v, want %v", got, smartEarlyFailWindow)
	}
	if got := s.hedgeDelay(target, "C"); got != smartHedgeMaxDelay {
		t.Fatalf("hedgeDelay ceiling = %v, want %v", got, smartHedgeMaxDelay)
	}
	// A fresh exact entry falls back to the aggregate (site prior) median.
	agg := testSmartTarget("aggregate.example")
	pushSamples(agg, "A", 300, 8, now)
	exact := testSmartTarget("sub.aggregate.example")
	exact.aggregate = agg
	if got := s.expectedTotalMs(exact, "A"); got != 300 {
		t.Fatalf("aggregate fallback = %v, want 300", got)
	}
}

func TestSmartMeasureConnAdaptiveHangWindow(t *testing.T) {
	// A path with history is declared hung from its own expected duration,
	// not the 3s default.
	client, server := net.Pipe()
	defer server.Close()
	failures := make(chan error, 1)
	measured := newSmartMeasureConn(client, smartConnCallbacks{
		earlyFailWindow: 100 * time.Millisecond,
		onEarlyFail:     func(err error) { failures <- err },
	})
	go func() {
		buffer := make([]byte, 8)
		server.Read(buffer) // consume, never respond
	}()
	if _, err := measured.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-failures:
	case <-time.After(time.Second):
		t.Fatal("hang not detected within the adaptive window")
	}
}

func TestSmartRaceSettleFastPath(t *testing.T) {
	// Before any response the race is opaque; once the winner is decided and
	// both buffers are drained it must become replaceable so the copy
	// pipeline can splice the winner connection directly.
	var upstream net.Conn
	dial := func() (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			buffer := make([]byte, 5)
			total := 0
			for total < 5 {
				n, err := server.Read(buffer[total:])
				if err != nil {
					return
				}
				total += n
			}
			server.Write([]byte("pong!"))
		}()
		upstream = client
		return client, nil
	}
	race := newSmartRaceConn([]*smartRaceCandidate{
		{tag: "A", flags: &smartRaceFlags{}, dial: dial},
	}, nil)
	defer race.Close()
	if race.ReaderReplaceable() || race.WriterReplaceable() {
		t.Fatal("race replaceable before any response")
	}
	if !race.NeedHandshakeForRead() || !race.NeedHandshakeForWrite() {
		t.Fatal("race not marked as needing handshake before settle")
	}
	if _, err := race.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 64)
	n, err := race.Read(buffer)
	if err != nil || string(buffer[:n]) != "pong!" {
		t.Fatalf("read %q err %v", buffer[:n], err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !race.ReaderReplaceable() {
		if time.Now().After(deadline) {
			t.Fatal("race never settled after winner + drained buffers")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if race.NeedHandshakeForRead() || race.NeedHandshakeForWrite() {
		t.Fatal("settled race still demands handshake")
	}
	if got := race.Upstream(); got != upstream {
		t.Fatalf("Upstream = %v, want winner conn", got)
	}
}

func TestSmartHedgeRescueAndPenalty(t *testing.T) {
	silentDial := func() (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			buffer := make([]byte, 16)
			server.Read(buffer) // consume, never respond
		}()
		return client, nil
	}
	newGroup := func() (*Smart, *smartTarget) {
		s := testSmartGroup("A", "B")
		s.setTestDetour("A", func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
			return silentDial()
		})
		s.setTestDetour("B", func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
			return raceDialer(t, 10*time.Millisecond, "standby-response", 5)()
		})
		target := testSmartTarget("hedge.example")
		// History keeps the hedge delay at its 200ms floor and the early-fail
		// window at its 500ms floor, so both fire inside the test.
		pushSamples(target, "A", 40, 8, time.Now())
		return s, target
	}

	// Hung primary: the standby rescues the connection and the primary is
	// charged exactly one failure — the hedge loss and the (later,
	// abandoned-suppressed) hang timer must not double-count.
	s, target := newGroup()
	primaryConn, err := silentDial()
	if err != nil {
		t.Fatal(err)
	}
	conn := s.dialHedged(context.Background(), target, s.memberSet.Load().byTag["A"], primaryConn,
		[]string{"B"}, M.ParseSocksaddrHostPort("hedge.example", 443), true)
	defer conn.Close()
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 64)
	n, err := conn.Read(buffer)
	if err != nil || string(buffer[:n]) != "standby-response" {
		t.Fatalf("read %q err %v, want standby rescue", buffer[:n], err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for s.lastUsed.Load() != "B" {
		if time.Now().After(deadline) {
			t.Fatal("winner accounting did not run")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Wait past the primary's early-fail window: the hang timer fires on the
	// abandoned loser and must be suppressed.
	time.Sleep(700 * time.Millisecond)
	target.mu.Lock()
	consecFail := target.members["A"].consecFail
	cooling := target.members["A"].cooldownUntil.After(time.Now())
	target.mu.Unlock()
	if consecFail != 1 {
		t.Fatalf("primary consecFail = %d, want exactly 1 (no double count)", consecFail)
	}
	if cooling {
		t.Fatal("single hedge loss must not cool the primary down")
	}

	// Exploration primary: losing the hedge is not a failure.
	s, target = newGroup()
	primaryConn, err = silentDial()
	if err != nil {
		t.Fatal(err)
	}
	conn2 := s.dialHedged(context.Background(), target, s.memberSet.Load().byTag["A"], primaryConn,
		[]string{"B"}, M.ParseSocksaddrHostPort("hedge.example", 443), false)
	defer conn2.Close()
	if _, err := conn2.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	n, err = conn2.Read(buffer)
	if err != nil || string(buffer[:n]) != "standby-response" {
		t.Fatalf("read %q err %v, want standby rescue", buffer[:n], err)
	}
	time.Sleep(700 * time.Millisecond)
	target.mu.Lock()
	consecFail = target.members["A"].consecFail
	target.mu.Unlock()
	if consecFail != 0 {
		t.Fatalf("explore primary consecFail = %d, want 0 (exempt)", consecFail)
	}

	// Fast primary: the standby never dials and nothing is charged.
	s, target = newGroup()
	standbyDialed := make(chan struct{}, 1)
	s.setTestDetour("B", func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
		standbyDialed <- struct{}{}
		return nil, net.ErrClosed
	})
	fastConn, err := raceDialer(t, 5*time.Millisecond, "primary-response", 5)()
	if err != nil {
		t.Fatal(err)
	}
	conn3 := s.dialHedged(context.Background(), target, s.memberSet.Load().byTag["A"], fastConn,
		[]string{"B"}, M.ParseSocksaddrHostPort("hedge.example", 443), true)
	defer conn3.Close()
	if _, err := conn3.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	n, err = conn3.Read(buffer)
	if err != nil || string(buffer[:n]) != "primary-response" {
		t.Fatalf("read %q err %v, want primary", buffer[:n], err)
	}
	select {
	case <-standbyDialed:
		t.Fatal("standby dialed although the primary answered within the delay")
	case <-time.After(400 * time.Millisecond):
	}
	target.mu.Lock()
	consecFail = target.members["A"].consecFail
	target.mu.Unlock()
	if consecFail != 0 {
		t.Fatalf("fast primary consecFail = %d, want 0", consecFail)
	}
}

func TestSmartRefreshStaleAlternative(t *testing.T) {
	// Sticky decisions must keep at least the stalest alternative supplied
	// with fresh comparison data, or the improvement channel starves once the
	// initial samples expire.
	engine, probes := testSmartEngine("JP", "HK", "US")
	views := testSmartViews("JP", "HK", "US")
	target := testSmartTarget("refresh-alt.example")
	now := time.Now()
	pushSamples(target, "JP", 20, 8, now)
	pushSamples(target, "HK", 60, 8, now)
	engine.decide(target, views, "tcp", now) // re-decision: current = JP
	if target.current != "JP" {
		t.Fatalf("setup: current = %s", target.current)
	}
	*probes = nil
	engine.decide(target, views, "tcp", now) // sticky
	found := false
	for _, probe := range *probes {
		if probe == "US:refresh-alt" {
			found = true
		}
		if probe == "HK:refresh-alt" {
			t.Fatalf("fresh alternative re-probed: %v", *probes)
		}
	}
	if !found {
		t.Fatalf("member with no record not treated as infinitely stale: %v", *probes)
	}
	// Once US has a recent probe on record, a stale HK becomes the target.
	target.stats("US").lastProbe = now
	stale := now.Add(16 * time.Minute)
	target.stats("JP").lastSample = stale // keep the current member fresh
	*probes = nil
	engine.decide(target, views, "tcp", stale)
	found = false
	for _, probe := range *probes {
		if probe == "HK:refresh-alt" {
			found = true
		}
	}
	if !found {
		t.Fatalf("stale alternative not refreshed: %v", *probes)
	}
	// UDP flows follow the learned state but must not trigger refreshes.
	target.stats("US").lastProbe = time.Time{}
	*probes = nil
	engine.decide(target, views, "udp", now)
	for _, probe := range *probes {
		if probe == "US:refresh-alt" || probe == "HK:refresh-alt" {
			t.Fatalf("UDP decision triggered an alternative refresh: %v", *probes)
		}
	}
}

func TestSmartExploreStaleRotation(t *testing.T) {
	// Once every member's TTFB window is full, exploration rotates by sample
	// staleness instead of stopping forever.
	engine, _ := testSmartEngine("JP", "HK", "US")
	views := testSmartViews("JP", "HK", "US")
	target := testSmartTarget("explore-stale.example")
	now := time.Now()
	pushSamples(target, "JP", 5, 8, now)
	engine.decide(target, views, "tcp", now) // threshold walk: current = JP (preferred)
	if target.currentWhy != smartReasonPreferred {
		t.Fatalf("setup: currentWhy = %s", target.currentWhy)
	}
	// Partially filled member wins over staleness rotation.
	pushTTFB(target, "HK", 30, smartTTFBWindowCap)
	target.stats("HK").lastTTFB = now.Add(-20 * time.Minute)
	pushTTFB(target, "US", 40, 10)
	target.stats("US").lastTTFB = now
	if got := engine.exploreCandidate(target, views, "tcp", now); got != "US" {
		t.Fatalf("exploreCandidate = %q, want US (window not full yet)", got)
	}
	// All full: the stalest member past the TTL is re-explored.
	pushTTFB(target, "US", 40, smartTTFBWindowCap)
	if got := engine.exploreCandidate(target, views, "tcp", now); got != "HK" {
		t.Fatalf("exploreCandidate = %q, want HK (stalest full window)", got)
	}
	// All full and fresh: exploration pauses.
	target.stats("HK").lastTTFB = now
	if got := engine.exploreCandidate(target, views, "tcp", now); got != "" {
		t.Fatalf("exploreCandidate = %q, want none when all windows are fresh", got)
	}
}

func TestSmartLegFactorStreak(t *testing.T) {
	// One noisy dial measurement must not flip the protocol leg factor; a
	// consistent re-classification over smartLegFactorStreak rounds does.
	s := testSmartGroup("A")
	member := s.memberSet.Load().byTag["A"]
	early := smartProbeResult{totalMs: 100, dialMs: 1, timingTrusted: true}     // classifies as leg 2
	blocking := smartProbeResult{totalMs: 100, dialMs: 95, timingTrusted: true} // classifies as leg 1
	ambiguous := smartProbeResult{totalMs: 70, dialMs: 35, timingTrusted: true} // middle band: no vote
	s.noteAnchorSuccess(member, ambiguous)
	s.noteAnchorSuccess(member, ambiguous)
	if member.legFactor != 1 || member.legStreak != 0 {
		t.Fatalf("legFactor = %v streak = %d after ambiguous rounds, want untouched", member.legFactor, member.legStreak)
	}
	s.noteAnchorSuccess(member, early)
	if member.legFactor != 1 {
		t.Fatalf("legFactor = %v after one early-data round, want 1 (streak pending)", member.legFactor)
	}
	s.noteAnchorSuccess(member, early)
	if member.legFactor != 2 {
		t.Fatalf("legFactor = %v after two early-data rounds, want 2", member.legFactor)
	}
	s.noteAnchorSuccess(member, blocking)
	if member.legFactor != 2 {
		t.Fatalf("legFactor = %v after one noisy round, want 2 kept", member.legFactor)
	}
	s.noteAnchorSuccess(member, early) // agreement resets the streak
	s.noteAnchorSuccess(member, blocking)
	if member.legFactor != 2 {
		t.Fatalf("legFactor = %v, want 2 (streak was reset)", member.legFactor)
	}
	s.noteAnchorSuccess(member, blocking)
	if member.legFactor != 1 {
		t.Fatalf("legFactor = %v after two blocking rounds, want 1", member.legFactor)
	}
}

func TestSmartAnchorDownGuard(t *testing.T) {
	// Every member failing its health round means the anchor endpoint died,
	// not the members: nobody gets demoted. One healthy member restores the
	// normal demotion path.
	s := testSmartGroup("A", "B")
	memberA := s.memberSet.Load().byTag["A"]
	memberB := s.memberSet.Load().byTag["B"]
	anchorErr := net.ErrClosed
	for i := 0; i < smartMemberDownThreshold; i++ {
		s.noteAnchorFailure(memberA, anchorErr)
		s.noteAnchorFailure(memberB, anchorErr)
	}
	memberA.mu.Lock()
	aliveA := memberA.alive
	memberA.mu.Unlock()
	if !aliveA {
		t.Fatal("member demoted although the anchor failed via every member")
	}
	// B recovers: the anchor is fine, A really is broken.
	s.noteAnchorSuccess(memberB, smartProbeResult{totalMs: 50, dialMs: 45})
	s.noteAnchorFailure(memberA, anchorErr)
	memberA.mu.Lock()
	aliveA = memberA.alive
	memberA.mu.Unlock()
	if aliveA {
		t.Fatal("member kept alive although another member reaches the anchor")
	}
	// A single-member group keeps plain demotion (cannot distinguish).
	single := testSmartGroup("S")
	memberS := single.memberSet.Load().byTag["S"]
	for i := 0; i < smartMemberDownThreshold; i++ {
		single.noteAnchorFailure(memberS, anchorErr)
	}
	memberS.mu.Lock()
	aliveS := memberS.alive
	memberS.mu.Unlock()
	if aliveS {
		t.Fatal("single-member group skipped demotion")
	}
}

func TestSmartUntrackedFailOpen(t *testing.T) {
	// With every member marked down (anchor death), untracked traffic must
	// fail open in priority order instead of erroring.
	s := testSmartGroup("A", "B")
	dialed := false
	s.setTestDetour("A", func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
		dialed = true
		client, server := net.Pipe()
		go func() { buffer := make([]byte, 8); server.Read(buffer[:]) }()
		return client, nil
	})
	s.setTestDetour("B", func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
		return nil, net.ErrClosed
	})
	for _, member := range s.memberSet.Load().members {
		member.mu.Lock()
		member.alive = false
		member.mu.Unlock()
	}
	conn, err := s.dialUntracked(context.Background(), "tcp", M.ParseSocksaddrHostPort("open.example", 443), s.memberViews())
	if err != nil || conn == nil {
		t.Fatalf("dialUntracked with all members down: err=%v, want fail-open dial", err)
	}
	conn.Close()
	if !dialed || s.lastUsed.Load() != "A" {
		t.Fatalf("fail-open used %q (dialed=%v), want A in priority order", s.lastUsed.Load(), dialed)
	}
}

func TestSmartProbeHTTPTiming(t *testing.T) {
	// probeHTTP must yield a timing sample shaped like passive samples, so
	// port-80 targets can feed the improvement channels (spec R3-2).
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				buffer := make([]byte, 1024)
				conn.Read(buffer)
				time.Sleep(20 * time.Millisecond)
				conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"))
			}(conn)
		}
	}()
	prober := newSmartProber(context.Background(), 1, 2*time.Second)
	detour := &fakeDialOutbound{
		Adapter: outbound.NewAdapter("test", "H", []string{"tcp"}, nil),
		dial: func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
			return net.Dial("tcp", listener.Addr().String())
		},
	}
	result, err := prober.probeHTTP(detour, "example.com", 80)
	if err != nil {
		t.Fatal(err)
	}
	if result.totalMs < 15 {
		t.Fatalf("totalMs = %v, want >= ~20ms (server delay observed)", result.totalMs)
	}
}

func TestSmartLegFactorUntrustedTiming(t *testing.T) {
	// Plain-HTTP probe timings cannot vote: a transparent proxy answering
	// the port-80 SYN locally fakes a near-zero dial on a blocking member.
	if _, definitive := legFactorFor(smartProbeResult{totalMs: 70, dialMs: 2}); definitive {
		t.Fatal("untrusted timing classified as definitive")
	}
	// Protocol truth still wins over an untrusted probe.
	if got, definitive := legFactorFor(smartProbeResult{totalMs: 70, dialMs: 2, earlyData: true}); got != 2 || !definitive {
		t.Fatalf("earlyData over HTTP probe = (%v, %v), want (2, definitive)", got, definitive)
	}
}
