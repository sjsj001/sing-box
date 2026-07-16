# Code review — commit 13f2c5769 "Add smart and failover outbound groups"

- Date: 2026-07-15
- Diff: `git diff 76b05d018...HEAD` (single commit, 42 files, +7954 lines)
- Spec source: `.scratch/smart-group/spec.md` + `.scratch/smart-group/review-fixes.md` (addenda)
- Standards sources: no prose coding standards in repo; `.golangci.yml` (tooling, skipped); established conventions in `protocol/group/` siblings + Fowler smell baseline

## Standards

### Consistency with established repo conventions (hard-ish)

1. **`option/smart.go`, `option/failover.go` — option placement/shape diverges from `option/group.go`.** All existing group outbounds (Selector, URLTest, LoadBalance) define options in `option/group.go` and embed `GroupCommonOption` (`Outbounds` + `Providers`/`Exclude`/`Include`/`UseAllProviders`). The new groups declare a bare `Outbounds []string` in separate files, silently dropping provider support their siblings all have.

2. **`smart_engine.go` — raw `"udp"` string literals** at lines 147, 359, 364, 416, 547, 643, 767, where every sibling (selector.go, urltest.go, even smart.go itself) uses `N.NetworkUDP`. Also a baseline **Repeated Switches / Primitive Obsession**: the same `network != "udp"` test recurs seven times.

3. **`smart.go` comments promise provider support that doesn't exist**: "provider updates swap the whole set atomically" (l.75), "static order + provider order" (l.79), "Provider members are appended at Start()/rebuild time" (l.157). Nothing ever swaps `memberSet` after `Start()`. **Possible Speculative Generality** — the atomic-snapshot rebuild machinery serves an unimplemented need; judgement call, but the comments are actively misleading.

### Baseline smells (all judgement calls)

- **Duplicated Code (strongest finding, smart ↔ failover):**
  - `smart.go runBaselineRound` ≈ `failover.go runHealthRound` — identical failStreak/okStreak/down-threshold/interrupt/recover logic; `smartMember` and `failoverMember` are near-identical structs.
  - Backoff shape `stats.backoff++; duration := base << (backoff-1); cap; needsRecovery=true` appears three times: `smart_engine.go applyCooldownLocked`, `failover.go onDialFailure` (l.563–574), `failover.go requestRecoveryProbe` (l.598–603).
  - `failoverTable`/`failoverTarget`/`stats()` (failover.go l.88–140) re-implements `smartTable`/`smartTarget`/`stats()`.
  - `smart.go adaptiveDialTimeout` ≈ `failover.go dialBudget` (max(3×median, 1s), cap 5s).
  - Within `smart_engine.go`: the challenger-reset triple `t.challenger = ""; t.challengerWhy = ""; t.confirmStreak = 0` appears 6×; the improve-margin computation is duplicated between `checkChallengerLocked` (l.714–718) and `onProbeResult` (l.808–813); `attempts := append([]string{X}, e.failoverChain(...)...)` 4× in `decideLocked`.
- **Data Clumps:** `(t *smartTarget, tag string, members smartMemberViews, network string, now time.Time)` travels through ~10 engine entry points — wants an event/context type.
- **Middle Man:** `smartFormat` (smart.go l.211) is a pure `fmt.Sprintf` wrapper.
- **Mysterious Name:** `t2 time.Time` in `smartMeasureConn` (smart_conn.go l.110) — nothing says "time client's post-handshake app record was sent".

Suppressed: mirrored TCP/UDP method pairs (siblings endorse it); string `Strategy` option (loadbalance.go precedent); anything staticcheck/modernize/gofumpt enforces.

## Spec

### (a) Missing or partial

1. **Provider support (review-fixes addendum) never landed.** review-fixes.md: "Provider support (`GroupCommonOption`: providers/include/exclude/use_all_providers), matching selector/urltest… Verified end-to-end." `option/smart.go` has no provider fields (no `GroupCommonOption` embed, unlike selector/urltest in `option/group.go`), `buildMembers` resolves only `staticTags`, and no ProviderManager callback exists. Comments like "Provider members are appended at Start()/rebuild time" (smart.go:157) are vestigial. The promised Compatible-placeholder fallback is also absent.
2. **No baseline self-calibration fallback.** Spec §3.2/B4: "锚点不可达时退化为自校准(近期全目标观测最小值)". `runBaselineRound` (smart.go:689) has no such path — 3 anchor failures instead mark the member **dead**, colliding with §5: "成员死只由区域组健康检查决定".
3. **Test-plan gaps.** §4.1 #8 ("legFactor: dial≈0 → 2;dial≈B+R → 1") and #12 ("SWR…恰一次后台刷新(singleflight)") have no corresponding tests in smart_test.go.

### (b) Scope creep

4. **Entire failover outbound group** (~1,600 lines: protocol/group/failover.go + tests, option/failover.go, both failover docs, config-failover.json, run3.sh/run4.sh, `TypeFailover`, `RegisterFailover`). Spec §2.1 #2: "smart 不实现主备" (nested urltest owns it); §3.1 limits upstream touchpoints to "TypeSmart + DisplayName case" and "RegisterSmart 一行".
5. **`smart-cache` CLI + smart_dump.go** (~266 lines). §2.1 #11: "无面板/clash api 集成;决策事件走结构化日志" — a snapshot-inspection command wasn't asked for (read-only, benign).
6. **docs/agents/{domain,issue-tracker,triage-labels}.md** — unrelated workflow docs bundled into the commit.

### (c) Implemented but deviating

7. **`anchor` option renamed/redefaulted.** Spec §3.2: "默认 `www.gstatic.com:443`,可配 `anchor`"; code ships `health_check` with default `1.1.1.1:80` (smart.go:36,122) — and still sends a TLS ClientHello (`probeTLS`) to port 80. Anchor also doubles as a member-liveness check (see finding 2), a semantic the spec never granted.
8. **Default tolerance 20ms** (smart.go:38, docs agree) vs spec §3.4 `"tolerance": "30ms"`.
9. **Persist is not dirty-gated.** §2.1 #9: "5 分钟脏写"; `persistLoop` snapshots unconditionally every tick. Minor.

### Review-fixes verification

All other addenda verified present: early-failure accrual across dials, hang timer, race per-candidate fail flag + `winnerErr` broadcast, `ensure` single critical section, UDP non-commit (`commit = network != "udp"`), local-abort filter, `common.Cast` probeAlive + short-read fallback, `ms > 0` probe samples, preferred challenger + affinity pin, `smartSwitchGrace`, `smartRelapseWindow`, `downRetryAt`, adaptive dial timeout, `NeedHandshakeForRead/Write`, fsync/0600/`persistMu`, member-down honoring `interrupt_exist_connections`, `AppendRealOutbound` on untracked paths. Only the provider follow-up (finding 1) is unfulfilled.

## Summary

Standards 轴 7 项发现,最严重的是 option 定义偏离 `option/group.go` 惯例、丢掉了 provider 支持;Spec 轴 9 项发现,最严重的是 review-fixes.md 承诺的 provider 支持完全没有落地(且 `health_check` 探测会向 80 端口发 TLS ClientHello,行为可疑)。
