# Post-implementation review findings & fixes (2026-07-15)

Three parallel review passes (concurrency; engine-vs-spec; conn/probe/integration)
were run over the smart-group code before commit; every confirmed finding below
was fixed the same day and covered by a regression test where practical. Spec
references are to `spec.md`.

## Fixed — high

1. **Dial-success reset defeated early-failure accrual for session protocols**
   (`smart_engine.go`). naive/cronet dials "succeed" locally; the real failure
   surfaces post-dial. `onDialSuccess` reset `consecFail`, so alternating
   dial-success/early-failure never reached `fail_limit` — observed live in
   integration S8 as 19 consecutive failed requests pinned to a dead HK
   forwarder. Fix: streak resets on a completed passive sample (a response
   byte), not on dial. Test: `TestSmartEarlyFailureAccrualAcrossDials`.
2. **Silent-drop hangs were invisible to failure detection** (`smart_conn.go`).
   A firewalled member path hangs without erroring; the 3s early-fail window
   only caught *errors*. Observed live in S6 (blocked JP IP → 10s dead
   request, no state change). Fix: zero-byte hang timer fires an early failure
   at the window boundary. Test: `TestSmartMeasureConnHangEarlyFail`.
3. **Race conn double-counted one candidate's failure** (`smart_race.go`): a
   broken conn errors in both the write pump and the head read, tripping
   "all failed" while a healthy candidate was still racing. Fix: per-candidate
   failed flag. Test: `TestSmartRaceSingleCandidateDoubleFailure`.
4. **Winner pump write error caused a missed wakeup** (`smart_race.go`):
   a Write blocked on the drain wait slept forever. Fix: `winnerErr` +
   broadcast. Test: `TestSmartRaceWinnerWriteErrorUnblocks`.
5. **`ensure` check-then-act split-brain** (`smart_table.go`): concurrent
   first dials to one key created distinct entries; samples/decisions landed
   in orphans. Fix: single-critical-section get-or-create (lock order
   tb.mu → target.mu). Test: `TestSmartTableEnsureConcurrent`.
6. **UDP flows rewrote the TCP-learned sticky member** (spec §2.1 #10
   violation). Fix: UDP re-decisions never commit `setCurrent`; deferral is
   per-flow. Test: `TestSmartUDPDoesNotRewriteSticky`.
7. **Local aborts counted as member failures** (`smart_conn.go`): user cancels
   / interrupt-group closes fed `onDialFailure` (2 aborts = spurious
   cooldown). Fix: `net.ErrClosed`/`context.Canceled` filtered. Test:
   `TestSmartMeasureConnLocalAbort`.
8. **`probeAlive` was a no-op through group wrappers** (`smart_probe.go`):
   direct type assertion missed `HandshakeContext` behind interrupt wrappers →
   false recoveries on non-443 targets. Fix: `common.Cast` (walks Upstream)
   plus the spec'd short-read fallback.
9. **Alive-only probes fabricated 0ms samples** (`smart.go`→`onProbeResult`):
   non-443 probe success pushed `0` into the timing window, making the member
   look infinitely fast. Fix: only push `ms > 0`. Test:
   `TestSmartProbeAliveNoSample`.
10. **Preferred members could never win back anycast targets** (spec S3):
    the improve channel was latency-only, so a race-winner held an anycast
    target forever even when preferred[0] qualified the threshold walk. Fix:
    threshold-walk winner ≠ current becomes a *preference-driven challenger*
    (same probe-confirm + dwell anti-flap gates). Verified in integration S3.
    Test: `TestSmartPreferredChallengeConvergence`.

## Fixed — medium/low

- Unsynchronized `t.aggregate` reads in four engine callbacks (Go memory-model
  race vs the re-link write) — now read under `t.mu`.
- Cooldown backoff was zeroed on recovery, making relapse escalation dead code
  (spec §2.1 #6). Backoff is now retained; `smartRelapseWindow` decides
  escalate-vs-restart. Straggler failures during an active cooldown no longer
  re-escalate. Test: extended `TestSmartFailureCooldownRecovery`.
- Target-down had no rate-limited retry (spec 全员失败 "限频重试"): added
  `downRetryAt` fail-fast window (3s) + one retry chain per window. Test:
  `TestSmartTargetDownRateLimit`.
- Cold-start backfill probes were race-path-only (spec §3.3 step 4): now the
  sequential TCP/UDP cold paths schedule `coldstart-fill` probes.
- Adaptive dial timeout (spec B9) implemented: per-attempt budget
  `clamp(3×median, 1s, 5s)`; falls back to the member anchor baseline.
- Race trigger extended to sniffed TLS on non-443 ports (spec B2).
- Origin affinity scoped to threshold-rule winners (spec §2.1 #8).
- In-memory record TTL (spec: 磁盘与内存统一 14 天) enforced in `ensure`.
  Test: `TestSmartTableInMemoryTTL`.
- Measurement wrapper now implements `NeedHandshakeForRead/Write` so the sing
  copy pipeline re-unwraps it after sampling (restores splice/zero-copy;
  spec 吸取 681194db1 教训).
- Immediate probe re-cool now applies only while `needsRecovery` (was: any
  `backoff > 0`, a hair-trigger), and skips when the target itself is down.
- Persistence: snapshot writers serialized (`persistMu`), fsync before rename,
  cache file mode 0600; loser-conn closes moved outside the race lock; head
  read treats `n>0 && err != nil` as a win per io.Reader contract; prober
  `lastRun` map GC'd; member-down now honors `interrupt_exist_connections`;
  `AppendRealOutbound` added to untracked paths.

## Known, deliberately not fixed (v1)

- TLS 1.3 TTFB heuristic can time NewSessionTicket arrival instead of origin
  first byte — record-layer ambiguity, accepted as best-effort (biases
  affinity toward *not* establishing; safe direction).
- Race conn deadlines don't unblock a candidate stuck in dial pre-winner;
  bounded by the inbound connection's own lifetime/timeouts.
- Raced connections don't `AppendRealOutbound` (winner unknown at return;
  mutating metadata later would race readers).

## Follow-ups completed (same day, second commit)

- **Provider support** — implemented and verified end-to-end, then
  **REMOVED the same day by owner decision** ("smart 的成员应是区域组而非
  订阅裸节点；没用就删掉"). The user-facing options
  (providers/include/exclude/use_all_providers) are gone; the internal
  atomic member-set snapshot and atomic engine order were kept as
  implementation structure. Any review comparing against this paragraph
  should treat the absence of provider options as intentional.
- **User docs**: `docs/configuration/outbound/smart.md` + `smart.zh.md`.
- **Extended integration scenarios** (`test/smart/run2.sh` +
  `config-local.json`, fault injection via per-member `routing_mark` +
  iptables/tc): S12 12-real-site breadth, S11 one member suddenly unable to
  reach one site (per-target isolation, ≤1 user-visible failure), S10 one
  member's network suddenly degraded (spike demotion, fast recovery). 7/7.
- **30-minute stability soak** (`test/smart/soak.sh`), spec S8 full version.
  The first soak FAILED (34 steady-state switches) and exposed two defects
  short tests could not see, both fixed in commit 5476a31aa:
  1. *Affinity ↔ preferred oscillation*: the preferred-challenge (added for
     S3) ignored established origin affinity, so the two channels traded the
     target forever (~2min cycle with test dwell). Fix: an established
     affinity pins its target until revoked (spec §3.3 step 2 — affinity
     overrides the walk). Test: `TestSmartAffinityPinsAgainstPreferredChallenge`.
  2. *Post-switch transient spikes*: the first samples through a freshly
     promoted member (session setup costs) read as 2× spikes and re-demoted
     it immediately. Fix: `smartSwitchGrace` (15s) suppresses spike
     accounting after a switch; hard failures still count.
  Re-soak after the fixes: 2030 requests, 0.54% failures, **0 steady-state
  switches** (spec's stretch goal).
