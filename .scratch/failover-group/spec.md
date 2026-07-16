# failover outbound group — design contract

Decided 2026-07-15 via an interview with the owner (each item explicitly
confirmed); implemented the same day. This is the review baseline.

## Purpose

Ordered same-region substitutes: one preferred member carries everything;
standbys take over on failure — per destination (member's egress cannot
reach one site) or entirely. No latency learning, no per-destination
optimization (that is smart's job; failover groups compose as smart
members).

## Confirmed decisions

| # | Decision |
|---|----------|
| 1 | Type name `failover` (rejected `fallback`: collides with urltest option) |
| 2 | Health endpoint field named `health_check` (flat host:port string); smart's `anchor` renamed to match |
| 3 | Primary-selection knob named `strategy`: `"order"` (strict declared order, default) / `"auto"` (elected by health baseline) |
| 4 | Hedged requests: single optional field `hedge_delay`; unset = disabled; fan-out 1; losers unpenalized; TCP only |
| 5 | UDP: full failover semantics (order, cooldown, same-call fallthrough), no hedge |
| 6 | Health probing hardcoded like smart: 1m interval, 3 down / 2 up |
| 7 | Per-target cooldown: same `cooldown` object as smart (2 / 1m / 15m), exact-key table, LRU 4096 hardcoded |
| 8 | No persistence (cooldowns are minute-scale transients) |
| 9 | auto election: rolling-min baseline, challenger needs >20% for 3 consecutive rounds, dead incumbent replaced immediately, re-election affects new connections only |
| 10 | `interrupt_exist_connections` on member-level down, default false |
| 11 | Same-call retry walks all remaining members; dial budget max(3×baseline, 1s) cap 5s; hedge starts early when the primary fails outright |

## Verification

Unit: order selection, per-target isolation, cooldown escalation +
straggler immunity, auto hysteresis, hedge timing (fast primary → standby
never dials; silent primary → standby wins after delay; failed primary →
immediate wake). Integration (test/smart/run4.sh): S15 one-site block —
zero failed requests, cooled per-target, control unaffected; S16 member
death — requests survive, down detected ≈3min, auto recovery; S18 auto
elects the lower-baseline member. 10/10.

---

## Addendum (2026-07-16, owner-delegated)

1. Decision #4's "losers unpenalized" is narrowed to **standby** losers: a
   standby win now charges the primary one dial failure toward the
   per-target cooldown (CAS-deduplicated with the primary's dial-error and
   early-failure paths). Rationale: a persistently silent primary taxed
   every connection with hedge_delay forever and was never demoted. See
   `.scratch/smart-group-r2/spec.md` Addendum R3-4.
2. Health-endpoint failure through **every** member no longer demotes
   members (endpoint death ≠ member death); single-member groups keep the
   plain demotion path. Mirrors smart's anchor-down guard (R3-3).
3. Recovery probes for port-80 targets use an HTTP HEAD (probeHTTP)
   instead of the banner-read liveness check (R3-2).
