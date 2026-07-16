# Response to review-13f2c5769

Disposition of every finding; fixes landed by amending 13f2c5769.

## Accepted and fixed

| Finding | Fix |
|---|---|
| Standards #2: raw `"udp"` literals ×7 in smart_engine.go | Replaced with `N.NetworkUDP` |
| Standards #3: stale provider comments (smart.go l.75/79/157, engine l.92) | Rewritten to describe reality (set published once at Start) |
| Middle Man: `smartFormat` | Inlined, deleted |
| Mysterious Name: `t2` | Renamed `appRequestAt` + doc comment |
| Duplication (low-risk subset) | `cooldownDuration()` shared by engine + failover (3 sites); `clearChallengerLocked()` (6 sites); `improveMarginMs()` (2 sites) |
| Spec (c)7 second half: TLS ClientHello sent to port 80 | `probeAnchor`: HTTP HEAD for non-443, TLS for 443 (smart + failover) |
| Spec (c)9: persist not dirty-gated | `dirty` flag; idle instances skip the 5-minute write |
| Spec (a)3: test-plan gaps | Added `TestSmartLegFactor` (via extracted `legFactorFor`), `TestSmartSWRRefresh`, `TestSmartProberSingleflight` |
| Root cause: stale project docs | review-fixes.md corrected (provider removal recorded); spec addendum lists owner-ratified deviations; `.scratch/failover-group/spec.md` written as failover's review baseline |

## Rejected — owner decisions the reviewer could not see

| Finding | Reality |
|---|---|
| Standards #1 / Spec (a)1: provider support "missing/dropped" | Implemented, verified, then **removed at owner direction** the same day; absence is intentional |
| Spec (b)4: failover group "scope creep" | Separate owner-requested feature with its own interviewed-and-confirmed contract |
| Spec (b)5: smart-cache CLI "not asked for" | Explicit owner request |
| Spec (c)7 first half / (c)8: health_check rename, 1.1.1.1:80, tolerance 20ms | Explicit owner directions |
| Spec (b)6: docs/agents bundled | Staged by the owner (workflow docs) |
| Standards #1 (placement): options not in option/group.go | Deliberate: new-file-only layout is this fork's zero-rebase-risk principle (spec §3.1) |

## Acknowledged, deliberately deferred

| Finding | Rationale |
|---|---|
| Spec (a)2: anchor-unreachable self-calibration fallback; liveness vs §5 | Current member-liveness semantics were owner-approved (grilling Q6/health_check) and are integration-tested; self-calibration remains an open follow-up, recorded in the spec addendum |
| Duplication: smart↔failover member structs / health rounds | Sharing would couple two lifecycles for modest savings; re-verification cost outweighs |
| Data clump: engine 5-tuple parameter | Mechanical churn across ~10 signatures + all tests; no behavior benefit |
| Speculative Generality: atomic memberSet/order | Kept as structure (lock-free readers, tiny); comments no longer overpromise |
