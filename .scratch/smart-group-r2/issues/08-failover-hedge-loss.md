# 08 failover hedge-loss accounting (R3-4)
Status: resolved

failover.go dialHedged：standby 胜出对 primary 记一次 onDialFailure，primaryAccounted
CAS 与 dial-error/early-fail 三路去重；2 次进入既有 per-target 冷却，恢复探测照旧。
standby 败者维持免罚。failover spec addendum 已记录语义修订。

## Comments

- 2026-07-16: Implemented; TestFailoverHedgeLossCoolsPrimary.
