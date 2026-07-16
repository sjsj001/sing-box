# 01 adaptive early-fail window
Status: resolved

smart_conn.go：smartConnCallbacks 增加 earlyFailWindow（0 = 维持 3s 默认），hang 定时器
与 reportEarlyFail 时间闸共用该值。smart.go：expectedTotalMs(target, tag)（精确表中位数
→ 聚合表中位数）+ earlyFailWindow = clamp(3×expected, 500ms, 3s)。UDP 包装器不变。
failover 复用 smartConnCallbacks 但不设新字段，行为不变。

## Comments

- 2026-07-15: Implemented and unit-tested (6 new tests; full package `go test -race -tags with_naive_outbound` green; whole-repo build green).
