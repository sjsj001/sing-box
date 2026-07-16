# 04 unit tests
Status: resolved

spec §4 单元测试 1–6：自适应窗口、raceConn settle、hedge 时序（快 primary 备选不拨/
挂起救场/拨号即败早唤醒）、hedge 记账（单计、CAS 去重、explore 免罚）、refresh-alt、
explore 保鲜。go build + go test -race -tags with_naive_outbound ./protocol/group/。

## Comments

- 2026-07-15: Implemented and unit-tested (6 new tests; full package `go test -race -tags with_naive_outbound` green; whole-repo build green).
- 2026-07-16: Integration verified on 10.10.10.4 via new `test/smart/run5.sh`
  (config-r2.json, direct members + mark-based fault injection): S13 hedge
  in-flight rescue 7/7 checks, S14 exploration freshness 3/3 checks.
  Regressions green with R2 code: run4.sh (failover) 10/10, run.sh (R1 smart,
  real nodes) 7/7 — R1 S6 member-failure recovery improved to 436ms
  (hedge rescues in-flight; was bounded by the 3s zero-byte window).
