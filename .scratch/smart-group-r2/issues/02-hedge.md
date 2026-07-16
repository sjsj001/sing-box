# 02 sticky-path hedge + raceConn settle fast-path
Status: resolved

smart_race.go：settledLocked()（胜者定 && winnerErr 空 && head/写缓冲排干 && 未关闭）→
Reader/WriterReplaceable、NeedHandshakeForRead/Write、Upstream。

smart.go：DialContext 成功分支（TCP、非冷启动竞速、attempts 有剩余）改走
dialHedged：primary 已拨通的 conn（measured+interrupt 包装后）作预连接候选 0，
attempts 剩余成员（支持 TCP，≤2）作延迟候选（D、2D；D = clamp(1.5×expected, 200ms, 2s)，
无数据 1s）。onWinner：备选胜出 → onDialSuccess(备选) + Info 日志 + （非 explore 决策时）
CAS 去重后对 primary 记 onDialFailure。primary 的 onEarlyFail 与 hedge 记账共享同一
CAS；abandoned 候选一律免罚（沿用竞速惯例）。

## Comments

- 2026-07-15: Implemented and unit-tested (6 new tests; full package `go test -race -tags with_naive_outbound` green; whole-repo build green).
