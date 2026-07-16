# 07 anchor-down guard + untracked fail-open (R3-3)
Status: resolved

smart.go：noteAnchorFailure 降级前检查 anchorDown（≥2 成员近窗全失败且无成功 → 判锚点
故障，不降级、告警日志）；dialUntracked/listenPacketUntracked 全员 down 时按优先序
fail-open（此前锚点死亡 3 分钟后 untracked 流量整体报错）。failover.go noteHealthFailure
同步守卫。完整 B4 自校准另行设计。

## Comments

- 2026-07-16: Implemented; TestSmartAnchorDownGuard, TestSmartUntrackedFailOpen,
  TestFailoverHealthEndpointDownGuard.
