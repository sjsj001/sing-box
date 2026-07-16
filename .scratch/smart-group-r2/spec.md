# Smart 出站组 R2：自适应故障窗口 + 在途接力 + 探索保鲜

Status: confirmed — 2026-07-15 与 owner 对话确认（对比调研 Surge/mihomo 后提出，owner
「记录下 spec，然后开始实现吧」批准）。基线：`.scratch/smart-group/spec.md`（含 Addendum）。

## 1. 背景与调研结论

对照两份外部实现做了对比调研：

- **Surge Smart Group**：官方 KB（kb.nssurge.com guidelines/smart-group）+ 社区发布帖
  #2536（Wayback 恢复 1–54 楼，官方回帖 #1/#18/#19/#29/#39）。
- **mihomo Smart**（vernesong fork）：clashparty.org/docs/guide/smart-core-principles。

关键外部事实（影响本轮设计的）：

1. Surge/mihomo 均以「握手耗时 > 1.5× 历史均值」触发怀疑，并**在途接力**：当前这条
   连接立刻由备用策略完成，上层无感（Surge KB 原文「上层的连接甚至不会察觉到发生了切换」）。
2. 两家都靠常态探索保持全员数据新鲜：Surge「从表现最好的几个策略中随机选取」（官方明说
   跳动是预期行为）、mihomo 随机探索 + 预计算。我们选择了粘性优先，就必须用定向后台刷新
   补数据新鲜度。
3. Surge 数据不落盘（#39：重启/改配置即清零）、混地域组退化为纯 fallback（#19：总延迟
   主导排序）。这两点确认现有实现（快照持久化、基线扣除 T_remote）已领先，不需动。
4. 带宽/吞吐两家都没做好（Surge 明文不考虑带宽，社区 5+ 次请求未获承诺；mihomo 只有粗糙
   加成）——被动吞吐观测是差异化机会，但**本轮不做**（见 §5 deferred）。

对现有代码的三处真实差距（本轮范围）：

- **G1 在途接力缺失**：sticky 路径单路拨号，路径挂起时连接本身以错误/客户端超时告终，
  降级只保护后续连接（smart.go DialContext 顺序链 + smart_conn.go 固定 3s hang 定时器）。
- **G2 探索饥饿**：latency 选出的目标，非 current 成员样本 15min 过期后再无刷新来源，
  improve/threshold 通道永久失明（decide 的 SWR 只刷 current；explore 仅覆盖
  preferred/affinity 目标，且 TTFB 窗口攒满 64 后 exploreCandidate 永久返回空）。
- **G3 固定失败窗口**：smartEarlyFailWindow 固定 3s，对中位数 50ms 的路径要等 3s 才
  发现挂起，而历史中位数就在手边（adaptiveDialTimeout 已用同样思路做拨号预算）。

## 2. 决策记录

| # | 决策点 | 结论 |
|---|--------|------|
| R2-1 | 早期失败窗口 | 自适应：clamp(3×期望总耗时, 500ms, 3s)；期望 = 精确表中位数 → 聚合表中位数兜底；无数据维持 3s。仅 TCP；UDP 包装器不变 |
| R2-2 | sticky 路径 hedge | 所有 tracked TCP 拨号（冷启动竞速分支除外）：primary 照旧同步拨号（保留顺序链、AppendRealOutbound、onDialSuccess），成功后作为预连接候选进 raceConn；备选 ≤2 个来自既有 attempts 链，延迟 D、2D 递延；首个响应字节者胜 |
| R2-3 | hedge 延迟 D | clamp(1.5×期望总耗时, 200ms, 2s)；无数据 1s。1.5× 沿用 Surge 的怀疑系数；早于失败宣告（3×），接力先于定罪 |
| R2-4 | hedge 败者记账 | 备选胜出 = primary 的「变坏快速通道」证据：对 primary 记一次 onDialFailure（2 次即冷却+降级）。经 atomic CAS 与 early-fail 路径去重，单事件只记一次；explore 决策的 primary（探索成员）不罚——慢≠坏，且备选本来就是 current |
| R2-5 | raceConn 快通道 | settle（胜者定、winnerErr 空、head 与写缓冲排干、未关闭）后实现 Reader/WriterReplaceable + NeedHandshakeForRead/Write + Upstream=胜者 conn，恢复 splice/零拷贝。冷启动竞速与 failover hedge 免费受益（解决 spec §3.2 的 splice 顾虑扩大化问题） |
| R2-6 | 备选数据保鲜 | sticky decide 时：非 current 的可用成员中，max(lastSample, lastProbe) 超过 sample_ttl 者取最陈旧一个发 "refresh-alt" 探测（每次 decide 至多 1 个；无 stats 的成员视为无限陈旧，覆盖快照恢复后的盲区）。稳态成本 ≈ 每活跃目标每成员每 15min 一次探测，由既有 prober 限频/信号量封顶 |
| R2-7 | explore 保鲜 | smartTargetStats 增加 lastTTFB 时间戳；exploreCandidate 先按原规则补样本数不足者，全员攒满后改选 lastTTFB 最陈旧且超过 sample_ttl 者；全新鲜则不探索。修复「TTFB 窗口攒满后探索永久停止、亲和结论冻结在陈旧对比数据上」 |
| R2-8 | 配置面 | 零新增配置。全部由既有 sample_ttl/中位数窗口推导；escape hatch 沿用 explore_interval=0（只关 explore，不关 refresh-alt） |
| R2-9 | 观测性 | hedge 由备选救场时记 Info 日志（`rescued by X from Y`）；AppendRealOutbound 仍记 primary（同步时机安全），备选救场的连接 real outbound 显示 primary——接受此误差（冷启动竞速路径至今完全不记，本方案严格更好） |

## 3. 风险与边界

- **首写字节重放**：hedge 触发时缓冲的客户端首写字节会重放给备选成员。TLS ClientHello
  完全安全；明文非幂等单发协议理论上可能双发——Surge/mihomo 的透明接力同样如此，接受。
  server-first 协议（SMTP/IMAP banner）无客户端字节，天然安全。
- **偶发多余连接**：健康路径抖动到 1.5× 中位数以上会多拨一条备选连接（旋即作废）。
  spike 判据本身是 2× 中位数，1.5× 触发的 hedge 只是保险性预热，不计失败。
- **hedge 误罚**：D 过小会把「偶慢」记成失败。防线：D 有 200ms 下限 + 1.5× 系数 +
  记账走既有 fail_limit=2 通道（单次误罚不降级）+ spike 通道本就有 2 次门槛。
- **refresh-alt 探测放大**：上限 = 活跃目标数 × (成员数-1) / sample_ttl。8192 目标全
  活跃、5 成员时 ≈ 36 probe/s 峰值，被全局信号量(8) + per-key 15s 限频自然压平；实际
  活跃目标远少于表容量。

## 4. 测试计划

单元（沿用 fake clock + 脚本化 prober + net.Pipe 模式）：

1. 自适应窗口：有中位数 → 3× clamp；仅聚合表有数据 → 聚合兜底；无数据 → 3s 默认。
2. raceConn settle：响应前 NeedHandshakeForRead=true/Replaceable=false；胜者定且 head
   排干后 Replaceable=true、Upstream=胜者 conn；全败/关闭不 settle。
3. hedge 时序：primary 在 D 内响应 → 备选永不拨号；primary 挂起 → 备选在 D 后拨号、
   胜出、数据直通；primary 拨号即败 → 备选立即唤醒。
4. hedge 记账：备选救场 → primary 恰好 +1 失败（连续 2 次救场 → 冷却+降级）；early-fail
   先触发时 CAS 去重不双计；explore 决策不罚 primary。
5. refresh-alt：sticky decide 且备选样本超龄 → 恰一次 refresh-alt 探测请求（最陈旧者）；
   新鲜时零请求；无 stats 成员被选中。
6. explore 保鲜：全员 TTFB 攒满且超龄 → 选最陈旧者；未满者优先；全新鲜 → 不探索。

集成（10.10.10.4，沿用 test/smart/ 框架，后续跟进）：

- S13 hedge 救场：iptables DROP（不 REJECT）当前成员使其静默挂起，验证在途连接由备选
  完成、无客户端可见错误、primary 两次后冷却。
- S14 保鲜：latency 目标长跑 >30min 后 tc netem 劣化 current，验证 improve 通道仍能
  发现并切换到（数据本应过期的）备选成员。

## 5. Deferred / 明确不做

- **被动吞吐信号**（bulk 流 bytes/sec p90 + 亲和式覆盖）：差异化机会已确认（两家都没
  做好、社区有真实需求、被动观测绕开「不能烧流量测速」反对），但噪声风险需单独设计，
  另立 slug 再议。
- **failover hedge 连败计数**（持续慢 primary 每连接白付 hedge_delay 且永不受罚）：
  failover spec 明确无延迟学习，暂不动；若实际观测到再议。
- **UDP 失败与 TCP 冷却解耦**：现状 UDP 失败冷却共享 (member,target) 状态会牵连 TCP；
  实际风险小（UDP 无 hang 定时器，静默黑洞不记失败），观察。
- Surge 式 top-N 概率抽样、mihomo 式 EMA/近期加权、LightGBM、ASN 聚合：维持 R1 spec
  §2.3 的否决。
- 成员级延迟乘数旋钮（policy-priority 类似物）：无需求不加。

## 6. 实现映射

| Issue | 文件 | 内容 |
|---|---|---|
| 01 | smart_conn.go, smart.go | R2-1 自适应早期失败窗口 + expectedTotalMs 助手 |
| 02 | smart_race.go, smart.go | R2-2/3/4/5 settle 快通道 + dialHedgedSticky + 记账 |
| 03 | smart_engine.go, smart_table.go | R2-6/7 refresh-alt + explore 保鲜 |
| 04 | smart_test.go | §4 单元测试 |

---

## Addendum R3 (2026-07-16, owner-delegated「吞吐不用做，其他的你看一下是否合理，合理就做」)

Follow-ups from the R2 verification round, each owner-delegated for judgment:

| # | 项 | 结论与实现 |
|---|---|-----------|
| R3-1 | legFactor 误判平滑 | 做。翻转需连续 2 轮一致分类（smartLegFactorStreak）；冷启动不再首轮即采纳（naive 成员 ~2min 后获得 factor 2，期间相对比较不受影响） |
| R3-2 | 非 443 探测无计时 | 收窄后做。仅 80 端口改用 probeHTTP（协议正确且有计时），其余端口维持 probeAlive——server-first 协议发 HEAD 计时是虚构。failover 恢复探测同步 |
| R3-3 | 锚点故障误伤成员 | 做防误伤，不做完整自校准。①全员健康检查同窗失败 → 判锚点故障、不降级（≥2 成员才可判定）；②untracked 路径全员 down 时按优先序 fail-open（此前会整体报错）。failover 同步 ①。完整 B4 自校准（目标观测导出基线）另行设计 |
| R3-4 | failover hedge 连败不受罚 | 做。standby 首字节胜出 = primary 对该目标一次 dial failure（CAS 与 dial-error/early-fail 去重），2 次进入既有冷却 → 不再每连接白付 hedge_delay。standby 败者维持免罚。修订 failover spec #4 的「losers unpenalized」语义（addendum 记录） |
| R3-5 | UDP 冷却解耦 | 不做。UDP 无 hang 定时器、静默失败本不记账；共享冷却的出口一致性可能是期望行为。等实测证据 |
