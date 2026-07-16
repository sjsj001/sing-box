# Smart 出站组（smart outbound group）

Status: confirmed — 面谈决策 + 2.md/3.md 对比借鉴已合入，待最终确认后实现

## 1. 背景与目标

用户有 n 个分布在不同区域（HK/US/DE/SG/JP…）的代理节点，本地→节点的带宽与延迟有保障；节点→目标网站的链路质量不确定（节点可能挂、链路可能临时劣化）。目标：所有本地请求走代理，且每个请求自动选到「以最大速度访问该目标」的节点。

核心痛点（来自 Surge Smart Group / mihomo smart 的体验）：**节点来回跳、不稳定**。经源码调研，mihomo 不稳定的根因：EMA 平滑对最新样本权重 ~67%、每个连接重新打分、per-目标粘性缓存一次失败即整条删除、探索用随机 shuffle。本方案把「稳定性」作为一等设计目标。

非目标：不移植 mihomo 代码，不引入 LightGBM/ML，不做面板（clash api）集成。

## 2. 决策记录

### 2.1 面谈结论

| # | 决策点 | 结论 |
|---|--------|------|
| 1 | 方案取向 | 自研轻量方案，一次做完整版 |
| 2 | 节点组织 | 嵌套复用：每区域一个 urltest(fallback) 组做主备，smart 成员 = 区域组（允许混搭裸节点）；smart 不实现主备 |
| 3 | 测量策略 | 混合：真实连接被动采样为主；主动探测仅用于冷启动、切换前验证、冷却恢复验证、惰性刷新 |
| 4 | 冷启动 | TCP 并行竞速（≤3 候选）；UDP 用先验单选 |
| 5 | 学习表 key | **双写两级**（2.md）：每样本同时写精确键（FQDN/精确 IP）与聚合键（eTLD+1 / IPv4 /24 / IPv6 /48）；读取精确优先、聚合兜底。新子域天然继承父域先验，异类子域随样本自动分化，零分裂判据 |
| 6 | 切换判据 | 双通道不对称：变好严格（优势 max(25%,30ms) + 主动探测连续 2 次确认 + 最小驻留 5min）；变坏快速（连续 2 次失败立即降级 + 指数冷却 1→15min + 全员失败检测） |
| 7 | anycast 处理 | **preferred 阈值遍历**（2.md）：无显式分类状态。按有效优先序（preferred 前缀 + 其余成员声明序）遍历，第一个「新鲜样本 T_remote ≤ 10ms 且健康」者直接胜出；都不达标 → 回退到「总耗时 tolerance 带内选 T_remote 最低」。preferred 只需列一两个在意的成员（前缀语义，自动补全） |
| 8 | TTFB 角色 | 源站亲和检测：对「由阈值规则胜出」的目标，用长窗口 TTFB 分布（中位数+p90）跨成员对比，差距显著且样本充足时自动优先回源快的成员（覆盖阈值规则结果）；1/16 探索流量攒对比样本 |
| 9 | 持久化 | **独立 JSON 快照**（二轮评审后推翻 cachefile bucket 决定）：可选 `cache_path`（缺省纯内存）；`.tmp`+rename 原子写，5 分钟脏写 + Close 落盘；14 天未访问淘汰；损坏/未知成员 tag → 警告+丢弃、干净启动；重启后粘性保持、首次命中触发后台探测刷新；内存 LRU 8192 条 |
| 10 | UDP | UDP 跟随 TCP：同 key 用 TCP 学习结果选同一成员；UDP 首包往返仅作观测与失败检测；成员不支持 UDP 时顺延；UDP 不竞速 |
| 11 | 可观测性 | 无面板/clash api 集成；决策事件走结构化日志；手动逃生 = 外面包一层 selector |
| 12 | 测试 | 单测（模拟时钟+模拟成员）+ 集成测试（10.10.10.4，rsync 源码远端编译；允许 iptables/tc 故障注入） |

### 2.2 自 2.md / 3.md 借鉴（已确认）

| 项 | 内容 | 来源 |
|---|------|------|
| B1 | 探测原语 = 隧道内拨号后发最小 TLS ClientHello（真实 SNI，`InsecureSkipVerify`），收到首字节即关闭；支持 `HandshakeContext` 的协议（naive，duck-typing `interface{ HandshakeContext(context.Context) error }`）优先用它取真实「代理→目标建连」时序。**依据：naive 出站是 `DialEarly`（protocol/naive/outbound.go:238，已验证），拨号立即返回，纯 TCP 建连计时不可用** | 2.md/3.md |
| B2 | 竞速 = raceConn 写扇出：DialContext 立即返回 raceConn，客户端首写（ClientHello）扇出到 K 条成员连接，首个读到响应字节者胜、其余关闭、此后透明直通；仅对 443/已嗅探 TLS 且无记录的目标触发；败者时序种入学习表 | 3.md |
| B3 | legFactor 归一化：早数据协议首写→首读 ≈ B+2R，阻塞式 ≈ B+R；用 dial 耗时 vs 基线的启发式（或 HandshakeContext 可用性）判定系数，`T_remote = max(0, M − B_min) / legFactor` | 3.md |
| B4 | 基线用滚动最小窗（16 样本）而非 EWMA；锚点法（隧道内对 anycast 锚点做 B1 同款测量）+ 自校准兜底（该成员近期全部目标观测值的窗口最小值 ≈ 基线） | 2.md/3.md |
| B5 | 持久化只存 T_total 统计，T_remote 运行时由「当前基线」导出，不落库 | 2.md |
| B6 | FakeIP 裸 IP（无域名）不入表，走 preferred 默认 | 3.md |
| B7 | interrupt 按成员分组；「变好切换」绝不打断存量连接，只有成员死亡才打断（且只打断该成员的连接） | 3.md |
| B8 | 探测风暴控制：per-key singleflight + 全局信号量（8）+ 0–500ms 抖动 + stale-while-revalidate（旧决策照常服务、后台刷新）；无后台全表扫描 | 3.md |
| B9 | 自适应拨号超时 max(3×历史耗时, 1s) 封顶 5s | 2.md |
| B10 | 集成测试故障注入补充「本地 TCP forwarder」手段：区域组内配「直连入口 + 经 forwarder 入口」两个成员指向同一真实节点，kill forwarder 模拟区域内主挂（解决每区域仅 1 个真实节点无法测主备的问题）；iptables/tc 保留用于整节点阻断与链路劣化 | 3.md |
| B11 | 代码放 `protocol/group/smart*.go` 同包新文件，直接复用包内 `getKey`（eTLD+1 提取）、`RealTag`、interrupt 惯用法；新文件零 rebase 冲突 | 2.md/3.md |
| B12 | 测试编译需 `-tags with_naive_outbound`（include/naive_outbound.go 有 build tag，已验证） | 3.md |

### 2.3 对比后明确不借鉴 / 已否决的评审建议

- EMA 打分（α=0.3 重蹈 mihomo 覆辙）→ 坚持固定窗口中位数。
- clashapi/面板集成（2.md 的 api_meta_group 分支、URLTest 按钮）→ 用户已裁掉，纯日志。
- 3.md 的 smart 内建 region/groups 配置 → 用户已选嵌套 urltest 组。
- 3.md 的「>24h 未用条目不落盘」→ 与 14 天保留目标冲突，磁盘与内存统一用 14 天 TTL。
- 2.md 的 7 天过期 → 保持 14 天。
- 评审建议「TTFB 源站亲和降级 v2」→ **用户否决，v1 全量实现**（含探索流量与自动覆盖；`explore_interval` 可配，设 0 关闭探索作为风控兜底开关）。
- 持久化于二轮评审改为 3.md 的独立 JSON（见 2.1 #9），砍掉 adapter 接口 + cachefile bucket 这个最大上游触点。

### 2.4 本方案独有、保留

- TTFB 源站亲和检测 + 探索预算（两案均未解决需求 5 的回源问题）。
- 变好切换需主动探测连续 2 次确认（两案仅 dwell+margin，无确认步）。
- 全员失败检测：窗口内所有成员对同一目标都失败 → 判定目标自身故障，不记冷却、不切换、限频重试（3.md 仅有 per-(节点,目标) 隔离，会造成冷却雪崩）。

## 3. 架构

### 3.1 文件布局与上游触点

新文件（零 rebase 风险），与现有组同包：

```
protocol/group/smart.go          # 注册、lifecycle、DialContext/ListenPacket、选路编排、Now()/All()
protocol/group/smart_engine.go   # 纯决策引擎（无 I/O；clock/prober 接口注入，可单测）：
                                 #   阈值遍历、tolerance 回退、双通道切换、冷却、全员失败、源站亲和
protocol/group/smart_table.go    # 双写两级学习表：键提取（复用 getKey）、LRU、per-target 状态
protocol/group/smart_conn.go     # 被动测量 wrapper：首写→首读、TLS record 状态机（TTFB）、fast-path 切换
protocol/group/smart_probe.go    # 探测器：基线锚点、ClientHello 探测、HandshakeContext、风暴控制
protocol/group/smart_race.go     # raceConn 写扇出竞速
protocol/group/smart_persist.go  # JSON 快照读写（版本号、原子替换、过期清理）
protocol/group/smart_test.go     # 单元测试
option/smart.go                  # SmartOutboundOptions（新文件）
test/smart/                      # 集成测试：config 模板、run.sh、forwarder、verify 脚本
```

上游触点仅 2 处单行插入：`constant/proxy.go`（TypeSmart + DisplayName case）、`include/registry.go`（RegisterSmart 一行）。不改 adapter/、cachefile、route/、InboundContext。

### 3.2 测量模型

**基线 B（本地→成员）**：周期（默认 60s，随健康检查顺带）通过成员对 anycast 锚点（默认 `www.gstatic.com:443`，可配 `anchor`）做 ClientHello 首字节测量，取滚动最小窗（16）→ `B_min`。锚点远端 RTT 2–6ms，对所有成员等量高估、不影响横向比较；顺带给 mux 暖机。锚点不可达时退化为自校准（近期全目标观测最小值）。

**被动采样（主力）**：每条 TCP 连接包测量 wrapper：
- `M = t(首个回包) − t(首个出包)`（TLS 即 ClientHello→ServerHello）
- `T_remote = max(0, M − B_min) / legFactor`（legFactor 见 B3；同为 naive 时 = 2）
- TTFB = 客户端首个应用数据 record → 服务端首个应用数据 record（TLS1.2 按 type-23；TLS1.3 用「客户端 CCS 后第 2 个 type-23」启发式；0-RTT/无法识别则放弃采样）
- 非 TLS：M 退化为首写→首读往返，跳过 TTFB
- 采样完成后 wrapper 切 fast-path（透传 N.* 接口与 ReadFrom/WriteTo，不破坏 splice；吸取 681194db1 教训）
- 拨号后 3s 内零字节读错误按失败计入（早数据协议的 CONNECT 失败以此浮现）

**主动探测（辅助）**：B1 原语。时机：①冷启动补样（竞速未覆盖的成员）；②变好切换前连续 2 次确认；③冷却期满恢复验证；④SWR 惰性刷新（TTL 15min 过期后由真实请求触发后台刷新，旧决策照常服务）。风暴控制见 B8。非 443 目标：对 host:port 拨号 + HandshakeContext 时序（无则短读超时探活）。

**样本聚合**：每 (key, member) 固定环形窗口 16 样本取中位数（T_remote）；TTFB 长窗口 64 样本取中位数 + p90。无 EMA。

### 3.3 选路算法（每条新连接）

```
key = getKey(metadata)          # Fqdn→SniffHost→Domain→IP；FakeIP 裸 IP 不入表
查表：精确键 → 聚合键 → 均无 = 冷启动
候选池 = 未处于该目标冷却期、成员级健康的成员

1. 粘性闸门：现任存在、健康、无降级触发 → 沿用（热路径 O(1)，atomic.Pointer 无锁读）
2. 阈值遍历：按有效优先序（preferred 前缀 + 其余声明序），第一个
   「新鲜样本 T_remote ≤ anycast_threshold 且健康」者胜出
   ——若该目标存在成立的源站亲和结论，用亲和成员覆盖本步结果
3. 回退排序：有新鲜数据的成员中，总耗时在 min+tolerance 带内者取 T_remote 最低
4. 冷启动：443/TLS → raceConn 竞速（候选 = preferred[0] + 全局胜率 top + 次优，≤3）；
   其余端口/UDP → 有效优先序第一个健康成员 + 后台探测补样
5. 探索：由阈值规则胜出的热点目标，每第 16 条连接分给需要攒 TTFB 样本的候选
```

后台状态迁移（事件驱动 + 探测回调）：
- **变好切换**：候选中位数优势 > max(25%, 30ms) 且探测连续 2 次确认且现任驻留 > 5min → 切换（不打断存量连接）
- **变坏降级**：现任对该目标连续 2 次失败/超时，或 M 突增 > 历史中位数 2 倍且重复 → 立即降级 + 冷却（1→2→4→…→15min），冷却满经探测验证才恢复候选资格；同一次调用内对剩余候选依序重试（≤3）
- **全员失败**：判定目标自身故障，不冷却、不切换、限频重试
- **源站亲和**：TTFB 中位数与 p90 同时显著更优（≥30% 且样本 ≥32/成员）且持续 → 成立；差距消失 → 撤销（同样带确认步）

### 3.4 配置示例与默认值

```json
{
  "type": "smart",
  "tag": "smart-out",
  "outbounds": ["HK组", "US组", "DE组", "SG组", "JP组"],
  "preferred": ["JP组"],

  "anchor": "www.gstatic.com:443",
  "anycast_threshold": "10ms",
  "tolerance": "30ms",
  "switch": { "improve_ratio": 0.25, "improve_min": "30ms", "dwell": "5m", "confirmations": 2 },
  "cooldown": { "fail_limit": 2, "base": "1m", "max": "15m" },
  "probe": { "concurrency": 8, "sample_ttl": "15m", "timeout": "5s" },
  "explore_interval": 16,
  "max_targets": 8192,
  "record_ttl": "14d",
  "cache_path": "smart-cache.json"
}
```

仅 `outbounds` 必填；`preferred` 是前缀语义（列一两个即可，缺省 = 声明顺序）；其余全部有默认值。区域组用现有 urltest fallback 配置。

## 4. 测试计划

### 4.1 单元测试（fake clock + 脚本化 prober，无网络，-race）

1. 键提取与双写两级：精确/聚合读写序、FakeIP 排除、IPv6 前缀
2. 阈值遍历：anycast 全员低 → preferred[0]；仅单成员达标 → 该成员；全不达标 → tolerance 带内 T_remote 最低
3. 防抖：现任 35ms、挑战者 30/40ms 交替 20 轮 → 零切换；真实改善 → dwell 内不切、过后经 2 次确认才切且恰 1 次
4. 变坏降级：连续失败→隔离→同调用顺延候选成功→冷却退避→探测恢复
5. 全员失败：不冷却、不切换、限频
6. 源站亲和：判定/样本不足不生效/覆盖/撤销
7. 竞速：胜者直通、败者关闭且样本入表、全败返回错误
8. legFactor：dial≈0 → 2；dial≈B+R → 1；基线滚动 min 抗 800ms 冷握手离群
9. 持久化：JSON round-trip、原子替换（写入中断不损坏旧文件）、14 天过期、LRU 淘汰、损坏/未知 tag 干净启动、T_remote 由基线导出
10. UDP 跟随、不支持 UDP 顺延、纯 UDP 冷启动
11. 测量 wrapper：TLS1.2/1.3/非 TLS/0-RTT 字节流用例；fast-path 切换后接口透传
12. SWR：TTL 过期旧决策照常服务 + 恰一次后台刷新（singleflight）

### 4.2 集成测试（10.10.10.4）

编译：本地 `go build -tags with_naive_outbound ./...` 验证；rsync 源码 → 远端编译运行。5 节点 = 5 区域组；HK 区域配两个成员（直连 + 本地 forwarder 转发同一节点）测主备。故障注入：kill forwarder（区域内主挂）、iptables 阻断（整成员挂）、tc netem（链路劣化），脚本 trap 清理。

| # | 场景 | 验收标准 |
|---|------|----------|
| S1 | 冷启动竞速：清表首访一批 443 站 | race 日志、胜者承载、败者样本入表；二访走粘性无竞速 |
| S2 | 区域收敛：日/德/美本土站各一批 | 收敛到对应区域且不再漂移 |
| S3 | anycast：google.com、cloudflare.com 等 | 阈值遍历命中 preferred[0]，即使它不是 RTT 最低 |
| S4 | 需求4 tie-break：两成员总耗时接近的目标 | 选 T_remote 更低者 |
| S5 | 区域内主备：kill HK forwarder | urltest 切备，smart 无感、数据延续 |
| S6 | 整成员故障：iptables 阻断当前成员 | 新连接 <5s 恢复次优；解除后经冷却+探测才回 |
| S7 | 链路劣化：tc netem 200ms/10% loss | 触发降级，撤销后恢复 |
| S8 | 长稳：≥30min 混合站点 | 无故障期每目标切换次数 ≤1（目标 0），每次切换日志可解释 |
| S9 | 重启持久化 | 粘性保持、无竞速风暴、陈旧记录后台刷新 |
| S10 | UDP/HTTP3：curl --http3 | 同 key TCP/UDP 同出口 |
| S11 | 源站亲和：回源慢的 CF 站长跑 | 探索样本积累后自动切换，日志含分布证据 |
| S12 | 基线合理性 | 各成员 B_min 与 ssh 实测到各服务器 RTT 交叉验证（±数 ms + 锚点远端 2–6ms） |

## 5. 风险与边界

- TLS1.3 TTFB 启发式有小概率误判（0-RTT）→ 放弃采样，宁缺毋滥。
- 探索流量（1/16，仅阈值规则命中的热点目标）让少量真实连接走非最优成员，且会造成同站出口 IP 偶发不一致（风控敏感站风险）——用户已确认接受，`explore_interval: 0` 可整体关闭。
- 成员无 IPv6 出口：只记 (成员,目标) 失败，不判成员死（成员死只由区域组健康检查决定）。
- 探测对目标可见性：低频、真实 SNI 半开握手，与浏览器放弃行为无异。
- 竞速仅限 TLS/443 且仅无记录目标，不放大常态连接数。

---

## Addendum (2026-07-15, post-implementation owner decisions)

The following deviations from the body above are **owner-ratified** and are
the current contract; reviewers should not flag them:

1. `anchor` option is named **`health_check`**, default **`1.1.1.1:80`**
   (was www.gstatic.com:443). Non-443 endpoints are probed with a plain
   HTTP HEAD, 443 with a TLS ClientHello.
2. Default `tolerance` is **20ms** (was 30ms).
3. The health check doubles as **member-level liveness** (3 down / 2 up) —
   §5's "成员死只由区域组健康检查决定" is superseded; the anchor-unreachable
   self-calibration fallback (§3.2/B4) is NOT implemented (open follow-up).
4. **Provider options were implemented, then removed** at owner direction;
   smart members are region groups, not raw subscription nodes.
5. The **`smart-cache` CLI** and the **`failover` group type** are separate
   owner-requested features (the latter has its own spec:
   `.scratch/failover-group/spec.md`), not scope creep against this spec.
6. Extra robustness beyond the body, added after live soaks: idle-mux
   warm-up samples are spike-neutral (60s gap), post-switch spike grace
   (15s), sparse-outlier resample probes, cold-start backfill covers all
   unraced members with 700ms stagger, dirty-gated persistence.
7. R2 follow-up (2026-07-15, separate effort): adaptive early-fail window,
   sticky-path hedged in-flight failover, raceConn settle fast-path, and
   exploration freshness fixes — see `.scratch/smart-group-r2/spec.md`.
