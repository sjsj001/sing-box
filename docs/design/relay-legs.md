# smart 中转链测量/评分/健康判定 —— 最终裁决设计

## 0. 裁决结论

**骨架取 `fold`**（三段分解：near / chain / remote）。它是四份里唯一在对抗评审下没有致命问题的一份，而且它的核心恒等式我逐行核过代码后确认成立。

嫁接进来的东西（附出处）：

| 来源 | 嫁接内容 |
|---|---|
| `third-leg` | 「中间跳的系数是 **−α 而不是 0**」这个重新表述 —— 中转不是没被看见，是被奖励。用作文档与提交信息的解释装置。 |
| `third-leg` 评审 | `α·(near+chain) = α·(RT − RemoteDial)` 的塌缩恒等式，用来证明评分不依赖服务端如何切分 Total。 |
| `probe` | 「探测目标按成员角色分工」而不是全局二选一；`probeTimeout` 必须按段给预算，且**必须与测量层同一批上线**。 |
| `observed` | `markDown` 必须忽略 `ErrDestinationUnreachable`（独立真 bug，先上）；「劣化降分，故障判死」的判据框架；把确认过的劣化用 `retire` 语义写回窗口。 |
| 各评审 | 缓存 JSON key 改名；`resetPath` 不清 chain 窗；验算必须打在 `current()` 而不是 `score()` 上；直连一侧必须用真实直连样本而不是反推。 |

明确否决（详见 §7）：改 wire format、给 chain 单开 beta 系数、chain 用 min 或中位、chain 存目标级、warmup 首遍改打真实地址、把探测超时的预算写回窗口、新增第三种健康状态、全员心跳改真实地址。

---

## 1. 核心决策与理由

### 1.1 那道「缝」不是缝，是一个已经躺在协议里没人读的量

链路 `客户端 → r1 → … → 出口 → 目标`。客户端手上有三个数（`protocol/naive/outbound.go:355-372`）：

- `RoundTrip` —— 客户端自测，请求头上线到响应头回来。
- `ServerSpan` = r1 的 `ConnectAck.Total`。`common/dialer/connect_timing.go:147-149` 的 relay 分支返回 `t.total`，`connect_timing.go:29-36` 解释了为什么 total 是实测而不是分段相加：**它包含 r1 之后所有跳的等待**。
- `RemoteDial` = `ConnectAck.Connect`。`protocol/naive/lazy.go:271-273` 的 `RecordInnerAck` 让最内层的 connect 原样穿过每一跳。

于是三个量**逐条精确**成立：

```
near  = RoundTrip  − ServerSpan     客户端 → r1          （= 今天的 Local()）
chain = ServerSpan − RemoteDial     r1 → 最内层跳的拨号之前
remote= RemoteDial                  最内层跳 → 目标

near + chain + remote = RoundTrip
```

三跳链自动成立：r1 的 Total 含 r2 的 Total，Connect 穿透，所以 chain = B1+B2+…+出口的名字解析。**任意链深不需要额外工作，wire format 一个字节不改。**

生产样本代入：

```
identity.ess.apple.com:443   rtt=3150  span=3018  remote=14
near = 132ms    chain = 3004ms    remote = 14ms    合计 3150ms ✔
```

`docs/configuration/outbound/smart.zh.md:29` 早就写着「存在代理链时，local 覆盖到最内层代理为止的所有跳」，`protocol/naive/measure.go:39-41` 与 `protocol/group/smart_decision.go:14-19` 也这么写。**代码做的是相反的事**，而且 `measure.go:40-41` 给出的理由（「因为每一跳上报的 span 已经包含等下一跳的时间」）恰好推出相反结论。这是一次 bug 修复，不是新语义。

### 1.2 中间跳今天的系数是 −0.7，不是 0

```
score = α·local + remote
      = α·(RT − Span) + Connect
      = α·RT − α·(Span − Connect) + (1−α)·Connect
      = α·RT − α·chain + (1−α)·remote
```

代入：`0.7×3150 − 0.7×3004 + 0.3×14 = 106.4ms`，与实测打分 106ms 精确吻合。

**中转的坏被折算成 2103ms 的信用。** 这解释了 30 分钟 84 次健康翻转为什么救不回来：评分一直在把流量往坏的那条推，判死只是结果，不是原因。

### 1.3 只改公式无效 —— 这是本设计存在的理由

`local` 走 `rollingMin`（`smart_decision.go:352-377`，窗口 8 取最小）。一个 25% 丢包的中间跳，最小值落在干净样本的概率是 `1 − 0.25⁸ = 99.998%`。所以把 `Local()` 改成 `RT − RemoteDial` 而估计量不动，等于什么都没改。

**定义和估计量必须一起改。** 这是四份方案唯一的共识，也是唯一被所有评审确认的判断。

### 1.4 事故的真实决策函数是 `current()`，不是 `score()`

生产拓扑是**同一个分组的两个成员** + `select: fastest`。走的是 `smartGroup.current()`（`smart_member.go:337-381`），它只比 `member.local()` —— 没有 alpha、没有 remote、没有 bonus。`score()` 那一整套只在多分组配置下起作用。

修 `local()` 同时修好两条路，但**所有验算必须打在 `current()` 上**（§3.3）。四份原始设计里有三份把验算打错了函数。

---

## 2. 测量层

### 2.1 三个量、三处存放、三种估计量

| 量 | 存哪 | 估计量 | 为什么 |
|---|---|---|---|
| `near` | `smartMember.window`（今天的窗，不改名不改语义） | `rollingMin`，8 样本取最小 | 干扰它的是**客户端侧伪影**：冷 TLS 池、warmup 时并发握手抢核（`smart_member.go:242-249` 记着生产事故：78ms→1.5s，顺序跟调度走不跟地理走）。这些只会把样本往上推，地板是稳定估计。**这条理由只属于 near，不迁移。** |
| `chain` | `smartMember.chainWindow`（新增） | `rollingCost`：8 样本，去掉单个最大值后取均值 | 见 §2.2 |
| `remote` | `destinationEntry.Remote[tag]` | 中位窗，5 样本（`smart_cache.go:144-183`） | 不动 |

三个窗各自独立可动，就是用户要求的「任意一段劣化都能被识别」在结构上的落地。

### 2.2 chain 为什么用「去掉单个最大值的均值」

**它估的是代价，不是能力。** `rollingMin` 的注释（`smart_decision.go:347-351`）说得很清楚：最小值估的是「这条路能做到多好」。一条丢包的路「能做到」的和健康时一模一样，所以 min 对丢包在数学上不可见，加深窗口只会更瞎。中位数同样不行：25% 丢包时四分之三的样本还在好的那一模里。

**为什么只丢一个最大值，而不是两个、也不是不丢。** 需要被拒绝的伪影只有一类，而且它的形状是「一个窗口里最多一个」：

1. **出口的冷 DNS 解析**（约 50ms，`smart_race.go:488-497` 的实测值）。`connect_timing.go:150` 把 DNS 从 `connect` 里减掉但留在 `total` 里，所以它整份落进 chain。**但它不需要靠丢弃来处理**：一个 50ms 的样本混进 7 个 0.75ms 的样本，普通均值也只有 6.9ms，远低于 §3.3 算出的 39.5ms 切换阈值。
2. **中转到出口的冷 TLS/H2 握手**（200–500ms 量级）。这才是需要丢的那一个。而它一个窗口最多出现一次 —— 因为一旦该成员被判定为中转，它的心跳每 60±20s 打一次真实地址（§4），**心跳本身就把中转到出口的池保持在热态**。冷样本只出现在进程启动后第一次、或休眠唤醒后第一次。
3. 单个 15.7s 的停顿会被丢掉。这是已知代价：一个窗口里出现第二个，就会被完整看见。

也就是说，「丢一个最大值」不是拍脑袋的稳健化，它是按**已知只会出现一次的那一类伪影**量身定的。

```go
// rollingCost keeps what the last n samples cost on average, with the single
// worst one set aside.
//
// A cost estimator, not a capability estimator — which is the whole reason it
// cannot be rollingMin next to it. rollingMin answers "how good can this path
// be", and a hop that drops one packet in four is, at its best,
// indistinguishable from a perfect one: with a window of eight the minimum
// lands on a clean sample 99.998% of the time. What a ranking has to compare is
// what the next hundred connections will *pay*, and that is a mean.
//
// Exactly one sample is set aside, because exactly one artefact can appear once
// per window: the relay's own cold session to its exit. A chained member's
// heartbeat dials somewhere real every round, which keeps that session warm, so
// a cold one is what a process start or a wake produces and nothing else. The
// exit's cold resolver — the other thing charged to nobody, around fifty
// milliseconds — does not need discarding at all: seven warm samples beside it
// still average an order of magnitude below the threshold that moves a
// selection.
//
// The median is the wrong middle here, and the case that rules it out is the
// one that matters: 25% loss on a link whose base latency did not change leaves
// three samples in four untouched, so the median sees nothing.
type rollingCost struct {
	samples [localWindow]time.Duration
	next    int
	count   int
}

func (r *rollingCost) add(sample time.Duration) { /* 同 rollingMin.add */ }

func (r *rollingCost) value() (time.Duration, bool) {
	if r.count < medianQuorum {
		// 见 §9 修订 1：这里**不**沿用 remoteWindow 的「不足三条取最新」。
		// 那条规则的前提是「窗里都是同一目标的可比读数」，chain 窗跨目标聚合，
		// 前提不成立；而这个窗的头两条样本必然来自成员的第一条真实连接，
		// 恰好是冷握手与冷解析同时出现的那一刻。
		return 0, false
	}
	var sum, worst time.Duration
	for i := 0; i < r.count; i++ {
		sum += r.samples[i]
		if r.samples[i] > worst {
			worst = r.samples[i]
		}
	}
	return (sum - worst) / time.Duration(r.count-1), true
}
```

复用已有的 `localWindow = 8`（`smart_decision.go:48`）与 `medianQuorum = 3`（`smart_cache.go:154`），不新增窗口深度常量。`rollingMin` **一行不动**，两段注释互相点名，防止后人把 min 的理由抄过来。

### 2.3 检测下限，以及为什么生产这一例必然被检出

窗口 8、丢 1，需要 ≥2 个坏样本才动。若把劣化建模成平稳的间歇丢包：`P(k≥2 | p=0.25) = 63%`，`p=0.35` 时 83%，`p<0.125` 时期望贡献为 0。

**但生产这一例不是平稳过程，是一次 regime change。** 供应商把中转→出口的路由从 0.7ms/0% 换成 171ms/25%，在劣化期内 **8 个样本全部抬高**，截尾均值 ≥ 171ms，检测是必然的。给定的「chain 中位 8.6 / p90 1141」是把健康期和劣化期两个 regime 混在两小时里算出来的聚合值，不能当成平稳分布的分位数用 —— 用它做「81% 的窗口看不见尾部」的推断，前提不成立。

真正剩下的盲区是「纯丢包、基础时延不变」。它由 §5.4 的第二检测器（`observeScore` 逐条比较 + 归因）覆盖，那是 P3 的内容。

### 2.4 `ConnMeasurement` 的访问器

`protocol/naive/measure.go:39-51`：**删掉 `Local()`**，换成两个访问器。理由是避免同名异义 —— `local` 这个词在文档里一直指「客户端到最内层代理」，把它留在只覆盖第一跳的量上，正是这次事故的根。

```go
// NearHop is the leg to the first proxy: the round trip with everything that
// proxy said it spent taken out. It is what the heartbeat address measures on
// its own, and the one leg whose floor the local handshake can bound.
func (m ConnMeasurement) NearHop() time.Duration

// ChainSpan is the interior of the chain: everything the first proxy spent that
// is neither the client's own leg nor the innermost hop's connect. On a direct
// member it is the exit's routing and name resolution — half a millisecond. On
// a relay it is every hop between the first proxy and the last one.
//
// ok is false when no hop reported a connect, because absent is not zero: zero
// would say the chain has no interior, which is exactly what a relay looks like
// today and exactly what this measurement exists to stop.
func (m ConnMeasurement) ChainSpan() (time.Duration, bool)
```

`NearHop()` = `max(0, RoundTrip − ServerSpan)`，算术与今天的 `Local()` 逐位相同。
`ChainSpan()` = `max(0, ServerSpan − RemoteDial)`，`ok = HasSpan && HasRemote`。

`Validated()`（`measure.go:78-98`）**一行不改**。它的两条不变式恰好保证三段非负：`:82` 的 `span ≤ rt + tol` 给出 `near ≥ 0`，`:89` 的 `remote ≤ span + tol` 给出 `chain ≥ 0`。只在注释里补一句信任边界的变化（§3.2）。

### 2.5 记录路径：自应答探测绝不能进 chain 窗

`protocol/naive/inbound.go:203-211` 里第一跳自答 `.probe.arpa`，写的是 `FormatConnectAck(ConnectAck{HasConnect: true})`，即 `Total=0, Connect=0, HasConnect=true`。于是：

- `near = rtt − 0 = rtt` —— 这**就是**第一跳的距离，与真实流量样本的 near 是同一个量。**near 窗不存在语义污染。**
- `chain = 0 − 0 = 0` —— 这个 0 是谎。它说「这条链没有内部」。

**如果不拦，每次心跳往 chain 窗塞一个 0，8 个样本的窗会被心跳冲刷成 0，中转的中间跳永远看不见。这是整个改动里最容易漏、漏了最致命的一处。** 这也和代码库既有的纪律一致：`connect_ack.go:20-23`、`connect_timing.go:70-71`、`measure.go:22-25` 都在说同一件事 —— absent 是 unknown，zero 是 adjacent。

判据用调用点已有的信息，不猜：

```go
// smart_member.go:291
func (m *smartMember) recordMeasurement(measurement naive.ConnMeasurement,
	used bool, traversed bool) (reported, floor time.Duration, corrected bool) {
	// ...:292-312 不变...
	near := measurement.NearHop()
	bound, hasBound := m.localFloorLocked()
	if hasBound && near < bound {
		m.window.add(bound)
		reported, floor, corrected = near, bound, true
	} else {
		m.window.add(near)
	}
	if traversed {
		if chain, ok := measurement.ChainSpan(); ok {
			m.chainWindow.add(chain)
		}
	}
	return reported, floor, corrected
}
```

`traversed` 由调用点给：`record` 恒为 `true`；`recordProbe` 传 `!naive.IsProbe(destination)` —— 因为 `confirmMembers` / 真实地址心跳也走 `recordProbe`，而它们**确实**穿越了整条链。所以 `used` 不能复用作判据，必须新加一个参数。

### 2.6 读出：`local()` 与 `chain()`

```go
// smart_member.go:160
// local is the client to the innermost proxy: the near hop plus the chain's
// interior. It is what the documentation has always called local, and what the
// ranking needs — the round trip minus the one leg that belongs to the
// destination.
func (m *smartMember) local() (time.Duration, bool) {
	m.access.Lock()
	defer m.access.Unlock()
	near, ok := m.window.value()
	if !ok {
		return 0, false
	}
	chain, _ := m.chainWindow.value() // 空窗口按 0：从未见过内部的成员按单跳处理
	return near + chain, true
}

func (m *smartMember) near() (time.Duration, bool)  // 审计、readyTimeout、probeTimeout 用
func (m *smartMember) chain() (time.Duration, bool) // 竞速路径、分类、审计用
```

**空 chain 窗按 0 而不是「不可排序」。** 拒绝排序会让 `current()`（`smart_member.go:351-354`）、`selectGroup`（`smart_decision.go:136`）、`fallbackGroup`（`:163`）三处一起跳过该成员，代价远大于收益；而且 `smart_group_test.go:442-466`（`TestAProxyCannotClaimToBeCloserThanItsOwnHandshake`）构造的样本 `HasRemote=false`，chain 永远未知，改成「不可排序」会直接把这个测试的意图作废。

### 2.7 `resetPath()` **不清** chain 窗 —— 这是必须改对的一处

`smart_member.go:178-185` 的 `resetPath` 存在的理由（`:166-177` 写得很明确）是：**每个窗都是最小值，而最小值永远不会从一个不再存在的地板上恢复**。这条理由：

- 对 `window`（near）、`setupWindow`、`roundTripWindow` 成立 —— 它们都是 min，且都测量客户端这一侧的路径。
- 对 `chainWindow` **不成立** —— 它不是最小值（不存在「地板不恢复」问题），而且它测的是中转→出口，完全在客户端网络变更的另一侧。

所以：

```go
func (m *smartMember) resetPath() {
	m.lastSetup.Store(0)
	m.access.Lock()
	defer m.access.Unlock()
	m.window = rollingMin{}
	m.setupWindow = rollingMin{}
	m.roundTripWindow = rollingMin{}
	// chainWindow deliberately survives. Every window above is a minimum, and
	// the reason they are dropped is that a minimum never recovers from a floor
	// that no longer exists. chainWindow is neither: it is a mean, and what it
	// measures — the relay to its exit — is on the far side of the change. It is
	// also what decides whether this member's heartbeat can see that leg at all,
	// so clearing it would put every relay back on the probe address after every
	// wifi-to-cellular switch, which is the state this whole change exists to
	// leave.
}
```

这一条同时拆掉了「分类自举成环」的问题（§4.3）。

---

## 3. 评分层

### 3.1 公式一个字符不改

```go
// smart_decision.go:62-64、:70-72 —— 签名与实现均不动
score(alpha, local, remote, bonus)      = α·local + remote − bonus
staticBound(alpha, local, bonus)        = α·local − bonus
```

变的只有 `local` 的含义：从「客户端→第一跳」变成「客户端→最内层代理」，也就是 `smart_decision.go:15-17` 与 `smart.zh.md:29` 早就宣称的那个语义。

### 3.2 alpha 折扣整个 local（含 chain）

**(a) 分类上 chain 属于 local 那一类。** alpha 折扣 local 的理由是「local 是节点属性、跨目标近恒定」（`smart_decision.go:14-19`）。chain 在最后一跳**之前**，与目标无关，跨目标恒定 —— 和 near 同类。用户既管不着也换不掉，只能换成员避开，这正是 alpha 的语义。

**(b) 拆开会多一个没人能调的旋钮。** `α·near + β·chain + remote` 里的 β 没有可陈述的运维含义，也没人能从生产数据里推它。

**(c) β=1 会把评分重新绑回服务端自报的 span。** 展开：`α·near + 1.0·chain + remote = α·RT + (1−α)·span`。低报 span 直接省 `0.3 × 3018 = 905ms`。而 `measure.go:59-77` 与 `localFloorDivisor`（`smart.go:110-119`）整套机制存在的理由就是防这一条。必须否掉。

**取 β=α 之后得到的性质（要诚实地说清楚它有多强）：**

逐条连接展开：`α·local + remote = α·(RT − RemoteDial) + RemoteDial = α·RT + (1−α)·RemoteDial`。**`ServerSpan` 从表达式里消失。** 于是一个**一贯地**高报或低报 span 的服务端，对排序的影响是零 —— 它把 near 和 chain 反向等量移动，和不变。今天同样的谎值 −1.0ms/ms。

**这个性质在估计量层只是近似成立，不能声称「span 从评分里完全消失」。** near 用 min、chain 用截尾均值，一个只在 8 个样本里的 1 个上多报 span 的服务端，min 会咬住那一份被压低的 near，而截尾均值恰好会把对应的最大 chain 丢掉，净收益仍是 `−α·δ`。这条残留由 `localFloorLocked`（`smart_member.go:256-266`）兜住 —— 它作用在 near 上，正是它今天在兜的那条攻击，**位置和强度都没有变**。所以：攻击面从「一贯撒谎也有效」收窄成「只有间歇撒谎才有效，且被握手地板夹住」，而不是归零。这一句要原样写进文档的信任边界一节。

**`localFloorLocked` 只作用于 near。** `setup` 是客户端到第一跳的 TCP+TLS（`smart.go:109-119` 的注释明写），它只界定第一跳。套在合并后的 local 上仍是合法下界，但松在要害上：中转可以用一个大的 chain 把 near 的低报藏起来。代码上不需要改 —— `localFloorLocked` 读的是 `setupWindow` 与 `roundTripWindow`，`recordMeasurement` 里它比较的对象改成 `near` 即可（§2.5 已经是这样写的）。

### 3.3 用生产真实数字验算

**估计量取值的来源（全部来自给定数据，无拟合）：**

- 直连 chain 截尾均值 ≈ **0.75ms**。给定分布中位 0.5 / p90 1.3 / max 4.4；真实直连样本 `span − remote = 1.79 − 1.04 = 0.75ms`，两者一致。
- 中转 chain 劣化期 ≈ **162ms**。由给定的聚合中位反推：`median(rtt) − min(near) − median(remote) = 307.9 − 132 − 14 = 161.9ms`。**与任务里独立给出的「路由劣化成 171ms」吻合**，两条互不依赖的数据对上了，所以这个数不是编的。
- 中转 chain 健康期 ≈ **8.6ms**（给定中位）。

**（a）组内 `current()` —— 事故的真实决策路径，只比 `local()`：**

| | 今天 local | 改后 local | 结果 |
|---|---|---|---|
| 中转 | `min(near) = 132.0` | `132.0 + 162 = 294.0` | |
| 直连 | `min(near) = 155.71` | `155.71 + 0.75 = 156.46` | |
| **判决** | 中转赢 **23.7ms** > 15ms → **选中转**（复现 bug） | 直连赢 **137.5ms** ≫ 15ms → **切到直连** ✔ | |

- **切换阈值**：`132 + chain > 156.46 + 15` → **chain > 39.5ms 时组内离开中转**。这是可以直接拿去和 audit 对账的一个数。
- **健康期不误伤**：chain = 8.6ms → 中转 local = 140.6 vs 直连 156.46，中转仍以 15.9ms 领先并留任。**修复没有变成「一刀切禁用中转链」。**
- **直连变化**：155.71 → 156.46，**+0.75ms / +0.48%**，比 `switchHysteresis`（15ms，`smart_decision.go:39`）低一个数量级。约束 2 由数据满足，不由特例保护满足。

**（b）组间 `score()`（α=0.7，bonus=0）：**

| | 今天 | 改后 | |
|---|---|---|---|
| 中转（单样本 3150/3018/14） | `0.7×132 + 14 = 106.4` | `0.7×3136 + 14 = 2209.2` | 恒等式校验：`0.7×3150 + 0.3×14 = 2209.2` ✔ |
| 直连（单样本 157.5/1.79/1.04） | `0.7×155.71 + 1.04 = 110.04` | `0.7×156.46 + 1.04 = 110.56` | **Δ = +0.52ms** |
| 中转（稳态中位） | **106.4**（更优，错） | **219.8** | |
| 直连（稳态中位） | 110.04 | **110.56**（更优） | |

- 今天中转以 3.6ms 领先；改后直连以 **109.2ms** 领先。方向反转。
- 对照现实：实测 rtt 中位 中转 307.9 vs 直连 217.2；p90 中转 1379 vs 直连 312.8。新排序与现实一致，旧排序与现实相反。
- `staticBound(中转) = 0.7 × 294 = 205.8ms > 直连 score 110.56ms` → 中转在竞速里被 `race.viable()`（`smart_decision.go:264`）直接剪掉，**连带省掉每次竞速为它付的等待延迟**。

**（c）`waitUntil` 免费变准。** `smart_decision.go:286-297` 的模型是「答复在 `local+remote` 到达」。新定义下 `local + remote = RoundTrip` 逐条精确，所以推导成立。`collectStragglers`（`smart_race.go:483-497`）要补的洞从「代理自己的 DNS + 整个中间跳」缩小成「只剩出口的 DNS」。宽限期保留（DNS 那部分的洞是真的），只改注释。

### 3.4 竞速路径：本轮 near + 成员 chain

`smart_race.go:302-306` 与 `:523-527` 今天写的是 `outcome.measurement.Local()`。改成：

```go
if outcome.measurement.HasSpan {
	chain, _ := member.chain()               // 成员级估计量，未知按 0
	leg := outcome.measurement.NearHop() + chain
	local[outcome.tag] = leg
	state.refreshLocal(outcome.tag, leg)
}
```

**为什么 near 用本轮样本、chain 用窗口值。** `refreshLocal` 的契约（`smart_decision.go:210-216`）是「同轮测量才可比」，而它要挡的是「冷池导致的单轮偏差」—— 那是**客户端这一侧**的现象，near 用本轮样本正好满足。chain 反过来：它是成员属性，单样本里混着中转到出口的冷握手，而竞速恰好是首次访问、也恰好是那条会话最可能冷的时刻。用一个冷握手在决定这个目标归属的那一轮把中转打死，是不对的。**这是一次契约细化，要在注释里写明，不能当 no-op 报告。**

（实现细节：`probe()` 的 `member` 在作用域内，`collectStragglers` 不在 —— 需要把 leg 算好放进 `probeOutcome`，见 §6。）

---

## 4. 探测层

### 4.1 三种探测的分工

| 探测 | 目标 | 覆盖 | 喂哪些窗 | 失败是否判死 |
|---|---|---|---|---|
| 常规心跳（**未分类为中转**的成员） | `<随机>.probe.arpa` | 仅 near | near / setup / roundTrip / 健康位。**不喂 chain** | 是（同今天） |
| 常规心跳（**已分类为中转**的成员） | `1.1.1.1:443`（拒绝则改问 `9.9.9.9:443`） | near + chain + remote | 全部 | 是（同今天） |
| 确认探测 / 复活探测 | 同上，两个地址 | 全链 | 全部 | 是（同今天，但见 §5.2） |

后两行合并成同一段代码：`probeEach` 已经接受 `destination func(*smartMember) M.Socksaddr`（`smart_heartbeat.go:98`），所以只要换掉 `keepWarm`：

```go
// smart_heartbeat.go:60
func (s *Smart) probeMembers(ctx context.Context, members []*smartMember) {
	s.probeEach(ctx, members, s.probeTarget)
}

// probeTarget picks what a routine heartbeat has to reach, from what this
// member has been measured to be.
//
// A member with no interior has nothing a real address could tell us that the
// probe address cannot, so it keeps the probe address and keeps emitting
// nothing. A member with one has two things only a real dial can settle: how
// much that interior currently costs, and whether it is there at all — a relay
// whose own upstream has died answers its probe address perfectly, every time.
func (s *Smart) probeTarget(member *smartMember) M.Socksaddr {
	if member.chained() {
		return reachabilityDestination
	}
	return naive.ProbeDestination()
}
```

`confirmMembers`（`:66-68`）保持独立 —— 它和常规心跳的区别不在目标，在语义（它永远跟在一次已经发生的真实失败后面）。`heartbeatRound`（`:191-217`）的 healthy/down 二分**不动**。

**直连成员的稳态行为、开销、语义逐字不变。** `smart_heartbeat_test.go:130-153`（`TestAHealthyIdleMemberIsKeptWarmWithoutEmittingTraffic`）那个不变量对直连成员原样成立 —— fixture 里 `strandedOutbound` 对 `.probe.arpa` 回的样本 chain=0，`chained()` 恒为 false。

### 4.2 「是不是中转」怎么判：现算，带滞回

```go
// chainProbeFloor and chainProbeCeiling classify a member by the interior it
// has been measured to have, with a gap between them so a member sitting on the
// line does not alternate between two kinds of heartbeat.
//
// The floor is set from production: direct members put the median of their
// span−remote at 0.5ms, the 90th percentile at 1.3ms and the largest of two
// hours at 4.4ms. Twenty milliseconds is more than four times the worst of
// those, and it is also below the 39.5ms at which this member's interior starts
// changing which member a group uses — so nothing that can move a selection
// goes unmeasured.
const (
	chainProbeFloor   = 20 * time.Millisecond
	chainProbeCeiling = 10 * time.Millisecond
)

func (m *smartMember) chained() bool {
	chain, measured := m.chain()
	if !measured {
		return m.chainedFlag.Load()
	}
	if chain > chainProbeFloor {
		m.chainedFlag.Store(true)
	} else if chain < chainProbeCeiling {
		m.chainedFlag.Store(false)
	}
	return m.chainedFlag.Load()
}
```

三个要点：

1. **用窗口值（已经丢掉单个最大值），不用单样本。** 一次冷 DNS（约 50ms）不可能把直连成员误判成中转。
2. **20ms 而不是 5ms。** 5ms 压在这个仓库自己的生产夹具上：`smart_race_test.go:525-548` 那个「诚实中转」的 chain = `62787 − 57773 = 5014µs`，比 5ms 高 14 微秒。20ms 把它正确地归到「没有值得测的内部」那一侧。
3. **滞回 20/10ms**，避免在边界上来回摆导致 chain 样本源断续。

### 4.3 冷启动：不加 bootstrap 探测

`chained()` 读 chain 窗，chain 窗只由真实流量和真实地址探测喂，真实地址探测只发给 `chained()` 为真的成员 —— 这看起来是个环。它被两件事拆开：

1. **`resetPath()` 不清 chain 窗（§2.7）。** 所以一个成员一旦被分类，本进程内不会退回未分类状态，移动端换网也不会。
2. **第一条真实连接或第一次竞速就产出 chain 样本。** 竞速在每个目标首次访问时发生，所以任何承载过流量的成员在秒级内被分类。

**明确不做 warmup 首遍改真实地址。** `fold` 提了这条，但它有两个真实代价：(a) `probeMember` 在 dial 失败时直接 `markDown`（`smart_heartbeat.go:264-267`），任何一个到 1.1.1.1:80 不通的部署会在启动时把**所有成员判死**，而 warmup 第二遍只给健康成员填池（`:42-44`），池全冷，心跳复活又走同一条路 —— 永久判死；(b) 它让每个直连成员在每次进程启动时向外发一个包，破坏约束 2 的字面承诺，且构成一个相当好认的启动指纹。

**残留缺口（明说）：** 一个从来不承载流量、也从没参加过竞速的中转，会一直用 `.probe.arpa` 心跳，它的上游断裂在被首次使用之前看不见 —— 与今天一致，无回归。而一旦它被 `current()` 选中，第一条真实连接就会写 chain 窗并把它纠正回来，代价是一条连接。这个窗口是有界的。

### 4.4 可观测特征的缓解，以及 `.probe.arpa` 的既有价值

`.probe.arpa` 的三条价值（`inbound.go:27-36`、`smart_heartbeat.go:70-78`）**对未分类为中转的成员逐字保留**：不外发流量、可高频跑、回来的往返纯粹是接入段。而且它的价值在新模型下**上升**了：它是唯一一个 `span` 恒等于 0、因而 near 完全不依赖服务端自报数字的读数 —— 从「唯一的心跳」升级成「接入段的黄金标准读数」。

对已分类为中转的成员，缓解按价值排序：

1. **结构性缓解（最重要）**：只有中转成员会发，且 `dueForHeartbeat`（`smart_heartbeat.go:234-249`）已经跳过近期承载流量的成员 —— 忙碌的中转从真实流量拿 chain 样本，一次探测都不发。所以周期性真实探测只出现在**被冷落的中转**上，而那恰好是最需要知道它好没好的成员。**单跳部署下游看到的东西完全没变。**
2. **时间抖动已有**：`nextHeartbeat()` 是 60s ± 20s（`smart.go:59/69`、`smart_heartbeat.go:180-182`），不动。
3. **保留 IP 字面量**：理由沿用 `smart_heartbeat.go:87-88`（代理侧坏掉的解析器不能把结论带偏），**外加一条新理由**：IP 字面量让出口不做 DNS，所以空闲中转的 chain 样本流天生不含 DNS 污染。
4. **端口从 `:80` 改成 `:443`**（可选，建议做）。我们从不发 payload，连上立刻 `Close()`（`smart_heartbeat.go:269`）。出口看到「一次到大厂 anycast 地址 443 端口的裸 TCP connect 后立即断开」，和每台设备每天做几百次的中断 TLS 尝试无法区分；`:80` 上的裸 connect 反而更扎眼。

---

## 5. 健康层

**中心判据：链路慢只降分，链路断才判死。** 今天在「健康」和「判死」之间没有中间态，所以一条间歇丢包的中间跳只能表达成反复超时 → 反复判死 → `.probe.arpa` 心跳被中转自答 → 立刻复活 → 再判死，就是实测的每 21 秒一次翻转。chain 项给了这个中间态，**而且不新增任何状态机**。

### 5.1 三段 × 两种烈度

| 段 | 断（不通） | 判据与动作 | 慢 / 丢包 | 判据与动作 |
|---|---|---|---|---|
| **A** 客户端→中转 | 拨号失败 / `errLegTimedOut` | **判死成员**。`reportFailure`（`smart.go:807-845`）→ `confirmDown`。**不动** | `near` 上升 | **只降分**。`local()` → `score()` / `current()`。**不动** |
| **B** 中转→出口 | 中转回 503 → `ErrNextHopUnreachable`（`lazy.go:80-85` + `outbound.go:389-395`） | **判死成员**。已经正确；空闲成员的可见性由 §4.1 的真实地址心跳补上 | `chain` 上升 | **只降分，绝不判死。** 新增的 chainWindow → `local()` → 评分。**这是本设计的主体** |
| **C** 出口→目标 | 出口回 502 → `ErrDestinationUnreachable` | **只冷却该目标**，递增退避。`entry.block` + `cooldownAfter`（`smart_cache.go:504-545`）。**不动** | `remote` 中位上升 | **只降该目标该组的分**。**不动** |

**「B 软劣化只降分不判死」是关键判据**，因为今天正是它反过来了。**评分修好之后，选择在失败之前就已经离开了中转，翻转的源头消失。**

**结构保证（必须在注释里写死）：** `awaitReady`（`smart_race.go:100-131`）只等「客户端到第一跳的连接建立」，`WaitReady` 的契约（`measure.go:134-139`）明写「只这一部分被客户端到代理这条腿界定」。**所以 chain 再慢也永远不会触发 `errLegTimedOut`。** 这是「B 劣化不判死」能成立的结构保证，不需要任何改动，但必须写死，否则后人会把 `awaitReady` 的边界挪到 CONNECT 响应上而毁掉它。

### 5.2 单个探测地址扛不住「判死」这个结论 —— 独立的真 bug，先修

`smart_heartbeat.go:312-319` 的 `markDown` 对任何非 `context.Canceled` 的错误都判死。而 `proveReachable` 只打一个地址：出口若封了 Cloudflare（供应商防火墙、出口侧规则集），最内层永远回 502 → `classifyHandshakeError`（`outbound.go:387-395`）→ `ErrDestinationUnreachable` → `markDown` → 该成员进 `dueForHeartbeat` 的 down 分支 → 又被 `confirmMembers` 打同一个地址 → 又 502。`current()`（`smart_member.go:349`）跳过不健康成员，**在多成员组里这个成员永久出局，没有任何路径能复活它**。

**本设计最初的修法是「`markDown` 忽略 `ErrDestinationUnreachable`」，那个修法错了，见 §9 修订 5。** 豁免 502 意味着一个出口整条出网断掉、对每个目标都回 502 的成员**永远判不死**，在单组双成员配置里它会一直被 `current()` 选中，把同组健康成员的流量全吃掉。哪一种是哪一种，从这一次拒绝里读不出来。

真正的修法是让**两个互相独立的真实地址**来分辨，两个都问过才判死：

```go
var reachabilityDestinations = [2]M.Socksaddr{
	M.ParseSocksaddr("1.1.1.1:443"),
	M.ParseSocksaddr("9.9.9.9:443"),
}
```

`probeMember` 的裁决顺序：

1. `ErrNextHopUnreachable`（503）—— 中转自己说它的下一跳没了，这已经是关于成员的判决，直接判死，第二个地址加不上任何东西。
2. 目标不是真实地址（自答的 `.probe.arpa`）—— 失败里不含代理之外的任何东西，按既有语义判死。
3. 其余（502、超时、黑洞）—— 换另一个真实地址再问一次。它答了，说明前一次是关于那个地址的；它也不答，才判死。

残留（明说）：一个把两个地址都封掉的出口会在健康时被判死。这比封掉其中任一个要小得多的靶子，而且失败方向是「把流量从一个我们无法确认的节点上挪开」。

**这条与本设计其余部分无关，应当单独提交、先行上线。**

### 5.3 探测预算必须按段给 —— 不改会造出新的抖动

`probeTimeout`（`smart_heartbeat.go:300-310`）今天是 `local × 8 + 2s`。改后：

```go
func (s *Smart) probeTimeout(member *smartMember, destination M.Socksaddr) time.Duration {
	var (
		leg      time.Duration
		measured bool
	)
	if naive.IsProbe(destination) {
		// Nothing is dialed for it: the budget bounds the near hop and nothing
		// else. Charging a chain-degraded member's interior to a probe that
		// never traverses it would blunt exactly the blackhole detection this
		// bound exists for.
		leg, measured = member.near()
	} else {
		leg, measured = member.local()
	}
	if !measured {
		return probeTimeoutCold
	}
	// ...:305-309 不变...
}
```

**这两处必须与测量层同一批上线。** 如果先把中转的心跳改成真实地址而 `local` 还是 near-only：预算 = `132 × 8 + 2000 = 3.06s`，而那条真实连接花了 **3.15s** → 探测超时 → `markDown` → 翻转比今天更糟。改后 `local = 294ms` → 预算 `4.35s`，且随 chain 劣化自动放宽。

同理，`readyTimeout()`（`smart_member.go:198-214`）的 `hasLocal` 回退分支**必须改读 near 窗**。它界定的是「连到代理」这一段（注释 `:196-197` 自己写的「setup 正是被界定的那个量」），中间跳不在里面。**今天它读的 `m.window` 就是 near，所以今天没有 bug**；但当 `local()` 语义拓宽后不改就变成 bug —— 一个 chain 劣化的成员会拿到过宽的黑洞检测预算。

### 5.4 间歇劣化的第二检测器（P3）

截尾均值对「纯丢包、基础时延不变」有下限（§2.3）。补一个不新增机制的检测器：让既有的 `observeScore`（`smart_race.go:826-885`）逐条比较，并做归因。

今天它的 `now` 用的是**窗口化的 local + 原始的 remote**，两把不同的尺子。改成两侧都用逐条量：

```go
// smart_race.go:869-871
was := score(s.alpha, recordedLocal, recordedRemote, 0)
now := score(s.alpha, measurement.NearHop()+liveChain, measurement.RemoteDial, 0)
```

其中 `liveChain` 取本次样本的 `ChainSpan()`（有值时）否则取成员窗口值。于是：

- 那条 3150ms 的连接：`was = 0.7×140.6 + 14 = 112.4`，`now = 0.7×3136 + 14 = 2209.2`，比值 19.7 ≥ `anomalyFactor = 2`。
- `anomalyStreak = 3` 提供平滑，`resetAnomalies()`（`:868-871`）保证需要真正连续的坏运。

~~并且要把确认过的证据写进成员的 chain 窗（`retireChain`）~~ —— **这一条推理错了，实现时删掉了，见 §9 修订 2。** `record` 在 `observeScore` 之前就跑（`smart_replay.go:179-181`），chain 窗由真实流量喂而不像 remote 只由竞速喂，所以攒满连击时窗里已经有三条坏样本，截尾均值早就动了。

归因规则（决定这次确认是记在成员头上还是目标头上）：

```go
// member 由调用方传入，不是 group.current()——见 §9 修订 3。
heldChain, _ := member.chain()
interiorMoved := hasLiveChain && heldChain > 0 &&
	liveChain > heldChain*anomalyFactor &&
	liveChain-heldChain > switchHysteresis        // 绝对下限，见 §9 修订 4
if !interiorMoved {
	entry.noteDegradation(group.tag, measurement.RemoteDial)  // C 段：现状不变
}
// B 段无需动作：那些读数已经由 record 逐条进过成员的 chain 窗。
```

**为什么这个归因是干净的：** `chain = span − remote`，客户端上行拥塞会同时抬高 `RoundTrip` 和 `near`，但**对 chain 完全没有影响**。所以「客户端自己网络变差 → 全体成员被 retire」这条误伤路径在构造上就不存在。这是三段分解白拿的一个性质。

`noteDegradation`（`smart_cache.go:406-414`）保持原样。chain 劣化时 `RemoteDial` 没变，`retire` 的 `sample <= w[lowest]` 守卫会让它什么都不做 —— 这是对的（目标段确实没变），而 B 段的证据由 `record` 逐条写进 chain 窗。

---

## 6. 逐文件改动清单

### `protocol/naive/measure.go`
- **:39-51** 删除 `Local()`；新增 `NearHop() time.Duration`（算术与今天的 `Local()` 逐位相同）与 `ChainSpan() (time.Duration, bool)`。
- **:8-37** 类型注释从两段改三段，写出恒等式 `NearHop + ChainSpan + RemoteDial = RoundTrip`，并**删掉 :39-41 那句反了的推理**。
- **:59-98** `Validated()` 逻辑不改；注释补一句 §3.2 的信任边界：一贯的 span 误报对排序中性，间歇的仍有 `−α·δ` 收益且由 near 的握手地板夹住；剩下唯一的谎是低报 `RemoteDial`，值 `(1−α)` 倍。

### `protocol/naive/connect_ack.go` / `lazy.go` / `inbound.go` / `common/dialer/connect_timing.go`
**全部不动。** 尤其 `inbound.go:203-211` 的 `.probe.arpa` 自答分支原样保留。在 PR 描述里明写这一点：中间跳 = `Total − Connect`，wire 已经够用。

### `protocol/naive/outbound.go`
- **:332-338** 调试行的 `local_us=` 换成 `near_us=` + `chain_us=`。这次诊断需要人工把 rtt/span/remote 拿计算器减 —— 加这两个字段以后一行就能读出三段。

### `protocol/group/smart_decision.go`
- **:345-377 旁** 新增 `rollingCost` 类型（§2.2 的完整代码与注释）。`rollingMin` **一行不动**，两段注释互相点名。
- **:8-22** 文件头模型注释：两段改三段，写清三种估计量各自的理由。
- **:51-64** `score` 的注释补上 `α·RT + (1−α)·remote` 的恒等式与 §3.2 的信任性质（含「估计量层只是近似」的限定）。
- **:83-89** `candidate.local` 的注释改成「客户端到最内层代理」；新增 `chain time.Duration` 字段（仅供日志/审计，决策不读）。
- **:210-225** `refreshLocal` 的注释按 §3.4 细化契约：near 取同轮，chain 取成员窗，并说明为什么。
- **:286-297** `waitUntil` 的注释：模型现在精确，`collectStragglers` 要补的洞缩小到只剩出口的 DNS。
- **:62-72、:128-180、:305-343** 代码全部不改。

### `protocol/group/smart_member.go`
- **:93-129** 新增 `chainWindow rollingCost`、`chainedFlag atomic.Bool`。`window` 不改名（它一直就是 near）。
- **:160-164** `local()` 改为 `near + chain`（§2.6）；新增 `near()`、`chain()`。（`retireChain()` 已否决，见 §9 修订 2。）
- **:166-185** `resetPath()`：**不清 chainWindow**，加 §2.7 的注释解释为什么这一条与其它三个窗不同。
- **:198-214** `readyTimeout()` 的 `hasLocal` 回退分支改读 `m.window`（near 窗）—— 代码上就是保持 `m.window.value()` 不变，但要加一行注释锁死意图，因为 `local()` 的语义已经拓宽。
- **:216-228** `Smart.record` / `Smart.recordProbe`：`recordProbe` 增加 `destination M.Socksaddr` 形参。
- **:268-289** `member.record` / `member.recordProbe` 各增加 `traversed bool` 透传。
- **:291-326** `recordMeasurement` 按 §2.5 改。
- **:256-266** `localFloorLocked` 代码不动，注释补一句「它界定的是第一跳」。
- **:337-381** `current()` **代码不动，含义变宽** —— 这就是用户要的组内切换。
- 新增 `chained()`（§4.2）。

### `protocol/group/smart_heartbeat.go`
- **:60-62** `probeMembers` 改用 `s.probeTarget`；新增 `probeTarget`（§4.1）。
- **:66-68** `confirmMembers` 不动。
- **:70-96** `keepWarm` / `proveReachable` 的注释补上 §4.1 的分工与 §4.4 的理由；`reachabilityDestination` 端口 `:80` → `:443`。
- **:98-123** `probeEach` 不动。
- **:191-217** `heartbeatRound` 的 healthy/down 二分不动，`split` 保留。
- **:254-296** `probeMember`：`s.probeTimeout(member, destination)`；`s.recordProbe(member, destination, measurement)`。
- **:300-310** `probeTimeout` 按 §5.3 改签名与实现。
- **:312-319** `markDown` 按 §5.2 改（**P0，独立提交**）。
- **:30-51** `warmup` **不动**（§4.3）。
- 新增常量 `chainProbeFloor = 20ms`、`chainProbeCeiling = 10ms`。

### `protocol/group/smart_race.go`
- **:25-30** `probeOutcome` 新增 `leg time.Duration; hasLeg bool`，在 `probe()` 内（member 在作用域里）按 §3.4 填。
- **:60-62** `s.record(member, measurement)` → `s.record(member, measurement)`（`record` 恒 traversed=true，签名不变）。
- **:302-306 / :523-527** 改用 `outcome.leg / outcome.hasLeg`，删掉两处 `measurement.Local()`。
- **:144-190** `raceReport` 新增 `chain map[string]time.Duration`，`String()` 打 `chain=`。**这是可运维性收益最高的一条** —— 任务描述里那整段诊断本来一行日志就该给出。
- **:826-885** `observeScore`：P2 阶段代码不动（chain 劣化经由 `liveLocal` 进入，`anomalyFactor=2`/`anomalyStreak=3` 自动触发重赛），只补注释；P3 按 §5.4 改。
- **:100-131** `awaitReady` 不动，注释补上 §5.1 的结构保证。
- **:483-497** `collectStragglers` 注释更新（洞缩小到只剩出口的 DNS，宽限期保留）。

### `protocol/group/smart_cache.go` —— 唯一的兼容性动作
- **:231-235** `Local map[string]time.Duration \`json:"local,omitempty"\`` → **`Path map[string]time.Duration \`json:"path,omitempty"\``**，Go 字段与 JSON key 一起改名，注释改成「客户端到最内层代理」。
- **:324-337** `localFor` → `pathFor`；`legsFor` 读 `Path`。
- **:596-607** `carryIncumbent` 读写 `Path`。

**为什么必须改 key，而不是「类型没变所以零影响」：**

旧盘上的值是旧语义（只有 near）。它有两个读者，两个都会出事：

1. `candidates()`（`smart.go:385-391`）在成员还没有活测量时回落到它 —— 链式组在重启后的那个窗口里拿到漏掉中间跳的 local，原 bug 原样复现。
2. `legsFor` → `observeScore`（`smart_race.go:850-874`），而 `observeScore` 在 `smart_replay.go:180-182` 上**每条落定的连接都调一次**。升级后 `recordedLocal` 是旧语义（中转 132），`liveLocal` 是新语义（294），`now ≥ was × 2` 立刻成立，3 条连接攒满 → `noteDegradation` + `raceDetached`。**所有以中转组为选中组的缓存目标在升级后几分钟内全部重赛** —— 正是持久化存在的意义被一次更新交付掉。

改 key 之后：旧行的 `Path` 缺失 → `pathFor` 返回 false、`legsFor` 返回 `ok=false` → `observeScore` 提前 return，`Remote` 窗口**一个都不动、一个目标都不重赛**。`preload`（`smart_cache.go:803-822`）对整条 unmarshal 失败才丢弃，加/改字段触发不了。降级方向：旧二进制忽略未知的 `path` 键，把 `local` 读成缺失，同样安全。

**代价（明说，路径已按实测更正——见 §9 修订 6）：** 升级后每个目标的第一轮 `carryIncumbent`（`:596-601` 要求 `hasLocal`）不携带，一轮之后恢复。而命中缓存却打不出分的目标，走的**不是** `fallbackGroup` 的配置顺序：`dialTCP`（`smart.go:633`）直接调 `selectGroup`，拿到空串后落到 `raceOrWait`（`:648`），也就是每个目标各跑一轮竞速——单飞、`raceConcurrency=8` 封顶，只有溢出的部分才落到 `dialWithoutRacing`/`fallbackGroup`。`Remote` 窗按 `extend` 续写不清空，`Selected` 由 incumbent 锚住，所以这一轮既不丢数据也不换出口。两者都是秒级、有界。

### `protocol/group/smart.go`
- **:364-396** `candidates()` 不动（`member.local()` 自动继承新语义），新增 `c.chain, _ = member.chain()` 供日志。
- **:807-845** `reportFailure` **零改动**，加注释说明「B 段慢由分数处理，这里只处理断」。
- **:903-917** `InterfaceUpdated` 的 `go s.probeMembers(...)` 现在走 `probeTarget`，中转成员自动拿到真实地址探测 —— 配合 §2.7 的「chain 窗跨 resetPath 存活」，换网后分类不丢失。**这两条必须成对，缺一个就是环。**
- **:109-119** `localFloorDivisor` 注释：它界定的是第一跳。

### `protocol/group/smart_audit.go`
- **:196-230** `auditLeg` 新增 `Chain string \`json:"chain,omitempty"\``。**`Local` 键名保留** —— 它一直就指「客户端到最内层代理」，现在才是名副其实；而且 `cmd/sing-box/cmd_smart.go:55-58` 读它，改名会静默破坏那个命令。（与缓存不同：审计只写不回读比较，缓存要拿旧值做数值比较，所以两者的处理方式不同，这个不对称要写进注释。）
- **:350-380** `legsFromCandidates`、**:465-492** `stateLegs`、**:277-321** `auditUsage`：填 `Chain`。

### `cmd/sing-box/cmd_smart.go`
**不改。** `encoding/json` 忽略新增的 `chain` 键。可选增强：节点列表加一列 chain —— 「哪个节点的链内段坏了」现在是个可回答的问题。

### 文档 `docs/configuration/outbound/smart.zh.md` / `smart.md`
- **:29** 两段模型改三段，写出恒等式；**明确说明这是让代码兑现文档一直写着的语义**。
- **:36** 公式旁补 `= α × 往返 + (1−α) × remote` 的恒等式，以及 §1.2 的「今天中间跳的系数是 −α」。
- **:47-53「节点的死活怎么判」** 按 §4.1 的表重写，说清「直连部署下什么都没变」。
- **:55-64「排序信任谁」** 按 §3.2 重写，含「估计量层只是近似」的限定。
- **:70** remote 中位窗那一段旁边，新增 chain 截尾均值窗的说明与「为什么这里不用 min / 不用中位」。
- **:93-101 alpha** 默认值不变，补一句它现在折扣的是「到最后一跳之前的全部路径」。
- 新增一段：链路慢只降分不判死，及其理由。

### 测试影响

**必须改的（两条）：**
- `smart_group_test.go:501` `require.Equal(t, 195976µs, relay.Local())` → `relay.NearHop()`。测试本意（`Validated` 整份丢弃）不变。
- `smart_heartbeat_test.go` 新增对称用例：已分类为中转的健康成员，心跳必须打非 probe 地址。

**已验算不会红的：**
- `smart_group_test.go:22-24` `measured(local)` → span=0、remote=0 → near=local、chain=0 → `local()` 不变。
- `smart_race_test.go:116-125` `answering(local, remote)` 令 `ServerSpan == RemoteDial` → chain=0 → **全部竞速测试不变**。
- `smart_race_test.go:524-548` `TestAnHonestRelayWinsTheDestinationItIsNearest`：us chain = `62787 − 57773 = 5014µs`，窗口 `[0, 5014]` 未到 quorum 取最新 = 5.014ms，local = 138.232ms，score = `0.7×138.232 + 57.773 − 25 = ` **129.5ms**（原 126.0）；jp chain = `168787 − 168625 = 162µs`，local = 80.578ms，score = `0.7×80.578 + 168.625 − 40 = ` **185.0ms**（原 184.9）。**us 仍胜，测试原样通过。**
- `smart_race_test.go:507-523` `TestARelayThatUnderstatesItsSpanIsNotRanked`：`Validated()` 整份作废，不变。
- `smart_group_test.go:442-466`（握手地板）、`smart_audit_test.go`：地板作用于 near，`HasRemote=false` → chain 未知按 0 → 数值全不变。
- `smart_heartbeat_test.go:130-153`：fixture 的 chain 恒为 0 → `chained()` 为 false → 只打 `.probe.arpa`，原样通过。
- `smart_decision_test.go:366-397` 三个 `rollingMin` 测试：`rollingMin` 一行不动 → 全部不变。

**新增（沿用本仓库「用真实生产数字建 fixture」的风格）：**
1. `naive`：`NearHop()` / `ChainSpan()` 的四种输入 —— 正常、`!HasSpan`、`!HasRemote`、容差内的负值；并断言 `near + chain + remote = RoundTrip`。
2. `rollingCost` vs `rollingMin`：同一个 25% 丢包的窗口序列，断言 `rollingMin` 读到快通道值、`rollingCost` 显著大于它。**把约束 4 钉成回归测试。**
3. `.probe.arpa` 样本不进 chain 窗：喂 20 次心跳形状的样本后，`chained()` 仍为 false、chain 窗仍为空。
4. **本次事故的端到端回归**：一个分组两个成员、`select: fastest`，中转喂 `{3150, 3018, 14}`、直连喂 `{157.5, 1.79, 1.04}`，断言 `group.current().tag == "direct"`，**并断言中转未被判死**。注释里写上 `identity.ess.apple.com` 与 30 分钟 244 次失败。
5. 健康期不误伤：chain = 8.6ms 时不发生切换。
6. `resetPath()` 之后 chain 窗仍在、`chained()` 仍为 true。
7. `markDown` 对 `ErrDestinationUnreachable` 不判死。
8. 快照 JSON 往返：旧格式（有 `local`、无 `path`）能读、`Remote` 窗完整保留、`legsFor` 返回 `ok=false`。

---

## 7. 不做什么

1. **改 wire format 让每一跳单独上报自己那一段。** 中间跳 = `Total − Connect` 客户端一次减法就有（`connect_ack.go:27-31` 已同带两个值）。每一跳描述它没测量过的跳，`Validated()`（`measure.go:59-77`）的一致性校验对一个列表不成立；而客户端能做的动作只有「换成员」，拿不到能作用于逐跳归因的动作。诊断多级链的地方是中转自己的日志。

2. ~~**加 `;n=<dns-µs>` 把出口的 DNS 从 chain 里剔出去。**~~ —— **已启用，见 §9 修订 7。** 下面这段是当时不做的理由，保留以说明触发条件是什么、以及为什么后来判断它够了。 `ParseConnectAck`（`connect_ack.go:68-72`）对未知的 `;k=v` 字段直接 `continue`，所以这是一个**双向兼容、随时可加**的扩展 —— 正因为随时可加，现在不必加。生产实测直连成员的 `span − remote`（这个量整个就是 DNS + 转发开销）两小时中位 0.5ms、最大 4.4ms；单次冷解析约 50ms，7 个热样本旁边只把均值抬到 6.9ms，远低于 39.5ms 的切换阈值。**留作后门并写进注释，触发条件：如果某个部署里一个直连成员的 chain 估计值持续 > 20ms（即被误分类成中转），就加这个字段。**

3. **加 `;r=1` 标记中继身份。** 同样是双向兼容的加法，能替掉 20ms 阈值。不做的理由：`chain > 20ms` 用手上已有的数据就能分类，不需要链上两端同步部署；而且它在「中转与出口同机」时正确退化 —— 那时确实没有值得测的内部。若阈值被证明有歧义，这是首选补丁。

4. **给 chain 单开一个 `beta` 配置项。** β=α 是推出来的不是调出来的（§3.2）。多一个旋钮就多一份配错的可能，还要波及 `staticBound`、`waitUntil`、`raceReport`、文档、schema 五处。

5. **chain 用 min。** `P(8 样本的 min 落在快通道) = 1 − p⁸`，p=0.25 时 0.999985。对丢包率的敏感度指数级趋零，可证明看不见，任何窗口深度救不回来。加深窗口只会让它更瞎，方向完全反了。

6. **chain 用中位数（与 remote 保持风格一致）。** 25% 丢包时四分之三的样本还在好的那一模里，中位数完全失明。中位对 remote 是对的，因为 remote 的双峰是**同一个问题的两个真答案**（anycast 这轮 0.5ms 下轮 150ms，`smart_cache.go:130-140`）；chain 的双峰里有一模是故障，取中位等于把故障票投掉。

7. **chain 用不裁剪的均值。** 一次中转到出口的冷 TLS 握手（200–500ms）就能把一个健康中转推过 39.5ms 的切换阈值，踢出服务一整个窗口。而这类样本一个窗口最多一个（§2.2），丢一个正好。

8. **chain 存目标级（`destinationEntry`）。** chain 在最后一跳之前，与目标无关。存成每目标等于把同一个节点事实学最多 16384 遍，各自独立过期、各自独立陈旧 —— 而且落选组的 chain 永远不会被刷新，正是「落选组快照永不复测」那个已知病灶的复刻。chain 是成员属性，存在成员上。

9. **warmup 首遍改打真实地址。** 见 §4.3：会在任何封了 1.1.1.1 的部署里造成启动即全员永久判死，且给直连成员制造启动指纹。

10. **常规心跳全员改真实地址。** 直接违反约束 2/3。`.probe.arpa` 的三条价值对没有内部的成员是净收益且零成本。分工方案让单跳部署下游看到的东西完全不变。

11. **让中转把 `.probe.arpa` 转发给下一跳、由最内层自答。** 信息上是最优解（零外发还能看见中间跳），两个理由否掉，第二个是决定性的：
   - `inbound.go:203` 的自答分支在 `newConnection` **之前**，此刻 inbound 不知道这个目标的出站是不是另一个 naive 跳。要么把它挪到路由之后并教会每一个非 naive 出站认识 `.probe.arpa`，要么教会 router —— blast radius 远超问题本身。而且若中转的出站本身就是 smart 组，随机标签会一次探测创建一条缓存项，`destinationKey` 按 FQDN 分片，LRU 会被打穿。
   - **版本错配会把监控功能变成停机**：新中转 + 旧出口 → 中转转发 `.probe.arpa` → 旧出口不认得、去做真实 DNS 解析 → `.arpa` 保留域 NXDOMAIN → 502 → 中转被判死。链上两端分开部署（dmit 和 so 就是）是常态，这个坑必然踩。
   - **列为服务端后续项**：真做了的话，§4.1 的角色分工整个可以删掉。

12. **把探测超时的预算写回窗口（「截尾样本」）。** `budget = 8×local + 2s`，而 `local` 又含 chain：写回去就是 `chain' ≈ 1.44 × chain + …`，**每次连续超时把估计值乘 1.44**，同时把下一次的预算撑大。探测槽只有 4 个（`probeConcurrency`，`smart.go:83`），一个病态中转能把整轮心跳堵死。超时就什么都不写，下一次成功的样本自然带来真实的 chain。

13. **chain 超过阈值就判死成员。** 这就是抖动发生器：判死 → 心跳复活 → 再判死，实测每 21 秒一次。链路慢的中转确实在承载流量。

14. **在健康/判死之间加第三个状态（「降级」）。** 分数本身就是连续的降级表达，chain 窗由真实流量逐条喂养且自动恢复。加第三态要改 `firstHealthy`、`current()`、`dueForHeartbeat`、`split`、审计五处，换一个一行就能给的东西 —— 正是「机制重叠是这个代码库最大架构病」点名的那类改动。

15. **调大 `switchHysteresis` 来吸收新增项。** 新增项对直连只有 0.75ms，没有东西需要吸收；调大只会让真正该发生的切换更慢。

16. **给 near 也换尾部估计量（覆盖 A 段间歇丢包）。** 会改动每个直连成员的分数（违反约束 2），且 `rollingMin` 挡的那类客户端侧伪影是真的（`smart_member.go:242-249`）。**这是本次交付明确的缺口，见 §8 结尾。**

---

## 8. 分阶段实施

每一阶段都可独立提交、独立回滚、独立验证。

### P0 —— 与本设计无关的先行修复（独立提交，可立刻上）

| 改动 | 验证判据 |
|---|---|
| 探测改用两个互相独立的真实地址，两个都被拒才判死（`smart_heartbeat.go`，§5.2） | 单测：构造一个只对第一个地址回 502 的成员，跑一轮 `confirmMembers`，断言 `healthy` 仍为 true；再构造一个对两个地址都回 502 的成员，断言它被判死 |
| 修正 `measure.go:39-41`、`smart_decision.go:14-19` 的错误推理与 `smart.zh.md:29` 的未实现断言 | 无代码变更，评审通过 |
| 诊断可观测性：`outbound.go:337` 打 `near_us`/`chain_us`；`raceReport` 与 `auditLeg` 加 `chain` | 跑一次真实中转连接，**一行日志能读出三段**，不再需要计算器 |

P0 的第三条价值最高：它立刻把这次诊断从「人工减三个数」变成「读一行」，且零风险。

### P1 —— 测量层落地，**不进评分**

改动：`NearHop`/`ChainSpan`、`chainWindow`、`rollingCost`、`traversed` 门、`chained()`。`member.local()` **仍然返回 near**。

验证判据：
- 单测：三段恒等式；两条真实样本（3150/3018/14 与 157.5/1.79/1.04）逐位对上；`.probe.arpa` 样本不进 chain 窗；`rollingCost` 与 `rollingMin` 在 25% 丢包序列上判决相反。
- **灰度线上**：看 audit 里 chain 的分布是否复现给定的「中转 中位 8.6 / p90 1141」与「直连 中位 0.5 / p90 1.3 / max 4.4」。

**这是最重要的一个分期决定**：它让「chain 估计量选得对不对」在不冒改排序风险的前提下先被真实数据回答一次。如果线上分布和预期不符，只需要调 `rollingCost` 一个类型，不需要回滚任何决策逻辑。

### P2 —— 接入评分（本次事故的实际修复）

改动：`member.local() = near + chain`；缓存 JSON key `local` → `path`；竞速路径改「本轮 near + 成员 chain」；`probeTimeout` 按段给预算；`readyTimeout` 回退分支锁定 near 窗；心跳按角色选目标；`resetPath` 不清 chain 窗；`InterfaceUpdated` 走 `probeTarget`。

**这一批必须整体上线。** 拆开任何一条都会造成回归：只改探测不改测量 → 3.06s 预算杀死 3.15s 的连接；只改测量不改 `probeTimeout` → 同样；改了 `resetPath` 却不改 `InterfaceUpdated` → 换网后分类丢失。

验证判据：
- 单测 4/5/6（端到端回归、健康期不误伤、换网后分类保留）。
- 升级验证：拿一份旧格式快照文件启动，断言 `Remote` 窗全部保留、`observeScore` 在头 5 分钟内触发次数为 0。
- **线上（按优先级看）**：① 成员健康翻转次数（基线：30 分钟 84 次，目标 < 5 次）；② 中转成员承载的连接比例（基线：30 分钟 240 次，目标接近 0）；③ 直连成员的 score 变化（目标 ≤ 1ms）；④ 竞速 elapsed 分布（不应变差 —— 中转被 `staticBound` 剪掉，应当变好）。

### P3 —— 间歇劣化的第二检测器

改动：`observeScore` 逐条比较 + 归因（§5.4）。

验证判据：
- 单测：构造一个 25% 丢包的 chain 序列，断言 3 连击后组内选择改到直连；断言一次客户端上行拥塞（`RoundTrip` 与 `near` 同时抬高、`chain` 不变）**不会**把证据记到成员头上；断言一个小 chain 的比值波动**不会**吞掉一次真实的目标搬家。
- 线上：看 `re-racing` 日志的频次与归因分布是否合理，不应出现全成员同时 retire。

### 交付边界（明说，不藏在风险节里）

- **P0–P3 覆盖**：A 断、A 整体变慢、B 断、B 整体变慢、B 间歇丢包、C 断、C 变远。
- **不覆盖**：**A 段的纯间歇丢包**（客户端到中转 20% 丢包、连接照常成功只是偶尔卡一秒）。`near` 仍走 `rollingMin`，min 对它同样可证明失明 —— 我用同一把尺子量出了 B 的问题，就必须承认它对 A 一样成立。部分兜底：A 段的丢包比 B 段更容易升级成连接失败（它是 `awaitReady`/`errLegTimedOut` 界定的那条腿），从而被 `reportFailure` → `confirmDown` 抓到；而 B 段的重传发生在中转自己的拨号内部，对客户端**永远不表现为失败、只表现为慢** —— 这恰恰是 B 更隐蔽、更该先修的理由。
- **不覆盖**：内层跳不上报 connect 的中转（旧版出口、非 naive 内层）。`ChainSpan()` 恒 `ok=false`，chain 未知按 0，该成员在组内 `current()` 里仍被低估。这类成员同时也拿不到 `HasRemote`，所以 `probe()`（`smart_race.go:75-81`）会让它退出每一场竞速，跨组评分不受影响；受影响的只有组内选择。彻底解决需要 §7 第 3 条的 `;r=1` wire 标记。任务里的 dmit→so 拓扑两端都是 sing-box naive，不落在这个分支里。
---

## 9. 修订记录

本节记录设计文档写完之后，实施与审计过程中被推翻或改正的推理。原文保留在上面，凡与本节冲突的以本节为准。

**修订 1（审计后修正 · 严重）—— `rollingCost` 在样本不足时必须报「未测量」，而不是报最新样本。**
原文 §2.2 沿用了 `remoteWindow` 的「不足三条取最新」。这条规则的正当性建立在「窗里都是同一目标的可比读数」上，`chainWindow` 跨目标聚合，前提不成立。更要命的是这个窗的头两条样本不是随机的一对：心跳由第一跳自答、永不穿越链内段（`traversed` 闸门），所以第一个写进这个窗的东西**必然**是该成员的第一条真实连接——恰好是中转到出口的会话和出口自己的解析器同时冷着的那一刻。不截尾地报出去，这一条样本就整份进了 `local()`，能把整组切到更慢的成员；而 `switchHysteresis` 随后会在窗口早已恢复之后仍然不让它切回来。§4.2 要点 1「用窗口值（已经丢掉单个最大值），不用单样本」在前两条样本上并不成立。改为 `count < medianQuorum` 时返回 `(0, false)`，代价是每个成员一生前两条连接的 chain 盲区。回归测试：`TestACostWindowTooShortToTrimReportsNothing`、`TestOneColdInteriorCannotMoveASelectionOnItsOwn`。

**修订 2（实施中发现）—— `retireChain` 是多余的，已删。**
见 §5.4 的删除线。`record` 在 `observeScore` 之前就跑（`smart_replay.go:179-181`），chain 窗由真实流量喂，不像 `Remote` 只由竞速喂。攒满 `anomalyStreak = 3` 时窗里已经有三条坏样本，截尾均值只丢一条，早就动了。`retireChain` 与 `record` 的唯一实质差别是丢「最小」还是丢「最旧」，在这个量级上不改变任何决策。

**修订 3（审计后修正）—— `observeScore` 必须用承载这条连接的成员，不能问 `group.current()`。**
到这里的是一条连接的证词，要和**那条连接的成员**的窗口比。心跳在另一个 goroutine 上把承载成员判死之后，`current()` 会当场失效切换返回兄弟成员，于是比较变成「一个成员的实时链内段 vs 另一个成员的窗口」，归因反向，证据被记到没动的那条腿上。成员由 `smart_replay.go` 透传进来。顺带消掉了 `current()`（一个会改选并写审计的选举）出现在观测路径上的副作用。回归测试：`TestDriftIsWeighedAgainstTheMemberThatCarriedTheConnection`。

**修订 4（审计后修正）—— 归因判据除了比值还要有绝对下限。**
原文只写了 `liveChain > heldChain * anomalyFactor`。而自己拨号的成员，链内段中位 0.5ms、p90 1.3ms，同一个健康成员的两条普通样本之间已经差了两倍以上——在这个尺度上比值就是在对噪声开火，把一次真实的目标搬家判成「链内段动了」，于是唯一被允许写进快照的那一条实测读数被整块丢掉。加上 `liveChain - heldChain > switchHysteresis`：本文件别处用同一个常量表达的就是「低于它的量不足以改变任何决定」，那它也不可能是改变了这一次的那个量。回归测试：`TestASmallInteriorCannotSwallowADestinationThatMoved`。

**修订 5（审计后修正 · 严重）—— 豁免 502 是错的修法，改用两个独立地址。**
见 §5.2 全文。原修法把「一个地址被封 → 成员永久出局」换成了「出口整条出网断掉 → 成员永远判不死」，后者在单组双成员配置里会让那个坏成员一直被 `current()` 选中、吃掉全部流量。回归测试：`TestAFilteredProbeAddressDoesNotCondemnTheMember`（一个被拒、一个应答 → 不判死，且断言第二个地址真的被问了）、`TestAnExitThatReachesNothingIsCondemned`（两个都被拒 → 判死）。

**修订 6（审计后更正 · 仅文档）—— 升级窗口走的是 `raceOrWait`，不是 `fallbackGroup`。**
见 §6 缓存一节。实测无数据损坏、无出口切换，代价比原文写的还小。

**审计确认仍然成立的（复核过、不必再查）：** 三段恒等式在每条能进窗的路径上都成立且被 `ackTolerance` 界定；`chain` 不会为负；`.probe.arpa` 自答样本不进 chain 窗（§2.5 那条「最容易漏、漏了最致命」的闸门是对的）；回滚方向安全；§7「明确否决」16 条无一被偷偷做了；§3.3 的全部算术。

**上面这份清单里原有一条「`chainWindow` 不持久化无害」，已被修订 1 推翻，见修订 9。**

**修订 7（审计后补齐）—— 出口的 DNS 解析耗时不再计入 chain，§7 第 2 条的后门已启用。**
审计发现这一条窗口满了也会出事：8 条样本里有 4 条冷解析就让直连成员的 chain 到 21.9ms，越过 `chainProbeFloor` 也越过 `switchHysteresis`。§7 第 2 条给这个后门写的触发条件是「持续 > 20ms」，实测形态是突发而非持续，严格讲没到闸门；但两处标定注释自相矛盾（`smart_member.go` 说直连最坏 4.4ms，`rollingCost` 说冷解析约 50ms，不可能描述同一批样本）说明这条缝一直没被算进去，所以按原设计预留的方式加了 `;n=<resolve-µs>`。

**关键实现选择：解析耗时是被「挪进 remote」，不是「从 chain 里扣掉」。** 直接扣掉有两个后果，都不能接受：(a) 三段之和不再恒等于往返，而 §3.3(c) 的 `waitUntil` 推导正是建立在这个恒等式上；(b) 更严重的是信任面——`score = α·local + remote` 展开成 `α·RT + (1−α)·remote`，一个被**减去**的项会让报大它的一跳每微秒赚 α = 0.7，而唯一划算的谎（低报 `RemoteDial`）只值 (1−α) = 0.3。也就是说「直接扣掉」会引入一个比现有杠杆值钱 2.3 倍、且不会像 span 那样自我抵消的新谎。挪进 `remote` 之后：`local = RT − remote'`，`score = α·RT + (1−α)·remote'`。**客户端从头到尾只消费 `Connect + Resolve` 这个和**，所以 `n` 在经济上与 `d` 不可区分，这个字段没有给任何一跳新的自由度，`Validated()` 的上界（`remote ≤ span + tol`）也一字未改。§3.2 那条「一贯高报或低报 span 对排序影响为零」的性质完整保留。

**但「报大 `RemoteDial` 只会让自己吃亏」这句话有一个它不覆盖的角落，必须写明（本节初稿把它写成了无限定的性质，那是错的）：**

- **组间**比较走 `score() = α·local + remote`（`selectGroup`、竞速、`waitUntil`），报大 `RemoteDial` 每微秒亏 (1−α) = 0.3。这一半成立。
- **组内**比较走 `current()`（`smart_member.go`），它只比 `local()`，**没有 remote 项**。报大 `RemoteDial` 会把该成员的 `chain` 压低同样多（`chain = span − remote`），于是 `local()` 每微秒降 1 —— 系数是 1，不是 −0.3。收益上限是该成员真实的链内段（`ChainSpan()` 在 0 处钳位、`Validated()` 卡住 `remote ≤ span + tol`），而且 `localFloorLocked` 只兜 `near`、不兜 `chain`。

这个角落**是基线就有的**：在 221b0e047 上多报 `d=`（connect）逐字节同效、同上界。`n=` 没有把它变宽——反过来，把诚实的解析耗时从 chain 移走之后，直连出口能偷的空间反而变小了（它的真实链内段更小了）。所以这是一条**要写进信任边界文档**的既有事实，不是本次改动的回归。代价方向也值得说清楚：同一份报大会让该组在**组间**变差 0.3δ，所以它是「在组内抢 `current()`」的谎，不是「让全系统偏爱我」的谎。

**估计量层还有一条既有的不对称，一并记下：** 逐条连接的代数成立，但排序用的是三个不同的窗。间歇性报大 `RemoteDial`（δ，占比 p）会把那几条的 chain 样本**压低**，而 `rollingCost` 丢的是**最大值**、留下被压低的样本；对应被抬高的 remote 样本进 5 样本**中位**窗，离群高值往往不移动中位数。实算（p=0.2、δ=30ms）净赚约 3ms，p ≥ 0.5 时转为净亏。**这同样是基线既有的**（多报 `d=` 完全复现），与 `n=` 无关。若将来要收口，方向是让 `rollingCost` 两头各去掉一个（丢最大的理由不变，丢最小同时堵死「间歇压低」与「一条走运样本」），但那会改变每个成员的 chain 估计，属于独立决策。

透传与兼容：`n=` 由最内层跳上报，中转按 `d=` 同一条路（`RecordInnerResolution`）原样透传；`ParseConnectAck` 对未知 `;k=v` 本来就跳过，所以新旧两端任意组合都能跑。出口太旧不发 `n=` 时，解析耗时留在 chain 里，即今天的行为——这个半升级组合的代价写进了 `chainProbeFloor` 的注释，也有测试钉住（`TestAColdLookupAtTheExitIsNotTheMembersInterior` 的后半段）。

**修订 8（Fable 复审后修正 · 严重）—— 双地址交叉验证对「静默丢包」原本不生效。**
`classifyHandshakeError`（`protocol/naive/outbound.go`）把一切非 502 的失败都归到 `ErrNextHopUnreachable`，**包括超时和黑洞**——这对真实流量是对的（未分类的错误绝不能读成关于目标的判决），但 `probeMember` 拿它当「代理明确说了自己的下一跳没了」来短路判死，短路发生在问第二个地址**之前**。于是修订 5 声称能分辨的两种情况里，只有「快速拒绝（502）」真的被交叉验证过；而它自己点名要防的 null route / 静默丢弃，走的正是短路那条路——出口黑洞掉第一个地址 → 探测超时 → 当场判死 → down 半边用同一个地址复探 → 永久出局。提交信息与中英文用户文档里「判死前两个地址都会被问过」这句话，当时是假的。

修法：`protocol/naive/lazy.go` 新增哨兵 `ErrProxyAnswered`，由 `classifyHandshakeError` 在**确实收到状态码**（`*cronet.HandshakeError` 在错误链里）时一并包上；`probeMember` 的短路条件改成 `ErrNextHopUnreachable && ErrProxyAnswered`。沉默从此落进交叉验证，出口真死时第二个地址同样失败，仍然判死，代价只是一份探测预算。测试双件也补齐了形态：原有的两个都是即时返回错误，新增 `blackholingConn`（阻塞到 `ctx.Done()`），回归测试 `TestASilentlyDroppedProbeAddressIsCrossCheckedToo` 与 `TestAProxyThatSaysItsNextHopIsGoneIsNotCrossChecked`。用户文档两版同步改成「唯一不交叉验证的是代理明确回答下一跳没了」。

**修订 9（Fable 复审后修正）—— 竞速对空 chain 窗改用本轮实测，并更正一条已失效的审计结论。**
修订 1 之后，`member.chain()` 在样本不足三条时返回未测量，而 `smart_race.go` 的竞速路径原本 `chain, _ := member.chain()` 丢弃 ok、按 0 参赛——于是中转在自己一生的头两条穿链连接里是**按单跳报价**的，失败方向从保守翻成乐观。改为空窗时回退到本轮的 `ChainSpan()`：这是这条连接真实付出的代价，是保守方向，而且经修订 7 之后它已经不含出口的解析耗时，正是当初「单样本太吵不能用」的那个理由已经消失。影响有界（实时 `local()` 每次拨号重排，三条穿链样本后自愈），Fable 复核确认原报告「被快照钉住数小时」不成立。

同时更正：§9 原「审计确认仍然成立」清单里的「`chainWindow` 不持久化无害」已被修订 1 推翻——它的论据「重启后第一场竞速自己就把窗灌上了」只在旧的「一条样本即报数」规则下成立。真实代价是「**每次进程启动后**、每个成员前两条穿链样本的 chain 盲区」（不是「一生前两条」），由本条的竞速回退与实时重排在数条连接内自愈。

**修订 10（Fable 复审后的结构性修正 · 本次交付的最后一块）—— 冷解析的读数在赛后重测一次。**

修订 7 把解析耗时挪进 `remote` 之后，Fable 指出它引入了一个**结构性偏袒**：在位组一直承载该目标的流量，它出口的解析器对那个域名是热的（约 1ms）；落选组上次解析是上一轮竞速（`snapshotUseLimit=128` 的节奏是几十分钟，DNS TTL 早过期），每轮复测都付一次冷解析（约 50ms）。心跳全是 IP 字面量，暖不了域名。于是挑战者每轮多背约 49ms 再加 15ms 迟滞，**轮轮如此**——这恰好蛀空了 use-limit 复测存在的意义。

**为什么记账层的方案全都不行（三条路都走过了）：**

- **只用于分类、不进排名**（chain 保持含解析）：Fable 指出基线上真正在兜底的是一个意外的恒温器——污染把 chain 推过 `chainProbeFloor` → 成员被误判成中转 → 心跳改打 IP 字面量（`traversed=true`）→ 每 75 秒灌一条正确的内部段样本 → 约 10 分钟洗净窗口。**修好分类等于拆掉这个恒温器**，于是污染从「钳在 20ms、会自愈」变成「约 45ms、永久、成员级」，比基线更差。
- **钳制（单侧）**：只钳进 remote 的那一侧、chain 仍减完整 n，超过 cap 的部分只减不加，报大 n 每微秒赚 α=0.7 且跨组生效——正是本节反复否决的减项漏洞换了个入口。
- **钳制（对称，`n_eff = min(n, cap)` 两侧同用）**：信任面守住了，但 cap 被两头夹死。截尾均值丢掉 4 条冷样本里最大的那条，残留是 3(50−cap)/7（cap=0 时给 21.9ms，正好复现生产实测值）：压过 `switchHysteresis` 需 cap > 16.2ms，而压住偏袒需 cap < 16ms。纯落选成员窗全是冷样本时更是大开口无解。

**根因：同一个 n 喂两个需求相反的消费者**——成员级 chain 窗要它越小越好，目标级 remote 窗要它别进去。**任何在记账层分配 n 的方案都在这条夹缝里。** 而算术上把它从总量里减掉又必然打开 α 红利，因为 `RoundTrip` 是客户端唯一自己测的量，任何由出口上报的减项都是白送的折扣。

**所以修的是测量，不是记账。** 竞速原样跑完——裁决、赢家隧道、快照全部用第一条（冷）读数，主循环一行不改。定局并落盘之后，对**域名目标**派一个 detached goroutine：对每个自报解析耗时超过 `switchHysteresis` 的组，**钉住当轮参赛的那个成员**（不是 `current()`，修订 3 的教训），再拨一次、测一次、立刻关，用暖读数替换该组 `remoteWindow` 里最新的那一条（`amendRemote`），最后统一 flush 一次。

几条判据写死在代码里：暖测失败**什么都不做**（冷读数留着，且绝不判死——健康已由刚跑完的那一轮定了，给一个只为磨一个数字的路径加判死通道是不划算的）；暖读数**无论更好更坏都算数**（取小者就是 remoteWindow 明确否决过的「永远相信更好看的那次」上移一层）；走 `probeSlots` 同一个闸门；按 `recordProbe` 而不是 `record` 入账（不打 `lastUsed`，否则一轮竞速会压掉所有参赛成员的下一轮心跳）。

**代价（明说）：** 域名目标的每一轮竞速里，出口需要现解析的那些组会多发一次不携带数据的连接，与第一次相隔不到一秒、同一出口 IP。用户文档中英两版的「目标只会看到一次连接」已按此改写。裁决与用户时延**都不延**（这是赛后修正相对「赛内串行二连探测」的关键优势——后者会连用户一起延，因为调用方要等裁决才拿隧道）。真实代价是本轮裁决仍用冷数字（一条连接可能骑到次优隧道），以及快照有个亚秒级窗口存着冷值等修正。

**修订 11（实现审查后的三条修正）—— 修订 10 落地时自己带了三个缺陷。**

1. **暖读数根本没落盘（严重）。** `flush()` 只写被标记为脏的 key（`smart_cache.go` 的 `dirty` 集合），而 `amendRemote` 是**原地改** entry、不经 `store`；`storeSnapshot` 又恰好在派出 goroutine 之前就 `store`+`flush` 把脏集合清空了。于是修正里那句 `flush()` 走到 `len(pending)==0` 直接返回，一个字节都不写——修正只活在内存里，`snapshotTTL` 内的任何一次重启都会把冷读数连同那约 49ms 的劣势原样读回来。`touch` 的注释原话就是「for updates that do not replace it」，正是这个场景。已加 `s.cache.touch(key)`，`amendColdResolutions` 相应多收一个 key 参数。**三路审查独立复现了同一条，而当时的五个测试一条都测不到——它们用的 cache 没有数据库，整个持久化分支在 `db()==nil` 处就返回了。** 已补 `TestAnAmendedReadingSurvivesARestart`，用真 bbolt 跑完竞速后模拟重启读盘。

2. **暖快照对上含冷解析的实测，会被误读成劣化。** `observeScore` 的 `was` 现在取的是被改暖的快照读数，而 `now` 取实时 `RemoteDial`（含冷解析）。对一个稀疏到出口 DNS 缓存每次都过期的域名目标，两者比值轻易越过 `anomalyFactor=2`：实测 α=0.7 / local=20ms / remote_warm=5ms / resolve=50ms 时 `was=19ms`、`now=69ms`，比值 3.63，三连击后把冷实测 `retire` 进在位组的窗并触发重赛——竞速频率从「6 小时或 128 次请求一轮」塌成约三次连接一轮，且方向反转成偏袒挑战者。已在 `observeScore` 里加守卫：这条连接自己付了超过 `switchHysteresis` 的解析耗时就不参与漂移判定。**且不重置连击计数**——那不是「没劣化」的证据，重置会让一个解析周期性过期的目标永远攒不满连击、彻底测不出真实搬家。

3. **量化开销三处未同步。** `snapshotUseLimit` 的注释与中英文档都还写着「一次比较的成本是每个分组一次拨号（五组 / 128 ≈ 4%）」。域名目标现在是 G + K 条，复测时 K = G−1，五组约 7%。三处已改。

**修订 12（收尾）—— 补上一直欠着的可观测性，以及把关键判断移出构建标签。**

第一次审计（§9 之前）报过一条一直没修的：**审计里三段拆不开**。修订 10 让它更糟——竞速审计记的是冷数字，而路由用的是暖数字，「为什么这个组赢了」比改动前更难回答。一并补齐：

- `stateLegs` 现在填 `Chain`。整表转储是重启前唯一落盘的全局视图，也是运维在事发几小时后手里唯一的东西；没有这一列，「往返几秒而分数几毫秒」的成员在上面看着完全正常——正是本模型存在的那个失败形态当初隐形的方式。`auditUsage` 同样填上。
- `candidate` 新增 `chain`（仅供日志，决策不读），于是 `auditSwitch` 也带得上——切换记录以前传 `nil`，说不出是哪条腿动了。
- 新记录类型 **`amend`**：重测写一行，带 `was`（被替换的读数）与 `resolve`（使重测值得做的那段解析）。中英文档的记录类型表已同步。
- `raceReport` 那一行以前用两把尺子：`local` 是窗口值、`chain` 是本轮单样本，相减不等于 near。现在 `chain` 取进入排名的那个值并新增 `near=`，`local = near + chain` 在行内成立。

**另一件：`RemoteDial = Connect + Resolve` 这个判断原本只存在于 `with_naive_outbound` 标签后面，而 CI 的测试标签集（`release/DEFAULT_BUILD_TAGS_OTHERS`）不含它——那条接线测试从不在 CI 跑，有人把它还原成 `ack.Connect`，CI 照绿。** 没有去动全局标签集（那会改变每个平台的发布内容），而是把判断本身提成 `ConnectAck.DestinationLeg()` 放进无标签的 `connect_ack.go`，标签后面只剩一行赋值；无标签测试 `TestTheDestinationLegIsTheConnectPlusFindingIt` 现在由 CI 常规跑到。

**补上的测试缺口：** 暖测的第二个调用点（`collectStragglers`，也就是调用方已经拿着隧道走了之后的那一半）此前一条测试都没覆盖——原有五条的出口都即时应答，`pending` 恒为 0，那条路根本走不到。已用 gate 构造确定性 straggler 补上，并验过删掉那个调用点它会变红。
