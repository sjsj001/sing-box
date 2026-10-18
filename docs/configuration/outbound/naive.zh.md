---
icon: material/new-box
---

!!! question "自 sing-box 1.13.0 起"

### 结构

```json
{
  "type": "naive",
  "tag": "naive-out",

  "server": "127.0.0.1",
  "server_port": 443,
  "username": "sekai",
  "password": "password",
  "insecure_concurrency": 0,
  "extra_headers": {},
  "stream_receive_window": "",
  "udp_over_tcp": false | {},
  "quic": false,
  "quic_congestion_control": "",
  "quic_session_receive_window": "",
  "tls": {},

  ... // 拨号字段
}
```

!!! warning "平台支持"

    NaiveProxy 出站仅在 Apple 平台、Android、Windows 和特定 Linux 构建上可用。

    **官方发布版本区别：**

    | 构建变体 | 平台 | 说明 |
    |---|---|---|
    | (无后缀) | Linux amd64/arm64 | purego 构建，包含 `libcronet.so` |
    | `-glibc` | Linux 386/amd64/arm/arm64/mipsle/mips64le/riscv64/loong64 | CGO 构建，动态链接 glibc，要求 glibc >= 2.31（loong64: >= 2.36） |
    | `-musl` | Linux 386/amd64/arm/arm64/mipsle/riscv64/loong64 | CGO 构建，静态链接 musl |
    | (无后缀) | Windows amd64/arm64 | purego 构建，包含 `libcronet.dll` |

    对于 Linux，请根据发行版的 libc 类型选择 glibc 或 musl 变体。

    **运行时要求：**

    - **Linux purego**：`libcronet.so` 必须位于 sing-box 二进制文件相同目录或系统库路径中
    - **Windows**：`libcronet.dll` 必须位于 `sing-box.exe` 相同目录或 `PATH` 中的任意目录

    自行构建请参阅 [从源代码构建](/zh/installation/build-from-source/#with_naive_outbound)。

### 字段

#### server

==必填==

服务器地址。

#### server_port

==必填==

服务器端口。

#### username

认证用户名。

#### password

认证密码。

#### insecure_concurrency

并发隧道连接数。

不填（或 `0`）为自适应：空闲时至多一条连接；有并发即至少两条（单管道是单点，坏一条不至于全体卡住）；同一目的地的并行流（分段下载）每连接至多两条——挤在一条上聚合会被单条 TCP 的上限封顶；在上限以内，每条连接不超过 4 条流。被大流量长传占据的连接会被新流绕开，刚排空的连接会优先复用而不是新建。上限 = 总内存的四分之一 ÷ 会话接收窗口，钳在 2–16 之间。总内存在 Linux 上取 cgroup 限额或整机内存，其他平台按 2GB 主机算，iOS 按 64MB 算；所以 2GB 主机用默认 `128mb` 窗时上限是 4 条连接；在 Linux 上这样的主机报出的总内存略少于 2GB，上限是 3 条。到达上限后，新流继续分到已有连接上，每条连接会超过 4 条流。启用 `quic` 且不填本字段时无连接池，恒为单连接。

显式填 N 则固定 N 个连接池，新流分配到最闲的一个，但仍绕开被大流量长传占据的连接，同一目的地每连接也仍至多两条。启用 `quic` 时显式填 N 会起 N 个 QUIC 引擎，各一条连接（iOS 上只起一个引擎）。

多连接使隧道更容易被流量分析检测，违背 NaiveProxy 抵抗流量分析的设计目的。

#### extra_headers

HTTP 请求中发送的额外头部。

#### stream_receive_window

HTTP/2 接收窗口，字节数或类似 `"20mb"` 的字符串。

!!! warning "这是 session 窗口，不是 stream 窗口"

    与名字相反，该值是**会话**（连接级）接收窗口，cronet 会把它的**一半**
    通告为单流窗口。填 `20mb` 意味着单条流在途上限 10MB——按腿定值时要保证
    值的一半仍覆盖 `带宽 × RTT`，否则单流吞吐达不到线速。

默认 `128mb`（NaiveProxy 的缺省；单流 64MB。Chromium 自己的缺省是会话 15MB、单流 6MB），iOS 上默认 `4mb`。启用 `quic` 时此语义不适用。

不填此字段时，在客户端套接字能报告往返时间的平台上（Linux、Android），单流窗口自动调整：会话按
1200Mbps × 路径往返时间给每条流定初始窗口（最少 8MB），一开始就够跑满。在此之上，只有当一条流
连续在两个往返内用掉一半窗口、且路径的平滑往返时间不超过最小值的 1.5 倍时，窗口才翻倍，最多到 64MB。所以慢读的流只占初始窗口，
不再占 64MB；路径上有排队时，窗口也不会跟着排队一起长。填了此字段就不自动调整，值的一半是固定的
单流窗口。设了 `detour` 时按默认值固定，不自动调整：那时测到的只是到 detour 这一跳的往返时间。

#### udp_over_tcp

UDP over TCP 配置。

参阅 [UDP Over TCP](/zh/configuration/shared/udp-over-tcp/)。

#### quic

使用 QUIC 代替 HTTP/2。

#### quic_congestion_control

QUIC 拥塞控制算法。

| 算法 | 描述 |
|------|------|
| `bbr` | BBR |
| `bbr2` | BBRv2 |
| `cubic` | CUBIC |
| `reno` | New Reno |

默认使用 `cubic`（NaiveProxy 基于的 Chromium 的默认值）。

#### quic_session_receive_window

QUIC 会话（连接级）接收窗口，字节数或类似 `"15mb"` 的字符串。

注意与 HTTP/2 不同：启用 `quic` 时 `stream_receive_window` 直接就是单流窗口（默认 6MB），本字段是会话窗口（默认 15MB），不存在减半规则。

仅在启用 `quic` 时生效。

#### tls

==必填==

TLS 配置, 参阅 [TLS](/zh/configuration/shared/tls/#出站)。

只有 `server_name`、`certificate`、`certificate_path` 和 `ech` 是被支持的。

自签名证书会显著改变流量行为，违背了 NaiveProxy 旨在抵抗流量分析的设计初衷，不应该在生产环境中使用。

### 拨号字段

参阅 [拨号字段](/zh/configuration/shared/dial/)。

到服务器的 TCP 连接由 sing-box 在 cronet 的线程池线程上拨号：拨号进行期间，同一引擎上的其他流照常进行；突发时同时发起的拨号并行执行。服务器被黑洞时，只有新会话要等一个完整的连接超时（默认 5 秒），`connect_timeout` 只决定这个会话等多久。拨号超过约 250 毫秒仍未完成时，会再并行发起一次（与 Chromium 对自身 socket 的做法相同），先连上的那条被使用。QUIC 的 UDP socket 仍在 cronet 的网络线程上拨号：需要握手的拨号（经 detour）进行期间，整个引擎要停住同样长的时间。
