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

不填（或 `0`）为自适应：空闲时至多一条连接；有并发即至少两条（单管道是单点，坏一条不至于全体卡住）；同一目的地的并行流（分段下载）每连接至多两条——挤在一条上聚合会被单条 TCP 的上限封顶；任何连接不超过 4 条流。被大流量长传占据的连接会被新流绕开，刚排空的连接会优先复用而不是新建。上限由可用内存 ÷ 会话接收窗口推导，钳在 2–16 之间。启用 `quic` 时无连接池，恒为单连接。

显式填 N 则固定 N 个连接池，新流分配到最闲的一个。

多连接使隧道更容易被流量分析检测，违背 NaiveProxy 抵抗流量分析的设计目的。

#### extra_headers

HTTP 请求中发送的额外头部。

#### stream_receive_window

HTTP/2 接收窗口，字节数或类似 `"20mb"` 的字符串。

!!! warning "这是 session 窗口，不是 stream 窗口"

    与名字相反，该值是**会话**（连接级）接收窗口，cronet 会把它的**一半**
    通告为单流窗口。填 `20mb` 意味着单条流在途上限 10MB——按腿定值时要保证
    值的一半仍覆盖 `带宽 × RTT`，否则单流吞吐达不到线速。

默认 `128mb`（Chromium 缺省；单流 64MB），iOS 上默认 `4mb`。启用 `quic` 时此语义不适用。

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
