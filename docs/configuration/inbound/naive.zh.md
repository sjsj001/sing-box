!!! quote "sing-box 1.13.0 中的更改"

    :material-plus: [quic_congestion_control](#quic_congestion_control)

### 结构

```json
{
"type": "naive",
"tag": "naive-in",
"network": "udp",

... // 监听字段

"users": [
{
"username": "sekai",
"password": "password"
}
],
"quic_congestion_control": "",
"send_queue_target": "",
"tls": {}
}
```

### 监听字段

参阅 [监听字段](/zh/configuration/shared/listen/)。

### 字段

#### network

监听的网络协议，`tcp` `udp` 之一。

默认所有。

#### users

==必填==

Naive 用户。

#### quic_congestion_control

!!! question "Since sing-box 1.13.0"

QUIC 拥塞控制算法。

| 算法             | 描述                 |
|----------------|--------------------|
| `bbr`          | BBR                |
| `cubic`        | CUBIC              |
| `reno`         | New Reno           |

默认使用 `bbr`。

#### send_queue_target

TCP 连接的大块写入最多可以给到客户端的路径增加多少排队延迟，格式为时长。不设时关闭；建议值 `40ms`。

一个 HTTP/2 会话承载客户端的所有流，它的流量控制窗口（naive 客户端每个会话 128MB）让一个大下载就能填满瓶颈队列，挡在其他所有流前面。设置后，4KiB 及以上的写入在以下条件同时成立时等待：连接的平滑 RTT 比路径的最小 RTT 高出超过此值（路径的最小 RTT 是到同一客户端地址的连接在最近 10 秒内报告的最小值），并且它自己未确认和未发送的字节会超过 交付速率 ×（这个最小 RTT + 此值）。更小的写入（例如 CONNECT 的应答）总是立即发出。

设为 40ms 时，经两跳链、8 个并行下载、同一路径上每 0.5 秒一个小请求：10Mbps 下小请求中位 175ms、最长 0.51 秒（关闭时 10.8 秒和 14.7 秒），吞吐 9Mbps（关闭时 10Mbps）；50Mbps 下中位 128–145ms、最长 0.37–1.2 秒（关闭时 0.7 秒和 8.8 秒），吞吐 47Mbps（关闭时 48Mbps）。不限速时 8 个流 1764Mbps（关闭时 1888Mbps），单个流 485Mbps 不变。

已知两种失败情况：

- 瓶颈与不经过它的基于丢包的流量（例如其他主机的 CUBIC 下载）共用时，它会让路：一个下载曾被压到约 2Mbps，而 iperf 拿到 45Mbps。现象是下载慢，但小请求不卡。
- 到客户端的路由变长后，最长约 5 分钟内（直到内核放掉旧的最小值），变长的往返被当作排队：从 35ms 变为 135ms 后，下载被压在 28Mbps（关闭时 47Mbps）。

只在中继到客户端的瓶颈只承载它自己流量的路径上设置。

仅在 Linux 上、仅对使用标准 TLS 配置的 TCP 生效：reality、kTLS、不带 TLS 和 QUIC 都不生效。

#### tls

TLS 配置, 参阅 [TLS](/zh/configuration/shared/tls/#入站)。

使用标准 TLS 配置时，`handshake_timeout` 对 TCP 监听不起作用：握手在 HTTP 服务器第一次读取时进行，改由 30 秒的请求头超时限制。不经 `crypto/tls` 提供的配置（例如 reality）仍使用这个设置。