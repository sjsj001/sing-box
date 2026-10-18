!!! quote "Changes in sing-box 1.13.0"

    :material-plus: [quic_congestion_control](#quic_congestion_control)

### Structure

```json
{
"type": "naive",
"tag": "naive-in",
"network": "udp",
...
// Listen Fields

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

### Listen Fields

See [Listen Fields](/configuration/shared/listen/) for details.

### Fields

#### network

Listen network, one of `tcp` `udp`.

Both if empty.

#### users

==Required==

Naive users.

#### quic_congestion_control

!!! question "Since sing-box 1.13.0"

QUIC congestion control algorithm.

| Algorithm      | Description                     |
|----------------|---------------------------------|
| `bbr`          | BBR                             |
| `cubic`        | CUBIC                           |
| `reno`         | New Reno                        |

`bbr` is used by default.

#### send_queue_target

How much queueing delay the bulk writes of TCP connections may add to the path to their client, as a duration. Off when unset; `40ms` is the suggested value.

An HTTP/2 session carries every stream of a client, and its flow-control windows (128MB per session from naive clients) let one large download fill the bottleneck queue in front of all the others. With this set, a write of 4KiB or more waits while the connection's smoothed RTT is more than this above the path's min RTT — the least that the connections to the same client address reported in the last 10 seconds — and its own unacknowledged and unsent bytes would exceed delivery rate × (that min RTT + this). Smaller writes, such as the answer to a CONNECT, always go at once.

At 40ms, through a two-hop chain with 8 parallel downloads and a small request every 0.5 seconds on the same path, the small request took 175ms median and 0.51s at worst at 10Mbps, against 10.8s and 14.7s when off, for 9 instead of 10Mbps; at 50Mbps 128–145ms and 0.37–1.2s, against 0.7s and 8.8s, for 47 instead of 48Mbps. Unlimited, 8 streams got 1764 instead of 1888Mbps and one stream 485 as before.

It fails in two known ways:

- On a bottleneck shared with loss-based traffic that does not go through it, such as CUBIC downloads of other hosts, it yields: one download was held at about 2Mbps while iperf took 45. Downloads are slow, but small requests do not stall.
- After the route to a client gets longer, the longer round trip reads as queueing for up to 5 minutes, until the kernel lets go of the old minimum: after a change from 35 to 135ms, a download was held at 28Mbps against 47 when off.

Only set it on a relay whose bottleneck to its clients carries nothing but its own traffic.

Only on Linux, and only over TCP with a standard TLS configuration: not with reality or kTLS, not without TLS, not over QUIC.

#### tls

TLS configuration, see [TLS](/configuration/shared/tls/#inbound).

With a standard TLS configuration, `handshake_timeout` does not apply to the TCP listener: the handshake runs inside the first read of the HTTP server, which bounds it by its 30s header timeout instead. Configurations served without `crypto/tls`, such as reality, keep the setting.