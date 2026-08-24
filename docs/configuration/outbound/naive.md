---
icon: material/new-box
---

!!! question "Since sing-box 1.13.0"

### Structure

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

  ... // Dial Fields
}
```

!!! warning "Platform Support"

    NaiveProxy outbound is only available on Apple platforms, Android, Windows and certain Linux builds.

    **Official Release Build Variants:**

    | Build Variant | Platforms | Description |
    |---------------|-----------|-------------|
    | (no suffix) | Linux amd64/arm64 | purego build, `libcronet.so` included |
    | `-glibc` | Linux 386/amd64/arm/arm64/mipsle/mips64le/riscv64/loong64 | CGO build, dynamically linked with glibc, requires glibc >= 2.31 (loong64: >= 2.36) |
    | `-musl` | Linux 386/amd64/arm/arm64/mipsle/riscv64/loong64 | CGO build, statically linked with musl |
    | (no suffix) | Windows amd64/arm64 | purego build, `libcronet.dll` included |

    For Linux, choose the glibc or musl variant based on your distribution's libc type.

    **Runtime Requirements:**

    - **Linux purego**: `libcronet.so` must be in the same directory as the sing-box binary or in system library path
    - **Windows**: `libcronet.dll` must be in the same directory as `sing-box.exe` or in a directory listed in `PATH`

    For self-built binaries, see [Build from source](/installation/build-from-source/#with_naive_outbound).

### Fields

#### server

==Required==

The server address.

#### server_port

==Required==

The server port.

#### username

Authentication username.

#### password

Authentication password.

#### insecure_concurrency

Number of concurrent tunnel connections.

When omitted (or `0`) the pool is adaptive: an idle client holds at most one connection; concurrent traffic rides at least two, so a single bad pipe cannot stall everything; parallel streams toward one destination — a segmented download — spread two per connection, since packing them caps the aggregate at a single TCP flow's ceiling; and no connection carries more than four streams. A bulk transfer hogging a pipe gets routed around, and a recently-drained connection is reused before a new one is dialed. The ceiling is derived from available memory divided by the session receive window, clamped between 2 and 16. With `quic` enabled there is no pooling — always a single connection.

An explicit value keeps exactly that many pools, with new streams balanced onto the least busy one — still skipping a pool a bulk transfer sits on, and still capping parallel streams to one destination at two per pool.

Multiple connections make the tunneling easier to detect through traffic analysis, which defeats the purpose of NaiveProxy's design to resist traffic analysis.

#### extra_headers

Extra headers to send in HTTP requests.

#### stream_receive_window

HTTP/2 receive window, as a number in bytes or a string like `"20mb"`.

!!! warning "Session window, not stream window"

    Despite the name, this value is the **session** (connection-level) receive
    window. Cronet advertises **half of it** as the per-stream window. A value
    of `20mb` therefore caps a single stream at 10MB in flight — size legs so
    that half the value still covers `bandwidth × RTT`, or single-stream
    throughput is capped below line rate.

`128mb` is used by default (Chromium's default; 64MB per stream), `4mb` on iOS. When `quic` is enabled the halving rule does not apply: the value is the per-stream window directly — see `quic_session_receive_window`.

#### udp_over_tcp

UDP over TCP protocol settings.

See [UDP Over TCP](/configuration/shared/udp-over-tcp/) for details.

#### quic

Use QUIC instead of HTTP/2.

#### quic_congestion_control

QUIC congestion control algorithm.

| Algorithm | Description |
|-----------|-------------|
| `bbr` | BBR |
| `bbr2` | BBRv2 |
| `cubic` | CUBIC |
| `reno` | New Reno |

`cubic` is used by default (the default of Chromium, which NaiveProxy is based on).

#### quic_session_receive_window

QUIC session (connection-level) receive window, as a number in bytes or a string like `"15mb"`.

Unlike `stream_receive_window`, the halving rule does not apply here: when `quic` is enabled, `stream_receive_window` is the per-stream window directly (default 6MB) and this field is the session window (default 15MB).

Only takes effect when `quic` is enabled.

#### tls

==Required==

TLS configuration, see [TLS](/configuration/shared/tls/#outbound).

Only `server_name`, `certificate`, `certificate_path` and `ech` are supported.

Self-signed certificates change traffic behavior significantly, which defeats the purpose of NaiveProxy's design to resist traffic analysis, and should not be used in production.

### Dial Fields

See [Dial Fields](/configuration/shared/dial/) for details.
