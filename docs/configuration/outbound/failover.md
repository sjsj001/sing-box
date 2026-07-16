### Structure

```json
{
  "type": "failover",
  "tag": "HK",

  "outbounds": [
    "hk-a",
    "hk-b"
  ],
  "strategy": "order",
  "health_check": "1.1.1.1:80",
  "cooldown": {
    "fail_limit": 2,
    "base": "1m",
    "max": "15m"
  },
  "hedge_delay": "",
  "interrupt_exist_connections": false
}
```

### How it works

A failover group serves every connection through one preferred member and
keeps standbys for when that member fails — either **for one destination**
(the member's egress cannot reach a specific site) or **entirely**. It does
no latency learning and no per-destination optimization: interchangeable
same-region nodes are its intended members. For cross-region, per-site
selection, use a [Smart](/configuration/outbound/smart) group — the two
compose naturally, with failover groups as smart members.

Failure handling has three layers, fastest first:

1. **Same-call retry**: a failed dial falls through to the next member
   within the same connection — the user sees nothing. Each attempt is
   bounded by an adaptive budget (`max(3 × member baseline, 1s)`, capped at
   5s) so a hung member cannot eat the caller's whole timeout.
2. **Hedged requests** (optional, see `hedge_delay`): when the primary
   accepts the connection but no response byte arrives in time, the standby
   is raced with the same request; the first response wins.
3. **Per-destination cooldown**: `fail_limit` consecutive failures (dial
   errors, early resets, or zero-byte hangs within 3s) exclude the member
   *for that destination only*, with exponential backoff on relapse and a
   verification probe against the destination before re-entry.

Member-level health is probed against `health_check` once a minute: three
consecutive failures take the member out entirely (optionally interrupting
its connections), two successes bring it back. A destination where *all*
members are excluded fails open to the preference order rather than going
dark.

State is in-memory only: cooldowns are minute-scale transients, so there is
nothing worth persisting across restarts.

### Fields

#### outbounds

==Required==

Ordered member outbound tags. The order is the preference order under
`strategy: order`, and the tie-break order under `strategy: auto`.

#### strategy

How the primary member is chosen while everyone is healthy:

| Value | Meaning |
|-------|---------|
| `order` (default) | Strictly the first healthy member of `outbounds` — for when you know which node should carry traffic |
| `auto` | The member with the lowest health-check baseline (rolling minimum) is elected primary — for when you don't care and want the system to pick |

Under `auto`, replacing an elected primary requires a challenger to hold a
>20% lower baseline for 3 consecutive health rounds (≈3 minutes) — no
flapping between near-identical nodes. A dead primary is replaced
immediately. Re-election only affects new connections.

#### health_check

`host:port` probed through every member once a minute for member liveness
(3 failures down / 2 successes up) and, under `strategy: auto`, the
baseline used to elect the primary. `1.1.1.1:80` is used by default.

#### cooldown

Per-destination exclusion, identical shape to the smart group's:
`fail_limit` consecutive failures cool the member for that destination for
`base`, doubling on quick relapse up to `max`; a verification probe against
the destination gates re-entry. A completed response byte resets the streak,
so multiplexed protocols whose dials always "succeed" are handled correctly.

| Field | Default |
|-------|---------|
| `fail_limit` | `2` |
| `base` | `1m` |
| `max` | `15m` |

#### hedge_delay

Enables hedged requests when set (e.g. `"1.5s"`); disabled when empty.

If the primary produces no response byte within this delay, the buffered
client bytes are replayed to the next eligible member and the first response
byte wins; the loser is closed and **not** penalized (slow is not broken —
only real errors count toward cooldown). A primary that fails outright
starts the hedge immediately instead of waiting out the delay. The user's
perceived latency is thereby capped near `hedge_delay + standby RTT` even
for silently black-holed paths.

Cost: a request that is slower than the delay briefly occupies two upstream
connections. TCP only; hedging UDP would duplicate datagrams to two
upstreams with unpredictable results for connection-oriented protocols like
QUIC. Fan-out is 1 (primary plus one standby).

#### interrupt_exist_connections

Interrupt existing connections when a member is taken down by health checks,
so they re-dial through a healthy member instead of lingering on a dead
path.

Only inbound connections are affected by this setting, internal connections
will always be interrupted.
