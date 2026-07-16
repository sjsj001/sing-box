### Structure

```json
{
  "type": "smart",
  "tag": "smart-out",

  "outbounds": [
    "HK",
    "US",
    "DE",
    "SG",
    "JP"
  ],
  "preferred": [
    "JP"
  ],
  "health_check": "1.1.1.1:80",
  "anycast_threshold": "10ms",
  "tolerance": "20ms",
  "switch": {
    "improve_ratio": 0.25,
    "improve_min": "30ms",
    "dwell": "5m",
    "confirmations": 2
  },
  "cooldown": {
    "fail_limit": 2,
    "base": "1m",
    "max": "15m"
  },
  "probe": {
    "concurrency": 8,
    "sample_ttl": "15m",
    "timeout": "5s"
  },
  "explore_interval": 16,
  "max_targets": 8192,
  "record_ttl": "14d",
  "cache_path": "",
  "interrupt_exist_connections": false
}
```

### How it works

A smart group learns, per destination, which member delivers responses
fastest and sticks to it. Members are typically per-region `urltest` groups.

**Measurement is passive-first**: every real connection is timed from its
first client write to the first server byte, so steady traffic needs no
probing at all. Per (destination, member) the last 16 samples form a fixed
window whose **median** drives all decisions — single outliers (a rebuilt
multiplexed session, a garbage-collected pause) cannot move it. Background
probes are only used to fill gaps: cold-start backfill, switch confirmation,
recovery verification after cooldown, and stale refresh.

From each total the engine derives a **remote RTT estimate**: it subtracts
the member's baseline (its floor latency to the `health_check` endpoint) and divides by the
protocol leg factor (2 for early-data protocols whose dial returns before
the tunnel exists, 1 for blocking ones). This makes members with different
local distances comparable by how close they are to the *destination*.

**Selection order** for a new connection:

1. *Sticky*: the current member is kept while it is healthy — the hot path.
2. *Threshold walk*: walking `preferred` order first, the first member whose
   remote RTT is within `anycast_threshold` wins. An established origin
   affinity overrides this step.
3. *Latency fallback*: among members whose totals are within `tolerance` of
   the fastest, the lowest remote RTT wins.
4. *Cold start*: TLS destinations race up to 3 candidates (first response
   byte wins, losers' samples are backfilled by probes); other ports take
   the first healthy member in priority order plus backfill probes.

**Switching is asymmetric.** Moving to a *better* member is slow and
deliberate: the challenger must win by a real margin, be confirmed by
consecutive probes, and the incumbent must have held the destination for the
dwell period. Moving away from a *failing* member is fast: consecutive
failures or repeated latency spikes demote it immediately, with the
remaining candidates retried within the same connection. Failures are
tracked per (destination, member) pair — one broken site on one member never
affects other destinations, and if *every* member fails the destination
itself is considered down: nobody is cooled and retries are rate-limited.

### Fields

#### outbounds

==Required==

List of member outbound tags. Members are equal candidates; per-destination
learning decides which one carries which destination. Typically per-region
`urltest` groups, so intra-region node failover stays inside the region
group and invisible to the smart layer.

#### preferred

Ordered member tags to prefer, prefix semantics: listing one or two entries
is enough, remaining members keep their declared order. Every listed tag
must exist in `outbounds`.

For anycast/CDN destinations that every member reaches quickly (see
`anycast_threshold`), the first *qualified* preferred member wins **even
when it is not the absolute fastest**. This is deliberate: for anycast
targets the preferred member's egress region controls which CDN edge and
egress IP you appear from, which matters more than a few milliseconds.

#### health_check

`host:port` of an always-reachable, anycast-served endpoint used for two
things: measuring each member's **baseline** (its floor latency, taken as a
rolling minimum, so cold connections cannot inflate it) and member-level
**liveness** (3 consecutive health-check failures mark the member down, 2
successes bring it back).

`1.1.1.1:80` is used by default: Cloudflare's anycast edge is close to any
sane member region and a plain HTTP-port round trip measures the pure path
without TLS processing cost. Change it only if your members cannot reach it.

#### anycast_threshold

The remote-RTT bound for step 2's walk: a member whose estimated
member→destination RTT is below this counts as "equally close", so
preference order — not raw speed — picks among them. `10ms` by default.

Raising it (e.g. `20ms`) pulls more destinations under preferred-member
control; lowering it makes the group chase pure latency more often.

#### tolerance

Step 3's equivalence band: members whose total durations lie within this of
the fastest are considered tied, and the tie is broken by the lowest remote
RTT (i.e. the member actually closest to the destination, not the one
closest to you). `20ms` by default.

A larger band favors members with better *origin* proximity even when their
totals are slightly worse; a smaller band sticks closer to raw totals.

#### switch

Hysteresis for the *improvement* direction. A challenger must beat the
current member's remote RTT by `max(improve_ratio × current, improve_min)`,
be confirmed by `confirmations` consecutive probe rounds, and the current
member must have held the destination for at least `dwell`. All four exist
to prevent flapping; the price is that improvements take minutes to land,
which is intentional.

| Field | Default | Meaning |
|-------|---------|---------|
| `improve_ratio` | `0.25` | relative advantage required (25% of current) |
| `improve_min` | `30ms` | absolute advantage floor |
| `dwell` | `5m` | minimum incumbency before any improvement switch |
| `confirmations` | `2` | consecutive confirming probes required |

Lower `dwell`/`confirmations` only in test environments; production values
below the defaults re-introduce oscillation risk.

#### cooldown

The *degradation* direction. `fail_limit` consecutive failures (dial errors,
zero-byte hangs, early resets — a completed response resets the count), or
repeated latency spikes beyond 2× the destination's median, demote the
member **for this destination only** and start a cooldown of `base`.
Relapsing within 5 minutes of a recovery doubles the cooldown each time up
to `max`; staying healthy resets the ladder. A cooled member must also pass
a verification probe before re-entering candidacy.

| Field | Default | Meaning |
|-------|---------|---------|
| `fail_limit` | `2` | consecutive failures before demotion |
| `base` | `1m` | first cooldown duration |
| `max` | `15m` | cooldown ceiling |

`fail_limit: 1` reacts faster but a single stray reset then demotes; `3` is
more tolerant at the cost of one extra failed user connection.

#### probe

Background probe controls.

| Field | Default | Meaning |
|-------|---------|---------|
| `concurrency` | `8` | global cap on in-flight probes |
| `timeout` | `5s` | per-probe deadline |
| `sample_ttl` | `15m` | how long samples stay decision-fresh |

Probes are per-(destination, member) rate-limited (≥15s apart), jittered,
and deduplicated, so bursts cannot stampede. When a sticky destination's
data goes stale past `sample_ttl`, the old decision keeps serving and a
background refresh is triggered by the next real connection
(stale-while-revalidate) — traffic never waits on a probe.

#### explore_interval

For destinations held by a preferred member, every Nth connection is lent to
the candidate with the fewest first-byte samples, feeding origin-affinity
detection (see below) with real application-level data. `16` by default;
`0` disables exploration — the risk-control off switch, at the cost of
never discovering origin affinity.

Origin affinity: when another member's application first-byte times (median
*and* p90, ≥32 samples each) are consistently ≤70% of the current member's,
that member takes the destination over even against preference — this
catches sites whose *origin* (not CDN edge) sits far from the preferred
region. The conclusion is revoked with the same hysteresis once the gap
closes past 85%.

#### max_targets

LRU capacity of the learned-destination table. `8192` by default. Each entry
costs roughly a kilobyte; raise it on boxes serving many distinct sites,
lower it on tiny devices.

#### record_ttl

Idle expiry for learned state, applied identically in memory and on disk.
`14d` by default. A destination not visited for this long starts fresh next
time.

#### cache_path

Path to a JSON file persisting learned state across restarts. Disabled when
empty. Written atomically (fsync + rename) every 5 minutes and on shutdown;
a corrupt or version-incompatible file is discarded on load, unknown member
tags are dropped.

Inspect it with `sing-box smart-cache` (path discovered from the
configuration) or `sing-box smart-cache <file>`: per destination it prints
the current outbound, the reason, hold time, switch count, affinity, and
each member's measured medians. `-f <substring>` filters targets, `--json`
dumps the raw snapshot.

#### interrupt_exist_connections

Interrupt existing connections when a member goes down at the member level
(health-check liveness), so they re-dial through a healthy member instead of
lingering on a dead path.

Only inbound connections are affected by this setting, internal connections
will always be interrupted.

### Multiplexed members and cold sessions

Members running multiplexed protocols (e.g. naive with
`insecure_concurrency`) tear down idle transport sessions; the first stream
afterwards pays session re-establishment and measures slow. The group
accounts for this in four places: medians absorb isolated outliers, the
first sample after a 60s idle gap never counts toward spike demotion, spike
accounting is suppressed for 15s after any switch, and member baselines use
rolling minimums. Sustained slowness — two consecutive spikes on a warm
path — still demotes immediately.
