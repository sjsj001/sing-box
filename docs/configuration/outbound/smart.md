### Structure

```json
{
  "type": "smart",
  "tag": "smart",

  "outbounds": [
    "hk",
    "de",
    "sg",
    "jp"
  ],

  "probe_interval": "",
  "ping_interval": "",
  "idle_timeout": "",

  "switch_margin": 15,
  "min_dwell": "",
  "tolerance": 20,
  "cdn_threshold": 25,
  "cdn_band": 15,
  "cdn_quorum": 2,

  "queue_delay_threshold": 200,
  "congest_suppress_max": 3,
  "handshake_verdict_timeout": "",

  "nodes": [
    {
      "tag": "jp",
      "clean_priority": 1,
      "bias_ms": 0
    }
  ],
  "regions": [
    {
      "name": "east-asia",
      "members": ["hk", "jp"],
      "mode": "prefer",
      "primary": "hk"
    }
  ],

  "interrupt_exist_connections": false
}
```

!!! warning "Requirements"

    Every member must be a `naive` outbound, and every member's **server** must
    run the `sbprobe`-capable naive build from this fork. The group refuses to
    start with a non-naive member; a server without the probe branch never
    answers a probe, and the group will report the member as dead and say so in
    the log.

    `providers` and `use_all_providers` are not supported yet — list the
    outbounds explicitly.

### Why

A `urltest` group measures one number per node — the latency to a single test
URL — and sends everything to whichever node wins. That is the wrong number
whenever the nodes are in different places from each other and the destinations
are in different places from each other: the node closest to `gstatic.com` is
not the node closest to a server in Frankfurt.

`smart` picks a node **per destination host**. It splits each request's latency
into two legs and measures them separately:

* **local** — you to the proxy. Measured by pinging each member through its own
  tunnel, kept as a rolling minimum over 90 seconds so transient queueing
  (bufferbloat) cannot inflate it.
* **remote** — the proxy to the destination. Measured **by the server**, which
  times its own TCP connect to the target and reports the result. This is why
  the number does not depend on the state of your tunnel, and why it stays
  correct for a destination you have never visited from that node.

The score for a node is `local + remote` (plus your manual offsets), so a
destination in Frankfurt lands on the German node while a destination in Hong
Kong lands on the Hong Kong node, at the same time, without you writing a
routing rule for either.

!!! info "There is no test URL"

    Coming from `urltest`, the first thing to look for is the `url` field. There
    isn't one, and there is nothing to point somewhere else.

    The local leg never touches an external target: it pings your own proxy
    server through its tunnel with a marker the server answers directly, so the
    number stays a measurement of *you to the proxy* and nothing else. The
    remote leg is measured against **the destination you are actually
    connecting to**, on the port you are actually using — connect to
    `1.1.1.1:80` and every member is timed connecting to `1.1.1.1:80`.

    Every destination is its own test, which is the whole point: one fixed URL
    can only ever be right for destinations near it.

### Behaviour

#### Per host, not per domain

Buckets are keyed by **host**, not by registered domain. A provider that serves
every region under one domain — say a Frankfurt box and a Singapore box both
under `example.net` — gets a separate decision per hostname. Destinations with
no domain (an IP literal) are keyed by address.

#### The first request is already optimal

The first time a host is seen, the group probes the remote leg from every usable
member before committing, then decides. This costs roughly half a second on that
one request and means there is no "connect first, correct later" phase.

A host whose traffic is UDP-only (STUN, WebRTC) has no TCP port to probe with,
so it skips this and takes the stable regional pick instead.

#### Sticky, with hysteresis

Once a host has a node, it keeps it. A challenger has to beat the incumbent by
`switch_margin` percent (floored at `tolerance` milliseconds), win **two
consecutive rounds of measurement**, and wait out `min_dwell` before anything
moves. Two rounds means two independent measurements — re-deciding on the same
samples does not count as a second win.

The exception is a pick that was made without a complete measurement — a
fallback, or a host measured while some member was unreachable. That one is
allowed a single immediate correction, so an early guess is fixed without
waiting out the whole window.

Switching is a latency improvement, never a failover, so it does not disturb
connections that are already open.

#### CDN destinations are chosen differently

When several members all report a very low remote latency to a host, that host
is anycast or CDN-backed — every node is "close" to it, so latency no longer
discriminates. For those, the group stops ranking by speed and picks the
**cleanest** node instead (see `clean_priority`), among those within `cdn_band`
of the fastest. This is what sends CDN traffic through your least-flagged exit
without affecting anything else.

#### Failure handling

Three independent layers, each deliberately damped so that a single bad event
cannot move traffic that does not need moving:

* **Per destination.** A member that fails against one host is blocked *for that
  host* — 30s, then 2m, then 10m on repeats. A successful probe lifts the block
  early and decays the escalation, so a path that recovers comes straight back
  to the short interval. One dead target never costs you a whole node.
* **Per member.** A member is declared dead only on sustained evidence: two
  consecutive hard errors, or a live connection failing against two *different*
  hosts inside a minute (one dead target repeats the same host, so it can never
  trip this). Coming back requires consecutive clean pings, and a member that
  keeps flapping has to stay clean for longer each time.
* **Congestion vs death.** A probe timing out is ambiguous — your own uplink may
  be saturated. Before blaming the member, the group looks for a *witness*:
  another member whose ping came back but inflated by more than
  `queue_delay_threshold`. A witness means the problem is local, and the member
  is given up to `congest_suppress_max` rounds of grace. With no witness, a
  final generous handshake on a brand-new session decides it.

An abruptly dead node costs about one failed request: the connection's first
hard error reroutes the host immediately, rather than waiting for the next ping.

### Fields

#### outbounds

==Required==

List of `naive` outbound tags to select between.

#### probe_interval

How often each active destination's remote leg is re-measured. `45s` is used if
empty.

This is the group's clock: a sample is considered fresh for two intervals, and
the sustained-lead requirement is counted in rounds. One round is bounded to one
interval by construction — destinations are refreshed most-recently-used first,
and the round stops dispatching when the interval is spent, deferring the cold
tail rather than running long.

#### ping_interval

How often each member's local leg is measured, which is also the health-check
cadence. `5s` is used if empty.

#### idle_timeout

How long a destination keeps its learned state after its last use. `30m` is used
if empty.

#### switch_margin

How much better a challenger must be, in percent, before the sticky node is
replaced. `15` is used if empty. The margin is measured against the incumbent's
own score, and is floored at `tolerance` so that near-zero scores still need a
real difference.

#### min_dwell

The minimum time a host stays on a node before it may be moved. `30s` is used if
empty.

#### tolerance

The band, in milliseconds, within which two totals count as equal. `20` is used
if empty.

Inside this band the group prefers the node with the lower **remote** leg. This
is what breaks ties toward the node that is genuinely nearer the destination
rather than the one that merely happens to have a faster link to you.

#### cdn_threshold

The remote latency, in milliseconds, below which a member counts as "close" to a
destination for CDN classification. `25` is used if empty; leaving the
classification requires 10ms more, so a borderline host cannot oscillate.

#### cdn_band

For a destination classified as CDN, how far above the fastest member's remote
latency another member may be and still be considered, in milliseconds. `15` is
used if empty. Within that band the cleanest node wins.

#### cdn_quorum

How many members must be below `cdn_threshold` before a destination is treated
as CDN. `2` is used if empty.

#### queue_delay_threshold

How much a member's ping must exceed its own rolling-minimum baseline, in
milliseconds, to count as inflated. `200` is used if empty.

This one threshold does two jobs. A member that is inflated *while another
member is not* has a degraded local leg, and is penalised in ranking by the
measured inflation until it recovers — traffic moves off it immediately instead
of waiting ~90 seconds for its baseline to catch up. And a member that is
inflated *while another member has timed out* is the witness that proves the
timeout was local congestion, not death.

#### congest_suppress_max

How many consecutive rounds a timing-out member may be excused because a witness
shows local congestion. `3` is used if empty. Past this, it faces the handshake
verdict anyway — otherwise a genuinely dead member could hide behind a
permanently congested uplink.

#### handshake_verdict_timeout

The timeout for the final verdict: a fresh TLS session to the member, opened on
its own connection pool. `8s` is used if empty.

It is deliberately generous. Reaching this point means the ambiguity has already
been narrowed to "slow or dead", and the cost of a wrong "dead" is far higher
than the cost of waiting.

#### nodes

Per-member tuning. Members not listed here take the defaults.

#### nodes.tag

The member's outbound tag.

#### nodes.clean_priority

An ordinal, **smaller means cleaner**. `100` is used if empty, so a member you
say nothing about ranks behind any member you mark.

This is the knob for "this exit is the one that is least likely to get me a
CAPTCHA". It is consulted only for destinations classified as CDN, where latency
has stopped discriminating — it never overrides a genuine latency difference to
an ordinary destination.

#### nodes.bias_ms

A manual offset added to the member's score, in milliseconds. Negative favours
the member. Clamped to ±100, with a warning, so a mistyped value cannot silently
hijack every decision.

#### regions

Groups members that sit in the same place.

#### regions.name

The region's name.

#### regions.members

The member tags in this region.

#### regions.mode

`prefer` uses the region's backups only when its primary is unavailable.
`equivalent` treats the members as interchangeable and lets measurement decide.
`equivalent` is used if empty.

#### regions.primary

The preferred member, for `prefer` mode.

#### interrupt_exist_connections

Interrupt a member's existing connections when it is declared dead.

Only inbound connections are affected by this setting, internal connections will
always be interrupted.

This applies to **death only**. A destination moving to a faster node never
interrupts anything.

### Observability

The group logs a fixed-size health line per member every minute at `info`, and
each time a destination changes node:

```
smart nodes: hk[health=0 local=27ms] de[health=0 local=164ms] sg[health=0 local=64ms]
smart: example.com switched de -> hk by=total
```

`by=` names the dimension that decided it: `total` (lowest local+remote),
`remote` (totals were within `tolerance`, so the nearer node won), or `cdn`
(classified as CDN, so the cleanest node won).

At `debug` the group also logs each refresh round's destination count and wall
time. Worth a look on a new deployment: every anti-flap threshold here is priced
in rounds, so if rounds are not landing near `probe_interval` the settings do not
mean what they say.

!!! note

    The per-destination switch line contains hostnames. If you would rather not
    have browsing history in your log, run at `warn`.
