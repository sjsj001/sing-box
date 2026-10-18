### Structure

```json
{
  "type": "smart",
  "tag": "smart",

  "alpha": 0.7,
  "groups": [
    {
      "tag": "hk",
      "outbounds": ["hk-1", "hk-2"],
      "select": "first"
    },
    {
      "tag": "jp",
      "outbounds": ["jp-1"],
      "select": "fastest",
      "bonus": "40ms"
    }
  ]
}
```

### How it works

A smart outbound chooses a route **per destination** rather than picking one for
everything.

A proxied path has two legs: **local**, from the client to the innermost proxy,
and **remote**, from that proxy to the destination. A naive protocol extension has
the proxy report how long it spent dialing the destination in its CONNECT
response, which makes the two separable. Across a proxy chain, local covers every
hop up to the innermost proxy — the relay's own leg to its exit included, see
below — and remote is always the last leg.

The split matters because the two behave differently: local is a property of the
**node** and barely varies between destinations, while remote is a property of the
**destination** and collapses to a millisecond or two for CDN and anycast
addresses.

Groups are ranked by the lowest:

```
score = alpha × local + remote − bonus
```

The first time a destination is used, every available group is probed at once
and the results compared; a group whose members are all down takes no part
while another group is available. The outcome is written to the disk cache, and
later connections use it directly without probing again.

The connection that started the race is handed back at once and its first write
is held. If that write is a TLS ClientHello — one whole record, offering no
early data — it goes out on every probe as soon as the probe is up, so the
winner's answer to it is already on its way when the race decides, rather than
one round trip to the winner later. Anything else, and everything written after
the ClientHello, goes to the winner alone once the race has decided. Refreshing
a snapshot, and every other probe nobody is waiting on, carries nothing.

What that changes for the caller:

- A race that ends with no group able to reach the destination fails the
  connection on its first read or write, not on the dial. A SOCKS or snell
  client sees the connection accepted and then closed, the same as a cached
  destination whose proxy refuses it. A relay whose outbound is smart still
  answers the CONNECT only after the race, with the winner's answer, or 502 when
  every group refused and 503 otherwise.
- A dial is still refused at once when no group is left to race — every group
  that is up is cooling down on this port — and when its context is already
  cancelled. After that the dial's context no longer ends the race; closing the
  connection does.

What it changes for the destination: a round used to show it one bare connection
from each racing exit; now it shows it the same ClientHello from each — the
same random, key share and session ticket — which links those exits to one
client exactly, where before it took matching the times. Nothing else reaches
it: the name and the ticket arrive with the winner's handshake anyway, and the
losing tunnels close before the client could answer. Each losing group costs one
ClientHello up and the server's first flight down, a few kilobytes between this
outbound and its proxies, and the destination one key exchange. Sending the
ClientHello to the first few groups only would keep the saving where it is
smallest, a near group winning, and lose it where it is largest, so there is no
option for it.

A destination that every group answered "cannot reach it" (502) for is refused
outright on its next connections, without dialing any group. The refusal is
remembered **per group and per port** — a host serving only 443 refuses 80, and
a provider that blocks 25 still carries 465 — for five minutes the first time,
doubling on each further refusal of the same port up to an hour. The refusal
also remembers **which member** gave it, and stops counting once the group uses
a different member, or once that member has been condemned since: an exit that
lost its own egress answers 502 for everything, and those refusals were about
the exit, not the destination. Otherwise nothing shortens it: a port that has
come back is dialed again only when its cooldown runs out, and the count resets
once the group reaches that port again.

A connection to a decided destination goes out at once with its opening write.
If the proxy then fails before answering the CONNECT, the connection moves to
another group and the opening is written again there, but only when nothing can
arrive twice: the proxy said it could not reach the destination, the tunnel
never carried the opening, or what it carried was a ClientHello alone, which
asks for nothing but a handshake. An opening that may already have reached the
destination is not sent again; the connection fails, and the application
retries it. Running out of time waiting for the answer does not count as the
proxy failing, since it may still deliver; only the bound on reaching the proxy
at all does.

For a destination named by hostname, a group whose exit had to look that name up
is asked once more after the round is over, and that second reading replaces the
first. The reason is that the lookup is not a property of the path: the group
already carrying the destination has the name cached and every other group does
not, so a refresh would measure the incumbent warm and its challengers cold,
every time, and freeze a handicap into the very round meant to correct one. The
retake costs those groups one further payload-free connection, sub-second after
the first and from the same exit; a group whose exit already had the name is not
asked again, which is the usual case once a destination has been raced before.
In a burst of new names the retake may be skipped — past a backlog of 16 it is
dropped rather than queued — and then the cold reading stands until the
snapshot's next refresh. An outbound with a single group never retakes, since
there is no other group to compare with.

The connection that triggered the race is never made to wait for the full
story: it moves onto the winning tunnel as soon as waiting longer could not
change what *it* gets. Groups still answering are collected for a short grace
in the background — an answer carries the proxy's own DNS time on top of the
two legs being scored, and first contact is exactly when that DNS is cold — and
the verdict is written from everyone who answered. If the full story names a
different winner, the destination's next connection uses it.

UDP is never raced: it has no handshake to time, so it goes where the host's
TCP goes. A UDP flow to a host whose TCP race is still running waits for that
race to decide, for up to a second, so a browser opening a new site over TCP
and QUIC at once sends both out of the same exit; a race that nothing answers
within that second leaves the flow to the static ranking rather than holding its
first packet for the race's whole ten seconds. A UDP flow to a host with no TCP
at all takes the static ranking.

!!! note ""

    Only naive outbounds can be placed in a smart group: the destination dial
    duration the ranking depends on is reported by naive alone.

### How a chain is measured

With a relay in front, `local` covers every hop up to the innermost proxy,
the leg between the relay and its exit included. That leg belongs to neither
the client's own access nor the distance to the destination, and it slows down
every connection just the same.

All three parts come out of what the protocol already carries:

```
near  = round trip - what the first proxy said it spent    client -> relay
chain = that span  - the innermost connect                 relay  -> exit
remote= the innermost connect                              exit   -> destination

near + chain + remote = round trip
```

The innermost hop also reports how much of its own work was *finding* the
destination rather than reaching it, and the client counts that as part of
`remote`. Without it a cold name lookup at the exit falls into `chain` — a
per-destination cost charged against the node, and enough of them at once will
move a group off a member that is behaving perfectly. It is counted into
`remote` rather than dropped so the three legs still add up to the round trip,
and so that the client only ever consumes the connect and the lookup as a sum —
which means reporting one grants a hop nothing it could not already claim. An
exit too old to report it leaves the lookup in `chain`, as before.

`local` is `near + chain`. A member that dials destinations itself has an
interior of its own — its routing and name resolution, measured at well under
a millisecond — so counting it leaves direct members where they were, while a
relay whose leg to its exit degrades sees its `local` rise and its group move
to a better member.

Each leg gets the estimator its behaviour calls for. The near hop keeps a
rolling minimum: queueing and cold handshakes only push those samples up, so
the floor is the stable reading. The interior keeps a mean with the single
worst sample set aside, because what matters there is what connections pay
rather than what the path could manage at its best — a hop dropping one packet
in four is, at its best, indistinguishable from a perfect one.

The interior is kept in memory only and needs three readings to count, so after
a start it is unknown until the warmup has crossed each member three times, and
an unknown interior counts as none. The static
ranking — what a connection gets while its destination has no snapshot, and
what UDP to a host never reached over TCP rides on — therefore puts a member
whose interior has not been measured after every member measured to sit behind
a relay, once there is one: otherwise a relay nothing has crossed yet looks
adjacent, and on a deployment whose members all reach their relays through a
local leg that sent the static choice to the furthest group.

### How a node is judged reachable

Idle members are probed on a timer, and what the probe asks for depends on what
the member has been measured to be.

A member with **no interior** — one that dials destinations itself, which is
every member of a single-hop deployment — is probed against an address the
server **answers itself instead of dialing**. Nothing leaves the proxy, so the
probe is free to run as often as it likes, and the round trip it measures is
purely the leg to the proxy, which is the quantity the ranking wants. For these
members nothing downstream sees anything at all, which is exactly the behaviour
this has always had.

A member with an interior — a **relay** — is probed against a real address
instead, because two things about it cannot be settled any other way: what that
interior currently costs, and whether it is still there. A relay whose own
upstream has died answers the proxy address perfectly, because it never dials
for it, so a heartbeat against it keeps reporting healthy while it refuses every
real destination. Seen in production: real traffic failing in the same second as
a heartbeat coming back in 128ms, with the member never once condemned.

Which of the two a member is has to be measured first, and the probe address
cannot measure it. So every member is probed against a real address until its
interior has three readings — after a start, the warmup goes round up to three
times for that, a second or two before the first UDP flow would otherwise have
nothing to go on — and only then does a member found to have no interior go
back to the probe address. That is three probes per member that leave the proxy
on every start. A member the warmup condemns is not asked again by it; its
rechecks take over.

The same real addresses are used when a real dial has failed and the question is
whether to **condemn** a member, and when a condemned member asks to be **let
back in**. A condemned member — by a confirming probe, the heartbeat or a
blackholed connection alike, since a relay being reloaded, a route flapping and a
provider's blip are all over in seconds — is confirmed again 5, 15 and 45
seconds later, let back in by the first that passes, and left to the heartbeat
only if all three fail. A
member condemned for the second time within ten minutes is held out for a
minute first: nothing that answers in the meantime, probe or traffic, lets it
back in, and one more probe lands when the minute is up, whether a probe, a
heartbeat or a blackholed connection condemned it. Every readmission moves each
destination on the group to a different exit address and back, and a node that
keeps dropping out should not do that every few minutes. When nothing else is
up there is no hold, and the first answer lets it back in. Nor is there one when
a member of another group was condemned within 30 seconds of it: exits in two
countries do not fail together, so that is this end's own uplink dropping, and
every member that answers once it is back is let in. A change of the default
network forgets the earlier condemnation, since it was about the old path.

A member without an interior that answered "cannot reach it" (502) for real
traffic since its last heartbeat gets one probe of this kind as well: an exit
whose own egress is gone gives that answer for every destination, while its
probe address keeps answering. A member that has refused three different
destinations within 30 seconds, with nothing getting through in between, is
probed this way at once rather than at the next heartbeat, at most once every
30 seconds — a port scan through a relay is one refusal after another.

There are two of them (`1.1.1.1:443` and `9.9.9.9:443`), run by different
operators. One address cannot carry that verdict on its own: whatever an exit
does to a particular address it does every round, so a member judged on one
alone is either condemned for the life of the process or never condemnable at
all, and neither is a reading about the member. One refused or dropped while the
other answers is a verdict about the address; both failing is a verdict about
the exit. Both are IP literals, so a broken resolver at the proxy cannot answer
the question the wrong way, and both are anycast, so they are near wherever the
exit is.

The one failure that is *not* cross-checked is the proxy answering that its own
next hop is gone. That is a statement it made about itself, and no choice of
destination routes around the leg it is about, so a second probe would only
delay the verdict. Everything else — a refusal about the address, a timeout, an
address that is silently dropped — is asked of the second address first.

**No answer** is not the same as **dead**. When a probe runs out of time — or a
connection misses the derived bound on reaching the proxy — on a member that
real traffic, TCP or UDP, has received data through within the last second, the
member is not condemned, and one `congested` line is recorded instead. A path
saturated by a bulk download leaves new streams unanswered for tens of seconds
while the download itself keeps arriving. Seen in production: one download
filled a group's only member, the confirming probe got no answer within its two
seconds and condemned it, the download moved to the next group and filled that
one, and three countries went down one after another behind a node that had
been delivering the whole time; moving the load does not cure a queue that
travels with it. A proxy that answered — 502, 503, a refused or reset
connection — is judged as before. A member that has really died stops
delivering too: congestion is data that keeps arriving, every few tens of
milliseconds on a saturated path, and a blackhole is data that stops. A probe
runs for at least two seconds, so one that goes unanswered with nothing having
arrived in its last second condemns the member, and so does a connection that
misses the bound a second after the last data. The grace was ten seconds, which
read the traffic from just before a blackhole as the member still delivering
and held a blackholed h2 relay in service for ten seconds instead of under one.

The excuse has an end. Each `congested` episode starts one more probe of the
two real addresses, given 30 seconds each — as long as a connection waits for
its CONNECT to be answered — and if neither answers in that time, or both are
refused, the member is condemned however much it is still delivering. Queues
measured behind real bottlenecks are one to fifteen seconds; a member whose new
streams go unanswered for thirty is failing every new connection placed on it,
which is what an exit with a full connection table looks like while its old
downloads carry on. The probe's answer ends the episode but is not recorded as
a measurement, since what it timed was the queue. A download that keeps the
path full starts one such probe each time a later probe is excused again.

A connection that has waited three seconds for the proxy to answer its CONNECT
also spends a confirming probe on the member, in the background and at most
once per member every fifteen seconds. The connection keeps waiting exactly as
long as before — a slow destination is still a working one. A member that has
silently stopped answering is found without waiting for the heartbeat, and with
the rule above a member that is merely full is not condemned for it.

Traffic itself keeps a busy relay measured, and a healthy member that carried
traffic within the last 30 seconds is skipped by the heartbeat entirely. Not
longer: rounds are up to 80 seconds apart and a relay drops a session idle for
two minutes, so a member skipped for traffic a minute before could sit silent
past that, and the first request after a pause paid for a new session. So a heartbeat sends
these probes only for idle relays, for idle members that answered 502 since
their last heartbeat, and for condemned members, which are probed every round
since traffic is kept off them while anything else is up. Those are the members
whose state is otherwise unknown.

### What the ranking trusts

Both measured quantities come from the proxy. Local is the round trip measured
locally minus the span the proxy reported for its own work, and remote is what
the proxy reported the destination cost. A server that overstates its span and
reports a zero connect therefore presents itself as sitting next to the client
and next to every destination at once, and wins every comparison it enters.

Two things narrow this:

- Reports that cannot be reconciled with what the client measured are discarded.
  A span cannot outlast the round trip that contained it, and a connect cannot
  outlast the span that contained it.
- The TLS handshake is timed on the client side, before the proxy has said
  anything. A node cannot claim to be closer than its own handshake allows.

What is left is a genuine trust boundary: a node can still understate its
distance within those bounds, and a client cannot tell. Put nodes you would not
trust with the routing decision in their own group and give the groups you do
trust a `bonus`, or leave them out of the smart outbound entirely.

One asymmetry inside that boundary is worth knowing if you use `select:
fastest`. Between *groups*, overstating the destination leg costs a node — the
score charges it for the claim. Between *members of one group* only `local` is
compared, and overstating the destination leg pushes that member's own interior
down by the same amount, so there it pays. What it can win that way is bounded
by its real interior, and the same claim makes its whole group look worse from
the outside — but if the members of one group are not equally trusted, `first`
does not offer the lever at all.

### Where decisions are stored

Race results are written to the sing-box cache file, so a restart does not
re-race every destination — and does not move traffic to a different country
while it does. Enable `experimental.cache_file`; without it every restart starts
from nothing.

Each group's remote leg keeps its **last few race readings**: past three of
them it stands for their median, and before that for the newest one. An
anycast destination genuinely answers two rounds differently — the same proxy
dials it in 0.5ms one race and 150ms the next — and keeping only the last
reading freezes whichever answer that round caught for the whole snapshot
lifetime: a group wins on one lucky sample and disappoints every connection
after it, or loses on one unlucky sample and sits out hours it would have won.
Once there is history, one discordant round is outvoted by the rounds around
it, while a real change — arriving round after round — becomes the majority
quickly.

Two readings are not a median. Taking the lower of them means always
believing the more flattering round and holding it for a round longer, which
is the opposite of what keeping history is for, so below three readings the
newest measurement stands on its own.

A snapshot also records which **member** stood for each group in the round that
decided it, including the groups that never answered. A group loses its node two
ways, and they call for opposite handling:

- **Its node broke and the group fell to a backup.** The snapshot still
  describes the node the group is about to be back on, and re-racing now would
  replace a good measurement with one taken during an outage — across every
  destination the group touches, at the moment the outage is already generating
  load. It is left alone.
  An outage that lasts across two refreshes of a destination is the
  exception: the first refresh carries the group's leg, a second silent round
  retires it. The group then has no remote for that destination and cannot be
  scored, so once its node is back the destination stays on the backup until
  the snapshot's next refresh — by age or by use. A destination first raced
  while every member of the group was down starts out the same way — the
  group answered nothing, so there is no leg to score — and likewise stays on
  the backup until that refresh.
- **Its node came back, or another member measured better, and the group left
  the backup.** Now the snapshot describes the backup: a spare that answered
  slowly, or did not answer at all inside a budget derived from the node that
  usually does. That verdict was never about the group as it stands, and nothing
  else can notice it, because a group that stayed silent left no measurement
  behind. Those destinations are decided again on their next use.

Whether the node that raced it is healthy separates the two. The effect is that
a node down for five minutes stops holding onto the decisions it distorted for
the rest of the snapshot's life.

Destinations are stored under a digest rather than by name, so the file is not
readable as a browsing history. The digest is unsalted — any salt would have to
live in the same file — so someone holding the file *and* a list of candidate
hostnames can still confirm which of them appear in it. Snapshots not refreshed
for a week are dropped at startup.

### When a snapshot is verified again

A snapshot is re-raced when it grows **old** or when it has been **used
enough**. Both run in the background, and the previous answer keeps serving
until the new one lands.

Expiring on time alone covers half the problem. The groups that were not
selected are only re-measured by a race, so however much traffic a snapshot
routes, its losing legs stay exactly as old as the round that wrote them.
Measured in production: a location service polling every 26 seconds rode one
outlier sample that excluded an exit for almost five hours — 789 requests,
each paying about 155ms more than the exit that had long since recovered, with
nothing due to look at it. So a snapshot is also verified again once it has
answered a number of requests: a race costs one dial per group — plus, for a
hostname, one retake for each group whose exit had to resolve it, which at a
refresh is every group but the incumbent — which is a few
percent of the traffic that consumed the answer. An idle destination never
reaches the limit and keeps the plain time-based lifetime; a busy one spends a
fraction of its own traffic keeping its answer honest.

A refresh that fails — no group answered — is not tried again for a minute, and
the previous answer serves meanwhile. A destination that has no answer yet is
reached by the static ranking for that minute; the snapshot is per host, so
when the first connection to a host happened to be on a port every group
refuses, the host's other ports go unraced for that minute too.

Separately, the group in use is re-raced immediately when several consecutive
live measurements come back far worse than the snapshot recorded, and those
measurements join that group's remote history. Live measurements are otherwise
kept out of the snapshot — only the selected group carries traffic, and
crediting it with its own warm numbers would entrench it — with confirmed
degradation the one exception: it can only make that group look worse, never
better.

### Fields

#### alpha

==Default: 0.7==

How much the local leg is discounted, between 0 and 1.

`1.0` ranks purely by total latency. Below 1, a shorter remote leg wins when
totals are close — useful when bandwidth to the proxy is assured and what matters
is the leg between the proxy and the destination. `0` ranks on the remote leg
alone.

It is a continuous quantity rather than a threshold on purpose: a threshold flips
back and forth at its boundary, which shows up as a site being reached from a
different exit address every few seconds.

#### audit_path

==Default: off==

Where to keep a durable record of every routing decision and the measurements
behind it, one JSON object per line. Each smart outbound needs a file of its own;
two sharing one is rejected at startup.

The log is not a substitute. A route that turns out to be wrong is usually
noticed hours later, by which time the journal that would have explained it has
been rotated away — and what is left of it is scattered over dozens of lines
that have to be joined by hand. This is one line per decision, each
self-contained:

| `type` | Written when |
|---|---|
| `race` | A round of probes decided a destination. Carries the legs, bonus, bound and score of each group that took part. A group that did not take part — down while another group was up, or cooling down on this port — does not appear |
| `switch` | The group carrying a destination changed. Carries the before, the after, and the numbers that moved it. On a failover the group it left was not a candidate, so the line may have no numbers for it |
| `member` | The member carrying a group changed — the same exit-address change, without any destination changing group |
| `state` | One destination as currently remembered. Written for the whole table hourly and at shutdown |
| `usage` | What each node carried over the last ten minutes, what put the traffic there, and which destinations it went to |
| `amend` | A destination reading taken again once the exit had the name, with what it replaced and the lookup that made it worth retaking |
| `health` | A member was condemned (`to` is `down`, and its group leg carries the error under `failed`) or let back in (`to` is `up`); or a probe went unanswered on a member still delivering, which was left in service (`to` is `congested`, the silence under `failed`, once per episode); or the default network changed and every member starts measuring from cold (`to` is `reset`, no groups) |

Counts are per window rather than running totals, because a restart resets them
and a trail spanning days is mostly across one. Summing windows gives a total;
summing totals would not.

To read it back:

```
sing-box smart nodes /var/lib/sing-box/smart-audit.jsonl [--list] [--top N]
```

which reports, per node, how many requests it carried and how many destinations
it currently holds — and with `--list`, those destinations and their request
counts, busiest first, narrowed by `--top`.

Requests cover every window in the file and the destinations come from the most
recent full dump; both ranges are printed at the top. A destination's count is
its total over every node, and it is listed under the node that holds it now,
while a node's row counts only what that node carried. So once destinations
have changed node, a node's list can add up to more or less than its row: more
when it now holds destinations that carried requests on another node, less when
destinations it carried have moved to another node or left the table. A
shortfall gets a line of its own; an excess is not marked. A node that holds no
destination now is listed too.

!!! warning ""

    This is the only place smart writes destination names to disk in the
    clear; its snapshot cache stores digests on purpose. Turning this on trades
    that away for being able to check the routing against reality. The cache
    file can still hold names of its own: with `store_fakeip`, `store_rdrc` or
    `store_dns` on, its fakeip, rdrc and DNS cache buckets keep domain names in
    the clear, in a file that is usually world-readable (0644). Set
    `UMask=0077` in the service unit; a `chmod 600` does not last, since the
    file is recreated with the default mode when a corrupt one is reset.

#### audit_size

==Default: `64MB`==

How large the trail grows before it is rotated, keeping one previous
generation — so the footprint is twice this and no more.

#### groups

The list of groups. A group asserts that **its members have the same remote leg
to any given destination** — usually because they share an egress network.

A group is probed once, stores one remote measurement, and never ranks its own
members against each other. Nodes on different egress ranges, taking different
paths to the destination, belong in separate groups.

#### groups.tag

The group's name. Required.

#### groups.outbounds

The group's outbound tags. All must be naive.

#### groups.select

==Default: `first`==

Which member of the group to use:

| Value | Behaviour |
|---|---|
| `first` | The first healthy member in configured order; moves only on failure and returns on recovery |
| `fastest` | The healthy member with the lowest local latency |

Exactly one member is used at a time, never a rotation — two connections to the
same destination leaving from different addresses is what breaks logged-in
sessions and trips risk checks.

#### groups.bonus

==Default: `0`==

How much extra latency using this group is worth.

Latency cannot measure whether an exit address is a good one to be seen from, so
that judgement is declared here. It decides the outcome when remote is small —
CDN and anycast destinations, where every group sees a millisecond or two — and is
drowned out when remote is large, so no CDN detection is needed anywhere.

The value follows from the measurements: if node A is 29ms away and node B is
80ms away with `alpha` at 0.7, B needs a bonus of `0.7 × (80 − 29) ≈ 36ms` to win
when their remote legs are comparable.

A negative value is accepted and works as a penalty: it raises the group's score
and its static bound by that much.
