package group

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"
)

// The smart outbound: the lifecycle, the dial paths, and the view of the groups
// it hands to the decision core.
//
// The rest of it lives beside this file. smart_decision.go ranks; smart_member.go
// holds what a group and a member remember between decisions; smart_race.go
// measures a destination nobody has measured yet; smart_replay.go carries a
// connection whose verdict has not arrived; smart_cache.go remembers the
// verdicts; smart_heartbeat.go keeps idle members measured; smart_audit.go
// writes down what was decided and why.

func RegisterSmart(registry *outbound.Registry) {
	outbound.Register[option.SmartOutboundOptions](registry, C.TypeSmart, NewSmart)
}

var (
	_ adapter.OutboundGroup           = (*Smart)(nil)
	_ adapter.InterfaceUpdateListener = (*Smart)(nil)
	_ adapter.Lifecycle               = (*Smart)(nil)
)

// udpRaceFollowLimit is how long a UDP flow to a host being raced waits for
// the race before it leaves by the static ranking. A race decides on its first
// answer, a round trip of the nearest exit's; past a second nothing is
// answering, and the race may run on for raceDialTimeout.
const udpRaceFollowLimit = time.Second

const (
	defaultAlpha = 0.7

	// heartbeatInterval is how often an idle member is pinged. It only fills
	// gaps: a member that carried real traffic within heartbeatSkipWindow is
	// skipped, so the traffic scales with idleness rather than with the number
	// of members.
	//
	// It can be this sparse because no connection waits on it. A connection to
	// a member that has died, or that refuses it, is carried on by failing over
	// to another group, not by the heartbeat. A member whose proxy stops
	// answering is marked down when a dial finds it, not on the next round;
	// one whose proxy answers 502 for everything is checked against a real
	// address once it has refused a few destinations in a row, and on the
	// next round if it refused fewer.
	//
	// What does wait on it is a recovery. A condemned member gets no traffic
	// while anything else can carry it — a group with every member down is
	// left out while another group is up — so the heartbeat is what lets it
	// back in, and until then the traffic it would carry runs on the groups
	// that are up. Only when nothing is up does the next request find it,
	// because a group with nothing healthy still offers a member to try.
	// Beyond that the heartbeat keeps idle connection pools warm, and those
	// live for minutes.
	heartbeatInterval = 60 * time.Second

	// heartbeatJitter spreads each round either way.
	//
	// The contents are encrypted, but the shape is not: an otherwise idle
	// connection carrying a small request and a small response exactly every
	// sixty seconds is a periodicity anyone watching the flow can pick out
	// without decrypting anything. Scattering the moment costs nothing here —
	// no connection waits on a round, and a recovered member is let back in
	// as soon on average — and leaves no period to lock onto.
	heartbeatJitter = heartbeatInterval / 3

	// heartbeatSkipWindow is how recently a member must have carried traffic
	// for a round to skip it. A relay closes a session idle for 120 seconds,
	// and rounds come up to heartbeatInterval+heartbeatJitter, 80 seconds,
	// apart: skipped for traffic up to the whole interval before, a member
	// could sit silent for 140 seconds, and the first request after a two
	// minute pause paid a new session's two round trips. 30 + 80 = 110, the
	// time the pool trusts an idle session for.
	heartbeatSkipWindow = 30 * time.Second

	// cacheFlushInterval is how often changed snapshots reach disk.
	cacheFlushInterval = 30 * time.Second

	// probeConcurrency bounds how many members are dialed at once.
	//
	// Every member is dialed at the same moment otherwise, which on a small
	// router is dozens of TLS handshakes competing for the same cores. What
	// comes back is then a handshake time that measures contention rather than
	// distance, and the handshake is what bounds how close a member may claim
	// to be — so a starved one is filed as further away than it is,
	// and stays that way until something cheaper is seen. Since a member that
	// is ranked too far away stops being dialed, that can be never.
	probeConcurrency = 4

	// condemnationRecheck is how long after a confirming probe condemned a
	// member its verdict is first checked again, before the heartbeat would.
	//
	// A relay being reloaded closes its listener, reads its configuration and
	// opens it again: tens to hundreds of milliseconds in which a dial is
	// refused — and the GOAWAY that the old instance sent makes the client dial
	// at exactly that moment. The confirming probe lands in the same gap, so
	// the member is condemned on a node that is back before the verdict is
	// logged, and stays condemned until the heartbeat: 40–80 seconds, during
	// which a single-member group runs on its backup in another country, on
	// every reload.
	//
	// It is the first of recheckSchedule, which then backs off.
	condemnationRecheck = 5 * time.Second

	// flapWindow and flapHoldFactor damp a member that keeps dropping out. A
	// member condemned for the second time within flapWindow is not let back
	// in until flapHoldFactor × condemnationRecheck — a minute — after that
	// condemnation; see Smart.markUp. A multiple of the recheck rather than a
	// duration of its own so a test that shortens the recheck shortens the
	// hold, and the probe that ends it, with it.
	flapWindow     = 10 * time.Minute
	flapHoldFactor = 12

	// flapCommonCause is how close a condemnation in another group has to be
	// for both to be read as one outage at this end rather than this member
	// flapping; see Smart.holding. Members carrying traffic are condemned
	// within seconds of an uplink dropping, and thirty seconds still misses an
	// idle one that only a heartbeat finds.
	flapCommonCause = 30 * time.Second

	// congestionGrace is how recently a member must have delivered payload for
	// a probe's silence to be read as congestion rather than death; see
	// Smart.congested. Congestion is data that keeps arriving: a download on
	// a saturated path reads every few tens of milliseconds (64KB is about
	// 50ms at 10Mbps, and one 16KB HTTP/2 frame lands every 130ms even at
	// 1Mbps). A blackhole is data that stops. The grace is shorter than
	// probeTimeoutFloor, so a probe that runs out its budget with nothing
	// arriving in the last second has watched the stop, and condemns; so does
	// a connection missing the establishment bound a second after the last
	// data. Only a saturated path below about 130kbps leaves a second between
	// two frames, and is read as dead.
	//
	// It was ten seconds, which counted the deliveries from just before a
	// blackhole as the member still delivering. Measured end to end over real
	// cronet: an h2 relay blackholed with its session kept or cut was
	// condemned after 10.0s instead of 1.0s and 0.2s (UDP over h2: 10.03s
	// instead of 1.0s), and 300 new destinations arriving three seconds after
	// the last delivery failed over 252 times, each paying the establishment
	// bound, where one condemnation had held it to 48.
	//
	// deliveryStampInterval is how stale the stamp may be allowed to get to
	// keep the reads that write it cheap.
	congestionGrace       = time.Second
	deliveryStampInterval = 100 * time.Millisecond

	// settleSuspicion is how long a settle may wait for the proxy's answer
	// before the member is suspected and a confirming probe is spent on it, in
	// the background; see Smart.suspect. The connection keeps waiting either
	// way — a distant destination is slow, not broken — so this is only how
	// long a member that has silently died can go on swallowing new
	// connections before anything checks. Fixed rather than derived: what it
	// waits on includes the destination's own connect, which nothing measured
	// for the member bounds, and a probe spent on a slow destination costs one
	// dial. suspicionInterval bounds those to one per member per window.
	settleSuspicion   = 3 * time.Second
	suspicionInterval = 15 * time.Second

	// refusalBurst and refusalWindow catch an exit that has lost its own egress,
	// which answers 502 for every destination while its probe address keeps
	// answering. Refusals of this many different destinations within the
	// window, with nothing getting through in between, spend a confirming
	// probe on the member at once rather than leaving it to the next
	// heartbeat; see smartMember.noteRefusal.
	//
	// refusalProbeInterval bounds those probes to one per member per window:
	// a port scan through a relay is refusal after refusal, and spent a probe
	// on every third.
	refusalBurst         = 3
	refusalWindow        = 30 * time.Second
	refusalProbeInterval = 30 * time.Second

	// readyTimeoutFloor and readyTimeoutFactor bound how long a dial may spend
	// establishing the connection to the proxy before the leg is called broken.
	// A blackholed route fails only when TCP gives up, which is far too late to
	// be useful, so the bound is derived from what establishing this member's
	// connection has actually cost — see smartMember.readyTimeout.
	readyTimeoutFloor  = 200 * time.Millisecond
	readyTimeoutFactor = 3

	// coldSetupFloor is the smallest Setup that can plausibly have contained a
	// handshake. Below it the stream landed on an already-established
	// connection, and a sample like that says nothing about what establishing
	// one costs — averaged in, it would drive the establishment bound towards
	// zero exactly on the members whose pools are healthiest.
	coldSetupFloor = 5 * time.Millisecond

	// localFloorDivisor turns the cheapest handshake seen for a member into a
	// lower bound on how far away it can be.
	//
	// The client-to-proxy leg is the round trip measured here minus the span the
	// proxy claimed for itself, so a proxy that overstates its own work reports
	// itself as arbitrarily close and captures every routing decision. The
	// handshake is the part it cannot touch: it is timed on this side before the
	// proxy has said anything, and TCP plus TLS costs two to four round trips to
	// complete. Dividing by five is deliberately generous — it never corrects an
	// honest node, and it caps how close a dishonest one can claim to be.
	localFloorDivisor = 5

	// probeTimeout* budget a heartbeat. Cold, a connection pays a handshake on
	// top of the round trip, so the budget scales with the member's distance;
	// probeTimeoutCold applies before anything has been measured.
	probeTimeoutFloor  = 2 * time.Second
	probeTimeoutFactor = 8
	probeTimeoutCold   = 10 * time.Second

	// raceConcurrency bounds how many destinations may race at once. The single
	// flight bounds rounds per destination; nothing bounded how many
	// destinations one page load can introduce, and a race is a dial per group.
	// Seen in production: a page brought a hundred ad-tech hosts in five
	// seconds, five hundred tunnel opens landed on the nodes together, and two
	// of them started resetting connections under the load — which condemned
	// them, which put every race still running up against their backups, which
	// wrote a quarter of the table as verdicts about the wrong nodes.
	//
	// The overflow is not queued: a caller is waiting, and the dial it gets on
	// the static ranking is the same one it would have gotten before any race
	// had run. The destination simply stays undecided until a calmer moment —
	// an unraced destination races on its next use, a stale one keeps its
	// snapshot until its own refresh gets a slot.
	raceConcurrency = 8
)

type Smart struct {
	outbound.Adapter
	ctx        context.Context
	logger     log.ContextLogger
	outbound   adapter.OutboundManager
	connection adapter.ConnectionManager
	alpha      float64
	groups     []*smartGroup
	cache      *smartCache
	audit      *smartAudit
	cancel     context.CancelFunc
	// pause reports whether the device is asleep, so the heartbeat does not
	// wake the radio to probe a network that is torn down — and does not read
	// the timeouts it collects there as members having died.
	pause pause.Manager
	// probeSlots bounds probe dials across the whole outbound rather than per
	// round, so warmup and a heartbeat overlapping cannot between them recreate
	// the handshake storm the bound exists to prevent.
	probeSlots     chan struct{}
	probeSlotsOnce sync.Once
	// recheckAfter overrides condemnationRecheck, and with it the whole recheck
	// schedule and the flap hold; zero means the constant. suspectAfter does the
	// same for settleSuspicion, and raceFollowLimit for udpRaceFollowLimit.
	// Set by tests, which run in parallel and cannot share a package variable.
	recheckAfter    time.Duration
	suspectAfter    time.Duration
	raceFollowLimit time.Duration
	// closed is raised by Close. s.ctx is the box's and outlives this
	// outbound when an embedder closes it without cancelling that first, and
	// the derived context is not stored where everything can read it.
	closed atomic.Bool
	// raceSlots bounds destinations racing at once, as probeSlots bounds probe
	// dials. See raceConcurrency for what happens without it.
	raceSlots     chan struct{}
	raceSlotsOnce sync.Once
	// amendQueued counts the cold-resolution retakes waiting on or holding a
	// probe slot. See amendBacklog.
	amendQueued atomic.Int32
	// pathAccess guards pathRound, which cancels the probe round the last
	// interface change started. See InterfaceUpdated.
	pathAccess sync.Mutex
	pathRound  context.CancelFunc
	// usageSince is when the window now open began, in nanoseconds. On the Smart
	// rather than inside the loop that ticks it, because the loop is not the only
	// thing that closes a window: shutdown closes the last one, and reaching for
	// the process start there reported a three-minute window as fourteen hours.
	usageSince atomic.Int64
	// races maps a destination to the round of probes in flight for it, so a
	// page load opening eight connections at once produces one round and seven
	// waiters rather than eight rounds. See Smart.raceOrWait.
	races sync.Map
	// raceIgnoresDeadline and raceGrace are set only by tests, whose outbounds
	// claim milliseconds they do not take: on a loaded machine the scheduler
	// can deliver an answer later than a deadline or grace derived from those
	// claims. raceIgnoresDeadline ends a race only when every group has
	// answered or been pruned on its static bound; raceGrace, when non-zero,
	// replaces raceAnswerGrace.
	raceIgnoresDeadline bool
	raceGrace           time.Duration
}

func NewSmart(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.SmartOutboundOptions) (adapter.Outbound, error) {
	if len(options.Groups) == 0 {
		return nil, E.New("missing groups")
	}
	alpha := defaultAlpha
	if options.Alpha != nil {
		alpha = *options.Alpha
	}
	if alpha < 0 || alpha > 1 {
		return nil, E.New("alpha must be between 0 and 1")
	}

	var all []string
	seenGroups := make(map[string]bool, len(options.Groups))
	seenMembers := make(map[string]string, len(options.Groups))
	groups := make([]*smartGroup, 0, len(options.Groups))
	for index, groupOptions := range options.Groups {
		if groupOptions.Tag == "" {
			return nil, E.New("group ", index, ": missing tag")
		}
		if seenGroups[groupOptions.Tag] {
			// Snapshots are filed per group tag, so two groups sharing one would
			// overwrite each other's measurements and be scored on whichever
			// wrote last.
			return nil, E.New("duplicate group tag: ", groupOptions.Tag)
		}
		seenGroups[groupOptions.Tag] = true
		if len(groupOptions.Outbounds) == 0 {
			return nil, E.New("group ", groupOptions.Tag, ": missing outbounds")
		}
		var selectFastest bool
		switch groupOptions.Select {
		case "", "first":
		case "fastest":
			selectFastest = true
		default:
			return nil, E.New("group ", groupOptions.Tag, ": unknown select: ", groupOptions.Select)
		}
		members := make([]*smartMember, 0, len(groupOptions.Outbounds))
		for _, memberTag := range groupOptions.Outbounds {
			if owner, taken := seenMembers[memberTag]; taken {
				// A group asserts that its members share a destination leg, so
				// one outbound in two groups asserts something contradictory —
				// and gives that node two independent health states and two
				// measurement windows, which drift apart.
				return nil, E.New("outbound ", memberTag, " is in both group ",
					owner, " and group ", groupOptions.Tag)
			}
			seenMembers[memberTag] = groupOptions.Tag
			members = append(members, &smartMember{tag: memberTag})
			all = append(all, memberTag)
		}
		groups = append(groups, &smartGroup{
			tag:           groupOptions.Tag,
			bonus:         time.Duration(groupOptions.Bonus),
			selectFastest: selectFastest,
			members:       members,
		})
	}

	smart := &Smart{
		Adapter:    outbound.NewAdapter(C.TypeSmart, tag, []string{N.NetworkTCP, N.NetworkUDP}, all),
		ctx:        ctx,
		logger:     logger,
		outbound:   service.FromContext[adapter.OutboundManager](ctx),
		connection: service.FromContext[adapter.ConnectionManager](ctx),
		alpha:      alpha,
		groups:     groups,
		cache:      newSmartCache(ctx, tag),
		probeSlots: make(chan struct{}, probeConcurrency),
		raceSlots:  make(chan struct{}, raceConcurrency),
		audit:      newSmartAudit(ctx, logger, options.AuditPath, int64(options.AuditSize.Value())),
		pause:      service.FromContext[pause.Manager](ctx),
	}
	for _, group := range groups {
		group.onMemberChange = smart.auditMember
	}
	if err := smart.sharedAuditPath(); err != nil {
		return nil, err
	}
	return smart, nil
}

func (s *Smart) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	switch stage {
	case adapter.StartStateStart:
		// Registered here so that Close still runs when a later stage fails.
		scope.Add(s.Close)
		return s.start()
	case adapter.StartStateStarted:
		return s.PostStart()
	}
	return nil
}

// sharedAuditPath refuses a trail another smart outbound already writes. Two
// trails in one file interleave unlabelled, and each one's rotation renames
// the file out from under the other: measured, 47 of 80 records ended up in
// the wrong file or nowhere.
//
// Checked at construction so that `sing-box check` catches it: a reload checks
// the new configuration, then closes the running one, then starts the new one,
// and a refusal at start is a process that exits. The manager constructs an
// outbound before listing it, so every smart outbound configured earlier is
// already there — and so, when one replaces another of the same tag, is the
// one it replaces, which is no conflict.
func (s *Smart) sharedAuditPath() error {
	if s.audit == nil || s.outbound == nil {
		return nil
	}
	for _, other := range s.outbound.Outbounds() {
		if peer, isSmart := other.(*Smart); isSmart && peer.Tag() != s.Tag() && peer.audit != nil && peer.audit.path == s.audit.path {
			return E.New("smart outbounds ", peer.Tag(), " and ", s.Tag(), " share audit_path ", s.audit.path)
		}
	}
	return nil
}

func (s *Smart) start() error {
	for _, group := range s.groups {
		for _, member := range group.members {
			detour, loaded := s.outbound.Outbound(member.tag)
			if !loaded {
				return E.New("outbound not found: ", member.tag)
			}
			if detour.Type() != C.TypeNaive {
				// The ranking is built on the destination dial duration that
				// only naive reports; anything else would have to be scored by
				// guesswork, and a guess mixed into measurements is worse than
				// no entry at all.
				return E.New("outbound ", member.tag, " is ", detour.Type(),
					", only naive outbounds can be used in a smart group")
			}
			member.outbound = detour
			member.healthy.Store(true)
		}
	}
	return nil
}

func (s *Smart) PostStart() error {
	ctx, cancel := context.WithCancel(s.ctx)
	s.cancel = cancel
	s.usageSince.Store(time.Now().UnixNano())
	// Read the stored decisions first: they are what stops a restart from
	// re-racing everything, and moving traffic to a different country while it
	// does. It has to happen inline — the listeners are already accepting by the
	// time this runs.
	s.cache.preload()
	if s.cache.db() == nil {
		// Not fatal, but worth saying out loud: without it every restart starts
		// from nothing and re-races every destination, which is exactly the
		// behaviour the snapshots exist to prevent.
		s.logger.Warn("cache file is not enabled; smart decisions will not survive a restart")
	}
	// Reaching every member once opens its first pool, so the first real
	// request does not pay for a TLS handshake it could have avoided, and the
	// first race compares nodes that are all equally warm. It runs in the
	// background because blocking here would hold up the rest of the start;
	// until it lands, dials fall back to configured order rather than failing.
	// Separately, not one after the other. Warmup is bounded work against
	// members that may all be unreachable, and every dial to one of those waits
	// out the cold probe budget — so chaining the periodic jobs behind it means
	// a start on a dead network gets no cache flush and no heartbeat for as long
	// as that takes.
	go s.warmup(ctx)
	go s.loop(ctx)
	return nil
}

func (s *Smart) Close() error {
	s.closed.Store(true)
	// The trail's close budget starts now rather than after the cache flush
	// below, so that a slow disk costs the two together the budget and not
	// each its own: the command line kills the process at ten seconds.
	s.audit.finishing()
	if s.cancel != nil {
		s.cancel()
	}
	s.pathAccess.Lock()
	if s.pathRound != nil {
		s.pathRound()
	}
	s.pathAccess.Unlock()
	// The loop's timer writes snapshots every half minute, so this is what
	// keeps the last of them. The cache file service may have closed its
	// database first, and failing to write into a closed database is not a
	// failure of this outbound.
	if err := s.cache.flush(); err != nil {
		s.logger.Debug("flush smart cache on close: ", err)
	}
	// The last things written are the whole table and whatever the final window
	// carried, so the trail ends with what this run believed and did rather than
	// with whatever happened to move last.
	s.auditUsage()
	s.auditState()
	return s.audit.Close()
}

// Now names the outbound currently in use, and All names every outbound that
// could be. They have to be drawn from the same set: a dashboard marks the entry
// in All that equals Now, so a Now built as "group/member" matches nothing and
// the panel shows a group with no selection at all.
func (s *Smart) Now() string {
	_, member := s.best()
	if member == nil {
		return ""
	}
	return member.tag
}

func (s *Smart) All() []string {
	var all []string
	for _, group := range s.groups {
		for _, member := range group.members {
			all = append(all, member.tag)
		}
	}
	return all
}

// candidates builds the decision-core view of the groups, using the cached
// snapshot for the destination when there is one.
//
// A group whose member has no live measurement is still offered, marked as
// having no local. Dropping it — which is what this used to do — leaves nothing
// to race in the window between the listeners opening and the warmup round
// landing, and every connection in that window fails with "no usable group"
// rather than merely being routed on incomplete information.
//
// A group whose every member is down is left out while another group is up.
// It used to be offered like any other, so on a single-member group each
// connection paid the full readiness timeout before failing over — the
// condemnation changed nothing about where traffic went. Its recovery no longer
// needs live traffic to be noticed: the heartbeat confirms down members on a
// real address. With nothing up anywhere it is offered all the same, since
// something has to be tried — but a group that is up and cooling down from
// refusing this destination counts as up. It has answered, and a group that is
// down cannot improve on the answer: offering it there cost every connection a
// readiness timeout and ended in this node's fault instead of the
// destination's.
func (s *Smart) candidates(entry *destinationEntry, port uint16, now time.Time) []candidate {
	candidates := make([]candidate, 0, len(s.groups))
	var down []candidate
	refused := false
	for _, group := range s.groups {
		member := group.current()
		if member == nil {
			continue
		}
		if entry != nil && entry.blocked(group.tag, port, member, now) {
			refused = refused || member.healthy.Load()
			continue
		}
		c := candidate{
			tag:            group.tag,
			member:         member.tag,
			bonus:          group.bonus,
			setupAllowance: member.setup(),
		}
		c.local, c.hasLocal = member.local()
		c.chain, c.hasChain = member.chain()
		if entry != nil {
			if remote, found := entry.remoteFor(group.tag); found {
				c.remote, c.hasRemote = remote, true
			}
			if !c.hasLocal {
				// Nothing measured live, but this destination was raced once and
				// the snapshot kept what each group's leg cost then. A stale
				// measurement is still a measurement, and it is what lets a
				// restart route from its cache instead of re-racing everything.
				c.local, c.hasLocal = entry.pathFor(group.tag)
			}
		}
		if !member.healthy.Load() {
			// current() only hands back a member that is down when the whole
			// group is.
			down = append(down, c)
			continue
		}
		candidates = append(candidates, c)
	}
	if len(candidates) == 0 && !refused {
		return down
	}
	return candidates
}

// chooseGroup picks a group from what is already known: the scored choice when
// the snapshot supports one, the static ranking when it does not.
//
// Every path that is not racing needs exactly this, and each had grown its own
// copy — which is how one of them ended up passing a different incumbent than
// the others by accident rather than by intent.
func (s *Smart) chooseGroup(candidates []candidate, incumbent string) (string, string) {
	if selected, reason := selectGroup(s.alpha, candidates, incumbent); selected != "" {
		return selected, reason
	}
	return fallbackGroup(s.alpha, candidates), reasonFallback
}

// incumbentOf is the group a snapshot has in use, or none when there is no
// snapshot. A failover passes none on purpose: hysteresis holds a destination
// on the group it is using, and that is the group being moved away from.
func incumbentOf(entry *destinationEntry) string {
	if entry == nil {
		return ""
	}
	return entry.selected()
}

// supersededRace reports whether this destination was decided against nodes
// that have since been replaced by better ones — not merely different ones.
//
// The distinction is the whole point. A group loses its node two ways, and they
// call for opposite handling:
//
// Its node broke, and the group fell to a backup. The snapshot still describes
// the node the group is about to be back on, and re-racing now would replace a
// good measurement with one taken during an outage — on every destination the
// group touches, at the moment the outage is already generating load. Leave it.
//
// Its node came back, or simply measured better, and the group left the backup.
// Now the snapshot describes the backup: a spare that answered slowly, or —
// the case that motivated this — did not answer at all inside a budget derived
// from the node that usually does. That verdict was never about the group as it
// stands, and nothing else can notice, because a group that stayed silent left
// no leg for the drift check to compare against.
//
// Whether the node that raced it is healthy separates the two exactly.
func (s *Smart) supersededRace(entry *destinationEntry, candidates []candidate, now time.Time) bool {
	superseded := false
	for _, raced := range entry.racedAgainstOthers(candidates) {
		if member := s.member(raced); member != nil && member.healthy.Load() {
			superseded = true
			break
		}
	}
	if !superseded {
		superseded = s.refusedByCondemned(entry, candidates)
	}
	if !superseded {
		return false
	}
	// Through the same backoff as any other refresh: a group changing members
	// must not turn every connection to every destination it touched into a
	// round of its own.
	return entry.attemptDue(now)
}

// refusedByCondemned reports a group missing from the snapshot because the
// member that stood for it refused the round, and was condemned soon after.
//
// An exit that loses its egress answers 502 for everything, and a 502 is filed
// against the destination — until the heartbeat, a minute or two later, finds
// the member itself is gone. A destination first raced inside that window
// lost the group for its whole snapshot lifetime: the backup that took over
// was never asked, and the member coming back changes nothing the snapshot
// records. Its refusals were about the member, so the round is about a
// different set of proxies than the one it is being applied to.
//
// Bounded to condemnations within a few heartbeats of the round. Later ones are
// a different outage, and without the bound every snapshot that ever missed
// the group would re-race each time its member went down.
func (s *Smart) refusedByCondemned(entry *destinationEntry, candidates []candidate) bool {
	entry.access.Lock()
	racedAt := entry.RacedAt.UnixNano()
	var suspects []string
	for _, c := range candidates {
		if _, measured := entry.Remote[c.tag]; !measured {
			suspects = append(suspects, entry.Members[c.tag])
		}
	}
	entry.access.Unlock()
	// Read outside the entry's lock: members are not the entry's to guard.
	for _, raced := range suspects {
		if member := s.member(raced); member != nil {
			downAt := member.downAt.Load()
			if downAt > racedAt && downAt-racedAt < int64(3*heartbeatInterval) {
				return true
			}
		}
	}
	return false
}

func (s *Smart) member(tag string) *smartMember {
	for _, group := range s.groups {
		for _, member := range group.members {
			if member.tag == tag {
				return member
			}
		}
	}
	return nil
}

func (s *Smart) group(tag string) *smartGroup {
	for _, group := range s.groups {
		if group.tag == tag {
			return group
		}
	}
	return nil
}

func (s *Smart) best() (*smartGroup, *smartMember) {
	candidates := s.candidates(nil, 0, time.Now())
	if len(candidates) == 0 {
		if len(s.groups) == 0 {
			return nil, nil
		}
		group := s.groups[0]
		return group, group.current()
	}
	group := s.group(fallbackGroup(s.alpha, candidates))
	if group == nil {
		return nil, nil
	}
	return group, group.current()
}

// destinationKey is what a snapshot is filed under. Port is excluded: the same
// host on 80 and 443 is the same machine at the same distance, and splitting
// them would just double the races.
//
// It is a digest rather than the host itself, because the snapshots are written
// to disk and a list of every destination a client has chosen a route for is a
// browsing history. sing-box's cache file is not encrypted, so anything readable
// in it is readable by whoever picks the device up, by a backup, or by an
// unrelated process.
//
// The protection this gives is worth stating exactly, since it is easy to
// overrate: the digest is unsalted — any salt would have to live in the same
// file — so somebody holding the file *and* a list of candidate hostnames can
// still confirm which of them are in it. What it removes is the file being
// legible as a history to anyone who merely opens it.
//
// It also has to be stable across restarts, which is the other reason for a
// plain digest over anything keyed.
func destinationKey(destination M.Socksaddr) string {
	host := destination.AddrString()
	if destination.IsFqdn() {
		host = destination.Fqdn
	}
	digest := sha256.Sum256([]byte(host))
	return hex.EncodeToString(digest[:16])
}

func (s *Smart) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	switch N.NetworkName(network) {
	case N.NetworkTCP:
		return s.dialTCP(ctx, destination)
	case N.NetworkUDP:
		// UDP has no handshake to time, so it cannot be ranked on its own. It
		// rides on the decision made for the same host over TCP, which also
		// keeps HTTP/3 and HTTP/2 to one site leaving from one address.
		key, group, member := s.groupFor(ctx, destination, N.NetworkUDP)
		if member == nil || !member.supportsUDP() {
			return nil, errNoUDPGroup(destination)
		}
		s.logger.DebugContext(ctx, "udp to ", destination, " via ", group.tag, "/", member.tag, " by=", reasonRideAlong)
		conn, err := member.outbound.DialContext(ctx, network, destination)
		if err != nil {
			return nil, err
		}
		// Counted once it is open, as TCP is: a flow that never opened carried
		// nothing.
		s.countRequest(key, group, member, reasonRideAlong, N.NetworkUDP)
		return member.udp.NewConn(&datagramConn{Conn: conn, member: member}, interrupt.IsExternalConnectionFromContext(ctx), interrupt.IsResourceDownloadFromContext(ctx)), nil
	default:
		return nil, E.Extend(N.ErrUnknownNetwork, network)
	}
}

func (s *Smart) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	key, group, member := s.groupFor(ctx, destination, N.NetworkUDP)
	if member == nil || !member.supportsUDP() {
		return nil, errNoUDPGroup(destination)
	}
	s.logger.DebugContext(ctx, "packet to ", destination, " via ", group.tag, "/", member.tag, " by=", reasonRideAlong)
	conn, err := member.outbound.ListenPacket(ctx, destination)
	if err != nil {
		return nil, err
	}
	s.countRequest(key, group, member, reasonRideAlong, N.NetworkUDP)
	return member.udp.NewPacketConn(&datagramPacketConn{NetPacketConn: bufio.NewPacketConn(conn), member: member},
		interrupt.IsExternalConnectionFromContext(ctx), interrupt.IsResourceDownloadFromContext(ctx)), nil
}

// datagramConn and datagramPacketConn stamp the member a UDP flow runs through
// whenever a datagram arrives, as replayConn does for TCP. Without it a QUIC
// download over udp_over_tcp filling a member's path was not delivering as far
// as Smart.congested could tell: the probe that timed out behind it condemned
// the member, and the condemnation cut the download with it.
//
// They create their upstream's read waiters and wrap them rather than offering
// themselves as replaceable, which would let the copy loop read past the stamp.
// Upstream stays exposed for what casts through it. Two types because the
// stream and the packet read-waiter creators share a method name.
type datagramConn struct {
	net.Conn
	member *smartMember
}

func (c *datagramConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.member.noteDelivered()
	}
	return n, err
}

func (c *datagramConn) CreateReadWaiter() (N.ReadWaiter, bool) {
	waiter, created := bufio.CreateReadWaiter(c.Conn)
	if !created {
		return nil, false
	}
	return &datagramReadWaiter{ReadWaiter: waiter, member: c.member}, true
}

func (c *datagramConn) Upstream() any { return c.Conn }

type datagramReadWaiter struct {
	N.ReadWaiter
	member *smartMember
}

func (w *datagramReadWaiter) WaitReadBuffer() (*buf.Buffer, error) {
	buffer, err := w.ReadWaiter.WaitReadBuffer()
	if err == nil {
		w.member.noteDelivered()
	}
	return buffer, err
}

type datagramPacketConn struct {
	N.NetPacketConn
	member *smartMember
}

func (c *datagramPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, addr, err := c.NetPacketConn.ReadFrom(p)
	if err == nil {
		c.member.noteDelivered()
	}
	return n, addr, err
}

func (c *datagramPacketConn) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	destination, err := c.NetPacketConn.ReadPacket(buffer)
	if err == nil {
		c.member.noteDelivered()
	}
	return destination, err
}

func (c *datagramPacketConn) CreateReadWaiter() (N.PacketReadWaiter, bool) {
	waiter, created := bufio.CreatePacketReadWaiter(c.NetPacketConn)
	if !created {
		return nil, false
	}
	return &datagramPacketReadWaiter{PacketReadWaiter: waiter, member: c.member}, true
}

func (c *datagramPacketConn) Upstream() any { return c.NetPacketConn }

type datagramPacketReadWaiter struct {
	N.PacketReadWaiter
	member *smartMember
}

func (w *datagramPacketReadWaiter) WaitReadPacket() (*buf.Buffer, M.Socksaddr, error) {
	buffer, destination, err := w.PacketReadWaiter.WaitReadPacket()
	if err == nil {
		w.member.noteDelivered()
	}
	return buffer, destination, err
}

// groupFor resolves a destination to a group without racing, for the paths that
// cannot race: UDP, and anything asked for before a snapshot exists.
//
// A group whose member in use cannot carry UDP is left out rather than swapped
// for a sibling that can: swapping would send the same destination out of two
// different addresses depending on the protocol, which is the split the grouping
// exists to prevent.
func (s *Smart) groupFor(ctx context.Context, destination M.Socksaddr, network string) (string, *smartGroup, *smartMember) {
	key := destinationKey(destination)
	if s.audit != nil {
		s.audit.note(key, destination.String())
	}
	entry := s.cache.load(key)
	if entry == nil || !entry.measured() {
		// A browser that knows a host speaks h3 opens it over TCP and QUIC at
		// once. Chosen now, the QUIC flow takes the static ranking while the
		// TCP connection takes the race's winner: one site, two exits.
		if run, racing := s.races.Load(key); racing {
			// Bounded: a race that nothing answers runs for raceDialTimeout,
			// and holding a QUIC flow's first packet that long is worse than
			// sending it by the static ranking.
			limit := time.NewTimer(cmp.Or(s.raceFollowLimit, udpRaceFollowLimit))
			select {
			case <-run.(*raceRun).decided:
			case <-limit.C:
			case <-ctx.Done():
			}
			limit.Stop()
			entry = s.cache.load(key)
		}
	}
	now := time.Now()
	candidates := s.usableCandidates(entry, destination.Port, now, network)
	if len(candidates) == 0 {
		group, member := s.best()
		return key, group, member
	}
	selected, _ := s.chooseGroup(candidates, incumbentOf(entry))
	group := s.group(selected)
	if group == nil {
		group, member := s.best()
		return key, group, member
	}
	return key, group, group.current()
}

// usableCandidates is candidates narrowed to the groups that can carry network.
//
// For UDP the narrowing comes after candidates has already left out the groups
// that are down or cooling, so when the only groups that can carry UDP are
// among those, nothing is left — and with members that do not all have
// udp_over_tcp, that turned one group being down into every UDP flow failing.
// What can carry it is offered anyway then, as candidates offers what is down
// when nothing is up: UDP has no refusal to wait for and no replay to lose.
func (s *Smart) usableCandidates(entry *destinationEntry, port uint16, now time.Time, network string) []candidate {
	candidates := s.candidates(entry, port, now)
	if network != N.NetworkUDP {
		return candidates
	}
	usable := candidates[:0]
	for _, c := range candidates {
		group := s.group(c.tag)
		if group == nil {
			continue
		}
		if member := group.current(); member != nil && member.supportsUDP() {
			usable = append(usable, c)
		}
	}
	if len(usable) > 0 {
		return usable
	}
	// Up before down: a group cooling on this port over TCP can still carry
	// the flow, one that is down most likely cannot.
	var down []candidate
	for _, group := range s.groups {
		if member := group.current(); member != nil && member.supportsUDP() {
			c := candidate{tag: group.tag, member: member.tag, bonus: group.bonus}
			c.local, c.hasLocal = member.local()
			if member.healthy.Load() {
				usable = append(usable, c)
			} else {
				down = append(down, c)
			}
		}
	}
	if len(usable) > 0 {
		return usable
	}
	return down
}

func (s *Smart) NewConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	s.connection.NewConnection(ctx, s, conn, metadata, onClose)
}

func (s *Smart) NewPacketConnection(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	s.connection.NewPacketConnection(ctx, s, conn, metadata, onClose)
}

// dialTCP is the decision path.
//
// With a usable snapshot the chosen group is dialed straight away, with no wait
// added: the tunnel carries the payload immediately and the answer is checked
// afterwards, so a correct decision costs nothing. Without one, the destination
// is raced.
func (s *Smart) dialTCP(ctx context.Context, destination M.Socksaddr) (net.Conn, error) {
	key := destinationKey(destination)
	// Named here rather than only where a decision is made, so a destination
	// restored from the cache is named as soon as it carries anything. Recording
	// it only on a race or a switch leaves everything preloaded at startup
	// showing as a digest in the dumps, which is most of the table.
	if s.audit != nil {
		s.audit.note(key, destination.String())
	}
	entry := s.cache.load(key)
	now := time.Now()

	if entry != nil && entry.measured() {
		candidates := s.candidates(entry, destination.Port, now)
		if entry.expired(key, now) {
			// Refresh in the background: the stale answer is still a real
			// measurement, and making the user wait for a new one buys nothing.
			go s.raceDetached(key, destination)
		} else if s.supersededRace(entry, candidates, now) {
			// Decided against nodes that have since been replaced by better ones,
			// so the answer is about a different set of proxies than the one it is
			// being applied to. Same background refresh, same reasoning.
			s.logger.DebugContext(ctx, "re-racing ", destination,
				": decided against members that have since been replaced")
			go s.raceDetached(key, destination)
		}
		if selected, reason := selectGroup(s.alpha, candidates, entry.selected()); selected != "" {
			if previous := entry.selected(); previous != "" && previous != selected &&
				!slices.ContainsFunc(candidates, func(c candidate) bool { return c.tag == previous }) {
				// Left out for being down or refusing, not outscored.
				reason = reasonFailover
				if group := s.group(previous); group != nil && !group.down() {
					// Up, so refusing this port only. The host stays where it
					// is for its other ports: moving it here and back on every
					// alternation — 25 refused, 465 not — filled the trail with
					// switches, and inside the hysteresis left 465 on the other
					// exit until the next refresh.
					return s.dialWithFailover(ctx, destination, key, selected, reason)
				}
			}
			if previous, changed := entry.selectGroup(selected); changed {
				s.cache.touch(key)
				s.auditSwitch(key, destination.String(), previous, selected, reason, candidates)
			}
			return s.dialWithFailover(ctx, destination, key, selected, reason)
		}
	}

	if entry != nil && !entry.attemptDue(now) {
		// A round has just run for this destination and produced nothing. Racing
		// again would repeat it in full, once per connection, against something
		// that is not answering.
		return s.dialWithoutRacing(ctx, key, destination)
	}
	return s.raceOrWait(ctx, key, destination, entry)
}

// A selection dead-end — no group, no member, nothing that can carry the
// network — is a fault in this node's configuration or state, not anything
// about the destination. Wrapped in ErrNextHopUnreachable, a relay reports it
// as its own failure rather than with the status that puts the destination on
// cooldown downstream. The two shapes shared across the dial paths live here.
func errNoUsableGroup(destination M.Socksaddr) error {
	return E.Cause(naive.ErrNextHopUnreachable, "no usable group for ", destination)
}

// errNoCandidate is why no group is left to try. When every group is cooling
// down from refusing this destination, each proxy has already said it cannot
// reach it, which is a verdict about the destination and has to leave as one: a
// relay then answers 502, so the hop downstream cools it down too and may
// replay, rather than 503, which charges this node with the failure.
func (s *Smart) errNoCandidate(entry *destinationEntry, destination M.Socksaddr, now time.Time) error {
	if entry == nil {
		return errNoUsableGroup(destination)
	}
	for _, group := range s.groups {
		if group.down() {
			// Not asked, and see candidates for why: the groups that are up
			// have all answered.
			continue
		}
		if !entry.blocked(group.tag, destination.Port, group.current(), now) {
			return errNoUsableGroup(destination)
		}
	}
	return E.Cause(naive.ErrDestinationUnreachable, "every group refused ", destination)
}

func errNoUDPGroup(destination M.Socksaddr) error {
	return E.Cause(naive.ErrNextHopUnreachable, "no group can carry UDP to ",
		destination, "; enable udp_over_tcp on the members that should")
}

// dialWithFailover dials the selected group and hands back a connection that can
// still move to another group if the proxy turns out not to be usable.
func (s *Smart) dialWithFailover(ctx context.Context, destination M.Socksaddr, key string, selected string, reason string) (net.Conn, error) {
	group := s.group(selected)
	if group == nil {
		// This node's own fault, not the destination's — see errNoUsableGroup.
		return nil, E.Cause(naive.ErrNextHopUnreachable, "group not found: ", selected)
	}
	member := group.current()
	if member == nil {
		return nil, E.Cause(naive.ErrNextHopUnreachable, "group ", selected, " has no usable member")
	}
	conn, err := member.outbound.DialContext(ctx, N.NetworkTCP, destination)
	if err != nil {
		s.reportFailure(key, destination, group, member, err)
		return s.replayOnAnotherGroup(ctx, destination, key, selected, err, selected)
	}
	s.countRequest(key, group, member, reason, N.NetworkTCP)
	s.logger.DebugContext(ctx, "tcp to ", destination, " via ", selected, "/", member.tag, " by=", reason)
	return &replayConn{
		Conn:        conn,
		smart:       s,
		destination: destination,
		key:         key,
		group:       group,
		member:      member,
	}, nil
}

// replayOnAnotherGroup retries a destination on the next best group.
//
// It is only ever reached from a signal that proves the destination received
// nothing — a proxy that answered "I could not reach it", or a leg that never
// carried the request at all. That is what makes replaying safe: no ambiguity,
// so no chance of a request arriving twice.
//
// cause is the failure being moved away from, and what is reported when no
// other group is left to try: a 502 from the first group is the verdict, and
// losing it reported every other group cooling down as this node's fault.
// from is the group it came from, for the log line.
func (s *Smart) replayOnAnotherGroup(ctx context.Context, destination M.Socksaddr, key string, from string, cause error, exclude ...string) (net.Conn, error) {
	excluded := make(map[string]bool, len(exclude))
	for _, tag := range exclude {
		excluded[tag] = true
	}
	lastError := cause
	for {
		now := time.Now()
		// Re-read the snapshot each round rather than closing over one: the
		// group that just failed may have been put on cooldown for this
		// destination a moment ago, and a stale copy would offer it again.
		entry := s.cache.load(key)
		var candidates []candidate
		for _, c := range s.candidates(entry, destination.Port, now) {
			if !excluded[c.tag] {
				candidates = append(candidates, c)
			}
		}
		if len(candidates) == 0 {
			if lastError == nil {
				lastError = errNoUsableGroup(destination)
			}
			return nil, lastError
		}
		selected, _ := s.chooseGroup(candidates, "")
		excluded[selected] = true

		group := s.group(selected)
		member := group.current()
		if member == nil {
			continue
		}
		conn, err := member.outbound.DialContext(ctx, N.NetworkTCP, destination)
		if err != nil {
			lastError, from = err, selected
			s.reportFailure(key, destination, group, member, err)
			continue
		}
		s.countRequest(key, group, member, reasonFailover, N.NetworkTCP)
		s.logger.InfoContext(ctx, "failed over ", destination, " from ", from, " to ", selected,
			" by=", reasonFailover, ": ", lastError)
		return &replayConn{
			Conn:        conn,
			smart:       s,
			destination: destination,
			key:         key,
			group:       group,
			member:      member,
			exclude:     excluded,
		}, nil
	}
}

// countRequest records that a member carried a request, and what put it there.
// Called only where a connection is handed to a caller, never for a probe:
// a probe is this outbound measuring, not anyone's traffic.
//
// The same request is counted twice, once against the node and once against the
// destination, because the two answer different questions: which node is
// carrying the load, and what the load is. Neither is derivable from the other
// — a node holding four hundred destinations and a node holding one can carry
// the same number of requests.
func (s *Smart) countRequest(key string, group *smartGroup, member *smartMember, reason string, network string) {
	if member == nil {
		return
	}
	if network == N.NetworkUDP {
		member.requestsUDP.Add(1)
	} else {
		member.requestsTCP.Add(1)
	}
	if group != nil {
		group.reasons[reasonIndex(reason)].Add(1)
	}
	// Unconditional, because the count is no longer only a diagnostic: it is
	// half of when a snapshot is due for re-verification, alongside its age
	// (see snapshotUseLimit). Behind the audit check, as it began, the
	// use-based refresh was silently absent from every deployment that had not
	// turned the trail on — the routing depending on a diagnostic being
	// enabled. The lookup it costs is one bounded LRU read per request.
	s.cache.load(key).count(network)
}

// reportFailure attributes a dial failure.
//
// A proxy reporting the destination unreachable says nothing about the proxy,
// so only that destination is put on cooldown for it. A failure of the leg to
// the proxy is about the member — and how it is handled follows from what
// retrying costs, not from how bad it looks. A blackholed leg makes every
// attempt pay the whole timeout, so it condemns on sight and the probe
// confirms. An instant failure costs a round trip to pay again, so the probe
// runs first and its verdict condemns — see the branch below for why that
// order matters.
//
// The snapshot is looked up here rather than passed in. A connection outlives
// the snapshot it was opened against — a refresh race replaces it — and a
// cooldown recorded on the superseded copy would be written to an object
// nothing reads again.
//
// With no snapshot to hold the cooldown, an empty one is made for it. That is
// a destination dialed without a race, because every race slot was busy, and
// left with nothing it was refused by every group on every connection: a
// tunnel per group each time, for as long as the slots stayed full — and the
// dead peers and scans such refusals come from are what keeps them full. It is
// the same empty entry noteFailedRace leaves, and like it costs one slot in a
// bounded cache. A slot is not free once the cache is full: the entry it
// pushes out may be a real snapshot, and the next flush deletes that one's row
// with it (see smartCache.evicted), so a restart does not bring it back either.
func (s *Smart) reportFailure(key string, destination M.Socksaddr, group *smartGroup, member *smartMember, err error) {
	if errors.Is(err, naive.ErrDestinationUnreachable) {
		now := time.Now()
		member.refused.Store(true)
		entry := s.cache.load(key)
		if entry == nil {
			entry = &destinationEntry{}
			s.cache.store(key, entry)
		}
		entry.block(group.tag, destination.Port, now, member)
		s.cache.touch(key)
		s.logger.Debug("group ", group.tag, " cannot reach ", destination, ": ", err)
		if member.noteRefusal(key, now) {
			// Several different destinations refused in a row is what an exit
			// that lost its egress looks like. member.refused would have the
			// next heartbeat ask a real address, but that is up to eighty
			// seconds of every new destination paying a round trip to be
			// refused, and earning a cooldown for it. The probe asks two real
			// addresses now, and only both refusing condemns.
			at, last := now.UnixNano(), member.refusalProbedAt.Load()
			if at-last >= int64(refusalProbeInterval) && member.refusalProbedAt.CompareAndSwap(last, at) {
				s.logger.Debug("member ", member.tag, " refused ", refusalBurst,
					" destinations in a row, verifying its egress")
				s.confirmDown(member)
			}
		}
		return
	}
	if errors.Is(err, naive.ErrClosedLocally) {
		// This process closed the tunnel while the proxy was still answering —
		// the same "nobody is waiting any more" as the deadline below, reached
		// by the other route. It is what a client dropping a speculative
		// connection looks like from here, and on a preconnect host it is the
		// ordinary case: 51 of 63 dials to one CDN name on a live deployment,
		// each one spending a confirming probe to establish that a node this
		// process had hung up on was still there.
		//
		// The sentinel and never net.ErrClosed, for the reason on
		// naive.ErrClosedLocally. markDown draws the same line, since a probe
		// reaches the verdict by the other door.
		return
	}
	if !errors.Is(err, errLegTimedOut) &&
		(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		// The caller walked away — a race that already has its winner, or a
		// request that went away. Nothing was learned about this member, and
		// treating it as a fault would take a healthy node out of service every
		// time it merely came second.
		//
		// errLegTimedOut is excluded because it is also a deadline, but one this
		// outbound set on purpose to catch a blackholed leg. Only the sentinel
		// tells the two apart: by the time either reaches here both look like
		// context.DeadlineExceeded.
		return
	}
	if errors.Is(err, errLegTimedOut) {
		// A blackholed leg fails only by timeout, so every connection that tries
		// this member while the verdict is out pays the whole budget again. That
		// is the one class expensive enough to condemn on sight and confirm
		// second.
		//
		// Unless the member is still delivering. A path saturated by a bulk
		// transfer misses the establishment bound the same way, and condemning
		// it moves the transfer onto the next group to saturate that one — see
		// Smart.congested, which the probe's verdict goes through. Bytes
		// arriving mean whatever is wrong is not a blackhole, so the probe
		// decides alone, as it does for an instant failure.
		if member.delivering(time.Now()) {
			s.logger.Debug("member ", member.tag, " missed the establishment bound while delivering, verifying: ", err)
		} else {
			s.condemn(member, err)
		}
		s.confirmDown(member)
		return
	}
	// Everything else failed in an instant: connection refused, a pooled
	// connection that died while idle, a handshake that broke. An instant
	// failure is cheap to pay again — this caller has already been replayed onto
	// another group, and the next one fails just as fast — and most of them are
	// one dead pooled connection rather than a dead node: over one real day, ten
	// of fifteen condemnations were "use of closed network connection", each
	// lifted by its own confirming probe within the same second. The flap is the
	// expensive part — the group falls to its backup, a race decided in that
	// window is decided against the wrong node, and the superseded-race
	// machinery then re-races everything it touched. So for these the probe runs
	// first and its verdict does the condemning: markDown on failure, nothing at
	// all on success.
	s.logger.Debug("member ", member.tag, " failed a dial, verifying: ", err)
	s.confirmDown(member)
}

// confirmDown probes a member that has just failed, unless a probe of it is
// already in flight.
//
// It has to be immediate. A group with nothing healthy still offers a member to
// try, so until the verdict is in every new connection reaching for this one
// pays the timeout again — waiting for the next heartbeat round would leave a
// group failing traffic long after the network came back.
//
// It also has to happen once. Every connection in flight to a node that has just
// died fails at roughly the same moment, and one probe per failed connection is
// the handshake storm probeSlots exists to prevent, arriving by the one path
// that never went through it — at the moment an outage is already generating the
// load that gets members condemned in the first place.
func (s *Smart) confirmDown(member *smartMember) {
	if !member.probing.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer member.probing.Store(false)
		// Against somewhere real, not the proxy's own probe address. What just
		// failed was a dial through this member to a destination, and the
		// address the proxy answers itself cannot tell a working proxy from one
		// that has lost its own upstream — it never dials for it, so it answers
		// either way. See proveReachable.
		s.confirmMembers(s.ctx, []*smartMember{member})
	}()
}

// recheckSchedule is when a condemned member is probed again, in multiples of
// condemnationRecheck after the condemnation: 5, 15 and 45 seconds, then the
// heartbeat. Whichever way it was condemned: by a confirming probe, by the
// heartbeat, on a blackhole, after outwaiting a queue.
//
// One recheck at five seconds caught a relay mid-reload and nothing longer: a
// node back after ten seconds — a restart, a route flapping, a provider's
// maintenance blip — then waited out the heartbeat, 40–80 seconds on its
// backup in another country. Backing off covers the first minute at three
// pairs of dials for a node that stays dead, and stops there, because past a
// minute the heartbeat is as good and costs nothing extra.
//
// Factors of the base, not durations: typed as durations only so that they
// multiply it.
var recheckSchedule = [...]time.Duration{1, 3, 9}

// scheduleRechecks lands the probes that may let a member just condemned back
// in before the heartbeat would.
//
// Every recheck runs the same probe as the heartbeat does for a member that
// is down, through confirmDown, so it is called off by anything that makes it
// moot: the member back in, condemned again since (which scheduled its own),
// or the outbound closed. A recheck landing while another probe of the member
// is in flight is folded into that probe by confirmDown's single flight; that
// probe's verdict counts the same, and the heartbeat remains behind both.
//
// The ones inside a flap hold are skipped while it holds, since their answer
// could not let the member in; the probe at the end of the hold comes from
// condemn, whichever way the member was condemned.
//
// condemn schedules them for every condemnation. They were once a confirming
// probe's alone, so a member condemned by the heartbeat or on a blackhole —
// the route flapping, a provider's blip, both of which are over in seconds as
// often as a reload is — sat out the heartbeat, a test blackhole lifted after
// nine seconds kept its member out for fifty more. A recheck that finds the
// member still down condemns nothing, so it schedules no more of them.
func (s *Smart) scheduleRechecks(member *smartMember) {
	base := cmp.Or(s.recheckAfter, condemnationRecheck)
	downAt := member.downAt.Load()
	for _, step := range recheckSchedule {
		time.AfterFunc(step*base, func() {
			if s.closed.Load() || member.healthy.Load() || member.downAt.Load() != downAt {
				return
			}
			if s.holding(member, time.Now()) {
				return
			}
			s.confirmDown(member)
		})
	}
}

// flapHold is how long a member condemned twice within flapWindow stays out;
// see Smart.markUp.
func (s *Smart) flapHold() time.Duration {
	return flapHoldFactor * cmp.Or(s.recheckAfter, condemnationRecheck)
}

// suspect spends a confirming probe on a member that has left a settle
// waiting for longer than settleSuspicion, at most once per suspicionInterval.
//
// A deadline running out on a settle says nothing about the member by itself —
// it is the caller walking away, or a destination slow to connect — which is
// why reportFailure does not treat it as a fault. But a member whose proxy has
// silently stopped answering looks exactly like that from inside each
// connection, and every new connection reaching for it waited the whole
// settle timeout, thirty seconds, until the heartbeat noticed. Suspecting
// costs a probe; the probe decides, and with Smart.congested in its path it
// condemns only a member that is answering nothing and delivering nothing.
func (s *Smart) suspect(member *smartMember) {
	if s.closed.Load() {
		return
	}
	now := time.Now().UnixNano()
	last := member.suspectedAt.Load()
	if now-last < int64(suspicionInterval) || !member.suspectedAt.CompareAndSwap(last, now) {
		return
	}
	s.logger.Debug("member ", member.tag, " left a connection unanswered for ",
		cmp.Or(s.suspectAfter, settleSuspicion), ", verifying")
	s.confirmDown(member)
}

// Network reports UDP only when some member can actually carry it, so a routing
// rule that needs UDP fails to match here rather than failing at dial time.
func (s *Smart) Network() []string {
	for _, group := range s.groups {
		for _, member := range group.members {
			if member.supportsUDP() {
				return []string{N.NetworkTCP, N.NetworkUDP}
			}
		}
	}
	return []string{N.NetworkTCP}
}

// paused reports whether the device is asleep. Probing then measures a network
// that is being torn down, and every timeout it collects reads as a member
// having died — a wake would start from everything condemned at once.
func (s *Smart) paused() bool {
	return s.pause != nil && (s.pause.IsDevicePaused() || s.pause.IsNetworkPaused())
}

// InterfaceUpdated runs when the default network underneath changed. Every
// member's naive outbound throws away its connection pools — the path they
// were opened over is gone — though not necessarily before this runs: when
// this outbound comes first in the configuration it is told first, and the
// probe round below can meet sessions closing under it (probeMember retries
// once for that). The measurements taken over the old path do not go by
// themselves, and they are rolling minimums: a floor measured on the old
// network is never raised by samples from the new one, it just condemns them.
// See smartMember.resetPath for the concrete failure. The reset returns
// every member to the cold state, and one probe round measures the new network
// so the next race is not decided on nothing.
//
// A round still running from an earlier change is called off first. It is
// measuring a network that is gone, and on a client switching between radios
// the changes come in bursts: five in a row once queued fifty probes behind
// four slots, each with the cold ten-second budget against a network that
// may be swallowing them, and every confirmation and heartbeat waited behind
// that queue.
func (s *Smart) InterfaceUpdated(context.Context) {
	var members []*smartMember
	for _, group := range s.groups {
		for _, member := range group.members {
			member.resetPath()
			members = append(members, member)
		}
	}
	// In the trail too: every reading after this line starts from cold.
	s.audit.write(auditRecord{Time: time.Now(), Type: auditTypeHealth, To: "reset"})
	ctx, cancel := context.WithCancel(s.ctx)
	s.pathAccess.Lock()
	if s.pathRound != nil {
		s.pathRound()
	}
	s.pathRound = cancel
	s.pathAccess.Unlock()
	if s.paused() {
		// Asleep: the interface change is the device drifting between radios,
		// and the wake will re-probe soon enough.
		return
	}
	go s.probeMembers(ctx, members)
}
