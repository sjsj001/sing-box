package group

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/naive"
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
)

const (
	defaultAlpha = 0.7

	// heartbeatInterval is how often an idle member is pinged. It only fills
	// gaps: a member that carried real traffic within the interval is skipped, so
	// the traffic scales with idleness rather than with the number of members.
	//
	// It can be this sparse because nothing time-critical depends on it. A
	// connection to a member that has died is rescued by failing over, not by
	// the heartbeat; a member that has died is marked so on first use, not on
	// the next round; and a member that has recovered is found by the next
	// request, because a group with nothing healthy still offers a member to
	// try. What is left is keeping idle connection pools warm, and those live
	// for minutes.
	heartbeatInterval = 60 * time.Second

	// heartbeatJitter spreads each round either way.
	//
	// The contents are encrypted, but the shape is not: an otherwise idle
	// connection carrying a small request and a small response exactly every
	// sixty seconds is a periodicity anyone watching the flow can pick out
	// without decrypting anything. Scattering the moment costs nothing here —
	// nothing time-critical depends on the interval — and leaves no period to
	// lock onto.
	heartbeatJitter = heartbeatInterval / 3

	// cacheFlushInterval is how often changed snapshots reach disk.
	cacheFlushInterval = 30 * time.Second

	// probeConcurrency bounds how many members are dialed at once.
	//
	// Every pool of every member is opened at the same moment otherwise, which
	// on a small router is dozens of TLS handshakes competing for the same
	// cores. What comes back is then a handshake time that measures contention
	// rather than distance, and the handshake is what bounds how close a member
	// may claim to be — so a starved one is filed as further away than it is,
	// and stays that way until something cheaper is seen. Since a member that
	// is ranked too far away stops being dialed, that can be never.
	probeConcurrency = 4

	// warmupDialsMax caps how many connections are opened to each member at
	// startup. naive spreads streams over isolated pools, so one dial leaves the
	// rest cold, and a cold pool costs a full TLS handshake at exactly the
	// moment a first race is trying to compare nodes fairly. How many pools
	// there are is the member's own concurrency setting — see
	// smartMember.warmupDials — and this only stops an extreme one from opening
	// an unreasonable number of connections at once.
	warmupDialsMax = 8

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
	// raceSlots bounds destinations racing at once, as probeSlots bounds probe
	// dials. See raceConcurrency for what happens without it.
	raceSlots     chan struct{}
	raceSlotsOnce sync.Once
	// started is when this run began.
	started time.Time
	// usageSince is when the window now open began, in nanoseconds. On the Smart
	// rather than inside the loop that ticks it, because the loop is not the only
	// thing that closes a window: shutdown closes the last one, and reaching for
	// the process start there reported a three-minute window as fourteen hours.
	usageSince atomic.Int64
	// races maps a destination to the round of probes in flight for it, so a
	// page load opening eight connections at once produces one round and seven
	// waiters rather than eight rounds. See Smart.raceOrWait.
	races sync.Map
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
	return smart, nil
}

func (s *Smart) Start() error {
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
	s.started = time.Now()
	s.usageSince.Store(s.started.UnixNano())
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
	// Warming every connection pool means the first real request does not pay
	// for a TLS handshake it could have avoided, and the first race compares
	// nodes that are all equally warm. It runs in the background because
	// blocking here would hold up the rest of the start; until it lands, dials
	// fall back to configured order rather than failing.
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
	if s.cancel != nil {
		s.cancel()
	}
	// Snapshots already reach disk as each race finishes, so this is a last
	// chance rather than the only one. The cache file service may have closed
	// its database first, and failing to write into a closed database is not a
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
func (s *Smart) candidates(entry *destinationEntry, now time.Time) []candidate {
	var candidates []candidate
	for _, group := range s.groups {
		member := group.current()
		if member == nil {
			continue
		}
		if entry != nil && entry.blocked(group.tag, now) {
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
		candidates = append(candidates, c)
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
		return false
	}
	// Through the same backoff as any other refresh: a group changing members
	// must not turn every connection to every destination it touched into a
	// round of its own.
	return entry.attemptDue(now)
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
	candidates := s.candidates(nil, time.Now())
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
		key, group, member := s.groupFor(destination, N.NetworkUDP)
		if member == nil || !member.supportsUDP() {
			return nil, errNoUDPGroup(destination)
		}
		s.countRequest(key, group, member, reasonRideAlong, N.NetworkUDP)
		s.logger.DebugContext(ctx, "udp to ", destination, " via ", group.tag, "/", member.tag, " by=", reasonRideAlong)
		return member.outbound.DialContext(ctx, network, destination)
	default:
		return nil, E.Extend(N.ErrUnknownNetwork, network)
	}
}

func (s *Smart) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	key, group, member := s.groupFor(destination, N.NetworkUDP)
	if member == nil || !member.supportsUDP() {
		return nil, errNoUDPGroup(destination)
	}
	s.countRequest(key, group, member, reasonRideAlong, N.NetworkUDP)
	s.logger.DebugContext(ctx, "packet to ", destination, " via ", group.tag, "/", member.tag, " by=", reasonRideAlong)
	return member.outbound.ListenPacket(ctx, destination)
}

// groupFor resolves a destination to a group without racing, for the paths that
// cannot race: UDP, and anything asked for before a snapshot exists.
//
// A group whose member in use cannot carry UDP is left out rather than swapped
// for a sibling that can: swapping would send the same destination out of two
// different addresses depending on the protocol, which is the split the grouping
// exists to prevent.
func (s *Smart) groupFor(destination M.Socksaddr, network string) (string, *smartGroup, *smartMember) {
	now := time.Now()
	key := destinationKey(destination)
	s.audit.note(key, destination.String())
	entry := s.cache.load(key)
	candidates := s.usableCandidates(entry, now, network)
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
func (s *Smart) usableCandidates(entry *destinationEntry, now time.Time, network string) []candidate {
	candidates := s.candidates(entry, now)
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
	return usable
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
	s.audit.note(key, destination.String())
	entry := s.cache.load(key)
	now := time.Now()

	if entry != nil && entry.measured() {
		candidates := s.candidates(entry, now)
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
	return s.raceOrWait(ctx, key, destination)
}

// A selection dead-end — no group, no member, nothing that can carry the
// network — is a fault in this node's configuration or state, not anything
// about the destination. Wrapped in ErrNextHopUnreachable, a relay reports it
// as its own failure rather than with the status that puts the destination on
// cooldown downstream. The two shapes shared across the dial paths live here.
func errNoUsableGroup(destination M.Socksaddr) error {
	return E.Cause(naive.ErrNextHopUnreachable, "no usable group for ", destination)
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
		return s.replayOnAnotherGroup(ctx, destination, key, selected)
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
		exclude:     map[string]bool{selected: true},
	}, nil
}

// replayOnAnotherGroup retries a destination on the next best group.
//
// It is only ever reached from a signal that proves the destination received
// nothing — a proxy that answered "I could not reach it", or a leg that never
// carried the request at all. That is what makes replaying safe: no ambiguity,
// so no chance of a request arriving twice.
func (s *Smart) replayOnAnotherGroup(ctx context.Context, destination M.Socksaddr, key string, exclude ...string) (net.Conn, error) {
	excluded := make(map[string]bool, len(exclude))
	for _, tag := range exclude {
		excluded[tag] = true
	}
	var lastError error
	for {
		now := time.Now()
		// Re-read the snapshot each round rather than closing over one: the
		// group that just failed may have been put on cooldown for this
		// destination a moment ago, and a stale copy would offer it again.
		entry := s.cache.load(key)
		var candidates []candidate
		for _, c := range s.candidates(entry, now) {
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
			lastError = err
			s.reportFailure(key, destination, group, member, err)
			continue
		}
		s.countRequest(key, group, member, reasonFailover, N.NetworkTCP)
		s.logger.InfoContext(ctx, "failed over ", destination, " to ", selected, " by=", reasonFailover)
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
func (s *Smart) reportFailure(key string, destination M.Socksaddr, group *smartGroup, member *smartMember, err error) {
	if errors.Is(err, naive.ErrDestinationUnreachable) {
		if entry := s.cache.load(key); entry != nil {
			entry.block(group.tag, time.Now())
			s.cache.touch(key)
		}
		s.logger.Debug("group ", group.tag, " cannot reach ", destination, ": ", err)
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
		if member.healthy.CompareAndSwap(true, false) {
			s.logger.Warn("member ", member.tag, " looks unreachable: ", err)
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
// member's naive outbound has already thrown away its connection pools — the
// path they were opened over is gone — but the measurements taken over that
// path would otherwise stay, and they are rolling minimums: a floor measured on
// the old network is never raised by samples from the new one, it just condemns
// them. See smartMember.resetPath for the concrete failure. The reset returns
// every member to the cold state, and one probe round measures the new network
// so the next race is not decided on nothing.
func (s *Smart) InterfaceUpdated(context.Context) {
	var members []*smartMember
	for _, group := range s.groups {
		for _, member := range group.members {
			member.resetPath()
			members = append(members, member)
		}
	}
	if s.paused() {
		// Asleep: the interface change is the device drifting between radios,
		// and the wake will re-probe soon enough.
		return
	}
	go s.probeMembers(s.ctx, members)
}
