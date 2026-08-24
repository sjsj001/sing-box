package group

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/service/filemanager"
)

// The audit trail: a durable record of every decision this outbound made and
// the measurements it made it from.
//
// It is deliberately not the log. A route that turns out to be wrong is usually
// noticed hours or days later, and by then the journal that would have
// explained it has been rotated away — and what is left in it is scattered over
// dozens of lines that have to be joined by hand. This is one file, one line per
// decision, each line self-contained, rotated on a size this outbound controls.
//
// It is off unless a path is configured, because it is the one place that writes
// destination names to disk in the clear. The snapshot cache deliberately does
// not: see destinationKey. Turning this on trades that away for the ability to
// check the routing against reality, which is a trade only the operator can
// make.
const (
	// auditDefaultSize is how large the trail grows before it is rotated. One
	// decision is a few hundred bytes, so this holds on the order of a hundred
	// thousand of them — days of a busy client, and enough that an audit is
	// looking at the period being asked about rather than the last hour.
	auditDefaultSize = 64 << 20

	// auditStateInterval is how often the whole table is written out, on top of
	// the dump taken at shutdown. The dumps are what make the file answer "what
	// did it think at the time" without replaying every event since the start.
	auditStateInterval = time.Hour
)

type smartAudit struct {
	ctx     context.Context
	logger  logger.Logger
	path    string
	maxSize int64

	// access guards the file and what belongs to it. The names map has its own
	// lock below: note sits on the dial path, and sharing a mutex with file
	// I/O would let one hung disk write hold up every new connection.
	access sync.Mutex
	file   *os.File
	size   int64
	broken bool
	// nameAccess guards names, which maps the stored key back to the
	// destination it stands for, so a state dump can name what it is reporting
	// on. The cache itself only holds digests. It is capped at the same size as
	// the cache; past that the names still reach the file on every decision
	// record, just not the dumps.
	nameAccess sync.Mutex
	names      map[string]string
}

func newSmartAudit(ctx context.Context, logger logger.Logger, path string, maxSize int64) *smartAudit {
	if path == "" {
		return nil
	}
	if maxSize <= 0 {
		maxSize = auditDefaultSize
	}
	return &smartAudit{
		ctx:     ctx,
		logger:  logger,
		path:    filemanager.BasePath(ctx, path),
		maxSize: maxSize,
		names:   make(map[string]string),
	}
}

// failLocked retires the trail, saying so once. The operator opted into the
// record; discovering weeks later that it silently stopped is worse than one
// loud line.
func (a *smartAudit) failLocked(what string, err error) {
	a.broken = true
	if a.logger != nil {
		a.logger.Error("smart audit trail stopped, no further decisions will be recorded: ", what, ": ", err)
	}
}

// note remembers what a key stands for, for the state dumps.
//
// Called on the dial path, so it stays cheap: one uncontended lock, and nothing
// written once a destination is known. The cap matches the snapshot cache — past
// it the names still reach the file on every decision record, just not the
// dumps.
func (a *smartAudit) note(key string, destination string) {
	if a == nil || key == "" || destination == "" {
		return
	}
	a.nameAccess.Lock()
	defer a.nameAccess.Unlock()
	if _, known := a.names[key]; known || len(a.names) >= destinationCacheSize {
		return
	}
	a.names[key] = destination
}

func (a *smartAudit) name(key string) string {
	a.nameAccess.Lock()
	defer a.nameAccess.Unlock()
	return a.names[key]
}

// write appends one record. A trail that cannot be written is reported once and
// then stops trying: it is a diagnostic, and failing to keep it must never take
// the traffic down with it.
func (a *smartAudit) write(record any) {
	if a == nil {
		return
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return
	}
	encoded = append(encoded, '\n')

	a.access.Lock()
	defer a.access.Unlock()
	if a.broken {
		return
	}
	if a.file == nil {
		file, openErr := filemanager.OpenFile(a.ctx, a.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
		if openErr != nil {
			a.failLocked("open", openErr)
			return
		}
		a.file = file
		if info, statErr := file.Stat(); statErr == nil {
			a.size = info.Size()
		}
	}
	if a.size+int64(len(encoded)) > a.maxSize {
		a.rotateLocked()
		if a.broken {
			return
		}
	}
	written, writeErr := a.file.Write(encoded)
	a.size += int64(written)
	if writeErr != nil {
		a.failLocked("write", writeErr)
	}
}

// rotateLocked moves the trail aside and starts a new one, keeping exactly one
// generation. Two files of the configured size is the whole footprint, which is
// what makes the setting mean something on a device with a small disk.
func (a *smartAudit) rotateLocked() {
	a.file.Close()
	a.file = nil
	if err := os.Rename(a.path, a.path+".1"); err != nil && !os.IsNotExist(err) {
		a.failLocked("rotate", err)
		return
	}
	file, err := filemanager.OpenFile(a.ctx, a.path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		a.failLocked("rotate: reopen", err)
		return
	}
	a.file, a.size = file, 0
}

func (a *smartAudit) Close() error {
	if a == nil {
		return nil
	}
	a.access.Lock()
	defer a.access.Unlock()
	// Retired, not merely closed. A race's background half and a confirming
	// probe both outlive Close by design, and each ends in a write; with the
	// file merely nil, that write would reopen the trail and the handle would
	// never be closed — one leaked descriptor per shutdown with traffic in
	// flight, appending records after the owner is gone.
	a.broken = true
	if a.file == nil {
		return nil
	}
	err := a.file.Close()
	a.file = nil
	return err
}

// auditLeg is one group as it stood when a decision was made. Every field is a
// duration rendered for reading rather than for arithmetic: the point of the
// trail is being able to see what happened without a tool.
type auditLeg struct {
	Tag    string `json:"tag"`
	Member string `json:"member,omitempty"`
	Local  string `json:"local,omitempty"`
	// Chain is the interior of the path: what the first proxy spent that is
	// neither this client's leg nor the innermost connect. Zero on a member
	// that dials the destination itself, and on a relay the leg that neither
	// Local nor Remote covers — which is why a relay whose next hop had gone
	// bad could not be seen in this record at all.
	Chain string `json:"chain,omitempty"`
	// Was and Resolve appear on amend records: the reading this one replaced,
	// and the lookup that made the first one worth taking again. Without them a
	// trail shows a race choosing on one set of numbers and the routing that
	// followed running on another, with nothing in between to explain it.
	Was     string `json:"was,omitempty"`
	Resolve string `json:"resolve,omitempty"`
	Remote  string `json:"remote,omitempty"`
	Bonus   string `json:"bonus,omitempty"`
	Score   string `json:"score,omitempty"`
	// Bound is the best score this group could reach for any destination. A
	// bound above the winner's score is why a group was dropped without being
	// waited for, and is the single most common thing an audit needs to see.
	Bound string `json:"bound,omitempty"`
	// Cached says the remote leg — and so the score built on it — came from the
	// stored snapshot rather than from this round. Without the distinction a
	// group that did not take part reads exactly like one that answered and
	// lost, and a reader comparing the winner against the lowest score on the
	// line concludes the wrong group won. It is not hypothetical: it is what
	// the first pass over this trail concluded.
	Cached bool `json:"cached,omitempty"`
	// RacedBy appears on a state record only when the member that stood for this
	// group in the deciding round is not the one carrying it now — so the leg
	// beside it describes a different node. Written only when they differ: it is
	// there to be noticed, and a field repeating Member on every line would not
	// be.
	RacedBy string `json:"raced_by,omitempty"`
	Blocked bool   `json:"blocked,omitempty"`
	Failed  string `json:"failed,omitempty"`
	// TCP, UDP and Reasons appear on usage records only, and count what was
	// carried since the previous one rather than since the process started.
	// Deltas because a restart resets the counters: totals that could not be
	// added up across one would be answering a different question from the one
	// asked of a trail that spans days.
	TCP     int64            `json:"tcp,omitempty"`
	UDP     int64            `json:"udp,omitempty"`
	Reasons map[string]int64 `json:"reasons,omitempty"`
}

// auditDestination is one destination's share of a usage window. Only the ones
// that carried something appear: the table holds thousands and all but a few are
// idle in any ten minutes, so listing them all would be a file of zeroes.
type auditDestination struct {
	Key         string `json:"key"`
	Destination string `json:"destination,omitempty"`
	TCP         int64  `json:"tcp,omitempty"`
	UDP         int64  `json:"udp,omitempty"`
}

type auditRecord struct {
	Time        time.Time  `json:"time"`
	Type        string     `json:"type"`
	Destination string     `json:"destination,omitempty"`
	Key         string     `json:"key,omitempty"`
	From        string     `json:"from,omitempty"`
	To          string     `json:"to,omitempty"`
	Reason      string     `json:"reason,omitempty"`
	Elapsed     string     `json:"elapsed,omitempty"`
	RacedAt     *time.Time `json:"raced_at,omitempty"`
	Groups      []auditLeg `json:"groups,omitempty"`
	// Destinations appears on usage records: what each destination carried over
	// the window, alongside what each node carried over the same one.
	Destinations []auditDestination `json:"destinations,omitempty"`
}

const (
	auditTypeRace   = "race"   // a round of probes decided a destination
	auditTypeSwitch = "switch" // the group carrying a destination changed
	auditTypeMember = "member" // the member carrying a group changed
	auditTypeState  = "state"  // one destination, as currently remembered
	auditTypeUsage  = "usage"  // what each node carried over a window
	auditTypeAmend  = "amend"  // a destination reading retaken against a warm resolver
)

// auditUsageInterval is how often what each node carried is written out. It is
// shorter than the state dump because it is a rate rather than a state: a
// window of hours averages away exactly the shifts worth seeing.
const auditUsageInterval = 10 * time.Minute

// auditUsage writes what each node and each destination carried since the last
// time this ran, and opens the next window.
//
// It closes the window itself rather than being told which one it is closing.
// Two callers close windows — the timer, and shutdown — and only one of them
// knows when the last one opened.
func (s *Smart) auditUsage() {
	if s.audit == nil {
		return
	}
	now := time.Now()
	since := time.Unix(0, s.usageSince.Swap(now.UnixNano()))
	legs := make([]auditLeg, 0, len(s.groups))
	for _, group := range s.groups {
		reasons := make(map[string]int64, reasonCount)
		for index := range group.reasons {
			if delta := group.reasons[index].delta(); delta > 0 {
				reasons[usageReasonNames[index]] = delta
			}
		}
		for _, member := range group.members {
			tcp, udp := member.requestsTCP.delta(), member.requestsUDP.delta()
			if tcp == 0 && udp == 0 {
				// A member that carried nothing is worth a line only when it is
				// the one in use, so an idle group still names its member.
				if member != group.selected.Load() {
					continue
				}
			}
			leg := auditLeg{Tag: group.tag, Member: member.tag, TCP: tcp, UDP: udp}
			if interior, measured := member.chain(); measured && interior > 0 {
				// What of this node's leg is the chain in front of it, so a
				// window where one node carried badly can be read without
				// going back to the races that put it there.
				leg.Chain = short(interior)
			}
			if local, measured := member.local(); measured {
				leg.Local = short(local)
			}
			if member == group.selected.Load() && len(reasons) > 0 {
				leg.Reasons = reasons
			}
			legs = append(legs, leg)
		}
	}
	destinations := s.usageDestinations()
	if len(legs) == 0 && len(destinations) == 0 {
		return
	}
	s.audit.write(auditRecord{
		Time:         now,
		Type:         auditTypeUsage,
		Elapsed:      short(now.Sub(since)),
		Groups:       legs,
		Destinations: destinations,
	})
}

// usageDestinations drains the per-destination counts for the window just ended.
//
// It walks the whole table rather than keeping a list of what moved: the table
// is bounded and in memory, and a list would need a lock on the dial path to
// maintain — which is what the counters on the entries exist to avoid.
func (s *Smart) usageDestinations() []auditDestination {
	var destinations []auditDestination
	for _, key := range s.cache.keys() {
		entry := s.cache.load(key)
		if entry == nil {
			continue
		}
		tcp, udp := entry.requestsTCP.delta(), entry.requestsUDP.delta()
		if tcp == 0 && udp == 0 {
			continue
		}
		destinations = append(destinations, auditDestination{
			Key:         key,
			Destination: s.audit.name(key),
			TCP:         tcp,
			UDP:         udp,
		})
	}
	return destinations
}

// legsFromCandidates renders the decision core's view for the trail.
func legsFromCandidates(alpha float64, candidates []candidate, remote map[string]time.Duration, chain map[string]time.Duration, failures map[string]error) []auditLeg {
	legs := make([]auditLeg, 0, len(candidates))
	for _, c := range candidates {
		leg := auditLeg{Tag: c.tag, Member: c.member}
		if interior, reported := chain[c.tag]; reported && interior > 0 {
			leg.Chain = short(interior)
		} else if c.hasChain && c.chain > 0 {
			// A switch record has no round behind it, so the interior comes from
			// the member's own window — which is the one that was ranked on.
			leg.Chain = short(c.chain)
		}
		if c.hasLocal {
			leg.Local = short(c.local)
			leg.Bound = short(c.staticBound(alpha))
		}
		if c.bonus != 0 {
			leg.Bonus = short(c.bonus)
		}
		measured, answered := remote[c.tag]
		if !answered && c.hasRemote {
			// The snapshot's value, which is what the decision would have used —
			// worth keeping, but only if it cannot be mistaken for a group that
			// took part in this round.
			measured, answered, leg.Cached = c.remote, true, true
		}
		if answered {
			leg.Remote = short(measured)
			if c.hasLocal {
				leg.Score = short(score(alpha, c.local, measured, c.bonus))
			}
		}
		if err, failed := failures[c.tag]; failed {
			leg.Failed = err.Error()
		}
		legs = append(legs, leg)
	}
	return legs
}

// auditRace records what a round of probes saw and chose.
func (s *Smart) auditRace(key string, destination string, report raceReport) {
	if s.audit == nil {
		return
	}
	s.audit.note(key, destination)
	s.audit.write(auditRecord{
		Time:        time.Now(),
		Type:        auditTypeRace,
		Destination: destination,
		Key:         key,
		To:          report.winner,
		Elapsed:     short(report.elapsed),
		Groups:      legsFromCandidates(report.alpha, report.candidates, report.remote, report.chain, report.failures),
	})
}

// auditSwitch records a destination moving from one group to another, with the
// measurements that moved it. This is the record an audit starts from: it names
// the before and the after in one line, so "why did this site change country"
// does not have to be reconstructed.
func (s *Smart) auditSwitch(key string, destination string, from string, to string, reason string, candidates []candidate) {
	if s.audit == nil || from == to {
		return
	}
	s.audit.note(key, destination)
	s.audit.write(auditRecord{
		Time:        time.Now(),
		Type:        auditTypeSwitch,
		Destination: destination,
		Key:         key,
		From:        from,
		To:          to,
		Reason:      reason,
		Groups:      legsFromCandidates(s.alpha, candidates, nil, nil, nil),
	})
}

// auditAmend records a destination reading taken again once the exit had the
// name, and what it replaced.
//
// The race that chose is already in the trail with the numbers it chose on, and
// those are not the numbers the next thousand connections are routed by. One
// line closes that gap; without it the two disagree and nothing says why.
func (s *Smart) auditAmend(key string, destination string, legs []auditLeg) {
	if s.audit == nil || len(legs) == 0 {
		return
	}
	s.audit.note(key, destination)
	s.audit.write(auditRecord{
		Time:        time.Now(),
		Type:        auditTypeAmend,
		Destination: destination,
		Key:         key,
		Groups:      legs,
	})
}

// auditMember records the member carrying a group changing, which moves every
// destination on that group to a different exit address without any of them
// switching group.
func (s *Smart) auditMember(group string, from string, to string, reason string) {
	if s.audit == nil || from == to {
		return
	}
	s.audit.write(auditRecord{
		Time:   time.Now(),
		Type:   auditTypeMember,
		From:   from,
		To:     to,
		Reason: reason,
		Groups: []auditLeg{{Tag: group, Member: to}},
	})
}

// auditState writes out every destination currently remembered, so the trail
// answers "what did it think at this moment" without replaying every event
// since the process started.
func (s *Smart) auditState() {
	if s.audit == nil {
		return
	}
	now := time.Now()
	for _, key := range s.cache.keys() {
		entry := s.cache.load(key)
		if entry == nil {
			continue
		}
		racedAt := entry.racedAt()
		s.audit.write(auditRecord{
			Time:        now,
			Type:        auditTypeState,
			Destination: s.audit.name(key),
			Key:         key,
			To:          entry.selected(),
			RacedAt:     &racedAt,
			Groups:      s.stateLegs(entry, now),
		})
	}
}

// stateLegs is every configured group as it stands for one destination: what
// the snapshot holds for it, and what it is measuring right now.
func (s *Smart) stateLegs(entry *destinationEntry, now time.Time) []auditLeg {
	legs := make([]auditLeg, 0, len(s.groups))
	for _, group := range s.groups {
		leg := auditLeg{Tag: group.tag, Blocked: entry.blocked(group.tag, now)}
		if member := group.current(); member != nil {
			leg.Member = member.tag
			if raced := entry.racedBy(group.tag); raced != "" && raced != member.tag {
				leg.RacedBy = raced
			}
			if interior, measured := member.chain(); measured && interior > 0 {
				// This dump is the only global view that reaches disk, and it is
				// what an operator has hours after the fact. Without the
				// interior on it, a member whose round trip is seconds while its
				// score is milliseconds reads as perfectly fine — which is how
				// the failure this model exists for stayed invisible.
				leg.Chain = short(interior)
			}
			if local, measured := member.local(); measured {
				leg.Local = short(local)
				leg.Bound = short(staticBound(s.alpha, local, group.bonus))
				if remote, found := entry.remoteFor(group.tag); found {
					leg.Remote = short(remote)
					leg.Score = short(score(s.alpha, local, remote, group.bonus))
					// Inherited from the round before, not measured in the last
					// one — the same distinction a race leg draws.
					leg.Cached = entry.carried(group.tag)
				}
			}
		}
		if group.bonus != 0 {
			leg.Bonus = short(group.bonus)
		}
		legs = append(legs, leg)
	}
	return legs
}
