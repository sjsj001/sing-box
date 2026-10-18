package group

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/contrab/freelru"
	"github.com/sagernet/sing/contrab/maphash"
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

	// auditRetryAfter is how long a trail that failed to write rests before
	// trying again. Retrying on every decision would turn a full disk into a
	// stream of failed syscalls and log lines; never retrying turned a few
	// minutes of full disk into weeks with no trail, behind a single line.
	auditRetryAfter = 10 * time.Minute

	// auditQueueSize is how many records may wait for the disk. A record is a
	// few hundred bytes, so this is well under a megabyte of memory; past it
	// records are dropped and counted, never waited for.
	auditQueueSize = 1024

	// auditCloseBudget is how long closing may spend on the trail, the last
	// dumps included. The command line kills the process when closing takes
	// FatalStopTimeout (10s), and on a disk slow enough to fill the queue
	// draining all of it would take the cache flush and every outbound closed
	// after this one down with it.
	auditCloseBudget = 3 * time.Second
)

// auditCloseGrace is what Close still waits for the writer when the cache
// flush before it spent the whole budget: a disk that is keeping up takes the
// two last dumps in a few milliseconds, and with no wait at all they were
// counted as lost to a disk that never saw them. A variable so a test can
// outwait a scheduler that is slower than the disk.
var auditCloseGrace = 200 * time.Millisecond

type smartAudit struct {
	ctx     context.Context
	logger  logger.Logger
	path    string
	maxSize int64

	// access guards the file and what belongs to it, and only the writer and
	// Close take it. Nothing on the dial path does: write hands the record to
	// the queue, and the names cache locks on its own. One hung disk write
	// held up every new destination's first connection — eight of them in
	// parallel waited up to sixteen seconds on a disk that took two per write.
	access sync.Mutex
	file   *os.File
	size   int64
	// brokenAt is when writing last failed; the trail rests for
	// auditRetryAfter and then tries again. closed is for good.
	brokenAt time.Time
	closed   atomic.Bool
	// queue carries records to the writer, which run drains until Close has
	// closed quit and it has emptied the queue, and then closes done. Full,
	// the record is dropped and counted in dropped, which the writer reports
	// the next time it gets to the disk.
	queue   chan auditBatch
	dropped atomic.Int64
	// final is raised as the outbound closes: the last usage and state dumps
	// are what the trail ends on and what a check reconciles against, so they
	// wait for the queue instead of being dropped on a slow disk.
	final atomic.Bool
	// deadline is when closing stops waiting for the disk, in Unix
	// nanoseconds; abandon is raised when it passes, and what is still queued
	// is counted as dropped instead of written.
	deadline atomic.Int64
	abandon  atomic.Bool
	start    sync.Once
	stop     sync.Once
	quit     chan struct{}
	done     chan struct{}
	// names maps the stored key back to the destination it stands for, so a
	// state dump can name what it is reporting on. The cache itself only holds
	// digests. Least recently used goes first at the cache's own size, so the
	// names kept are the destinations the dumps are about; a map that stopped
	// at the first 16384 ever seen named none of them after a few weeks.
	names *freelru.Cache[string, string]
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
		path:    filepath.Clean(filemanager.BasePath(ctx, path)),
		maxSize: maxSize,
		queue:   make(chan auditBatch, auditQueueSize),
		quit:    make(chan struct{}),
		done:    make(chan struct{}),
		names: common.Must1(freelru.New[string, string](
			destinationCacheSize, maphash.NewHasher[string]().Hash32, false)),
	}
}

// auditBatch is what the queue carries: the records of one call, written
// together. A state dump is one batch, so it is kept or dropped whole and does
// not take the queue's every slot from the decisions made while it was written.
type auditBatch struct {
	lines [][]byte
	// settled, if set, is closed once everything queued before it is on disk.
	// For tests, which otherwise have no way to know when that is.
	settled chan struct{}
}

// failLocked rests the trail, saying so. The operator opted into the record;
// discovering weeks later that it silently stopped is worse than a loud line.
func (a *smartAudit) failLocked(what string, err error) {
	a.brokenAt = time.Now()
	if a.logger != nil {
		a.logger.Error("smart audit trail stopped, retrying in ", auditRetryAfter, ": ", what, ": ", err)
	}
}

// note remembers what a key stands for, for the state dumps.
//
// Called on the dial path, so it stays cheap: one lookup that marks the name
// recently used, and a write only for a destination not yet known.
func (a *smartAudit) note(key string, destination string) {
	if a == nil || key == "" || destination == "" {
		return
	}
	if _, known := a.names.Get(key); !known {
		a.names.Add(key, destination)
	}
}

func (a *smartAudit) name(key string) string {
	name, _ := a.names.Peek(key)
	return name
}

// write appends one record. A trail that cannot be written is reported and
// rests for auditRetryAfter before trying again: it is a diagnostic, and
// failing to keep it must never take the traffic down with it.
func (a *smartAudit) write(record any) {
	if a == nil {
		return
	}
	if encoded, ok := encodeAudit(record); ok {
		a.enqueue(encoded)
	}
}

func encodeAudit(record any) ([]byte, bool) {
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, false
	}
	return append(encoded, '\n'), true
}

// enqueue hands lines to the writer without waiting for it. Called on the
// dial path, which is why it never blocks: a full queue means the disk is not
// keeping up, and the record is dropped and counted rather than waited for.
func (a *smartAudit) enqueue(lines ...[]byte) {
	if a == nil || len(lines) == 0 || a.closed.Load() {
		return
	}
	a.start.Do(func() { go a.run() })
	if a.final.Load() {
		// Room first: past the deadline the timer is already due, and a select
		// between it and a queue with room picks either, dropping half the
		// last dumps on a disk that was keeping up.
		select {
		case a.queue <- auditBatch{lines: lines}:
			return
		default:
		}
		wait := time.NewTimer(time.Until(time.Unix(0, a.deadline.Load())))
		defer wait.Stop()
		select {
		case a.queue <- auditBatch{lines: lines}:
		case <-wait.C:
			a.dropped.Add(int64(len(lines)))
		}
		return
	}
	select {
	case a.queue <- auditBatch{lines: lines}:
	default:
		a.dropped.Add(int64(len(lines)))
	}
}

// finishing makes what is written from here on wait for the queue rather than
// be dropped. Close's last dumps follow it.
func (a *smartAudit) finishing() {
	if a != nil {
		a.deadline.CompareAndSwap(0, time.Now().Add(auditCloseBudget).UnixNano())
		a.final.Store(true)
	}
}

// settle returns once everything queued before it has been written. Tests.
func (a *smartAudit) settle() {
	a.start.Do(func() { go a.run() })
	settled := make(chan struct{})
	a.queue <- auditBatch{settled: settled}
	<-settled
}

// run is the writer. It outlives quit by exactly what is still queued.
func (a *smartAudit) run() {
	defer close(a.done)
	for {
		select {
		case batch := <-a.queue:
			a.append(batch)
		case <-a.quit:
			for {
				select {
				case batch := <-a.queue:
					a.append(batch)
				default:
					return
				}
			}
		}
	}
}

func (a *smartAudit) append(batch auditBatch) {
	if batch.settled != nil {
		close(batch.settled)
		return
	}
	if a.abandon.Load() {
		a.dropped.Add(int64(len(batch.lines)))
		return
	}
	a.access.Lock()
	defer a.access.Unlock()
	if dropped := a.dropped.Swap(0); dropped > 0 && a.logger != nil {
		a.logger.Warn("smart audit trail dropped ", dropped, " records: the disk could not keep up")
	}
	for index, line := range batch.lines {
		// A state dump is one batch of a line per destination, up to the
		// cache size: the deadline is checked per line, or that one batch
		// alone would outlast it.
		if index > 0 && a.abandon.Load() {
			a.dropped.Add(int64(len(batch.lines) - index))
			return
		}
		a.appendLocked(line)
	}
}

func (a *smartAudit) appendLocked(encoded []byte) {
	if time.Since(a.brokenAt) < auditRetryAfter {
		return
	}
	if a.file == nil {
		file, openErr := filemanager.OpenFile(a.ctx, a.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
		if openErr != nil {
			a.failLocked("open", openErr)
			return
		}
		a.file = file
		if info, statErr := file.Stat(); statErr == nil {
			a.size = wholeLines(file, info.Size())
		}
	}
	if a.size+int64(len(encoded)) > a.maxSize {
		a.rotateLocked()
		if a.file == nil {
			return
		}
	}
	written, writeErr := a.file.Write(encoded)
	if writeErr != nil {
		// What went out before the disk refused is a line without its end,
		// and the next record would complete it into one line nothing can
		// parse: a full disk left 348 bytes that cost the record after it too.
		// Closed rather than kept: a cut that failed too would leave the half
		// line under the next write, while reopening goes through wholeLines.
		a.file.Truncate(a.size)
		a.file.Close()
		a.file = nil
		a.failLocked("write", writeErr)
		return
	}
	a.size += int64(written)
}

// wholeLines is how much of the file ends in a newline, and cuts it there. A
// half record survives on disk when the process dies between a failed write and
// the truncation above, and a trail that starts by appending to it loses that
// record and the next.
func wholeLines(file *os.File, size int64) int64 {
	chunk := make([]byte, 4096)
	for end := size; end > 0; {
		start := max(end-int64(len(chunk)), 0)
		n, err := file.ReadAt(chunk[:end-start], start)
		if err != nil && !errors.Is(err, io.EOF) {
			return size
		}
		if cut := bytes.LastIndexByte(chunk[:n], '\n'); cut >= 0 {
			return cutRecord(file, start+int64(cut)+1, size)
		}
		end = start
	}
	return cutRecord(file, 0, size)
}

// cutRecord cuts the file at keep if what follows looks like the start of a
// record, and leaves it whole otherwise: audit_path pointed at some other file
// must not lose its last line, or all of it.
func cutRecord(file *os.File, keep int64, size int64) int64 {
	if keep >= size {
		return size
	}
	first := make([]byte, 1)
	if _, err := file.ReadAt(first, keep); err != nil || first[0] != '{' {
		return size
	}
	file.Truncate(keep)
	return keep
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

// Close writes out what is queued and closes the file. Retired, not merely
// closed: a race's background half and a confirming probe both outlive Close by
// design, and each ends in a write; with the file merely nil, that write would
// reopen the trail and the handle would never be closed — one leaked descriptor
// per shutdown with traffic in flight, appending records after the owner is
// gone.
func (a *smartAudit) Close() error {
	if a == nil {
		return nil
	}
	a.closed.Store(true)
	a.deadline.CompareAndSwap(0, time.Now().Add(auditCloseBudget).UnixNano())
	a.start.Do(func() { go a.run() })
	a.stop.Do(func() { close(a.quit) })
	wait := time.NewTimer(max(time.Until(time.Unix(0, a.deadline.Load())), auditCloseGrace))
	select {
	case <-a.done:
	case <-wait.C:
		// ponytail: still waits out the write in progress; a disk that never
		// returns one would need the file closed from under the writer.
		a.abandon.Store(true)
		<-a.done
	}
	wait.Stop()
	if dropped := a.dropped.Swap(0); dropped > 0 && a.logger != nil {
		a.logger.Warn("smart audit trail lost ", dropped, " records at close: the disk could not keep up")
	}
	a.access.Lock()
	defer a.access.Unlock()
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
	// Down appears on a state record when every member of the group is
	// condemned. Any score beside it is the snapshot's, from before the
	// outage, so without the flag a reader comparing it with the group in use
	// sees a better group passed over when it was never available.
	Down   bool   `json:"down,omitempty"`
	Failed string `json:"failed,omitempty"`
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
	auditTypeHealth = "health" // a member condemned, let back in or excused as congested, or every member's path reset
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

// auditHealth records a member condemned ("down", with why), let back in
// ("up"), or found silent but delivering and left in service ("congested",
// with the silence it was excused for).
//
// Without it a group out of service for an hour left nothing in the trail but
// the down flag on the next hourly dump, and the trail is what is left once the
// journal that said more has been rotated away.
func (s *Smart) auditHealth(member *smartMember, to string, err error) {
	if s.audit == nil {
		return
	}
	leg := auditLeg{Member: member.tag}
	for _, group := range s.groups {
		if slices.Contains(group.members, member) {
			leg.Tag = group.tag
		}
	}
	record := auditRecord{Time: time.Now(), Type: auditTypeHealth, To: to}
	if err != nil {
		leg.Failed = err.Error()
	}
	record.Groups = []auditLeg{leg}
	s.audit.write(record)
}

// auditState writes out every destination currently remembered, so the trail
// answers "what did it think at this moment" without replaying every event
// since the process started.
func (s *Smart) auditState() {
	if s.audit == nil {
		return
	}
	now := time.Now()
	keys := s.cache.keys()
	lines := make([][]byte, 0, len(keys))
	for _, key := range keys {
		entry := s.cache.load(key)
		if entry == nil {
			continue
		}
		racedAt := entry.racedAt()
		encoded, ok := encodeAudit(auditRecord{
			Time:        now,
			Type:        auditTypeState,
			Destination: s.audit.name(key),
			Key:         key,
			To:          entry.selected(),
			RacedAt:     &racedAt,
			Groups:      s.stateLegs(entry, now),
		})
		if ok {
			lines = append(lines, encoded)
		}
	}
	// One batch: the dump is kept or dropped whole, and does not take every
	// slot in the queue from the decisions made while it was being written.
	s.audit.enqueue(lines...)
}

// stateLegs is every configured group as it stands for one destination: what
// the snapshot holds for it, and what it is measuring right now.
func (s *Smart) stateLegs(entry *destinationEntry, now time.Time) []auditLeg {
	legs := make([]auditLeg, 0, len(s.groups))
	for _, group := range s.groups {
		member := group.current()
		leg := auditLeg{Tag: group.tag, Blocked: entry.blockedOnAnyPort(group.tag, member, now), Down: group.down()}
		if member != nil {
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
