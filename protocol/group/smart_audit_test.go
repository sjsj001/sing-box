package group

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/stretchr/testify/require"
)

func readTrail(t *testing.T, path string) []auditRecord {
	t.Helper()
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()

	var records []auditRecord
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		var record auditRecord
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &record),
			"every line has to stand on its own: %s", scanner.Text())
		records = append(records, record)
	}
	require.NoError(t, scanner.Err())
	return records
}

func auditedSmart(t *testing.T) (*Smart, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "smart-audit.jsonl")
	return &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		alpha:  testAlpha,
		cache:  newSmartCache(context.Background(), "smart"),
		audit:  newSmartAudit(context.Background(), logger.NOP(), path, auditDefaultSize),
	}, path
}

func TestASwitchIsRecordedWithWhatMovedIt(t *testing.T) {
	t.Parallel()
	// The record an audit starts from: where a destination went, where it came
	// from, and every group's numbers at that moment — on one line, so "why did
	// this site change country" needs no reconstruction.
	smart, path := auditedSmart(t)
	candidates := []candidate{
		{tag: "hk", local: 28727 * time.Microsecond, hasLocal: true, remote: 900 * time.Microsecond, hasRemote: true},
		{tag: "jp", local: 79714 * time.Microsecond, hasLocal: true, bonus: 50 * time.Millisecond, remote: 930 * time.Microsecond, hasRemote: true},
	}
	smart.auditSwitch("3f2a", "ip.skk.moe:443", "hk", "jp", reasonScore, candidates)
	require.NoError(t, smart.audit.Close())

	records := readTrail(t, path)
	require.Len(t, records, 1)
	record := records[0]
	require.Equal(t, auditTypeSwitch, record.Type)
	require.Equal(t, "ip.skk.moe:443", record.Destination)
	require.Equal(t, "hk", record.From)
	require.Equal(t, "jp", record.To)
	require.Equal(t, reasonScore, record.Reason)

	require.Len(t, record.Groups, 2)
	require.Equal(t, "hk", record.Groups[0].Tag)
	require.Equal(t, "21ms", record.Groups[0].Score)
	// jp: 0.7×79.714 + 0.93 − 50 = 6.7ms, and a bound of 5.8ms — both of which
	// have to be readable without recomputing them.
	require.Equal(t, "jp", record.Groups[1].Tag)
	require.Equal(t, "50ms", record.Groups[1].Bonus)
	require.Equal(t, "6.7ms", record.Groups[1].Score)
	require.Equal(t, "5.8ms", record.Groups[1].Bound)
}

func TestAMoveWithinAGroupIsRecordedToo(t *testing.T) {
	t.Parallel()
	// It changes the exit address every destination on that group is seen from,
	// which is exactly as visible to a site as a group change.
	smart, path := auditedSmart(t)
	group := &smartGroup{tag: "hk", onMemberChange: smart.auditMember}
	first := newMember("out-hk-r0", ms(29))
	second := newMember("out-hk-r1", ms(31))
	group.members = []*smartMember{first, second}

	require.Equal(t, "out-hk-r0", group.current().tag)
	first.healthy.Store(false)
	require.Equal(t, "out-hk-r1", group.current().tag)
	require.NoError(t, smart.audit.Close())

	records := readTrail(t, path)
	require.Len(t, records, 1, "only the change is recorded, not every lookup")
	require.Equal(t, auditTypeMember, records[0].Type)
	require.Equal(t, "out-hk-r0", records[0].From)
	require.Equal(t, "out-hk-r1", records[0].To)
	require.Equal(t, "hk", records[0].Groups[0].Tag)
}

func TestTheStateDumpNamesWhatItReportsOn(t *testing.T) {
	t.Parallel()
	// The cache holds digests, on purpose. A dump that only had those would be
	// useless for checking the routing against reality, so the trail keeps the
	// names it has already written in the clear.
	smart, path := auditedSmart(t)
	member := newMember("out-jp-ddps", 79714*time.Microsecond)
	smart.groups = []*smartGroup{{tag: "jp", bonus: 50 * time.Millisecond, members: []*smartMember{member}}}

	key := destinationKey(M.ParseSocksaddr("ip.skk.moe:443"))
	smart.audit.note(key, "ip.skk.moe:443")
	smart.cache.store(key, &destinationEntry{
		Remote:   map[string]remoteWindow{"jp": {930 * time.Microsecond}},
		Path:     map[string]time.Duration{"jp": 79714 * time.Microsecond},
		Selected: "jp",
		RacedAt:  time.Now(),
	})

	smart.auditState()
	require.NoError(t, smart.audit.Close())

	records := readTrail(t, path)
	require.Len(t, records, 1)
	require.Equal(t, auditTypeState, records[0].Type)
	require.Equal(t, "ip.skk.moe:443", records[0].Destination)
	require.Equal(t, key, records[0].Key)
	require.Equal(t, "jp", records[0].To)
	require.NotNil(t, records[0].RacedAt)
	require.Equal(t, "out-jp-ddps", records[0].Groups[0].Member)
	require.Equal(t, "6.7ms", records[0].Groups[0].Score)
}

func TestTheTrailIsBoundedOnDisk(t *testing.T) {
	t.Parallel()
	// It runs on a router. A diagnostic that fills the disk is worse than no
	// diagnostic, so it rotates on a size this outbound controls and keeps
	// exactly one generation.
	path := filepath.Join(t.TempDir(), "smart-audit.jsonl")
	audit := newSmartAudit(context.Background(), logger.NOP(), path, 4096)
	for i := range 400 {
		audit.write(auditRecord{
			Time:        time.Now(),
			Type:        auditTypeSwitch,
			Destination: strings.Repeat("d", 40) + string(rune('a'+i%26)),
			From:        "hk",
			To:          "jp",
		})
	}
	require.NoError(t, audit.Close())

	current, err := os.Stat(path)
	require.NoError(t, err)
	require.LessOrEqual(t, current.Size(), int64(4096))

	previous, err := os.Stat(path + ".1")
	require.NoError(t, err, "one generation is kept so a rotation does not lose the recent past")
	require.LessOrEqual(t, previous.Size(), int64(4096+512))

	// And what survived is still readable line by line.
	require.NotEmpty(t, readTrail(t, path))
}

func TestATrailThatCannotBeWrittenDoesNotStopTraffic(t *testing.T) {
	t.Parallel()
	// It is a diagnostic. Losing it must never take the routing down with it.
	audit := newSmartAudit(context.Background(), logger.NOP(), filepath.Join(t.TempDir(), "no-such-dir", "trail.jsonl"), auditDefaultSize)
	for range 3 {
		audit.write(auditRecord{Time: time.Now(), Type: auditTypeSwitch})
	}
	require.True(t, audit.broken, "it gives up rather than retrying on every decision")
	require.NoError(t, audit.Close())
}

func TestNoTrailIsKeptUnlessOneIsAskedFor(t *testing.T) {
	t.Parallel()
	// It is the only place destination names reach the disk in the clear — the
	// snapshot cache stores digests — so it stays off until an operator decides
	// otherwise.
	require.Nil(t, newSmartAudit(context.Background(), logger.NOP(), "", auditDefaultSize))

	smart := &Smart{ctx: context.Background(), logger: logger.NOP(), cache: newSmartCache(context.Background(), "smart")}
	require.Nil(t, smart.audit)
	// Every entry point has to tolerate that.
	smart.auditSwitch("k", "d", "hk", "jp", reasonScore, nil)
	smart.auditMember("hk", "a", "b", reasonFailover)
	smart.auditRace("k", "d", raceReport{})
	smart.auditState()
	smart.auditUsage()
	require.NoError(t, smart.audit.Close())
}

func TestARaceIsRecordedWithEveryGroupItSaw(t *testing.T) {
	t.Parallel()
	smart, path := auditedSmart(t)
	smart.auditRace("3f2a", "ash.lg.speedypage.com:443", raceReport{
		alpha: testAlpha,
		candidates: []candidate{
			{tag: "us", local: 133218 * time.Microsecond, hasLocal: true, bonus: 25 * time.Millisecond},
			{tag: "jp", local: 80416 * time.Microsecond, hasLocal: true, bonus: 50 * time.Millisecond},
		},
		remote:    map[string]time.Duration{"us": 57773 * time.Microsecond, "jp": 168625 * time.Microsecond},
		winner:    "us",
		hasWinner: true,
		elapsed:   250 * time.Millisecond,
	})
	require.NoError(t, smart.audit.Close())

	records := readTrail(t, path)
	require.Len(t, records, 1)
	require.Equal(t, auditTypeRace, records[0].Type)
	require.Equal(t, "us", records[0].To)
	require.Equal(t, "250ms", records[0].Elapsed)
	require.False(t, records[0].Groups[0].Cached, "both of these answered in this round")
	// us: 0.7×133.218 +  57.773 − 25 = 126.0ms
	// jp: 0.7× 80.416 + 168.625 − 50 = 174.9ms
	require.Equal(t, "126ms", records[0].Groups[0].Score)
	require.Equal(t, "174.9ms", records[0].Groups[1].Score)
}

func TestUsageIsWrittenAsWindowsNotRunningTotals(t *testing.T) {
	t.Parallel()
	// A restart resets the counters, so a trail of running totals cannot be
	// added up across one — and a trail spanning days is mostly across one.
	// Windows can simply be summed.
	smart, path := auditedSmart(t)
	member := newMember("out-jp-ddps", 79400*time.Microsecond)
	group := &smartGroup{tag: "jp", members: []*smartMember{member}}
	group.selected.Store(member)
	smart.groups = []*smartGroup{group}

	smart.cache.store("3f2a", &destinationEntry{Selected: "jp"})
	smart.audit.note("3f2a", "ip.skk.moe:443")

	for range 5 {
		smart.countRequest("3f2a", group, member, reasonScore, N.NetworkTCP)
	}
	smart.countRequest("3f2a", group, member, reasonRaced, N.NetworkUDP)
	smart.auditUsage()

	// A second window reports only what happened in it.
	smart.countRequest("3f2a", group, member, reasonScore, N.NetworkTCP)
	smart.auditUsage()
	require.NoError(t, smart.audit.Close())

	records := readTrail(t, path)
	require.Len(t, records, 2)
	require.Equal(t, auditTypeUsage, records[0].Type)
	require.Equal(t, int64(5), records[0].Groups[0].TCP)
	require.Equal(t, int64(1), records[0].Groups[0].UDP)
	require.Equal(t, map[string]int64{reasonScore: 5, reasonRaced: 1}, records[0].Groups[0].Reasons)

	require.Equal(t, int64(1), records[1].Groups[0].TCP, "the second window is not cumulative")
	require.Zero(t, records[1].Groups[0].UDP)
	require.Equal(t, map[string]int64{reasonScore: 1}, records[1].Groups[0].Reasons)

	// The same requests counted against the destination, over the same windows.
	require.Equal(t, []auditDestination{
		{Key: "3f2a", Destination: "ip.skk.moe:443", TCP: 5, UDP: 1},
	}, records[0].Destinations)
	require.Equal(t, []auditDestination{
		{Key: "3f2a", Destination: "ip.skk.moe:443", TCP: 1},
	}, records[1].Destinations, "the second window is not cumulative either")
}

func TestDestinationCountsSurviveARefresh(t *testing.T) {
	t.Parallel()
	// A refresh replaces the entry rather than editing it, and the destinations
	// that get re-raced most are the ones used most — so a count that did not
	// move across would be smallest exactly where it mattered.
	smart, path := auditedSmart(t)
	member := newMember("out-jp-ddps", 79400*time.Microsecond)
	group := &smartGroup{tag: "jp", members: []*smartMember{member}}
	group.selected.Store(member)
	smart.groups = []*smartGroup{group}
	smart.cache.store("3f2a", &destinationEntry{Selected: "jp"})

	for range 3 {
		smart.countRequest("3f2a", group, member, reasonScore, N.NetworkTCP)
	}
	smart.storeSnapshot("3f2a", M.ParseSocksaddr("ip.skk.moe:443"), snapshotBasis{replacing: smart.cache.load("3f2a")},
		raceReport{winner: "jp", remote: map[string]time.Duration{"jp": time.Millisecond}}, nil, "jp")
	smart.countRequest("3f2a", group, member, reasonScore, N.NetworkTCP)

	smart.auditUsage()
	require.NoError(t, smart.audit.Close())

	records := readTrail(t, path)
	require.Len(t, records, 1)
	require.Equal(t, []auditDestination{{Key: "3f2a", TCP: 4}}, records[0].Destinations)
}

func TestProbesAreNotCountedAsTraffic(t *testing.T) {
	t.Parallel()
	// A heartbeat is this outbound measuring, not anyone's request. Counting one
	// would make an idle member look busy — and idleness is exactly what the
	// heartbeat exists to cover.
	smart, path := auditedSmart(t)
	member := newMember("out-jp-ddps", 79400*time.Microsecond)
	group := &smartGroup{tag: "jp", members: []*smartMember{member}}
	group.selected.Store(member)
	smart.groups = []*smartGroup{group}

	// What a heartbeat does: records a measurement, nothing more.
	smart.record(member, measured(ms(80)))
	smart.auditUsage()
	require.NoError(t, smart.audit.Close())

	records := readTrail(t, path)
	require.Len(t, records, 1, "the group in use is still named, so the line exists")
	require.Zero(t, records[0].Groups[0].TCP)
	require.Zero(t, records[0].Groups[0].UDP)
}

func TestADestinationRestoredFromCacheIsStillNamed(t *testing.T) {
	t.Parallel()
	// Names were only recorded where a decision was made, so everything the
	// cache restored at startup appeared in the dumps as a digest — which after
	// a restart is most of the table, and a dump nobody can read the names in
	// cannot be checked against reality at all.
	audit := newSmartAudit(context.Background(), logger.NOP(), filepath.Join(t.TempDir(), "t.jsonl"), auditDefaultSize)
	key := destinationKey(M.ParseSocksaddr("ip.skk.moe:443"))
	require.Empty(t, audit.name(key))

	audit.note(key, "ip.skk.moe:443")
	require.Equal(t, "ip.skk.moe:443", audit.name(key))

	// The dial path calls it on every connection, so it has to be idempotent and
	// must not let a later caller rewrite what a destination is called.
	audit.note(key, "something-else:443")
	require.Equal(t, "ip.skk.moe:443", audit.name(key))
	audit.note("", "x")
	audit.note("k", "")
	require.Empty(t, audit.name("k"))
}

func TestAGroupThatDidNotTakePartIsNotDressedUpAsOneThatLost(t *testing.T) {
	t.Parallel()
	// A race stores only what answered in that round, so the candidates still
	// carry the previous snapshot's numbers for everyone else. Rendering those
	// as though they were this round's makes a group that never answered look
	// like it answered and lost — and a reader comparing the winner against the
	// lowest score on the line then concludes the wrong group won.
	//
	// That is not hypothetical. It is what a first pass over a production trail
	// concluded, for six destinations, before the values were noticed repeating
	// unchanged across races minutes apart.
	smart, path := auditedSmart(t)
	smart.auditRace("9e54", "gs-loc.apple.com:443", raceReport{
		alpha: testAlpha,
		candidates: []candidate{
			// Answered now.
			{tag: "jp", local: 79300 * time.Microsecond, hasLocal: true, bonus: 50 * time.Millisecond},
			// Did not answer; 38ms is what the snapshot still held from an
			// earlier round.
			{tag: "hk", local: 29700 * time.Microsecond, hasLocal: true,
				remote: 38 * time.Millisecond, hasRemote: true},
		},
		remote:    map[string]time.Duration{"jp": 107500 * time.Microsecond},
		winner:    "jp",
		hasWinner: true,
		elapsed:   187500 * time.Microsecond,
	})
	require.NoError(t, smart.audit.Close())

	groups := readTrail(t, path)[0].Groups
	require.Equal(t, "jp", groups[0].Tag)
	require.False(t, groups[0].Cached)
	require.Equal(t, "113ms", groups[0].Score)

	require.Equal(t, "hk", groups[1].Tag)
	require.True(t, groups[1].Cached,
		"58.8ms is the snapshot's, not this round's, and the line has to say so")
	require.Equal(t, "58.8ms", groups[1].Score)
}

func TestAMeasurementRejectedByTheFloorIsNotHiddenFromTheTrail(t *testing.T) {
	t.Parallel()
	// The starved-handshake failure was a silent rewrite of a measurement. What
	// the proxy reported and what was recorded instead both have to be visible.
	member := &smartMember{tag: "out-jp-ddps"}
	reported, floor, corrected := member.record(naive.ConnMeasurement{
		Setup: 240 * time.Millisecond, RoundTrip: 200 * time.Millisecond,
		ServerSpan: 199 * time.Millisecond, HasSpan: true,
	})
	require.True(t, corrected)
	require.Equal(t, time.Millisecond, reported)
	require.Equal(t, 48*time.Millisecond, floor)
}

func TestAClosedTrailStaysClosed(t *testing.T) {
	t.Parallel()
	// A race's background half and a confirming probe both outlive Close by
	// design, and each ends in a write. Treating a nil file as "open it" made
	// that write reopen the trail behind the owner's back: one descriptor
	// leaked per shutdown with traffic in flight, and in an in-process restart
	// the old handle keeps appending into a file the new instance has rotated.
	path := filepath.Join(t.TempDir(), "trail.jsonl")
	audit := newSmartAudit(context.Background(), logger.NOP(), path, auditDefaultSize)
	audit.write(auditRecord{Type: auditTypeRace, Key: "3f2a"})
	require.NoError(t, audit.Close())

	before, err := os.ReadFile(path)
	require.NoError(t, err)

	audit.write(auditRecord{Type: auditTypeRace, Key: "late"})

	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after, "a write after Close must not reach the file")

	audit.access.Lock()
	defer audit.access.Unlock()
	require.Nil(t, audit.file, "and must not have reopened it")
}
