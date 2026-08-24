package group

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/protocol/naive"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/stretchr/testify/require"
)

// resolved is a reading whose destination leg includes the exit looking the
// destination up, which is what the innermost hop reports separately.
func resolved(near time.Duration, connect time.Duration, resolve time.Duration) naive.ConnMeasurement {
	return naive.ConnMeasurement{
		RoundTrip:  near + connect + resolve,
		ServerSpan: connect + resolve,
		RemoteDial: connect + resolve,
		HasSpan:    true,
		HasRemote:  true,
		Resolve:    resolve,
		HasResolve: true,
	}
}

// resolvingOutbound is a proxy whose exit has to look a destination up the first
// time it is asked for one, and has it cached for every dial after that.
type resolvingOutbound struct {
	adapter.Outbound
	cold     naive.ConnMeasurement
	warm     naive.ConnMeasurement
	failWarm bool
	// coldDelay holds the first answer back, which is how a test reaches the
	// half of a race that runs after the caller has left with its tunnel.
	coldDelay time.Duration

	access sync.Mutex
	dialed []M.Socksaddr
}

func (o *resolvingOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *resolvingOutbound) DialContext(_ context.Context, _ string, destination M.Socksaddr) (net.Conn, error) {
	o.access.Lock()
	o.dialed = append(o.dialed, destination)
	nth := len(o.dialed)
	o.access.Unlock()
	if nth == 1 {
		return &answeringConn{delay: o.coldDelay, measurement: o.cold}, nil
	}
	if o.failWarm {
		return nil, E.Cause(naive.ErrNextHopUnreachable, "the retake did not get through")
	}
	return &answeringConn{measurement: o.warm}, nil
}

func (o *resolvingOutbound) dials() int {
	o.access.Lock()
	defer o.access.Unlock()
	return len(o.dialed)
}

func newWarmFixture(t *testing.T, outs map[string]*resolvingOutbound) *Smart {
	t.Helper()
	return newWarmFixtureOn(t, newSmartCache(context.Background(), "smart"), outs)
}

func newWarmFixtureOn(t *testing.T, cache *smartCache, outs map[string]*resolvingOutbound) *Smart {
	t.Helper()
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		alpha:  testAlpha,
		cache:  cache,
	}
	for _, tag := range []string{"a", "b"} {
		out, configured := outs[tag]
		if !configured {
			continue
		}
		member := newMember(tag+"-1", ms(20))
		member.outbound = out
		smart.groups = append(smart.groups, &smartGroup{tag: tag, members: []*smartMember{member}})
	}
	return smart
}

// warmSibling is a second group whose exit already has the name, so it takes no
// retake of its own. Its presence is what makes a retake mean anything: the bias
// being corrected is between the group carrying a destination and the ones
// challenging it, so a fixture with one group is a fixture where nothing is
// being corrected.
func warmSibling() *resolvingOutbound {
	return &resolvingOutbound{
		cold: resolved(ms(20), ms(5), 0),
		warm: resolved(ms(20), ms(5), 0),
	}
}

func remoteOf(t *testing.T, smart *Smart, key string, tag string) time.Duration {
	t.Helper()
	entry := smart.cache.load(key)
	require.NotNil(t, entry)
	value, measured := entry.remoteFor(tag)
	require.True(t, measured)
	return value
}

func TestAChallengerIsNotJudgedOnItsExitsColdResolver(t *testing.T) {
	t.Parallel()
	// The bias smoothing cannot reach, because it is not noise. The group
	// carrying a destination keeps its exit's resolver warm for that name;
	// every other group last resolved it during the previous race, long past
	// any TTL. So a refresh measures the incumbent warm and the challenger
	// cold, every time, and freezing that into the snapshot makes it a standing
	// handicap renewed by the very round meant to correct one.
	incumbent := &resolvingOutbound{
		// Already warm: the lookup costs nothing worth retaking.
		cold: resolved(ms(20), ms(30), 0),
		warm: resolved(ms(20), ms(30), 0),
	}
	challenger := &resolvingOutbound{
		// Genuinely nearer the destination, and it looks slower only because
		// its exit had to find it.
		cold: resolved(ms(20), ms(5), ms(50)),
		warm: resolved(ms(20), ms(5), 0),
	}
	smart := newWarmFixture(t, map[string]*resolvingOutbound{"a": incumbent, "b": challenger})
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)

	_, conn, err := smart.race(context.Background(), key, destination, nil)
	require.NoError(t, err)
	if conn != nil {
		require.NoError(t, conn.Close())
	}

	// The race itself is decided on what it measured, cold lookup and all.
	require.Equal(t, ms(55), remoteOf(t, smart, key, "b"),
		"the round's own verdict stands on the numbers the round had")

	require.Eventually(t, func() bool {
		return remoteOf(t, smart, key, "b") == ms(5)
	}, time.Second, time.Millisecond,
		"the reading the next thousand connections are routed on must be the one taken warm")

	require.Equal(t, 2, challenger.dials(), "the cold group is asked again, exactly once")
	require.Equal(t, 1, incumbent.dials(),
		"and a group whose exit already had the name is not asked twice at all")
	require.Equal(t, ms(30), remoteOf(t, smart, key, "a"), "nor is its reading touched")
}

func TestARetakeThatFailsLeavesTheReadingAndTheMemberAlone(t *testing.T) {
	t.Parallel()
	// A retake decides nothing. It is not a caller's connection and not a
	// heartbeat — health was settled by the race that just ran — so letting one
	// condemn a node would add a death sentence to a path whose only job is to
	// sharpen a number.
	challenger := &resolvingOutbound{
		cold:     resolved(ms(20), ms(5), ms(50)),
		failWarm: true,
	}
	smart := newWarmFixture(t, map[string]*resolvingOutbound{"a": challenger, "b": warmSibling()})
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)

	_, conn, err := smart.race(context.Background(), key, destination, nil)
	require.NoError(t, err)
	if conn != nil {
		require.NoError(t, conn.Close())
	}

	require.Eventually(t, func() bool { return challenger.dials() == 2 }, time.Second, time.Millisecond)
	require.Equal(t, ms(55), remoteOf(t, smart, key, "a"),
		"the cold reading simply stands when the retake does not get through")
	require.True(t, smart.groups[0].members[0].healthy.Load(),
		"and nothing about the member was decided by it")
}

func TestARetakeCountsEvenWhenItComesBackWorse(t *testing.T) {
	t.Parallel()
	// Keeping the smaller of the two would be the rule remoteWindow rejects one
	// level down: it turns every retake into a licence to keep the more
	// flattering round, and one lucky reading would then outlive the round that
	// produced it.
	challenger := &resolvingOutbound{
		cold: resolved(ms(20), ms(5), ms(50)),
		warm: resolved(ms(20), ms(90), 0),
	}
	smart := newWarmFixture(t, map[string]*resolvingOutbound{"a": challenger, "b": warmSibling()})
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)

	_, conn, err := smart.race(context.Background(), key, destination, nil)
	require.NoError(t, err)
	if conn != nil {
		require.NoError(t, conn.Close())
	}

	require.Eventually(t, func() bool {
		return remoteOf(t, smart, key, "a") == ms(90)
	}, time.Second, time.Millisecond,
		"the retake is the newer reading, better or worse")
}

func TestAnAddressDestinationIsNeverAskedTwice(t *testing.T) {
	t.Parallel()
	// Nothing to look up, so both sides of a race already met it on equal
	// terms, and a second connection would buy nothing at all.
	out := &resolvingOutbound{
		cold: resolved(ms(20), ms(5), ms(50)),
		warm: resolved(ms(20), ms(5), 0),
	}
	smart := newWarmFixture(t, map[string]*resolvingOutbound{"a": out})
	destination := M.ParseSocksaddr("93.184.216.34:443")
	require.False(t, destination.IsFqdn())
	key := destinationKey(destination)

	_, conn, err := smart.race(context.Background(), key, destination, nil)
	require.NoError(t, err)
	if conn != nil {
		require.NoError(t, conn.Close())
	}

	require.Never(t, func() bool { return out.dials() > 1 }, 100*time.Millisecond, 5*time.Millisecond)
}

func TestTheOnlyGroupIsNeverAskedTwice(t *testing.T) {
	t.Parallel()
	// A retake corrects the bias between the group carrying a destination and
	// the ones challenging it. With one group there is no challenger, so the
	// corrected reading cannot change a selection — there is nothing to select
	// against — and all that is left is one more connection the destination
	// sees, from the same exit, for a comparison that never happens. Two of the
	// smart outbounds in production are configured this way.
	out := &resolvingOutbound{
		cold: resolved(ms(20), ms(5), ms(50)),
		warm: resolved(ms(20), ms(5), 0),
	}
	smart := newWarmFixture(t, map[string]*resolvingOutbound{"a": out})
	require.Len(t, smart.groups, 1)
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)

	_, conn, err := smart.race(context.Background(), key, destination, nil)
	require.NoError(t, err)
	if conn != nil {
		require.NoError(t, conn.Close())
	}

	require.Never(t, func() bool { return out.dials() > 1 }, 100*time.Millisecond, 5*time.Millisecond)
	require.Equal(t, ms(55), remoteOf(t, smart, key, "a"),
		"the cold reading stands, which costs nothing where nothing compares against it")
}

func TestASmallLookupIsNotWorthASecondConnection(t *testing.T) {
	t.Parallel()
	// The gate is what the rest of the package means by "large enough to change
	// a decision". Below it the warm and cold readings cannot move a selection
	// apart, so the destination is spared the connection.
	out := &resolvingOutbound{
		cold: resolved(ms(20), ms(5), warmResolverBias-time.Millisecond),
		warm: resolved(ms(20), ms(5), 0),
	}
	smart := newWarmFixture(t, map[string]*resolvingOutbound{"a": out})
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)

	_, conn, err := smart.race(context.Background(), key, destination, nil)
	require.NoError(t, err)
	if conn != nil {
		require.NoError(t, conn.Close())
	}

	require.Never(t, func() bool { return out.dials() > 1 }, 100*time.Millisecond, 5*time.Millisecond)
}

func TestAnAmendedReadingSurvivesARestart(t *testing.T) {
	t.Parallel()
	// A flush writes what has been marked changed, and an amendment alters an
	// entry in place rather than replacing it — so without a mark of its own it
	// writes nothing at all, and every restart inside the snapshot's life brings
	// the handicap back. The earlier tests in this file cannot see that: their
	// cache has no database behind it, so the whole persistence path returns
	// before it begins.
	cache := onDisk(t)
	challenger := &resolvingOutbound{
		cold: resolved(ms(20), ms(5), ms(50)),
		warm: resolved(ms(20), ms(5), 0),
	}
	smart := newWarmFixtureOn(t, cache, map[string]*resolvingOutbound{"a": challenger, "b": warmSibling()})
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)

	_, conn, err := smart.race(context.Background(), key, destination, nil)
	require.NoError(t, err)
	if conn != nil {
		require.NoError(t, conn.Close())
	}
	require.Eventually(t, func() bool {
		return remoteOf(t, smart, key, "a") == ms(5)
	}, time.Second, time.Millisecond)

	// What a restart sees: a fresh cache over the same database. Read rather
	// than assumed, because the amendment reaches memory before it reaches the
	// disk and the gap between the two is exactly what this is checking.
	onRestart := func() (time.Duration, bool) {
		restarted := newSmartCache(context.Background(), "smart")
		restarted.resolve.Do(func() { restarted.cacheFile = cache.cacheFile })
		restarted.preload()
		entry := restarted.load(key)
		if entry == nil {
			return 0, false
		}
		return entry.remoteFor("a")
	}
	require.Eventually(t, func() bool {
		stored, measured := onRestart()
		return measured && stored == ms(5)
	}, time.Second, 5*time.Millisecond,
		"the reading on disk must be the one taken warm, not the one it replaced")

	stored, measured := onRestart()
	require.True(t, measured, "the snapshot has to be on disk at all")
	require.Equal(t, ms(5), stored)
}

func TestAConnectionThatPaidForALookupIsNotReadAsDrift(t *testing.T) {
	t.Parallel()
	// The record a destination is compared against now stands for the warm
	// case, because that is what a retake makes it. A connection that had to
	// resolve the name is not comparable to it: read as drift, a destination
	// sparse enough for its exit's cache to lapse between connections would
	// re-race itself every few connections, and the confirmed reading would
	// retire a lookup into the incumbent's own window.
	fixture := newRaceFixture(t, 0, map[string][2]time.Duration{"a": {ms(20), ms(5)}})
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)
	group := fixture.smart.groups[0]
	member := group.members[0]

	fixture.smart.cache.store(key, &destinationEntry{
		// What a retake leaves behind: the destination leg as measured once the
		// exit had the name.
		Remote:   map[string]remoteWindow{"a": {ms(5)}},
		Path:     map[string]time.Duration{"a": ms(20)},
		Selected: "a",
		RacedAt:  time.Now(),
	})

	// The same path, on a connection whose exit had to look the name up again.
	lapsed := resolved(ms(20), ms(5), ms(50))
	require.Greater(t, score(testAlpha, lapsed.NearHop(), lapsed.RemoteDial, 0),
		score(testAlpha, ms(20), ms(5), 0)*anomalyFactor,
		"the lookup alone has to clear the anomaly threshold, or this proves nothing")

	for range 10 * anomalyStreak {
		fixture.smart.observeScore(key, destination, group, member, lapsed)
	}

	entry := fixture.smart.cache.load(key)
	remote, _ := entry.remoteFor("a")
	require.Equal(t, ms(5), remote,
		"a lookup must not retire itself into the destination's window")
	require.Never(t, func() bool { return fixture.outbounds["a"].dials.Load() > 0 },
		100*time.Millisecond, 5*time.Millisecond,
		"nor start a race the destination did nothing to deserve")
}

// gatedResolvingOutbound answers its first dial only once the test opens the
// gate, which is how a straggler is made deterministically rather than by
// timing. Its retake answers immediately, as a warm one would.
type gatedResolvingOutbound struct {
	adapter.Outbound
	gate chan struct{}
	cold naive.ConnMeasurement
	warm naive.ConnMeasurement

	access sync.Mutex
	dialed []M.Socksaddr
}

func (o *gatedResolvingOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *gatedResolvingOutbound) DialContext(_ context.Context, _ string, destination M.Socksaddr) (net.Conn, error) {
	o.access.Lock()
	o.dialed = append(o.dialed, destination)
	nth := len(o.dialed)
	o.access.Unlock()
	if nth == 1 {
		return &gatedAnsweringConn{answeringConn: answeringConn{measurement: o.cold}, gate: o.gate}, nil
	}
	return &answeringConn{measurement: o.warm}, nil
}

func (o *gatedResolvingOutbound) dials() int {
	o.access.Lock()
	defer o.access.Unlock()
	return len(o.dialed)
}

func TestAReadingCollectedAfterTheCallerLeftIsRetakenToo(t *testing.T) {
	t.Parallel()
	// A race hands its unfinished half to a background collector once the caller
	// has what it came for, and that half writes the final snapshot. It is a
	// second place the retake has to happen, and the other tests here cannot
	// reach it: their groups all answer at once, so nothing is ever handed off.
	gate := make(chan struct{})
	slow := &gatedResolvingOutbound{
		gate: gate,
		cold: resolved(ms(20), ms(1), ms(50)),
		warm: resolved(ms(20), ms(1), 0),
	}
	slowMember := newMember("a-1", ms(20))
	slowMember.outbound = slow
	fast := &resolvingOutbound{
		cold: resolved(ms(10), ms(100), 0),
		warm: resolved(ms(10), ms(100), 0),
	}
	fastMember := newMember("b-1", ms(10))
	fastMember.outbound = fast
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		alpha:  testAlpha,
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{
			{tag: "a", bonus: ms(40), members: []*smartMember{slowMember}},
			{tag: "b", members: []*smartMember{fastMember}},
		},
	}
	destination := M.ParseSocksaddr("courier.example.com:443")
	key := destinationKey(destination)

	winner, conn, err := smart.race(context.Background(), key, destination, nil)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	require.Equal(t, "b", winner, "the caller was not made to wait for the straggler")
	require.Equal(t, 1, slow.dials(), "and nothing has been retaken while it is still answering")

	close(gate)
	require.Eventually(t, func() bool {
		entry := smart.cache.load(key)
		if entry == nil {
			return false
		}
		value, measured := entry.remoteFor("a")
		return measured && value == ms(1)
	}, 2*time.Second, time.Millisecond,
		"the straggler's reading is retaken by the half of the race that recorded it")

	require.Equal(t, 2, slow.dials())
	require.Equal(t, 1, fast.dials(), "and the group that was already warm is still asked once")
}

func TestARetakeLeavesALineSayingWhatItReplaced(t *testing.T) {
	t.Parallel()
	// The race that chose is already in the trail with the numbers it chose on,
	// and after a retake those are not the numbers the destination is routed by.
	// Without a line closing that gap the two disagree and nothing says why —
	// the same blind spot the third leg was added to remove, one level up.
	//
	// Driven directly rather than through a race, so the assertion is about what
	// gets written and not about which of two goroutines got there first.
	audited, path := auditedSmart(t)
	// Only the retake is driven here, so this stands for the exit once it has
	// the name: the cold round it replaces is seeded into the snapshot below.
	challenger := &resolvingOutbound{
		cold: resolved(ms(20), ms(5), 0),
		warm: resolved(ms(20), ms(5), 0),
	}
	member := newMember("a-1", ms(20))
	member.outbound = challenger
	sibling := newMember("b-1", ms(20))
	sibling.outbound = warmSibling()
	audited.groups = []*smartGroup{
		{tag: "a", members: []*smartMember{member}},
		{tag: "b", members: []*smartMember{sibling}},
	}

	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)
	entry := &destinationEntry{
		Remote:   map[string]remoteWindow{"a": {ms(55)}},
		Path:     map[string]time.Duration{"a": ms(20)},
		Selected: "a",
		RacedAt:  time.Now(),
	}
	audited.cache.store(key, entry)

	audited.amendColdResolutions(key, destination, entry,
		map[string]racedLeg{"a": {member: member, resolve: ms(50)}})
	require.NoError(t, audited.audit.Close())

	value, measured := entry.remoteFor("a")
	require.True(t, measured)
	require.Equal(t, ms(5), value)

	var amend *auditRecord
	records := readTrail(t, path)
	for i := range records {
		if records[i].Type == auditTypeAmend {
			amend = &records[i]
		}
	}
	require.NotNil(t, amend, "a retake has to leave a line of its own")
	require.Equal(t, destination.String(), amend.Destination)
	require.Len(t, amend.Groups, 1)
	require.Equal(t, "a", amend.Groups[0].Tag)
	require.Equal(t, "a-1", amend.Groups[0].Member, "the member that raced, not whichever one is current")
	require.Equal(t, short(ms(5)), amend.Groups[0].Remote, "what the destination is routed by now")
	require.Equal(t, short(ms(55)), amend.Groups[0].Was, "and what that replaced")
	require.Equal(t, short(ms(50)), amend.Groups[0].Resolve, "with the lookup that made it worth retaking")
}

func TestTheStateDumpCanTellAnInteriorFromAScore(t *testing.T) {
	t.Parallel()
	// The state dump is the only global view that reaches disk, and it is what
	// an operator has hours after the fact. Without the interior on it, a member
	// whose round trip is seconds while its score is milliseconds looks
	// perfectly fine — which is exactly how the failure that started all of this
	// stayed invisible.
	audited, path := auditedSmart(t)
	relay := newMember("relay", 0)
	for range localWindow {
		relay.record(naive.ConnMeasurement{
			RoundTrip: ms(300), ServerSpan: ms(170), RemoteDial: ms(8),
			HasSpan: true, HasRemote: true,
		})
	}
	audited.groups = []*smartGroup{{tag: "us", members: []*smartMember{relay}}}
	audited.cache.store("3f2a", &destinationEntry{
		Remote:   map[string]remoteWindow{"us": {ms(8)}},
		Path:     map[string]time.Duration{"us": ms(292)},
		Selected: "us",
		RacedAt:  time.Now(),
	})
	audited.audit.note("3f2a", "example.com:443")

	audited.auditState()
	require.NoError(t, audited.audit.Close())

	var state *auditRecord
	records := readTrail(t, path)
	for i := range records {
		if records[i].Type == auditTypeState {
			state = &records[i]
		}
	}
	require.NotNil(t, state)
	require.Len(t, state.Groups, 1)
	require.Equal(t, short(ms(162)), state.Groups[0].Chain,
		"the interior has to be readable without subtracting three other numbers by hand")
	require.Equal(t, short(ms(292)), state.Groups[0].Local)
}
