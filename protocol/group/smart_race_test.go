package group

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
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

// answeringConn is a tunnel whose proxy answers with a measurement, after an
// optional delay so a test can decide the order a race sees its answers in.
type answeringConn struct {
	net.Conn
	delay       time.Duration
	measurement naive.ConnMeasurement
	closed      atomic.Int32
}

func (c *answeringConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *answeringConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *answeringConn) Close() error                     { c.closed.Add(1); return nil }
func (c *answeringConn) SetDeadline(time.Time) error      { return nil }
func (c *answeringConn) SetReadDeadline(time.Time) error  { return nil }
func (c *answeringConn) SetWriteDeadline(time.Time) error { return nil }
func (c *answeringConn) WaitReady(context.Context) error  { return nil }

func (c *answeringConn) Measure(ctx context.Context) (naive.ConnMeasurement, error) {
	if c.delay > 0 {
		select {
		case <-time.After(c.delay):
		case <-ctx.Done():
			return naive.ConnMeasurement{}, ctx.Err()
		}
	}
	// Validated, because the real one is: a stub that skips it would let a test
	// pass on a report the shipping code would have thrown away.
	return c.measurement.Validated(), nil
}

// answeringOutbound hands out a fresh tunnel per dial and counts them, which is
// how a test sees how much a destination was actually touched.
type answeringOutbound struct {
	adapter.Outbound
	delay       time.Duration
	measurement naive.ConnMeasurement

	dials  atomic.Int32
	access sync.Mutex
	handed []*answeringConn
	dialed []M.Socksaddr
}

func (o *answeringOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *answeringOutbound) DialContext(_ context.Context, _ string, destination M.Socksaddr) (net.Conn, error) {
	o.dials.Add(1)
	conn := &answeringConn{delay: o.delay, measurement: o.measurement}
	o.access.Lock()
	o.handed = append(o.handed, conn)
	o.dialed = append(o.dialed, destination)
	o.access.Unlock()
	return conn, nil
}

// dialedTo reports every address this outbound was asked for, which is how a
// test catches a caller that reconstructed one and got it wrong.
func (o *answeringOutbound) dialedTo() []string {
	o.access.Lock()
	defer o.access.Unlock()
	addresses := make([]string, 0, len(o.dialed))
	for _, destination := range o.dialed {
		addresses = append(addresses, destination.String())
	}
	return addresses
}

// settledConns counts the tunnels this outbound handed out and has since seen
// closed, which is how a test waits for a detached round rather than for the
// dial that started it.
func (o *answeringOutbound) settledConns() int {
	o.access.Lock()
	defer o.access.Unlock()
	var settled int
	for _, conn := range o.handed {
		if conn.closed.Load() > 0 {
			settled++
		}
	}
	return settled
}

func (o *answeringOutbound) openConns() int {
	o.access.Lock()
	defer o.access.Unlock()
	var open int
	for _, conn := range o.handed {
		if conn.closed.Load() == 0 {
			open++
		}
	}
	return open
}

// answering builds a measurement a proxy would report for a destination that
// cost remote to reach, from a node that is local away.
func answering(local time.Duration, remote time.Duration) naive.ConnMeasurement {
	return naive.ConnMeasurement{
		RoundTrip:  local + remote,
		ServerSpan: remote,
		RemoteDial: remote,
		HasSpan:    true,
		HasRemote:  true,
	}
}

// raceFixture is a smart outbound over the given groups, each with one member
// answering at its own distance.
type raceFixture struct {
	smart     *Smart
	outbounds map[string]*answeringOutbound
}

func newRaceFixture(t *testing.T, delay time.Duration, legs map[string][2]time.Duration) *raceFixture {
	t.Helper()
	fixture := &raceFixture{outbounds: make(map[string]*answeringOutbound, len(legs))}
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		alpha:  testAlpha,
		cache:  newSmartCache(context.Background(), "smart"),
	}
	// Sorted by tag so the configured order — which fallbacks read — is stable.
	for _, tag := range []string{"a", "b", "c"} {
		leg, configured := legs[tag]
		if !configured {
			continue
		}
		out := &answeringOutbound{delay: delay, measurement: answering(leg[0], leg[1])}
		fixture.outbounds[tag] = out
		member := newMember(tag+"-1", leg[0])
		member.outbound = out
		smart.groups = append(smart.groups, &smartGroup{tag: tag, members: []*smartMember{member}})
	}
	fixture.smart = smart
	return fixture
}

func (f *raceFixture) totalDials() int {
	var total int
	for _, out := range f.outbounds {
		total += int(out.dials.Load())
	}
	return total
}

func TestARacePicksTheLowestScoreAndKeepsItsTunnel(t *testing.T) {
	t.Parallel()
	// b is further away but much closer to the destination: 0.7*80+5 = 61
	// against a's 0.7*29+120 = 140.
	fixture := newRaceFixture(t, 0, map[string][2]time.Duration{
		"a": {ms(29), ms(120)},
		"b": {ms(80), ms(5)},
	})
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)

	winner, conn, err := fixture.smart.race(context.Background(), key, destination, nil)
	require.NoError(t, err)
	require.Equal(t, "b", winner)
	require.NotNil(t, conn, "the winner's tunnel is handed back rather than redialed")

	// Everything that did not win is closed; the winner's is not.
	require.Equal(t, 0, fixture.outbounds["a"].openConns(), "a losing probe must not stay open")
	require.Equal(t, 1, fixture.outbounds["b"].openConns())
	require.NoError(t, conn.Close())
	require.Equal(t, 0, fixture.outbounds["b"].openConns())
}

func TestARaceStoresWhatEveryGroupMeasured(t *testing.T) {
	t.Parallel()
	fixture := newRaceFixture(t, 0, map[string][2]time.Duration{
		"a": {ms(29), ms(120)},
		"b": {ms(80), ms(5)},
	})
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)

	_, conn, err := fixture.smart.race(context.Background(), key, destination, nil)
	require.NoError(t, err)
	conn.Close()

	entry := fixture.smart.cache.load(key)
	require.NotNil(t, entry, "a race must leave a snapshot or the next connection races again")
	require.Equal(t, "b", entry.selected())

	// Both legs, for every group that answered — the local one is what makes a
	// node that has become slow visible later.
	local, remote, ok := entry.legsFor("b")
	require.True(t, ok)
	require.Equal(t, ms(80), local)
	require.Equal(t, ms(5), remote)
	_, _, ok = entry.legsFor("a")
	require.True(t, ok, "a group that lost still contributes a measurement")
}

func TestOneRoundOfProbesPerDestination(t *testing.T) {
	t.Parallel()
	// A page load opening several connections to a host nobody has visited must
	// produce one round of probes. Without the waiting half of the single
	// flight, each connection starts its own round, and what the destination
	// sees is a burst from every exit address at once — the very pattern
	// grouping exists to avoid, at the one moment no group has been chosen yet.
	const connections = 8
	fixture := newRaceFixture(t, 30*time.Millisecond, map[string][2]time.Duration{
		"a": {ms(29), ms(120)},
		"b": {ms(80), ms(5)},
		"c": {ms(64), ms(60)},
	})
	destination := M.ParseSocksaddr("example.com:443")

	var wg sync.WaitGroup
	conns := make([]net.Conn, connections)
	errs := make([]error, connections)
	for i := range connections {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conns[i], errs[i] = fixture.smart.dialTCP(context.Background(), destination)
		}(i)
	}
	wg.Wait()

	for i := range connections {
		require.NoError(t, errs[i])
		require.NotNil(t, conns[i])
		conns[i].Close()
	}

	// Three probes for the single round, plus one real connection for each of
	// the seven that waited — the eighth is carried by the winner's own tunnel.
	require.Equal(t, len(fixture.outbounds)+connections-1, fixture.totalDials(),
		"a second round of probes means the single flight is not wired up")

	// And they all left through the same group, which is the point of deciding
	// once.
	entry := fixture.smart.cache.load(destinationKey(destination))
	require.NotNil(t, entry)
	require.Equal(t, "b", entry.selected())
}

func TestABackgroundRefreshDoesNotStartOverTheTopOfARound(t *testing.T) {
	t.Parallel()
	fixture := newRaceFixture(t, 40*time.Millisecond, map[string][2]time.Duration{
		"a": {ms(29), ms(120)},
		"b": {ms(80), ms(5)},
	})
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := fixture.smart.raceOrWait(context.Background(), key, destination)
		if err == nil {
			conn.Close()
		}
	}()
	require.Eventually(t, func() bool {
		_, running := fixture.smart.races.Load(key)
		return running
	}, time.Second, time.Millisecond)

	fixture.smart.raceDetached(key, destination)
	wg.Wait()
	require.Equal(t, len(fixture.outbounds), fixture.totalDials(),
		"a refresh arriving mid-round must join it rather than duplicate the probes")
}

func TestARaceWithNothingReachableLeavesNoSnapshot(t *testing.T) {
	t.Parallel()
	fixture := newRaceFixture(t, 0, map[string][2]time.Duration{"a": {ms(29), ms(120)}})
	// A proxy that answers without measuring the destination cannot be ranked
	// for it, so it drops out rather than being read as a zero.
	fixture.outbounds["a"].measurement = naive.ConnMeasurement{
		RoundTrip: ms(150), ServerSpan: ms(120), HasSpan: true,
	}
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)

	_, _, err := fixture.smart.race(context.Background(), key, destination, nil)
	require.Error(t, err)
	require.Equal(t, 0, fixture.outbounds["a"].openConns(),
		"a probe that could not be used must still be closed")

	// Nothing usable was measured, so nothing is routable from it — but the
	// attempt is remembered. Without that, a destination that never answers
	// starts a fresh round on every single connection to it: twenty-one of them
	// for one IPv6-only host in production, none of which could have succeeded.
	entry := fixture.smart.cache.load(key)
	require.NotNil(t, entry)
	require.False(t, entry.measured(), "a failed round leaves nothing to route on")
	require.False(t, entry.attemptDue(time.Now()), "and the next round has to wait")
	require.True(t, entry.attemptDue(time.Now().Add(refreshBackoff+time.Second)),
		"but the backoff has to lapse, or the destination is never measured again")
}

func TestAFailedRefreshBacksOffInsteadOfRetryingEveryConnection(t *testing.T) {
	t.Parallel()
	// A race that finds nothing leaves RacedAt where it was, so the entry stays
	// expired. Without recording the attempt, every following connection starts
	// another refresh, each running its full dial budget.
	destination := "3f2a"
	now := time.Now()
	entry := &destinationEntry{RacedAt: now.Add(-2 * snapshotTTL)}
	require.True(t, entry.expired(destination, now))

	entry.noteAttempt(now)
	require.False(t, entry.expired(destination, now),
		"a refresh that just ran must not be started again immediately")
	require.False(t, entry.expired(destination, now.Add(refreshBackoff-time.Second)))
	require.True(t, entry.expired(destination, now.Add(refreshBackoff+time.Second)),
		"but the backoff has to lapse, or the snapshot is never refreshed again")
}

func TestDriftReRacesTheDestinationItWasMeasuredAgainst(t *testing.T) {
	t.Parallel()
	// The key deliberately drops the port, so rebuilding an address from it
	// yields port 0 and the refresh dials somewhere that cannot answer — which
	// is the whole self-healing path silently doing nothing.
	fixture := newRaceFixture(t, 0, map[string][2]time.Duration{"a": {ms(29), ms(5)}})
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)
	group := fixture.smart.groups[0]

	fixture.smart.cache.store(key, &destinationEntry{
		Remote:   map[string]remoteWindow{"a": {ms(5)}},
		Path:     map[string]time.Duration{"a": ms(29)},
		Selected: "a",
		RacedAt:  time.Now(),
	})

	// Three consecutive readings far worse than the snapshot: a destination
	// that moved, or a node that has become slow.
	drifted := answering(ms(29), ms(400))
	for range anomalyStreak {
		fixture.smart.observeScore(key, destination, group, group.members[0], drifted)
	}

	// Waiting on the dial alone would race the refresh: the tunnel is handed out
	// before the round that asked for it has finished with it.
	require.Eventually(t, func() bool {
		return fixture.outbounds["a"].settledConns() == 1
	}, time.Second, time.Millisecond,
		"sustained drift must trigger a refresh, and the refresh must close its tunnel")

	require.Equal(t, []string{"example.com:443"}, fixture.outbounds["a"].dialedTo(),
		"the refresh must go to the destination itself, not to an address rebuilt from the key")
}

func TestAConfirmedAnomalyEntersTheWindow(t *testing.T) {
	t.Parallel()
	// The streak that triggers a refresh is confirmed evidence, and the window
	// must hold it before the refresh judges anything: a degraded group is
	// exactly the one likely to sit the refresh out, and its carried window
	// would otherwise still be innocent of the readings that forced the round.
	fixture := newRaceFixture(t, 0, map[string][2]time.Duration{"a": {ms(29), ms(5)}})
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)
	group := fixture.smart.groups[0]

	fixture.smart.cache.store(key, &destinationEntry{
		Remote:   map[string]remoteWindow{"a": {ms(5)}},
		Path:     map[string]time.Duration{"a": ms(29)},
		Selected: "a",
		RacedAt:  time.Now(),
	})

	for range anomalyStreak {
		fixture.smart.observeScore(key, destination, group, group.members[0], answering(ms(29), ms(400)))
	}

	entry := fixture.smart.cache.load(key)
	require.Equal(t, remoteWindow{ms(400)}, entry.windowFor("a"),
		"the confirmed sample retires the best reading held, which it has just disproved")
	remoteNow, ok := entry.remoteFor("a")
	require.True(t, ok)
	require.Equal(t, ms(400), remoteNow,
		"and it counts immediately: appending would have left the leg reading 5ms")

	// The refresh the streak started must finish before the test tears the
	// fixture down, and what it writes carries the evidence forward: the round
	// extends the window it inherited rather than starting clean, so a group
	// that answers healthily once does not erase a confirmed degradation.
	require.Eventually(t, func() bool {
		return fixture.outbounds["a"].settledConns() == 1
	}, time.Second, time.Millisecond)
	refreshed := fixture.smart.cache.load(key)
	require.Equal(t, remoteWindow{ms(400), ms(5)}, refreshed.windowFor("a"),
		"the refreshed snapshot keeps the degradation alongside the round's own sample")

	// The leg the refresh reports is still the healthy one, and that is the
	// right answer here rather than a miss: this group answered the refresh in
	// 5ms, so a fresh dial to this destination really is fast, and one bout of
	// slow traffic between two healthy rounds is what a median is for. The
	// evidence is not discarded either — it sits in the window, and a
	// degradation that is real recurs, at which point it is the majority.
	remote, ok := refreshed.remoteFor("a")
	require.True(t, ok)
	require.Equal(t, ms(5), remote, "two healthy rounds either side of one bad spell")
}

func TestDriftDetectionSurvivesALargeBonus(t *testing.T) {
	t.Parallel()
	// Comparing scores with the bonus still in them drives the pair through
	// zero once the bonus is large enough, and the guard against a non-positive
	// baseline then switches drift detection off for that group permanently.
	fixture := newRaceFixture(t, 0, map[string][2]time.Duration{"a": {ms(29), ms(5)}})
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)
	group := fixture.smart.groups[0]
	group.bonus = ms(500)

	fixture.smart.cache.store(key, &destinationEntry{
		Remote:   map[string]remoteWindow{"a": {ms(5)}},
		Path:     map[string]time.Duration{"a": ms(29)},
		Selected: "a",
		RacedAt:  time.Now(),
	})
	require.Less(t, score(testAlpha, ms(29), ms(5), group.bonus), time.Duration(0),
		"the case is only interesting while the bonus makes the score negative")

	for range anomalyStreak {
		fixture.smart.observeScore(key, destination, group, group.members[0], answering(ms(29), ms(400)))
	}
	// On the settle rather than on the dial: the refresh runs detached, and a
	// test that returns while it is still in flight leaves a goroutine writing
	// into state nothing is synchronising with any more.
	require.Eventually(t, func() bool {
		return fixture.outbounds["a"].settledConns() == 1
	}, time.Second, time.Millisecond, "a preferred group must still be watched for drift")
}

func TestOrdinaryJitterDoesNotReRace(t *testing.T) {
	t.Parallel()
	fixture := newRaceFixture(t, 0, map[string][2]time.Duration{"a": {ms(29), ms(5)}})
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)
	group := fixture.smart.groups[0]

	fixture.smart.cache.store(key, &destinationEntry{
		Remote:   map[string]remoteWindow{"a": {ms(5)}},
		Path:     map[string]time.Duration{"a": ms(29)},
		Selected: "a",
		RacedAt:  time.Now(),
	})
	for range 10 * anomalyStreak {
		fixture.smart.observeScore(key, destination, group, group.members[0], answering(ms(29), ms(7)))
	}
	// Never rather than sleep-then-check: a sleep long enough to be meaningful
	// is a sleep in every run, and one short enough not to be would pass on a
	// loaded machine because the refresh had not got around to dialing yet.
	require.Never(t, func() bool {
		return fixture.outbounds["a"].dials.Load() > 0
	}, 100*time.Millisecond, 5*time.Millisecond,
		"a couple of milliseconds either way is not a destination moving")
}

// us is the digit-for-digit shape of a relay whose session to its next hop was
// already up when it was asked to dial: 29µs of self-reported work with a
// 57.8ms connect nested inside it, which is impossible. jp is the same
// destination through a hop that reports honestly.
//
// Both are real readings for ash.lg.speedypage.com.
func productionRelayCase(t *testing.T, usMeasurement naive.ConnMeasurement) *Smart {
	t.Helper()
	us := newMember("out-us-dmit", 133218*time.Microsecond) // its own heartbeat
	us.outbound = &answeringOutbound{measurement: usMeasurement}
	jp := newMember("out-jp-ddps", 80416*time.Microsecond)
	jp.outbound = &answeringOutbound{measurement: naive.ConnMeasurement{
		RoundTrip: 249203 * time.Microsecond, ServerSpan: 168787 * time.Microsecond,
		RemoteDial: 168625 * time.Microsecond, HasSpan: true, HasRemote: true,
	}}
	return &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		alpha:  0.7,
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{
			{tag: "us", bonus: 25 * time.Millisecond, members: []*smartMember{us}},
			{tag: "jp", bonus: 40 * time.Millisecond, members: []*smartMember{jp}},
		},
	}
}

func TestARelayThatUnderstatesItsSpanIsNotRanked(t *testing.T) {
	t.Parallel()
	// What shipped: the connect is thrown away as impossible, the group has no
	// destination timing, and it drops out of every race it enters — so an
	// American destination goes to Tokyo, from a node 133ms away that is 58ms
	// from it, because the one node that could say so cannot be believed.
	smart := productionRelayCase(t, naive.ConnMeasurement{
		RoundTrip: 196005 * time.Microsecond, ServerSpan: 29 * time.Microsecond,
		RemoteDial: 57773 * time.Microsecond, HasSpan: true, HasRemote: true,
	})
	destination := M.ParseSocksaddr("ash.lg.speedypage.com:443")

	winner, conn, err := smart.race(context.Background(), destinationKey(destination), destination, nil)
	require.NoError(t, err)
	conn.Close()
	require.Equal(t, "jp", winner)
}

func TestAnHonestRelayWinsTheDestinationItIsNearest(t *testing.T) {
	t.Parallel()
	// The same reading once the hop counts the wait for its next hop's answer:
	// 196.0ms round trip, 133.2ms of it the client's own leg, so 62.8ms of work
	// with the 57.8ms connect sitting inside it.
	//
	//	us  0.7×133.2 +  57.8 − 25 = 126.0ms
	//	jp  0.7× 80.4 + 168.6 − 40 = 184.9ms
	smart := productionRelayCase(t, naive.ConnMeasurement{
		RoundTrip: 196005 * time.Microsecond, ServerSpan: 62787 * time.Microsecond,
		RemoteDial: 57773 * time.Microsecond, HasSpan: true, HasRemote: true,
	})
	destination := M.ParseSocksaddr("ash.lg.speedypage.com:443")

	winner, conn, err := smart.race(context.Background(), destinationKey(destination), destination, nil)
	require.NoError(t, err)
	conn.Close()
	require.Equal(t, "us", winner, "an Ashburn destination belongs on the Ashburn exit")

	// And the decision sticks, so the next connection does not re-race it.
	entry := smart.cache.load(destinationKey(destination))
	require.NotNil(t, entry)
	require.Equal(t, "us", entry.selected())
}

func TestARaceScoresOnWhatItJustMeasured(t *testing.T) {
	t.Parallel()
	// A member that has never carried anything enters with no local at all.
	// Scoring it as though it were zero would let it win every race it is in,
	// so the reading the round itself produced has to replace what was known.
	unmeasured := candidate{tag: "new"}
	r := newRace(testAlpha, []candidate{hk, unmeasured})

	require.Len(t, r.viable(), 2)
	r.observe("hk", ms(1))
	viable := r.viable()
	require.Len(t, viable, 1, "a group with no bound to test must never be pruned on one")
	require.Equal(t, "new", viable[0].tag)

	bound, hasBound := r.waitUntil(0)
	require.True(t, hasBound)
	require.Equal(t, raceHardTimeout, bound,
		"with no local there is nothing to derive a deadline from, so it waits the cap")

	// It answers as a distant node with a slow destination leg, and loses on
	// the measurement rather than winning on the absence of one.
	r.refreshLocal("new", ms(182))
	r.observe("new", ms(200))
	winner, ok := r.winner()
	require.True(t, ok)
	require.Equal(t, "hk", winner)
}

func TestRaceOverflowFallsBackInsteadOfJoiningTheStorm(t *testing.T) {
	t.Parallel()
	// One page load can introduce a hundred destinations at once, and a race is
	// a dial per group. Unbounded, the burst lands on every node together, gets
	// connections reset by the load, and the resets condemn the very nodes
	// being measured — so past the bound, a new destination is dialed on the
	// static ranking and simply stays undecided until a calmer moment.
	fixture := newRaceFixture(t, 0, map[string][2]time.Duration{
		"a": {ms(29), ms(120)},
		"b": {ms(80), ms(5)},
	})
	for range raceConcurrency {
		fixture.smart.raceBound() <- struct{}{}
	}
	destination := M.ParseSocksaddr("overflow.example.com:443")

	conn, err := fixture.smart.dialTCP(context.Background(), destination)
	require.NoError(t, err)
	require.NotNil(t, conn)
	conn.Close()

	require.Equal(t, 1, fixture.totalDials(),
		"the overflow dial goes to one group on the static ranking, not to all of them")
	require.Nil(t, fixture.smart.cache.load(destinationKey(destination)),
		"nothing was measured, so nothing may be remembered")

	// And once the storm has passed, the same destination races normally.
	for range raceConcurrency {
		<-fixture.smart.raceSlots
	}
	conn, err = fixture.smart.dialTCP(context.Background(), destination)
	require.NoError(t, err)
	conn.Close()
	require.NotNil(t, fixture.smart.cache.load(destinationKey(destination)),
		"the deferred destination is decided by its next connection")
}

func TestADeferredRefreshKeepsTheSnapshot(t *testing.T) {
	t.Parallel()
	// A refresh has no caller waiting, so under pressure it gives way first —
	// and the stale snapshot must survive, because stale is still measured.
	fixture := newRaceFixture(t, 0, map[string][2]time.Duration{
		"a": {ms(29), ms(120)},
	})
	key := destinationKey(M.ParseSocksaddr("stale.example.com:443"))
	fixture.smart.cache.store(key, &destinationEntry{
		Remote:   map[string]remoteWindow{"a": {ms(120)}},
		Selected: "a",
		RacedAt:  time.Now().Add(-2 * snapshotTTL),
	})
	for range raceConcurrency {
		fixture.smart.raceBound() <- struct{}{}
	}

	fixture.smart.raceDetached(key, M.ParseSocksaddr("stale.example.com:443"))

	require.Zero(t, fixture.totalDials(), "a deferred refresh must not dial anything")
	entry := fixture.smart.cache.load(key)
	require.NotNil(t, entry)
	require.Equal(t, "a", entry.selected(), "the stale snapshot stays until a refresh gets a slot")
}

// gatedAnsweringOutbound answers only once the test opens its gate, which is
// how a test creates a straggler deterministically instead of by timing.
type gatedAnsweringOutbound struct {
	adapter.Outbound
	gate        chan struct{}
	measurement naive.ConnMeasurement
	dials       atomic.Int32
}

func (o *gatedAnsweringOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *gatedAnsweringOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	o.dials.Add(1)
	return &gatedAnsweringConn{answeringConn: answeringConn{measurement: o.measurement}, gate: o.gate}, nil
}

type gatedAnsweringConn struct {
	answeringConn
	gate chan struct{}
}

func (c *gatedAnsweringConn) Measure(ctx context.Context) (naive.ConnMeasurement, error) {
	select {
	case <-c.gate:
		return c.answeringConn.measurement, nil
	case <-ctx.Done():
		return naive.ConnMeasurement{}, ctx.Err()
	}
}

func TestAStragglerAnsweringInTheGraceRewritesTheVerdict(t *testing.T) {
	t.Parallel()
	// The deadline models an answer as arriving at local + remote; a real answer
	// arrives at local + span, DNS included, so the strongest challenger — the
	// one with the longest deadline — is exactly the one a tight deadline cuts
	// off mid-flight. The caller cannot be kept waiting for it. The verdict can:
	// the snapshot is written after a grace, and the straggler's answer takes it.
	gate := make(chan struct{})
	slow := &gatedAnsweringOutbound{gate: gate, measurement: answering(ms(20), ms(1))}
	slowMember := newMember("a-1", ms(20))
	slowMember.outbound = slow
	fast := &answeringOutbound{measurement: answering(ms(10), ms(100))}
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

	// The gate is still shut when race returns: the caller was not made to wait
	// for the straggler, and it left with the best answer there was.
	winner, conn, err := smart.race(context.Background(), key, destination, nil)
	require.NoError(t, err)
	conn.Close()
	require.Equal(t, "b", winner)

	provisional := smart.cache.load(key)
	require.NotNil(t, provisional, "the snapshot the next connection routes on is already there")
	require.Equal(t, "b", provisional.selected())

	// a answers within the grace: 0.7×20 + 1 − 40 = -25ms against b's 107ms.
	close(gate)
	require.Eventually(t, func() bool {
		entry := smart.cache.load(key)
		if entry == nil {
			return false
		}
		_, measured := entry.remoteFor("a")
		return measured && entry.selected() == "a"
	}, 2*time.Second, 5*time.Millisecond,
		"the straggler's answer must reach the snapshot and take the verdict")
}

// failingOutbound answers with an error on the leg to the proxy — a pooled
// connection that died, a reset — as distinct from the destination being
// unreachable.
type failingOutbound struct {
	adapter.Outbound
	err error
}

func (o *failingOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *failingOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, o.err
}

func TestOneSilentRoundKeepsTheIncumbentEvenWhenAStragglerRewritesIt(t *testing.T) {
	t.Parallel()
	// A race that hands the caller a provisional answer writes the snapshot
	// twice, and the second write judges the carry. Reading the provisional's
	// own carry mark as a previous silent round retires the incumbent's leg
	// after one round instead of two, which is the whole contract: a group that
	// stayed quiet once keeps its place, and only a second silent round takes
	// it. Without that the destination changes exit address on any round its
	// incumbent sits out.
	gate := make(chan struct{})
	straggler := &gatedAnsweringOutbound{gate: gate, measurement: answering(ms(20), ms(5))}
	stragglerMember := newMember("c-1", ms(20))
	stragglerMember.outbound = straggler
	// The incumbent's leg breaks on the way to the proxy, which says nothing
	// about this destination — exactly the case whose leg must be carried.
	silent := &failingOutbound{err: E.New("use of closed network connection")}
	silentMember := newMember("a-1", ms(12))
	silentMember.outbound = silent
	fast := &answeringOutbound{measurement: answering(ms(10), ms(100))}
	fastMember := newMember("b-1", ms(10))
	fastMember.outbound = fast

	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		alpha:  testAlpha,
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{
			{tag: "a", members: []*smartMember{silentMember}},
			{tag: "b", members: []*smartMember{fastMember}},
			{tag: "c", bonus: ms(60), members: []*smartMember{stragglerMember}},
		},
	}
	destination := M.ParseSocksaddr("carried.example.com:443")
	key := destinationKey(destination)
	smart.cache.store(key, &destinationEntry{
		Remote:   map[string]remoteWindow{"a": {ms(30)}, "b": {ms(100)}},
		Path:     map[string]time.Duration{"a": ms(12), "b": ms(10)},
		Selected: "a",
		RacedAt:  time.Now(),
	})

	winner, conn, err := smart.race(context.Background(), key, destination, nil)
	require.NoError(t, err)
	conn.Close()
	require.Equal(t, "b", winner, "only b answered before the deadline")

	provisional := smart.cache.load(key)
	require.NotNil(t, provisional)
	_, carriedNow := provisional.remoteFor("a")
	require.True(t, carriedNow, "the incumbent sat one round out; its leg is carried")
	require.True(t, provisional.carried("a"), "and it is marked as carried, not measured")

	// The straggler answers inside the grace, so the final snapshot is written
	// from the same round. The incumbent has still only been silent once.
	close(gate)
	require.Eventually(t, func() bool {
		entry := smart.cache.load(key)
		if entry == nil {
			return false
		}
		_, answered := entry.remoteFor("c")
		return answered
	}, 2*time.Second, 5*time.Millisecond, "the straggler's answer must reach the snapshot")

	final := smart.cache.load(key)
	remote, stillCarried := final.remoteFor("a")
	require.True(t, stillCarried,
		"the second write of one round must not read its own carry mark as a previous silent round")
	require.Equal(t, ms(30), remote, "and the leg carried is the one measured before the round")
	require.Equal(t, "a", final.selected(), "so hysteresis still has something to hold")
}

func TestACallerLeavingBeforeAnyAnswerDoesNotDiscardTheRound(t *testing.T) {
	t.Parallel()
	// The probes outlive the caller by design: what they measure decides the
	// next six hours. Cancelling them when the caller walks away threw the
	// round out and then stamped a failed-race backoff on a destination that
	// was answering perfectly — so for the next minute every connection routed
	// on the static ranking instead.
	gate := make(chan struct{})
	slow := &gatedAnsweringOutbound{gate: gate, measurement: answering(ms(15), ms(40))}
	member := newMember("a-1", ms(15))
	member.outbound = slow
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		alpha:  testAlpha,
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{{tag: "a", members: []*smartMember{member}}},
	}
	destination := M.ParseSocksaddr("abandoned.example.com:443")
	key := destinationKey(destination)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := smart.race(ctx, key, destination, nil)
	require.Error(t, err, "the caller that hung up gets nothing")

	// The probe answers afterwards, and its measurement is still banked.
	close(gate)
	require.Eventually(t, func() bool {
		entry := smart.cache.load(key)
		if entry == nil {
			return false
		}
		remote, measured := entry.remoteFor("a")
		return measured && remote == ms(40)
	}, 2*time.Second, 5*time.Millisecond,
		"the round the caller abandoned still decides where this destination goes")
}

func TestAnAbandonedRoundHoldsItsSlotUntilTheProbesAreDone(t *testing.T) {
	t.Parallel()
	// The bound counts destinations racing at once, and a round handed to the
	// background half still has its dials outstanding. Giving the slot back
	// when the caller leaves lets a client that opens connections to new
	// destinations and cancels them keep every slot free while the tails pile
	// up — the storm the bound exists to stop, arriving by the one path that
	// no longer goes through it.
	gate := make(chan struct{})
	slow := &gatedAnsweringOutbound{gate: gate, measurement: answering(ms(15), ms(40))}
	member := newMember("a-1", ms(15))
	member.outbound = slow
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		alpha:  testAlpha,
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{{tag: "a", members: []*smartMember{member}}},
	}
	destination := M.ParseSocksaddr("abandoned-slot.example.com:443")
	key := destinationKey(destination)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := smart.raceOrWait(ctx, key, destination)
	require.Error(t, err)

	// The caller is gone but the probe is still out, so the slot is still taken.
	require.Len(t, smart.raceSlots, 1, "the slot belongs to the round, not to the caller")

	close(gate)
	require.Eventually(t, func() bool {
		return len(smart.raceSlots) == 0
	}, 2*time.Second, 5*time.Millisecond,
		"and it comes back once the round has finished collecting")
}
