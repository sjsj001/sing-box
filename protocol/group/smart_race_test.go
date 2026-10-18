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
	// gate, when set, holds every dial until it is closed. See hold.
	gate chan struct{}

	dials  atomic.Int32
	access sync.Mutex
	handed []*answeringConn
	dialed []M.Socksaddr
}

func (o *answeringOutbound) Network() []string { return []string{N.NetworkTCP} }

// hold keeps every dial through this outbound waiting until release is called,
// which is how a test reads what a detached refresh's trigger left behind
// before the refresh itself can overwrite it. These outbounds answer at once,
// so without it the refresh often lands first. The gate is opened on cleanup
// too, so an assertion that fails first does not leave the refresh stuck.
func (o *answeringOutbound) hold(t *testing.T) (release func()) {
	gate := make(chan struct{})
	o.gate = gate
	release = sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)
	return release
}

func (o *answeringOutbound) DialContext(ctx context.Context, _ string, destination M.Socksaddr) (net.Conn, error) {
	o.dials.Add(1)
	if o.gate != nil {
		select {
		case <-o.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
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
		// These outbounds answer at once; what is under test is what the
		// answers say, not whether a loaded scheduler delivered them in time.
		raceIgnoresDeadline: true,
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

	winner, _, conn, err := fixture.smart.race(context.Background(), key, destination, nil)
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

	_, _, conn, err := fixture.smart.race(context.Background(), key, destination, nil)
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

func TestAConnectionThatMissedARoundDoesNotStartAnother(t *testing.T) {
	t.Parallel()
	// The connection looked at the cache before the round wrote and claimed
	// after it let go, so it found no round to wait on and led one of its own:
	// every exit probed the destination twice. Seen under load.
	fixture := newRaceFixture(t, 0, map[string][2]time.Duration{
		"a": {ms(29), ms(120)},
		"b": {ms(80), ms(5)},
		"c": {ms(64), ms(60)},
	})
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)
	_, _, conn, err := fixture.smart.race(context.Background(), key, destination, nil)
	require.NoError(t, err)
	conn.Close()
	// Every probe out, including one a loaded scheduler started after the
	// round had already decided.
	require.Eventually(t, func() bool { return fixture.totalDials() == len(fixture.outbounds) },
		time.Second, time.Millisecond)

	conn, err = fixture.smart.raceOrWait(context.Background(), key, destination, nil)
	require.NoError(t, err)
	conn.Close()
	require.Equal(t, len(fixture.outbounds)+1, fixture.totalDials(), "one dial on what the round decided, not a second round")
}

func TestABackgroundRefreshDoesNotStartOverTheTopOfARound(t *testing.T) {
	t.Parallel()
	fixture := newRaceFixture(t, 40*time.Millisecond, map[string][2]time.Duration{
		"a": {ms(29), ms(120)},
		"b": {ms(80), ms(5)},
	})
	destination := M.ParseSocksaddr("example.com:443")
	key := destinationKey(destination)
	// Held, so the round is certainly still running when the refresh arrives;
	// on a loaded machine its 40ms could otherwise be over by then.
	releaseA := fixture.outbounds["a"].hold(t)
	releaseB := fixture.outbounds["b"].hold(t)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := fixture.smart.raceOrWait(context.Background(), key, destination, nil)
		if err == nil {
			_, _ = conn.Read(make([]byte, 1)) // the race runs behind the connection; a read waits it out
			conn.Close()
		}
	}()
	require.Eventually(t, func() bool {
		_, running := fixture.smart.races.Load(key)
		return running
	}, time.Second, time.Millisecond)

	fixture.smart.raceDetached(key, destination)
	releaseA()
	releaseB()
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

	_, _, _, err := fixture.smart.race(context.Background(), key, destination, nil)
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

	// Held until the window is read: the refresh the streak starts rewrites it.
	release := fixture.outbounds["a"].hold(t)
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
	release()
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
		ctx:                 context.Background(),
		logger:              logger.NOP(),
		alpha:               0.7,
		cache:               newSmartCache(context.Background(), "smart"),
		raceIgnoresDeadline: true,
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

	winner, _, conn, err := smart.race(context.Background(), destinationKey(destination), destination, nil)
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

	winner, _, conn, err := smart.race(context.Background(), destinationKey(destination), destination, nil)
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
	_, _ = conn.Read(make([]byte, 1)) // the race runs behind the connection; a read waits it out
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

// unreachableOutbound is an exit that cannot reach anything it is asked for
// and says so at once, counting the tunnels opened through it.
type unreachableOutbound struct {
	adapter.Outbound
	dials atomic.Int32
}

func (o *unreachableOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *unreachableOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	o.dials.Add(1)
	return nil, E.Cause(naive.ErrDestinationUnreachable, "connection refused")
}

func unreachableSmart(tags ...string) (*Smart, func() int) {
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		alpha:  testAlpha,
		cache:  newSmartCache(context.Background(), "smart"),
	}
	var exits []*unreachableOutbound
	for _, tag := range tags {
		exit := &unreachableOutbound{}
		exits = append(exits, exit)
		member := newMember(tag+"-1", ms(30))
		member.outbound = exit
		smart.groups = append(smart.groups, &smartGroup{tag: tag, members: []*smartMember{member}})
	}
	return smart, func() (total int) {
		for _, exit := range exits {
			total += int(exit.dials.Load())
		}
		return total
	}
}

func TestADestinationRefusedWithoutARaceIsStillCooledDown(t *testing.T) {
	t.Parallel()
	// With every race slot busy a new destination is dialed on the static
	// ranking, and that path makes no snapshot — while a refusal's cooldown was
	// only ever filed on one. A dead peer reached that way was asked of every
	// group on every connection, a tunnel per group each time, for as long as
	// the slots stayed full; and what keeps them full is mostly dead peers.
	smart, dials := unreachableSmart("a", "b", "c")
	for range raceConcurrency {
		smart.raceBound() <- struct{}{}
	}
	destination := M.ParseSocksaddr("203.0.113.9:6881")

	_, err := smart.dialTCP(context.Background(), destination)
	require.ErrorIs(t, err, naive.ErrDestinationUnreachable)
	require.Equal(t, 3, dials(), "the first connection asks every group, as it must")

	_, err = smart.dialTCP(context.Background(), destination)
	require.ErrorIs(t, err, naive.ErrDestinationUnreachable,
		"the next one is answered with the same verdict")
	require.Equal(t, 3, dials(), "from the cooldowns, without a tunnel")
}

func TestARoundEveryGroupRefusedKeepsTheCooldownsItFiled(t *testing.T) {
	t.Parallel()
	// A round on a destination with no snapshot files each refusal on an entry
	// the refusal makes, and then records the failed attempt. Recorded on a
	// fresh entry, the attempt replaced the one holding the cooldowns, and the
	// next connection asked again the group the round had just heard refuse.
	// One group, so the round files exactly one cooldown and the count is exact.
	smart, dials := unreachableSmart("a")
	destination := M.ParseSocksaddr("203.0.113.9:6881")

	// The round runs behind the connection, so its verdict is the first read's.
	conn, err := smart.dialTCP(context.Background(), destination)
	require.NoError(t, err)
	_, err = conn.Read(make([]byte, 1))
	require.ErrorIs(t, err, naive.ErrDestinationUnreachable)
	conn.Close()
	require.Equal(t, 1, dials())

	_, err = smart.dialTCP(context.Background(), destination)
	require.ErrorIs(t, err, naive.ErrDestinationUnreachable)
	require.Equal(t, 1, dials(), "the round's own refusal still holds")
}

func TestAFirstRoundSomeoneAnsweredKeepsTheCooldownsItFiled(t *testing.T) {
	t.Parallel()
	// The same round with a group answering writes a snapshot instead, and it
	// replaced the entry holding the cooldowns with the nil the round started
	// from. The next failover dialed a again to hear the same 502.
	refusing := newMember("a-1", ms(30))
	refusing.outbound = &unreachableOutbound{}
	// b scores 0.7×30 + 100 = 121ms, so a — bounded at 21ms — is waited for
	// rather than pruned, and its refusal is in before the snapshot is written.
	fixture := newRaceFixture(t, 0, map[string][2]time.Duration{"b": {ms(30), ms(100)}})
	smart := fixture.smart
	smart.groups = append([]*smartGroup{{tag: "a", members: []*smartMember{refusing}}}, smart.groups...)
	destination := M.ParseSocksaddr("example.com:25")
	key := destinationKey(destination)

	winner, _, conn, err := smart.race(context.Background(), key, destination, nil)
	require.NoError(t, err)
	conn.Close()
	require.Equal(t, "b", winner)
	require.True(t, smart.cache.load(key).blocked("a", 25, refusing, time.Now()))
}

func TestAFirstRoundKeepsTheCooldownsItFiledThroughItsProvisionalSnapshot(t *testing.T) {
	t.Parallel()
	// The same, on the snapshot a race writes before its stragglers are in.
	gate := make(chan struct{})
	t.Cleanup(sync.OnceFunc(func() { close(gate) }))
	refusing := newMember("a-1", ms(30))
	refusing.outbound = &unreachableOutbound{}
	fast := newMember("b-1", ms(30))
	fast.outbound = &answeringOutbound{measurement: answering(ms(30), ms(100))}
	straggler := newMember("c-1", ms(20))
	straggler.outbound = &gatedAnsweringOutbound{gate: gate, measurement: answering(ms(20), ms(1))}
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		alpha:  testAlpha,
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{
			{tag: "a", members: []*smartMember{refusing}},
			{tag: "b", members: []*smartMember{fast}},
			{tag: "c", members: []*smartMember{straggler}},
		},
	}
	destination := M.ParseSocksaddr("example.com:25")
	key := destinationKey(destination)
	run, _ := smart.claimRace(key)

	winner, _, conn, err := smart.race(context.Background(), key, destination, run)
	require.NoError(t, err)
	conn.Close()
	require.Equal(t, "b", winner)
	require.True(t, smart.cache.load(key).blocked("a", 25, refusing, time.Now()), "the provisional snapshot keeps it")
	<-run.done
	require.True(t, smart.cache.load(key).blocked("a", 25, refusing, time.Now()), "and so does the one written after the grace")
}

// gatedAnsweringOutbound answers only once the test opens its gate, which is
// how a test creates a straggler deterministically instead of by timing.
type gatedAnsweringOutbound struct {
	adapter.Outbound
	gate        chan struct{}
	measurement naive.ConnMeasurement
	// deaf makes Measure wait for the gate even after the round is cancelled,
	// so a test decides when the outcome reaches the channel.
	deaf   bool
	dials  atomic.Int32
	handed atomic.Pointer[gatedAnsweringConn]
}

func (o *gatedAnsweringOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *gatedAnsweringOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	o.dials.Add(1)
	conn := &gatedAnsweringConn{answeringConn: answeringConn{measurement: o.measurement}, gate: o.gate, deaf: o.deaf}
	o.handed.Store(conn)
	return conn, nil
}

// closedConn reports whether the last tunnel this outbound handed out has
// been closed.
func (o *gatedAnsweringOutbound) closedConn() bool {
	conn := o.handed.Load()
	return conn != nil && conn.closed.Load() > 0
}

type gatedAnsweringConn struct {
	answeringConn
	gate chan struct{}
	deaf bool
}

func (c *gatedAnsweringConn) Measure(ctx context.Context) (naive.ConnMeasurement, error) {
	if c.deaf {
		<-c.gate
		return c.answeringConn.measurement, nil
	}
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
	// can run over it — this connection's interior beyond the banked estimate, a
	// lookup cold when races run — so the strongest challenger, the one with the
	// longest deadline, is exactly the one a tight deadline cuts off mid-flight. The caller cannot be kept waiting for it. The verdict can:
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
		// The gate opens after race returns, and on a loaded machine the
		// answer can take longer than the real grace to come back.
		raceGrace: raceDialTimeout,
	}
	destination := M.ParseSocksaddr("courier.example.com:443")
	key := destinationKey(destination)

	// The gate is still shut when race returns: the caller was not made to wait
	// for the straggler, and it left with the best answer there was.
	winner, _, conn, err := smart.race(context.Background(), key, destination, nil)
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
	require.True(t, slow.closedConn(), "the straggler's tunnel is surplus and must be closed")
}

func TestAStragglerThatMissesTheGraceIsStillClosed(t *testing.T) {
	t.Parallel()
	gate := make(chan struct{})
	t.Cleanup(sync.OnceFunc(func() { close(gate) }))
	slow := &gatedAnsweringOutbound{gate: gate, measurement: answering(ms(20), ms(1))}
	slowMember := newMember("a-1", ms(20))
	slowMember.outbound = slow
	fastMember := newMember("b-1", ms(10))
	fastMember.outbound = &answeringOutbound{measurement: answering(ms(10), ms(100))}
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
	destination := M.ParseSocksaddr("missed.example.com:443")
	key := destinationKey(destination)
	run, _ := smart.claimRace(key)

	winner, _, conn, err := smart.race(context.Background(), key, destination, run)
	require.NoError(t, err)
	conn.Close()
	require.Equal(t, "b", winner)
	// The round is over once the grace has run out. The cancel that follows
	// makes a's probe give up with its tunnel open, and nobody but the drainer
	// is left to read it.
	<-run.done
	require.Eventually(t, slow.closedConn, time.Second, time.Millisecond,
		"a probe still out when the grace ends must not leave its tunnel open")
}

func TestARoundStoppedBeforeAnyAnswerClosesTheLateTunnels(t *testing.T) {
	t.Parallel()
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)
	slow := &gatedAnsweringOutbound{gate: gate, measurement: answering(ms(20), ms(1)), deaf: true}
	member := newMember("a-1", ms(20))
	member.outbound = slow
	// The outbound is shutting down: the round stops with nothing in hand,
	// and the probe answers only after race has returned.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	smart := &Smart{
		ctx:    ctx,
		logger: logger.NOP(),
		alpha:  testAlpha,
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{{tag: "a", members: []*smartMember{member}}},
	}
	destination := M.ParseSocksaddr("stopped.example.com:443")

	_, _, _, err := smart.race(context.Background(), destinationKey(destination), destination, nil)
	require.Error(t, err)
	release()
	require.Eventually(t, slow.closedConn, time.Second, time.Millisecond,
		"a probe that answers after the round stopped must not leave its tunnel open")
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
		raceGrace: raceDialTimeout, // see TestAStragglerAnsweringInTheGraceRewritesTheVerdict
	}
	destination := M.ParseSocksaddr("carried.example.com:443")
	key := destinationKey(destination)
	smart.cache.store(key, &destinationEntry{
		Remote:   map[string]remoteWindow{"a": {ms(30)}, "b": {ms(100)}},
		Path:     map[string]time.Duration{"a": ms(12), "b": ms(10)},
		Selected: "a",
		RacedAt:  time.Now(),
	})

	winner, _, conn, err := smart.race(context.Background(), key, destination, nil)
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
	_, _, _, err := smart.race(ctx, key, destination, nil)
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
	_, err := smart.raceOrWait(ctx, key, destination, nil)
	require.Error(t, err)

	// The caller is gone but the probe is still out, so the slot is still taken.
	require.Len(t, smart.raceSlots, 1, "the slot belongs to the round, not to the caller")

	close(gate)
	require.Eventually(t, func() bool {
		return len(smart.raceSlots) == 0
	}, 2*time.Second, 5*time.Millisecond,
		"and it comes back once the round has finished collecting")
}

func TestJitterNextToANearProxyIsNotAMove(t *testing.T) {
	t.Parallel()
	// A 5ms proxy and a 1ms CDN score about 4.5ms, so 5ms of jitter on the
	// near hop doubles the score. The multiple alone read that as the
	// destination moving, retired its best reading and re-raced it.
	fixture := newRaceFixture(t, 0, map[string][2]time.Duration{"a": {ms(5), ms(1)}})
	destination := M.ParseSocksaddr("cdn.example.com:443")
	key := destinationKey(destination)
	group := fixture.smart.groups[0]
	fixture.smart.cache.store(key, &destinationEntry{
		Remote:   map[string]remoteWindow{"a": {ms(1), ms(1), ms(1)}},
		Path:     map[string]time.Duration{"a": ms(5)},
		Selected: "a",
		RacedAt:  time.Now(),
	})

	for range anomalyStreak {
		fixture.smart.observeScore(key, destination, group, group.members[0],
			answering(ms(12), ms(1)+300*time.Microsecond))
	}
	require.Equal(t, remoteWindow{ms(1), ms(1), ms(1)}, fixture.smart.cache.load(key).windowFor("a"))
	require.Zero(t, fixture.outbounds["a"].dials.Load())
}

func TestARoundThatFoundNothingSaysWhose(t *testing.T) {
	t.Parallel()
	// A relay answers 502 for "every group refused", and the hop downstream
	// cools the destination down. When the round failed on its own legs, that
	// was the wrong verdict: an outage here left every new destination cooling
	// down downstream, and still cooling after it ended.
	smartWith := func(outbound func() adapter.Outbound) *Smart {
		s := &Smart{ctx: context.Background(), logger: logger.NOP(), alpha: testAlpha,
			cache: newSmartCache(context.Background(), "smart")}
		for _, tag := range []string{"a", "b"} {
			member := newMember(tag+"-1", ms(30))
			member.outbound = outbound()
			s.groups = append(s.groups, &smartGroup{tag: tag, members: []*smartMember{member}})
		}
		return s
	}
	destination := M.ParseSocksaddr("fresh.example:443")
	key := destinationKey(destination)

	broken := smartWith(func() adapter.Outbound { return &countingOutbound{} })
	_, _, _, err := broken.race(context.Background(), key, destination, nil)
	require.ErrorIs(t, err, naive.ErrNextHopUnreachable)
	require.NotErrorIs(t, err, naive.ErrDestinationUnreachable)

	refused := smartWith(func() adapter.Outbound {
		return &egressOutbound{reach: func(M.Socksaddr) bool { return false }, dialed: make(chan M.Socksaddr, 8)}
	})
	_, _, _, err = refused.race(context.Background(), key, destination, nil)
	require.ErrorIs(t, err, naive.ErrDestinationUnreachable)
}

// racedTunnel is a won race's tunnel with a download to read, a proxy's
// ack to report and a request side to close.
type racedTunnel struct {
	answeringConn
	closedWrite atomic.Int32
}

func (c *racedTunnel) Read(p []byte) (int, error) { return copy(p, "payload"), nil }
func (c *racedTunnel) CloseWrite() error          { c.closedWrite.Add(1); return nil }
func (c *racedTunnel) RemoteAck(context.Context) (naive.ConnectAck, bool, error) {
	return naive.ConnectAck{Total: ms(7)}, true, nil
}

type racedTunnelOutbound struct {
	adapter.Outbound
	conn *racedTunnel
}

func (o *racedTunnelOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *racedTunnelOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return o.conn, nil
}

func TestARacedConnectionIsItsMembersAsAnyOther(t *testing.T) {
	t.Parallel()
	// The winner's tunnel went to the caller as it was, so a raced download
	// stamped nothing: the member carrying it looked idle to Smart.congested,
	// and a probe left unanswered behind that download condemned it.
	tunnel := &racedTunnel{answeringConn: answeringConn{measurement: answering(ms(30), ms(5))}}
	member := newMember("a-1", ms(30))
	member.outbound = &racedTunnelOutbound{conn: tunnel}
	s := &Smart{
		ctx: context.Background(), logger: logger.NOP(), alpha: testAlpha,
		cache:               newSmartCache(context.Background(), "smart"),
		groups:              []*smartGroup{{tag: "a", members: []*smartMember{member}}},
		raceIgnoresDeadline: true,
	}
	destination := M.ParseSocksaddr("example.com:443")

	conn, err := s.raceOrWait(context.Background(), destinationKey(destination), destination, nil)
	require.NoError(t, err)
	defer conn.Close()
	require.Zero(t, member.deliveredAt.Load())
	_, err = conn.Read(make([]byte, 16))
	require.NoError(t, err)
	require.EqualValues(t, 1, member.requestsTCP.total.Load(), "counted once the race has decided")
	require.NotZero(t, member.deliveredAt.Load(), "a read stamps the member that raced")

	reporter, isReporter := conn.(naive.RemoteDialReporter)
	require.True(t, isReporter)
	ack, hasAck, err := reporter.RemoteAck(context.Background())
	require.NoError(t, err)
	require.True(t, hasAck)
	require.Equal(t, ms(7), ack.Total)
	require.NoError(t, conn.(N.WriteCloser).CloseWrite())
	require.EqualValues(t, 1, tunnel.closedWrite.Load())
}
