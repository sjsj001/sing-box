package group

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/bbolt"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/experimental/cachefile"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/naive"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

func TestSnapshotLifetimeStaysWithinAQuarterEitherWay(t *testing.T) {
	t.Parallel()
	for _, destination := range []string{"apple.com", "1.1.1.1", "mirror.de.leaseweb.net", ""} {
		lifetime := snapshotLifetime(destination)
		require.GreaterOrEqual(t, lifetime, snapshotTTL*3/4, destination)
		require.Less(t, lifetime, snapshotTTL*5/4, destination)
	}
}

func TestSnapshotLifetimeIsStableForADestination(t *testing.T) {
	t.Parallel()
	// Derived from the destination, not drawn at random: a restart must not
	// reshuffle when everything is due, or the spread buys nothing.
	first := snapshotLifetime("apple.com")
	for range 10 {
		require.Equal(t, first, snapshotLifetime("apple.com"))
	}
}

func TestSnapshotLifetimeSpreadsAcrossDestinations(t *testing.T) {
	t.Parallel()
	// A page load reaches many hosts at once; they must not all come due
	// together six hours later.
	seen := make(map[time.Duration]bool)
	for _, destination := range []string{
		"a.example.com", "b.example.com", "c.example.com", "d.example.com",
		"e.example.com", "f.example.com", "g.example.com", "h.example.com",
	} {
		seen[snapshotLifetime(destination)] = true
	}
	require.Greater(t, len(seen), 6, "lifetimes should be spread, not clustered")
}

func TestEntryExpiresOnItsOwnLifetime(t *testing.T) {
	t.Parallel()
	const destination = "apple.com"
	lifetime := snapshotLifetime(destination)
	now := time.Now()
	entry := &destinationEntry{RacedAt: now.Add(-lifetime + time.Minute)}
	require.False(t, entry.expired(destination, now))

	entry.RacedAt = now.Add(-lifetime - time.Minute)
	require.True(t, entry.expired(destination, now))
}

func TestUnreachableGroupIsExcludedUntilTheCooldownPasses(t *testing.T) {
	t.Parallel()
	// A proxy that cannot reach one destination is usually fine for everything
	// else, so the exclusion is scoped to the destination and it lapses.
	now := time.Now()
	entry := &destinationEntry{Remote: map[string]remoteWindow{"hk": {time.Millisecond}}}
	require.False(t, entry.blocked("hk", 443, nil, now))

	entry.block("hk", 443, now, nil)
	require.True(t, entry.blocked("hk", 443, nil, now))
	require.True(t, entry.blocked("hk", 443, nil, now.Add(destinationFailureCooldown-time.Second)))
	require.False(t, entry.blocked("hk", 443, nil, now.Add(destinationFailureCooldown+time.Second)))
	require.False(t, entry.blocked("jp", 443, nil, now), "other groups must be untouched")
}

func TestAnomalyNeedsASustainedRun(t *testing.T) {
	t.Parallel()
	// The snapshot is frozen between races, so a destination that moves is
	// otherwise invisible. Reacting to one reading would re-race on jitter;
	// reacting to none would leave traffic on a route that no longer exists.
	entry := &destinationEntry{}
	require.Less(t, entry.countAnomaly(), anomalyStreak)
	require.Less(t, entry.countAnomaly(), anomalyStreak)
	require.Equal(t, anomalyStreak, entry.countAnomaly())

	// A normal reading clears the run.
	entry.resetAnomalies()
	require.Less(t, entry.countAnomaly(), anomalyStreak)

	// Destinations are counted independently, and the count goes away with the
	// entry rather than outliving it in a map beside the cache.
	require.Less(t, (&destinationEntry{}).countAnomaly(), anomalyStreak)
}

func TestTheStoredKeyIsNotTheHostname(t *testing.T) {
	t.Parallel()
	// The snapshots are a list of every destination a route was chosen for,
	// written to a file sing-box does not encrypt. Whatever else it is, it must
	// not be legible as a browsing history.
	key := destinationKey(M.ParseSocksaddr("private.example.com:443"))
	require.NotContains(t, key, "example")
	require.NotContains(t, key, "private")

	// Stable across restarts, or every restart re-races everything.
	require.Equal(t, key, destinationKey(M.ParseSocksaddr("private.example.com:443")))
	// Port-independent, since the same host on 80 and 443 is one machine at one
	// distance.
	require.Equal(t, key, destinationKey(M.ParseSocksaddr("private.example.com:80")))
	require.NotEqual(t, key, destinationKey(M.ParseSocksaddr("other.example.com:443")))
}

func TestCooldownsAreCopiedOntoTheSnapshotThatReplacesThem(t *testing.T) {
	t.Parallel()
	// Handing the map over instead of copying it leaves two entries — each with
	// its own mutex — writing the same map, and Go aborts the process for that
	// rather than merely reporting a race.
	previous := &destinationEntry{Remote: map[string]remoteWindow{"a": {ms(1)}}}
	now := time.Now()
	previous.block("a", 443, now, nil)

	updated := &destinationEntry{Remote: map[string]remoteWindow{"a": {ms(1)}}}
	updated.carryCooldowns(previous)
	require.True(t, updated.blocked("a", 443, nil, now), "the cooldown must survive the swap")

	updated.block("b", 443, now, nil)
	require.False(t, previous.blocked("b", 443, nil, now), "the two entries must not share a map")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 500 {
			previous.block("c", 443, now, nil)
		}
	}()
	go func() {
		defer wg.Done()
		for range 500 {
			updated.block("c", 443, now, nil)
		}
	}()
	wg.Wait()
}

// onDisk gives a cache a real database, bypassing the cache-file service that
// only exists inside a running box.
func onDisk(t *testing.T) *smartCache {
	t.Helper()
	db, err := bbolt.Open(filepath.Join(t.TempDir(), "cache.db"), 0o600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	return anotherOutboundOn(t, db, "smart")
}

// anotherOutboundOn gives one more outbound a cache on a database another
// already has, so a test can say what two of them do to each other's rows.
func anotherOutboundOn(t *testing.T, db *bbolt.DB, tag string) *smartCache {
	t.Helper()
	cache := newSmartCache(context.Background(), tag)
	cache.resolve.Do(func() { cache.cacheFile = &cachefile.CacheFile{DB: db} })
	return cache
}

func TestTwoOutboundsDoNotOverwriteEachOthersSnapshots(t *testing.T) {
	t.Parallel()
	// A destination key is a digest of the host, so two smart outbounds in one
	// process file the same destination under the same key. Sharing a bucket
	// then means each start hands one of them the other's row — naming groups it
	// does not have, for a destination it cannot re-race until that row expires.
	main := onDisk(t)
	force := anotherOutboundOn(t, main.db(), "out-us")

	main.store("3f2a", &destinationEntry{
		Remote:   map[string]remoteWindow{"jp": {ms(6)}, "us": {ms(90)}},
		Path:     map[string]time.Duration{"jp": ms(80), "us": ms(126)},
		Selected: "jp",
		RacedAt:  time.Now(),
	})
	require.NoError(t, main.flush())
	force.store("3f2a", &destinationEntry{
		Remote:   map[string]remoteWindow{"us": {ms(90)}},
		Path:     map[string]time.Duration{"us": ms(126)},
		Selected: "us",
		RacedAt:  time.Now(),
	})
	require.NoError(t, force.flush())

	restarted := anotherOutboundOn(t, main.db(), "smart")
	restarted.preload()
	entry := restarted.load("3f2a")
	require.NotNil(t, entry)
	require.Equal(t, "jp", entry.selected(),
		"the single-group outbound's row must not become the five-group one's")
	_, _, ok := entry.legsFor("jp")
	require.True(t, ok, "the groups it can score have to survive the other outbound's write")

	other := anotherOutboundOn(t, main.db(), "out-us")
	other.preload()
	require.Equal(t, "us", other.load("3f2a").selected())
}

func TestSnapshotsSurviveTheCacheFilesOwnStartup(t *testing.T) {
	t.Parallel()
	// The cache file deletes everything at its root it does not recognise, on
	// every start. A bucket per outbound sitting there would be written, wiped
	// by the next start, and the persistence it exists for would be quietly
	// gone — while every test that mounts a bare database went on passing. That
	// is what this one is for: it is the only one that runs the cache file's
	// own startup.
	path := filepath.Join(t.TempDir(), "cache.db")
	ctx := context.Background()

	firstScope := adapter.NewScope(ctx, logger.NOP())
	first := cachefile.New(ctx, logger.NOP(), option.CacheFileOptions{Path: path})
	require.NoError(t, first.Start(adapter.StartStateInitialize, firstScope))
	cache := newSmartCache(ctx, "out-main")
	cache.resolve.Do(func() { cache.cacheFile = first })
	cache.store("3f2a", &destinationEntry{
		Remote:   map[string]remoteWindow{"jp": {ms(6)}},
		Path:     map[string]time.Duration{"jp": ms(80)},
		Selected: "jp",
		RacedAt:  time.Now(),
	})
	require.NoError(t, cache.flush())
	require.NoError(t, firstScope.Close())

	secondScope := adapter.NewScope(ctx, logger.NOP())
	second := cachefile.New(ctx, logger.NOP(), option.CacheFileOptions{Path: path})
	require.NoError(t, second.Start(adapter.StartStateInitialize, secondScope))
	t.Cleanup(func() { secondScope.Close() })
	restarted := newSmartCache(ctx, "out-main")
	restarted.resolve.Do(func() { restarted.cacheFile = second })
	restarted.preload()

	entry := restarted.load("3f2a")
	require.NotNil(t, entry, "the cache file's own startup must not take these with it")
	require.Equal(t, "jp", entry.selected())
}

func TestTheRowsFromBeforeSeparationAreDropped(t *testing.T) {
	t.Parallel()
	// Which outbound each of those rows belongs to cannot be settled, so none of
	// them is inherited. Nothing else would ever remove them either: the
	// compaction that bounds this file only walks the rows an outbound owns.
	cache := onDisk(t)
	require.NoError(t, cache.db().Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(smartBucket)
		if err != nil {
			return err
		}
		return bucket.Put([]byte("3f2a"), []byte(`{"remote":{"us":[90000000]},"selected":"us","raced_at":"2026-08-11T00:00:00Z"}`))
	}))

	cache.preload()

	require.Nil(t, cache.load("3f2a"), "a row of unsettled ownership is not inherited")
	require.NoError(t, cache.db().View(func(tx *bbolt.Tx) error {
		require.Nil(t, tx.Bucket(smartBucket).Get([]byte("3f2a")))
		return nil
	}))
}

func TestARenamedOutboundsSnapshotsAreEventuallyReclaimed(t *testing.T) {
	t.Parallel()
	// A renamed or removed outbound leaves its sub-bucket behind, and no other
	// outbound's compaction reaches it — the same "nothing else ever will" that
	// makes the pre-separation rows worth dropping. What settles it is age: a
	// sub-bucket whose newest snapshot is past snapshotRetention holds only what
	// that constant already calls somewhere the user went once.
	cache := onDisk(t)
	gone := anotherOutboundOn(t, cache.db(), "out-old")
	gone.store("3f2a", &destinationEntry{
		Remote:  map[string]remoteWindow{"us": {ms(90)}},
		RacedAt: time.Now().Add(-snapshotRetention - time.Hour),
	})
	require.NoError(t, gone.flush())

	busy := anotherOutboundOn(t, cache.db(), "out-idle")
	busy.store("7c19", &destinationEntry{
		Remote:  map[string]remoteWindow{"us": {ms(90)}},
		RacedAt: time.Now().Add(-2 * snapshotTTL),
	})
	require.NoError(t, busy.flush())

	cache.preload()

	require.NoError(t, cache.db().View(func(tx *bbolt.Tx) error {
		require.Nil(t, tx.Bucket(smartBucket).Bucket([]byte("out-old")))
		require.NotNil(t, tx.Bucket(smartBucket).Bucket([]byte("out-idle")),
			"stale is not abandoned; per-row expiry is what drops those")
		return nil
	}))
}

func TestSnapshotsSurviveARestart(t *testing.T) {
	t.Parallel()
	// The whole reason for writing them down: without this every restart
	// re-races every destination, and moves traffic to a different country
	// while it does.
	cache := onDisk(t)
	cache.store("3f2a", &destinationEntry{
		Remote:   map[string]remoteWindow{"hk": {ms(5)}},
		Path:     map[string]time.Duration{"hk": ms(29)},
		Selected: "hk",
		RacedAt:  time.Now(),
	})
	require.NoError(t, cache.flush())

	restarted := newSmartCache(context.Background(), "smart")
	restarted.resolve.Do(func() { restarted.cacheFile = cache.cacheFile })
	restarted.preload()

	entry := restarted.load("3f2a")
	require.NotNil(t, entry)
	require.Equal(t, "hk", entry.selected())
	local, remote, ok := entry.legsFor("hk")
	require.True(t, ok)
	require.Equal(t, ms(29), local)
	require.Equal(t, ms(5), remote)
}

func TestAbandonedSnapshotsAreNotKeptForever(t *testing.T) {
	t.Parallel()
	// An expired snapshot is still a real measurement and is used while a
	// refresh runs behind it, so expiry is not a reason to drop one. A
	// destination not reached for a week is another matter: what is left is a
	// record of somewhere the user went once.
	cache := onDisk(t)
	cache.store("recent", &destinationEntry{
		Remote:  map[string]remoteWindow{"hk": {ms(5)}},
		RacedAt: time.Now().Add(-2 * snapshotTTL),
	})
	cache.store("abandoned", &destinationEntry{
		Remote:  map[string]remoteWindow{"hk": {ms(5)}},
		RacedAt: time.Now().Add(-snapshotRetention - time.Hour),
	})
	require.NoError(t, cache.flush())

	restarted := newSmartCache(context.Background(), "smart")
	restarted.resolve.Do(func() { restarted.cacheFile = cache.cacheFile })
	restarted.preload()

	require.NotNil(t, restarted.load("recent"), "stale is not the same as abandoned")
	require.Nil(t, restarted.load("abandoned"))

	// And it is gone from the file, not merely skipped on the way in.
	require.NoError(t, restarted.db().View(func(tx *bbolt.Tx) error {
		require.Nil(t, restarted.ownBucket(tx).Get([]byte("abandoned")))
		return nil
	}))
}

func TestASnapshotFromTheFutureIsKeptAsWritten(t *testing.T) {
	t.Parallel()
	// Booted with the clock behind (no RTC, a restored VM), every row on disk
	// is from the future. Its date is the only record of when it was raced:
	// dropping it, or dating it by this clock and writing that back, loses it
	// once NTP puts the clock right. So it is loaded and left exactly as it is.
	cache := onDisk(t)
	cache.store("recent", &destinationEntry{
		Remote:  map[string]remoteWindow{"hk": {ms(5)}},
		RacedAt: time.Now(),
	})
	racedAt := time.Now().Add(30 * 24 * time.Hour)
	cache.store("future", &destinationEntry{
		Remote:  map[string]remoteWindow{"hk": {ms(5)}},
		RacedAt: racedAt,
	})
	require.NoError(t, cache.flush())

	restarted := newSmartCache(context.Background(), "smart")
	restarted.resolve.Do(func() { restarted.cacheFile = cache.cacheFile })
	restarted.preload()

	require.NotNil(t, restarted.load("recent"))
	future := restarted.load("future")
	require.NotNil(t, future, "kept: the wrong clock may be this one")
	require.True(t, future.RacedAt.Equal(racedAt), "dated as written, not by this clock")
	require.NoError(t, restarted.flush())
	require.NoError(t, restarted.db().View(func(tx *bbolt.Tx) error {
		var onDisk destinationEntry
		require.NoError(t, json.Unmarshal(restarted.ownBucket(tx).Get([]byte("future")), &onDisk))
		require.True(t, onDisk.RacedAt.Equal(racedAt), "and the row on disk still says so")
		return nil
	}))
}

func TestAFailedFlushDoesNotDiscardWhatItWasWriting(t *testing.T) {
	t.Parallel()
	// The marks are cleared before the write on the assumption it lands. If a
	// failure left them cleared, every decision made since the last successful
	// flush would be dropped without a trace.
	cache := onDisk(t)
	cache.store("3f2a", &destinationEntry{
		Remote:  map[string]remoteWindow{"hk": {ms(5)}},
		RacedAt: time.Now(),
	})
	require.NoError(t, cache.db().Close())

	require.Error(t, cache.flush(), "writing into a closed database has to be reported")
	cache.access.Lock()
	stillDirty := cache.dirty["3f2a"]
	cache.access.Unlock()
	require.True(t, stillDirty, "the entry must still be queued for the next attempt")
}

func TestTheGroupInUseKeepsItsPlaceThroughOneSilentRound(t *testing.T) {
	t.Parallel()
	// A race stores only what answered, so a round the incumbent sat out leaves
	// it unscoreable — and hysteresis has nothing to hold, so the selection moves
	// to whoever did answer. On a destination whose reachability varies per exit
	// that repeats every round: one host was measured cycling through all five
	// exits in twenty minutes, which is the exact split grouping exists to stop.
	previous := &destinationEntry{
		Remote:   map[string]remoteWindow{"jp": {ms(2)}, "hk": {ms(3)}},
		Path:     map[string]time.Duration{"jp": ms(80), "hk": ms(29)},
		Selected: "jp",
	}
	updated := &destinationEntry{
		Remote: map[string]remoteWindow{"hk": {ms(4)}},
		Path:   map[string]time.Duration{"hk": ms(29)},
	}
	updated.carryOver(previous, "jp")

	remote, found := updated.remoteFor("jp")
	require.True(t, found, "the group in use has to stay comparable")
	require.Equal(t, ms(2), remote)
	require.True(t, updated.carried("jp"), "and be marked as inherited rather than measured")
	require.False(t, updated.carried("hk"))

	// One round only. A group that stays silent through a second loses its place,
	// so a stale value cannot live on a destination nobody can reach.
	third := &destinationEntry{Remote: map[string]remoteWindow{"hk": {ms(5)}}}
	third.carryOver(updated, "jp")
	_, found = third.remoteFor("jp")
	require.False(t, found)
}

func TestOnlyTheGroupInUseIsCarried(t *testing.T) {
	t.Parallel()
	// Everything else has to earn its place in the round that wrote the
	// snapshot, or the comparison stops being between measurements taken under
	// the same conditions.
	previous := &destinationEntry{
		Remote:   map[string]remoteWindow{"jp": {ms(2)}, "sg": {ms(9)}, "eu": {ms(40)}},
		Path:     map[string]time.Duration{"jp": ms(80), "sg": ms(63), "eu": ms(160)},
		Selected: "jp",
	}
	updated := &destinationEntry{
		Remote: map[string]remoteWindow{"hk": {ms(4)}},
		Path:   map[string]time.Duration{"hk": ms(29)},
	}
	updated.carryOver(previous, "jp")

	require.Len(t, updated.Remote, 2, "only hk, which answered, and jp, which is in use")
	for _, tag := range []string{"sg", "eu"} {
		_, found := updated.remoteFor(tag)
		require.False(t, found, tag+" did not answer and is not in use")
	}
}

func TestSurplusEntriesForgetsTheLeastRecent(t *testing.T) {
	t.Parallel()
	// Nothing is deleted as decisions are made, so the file would otherwise grow
	// for every destination ever visited and be read back in full at each start.
	now := time.Now()
	stored := map[string]*destinationEntry{
		"newest": {RacedAt: now},
		"middle": {RacedAt: now.Add(-time.Hour)},
		"oldest": {RacedAt: now.Add(-24 * time.Hour)},
	}

	require.Nil(t, surplusEntries(stored, 3), "nothing to drop when it already fits")
	require.Nil(t, surplusEntries(stored, 10))

	dropped := surplusEntries(stored, 2)
	require.Len(t, dropped, 1)
	require.True(t, dropped["oldest"], "the least recently raced goes first")

	dropped = surplusEntries(stored, 1)
	require.Len(t, dropped, 2)
	require.False(t, dropped["newest"], "the most recent decision must survive")
}

func TestSnapshotKeepsBothLegsSoDriftIsVisible(t *testing.T) {
	t.Parallel()
	// Storing only the destination leg leaves the other way a decision goes
	// stale invisible: a node that has become slow. That case is the worse of
	// the two, because the groups a race pruned hold no snapshot entry and are
	// not eligible to replace the incumbent — so the traffic would stay on it
	// until the snapshot expired.
	entry := &destinationEntry{
		Remote: map[string]remoteWindow{"hk": {ms(5)}},
		Path:   map[string]time.Duration{"hk": ms(29)},
	}
	local, remote, ok := entry.legsFor("hk")
	require.True(t, ok)
	require.Equal(t, ms(29), local)
	require.Equal(t, ms(5), remote)

	// A snapshot written before both legs were kept degrades to "unknown"
	// rather than to a wrong comparison.
	old := &destinationEntry{Remote: map[string]remoteWindow{"hk": {ms(5)}}}
	_, _, ok = old.legsFor("hk")
	require.False(t, ok)

	_, _, ok = entry.legsFor("de")
	require.False(t, ok, "a group that never answered has no legs to compare")
}

func TestScoreDriftIsWhatTriggersAReRace(t *testing.T) {
	t.Parallel()
	// Either leg moving must be able to trigger a refresh, since either one can
	// make the stored decision the wrong one.
	const alpha = 0.7
	was := score(alpha, ms(29), ms(5), 0)

	destinationMoved := score(alpha, ms(29), ms(200), 0)
	require.GreaterOrEqual(t, destinationMoved, was*anomalyFactor)

	nodeBecameSlow := score(alpha, ms(400), ms(5), 0)
	require.GreaterOrEqual(t, nodeBecameSlow, was*anomalyFactor,
		"a node that has become slow must be as visible as a destination that moved")

	ordinaryJitter := score(alpha, ms(33), ms(7), 0)
	require.Less(t, ordinaryJitter, was*anomalyFactor, "jitter must not trigger anything")
}

// failoverSmart is a jp group of two members plus an sg group of one, which is
// the shape every case below needs: a group that can lose its node, and one to
// lose the destination to.
func failoverSmart(t *testing.T) (*Smart, *smartMember, *smartMember) {
	t.Helper()
	rfc, ddps := newMember("out-jp-rfc", ms(57)), newMember("out-jp-ddps", ms(78))
	jp := &smartGroup{tag: "jp", selectFastest: true, members: []*smartMember{rfc, ddps}}
	sg := &smartGroup{tag: "sg", members: []*smartMember{newMember("out-sg-la", ms(63))}}
	return &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		alpha:  testAlpha,
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{jp, sg},
	}, rfc, ddps
}

func TestSnapshotDecidedDuringAFailoverIsRedecided(t *testing.T) {
	t.Parallel()
	// The case this exists for: jp's node dies mid-race, jp falls to its backup,
	// the round reaches a verdict about the backup, and jp is back on its usual
	// node five minutes later — with the verdict cached for six hours. Nothing
	// else notices, because a group that did not answer leaves no leg for the
	// drift check to compare against.
	smart, _, _ := failoverSmart(t)
	entry := &destinationEntry{
		Remote:  map[string]remoteWindow{"sg": {ms(2)}},
		Members: map[string]string{"jp": "out-jp-ddps", "sg": "out-sg-la"},
	}
	now := time.Now()
	unchanged := []candidate{{tag: "jp", member: "out-jp-ddps"}, {tag: "sg", member: "out-sg-la"}}
	require.False(t, smart.supersededRace(entry, unchanged, now),
		"still the same nodes, so the answer still applies")

	recovered := []candidate{{tag: "jp", member: "out-jp-rfc"}, {tag: "sg", member: "out-sg-la"}}
	require.True(t, smart.supersededRace(entry, recovered, now),
		"jp is back on the node that never got to answer")
}

func TestALiveOutageDoesNotReRaceEverythingTheGroupTouched(t *testing.T) {
	t.Parallel()
	// The other direction, and the one that would hurt: jp has just lost its
	// node and is running on the backup. Re-racing now would replace good
	// measurements with ones taken during an outage — across every destination
	// jp touches, at the moment the outage is already generating load.
	smart, rfc, _ := failoverSmart(t)
	entry := &destinationEntry{
		Remote:  map[string]remoteWindow{"jp": {ms(1)}},
		Members: map[string]string{"jp": "out-jp-rfc"},
	}
	rfc.healthy.Store(false)
	failedOver := []candidate{{tag: "jp", member: "out-jp-ddps"}}
	require.False(t, smart.supersededRace(entry, failedOver, time.Now()),
		"the node that raced it is down, not superseded — wait for it")

	// And once it is back, the snapshot is about the right node again.
	rfc.healthy.Store(true)
	back := []candidate{{tag: "jp", member: "out-jp-rfc"}}
	require.False(t, smart.supersededRace(entry, back, time.Now()))
}

func TestARefreshForChangedMembersStillWaitsOutTheBackoff(t *testing.T) {
	t.Parallel()
	// A member flapping must not turn every connection to every destination it
	// touched into a round of its own.
	smart, _, _ := failoverSmart(t)
	entry := &destinationEntry{Members: map[string]string{"jp": "out-jp-ddps"}}
	drifted := []candidate{{tag: "jp", member: "out-jp-rfc"}}
	now := time.Now()
	require.True(t, smart.supersededRace(entry, drifted, now))

	entry.noteAttempt(now)
	require.False(t, smart.supersededRace(entry, drifted, now))
	require.True(t, smart.supersededRace(entry, drifted, now.Add(refreshBackoff+time.Second)))
}

func TestASnapshotFromBeforeMembersWereRecordedIsLeftAlone(t *testing.T) {
	t.Parallel()
	// Every entry restored from a cache written by an older build has no members
	// at all. Reading that as "raced against something else" would re-race the
	// whole table on the first dial after an upgrade.
	smart, _, _ := failoverSmart(t)
	entry := &destinationEntry{Remote: map[string]remoteWindow{"jp": {ms(1)}}}
	racing := []candidate{{tag: "jp", member: "out-jp-rfc"}, {tag: "sg", member: "out-sg-la"}}
	require.False(t, smart.supersededRace(entry, racing, time.Now()))
}

func TestASnapshotRecordsWhoWasAskedIncludingTheGroupsThatDidNot(t *testing.T) {
	t.Parallel()
	// The groups that never answered are the ones worth recording: they leave no
	// leg, so who was asked is the only trace that the round happened while one
	// of them was somewhere else.
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		alpha:  testAlpha,
		cache:  newSmartCache(context.Background(), "smart"),
	}
	report := raceReport{
		winner: "sg",
		remote: map[string]time.Duration{"sg": ms(2)},
		candidates: []candidate{
			{tag: "jp", member: "out-jp-ddps"},
			{tag: "sg", member: "out-sg-la"},
		},
	}
	smart.storeSnapshot("3f2a", M.ParseSocksaddr("18-courier2.push.apple.com:5223"), snapshotBasis{}, report, nil, "")

	stored := smart.cache.load("3f2a")
	require.NotNil(t, stored)
	require.Equal(t, "out-jp-ddps", stored.racedBy("jp"), "jp answered nothing, but it was asked")
	require.Equal(t, "out-sg-la", stored.racedBy("sg"))
}

func TestALegBrokenOnTheWayToTheProxyDoesNotRetireTheIncumbent(t *testing.T) {
	t.Parallel()
	// The two failures a round can produce mean opposite things. "I could not
	// reach the destination" is news about this destination and must replace
	// what was known. Everything else — a pooled connection that died, a reset,
	// a refused dial — is about the leg to the proxy and says nothing about the
	// destination at all; retiring the group's leg for one of those costs it its
	// eligibility for the whole snapshot lifetime, because a group with no
	// measurement cannot be selected.
	//
	// Seen on lh3.googleusercontent.com: one "connection closed" during a
	// refresh moved a CDN scoring -8ms onto a group scoring 22ms, and left it
	// there for six hours.
	previous := &destinationEntry{
		Remote: map[string]remoteWindow{"jp": {ms(2)}},
		Path:   map[string]time.Duration{"jp": ms(57)},
	}

	carried := &destinationEntry{Remote: map[string]remoteWindow{"hk": {ms(1)}}}
	carried.carryOver(previous, "jp")
	_, remote, ok := carried.legsFor("jp")
	require.True(t, ok, "the broken leg says nothing about this destination")
	require.Equal(t, ms(2), remote)
	require.True(t, carried.carried("jp"), "and it is marked as inherited, not measured")
}

func TestPreloadDoesNotOverwriteADecisionMadeWhileItRan(t *testing.T) {
	t.Parallel()
	// The listeners accept before preload runs, so a destination can be raced
	// while the bucket is still being read. The live entry is the fresher one —
	// the disk copy predates the very flush that wrote it — and overwriting it
	// resurrects the previous run's decision, then flushes the stale copy back
	// over the fresh row under its still-standing dirty mark.
	cache := onDisk(t)
	key := "3f2a"
	cache.store(key, &destinationEntry{
		Remote:   map[string]remoteWindow{"jp": {ms(120)}},
		Selected: "jp",
		RacedAt:  time.Now().Add(-time.Hour),
	})
	require.NoError(t, cache.flush())

	restarted := newSmartCache(context.Background(), "smart")
	restarted.resolve.Do(func() { restarted.cacheFile = cache.cacheFile })
	// A race lands before preload reaches this key, exactly as it does when a
	// client reconnects into a process whose listeners are already open.
	restarted.store(key, &destinationEntry{
		Remote:   map[string]remoteWindow{"sg": {ms(8)}},
		Selected: "sg",
		RacedAt:  time.Now(),
	})
	require.NoError(t, restarted.flush())

	restarted.preload()

	live := restarted.load(key)
	require.NotNil(t, live)
	require.Equal(t, "sg", live.selected(),
		"the decision made seconds ago outranks the one read off disk")
	require.NoError(t, restarted.flush())

	final := newSmartCache(context.Background(), "smart")
	final.resolve.Do(func() { final.cacheFile = cache.cacheFile })
	final.preload()
	require.Equal(t, "sg", final.load(key).selected(),
		"and the stale copy must not have been flushed back over it")
}

func TestWithoutACacheFileNothingAccumulatesDirtyMarks(t *testing.T) {
	t.Parallel()
	// Nothing ever flushes without one, so a mark is never cleared: the set
	// would grow by a key per destination ever visited, for the life of the
	// process, while the LRU it shadows stays bounded.
	cache := newSmartCache(context.Background(), "smart")
	for i := range 128 {
		key := destinationKey(M.ParseSocksaddr(strconv.Itoa(i) + ".example.com:443"))
		cache.store(key, &destinationEntry{RacedAt: time.Now()})
		cache.touch(key)
	}
	require.NoError(t, cache.flush())

	cache.access.Lock()
	defer cache.access.Unlock()
	require.Empty(t, cache.dirty, "no cache file, no marks to accumulate")
}

func TestAWindowTooShortForAMedianReportsItsNewestSample(t *testing.T) {
	t.Parallel()
	// Two samples have no middle. Reporting the lower of them would be
	// reporting the better one, and holding it while a newer, worse reading
	// sat unused — one lucky round given two rounds of authority, which is the
	// opposite of what a window is for. Below the quorum the newest reading
	// stands on its own, exactly as the single stored sample once did.
	_, ok := remoteWindow(nil).value()
	require.False(t, ok, "an empty window stands for nothing")

	single, ok := remoteWindow{ms(5)}.value()
	require.True(t, ok)
	require.Equal(t, ms(5), single, "one sample is the value, as it always was")

	pair, _ := remoteWindow{ms(5), ms(286)}.value()
	require.Equal(t, ms(286), pair, "the newest reading, not the more flattering one")

	recovered, _ := remoteWindow{ms(286), ms(5)}.value()
	require.Equal(t, ms(5), recovered, "and it follows a recovery just as readily")
}

func TestAMedianNeedsAQuorumBeforeItDecides(t *testing.T) {
	t.Parallel()
	// At three samples there is a middle, and from there the window does what
	// it exists to do: one discordant round is outvoted by the two around it.
	odd, _ := remoteWindow{ms(150), ms(1), ms(30)}.value()
	require.Equal(t, ms(30), odd, "the middle sample, wherever it arrived in the sequence")

	require.Len(t, remoteWindow{ms(1), ms(1)}, medianQuorum-1,
		"the quorum is what separates the two cases above")

	// The lower of the two middles on even counts keeps the value to something
	// a round actually measured rather than an average of two that disagree.
	even, _ := remoteWindow{ms(1), ms(1), ms(280), ms(283)}.value()
	require.Equal(t, ms(1), even, "the lower middle is a reading, not a compromise")
}

func TestOneOutlierSampleDoesNotMoveTheLeg(t *testing.T) {
	t.Parallel()
	// The failure this type was built against: an anycast destination handed
	// one round 286ms on a leg that typically measures under a millisecond,
	// and the single stored sample froze that answer for the whole snapshot
	// lifetime. In a window, one discordant round is outvoted by the history
	// around it — while a genuine change of regime, arriving round after
	// round, takes the median over as soon as it holds the majority.
	window := remoteWindow{ms(1), ms(1), ms(1)}

	outlier := window.extend(ms(286))
	value, _ := outlier.value()
	require.Equal(t, ms(1), value, "one bad round is luck, not news")

	degrading := outlier.extend(ms(280)).extend(ms(283))
	value, _ = degrading.value()
	require.Equal(t, ms(280), value, "three bad rounds of five are the new regime")
}

func TestConfirmedDegradationCountsAtEveryDepth(t *testing.T) {
	t.Parallel()
	// Appending confirmed evidence could not be relied on to change anything.
	// The median sits at index (len-1)/2, so on an odd-length window a larger
	// sample slides in above the middle and leaves it exactly where it was —
	// and one sample is where most legs live, so the evidence was dropped
	// precisely where it was needed. Retiring the best reading held moves the
	// value at every depth, because that reading is what the confirmation
	// disproved.
	held := remoteWindow{ms(10), ms(11), ms(12), ms(13), ms(14)}
	for depth := 1; depth <= remoteWindowDepth; depth++ {
		window := held[:depth]
		before, _ := window.value()
		after, ok := window.retire(ms(300)).value()
		require.True(t, ok)
		require.Greater(t, after, before, "confirmed degradation must count at depth %d", depth)
	}
}

func TestConfirmedDegradationNeverFlattersTheGroup(t *testing.T) {
	t.Parallel()
	// Degradation is confirmed on the score, which carries both legs, so a
	// group whose client-side leg collapsed arrives here with a destination
	// sample smaller than the one on record. Appended, it became the new
	// minimum and made the group look better — the exact opposite of what the
	// confirmation meant.
	window := remoteWindow{ms(50)}
	require.Equal(t, window, window.retire(ms(1)),
		"a sample no worse than the best reading held changes nothing")

	deep := remoteWindow{ms(30), ms(40), ms(50)}
	before, _ := deep.value()
	after, _ := deep.retire(ms(35)).value()
	require.GreaterOrEqual(t, after, before, "the value may only rise, whatever the sample")
}

func TestAWindowKeepsOnlyTheNewestSamples(t *testing.T) {
	t.Parallel()
	window := remoteWindow{}
	for i := 1; i <= remoteWindowDepth+2; i++ {
		window = window.extend(ms(i))
	}
	require.Len(t, window, remoteWindowDepth)
	require.Equal(t, ms(3), window[0], "the oldest samples fall off the far end")
	require.Equal(t, ms(remoteWindowDepth+2), window[len(window)-1])

	// Windows are shared between snapshots by the carry, so extending one must
	// never write into the history another entry is still reading.
	shared := remoteWindow{ms(1), ms(2)}
	_ = shared.extend(ms(3))
	require.Equal(t, remoteWindow{ms(1), ms(2)}, shared, "extend copies; nothing writes in place")
}

func TestOldSingleSampleSnapshotsStillDecode(t *testing.T) {
	t.Parallel()
	// The previous format stored one number per group. Failing to decode it
	// would drop every row on upgrade — re-racing every destination is the
	// restart churn persistence exists to prevent, delivered by an update.
	var decoded destinationEntry
	require.NoError(t, json.Unmarshal(
		[]byte(`{"remote":{"jp":930000000},"selected":"jp","raced_at":"2026-08-05T12:00:00Z"}`),
		&decoded))
	remote, ok := decoded.remoteFor("jp")
	require.True(t, ok)
	require.Equal(t, 930*time.Millisecond, remote, "one stored sample reads as a window of one")

	// And what this version writes reads back as itself.
	decoded.noteDegradation("jp", ms(950))
	encoded, err := decoded.marshal()
	require.NoError(t, err)
	var roundTripped destinationEntry
	require.NoError(t, json.Unmarshal(encoded, &roundTripped))
	require.Equal(t, decoded.windowFor("jp"), roundTripped.windowFor("jp"))
}

func TestAWellUsedSnapshotExpiresEarly(t *testing.T) {
	t.Parallel()
	// The time-based lifetime alone leaves the busiest destinations exposed:
	// the losing legs are only re-measured by a race, so however much traffic
	// a snapshot routes, its alternatives stay exactly as old as the round
	// that wrote them. Measured once: a location service riding one outlier
	// sample for five hours, 789 requests each paying ~155ms over the exit
	// that had recovered. Use is the second hand on the clock.
	destination := "3f2a"
	now := time.Now()
	entry := &destinationEntry{RacedAt: now}
	require.False(t, entry.expired(destination, now), "fresh and unused")

	for range snapshotUseLimit - 1 {
		entry.count("tcp")
	}
	require.False(t, entry.expired(destination, now), "under the limit it is still trusted")

	entry.count("tcp")
	require.True(t, entry.expired(destination, now),
		"a snapshot that has answered its limit is due for re-verification")

	entry.noteAttempt(now)
	require.False(t, entry.expired(destination, now),
		"the refresh backoff still throttles it: expiry queues a refresh, it does not spin one per connection")
}

func TestARaceWritingTwiceLandsOneSampleForTheRound(t *testing.T) {
	t.Parallel()
	// A race writes its snapshot twice — provisional when the caller is
	// answered, final when the stragglers are in. Both writes extend the
	// window of the round *before* the race, never the provisional one:
	// extended from the provisional, the early answerers' samples would count
	// twice in one round, and five such races would fill the window with one
	// round's opinion repeated.
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		alpha:  testAlpha,
		cache:  newSmartCache(context.Background(), "smart"),
	}
	prior := &destinationEntry{
		Remote:  map[string]remoteWindow{"sg": {ms(10), ms(20)}},
		Path:    map[string]time.Duration{"sg": ms(60)},
		RacedAt: time.Now().Add(-time.Hour),
	}
	smart.cache.store("3f2a", prior)
	report := raceReport{
		winner: "sg",
		remote: map[string]time.Duration{"sg": ms(30)},
		candidates: []candidate{
			{tag: "sg", member: "out-sg-la"},
		},
	}

	smart.storeSnapshot("3f2a", M.ParseSocksaddr("example.com:443"),
		snapshotBasis{replacing: prior, carryFrom: prior}, report, nil, "")
	provisional := smart.cache.load("3f2a")
	require.Equal(t, remoteWindow{ms(10), ms(20), ms(30)}, provisional.windowFor("sg"),
		"the provisional write lands the round's sample once")

	smart.storeSnapshot("3f2a", M.ParseSocksaddr("example.com:443"),
		snapshotBasis{replacing: provisional, carryFrom: prior}, report, nil, "")
	final := smart.cache.load("3f2a")
	require.Equal(t, remoteWindow{ms(10), ms(20), ms(30)}, final.windowFor("sg"),
		"the final write recomputes the same extension instead of stacking a second sample")
}

func TestARefusedDestinationBacksOffInsteadOfRetryingForEver(t *testing.T) {
	t.Parallel()
	// Some destinations are never coming back: an exit with no IPv6 route,
	// asked for an IPv6 literal, refuses it every time. On a flat cooldown
	// that is a full round of doomed dials every five minutes for as long as
	// anything keeps asking — seven thousand error lines in one production
	// day, which buried a real outage that happened in the middle of them.
	entry := &destinationEntry{}
	now := time.Now()

	entry.block("us", 443, now, nil)
	require.True(t, entry.blocked("us", 443, nil, now.Add(destinationFailureCooldown-time.Second)))
	require.False(t, entry.blocked("us", 443, nil, now.Add(destinationFailureCooldown+time.Second)),
		"the first refusal costs the base cooldown, as it always did")

	// Each refusal in a row doubles what the next one costs.
	entry.block("us", 443, now, nil)
	require.True(t, entry.blocked("us", 443, nil, now.Add(2*destinationFailureCooldown-time.Second)))
	entry.block("us", 443, now, nil)
	require.True(t, entry.blocked("us", 443, nil, now.Add(4*destinationFailureCooldown-time.Second)))

	// And it stops doubling, so a destination that does recover is not left
	// waiting hours for anyone to look at it again.
	for range 20 {
		entry.block("us", 443, now, nil)
	}
	require.True(t, entry.blocked("us", 443, nil, now.Add(destinationFailureCooldownCap-time.Second)))
	require.False(t, entry.blocked("us", 443, nil, now.Add(destinationFailureCooldownCap+time.Second)),
		"the escalation is capped")
}

func TestOneGroupsRefusalsDoNotFollowAnother(t *testing.T) {
	t.Parallel()
	// The cooldown is scoped to the group that was refused, and so is what it
	// escalates from: a group that has never failed this destination starts at
	// the base however long its neighbour has been failing.
	entry := &destinationEntry{}
	now := time.Now()
	for range 5 {
		entry.block("us", 443, now, nil)
	}
	entry.block("jp", 443, now, nil)

	require.True(t, entry.blocked("us", 443, nil, now.Add(8*destinationFailureCooldown)))
	require.False(t, entry.blocked("jp", 443, nil, now.Add(destinationFailureCooldown+time.Second)),
		"jp was refused once and pays for once")
}

func TestAPortScanThroughOneHostLeavesABoundedCooldownTable(t *testing.T) {
	t.Parallel()
	// A refusal is filed per group and port, and an expired one was removed
	// only by the group reaching that port again — which on a scanned port
	// never happens. A full port scan through the smart outbound filed 100000
	// cooldowns on one host, held for as long as the host stayed in use and
	// copied whole by every refresh of it.
	entry := &destinationEntry{}
	now := time.Now()
	for port := range uint16(1000) {
		entry.block("us", port+1, now, nil)
	}
	entry.access.Lock()
	cooling, escalating := len(entry.unreachable), len(entry.refusals)
	entry.access.Unlock()
	require.Equal(t, cooldownKeysCap, cooling)
	require.Equal(t, cooldownKeysCap, escalating)

	// The bound is on how many ports are remembered, not on how far each one
	// escalates: a port already cooling is refused again as it always was.
	entry.block("us", 1, now, nil)
	require.True(t, entry.blocked("us", 1, nil, now.Add(2*destinationFailureCooldown-time.Second)),
		"a second refusal of a remembered port doubles its cooldown, full or not")

	// Expired cooldowns are what make room, so a scan that has run its course
	// does not keep the next real refusal out.
	later := now.Add(destinationFailureCooldownCap)
	entry.block("us", 443, later, nil)
	require.True(t, entry.blocked("us", 443, nil, later))
	entry.access.Lock()
	cooling = len(entry.unreachable)
	entry.access.Unlock()
	require.Equal(t, 1, cooling, "the expired cooldowns are gone, not only outnumbered")
}

func TestARefreshOnABlockedPortDoesNotDropTheGroupForTheHost(t *testing.T) {
	t.Parallel()
	// The snapshot is per host and a refusal is per port. A refresh that
	// happened to run on 25 dropped every group refusing 25 — the one that
	// answered 502 and the one its cooldown kept out — and 443 moved to
	// another exit for hours.
	smart := &Smart{ctx: context.Background(), logger: logger.NOP(), alpha: testAlpha,
		cache: newSmartCache(context.Background(), "smart")}
	for _, tag := range []string{"a", "b", "c"} {
		smart.groups = append(smart.groups, &smartGroup{tag: tag, members: []*smartMember{newMember(tag+"-1", ms(30))}})
	}
	smtp := M.ParseSocksaddr("mail.example.com:25")
	now := time.Now()
	previous := &destinationEntry{
		Remote:   map[string]remoteWindow{"a": {ms(9)}, "b": {ms(1)}, "c": {ms(2)}},
		Path:     map[string]time.Duration{"a": ms(80), "b": ms(30), "c": ms(40)},
		Selected: "b",
		RacedAt:  now.Add(-time.Hour),
	}
	previous.block("b", 25, now, nil)
	smart.cache.store("3f2a", previous)
	refusal := E.Cause(naive.ErrDestinationUnreachable, "unexpected response status: 502")
	round := func(basis *destinationEntry) *destinationEntry {
		report := raceReport{
			winner:     "a",
			remote:     map[string]time.Duration{"a": ms(9)},
			candidates: []candidate{{tag: "a", member: "a-1"}, {tag: "c", member: "c-1"}},
			failures:   map[string]error{"c": refusal},
		}
		smart.storeSnapshot("3f2a", smtp, snapshotBasis{replacing: basis, carryFrom: basis}, report, nil, basis.selected())
		return smart.cache.load("3f2a")
	}

	first := round(previous)
	for _, tag := range []string{"b", "c"} {
		_, found := first.remoteFor(tag)
		require.True(t, found, tag+" keeps its leg for the other ports")
		require.True(t, first.carried(tag))
	}
	require.True(t, first.blocked("b", 25, nil, now), "the port's cooldown is what keeps it off 25")

	// One round only: a group refusing the host on every port still retires.
	second := round(first)
	for _, tag := range []string{"b", "c"} {
		_, found := second.remoteFor(tag)
		require.False(t, found, tag+" stayed out of two rounds")
	}
}

func TestARefreshAProbeRacedAClosingSessionInKeepsItsLegs(t *testing.T) {
	t.Parallel()
	// A probe that raced its pooled session's idle GOAWAY was refused
	// unprocessed. That is no reading of the group, and dropping its leg left
	// it unselectable here until the snapshot expired.
	smart := &Smart{ctx: context.Background(), logger: logger.NOP(), alpha: testAlpha,
		cache: newSmartCache(context.Background(), "smart")}
	for _, tag := range []string{"a", "b"} {
		smart.groups = append(smart.groups, &smartGroup{tag: tag, members: []*smartMember{newMember(tag+"-1", ms(30))}})
	}
	previous := &destinationEntry{
		Remote:   map[string]remoteWindow{"a": {ms(9)}, "b": {ms(1)}},
		Path:     map[string]time.Duration{"a": ms(80), "b": ms(30)},
		Selected: "a",
		RacedAt:  time.Now().Add(-time.Hour),
	}
	smart.cache.store("3f2a", previous)
	report := raceReport{
		winner:     "a",
		remote:     map[string]time.Duration{"a": ms(9)},
		candidates: []candidate{{tag: "a", member: "a-1"}, {tag: "b", member: "b-1"}},
		failures: map[string]error{"b": fmt.Errorf("%w: %w: http2 server refused stream",
			naive.ErrNextHopUnreachable, naive.ErrStreamRefused)},
	}
	smart.storeSnapshot("3f2a", M.ParseSocksaddr("example.com:443"),
		snapshotBasis{replacing: previous, carryFrom: previous}, report, nil, "a")

	updated := smart.cache.load("3f2a")
	_, found := updated.remoteFor("b")
	require.True(t, found)
	require.True(t, updated.carried("b"))
}

func TestARefreshAProbeThisEndClosedKeepsItsLegs(t *testing.T) {
	t.Parallel()
	// A member drops its sessions on a network change, and a refresh racing
	// at that moment saw its probe aborted from this side. That is no reading
	// of the group, and dropping its leg left it unselectable here for hours.
	smart := &Smart{ctx: context.Background(), logger: logger.NOP(), alpha: testAlpha,
		cache: newSmartCache(context.Background(), "smart")}
	for _, tag := range []string{"a", "b"} {
		smart.groups = append(smart.groups, &smartGroup{tag: tag, members: []*smartMember{newMember(tag+"-1", ms(30))}})
	}
	previous := &destinationEntry{
		Remote:   map[string]remoteWindow{"a": {ms(9)}, "b": {ms(1)}},
		Path:     map[string]time.Duration{"a": ms(80), "b": ms(30)},
		Selected: "a",
		RacedAt:  time.Now().Add(-time.Hour),
	}
	smart.cache.store("3f2a", previous)
	report := raceReport{
		winner:     "a",
		remote:     map[string]time.Duration{"a": ms(9)},
		candidates: []candidate{{tag: "a", member: "a-1"}, {tag: "b", member: "b-1"}},
		failures:   map[string]error{"b": fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, naive.ErrClosedLocally)},
	}
	smart.storeSnapshot("3f2a", M.ParseSocksaddr("example.com:443"),
		snapshotBasis{replacing: previous, carryFrom: previous}, report, nil, "a")

	updated := smart.cache.load("3f2a")
	_, found := updated.remoteFor("b")
	require.True(t, found)
	require.True(t, updated.carried("b"))
}

func TestAnAnswerOnOnePortDoesNotClearAnotherPortsRefusals(t *testing.T) {
	t.Parallel()
	// Reaching the host on 443 says nothing about 25. Clearing 25's run there
	// reset its doubling on every refresh of a busy host, and each reset cost
	// a round of dials against a port the exit blocks.
	smart := &Smart{ctx: context.Background(), logger: logger.NOP(), alpha: testAlpha,
		cache: newSmartCache(context.Background(), "smart")}
	now := time.Now()
	previous := &destinationEntry{RacedAt: now.Add(-time.Hour)}
	previous.block("us", 25, now, nil)
	previous.block("us", 25, now, nil)
	smart.cache.store("3f2a", previous)
	report := raceReport{
		winner:     "us",
		remote:     map[string]time.Duration{"us": ms(20)},
		candidates: []candidate{{tag: "us", member: "out-us"}},
	}
	smart.storeSnapshot("3f2a", M.ParseSocksaddr("mail.example.com:443"),
		snapshotBasis{replacing: previous, carryFrom: previous}, report, nil, "")

	updated := smart.cache.load("3f2a")
	updated.block("us", 25, now, nil)
	require.True(t, updated.blocked("us", 25, nil, now.Add(4*destinationFailureCooldown-time.Second)),
		"the third refusal on 25 still costs four times the base")
}

func TestReachingADestinationClearsWhatItsRefusalsEarned(t *testing.T) {
	t.Parallel()
	// A destination that comes back must not serve out the escalation its
	// outage bought. Answering a race with a destination leg is the proof, and
	// it lifts both the cooldown and the run behind it.
	smart := &Smart{
		ctx:    context.Background(),
		logger: logger.NOP(),
		alpha:  testAlpha,
		cache:  newSmartCache(context.Background(), "smart"),
	}
	now := time.Now()
	previous := &destinationEntry{RacedAt: now.Add(-time.Hour)}
	for range 4 {
		previous.block("us", 443, now, nil)
	}
	previous.block("jp", 443, now, nil)
	smart.cache.store("3f2a", previous)
	require.True(t, previous.blocked("us", 443, nil, now.Add(4*destinationFailureCooldown)))

	// us answers the next round; jp stays silent.
	report := raceReport{
		winner:     "us",
		remote:     map[string]time.Duration{"us": ms(20)},
		candidates: []candidate{{tag: "us", member: "out-us"}, {tag: "jp", member: "out-jp"}},
	}
	smart.storeSnapshot("3f2a", M.ParseSocksaddr("example.com:443"),
		snapshotBasis{replacing: previous, carryFrom: previous}, report, nil, "")

	updated := smart.cache.load("3f2a")
	require.False(t, updated.blocked("us", 443, nil, now), "the group that answered is off cooldown")
	require.True(t, updated.blocked("jp", 443, nil, now), "the one that did not still owes its own")

	// And the run is gone, not merely the timer: the next refusal starts over.
	updated.block("us", 443, now, nil)
	require.False(t, updated.blocked("us", 443, nil, now.Add(destinationFailureCooldown+time.Second)),
		"a group that recovered pays the base again, not what it owed before")
}

func sizedOn(t *testing.T, db *bbolt.DB, size int) *smartCache {
	t.Helper()
	cache := newSmartCacheSized(context.Background(), "smart", size)
	cache.resolve.Do(func() { cache.cacheFile = &cachefile.CacheFile{DB: db} })
	return cache
}

func rowCount(t *testing.T, cache *smartCache) int {
	t.Helper()
	var rows int
	require.NoError(t, cache.db().View(func(tx *bbolt.Tx) error {
		if bucket := cache.ownBucket(tx); bucket != nil {
			rows = bucket.Stats().KeyN
		}
		return nil
	}))
	return rows
}

func TestARowPushedOutOfTheCacheLeavesTheDisk(t *testing.T) {
	t.Parallel()
	// The rows used to outlive their entries for the rest of the run, and the
	// next start read every one of them back only to keep the newest.
	db, err := bbolt.Open(filepath.Join(t.TempDir(), "cache.db"), 0o600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	cache := sizedOn(t, db, 8)
	for i := range 20 {
		cache.store(fmt.Sprint("dest", i), &destinationEntry{Selected: "a", RacedAt: time.Now()})
		if i%3 == 0 {
			require.NoError(t, cache.flush())
		}
	}
	require.NoError(t, cache.flush())
	require.Equal(t, cache.entries.Len(), rowCount(t, cache))

	// And one pushed out but decided again before the flush keeps its row.
	cache.store("dest0", &destinationEntry{Selected: "b", RacedAt: time.Now()})
	require.NoError(t, cache.flush())
	restarted := sizedOn(t, db, 8)
	restarted.preload()
	require.NotNil(t, restarted.load("dest0"))
}

func TestPreloadDoesNotPushOutADecisionMadeBeforeIt(t *testing.T) {
	t.Parallel()
	// A destination raced while the bucket was being read is not on disk yet
	// and is the oldest entry by the time preload adds to the cache. With the
	// disk full it was the first thing pushed out: the decision was lost, and
	// last run's row for it came back on the next start.
	db, err := bbolt.Open(filepath.Join(t.TempDir(), "cache.db"), 0o600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	previous := sizedOn(t, db, 8)
	for i := range 8 {
		previous.store(fmt.Sprint("old", i), &destinationEntry{Selected: "a", RacedAt: time.Now().Add(-time.Hour)})
	}
	require.NoError(t, previous.flush())

	restarted := sizedOn(t, db, 8)
	restarted.store("fresh", &destinationEntry{Selected: "sg", RacedAt: time.Now()})
	restarted.preload()
	require.NotNil(t, restarted.load("fresh"))
	require.NoError(t, restarted.flush())

	final := sizedOn(t, db, 8)
	final.preload()
	require.NotNil(t, final.load("fresh"))
	require.Equal(t, final.entries.Len(), rowCount(t, final))
}

func TestAFailedFlushStillDeletesWhatWasPushedOutLater(t *testing.T) {
	t.Parallel()
	db, err := bbolt.Open(filepath.Join(t.TempDir(), "cache.db"), 0o600, nil)
	require.NoError(t, err)
	cache := sizedOn(t, db, 8)
	for i := range 12 {
		cache.store(fmt.Sprint("dest", i), &destinationEntry{Selected: "a", RacedAt: time.Now()})
	}
	require.NoError(t, db.Close())
	require.Error(t, cache.flush())
	cache.access.Lock()
	defer cache.access.Unlock()
	require.Len(t, cache.evicted, 4, "kept for the next flush, as the dirty marks are")
}

func TestTheCacheFollowsTheFileThroughARecovery(t *testing.T) {
	t.Parallel()
	// The cache file resets a corrupt database and swaps the handle under its
	// own lock. Smart read the handle without it — a race with the swap — and
	// ran its transactions outside the recovery, so a corrupt file it touched
	// first took the process down instead of being reset. The race is what
	// fails without the fix, so this needs -race to mean anything.
	path := filepath.Join(t.TempDir(), "cache.db")
	ctx := context.Background()
	scope := adapter.NewScope(ctx, logger.NOP())
	file := cachefile.New(ctx, logger.NOP(), option.CacheFileOptions{Path: path})
	require.NoError(t, file.Start(adapter.StartStateInitialize, scope))
	t.Cleanup(func() { scope.Close() })
	cache := newSmartCache(ctx, "out-main")
	cache.resolve.Do(func() { cache.cacheFile = file })

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 50 {
			cache.store(fmt.Sprint("dest", i), &destinationEntry{Selected: "jp", RacedAt: time.Now()})
			_ = cache.flush()
		}
	}()
	// What a transaction on a corrupt file looks like from here: a panic,
	// which the cache file turns into a reset.
	require.Error(t, file.View(func(*bbolt.Tx) error { panic("simulated corruption") }))
	<-done

	cache.store("after", &destinationEntry{Selected: "sg", RacedAt: time.Now()})
	require.NoError(t, cache.flush())
	restarted := newSmartCache(ctx, "out-main")
	restarted.resolve.Do(func() { restarted.cacheFile = file })
	restarted.preload()
	require.NotNil(t, restarted.load("after"), "written into the database the reset opened")
}

// A race wrote its result to disk before handing the caller its connection:
// 11-20ms on every new destination, for a write the periodic flush makes a
// moment later anyway.
func TestARaceHandsOverItsConnectionBeforeWritingToDisk(t *testing.T) {
	t.Parallel()
	cache := onDisk(t)
	smart := newWarmFixtureOn(t, cache, map[string]*resolvingOutbound{"a": warmSibling(), "b": warmSibling()})
	// An address, so there is no lookup for an amendment to flush.
	destination := M.ParseSocksaddr("203.0.113.7:443")
	key := destinationKey(destination)
	stored := func() []byte {
		var row []byte
		require.NoError(t, cache.db().View(func(tx *bbolt.Tx) error {
			if top := tx.Bucket(smartBucket); top != nil {
				if owner := top.Bucket([]byte("smart")); owner != nil {
					row = owner.Get([]byte(key))
				}
			}
			return nil
		}))
		return row
	}

	_, _, conn, err := smart.race(context.Background(), key, destination, nil)
	require.NoError(t, err)
	if conn != nil {
		require.NoError(t, conn.Close())
	}
	require.NotNil(t, cache.load(key))
	require.Nil(t, stored(), "nothing was written before the caller had its connection")
	require.NoError(t, cache.flush())
	require.NotNil(t, stored(), "the next flush writes it")
}

func TestACooldownSpeaksOnlyForTheMemberThatEarnedIt(t *testing.T) {
	t.Parallel()
	// A refusal is only as good as the exit that gave it. One that lost its
	// own egress refused everything, and each destination it refused stayed
	// off the group for five minutes after it came back — or after the group
	// moved to a sibling that never refused anything.
	now := time.Now()
	main := newMember("us-1", ms(30))
	spare := newMember("us-2", ms(40))
	entry := &destinationEntry{}
	entry.block("us", 443, now, main)
	require.True(t, entry.blocked("us", 443, main, now))
	require.False(t, entry.blocked("us", 443, spare, now), "the group moved to a member that never refused it")

	main.downAt.Store(now.Add(time.Second).UnixNano())
	require.False(t, entry.blocked("us", 443, main, now.Add(2*time.Second)),
		"condemned since, so what it refused then is explained by what condemned it")

	later := now.Add(3 * time.Second)
	entry.block("us", 443, later, main)
	require.True(t, entry.blocked("us", 443, main, later), "a refusal after it came back is filed afresh")
	require.True(t, entry.blocked("us", 443, main, later.Add(2*destinationFailureCooldown-time.Second)),
		"and still escalates: the run of refusals is untouched")
}
