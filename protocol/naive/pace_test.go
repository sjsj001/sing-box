package naive

import (
	"io"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// At 10Mbps, 35ms min RTT and a 40ms target the limit is
// 1.25MB/s × 75ms = 93750 bytes.
const (
	testRate   = 1250000
	testMinRTT = 35 * time.Millisecond
	testTarget = 40 * time.Millisecond
	testRecord = 16413
)

func testInfo(rtt time.Duration, queued uint64) paceInfo {
	return paceInfo{rtt: rtt, minRTT: testMinRTT, rate: testRate, queued: queued}
}

func TestPaceDecideOpensWithoutQueue(t *testing.T) {
	for name, info := range map[string]paceInfo{
		"queueing at the target": testInfo(testMinRTT+testTarget, 1<<20),
		"no rate":                {rtt: 200 * time.Millisecond, minRTT: testMinRTT, queued: 1 << 20},
	} {
		sleep, clear := paceDecide(info, testMinRTT, testTarget, testRecord)
		require.Zero(t, sleep, name)
		require.True(t, clear, name)
	}
	sleep, clear := paceDecide(testInfo(200*time.Millisecond, 1<<20), 0, testTarget, testRecord)
	require.Zero(t, sleep, "no baseline")
	require.True(t, clear)
}

func TestPaceDecideHoldsOwnQueue(t *testing.T) {
	sleep, clear := paceDecide(testInfo(135*time.Millisecond, 200000), testMinRTT, testTarget, testRecord)
	require.Equal(t, 10*time.Millisecond, sleep)
	require.False(t, clear)
	sleep, _ = paceDecide(testInfo(78*time.Millisecond, 200000), testMinRTT, testTarget, testRecord)
	require.Equal(t, 9750*time.Microsecond, sleep, "srtt/8")
	// About 93750 - testRecord is the most that may be queued for the record
	// to go; the limit is computed in floating point.
	sleep, clear = paceDecide(testInfo(135*time.Millisecond, 93750-testRecord-100), testMinRTT, testTarget, testRecord)
	require.Zero(t, sleep)
	require.False(t, clear, "a queue of ours is read at every write")
	sleep, _ = paceDecide(testInfo(135*time.Millisecond, 93750-testRecord+100), testMinRTT, testTarget, testRecord)
	require.NotZero(t, sleep)

	sleep, _ = paceDecide(paceInfo{rtt: 4 * time.Millisecond, rate: testRate, queued: 1 << 20}, time.Millisecond, time.Millisecond, testRecord)
	require.Equal(t, time.Millisecond, sleep, "1ms at least")
}

func TestPaceDecideFloor(t *testing.T) {
	info := paceInfo{rtt: 200 * time.Millisecond, rate: 1000, queued: paceFloor - testRecord}
	sleep, _ := paceDecide(info, testMinRTT, testTarget, testRecord)
	require.Zero(t, sleep)
	info.queued++
	sleep, _ = paceDecide(info, testMinRTT, testTarget, testRecord)
	require.NotZero(t, sleep)
}

func testPath(members ...*pacedConn) *pacePath {
	path := &pacePath{members: make(map[*pacedConn]paceReport)}
	for _, member := range members {
		path.members[member] = paceReport{}
		member.path = path
	}
	return path
}

func TestALateConnectionTakesItsSiblingsMinRTT(t *testing.T) {
	early, late := &pacedConn{}, &pacedConn{}
	path := testPath(early, late)
	now := time.Now()
	require.Equal(t, testMinRTT, path.report(early, paceInfo{rtt: 40 * time.Millisecond, minRTT: testMinRTT}, now))
	// Opened behind the queue the early one built: its own min RTT is the
	// queue's.
	require.Equal(t, testMinRTT, path.report(late, paceInfo{rtt: 250 * time.Millisecond, minRTT: 204 * time.Millisecond}, now))

	later := now.Add(pathReportFresh)
	require.Equal(t, 204*time.Millisecond, path.report(late, paceInfo{rtt: 250 * time.Millisecond, minRTT: 204 * time.Millisecond}, later),
		"a report older than 10s no longer counts")
	require.Equal(t, testMinRTT, path.report(early, paceInfo{rtt: 40 * time.Millisecond, minRTT: testMinRTT}, later))
	path.leave(early, later)
	require.Equal(t, 204*time.Millisecond, path.report(late, paceInfo{rtt: 250 * time.Millisecond, minRTT: 204 * time.Millisecond}, later),
		"nor does one that left")
}

func TestAPathIgnoresMinRTTsThatAreNoSample(t *testing.T) {
	member := &pacedConn{}
	path := testPath(member)
	now := time.Now()
	require.Zero(t, path.report(member, paceInfo{rtt: 40 * time.Millisecond}, now))
	require.Zero(t, path.report(member, paceInfo{minRTT: time.Duration(^uint32(0)) * time.Microsecond}, now), "~0 before the first sample")
}

func TestALeftConnectionStaysOff(t *testing.T) {
	member := &pacedConn{}
	path := testPath(member)
	now := time.Now()
	path.leave(member, now)
	require.Zero(t, path.report(member, paceInfo{rtt: 40 * time.Millisecond, minRTT: testMinRTT}, now))
	require.Empty(t, path.members)
	require.Equal(t, now, path.emptyAt)
}

func TestPathTableSharesAndBounds(t *testing.T) {
	table := &pacePaths{paths: make(map[netip.Addr]*pacePath)}
	now := time.Now()
	peer := netip.MustParseAddr("192.0.2.1")
	first, second := &pacedConn{}, &pacedConn{}
	path := table.join(peer, first, now)
	require.Same(t, path, table.join(peer, second, now), "one path per peer")
	require.Len(t, path.members, 2)

	for i := 1; len(table.paths) < pathTableSize; i++ {
		other := table.join(netip.AddrFrom4([4]byte{198, 51, byte(i >> 8), byte(i)}), &pacedConn{}, now)
		require.NotNil(t, other)
	}
	extra := netip.MustParseAddr("203.0.113.1")
	require.Nil(t, table.join(extra, &pacedConn{}, now), "full, and nothing empty for 5 minutes")

	path.leave(first, now)
	path.leave(second, now)
	require.Nil(t, table.join(extra, &pacedConn{}, now.Add(pathLinger)))
	require.NotNil(t, table.join(extra, &pacedConn{}, now.Add(pathLinger+time.Second)), "an empty path makes room 5 minutes on")
	require.NotContains(t, table.paths, peer)
	require.Len(t, table.paths, pathTableSize)
}

// heldConn is a pacedConn whose gate never opens of itself: a queue of 1MB
// at 200ms smoothed RTT on a 35ms path. closed fails its TCP_INFO, as a
// closed socket's.
func heldConn(t *testing.T) (conn *pacedConn, reads, closed *atomic.Int32) {
	client, server := net.Pipe()
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	go io.Copy(io.Discard, server)
	reads, closed = new(atomic.Int32), new(atomic.Int32)
	conn = &pacedConn{Conn: client, target: testTarget}
	testPath(conn)
	conn.info = func() (paceInfo, error) {
		reads.Add(1)
		if closed.Load() != 0 {
			return paceInfo{}, net.ErrClosed
		}
		return testInfo(200*time.Millisecond, 1<<20), nil
	}
	return conn, reads, closed
}

func TestPacedConnSmallWriteBypasses(t *testing.T) {
	conn, reads, _ := heldConn(t)
	_, err := conn.Write(make([]byte, paceSmallWrite-1))
	require.NoError(t, err)
	require.Zero(t, reads.Load())
}

func TestPacedConnReadsOncePerMillisecond(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go io.Copy(io.Discard, server)
	var reads atomic.Int32
	conn := &pacedConn{Conn: client, target: testTarget}
	testPath(conn)
	conn.info = func() (paceInfo, error) {
		reads.Add(1)
		return testInfo(testMinRTT, 0), nil
	}
	start := time.Now()
	for range 3 {
		_, err := conn.Write(make([]byte, testRecord))
		require.NoError(t, err)
	}
	if time.Since(start) < paceFresh {
		require.Equal(t, int32(1), reads.Load())
	}
}

func TestPacedConnHonoursDeadline(t *testing.T) {
	conn, _, _ := heldConn(t)
	// The gate lets the write go once its next 10ms sleep would reach the
	// deadline, so with one deadline the write would meet it 0–10ms early,
	// less a late timer. So the gate's deadline is set 100ms before the pipe's:
	// a gate that ignored it would hold the write for paceMaxWait and fail it
	// on the pipe's, and a late timer on a busy machine has 100ms.
	start := time.Now()
	require.NoError(t, conn.SetWriteDeadline(start.Add(200*time.Millisecond)))
	conn.setWriteDeadline(start.Add(100 * time.Millisecond))
	_, err := conn.Write(make([]byte, testRecord))
	elapsed := time.Since(start)
	require.NoError(t, err, "the write goes before the deadline")
	require.GreaterOrEqual(t, elapsed, 80*time.Millisecond, "held until then")
	require.Less(t, elapsed, time.Second)
}

func TestPacedConnOpensOnClose(t *testing.T) {
	conn, _, closed := heldConn(t)
	go func() {
		time.Sleep(50 * time.Millisecond)
		conn.Close()
		closed.Store(1)
	}()
	start := time.Now()
	_, err := conn.Write(make([]byte, testRecord))
	require.ErrorIs(t, err, io.ErrClosedPipe)
	require.Less(t, time.Since(start), time.Second)
	require.Empty(t, conn.path.members, "Close leaves the path")
}

func TestPacedConnWaitIsBounded(t *testing.T) {
	saved := paceMaxWait
	paceMaxWait = 200 * time.Millisecond
	t.Cleanup(func() { paceMaxWait = saved })
	conn, _, _ := heldConn(t)
	start := time.Now()
	_, err := conn.Write(make([]byte, testRecord))
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Greater(t, elapsed, paceMaxWait-50*time.Millisecond)
	require.Less(t, elapsed, 2*paceMaxWait)
}
