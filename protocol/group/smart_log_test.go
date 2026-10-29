package group

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/protocol/naive"
	F "github.com/sagernet/sing/common/format"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

// recordingLogger keeps what an operator running at Info sees, which is the
// level these lines are written for. The rest goes to the no-op.
type recordingLogger struct {
	logger.ContextLogger
	access sync.Mutex
	lines  []string
}

func newRecordingLogger() *recordingLogger {
	return &recordingLogger{ContextLogger: logger.NOP()}
}

func (l *recordingLogger) add(args []any) {
	l.access.Lock()
	defer l.access.Unlock()
	l.lines = append(l.lines, F.ToString(args...))
}

func (l *recordingLogger) Info(args ...any)                           { l.add(args) }
func (l *recordingLogger) Warn(args ...any)                           { l.add(args) }
func (l *recordingLogger) InfoContext(_ context.Context, args ...any) { l.add(args) }
func (l *recordingLogger) WarnContext(_ context.Context, args ...any) { l.add(args) }

func (l *recordingLogger) matching(fragment string) []string {
	l.access.Lock()
	defer l.access.Unlock()
	var matched []string
	for _, line := range l.lines {
		if strings.Contains(line, fragment) {
			matched = append(matched, line)
		}
	}
	return matched
}

func TestEveryWayBackFromCondemnedIsLoggedOnce(t *testing.T) {
	t.Parallel()
	// Only the heartbeat used to say a member was back. Traffic, a race and a
	// retake let it back in silently, so after an outage the journal held a
	// run of "is unreachable" and almost nothing answering it — members that
	// had been carrying traffic for hours read as never having returned.
	lines := newRecordingLogger()
	s := &Smart{ctx: context.Background(), logger: lines}
	member := newMember("out-hk", ms(30))

	s.markDown(member, errors.New("connection refused"))
	s.record(member, answering(ms(30), ms(1)))
	s.record(member, answering(ms(30), ms(1)))
	require.Len(t, lines.matching("member out-hk is reachable again"), 1,
		"traffic brings it back and says so, once; a member already up says nothing")

	s.markDown(member, errors.New("connection refused"))
	s.recordProbe(member, M.ParseSocksaddr("1.1.1.1:443"), answering(ms(30), ms(1)))
	require.Len(t, lines.matching("member out-hk is reachable again"), 2)
	require.Len(t, lines.matching("member out-hk is unreachable"), 2, "and every line is paired")

	// The heartbeat goes through the same door, so it no longer says it twice.
	s.markDown(member, errors.New("connection refused"))
	out := &strandedOutbound{dialed: make(chan M.Socksaddr, 1)}
	member.outbound = out
	require.NoError(t, s.probeOnce(context.Background(), member, naive.ProbeDestination(), 0))
	require.Len(t, lines.matching("member out-hk is reachable again"), 3)
}

func TestABlackholeIsCondemnedInTheSameWordsAsAnyOtherFailure(t *testing.T) {
	t.Parallel()
	// Two wordings for one verdict, and the checklist counted only one of them.
	lines := newRecordingLogger()
	member := newMember("out-hk", ms(30))
	member.probing.Store(true) // the confirming probe is not what this is about
	group := &smartGroup{tag: "hk", members: []*smartMember{member}}
	s := &Smart{
		ctx: context.Background(), logger: lines,
		cache: newSmartCache(context.Background(), "smart"), groups: []*smartGroup{group},
	}

	s.reportFailure("3f2a", M.ParseSocksaddr("example.com:443"), group, member,
		fmt.Errorf("%w: %w", naive.ErrNextHopUnreachable, errLegTimedOut))
	require.False(t, member.healthy.Load())
	require.Len(t, lines.matching("member out-hk is unreachable: "), 1)
	require.Empty(t, lines.matching("looks unreachable"))
}

func TestAFailoverSaysWhereItCameFromAndWhy(t *testing.T) {
	t.Parallel()
	// "failed over X to jp" and nothing else, at Info: a run of them read as
	// one smooth failover each, while every group in turn was refusing. The
	// group left and what it said are the half of the line that matters.
	refused := fmt.Errorf("%w: %w: unexpected response status: 502",
		naive.ErrDestinationUnreachable, naive.ErrProxyAnswered)
	hk := newMember("out-hk", ms(31))
	hk.outbound = &failingOutbound{err: refused}
	jp := newMember("out-jp", ms(59))
	jp.outbound = &failingOutbound{err: refused}
	sg := newMember("out-sg", ms(90))
	sg.outbound = &answeringOutbound{measurement: answering(ms(90), ms(1))}
	lines := newRecordingLogger()
	s := &Smart{
		ctx: context.Background(), logger: lines, alpha: testAlpha,
		cache: newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{
			{tag: "hk", members: []*smartMember{hk}},
			{tag: "jp", members: []*smartMember{jp}},
			{tag: "sg", members: []*smartMember{sg}},
		},
	}
	destination := M.ParseSocksaddr("example.com:443")
	s.cache.store(destinationKey(destination), &destinationEntry{
		Remote:   map[string]remoteWindow{"hk": {ms(1)}, "jp": {ms(1)}, "sg": {ms(1)}},
		Path:     map[string]time.Duration{"hk": ms(31), "jp": ms(59), "sg": ms(90)},
		Selected: "hk",
		RacedAt:  time.Now(),
	})

	conn, err := s.dialTCP(context.Background(), destination)
	require.NoError(t, err)
	defer conn.Close()
	require.Empty(t, lines.matching("failed over"), "opening a tunnel does not confirm the target answered")
	require.NoError(t, conn.(*replayConn).settle())
	// hk refused, jp was tried next and refused too: the line names the last
	// group left, not the first, and carries its answer.
	require.Equal(t, []string{"failed over example.com:443 from jp to sg by=failover: " + refused.Error()},
		lines.matching("failed over"))
}

func TestAFailoverThatRanOutOfGroupsNamesThemAll(t *testing.T) {
	t.Parallel()
	// The last group's refusal is what the caller gets, and on its own it reads
	// as that group alone having failed. Still the same verdict underneath.
	conn, _ := brokenTunnelFixture(t, &brokenTunnelConn{})
	spare := conn.smart.groups[1].members[0]
	spare.outbound = &gatedOutbound{conn: &brokenTunnelConn{}}

	_, err := conn.Read(make([]byte, 1))
	require.ErrorIs(t, err, naive.ErrNextHopUnreachable)
	require.True(t, strings.HasPrefix(err.Error(), "tried a,b: "), err.Error())
}

// markSeeingOutbound notes, per dial, whether the smart outbound marked it as
// its own measuring — what the naive outbound reads to keep it out of Info.
type markSeeingOutbound struct {
	*answeringOutbound
	seen  sync.Mutex
	marks []bool
}

func (o *markSeeingOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	o.seen.Lock()
	o.marks = append(o.marks, naive.IsProbeContext(ctx))
	o.seen.Unlock()
	return o.answeringOutbound.DialContext(ctx, network, destination)
}

func (o *markSeeingOutbound) taken() []bool {
	o.seen.Lock()
	defer o.seen.Unlock()
	marks := o.marks
	o.marks = nil
	return marks
}

func TestOnlyACallersOwnDialIsLoggedAsTraffic(t *testing.T) {
	t.Parallel()
	// Every heartbeat, confirming probe, race probe and retake went through
	// the member's DialContext like traffic, and was logged at Info as
	// "outbound connection to": 1.1.1.1 and 9.9.9.9 read as someone visiting
	// them, and a five-group race as one connection dialed five times.
	out := &markSeeingOutbound{answeringOutbound: &answeringOutbound{measurement: answering(ms(30), ms(1))}}
	member := newMember("out-hk", ms(30))
	member.outbound = out
	s := &Smart{
		ctx: context.Background(), logger: logger.NOP(), alpha: testAlpha,
		cache:  newSmartCache(context.Background(), "smart"),
		groups: []*smartGroup{{tag: "hk", members: []*smartMember{member}}},
	}
	destination := M.ParseSocksaddr("example.com:443")

	require.NoError(t, s.probeOnce(context.Background(), member, naive.ProbeDestination(), 0))
	require.NoError(t, s.probeOnce(context.Background(), member, M.ParseSocksaddr("1.1.1.1:443"), 0))
	_, warmed := s.warmLeg(member, destination)
	require.True(t, warmed)
	require.Equal(t, []bool{true, true, true}, out.taken(), "heartbeat, confirmation and retake")

	conn, err := s.dialTCP(context.Background(), destination)
	require.NoError(t, err)
	_, _ = conn.Read(make([]byte, 1)) // the race runs behind the connection; a read waits it out
	conn.Close()
	require.Equal(t, []bool{true}, out.taken(), "no snapshot yet, so the race probe")

	conn, err = s.dialTCP(context.Background(), destination)
	require.NoError(t, err)
	conn.Close()
	require.Equal(t, []bool{false}, out.taken(), "decided, so dialed for the caller: logged as before")
}
