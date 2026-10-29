package group

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/protocol/naive"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/stretchr/testify/require"
)

type rebootOutbound struct {
	adapter.Outbound
	up       atomic.Bool
	dials    atomic.Int32
	early    bool
	gate     <-chan struct{}
	controls bool
	probes   atomic.Int32
}

func (o *rebootOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *rebootOutbound) DialContext(ctx context.Context, _ string, destination M.Socksaddr) (net.Conn, error) {
	if o.controls && destination.Port != 22 {
		o.probes.Add(1)
		return &answeringConn{measurement: answering(ms(30), ms(1))}, nil
	}
	o.dials.Add(1)
	if o.gate != nil {
		select {
		case <-o.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if o.early {
		return &rebootConn{answeringConn: answeringConn{measurement: answering(ms(30), ms(1))}, up: o.up.Load()}, nil
	}
	if !o.up.Load() {
		return nil, fmt.Errorf("%w: %w: unexpected response status: 502", naive.ErrDestinationUnreachable, naive.ErrProxyAnswered)
	}
	return &answeringConn{measurement: answering(ms(30), ms(1))}, nil
}

func TestDestinationRecoveryChecksEgressWithoutCondemningHealthyExit(t *testing.T) {
	t.Parallel()
	f := newRaceFixture(t, 0, map[string][2]time.Duration{"a": {ms(30), ms(1)}})
	o := &rebootOutbound{controls: true}
	group := f.smart.groups[0]
	member := group.current()
	member.outbound = o
	destination := M.ParseSocksaddr("reboot.example:22")
	entry := &destinationEntry{Selected: "a"}
	entry.block("a", 22, time.Now().Add(-2*time.Second), member)
	f.smart.cache.store(destinationKey(destination), entry)
	_, err := f.smart.dialTCP(context.Background(), destination)
	require.ErrorIs(t, err, naive.ErrDestinationUnreachable)
	require.Eventually(t, func() bool { return o.probes.Load() > 0 && !member.probing.Load() }, time.Second, time.Millisecond)
	require.True(t, member.healthy.Load(), "a reachable control address attributes this failure to the target")
	require.EqualValues(t, 1, o.dials.Load(), "no migration while the target stays down")
	_, err = f.smart.dialTCP(context.Background(), destination)
	require.ErrorIs(t, err, naive.ErrDestinationUnreachable)
	require.EqualValues(t, 1, o.dials.Load(), "traffic cannot bypass recovery backoff")
}

func TestDestinationRecoveryCancellationDoesNotCheckEgress(t *testing.T) {
	t.Parallel()
	f := newRaceFixture(t, 0, map[string][2]time.Duration{"a": {ms(30), ms(1)}})
	member := f.smart.groups[0].current()
	o := &rebootOutbound{controls: true, gate: make(chan struct{})}
	member.outbound = o
	destination := M.ParseSocksaddr("reboot.example:22")
	entry := &destinationEntry{Selected: "a"}
	entry.block("a", 22, time.Now().Add(-2*time.Second), member)
	f.smart.cache.store(destinationKey(destination), entry)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := f.smart.dialTCP(ctx, destination)
		done <- err
	}()
	require.Eventually(t, func() bool { return o.dials.Load() == 1 }, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Zero(t, member.refusalProbedAt.Load(), "a cancelled caller supplies no evidence about egress")
	require.Empty(t, f.smart.slots())
	require.True(t, member.healthy.Load())
}

func TestDestinationRecoveryDetectsExitEgressFailure(t *testing.T) {
	t.Parallel()
	s, _ := unreachableSmart("a")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.ctx = ctx
	member := s.groups[0].current()
	destination := M.ParseSocksaddr("reboot.example:22")
	entry := &destinationEntry{Selected: "a"}
	entry.block("a", 22, time.Now().Add(-2*time.Second), member)
	s.cache.store(destinationKey(destination), entry)
	_, err := s.dialTCP(ctx, destination)
	require.ErrorIs(t, err, naive.ErrDestinationUnreachable)
	require.Eventually(t, func() bool { return !member.healthy.Load() && !member.probing.Load() }, time.Second, time.Millisecond,
		"refusing both independent controls attributes the failure to the exit")
	require.False(t, entry.blocked("a", 22, member, time.Now()), "the target refusal is invalidated when its exit is condemned")
}

type rebootConn struct {
	answeringConn
	up bool
}

func (c *rebootConn) Measure(ctx context.Context) (naive.ConnMeasurement, error) {
	if !c.up {
		return naive.ConnMeasurement{}, fmt.Errorf("%w: %w: unexpected response status: 502", naive.ErrDestinationUnreachable, naive.ErrProxyAnswered)
	}
	return c.answeringConn.Measure(ctx)
}

func TestDestinationRebootRecoversBeforeRouteCooldown(t *testing.T) {
	t.Parallel()
	for _, early := range []bool{false, true} {
		t.Run(fmt.Sprint("early=", early), func(t *testing.T) { testDestinationRebootRecovery(t, early) })
	}
}

func TestDestinationRecoveryBackoffAndPortIsolation(t *testing.T) {
	t.Parallel()
	entry := &destinationEntry{}
	member := newMember("a-1", ms(30))
	now := time.Now()
	for _, delay := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second} {
		entry.block("a", 22, now, member)
		require.False(t, entry.retryRefused("a", 22, member, now.Add(delay-time.Millisecond)))
		require.True(t, entry.retryRefused("a", 22, member, now.Add(delay)))
		require.False(t, entry.retryRefused("a", 22, member, now.Add(delay)), "concurrent callers cannot claim the same interval")
		require.False(t, entry.blocked("a", 443, member, now), "SSH failure does not block HTTPS")
		now = now.Add(delay)
	}
	entry.reached("a", 22)
	entry.block("a", 22, now, member)
	require.True(t, entry.retryRefused("a", 22, member, now.Add(time.Second)), "a successful recovery resets the backoff")
}

func TestDestinationRecoveryHasOnlyOneDialInFlight(t *testing.T) {
	t.Parallel()
	f := newRaceFixture(t, 0, map[string][2]time.Duration{"a": {ms(30), ms(1)}})
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)
	o := &rebootOutbound{early: true, gate: gate}
	o.up.Store(true)
	group := f.smart.groups[0]
	group.members[0].outbound = o
	destination := M.ParseSocksaddr("reboot.example:22")
	entry := &destinationEntry{Selected: "a"}
	entry.block("a", 22, time.Now().Add(-2*time.Second), group.current())
	f.smart.cache.store(destinationKey(destination), entry)
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := f.smart.dialTCP(context.Background(), destination)
		if conn != nil {
			conn.Close()
		}
		if err != nil {
			t.Errorf("recovery failed: %v", err)
		}
	}()
	require.Eventually(t, func() bool { return o.dials.Load() == 1 }, time.Second, time.Millisecond)
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			conn, err := f.smart.dialTCP(context.Background(), destination)
			if conn != nil {
				conn.Close()
			}
			if err == nil {
				t.Error("a duplicate recovery dial was admitted")
			}
		}()
	}
	wait.Wait()
	require.EqualValues(t, 1, o.dials.Load())
	release()
	<-done
	require.Empty(t, f.smart.raceBound())
	require.Empty(t, f.smart.slots())
}

func testDestinationRebootRecovery(t *testing.T, early bool) {
	f := newRaceFixture(t, 0, map[string][2]time.Duration{"a": {ms(30), ms(1)}, "b": {ms(60), ms(1)}})
	lines := newRecordingLogger()
	f.smart.logger = lines
	destination := M.ParseSocksaddr("reboot.example:22")
	key := destinationKey(destination)
	entry := &destinationEntry{
		Remote:   map[string]remoteWindow{"a": {ms(1)}, "b": {ms(1)}},
		Path:     map[string]time.Duration{"a": ms(30), "b": ms(60)},
		Selected: "a", RacedAt: time.Now(),
	}
	f.smart.cache.store(key, entry)
	var exits []*rebootOutbound
	for _, group := range f.smart.groups {
		o := &rebootOutbound{early: early}
		exits = append(exits, o)
		group.members[0].outbound = o
	}
	conn, err := f.smart.dialTCP(context.Background(), destination)
	if err == nil {
		_, err = conn.Read(make([]byte, 1))
		conn.Close()
	}
	require.ErrorIs(t, err, naive.ErrDestinationUnreachable)
	require.Empty(t, lines.matching("failed over"), "no exit reached the target, so no migration succeeded")
	for _, group := range f.smart.groups {
		require.True(t, group.current().healthy.Load(), "a target reboot must not condemn an exit")
	}
	require.Equal(t, "a", entry.selected())
	for _, o := range exits {
		o.up.Store(true)
	}

	// Advance only the refusal timestamps; the five-minute route blocks stay live.
	entry.access.Lock()
	for k, c := range entry.unreachable {
		c.filed = time.Now().Add(-2 * time.Second).UnixNano()
		entry.unreachable[k] = c
	}
	entry.access.Unlock()
	conn, err = f.smart.dialTCP(context.Background(), destination)
	require.NoError(t, err, "a recovered target must be tried before the route cooldown expires")
	defer conn.Close()
	_, err = conn.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, "a", entry.selected(), "recovery keeps the original exit")
	require.EqualValues(t, 2, exits[0].dials.Load())
	require.EqualValues(t, 1, exits[1].dials.Load(), "recovery must not repeat the tour of all exits")
}
