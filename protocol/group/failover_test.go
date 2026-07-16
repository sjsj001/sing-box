package group

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
)

func testFailover(strategy string, tags ...string) *Failover {
	s := &Failover{
		logger:       log.NewNOPFactory().Logger(),
		tags:         tags,
		strategy:     strategy,
		failLimit:    2,
		cooldownBase: time.Minute,
		cooldownMax:  15 * time.Minute,
		byTag:        make(map[string]*failoverMember),
		table:        newFailoverTable(64),
		recoveryHook: func(*failoverTarget, string) {},
	}
	for _, tag := range tags {
		member := &failoverMember{
			tag:       tag,
			interrupt: interrupt.NewGroup(),
			udp:       true,
			baseline:  newSmartWindow(smartWindowCap),
			alive:     true,
		}
		s.members = append(s.members, member)
		s.byTag[tag] = member
	}
	return s
}

func TestFailoverOrderSelectionAndIsolation(t *testing.T) {
	s := testFailover(failoverStrategyOrder, "A", "B")
	now := time.Now()
	target := s.table.ensure("site.example", "site.example", 443)
	other := s.table.ensure("other.example", "other.example", 443)

	if got := s.candidates(target, "tcp", now); len(got) != 2 || got[0] != "A" {
		t.Fatalf("candidates = %v, want [A B]", got)
	}
	// Two failures on A for site.example → cooled for that target only.
	s.onDialFailure(target, "A", now)
	s.onDialFailure(target, "A", now.Add(time.Second))
	if got := s.candidates(target, "tcp", now.Add(2*time.Second)); len(got) != 1 || got[0] != "B" {
		t.Fatalf("candidates after cooldown = %v, want [B]", got)
	}
	if got := s.candidates(other, "tcp", now.Add(2*time.Second)); len(got) != 2 || got[0] != "A" {
		t.Fatalf("other target affected: %v, want [A B] (per-target isolation)", got)
	}
	// B also cooled → fail open in preference order.
	s.onDialFailure(target, "B", now.Add(2*time.Second))
	s.onDialFailure(target, "B", now.Add(3*time.Second))
	if got := s.candidates(target, "tcp", now.Add(4*time.Second)); len(got) != 2 || got[0] != "A" {
		t.Fatalf("fail-open candidates = %v, want [A B]", got)
	}
	// A response byte through A resets its streak.
	s.onFirstByte(target, "A")
	stats := target.members["A"]
	if stats.consecFail != 0 || stats.needsRecovery {
		t.Fatal("first byte did not reset failure state")
	}
}

func TestFailoverCooldownEscalationAndStragglers(t *testing.T) {
	s := testFailover(failoverStrategyOrder, "A", "B")
	now := time.Now()
	target := s.table.ensure("flappy.example", "flappy.example", 443)
	s.onDialFailure(target, "A", now)
	s.onDialFailure(target, "A", now.Add(time.Second))
	stats := target.members["A"]
	if stats.backoff != 1 || !stats.cooldownUntil.After(now) {
		t.Fatalf("backoff = %d, want 1 with active cooldown", stats.backoff)
	}
	// Stragglers during the cooldown must not escalate.
	s.onDialFailure(target, "A", now.Add(2*time.Second))
	s.onDialFailure(target, "A", now.Add(3*time.Second))
	if stats.backoff != 1 {
		t.Fatalf("backoff = %d after stragglers, want 1", stats.backoff)
	}
	// Recovery then quick relapse escalates; relapse after the window resets.
	stats.needsRecovery = false
	stats.lastRecovery = now.Add(2 * time.Minute)
	stats.cooldownUntil = time.Time{}
	relapse := now.Add(3 * time.Minute)
	s.onDialFailure(target, "A", relapse)
	s.onDialFailure(target, "A", relapse.Add(time.Second))
	if stats.backoff != 2 {
		t.Fatalf("backoff = %d after quick relapse, want 2", stats.backoff)
	}
}

func TestFailoverUDPSkipsNonUDPMembers(t *testing.T) {
	s := testFailover(failoverStrategyOrder, "A", "B")
	s.byTag["A"].udp = false
	target := s.table.ensure("udp.example", "udp.example", 443)
	if got := s.candidates(target, "udp", time.Now()); len(got) != 1 || got[0] != "B" {
		t.Fatalf("udp candidates = %v, want [B]", got)
	}
}

func TestFailoverAutoElection(t *testing.T) {
	s := testFailover(failoverStrategyAuto, "A", "B")
	pushBaseline := func(tag string, ms float64) {
		member := s.byTag[tag]
		member.mu.Lock()
		member.baseline.Push(ms)
		member.mu.Unlock()
	}
	// Initial election takes the best available immediately.
	pushBaseline("A", 100)
	pushBaseline("B", 50)
	s.electPrimary()
	if s.elected.Load() != "B" {
		t.Fatalf("elected = %q, want B (initial)", s.elected.Load())
	}
	if order := s.memberOrder(); order[0] != "B" || order[1] != "A" {
		t.Fatalf("memberOrder = %v, want [B A]", order)
	}
	// A challenger below 80% of the incumbent needs 3 consecutive rounds.
	pushBaseline("A", 30) // A's rolling min now 30 < 0.8×50
	s.electPrimary()
	s.electPrimary()
	if s.elected.Load() != "B" {
		t.Fatal("switched primary before hysteresis streak completed")
	}
	s.electPrimary()
	if s.elected.Load() != "A" {
		t.Fatalf("elected = %q, want A after 3 rounds", s.elected.Load())
	}
	// A marginally better challenger never takes over.
	pushBaseline("B", 29)
	for i := 0; i < 5; i++ {
		s.electPrimary()
	}
	if s.elected.Load() != "A" {
		t.Fatalf("elected = %q, marginal challenger must not win", s.elected.Load())
	}
	// A dead incumbent is replaced immediately.
	s.byTag["A"].mu.Lock()
	s.byTag["A"].alive = false
	s.byTag["A"].mu.Unlock()
	s.electPrimary()
	if s.elected.Load() != "B" {
		t.Fatalf("elected = %q, want B after incumbent death", s.elected.Load())
	}
}

func TestFailoverHedgeDelayedCandidate(t *testing.T) {
	// Fast primary: the delayed standby never dials.
	standbyDialed := make(chan struct{}, 1)
	race := newSmartRaceConn([]*smartRaceCandidate{
		{tag: "A", flags: &smartRaceFlags{}, dial: raceDialer(t, 10*time.Millisecond, "primary-response", 5)},
		{tag: "B", flags: &smartRaceFlags{}, delay: 300 * time.Millisecond, dial: func() (net.Conn, error) {
			standbyDialed <- struct{}{}
			return nil, net.ErrClosed
		}},
	}, nil)
	if _, err := race.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 64)
	n, err := race.Read(buffer)
	if err != nil || string(buffer[:n]) != "primary-response" {
		t.Fatalf("read %q err %v", buffer[:n], err)
	}
	race.Close()
	select {
	case <-standbyDialed:
		t.Fatal("standby dialed although the primary answered within the delay")
	case <-time.After(500 * time.Millisecond):
	}

	// Silent primary: the standby starts after the delay and wins.
	silentDial := func() (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			buffer := make([]byte, 16)
			server.Read(buffer) // consume, never respond
		}()
		return client, nil
	}
	race = newSmartRaceConn([]*smartRaceCandidate{
		{tag: "A", flags: &smartRaceFlags{}, dial: silentDial},
		{tag: "B", flags: &smartRaceFlags{}, delay: 100 * time.Millisecond, dial: raceDialer(t, 10*time.Millisecond, "standby-response", 5)},
	}, nil)
	defer race.Close()
	start := time.Now()
	if _, err := race.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	n, err = race.Read(buffer)
	if err != nil || string(buffer[:n]) != "standby-response" {
		t.Fatalf("read %q err %v", buffer[:n], err)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("standby won in %s, before the hedge delay", elapsed)
	}

	// Failing primary wakes the delayed standby immediately.
	race = newSmartRaceConn([]*smartRaceCandidate{
		{tag: "A", flags: &smartRaceFlags{}, dial: func() (net.Conn, error) { return nil, net.ErrClosed }},
		{tag: "B", flags: &smartRaceFlags{}, delay: 5 * time.Second, dial: raceDialer(t, 10*time.Millisecond, "woken-response", 5)},
	}, nil)
	defer race.Close()
	start = time.Now()
	if _, err := race.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	n, err = race.Read(buffer)
	if err != nil || string(buffer[:n]) != "woken-response" {
		t.Fatalf("read %q err %v", buffer[:n], err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("standby took %s to start after primary failure, want immediate wake", elapsed)
	}
}

func TestFailoverHedgeLossCoolsPrimary(t *testing.T) {
	// A standby win charges the primary one failure (deduplicated with its
	// own early-fail path); two wins cool the primary for the target so
	// later requests stop paying hedge_delay.
	newHedgeGroup := func() *Failover {
		s := testFailover(failoverStrategyOrder, "A", "B")
		s.hedgeDelay = 100 * time.Millisecond
		s.byTag["A"].detour = &fakeDialOutbound{
			Adapter: outbound.NewAdapter("test", "A", []string{"tcp"}, nil),
			dial: func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
				client, server := net.Pipe()
				go func() { buffer := make([]byte, 16); server.Read(buffer) }() // consume, never respond
				return client, nil
			},
		}
		s.byTag["B"].detour = &fakeDialOutbound{
			Adapter: outbound.NewAdapter("test", "B", []string{"tcp"}, nil),
			dial: func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
				return raceDialer(t, 5*time.Millisecond, "standby-response", 5)()
			},
		}
		return s
	}
	s := newHedgeGroup()
	target := s.table.ensure("hedge-loss.example", "hedge-loss.example", 443)
	dest := M.ParseSocksaddrHostPort("hedge-loss.example", 443)
	rescue := func() {
		conn, err := s.dialHedged(context.Background(), target, "A", "B", dest, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, err := conn.Write([]byte("hello")); err != nil {
			t.Fatal(err)
		}
		buffer := make([]byte, 64)
		n, err := conn.Read(buffer)
		if err != nil || string(buffer[:n]) != "standby-response" {
			t.Fatalf("read %q err %v, want standby rescue", buffer[:n], err)
		}
	}
	rescue()
	deadline := time.Now().Add(2 * time.Second)
	for {
		target.mu.Lock()
		consecFail := target.stats("A").consecFail
		target.mu.Unlock()
		if consecFail == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("primary consecFail = %d, want exactly 1 after one hedge loss", consecFail)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Let the primary's early-fail window pass: the abandoned loser and the
	// CAS must both prevent a second charge from the same event.
	time.Sleep(400 * time.Millisecond)
	target.mu.Lock()
	consecFail := target.stats("A").consecFail
	target.mu.Unlock()
	if consecFail != 1 {
		t.Fatalf("primary consecFail = %d after settling, want 1 (no double count)", consecFail)
	}
	rescue()
	deadline = time.Now().Add(2 * time.Second)
	for {
		target.mu.Lock()
		cooling := target.stats("A").cooldownUntil.After(time.Now())
		target.mu.Unlock()
		if cooling {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("two hedge losses did not cool the primary")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := s.candidates(target, "tcp", time.Now()); len(got) != 1 || got[0] != "B" {
		t.Fatalf("candidates after cooldown = %v, want [B]", got)
	}
}

func TestFailoverHealthEndpointDownGuard(t *testing.T) {
	s := testFailover(failoverStrategyOrder, "A", "B")
	healthErr := net.ErrClosed
	for i := 0; i < failoverDownThreshold; i++ {
		s.noteHealthFailure(s.byTag["A"], healthErr)
		s.noteHealthFailure(s.byTag["B"], healthErr)
	}
	if !s.memberAlive(s.byTag["A"]) || !s.memberAlive(s.byTag["B"]) {
		t.Fatal("members demoted although the health endpoint failed via both")
	}
	// B reaches the endpoint again: A's failures are its own now.
	memberB := s.byTag["B"]
	memberB.mu.Lock()
	memberB.lastHealthOK = time.Now()
	memberB.mu.Unlock()
	s.noteHealthFailure(s.byTag["A"], healthErr)
	if s.memberAlive(s.byTag["A"]) {
		t.Fatal("member kept alive although another member reaches the endpoint")
	}
}
