package group

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing/common"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// recoverDestination is reached only when no normal route is available. A
// target refused by every healthy exit gets one check on its original exit,
// rather than another tour of all exits or a five-minute cached refusal.
func (s *Smart) recoverDestination(ctx context.Context, key string, destination M.Socksaddr, entry *destinationEntry) (net.Conn, error) {
	now := time.Now()
	failure := s.errNoCandidate(entry, destination, now)
	var group *smartGroup
	for _, g := range s.groups {
		member := g.current()
		if member == nil || !member.healthy.Load() {
			continue
		}
		if entry == nil || !entry.blocked(g.tag, destination.Port, member, now) {
			return nil, failure
		}
		if group == nil || g.tag == entry.selected() {
			group = g
		}
	}
	if group == nil || ctx.Err() != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, failure
	}
	flight := cooldownKey{key, destination.Port}
	if _, loaded := s.recoveries.LoadOrStore(flight, struct{}{}); loaded {
		return nil, failure
	}
	defer s.recoveries.Delete(flight)
	// Share the probe bound, but never queue recovery behind routine probes.
	select {
	case s.slots() <- struct{}{}:
		defer func() { <-s.slots() }()
	default:
		return nil, failure
	}
	member := group.current()
	if !entry.retryRefused(group.tag, destination.Port, member, now) {
		return nil, failure
	}
	checkCtx, cancel := context.WithTimeout(ctx, raceDialTimeout)
	defer cancel()
	conn, err := member.outbound.DialContext(checkCtx, N.NetworkTCP, destination)
	if err == nil {
		if measured, ok := common.Cast[naive.MeasuredConn](conn); ok {
			err = s.awaitReady(checkCtx, member, conn)
			if err == nil {
				var measurement naive.ConnMeasurement
				measurement, err = measured.Measure(checkCtx)
				if err == nil {
					s.record(member, measurement)
				}
			}
		}
	}
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		s.reportFailure(key, destination, group, member, err)
		// The same target failing repeatedly never reaches the distinct-target
		// refusal threshold. Check real egress as well, once per probe window.
		at, last := time.Now().UnixNano(), member.refusalProbedAt.Load()
		if errors.Is(err, naive.ErrDestinationUnreachable) &&
			at-last >= int64(refusalProbeInterval) && member.refusalProbedAt.CompareAndSwap(last, at) {
			s.confirmDown(member)
		}
		return nil, err
	}
	if current := s.cache.load(key); current != nil {
		current.reached(group.tag, destination.Port)
	}
	s.countRequest(key, group, member, reasonSticky, N.NetworkTCP)
	s.logger.DebugContext(ctx, "destination reachable again: ", destination, " via ", group.tag, "/", member.tag)
	return &replayConn{Conn: conn, smart: s, destination: destination, key: key,
		group: group, member: member, settled: true}, nil
}
