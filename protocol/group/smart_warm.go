package group

import (
	"context"
	"sync"
	"time"

	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing/common"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// racedLeg is what one group brought to a race, kept only long enough to decide
// whether its destination reading has to be taken again.
type racedLeg struct {
	member  *smartMember
	resolve time.Duration
}

// warmResolverBias is how much of a destination reading has to be the exit
// looking the destination up before that reading is taken again.
//
// switchHysteresis is what the rest of this package means by "large enough to
// change a decision", and that is the whole question here. Below it, the
// difference between a warm resolver and a cold one cannot move a selection, so
// a second connection buys nothing and is not worth what it costs the
// destination to see. Above it, it can — and it moves it the same way at every
// refresh, for as long as the snapshot lives.
const warmResolverBias = switchHysteresis

// amendColdResolutions takes a second destination reading for every group whose
// first one was mostly the exit looking the destination up.
//
// The bias this removes is not noise, which is why smoothing cannot reach it.
// The group carrying a destination's traffic keeps its exit's resolver warm for
// that name; every other group last resolved it during the previous race, tens
// of minutes ago and well past any TTL. So a refresh measures the incumbent
// warm and every challenger cold, again and again, and the gap is a fact about
// who was used last rather than about the path. Frozen into a snapshot it
// becomes a standing handicap renewing itself at the exact moment the refresh
// exists to correct one.
//
// Taking the reading again is the only way out that does not move the problem.
// The lookup cannot be discounted arithmetically instead: the client's own
// round trip is the one number it measures itself, and subtracting anything the
// exit reports from it hands the exit a discount worth alpha per microsecond —
// see the note in naive.Measure. What is wrong here is the measurement, so the
// measurement is what gets fixed.
//
// Runs after the race is decided and after its snapshot has landed, so neither
// the caller nor the verdict waits for any of it. The round's own conclusion
// stands on the cold numbers; what this corrects is the record the next
// thousand connections are routed on.
func (s *Smart) amendColdResolutions(key string, destination M.Socksaddr, entry *destinationEntry, legs map[string]racedLeg) {
	if entry == nil || !destination.IsFqdn() {
		// An address needs no looking up, so both sides of a race already met it
		// on equal terms.
		return
	}
	if len(s.groups) < 2 {
		// The bias this removes is between the group carrying a destination and
		// the ones challenging it. With one group there is no challenger: the
		// reading it corrects cannot change a selection, because there is
		// nothing to select against. What is left is the cost — one more
		// connection to the destination, from the same exit, for a comparison
		// that never happens.
		//
		// Checked before the legs are walked so a single-group outbound does not
		// pay for classifying resolutions it will not act on, and read live so
		// that adding a second group restores the retake with no further change.
		return
	}
	var cold []string
	for tag, leg := range legs {
		if leg.member != nil && leg.resolve > warmResolverBias {
			cold = append(cold, tag)
		}
	}
	if len(cold) == 0 {
		// Every exit already had the name. Nothing leaves here, which is the
		// common case once a destination has been raced before.
		return
	}

	slots := s.slots()
	var (
		wait    sync.WaitGroup
		access  sync.Mutex
		amended []auditLeg
	)
	for _, tag := range cold {
		wait.Add(1)
		go func(tag string) {
			defer wait.Done()
			// The same gate the heartbeat uses. These dials are worth less than
			// anything a caller is waiting for, and a race across many groups
			// would otherwise put a burst of them out at once.
			select {
			case slots <- struct{}{}:
			case <-s.ctx.Done():
				return
			}
			defer func() { <-slots }()

			leg := legs[tag]
			warm, taken := s.warmLeg(leg.member, destination)
			if !taken {
				return
			}
			was, hadReading := entry.remoteFor(tag)
			entry.amendRemote(tag, warm)
			record := auditLeg{
				Tag:     tag,
				Member:  leg.member.tag,
				Remote:  short(warm),
				Resolve: short(leg.resolve),
			}
			if hadReading {
				record.Was = short(was)
			}
			access.Lock()
			amended = append(amended, record)
			access.Unlock()
		}(tag)
	}
	wait.Wait()
	if len(amended) == 0 {
		return
	}
	// The race that chose is already in the trail with the numbers it chose on,
	// and those are no longer the numbers this destination is routed by.
	s.auditAmend(key, destination.String(), amended)
	// Marked before flushing, and that order is the whole of it: a flush writes
	// what has been marked changed, and amendRemote alters an entry in place
	// rather than replacing it, so nothing marks it. Without this the flush
	// below finds an empty set and writes nothing — the amendment lives in
	// memory until the process ends, and a restart brings back the very reading
	// it was taken to replace, handicap and all.
	s.cache.touch(key)
	// Persisted for the same reason a race result is: this is the reading the
	// snapshot will be believed on for hours.
	if err := s.cache.flush(); err != nil {
		s.logger.Debug("persist amended snapshot for ", destination, ": ", err)
	}
}

// warmLeg measures the destination through one member again, against an exit
// that has just resolved it.
//
// The member is the one that raced, passed down rather than asked of the group:
// what is being amended is that member's reading, and a group hands out
// whichever member it would use now, which after a failover is a different node
// entirely.
//
// A failure here decides nothing. It is not a caller's connection and not a
// heartbeat — the member's health was settled by the race that just ran, and
// letting a background retake condemn a node would add a death sentence to a
// path whose only job is to sharpen a number. The cold reading simply stands.
func (s *Smart) warmLeg(member *smartMember, destination M.Socksaddr) (time.Duration, bool) {
	if member == nil || member.outbound == nil {
		return 0, false
	}
	ctx, cancel := context.WithTimeout(s.ctx, s.probeTimeout(member, destination))
	defer cancel()

	conn, err := member.outbound.DialContext(ctx, N.NetworkTCP, destination)
	if err != nil {
		s.logger.Debug("warm ", destination, " through ", member.tag, ": ", err)
		return 0, false
	}
	// Nothing is ever written: the destination sees the same payload-free
	// connection attempt a race probe makes, from an exit that has just made
	// one, and it is closed the moment the proxy has answered.
	defer conn.Close()

	measured, isMeasured := common.Cast[naive.MeasuredConn](conn)
	if !isMeasured {
		return 0, false
	}
	measurement, err := measured.Measure(ctx)
	if err != nil {
		s.logger.Debug("warm ", destination, " through ", member.tag, ": ", err)
		return 0, false
	}
	if !measurement.HasRemote {
		return 0, false
	}
	// Banked as a probe rather than as use: this is not the member carrying
	// traffic, and stamping it as such would let a race suppress the next
	// heartbeat round for every member that took part.
	s.recordProbe(member, destination, measurement)
	// The retake stands whether it came back better or worse. Keeping the
	// smaller of the two would be the "lower of two middles" rule this package
	// rejected for remoteWindow, one level up: it turns every retake into a
	// licence to keep the more flattering round.
	return measurement.RemoteDial, true
}
