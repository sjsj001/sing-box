package dialer

import (
	"context"
	"sync"
	"time"
)

// ConnectTiming accumulates what establishing one outbound connection cost, so
// an inbound that reports timings back to its client has something honest to
// report.
//
// It is opt-in: only an inbound that seeds it with WithConnectTiming pays
// anything, and every other dial does one nil check.
//
// Two spans come out of it, because a client needs different things from each:
//
//   - total is everything this hop spent, from accepting the request to having
//     the destination reachable. A client subtracts it from its own observed
//     span to work out how far away this hop is.
//   - connect is the innermost hop's pure TCP connect, with name resolution
//     taken out. That subtraction is the point of the whole type: a cold
//     resolver cache can cost more than a second, and left in, it would be
//     attributed to the path to the destination and make a perfectly good exit
//     look unusable for as long as the measurement is cached.
type ConnectTiming struct {
	access sync.Mutex
	// start is when this hop took the request on, and total is how long it had
	// spent by the time it was ready to answer. Total is measured rather than
	// added up from the parts because the parts are not all visible from here:
	// a relay's dial only opens a stream and returns, and everything the hop
	// actually spends getting the destination reachable happens afterwards,
	// while it waits for the next hop's answer. Summing what could be seen gave
	// a total smaller than the connect time it was supposed to contain — which
	// is impossible, and which a client subtracting it from its own round trip
	// reads as this hop being infinitely close.
	start        time.Time
	total        time.Duration
	dial         time.Duration
	dns          time.Duration
	inner        time.Duration
	innerDNS     time.Duration
	haveTotal    bool
	haveDial     bool
	haveInner    bool
	haveInnerDNS bool
	relay        bool
}

type connectTimingKey struct{}

// WithConnectTiming seeds a context so the dials made under it are measured.
func WithConnectTiming(ctx context.Context) (context.Context, *ConnectTiming) {
	timing := &ConnectTiming{start: time.Now()}
	return context.WithValue(ctx, connectTimingKey{}, timing), timing
}

// ConnectTimingFromContext returns the timing seeded on ctx, or nil. Every
// method tolerates a nil receiver so callers never have to check.
func ConnectTimingFromContext(ctx context.Context) *ConnectTiming {
	timing, _ := ctx.Value(connectTimingKey{}).(*ConnectTiming)
	return timing
}

// MarkRelay declares that this connection goes through another proxy rather
// than to the destination itself.
//
// It matters because "dial minus resolution" then measures establishing a
// session with the next hop, which has nothing to do with how far the
// destination is. On a relay the only honest connect time is the one the next
// hop reports, so without RecordInnerAck none is reported at all — absent says
// "unknown", whereas a small number would say "very close", and that lie would
// win every comparison it took part in.
func (t *ConnectTiming) MarkRelay() {
	if t == nil {
		return
	}
	t.access.Lock()
	defer t.access.Unlock()
	t.relay = true
}

// RecordDial stores the full dial span. First write wins: one routed connection
// dials once.
func (t *ConnectTiming) RecordDial(dial time.Duration) {
	if t == nil {
		return
	}
	t.access.Lock()
	defer t.access.Unlock()
	if !t.haveDial {
		t.dial, t.haveDial = dial, true
	}
}

// RecordDNS stores a name resolution span, keeping the largest seen — that is
// the one most likely to have held the dial up.
func (t *ConnectTiming) RecordDNS(lookup time.Duration) {
	if t == nil {
		return
	}
	t.access.Lock()
	defer t.access.Unlock()
	if lookup > t.dns {
		t.dns = lookup
	}
}

// RecordInnerAck stores the connect time the next hop reported, which is what a
// relay passes through as its own. First write wins.
func (t *ConnectTiming) RecordInnerAck(connect time.Duration) {
	if t == nil {
		return
	}
	t.access.Lock()
	defer t.access.Unlock()
	t.relay = true
	if !t.haveInner {
		t.inner, t.haveInner = connect, true
	}
}

// RecordInnerResolution stores the resolution span the next hop reported, which
// a relay passes through for the same reason it passes the connect through: the
// lookup that matters is the one that found the destination, and this hop's own
// found a proxy. First write wins.
//
// Separate from RecordInnerAck because the two arrive together but not always
// both: a next hop old enough to report a connect and not a resolution is a
// deployment that exists, and it must keep working.
func (t *ConnectTiming) RecordInnerResolution(resolve time.Duration) {
	if t == nil {
		return
	}
	t.access.Lock()
	defer t.access.Unlock()
	t.relay = true
	if !t.haveInnerDNS {
		t.innerDNS, t.haveInnerDNS = resolve, true
	}
}

// Resolution reports how much of reaching the destination was finding it,
// which is the part of a hop's work that belongs to the destination rather than
// to the hop.
//
// On a relay it is whatever the next hop reported, unchanged. Otherwise it is
// this hop's own lookup, and it is reported even when zero — that is a real
// claim, not an absence: the destination was already known, or was an address
// to begin with. Reported alongside a dial rather than on its own, because the
// invariant a client relies on is connect + resolve = dial, and there is no
// dial to hold that up without one.
func (t *ConnectTiming) Resolution() (time.Duration, bool) {
	if t == nil {
		return 0, false
	}
	t.access.Lock()
	defer t.access.Unlock()
	if t.relay {
		return t.innerDNS, t.haveInnerDNS
	}
	if !t.haveDial || t.dial < t.dns {
		return 0, false
	}
	return t.dns, true
}

// Spans reports the two wire values. hasConnect is false when nothing usable is
// known about the connect: a relay whose next hop reported nothing, or a dial
// whose recorded resolution exceeds it (see below). ok is false if no dial
// completed.
//
// The total is stamped on the first call and kept, so asking twice cannot
// report two different numbers. The first call is the one made just before the
// response goes out, which is exactly where the span the client subtracts is
// supposed to end.
func (t *ConnectTiming) Spans() (total time.Duration, connect time.Duration, hasConnect bool, ok bool) {
	if t == nil {
		return 0, 0, false, false
	}
	t.access.Lock()
	defer t.access.Unlock()
	if !t.haveDial {
		return 0, 0, false, false
	}
	if !t.haveTotal {
		t.total, t.haveTotal = time.Since(t.start), true
		if t.start.IsZero() || t.total < t.dial {
			// Built without WithConnectTiming, or a clock that went backwards.
			// The dial is the one span that is certainly inside the total.
			t.total = t.dial
		}
	}
	if t.relay {
		return t.total, t.inner, t.haveInner, true
	}
	connect = t.dial - t.dns
	if connect < 0 {
		// A resolution span larger than the dial that contained it: clock skew,
		// or a lookup that belonged to something else — a resolver bootstrapping
		// its own upstream records into the same timing. Nothing useful is known
		// about the connect, and reporting zero would say "on top of the
		// destination", which wins every comparison it takes part in. Absent
		// says unknown.
		return t.total, 0, false, true
	}
	return t.total, connect, true, true
}
