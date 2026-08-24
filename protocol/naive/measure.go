package naive

import (
	"context"
	"time"
)

// ConnMeasurement is what a single connection reveals about the two legs of the
// path it took.
//
// Setup covers establishing the connection to the proxy — TCP, TLS and the
// HTTP/2 session. It is near zero when the stream lands on an already
// established connection, which makes it the cold/warm indicator for a
// multiplexed pool: a sample with a large Setup measured a handshake, not a
// path.
//
// RoundTrip is the request headers reaching the wire until the response headers
// come back, and so contains exactly one client-to-proxy round trip plus
// whatever the proxy spent establishing the destination connection.
//
// ServerSpan is what the proxy says that establishment cost it, and RemoteDial
// is the innermost hop's pure connect time. HasRemote reports whether the proxy
// measured anything at all: a server that answers without measuring is not the
// same as one that measured zero, and treating the two alike would rank a
// silent proxy as though every destination sat next to it.
type ConnMeasurement struct {
	Setup      time.Duration
	RoundTrip  time.Duration
	ServerSpan time.Duration
	RemoteDial time.Duration
	// HasSpan reports that the proxy said what its own work cost, which is what
	// makes Local meaningful. HasRemote reports that a hop measured a real
	// connect to the destination — a relay whose next hop stayed silent can
	// honestly report the first without the second.
	HasSpan   bool
	HasRemote bool
	// Resolve is how much of RemoteDial was finding the destination rather than
	// reaching it, when the innermost hop reported it. It is already inside
	// RemoteDial and must not be applied to any leg a second time — it is kept
	// separately because one thing has to be decided from it that the sum
	// cannot answer: whether the exit's resolver was cold for this destination,
	// which is a property of when that exit last used it and not of the path.
	Resolve    time.Duration
	HasResolve bool
}

// Local is the client-to-proxy leg with the proxy's own work removed. Across a
// chain it covers every hop up to the innermost proxy, because the span each hop
// reports already contains the wait for the hop beyond it.
func (m ConnMeasurement) Local() time.Duration {
	local := m.RoundTrip - m.ServerSpan
	if local < 0 {
		// Clock skew between the two ends, or a proxy reporting a duration that
		// overlaps its own response write. Neither is worth propagating as a
		// negative latency.
		return 0
	}
	return local
}

// The path divides into three, and every one of them is already on the wire:
//
//	NearHop   = RoundTrip  - ServerSpan     this client to the first proxy
//	ChainSpan = ServerSpan - RemoteDial     the first proxy to the innermost one
//	RemoteDial                              the innermost proxy to the destination
//
//	NearHop + ChainSpan + RemoteDial = RoundTrip
//
// The middle one holds for any depth of chain without anything being added to
// the protocol: each hop's span already contains the wait for the hop beyond
// it, and the innermost connect is passed up unchanged (see the responder's
// settleFromNextHop). So a three-hop chain reports the interior of the whole
// chain in one number, which is what a client needs to know about it.
//
// On a proxy that dials the destination itself the interior is its own routing
// and name resolution — half a millisecond, measured. On a relay it is every
// hop in between, and a relay whose next hop has gone bad is a case nothing
// else here can see: it is not the client's leg, and it is not the distance to
// the destination.

// NearHop is the leg to the first proxy — the round trip with everything that
// proxy said it spent taken out. It is what the heartbeat address measures on
// its own, since the proxy answers that one without dialing, and the one leg
// whose floor the locally-timed handshake can bound.
func (m ConnMeasurement) NearHop() time.Duration {
	return m.Local()
}

// ChainSpan is the interior of the chain: what the first proxy spent that is
// neither this client's leg nor the innermost hop's reach for the destination.
//
// That last part includes the innermost hop's own name resolution when it
// reports one, because finding the destination belongs to the destination. An
// exit too old to report it leaves its lookups here, where they are charged to
// the node instead — the one case this leg still measures something that is not
// about the node.
//
// ok is false when no hop reported a connect, because absent is not zero: zero
// would say the chain has no interior, which is exactly what a relay looks
// like when nothing measures it, and exactly the reading this exists to stop.
func (m ConnMeasurement) ChainSpan() (time.Duration, bool) {
	if !m.HasSpan || !m.HasRemote {
		return 0, false
	}
	chain := m.ServerSpan - m.RemoteDial
	if chain < 0 {
		// Validated already rejects a connect that outlasts the span it is part
		// of, so this is the sub-tolerance remainder of the same disagreement.
		return 0, true
	}
	return chain, true
}

// ackTolerance absorbs the disagreement between two ends measuring overlapping
// intervals: the proxy opens its span after reading the request and closes it
// before writing the response, and neither boundary lines up exactly with what
// the client timed.
const ackTolerance = 5 * time.Millisecond

// Validated drops an account of the path that cannot be reconciled with what
// this side measured.
//
// Both quantities a ranking would use are the proxy's word. Local is the round
// trip measured here minus the span the proxy claimed for itself, and remote is
// what the proxy claimed the destination cost. A proxy that reports its span as
// the whole round trip and its connect as zero therefore presents itself as
// sitting next to the client and next to every destination at once, and wins
// every comparison it is entered into. That has to be caught where the numbers
// arrive: by the time they reach a ranking they are indistinguishable from a
// genuinely excellent node.
//
// Only consistency can be checked from here — a span cannot exceed the round
// trip that contains it, and the connect cannot exceed the span that contains
// it. Understating within those bounds is not detectable on this side at all,
// which is a property of the protocol rather than of this function: a proxy is
// the one measuring its own work. The smart outbound narrows the remaining
// room with a floor taken from its own handshake timings, and what is left is a
// trust boundary that belongs in the documentation rather than in code.
func (m ConnMeasurement) Validated() ConnMeasurement {
	if !m.HasSpan {
		return m
	}
	if m.RoundTrip > 0 && m.ServerSpan > m.RoundTrip+ackTolerance {
		// The proxy cannot have spent longer than the round trip that bracketed
		// it. One impossible field makes the whole report untrustworthy — the
		// same call the header parser makes about a malformed connect.
		m.HasSpan, m.HasRemote, m.HasResolve = false, false, false
		return m
	}
	if m.HasRemote && m.RemoteDial > m.ServerSpan+ackTolerance {
		// The connect is part of the span, so it cannot outlast it — and the
		// span is the more damaging of the two to keep. Local is the round trip
		// minus the span, so a span understated to near zero hands the node the
		// entire distance to the destination as though it were its own, and the
		// node is then ranked on wherever it happened to be asked for.
		m.HasSpan, m.HasRemote, m.HasResolve = false, false, false
	}
	return m
}

// PooledOutbound is implemented by outbounds that spread their streams over
// several isolated connection pools, so a caller that wants none of them cold
// knows how many connections that takes.
type PooledOutbound interface {
	// Pools reports how many isolated pools streams are spread over. It is
	// always at least one.
	Pools() int
}

// PayloadCarrier is implemented by outbound connections that can report whether
// any payload has been handed to the tunnel. A connection that never carried
// payload can have its opening write replayed elsewhere without the
// destination receiving a request twice; one that did carry payload cannot
// prove non-delivery, whatever error it failed with. The answer is final once
// the connection has been closed.
//
// The guarantee is about payload, not about the connection itself: a hop that
// had already dialed the destination before the tunnel broke leaves it one
// bare TCP connect, and the replay adds a second. That is the CONNECT
// semantics — establishment is repeatable, the request is not.
type PayloadCarrier interface {
	CarriedPayload() bool
}

// MeasuredConn is implemented by outbound connections that can report how the
// latency of their path divides between the client-to-proxy and
// proxy-to-destination legs.
type MeasuredConn interface {
	// Measure blocks until the proxy has answered the CONNECT, then reports the
	// split. It fails with ErrDestinationUnreachable or ErrNextHopUnreachable
	// when the proxy refused, which tells a caller whether to avoid this
	// destination or this node.
	Measure(ctx context.Context) (ConnMeasurement, error)

	// WaitReady blocks only until the connection to the proxy is established.
	// It is separate from Measure because only this part is bounded by the
	// client-to-proxy leg: how long the proxy then takes to answer depends on
	// the destination, and a destination that is simply far away must not be
	// mistaken for a broken proxy.
	WaitReady(ctx context.Context) error
}
