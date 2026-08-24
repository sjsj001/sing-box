package naive

import (
	"strconv"
	"strings"
	"time"
)

// ConnectAckHeader carries what this hop measured while establishing the
// destination connection, as "<total-µs>[;d=<connect-µs>][;n=<resolve-µs>]".
//
//   - total spans accepting the CONNECT to having the destination reachable. On
//     the last hop that is routing, name resolution and the connect; on a relay
//     it includes waiting for the next hop's own answer. A client subtracts it
//     from its observed span to derive how far away this hop is.
//   - connect is the *innermost* hop's pure TCP connect, with name resolution
//     removed, passed through relay hops unchanged so it always means "last hop
//     to destination".
//   - resolve is what that same hop took out of connect: finding the
//     destination, as opposed to reaching it. Passed through relays for the same
//     reason connect is — a relay's own lookup found a proxy, not a destination.
//
// Why resolve is on the wire at all, given connect already excludes it. Without
// it the client can only compute the chain's interior as total minus connect,
// and the exit's resolution lands inside that: a node-level quantity, charged
// against the node, for something that belongs to whichever destination
// happened to be asked for. Four cold lookups in a window of eight put a
// single-hop member's interior past the point where it changes which member a
// group uses. Reported, the client puts it back where it belongs — see
// ConnMeasurement.
//
// Both extra fields are optional and unknown ones are skipped, so a hop that
// predates either of them interoperates in both directions and simply teaches
// less.
//
// The header being present is the capability signal. A server that answers
// optimistically without measuring anything sends no header, and a client must
// treat that as "unknown" rather than as zero — zero would read as a
// destination sitting next to the exit, which wins every comparison it enters.
const ConnectAckHeader = "-connect-ack"

// ConnectAck is one parsed header value.
type ConnectAck struct {
	Total      time.Duration
	Connect    time.Duration
	HasConnect bool
	// Resolve is the innermost hop's name resolution span. Absent rather than
	// zero when the hop does not report it, because zero is a claim — it says
	// the destination was already known — and a hop that predates the field
	// makes no claim either way.
	Resolve    time.Duration
	HasResolve bool
}

// DestinationLeg is what reaching the destination cost this hop: the connect
// plus finding what to connect to.
//
// The two travel apart on the wire and are consumed together here, and that is
// deliberate. Finding the destination belongs to the destination — left in the
// hop's own leg it charges a node for whichever name it happened to be asked
// for — but it may only ever be *moved*, never dropped: the client's round trip
// is the one quantity it measures itself, so anything a hop reports as
// subtractable from it is a discount that hop grants itself, worth alpha per
// microsecond to overstate. Summed into the destination leg it is worth exactly
// what the connect beside it is worth, and overstating it costs the hop instead.
//
// Absent when no hop reported a connect, because a resolution with nothing to
// add it to describes no leg at all.
func (ack ConnectAck) DestinationLeg() (time.Duration, bool) {
	if !ack.HasConnect {
		return 0, false
	}
	return ack.Connect + ack.Resolve, true
}

// maxAckMicros bounds a plausible value. Beyond this the input is garbage, and
// more importantly multiplying it up to a Duration would overflow int64 into a
// *negative* span — which reads as an impossibly close destination and would
// let a broken or hostile proxy capture every routing decision.
const maxAckMicros = int64(10 * time.Minute / time.Microsecond)

func FormatConnectAck(ack ConnectAck) string {
	value := strconv.FormatInt(ack.Total.Microseconds(), 10)
	if ack.HasConnect {
		value += ";d=" + strconv.FormatInt(ack.Connect.Microseconds(), 10)
	}
	if ack.HasResolve {
		value += ";n=" + strconv.FormatInt(ack.Resolve.Microseconds(), 10)
	}
	return value
}

func parseAckMicros(field string) (time.Duration, bool) {
	micros, err := strconv.ParseInt(field, 10, 64)
	if err != nil || micros < 0 || micros > maxAckMicros {
		return 0, false
	}
	return time.Duration(micros) * time.Microsecond, true
}

// ParseConnectAck reads a header value. It reports false when the header is
// absent or unusable, which the caller must treat as "this hop measured
// nothing" rather than as a measurement of zero.
func ParseConnectAck(value string) (ConnectAck, bool) {
	if value == "" {
		return ConnectAck{}, false
	}
	fields := strings.Split(value, ";")
	total, ok := parseAckMicros(strings.TrimSpace(fields[0]))
	if !ok {
		return ConnectAck{}, false
	}
	ack := ConnectAck{Total: total}
	for _, field := range fields[1:] {
		field = strings.TrimSpace(field)
		switch {
		case strings.HasPrefix(field, "d="):
			connect, connectOK := parseAckMicros(field[2:])
			if !connectOK {
				// A malformed connect field makes the whole value untrustworthy:
				// reporting only the total would silently reattribute the
				// destination leg to the hop.
				return ConnectAck{}, false
			}
			ack.Connect, ack.HasConnect = connect, true
		case strings.HasPrefix(field, "n="):
			resolve, resolveOK := parseAckMicros(field[2:])
			if !resolveOK {
				// Held to the same standard, and for a sharper reason: the
				// client moves this span out of the node's leg and onto the
				// destination's, so a garbled one moves time between two legs
				// that are ranked against each other.
				return ConnectAck{}, false
			}
			ack.Resolve, ack.HasResolve = resolve, true
		}
		// Anything else is a field this build does not know, and skipping it is
		// what lets either side add one without waiting for the other.
	}
	return ack, true
}
