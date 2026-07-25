package group

import (
	"context"
	"encoding/binary"
	"io"
	"time"

	"github.com/sagernet/sing-box/adapter"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// smart_probe.go is the client side of the measurement protocol. It dials a
// magic authority through a naive member; the naive outbound puts the marker
// verbatim into -connect-authority, so no outbound change is needed. The frame
// layout MUST match protocol/naive/probe.go.
const (
	smartProbeFrameLen = 5
	smartProbeStatusOK = 0
)

var errProbeUnreachable = E.New("smart probe: target unreachable")

// probeContexter is implemented by the naive outbound (cronet-go fork): it pins
// a dial to a named socket-pool partition so probes get a dedicated session
// separate from data, and a fresh session when the key changes. Kept as an
// interface so the group package needs no cronet import.
type probeContexter interface {
	ProbeIsolationContext(ctx context.Context, key string) context.Context
}

func probeCtx(ctx context.Context, member adapter.Outbound, key string) context.Context {
	if key == "" {
		return ctx
	}
	if pc, ok := member.(probeContexter); ok {
		return pc.ProbeIsolationContext(ctx, key)
	}
	return ctx
}

// probePing measures local RTT = dial → first response byte through a member.
// isoKey pins the probe's socket-pool partition (probe generation / verdict).
func probePing(ctx context.Context, member adapter.Outbound, isoKey string) (float64, error) {
	ctx = probeCtx(ctx, member, isoKey)
	marker := M.Socksaddr{Fqdn: "sbprobe--ping--x", Port: 443}
	start := time.Now()
	conn, err := member.DialContext(ctx, N.NetworkTCP, marker)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	closeOnDone(ctx, conn)
	frame := make([]byte, smartProbeFrameLen)
	if _, err = io.ReadFull(conn, frame); err != nil {
		return 0, err
	}
	return float64(time.Since(start).Milliseconds()), nil
}

// probeRemote measures the proxy→target TCP-connect time via a member. The
// server measures authoritatively and returns the result, so the value is
// independent of the client tunnel's warm/cold state.
func probeRemote(ctx context.Context, member adapter.Outbound, host string, port uint16, isoKey string) (float64, error) {
	ctx = probeCtx(ctx, member, isoKey)
	marker := M.Socksaddr{Fqdn: "sbprobe--tcp--" + host, Port: port}
	conn, err := member.DialContext(ctx, N.NetworkTCP, marker)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	closeOnDone(ctx, conn)
	frame := make([]byte, smartProbeFrameLen)
	if _, err = io.ReadFull(conn, frame); err != nil {
		return 0, err
	}
	if frame[2] != smartProbeStatusOK {
		return 0, errProbeUnreachable
	}
	return float64(binary.BigEndian.Uint16(frame[3:5])), nil
}

// closeOnDone unblocks a Read that does not honor ctx by closing the conn when
// ctx is cancelled (naive tunnel conns do not all support read deadlines).
func closeOnDone(ctx context.Context, closer interface{ Close() error }) {
	go func() {
		<-ctx.Done()
		_ = closer.Close()
	}()
}
