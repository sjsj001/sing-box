package naive

import (
	"context"
	"encoding/binary"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sagernet/sing-box/adapter"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
)

// errProbeBlockedTarget rejects a probe dial to a non-global address.
var errProbeBlockedTarget = E.New("smart probe: blocked non-global target")

// blockedProbeAddr reports whether a resolved address must not be probed: the
// probe dials directly (bypassing the server router), so restrict it to global
// unicast to deny it as an SSRF / internal-port-scan primitive against the
// server's own loopback, LAN, and cloud metadata (169.254.169.254) (NG1).
func blockedProbeAddr(ip netip.Addr) bool {
	return !ip.IsValid() || ip.IsLoopback() || ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() || ip.IsMulticast()
}

// probe.go implements the server side of the smart-group measurement protocol
// (tingly-riding-parrot-final.md §2). A probe arrives as a CONNECT whose
// authority carries a marker prefix; it is served on the authenticated,
// post-200 path so it reuses the existing padding framing. The response is a
// small fixed frame the client de-pads and reads.
//
// The frame layout (before padding) MUST stay in sync with the client copy in
// protocol/group/smart_probe.go.
const (
	smartProbeMarker   = "sbprobe--"
	smartProbeFrameLen = 5
	smartProbeVersion  = 1
	smartProbeModePing = 1
	smartProbeModeTCP  = 2

	smartProbeStatusOK   = 0
	smartProbeStatusFail = 1
)

// isSmartProbe reports whether a CONNECT authority host is a smart probe.
func isSmartProbe(fqdn string) bool {
	return strings.HasPrefix(fqdn, smartProbeMarker)
}

// handleSmartProbe answers a probe on an already-established (200-sent) conn and
// closes it. conn is a naiveConn/naiveH2Conn, so Write applies the padding the
// client expects. destination.Fqdn is "sbprobe--<mode>--<realhost>"; the target
// port for tcp mode is the authority port.
func (n *Inbound) handleSmartProbe(ctx context.Context, conn net.Conn, destination M.Socksaddr) {
	defer conn.Close()

	frame := make([]byte, smartProbeFrameLen)
	frame[0] = smartProbeVersion

	rest := destination.Fqdn[len(smartProbeMarker):]
	mode, realHost, _ := strings.Cut(rest, "--")
	switch mode {
	case "ping":
		frame[1] = smartProbeModePing
		frame[2] = smartProbeStatusOK
	case "tcp":
		frame[1] = smartProbeModeTCP
		if realHost == "" {
			frame[2] = smartProbeStatusFail
			break
		}
		ms, err := n.probeTCPConnect(ctx, realHost, destination.Port)
		if err != nil {
			frame[2] = smartProbeStatusFail
		} else {
			frame[2] = smartProbeStatusOK
			binary.BigEndian.PutUint16(frame[3:5], clampProbeMs(ms))
		}
	default:
		frame[2] = smartProbeStatusFail
	}
	_, _ = conn.Write(frame)
}

// probeTargetTimeout bounds the whole server-side probe — name resolution plus
// the connect. The client has its own deadline, but the server must not be left
// holding a stream open on a name that never resolves.
const probeTargetTimeout = 5 * time.Second

// probeTCPConnect measures the TCP-connect time to the real target. It dials
// directly (not through the server router) so the sample reflects the raw
// proxy→target leg.
//
// Name resolution goes through the router's DNS — the same resolver real
// traffic through this inbound uses. With the system resolver, a geo-DNS name
// could hand the probe a different address than the one the proxied connection
// actually reaches, so the group would be ranking a host it never talks to.
// Resolving first also takes DNS back out of the reported number, leaving it a
// clean connect time: real traffic answers from the DNS cache in steady state,
// so charging every probe for a fresh lookup only added noise.
func (n *Inbound) probeTCPConnect(ctx context.Context, host string, port uint16) (float64, error) {
	ctx, cancel := context.WithTimeout(ctx, probeTargetTimeout)
	defer cancel()
	target, err := n.resolveProbeTarget(ctx, host)
	if err != nil {
		return 0, err
	}
	dialer := net.Dialer{
		// Belt and braces: the address is already screened below, but this also
		// covers the system-resolver fallback path, where the name is handed to
		// the dialer unresolved (NG1).
		Control: func(network, address string, c syscall.RawConn) error {
			addrPort, err := netip.ParseAddrPort(address)
			if err != nil || blockedProbeAddr(addrPort.Addr()) {
				return errProbeBlockedTarget
			}
			return nil
		},
	}
	start := time.Now()
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(target, strconv.Itoa(int(port))))
	if err != nil {
		return 0, err
	}
	elapsed := time.Since(start)
	_ = conn.Close()
	return float64(elapsed.Milliseconds()), nil
}

// resolveProbeTarget picks the host the probe should dial: the literal itself,
// or the first usable answer from the router's DNS. Non-global-unicast results
// are rejected here rather than only at connect time, so a name that resolves
// entirely to internal addresses fails without a SYN (NG1).
func (n *Inbound) resolveProbeTarget(ctx context.Context, host string) (string, error) {
	if addr, parseErr := netip.ParseAddr(host); parseErr == nil {
		if blockedProbeAddr(addr) {
			return "", errProbeBlockedTarget
		}
		return host, nil
	}
	if n.dnsRouter == nil {
		// Standalone inbound with no router DNS: hand the name to the dialer and
		// let the system resolver handle it. The Control guard still screens
		// whatever it picks.
		return host, nil
	}
	addrs, err := n.dnsRouter.Lookup(ctx, host, adapter.DNSQueryOptions{})
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		if !blockedProbeAddr(addr) {
			// First usable answer, matching the order a dialer would try them.
			return addr.String(), nil
		}
	}
	return "", errProbeBlockedTarget
}

func clampProbeMs(ms float64) uint16 {
	if ms < 0 {
		return 0
	}
	if ms > 65535 {
		return 65535
	}
	return uint16(ms)
}
