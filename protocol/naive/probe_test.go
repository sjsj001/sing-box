package naive

import (
	"context"
	"net/netip"
	"testing"

	"github.com/sagernet/sing-box/adapter"

	"github.com/stretchr/testify/require"
)

// stubDNSRouter answers Lookup and nothing else; the embedded nil interface
// makes any other call panic, which is the point — resolveProbeTarget must not
// reach for anything but the lookup.
type stubDNSRouter struct {
	adapter.DNSRouter
	addrs  []netip.Addr
	err    error
	asked  string
	called bool
}

func (s *stubDNSRouter) Lookup(_ context.Context, domain string, _ adapter.DNSQueryOptions) ([]netip.Addr, error) {
	s.called = true
	s.asked = domain
	return s.addrs, s.err
}

func addrs(in ...string) []netip.Addr {
	out := make([]netip.Addr, 0, len(in))
	for _, s := range in {
		out = append(out, netip.MustParseAddr(s))
	}
	return out
}

// The probe must resolve through the router's DNS, not the system resolver:
// with a geo-DNS name the two can disagree, and then the smart group would be
// ranking nodes by the latency to an address the proxied traffic never reaches.
func TestProbeResolvesThroughRouterDNS(t *testing.T) {
	dns := &stubDNSRouter{addrs: addrs("93.184.216.34")}
	n := &Inbound{dnsRouter: dns}

	target, err := n.resolveProbeTarget(context.Background(), "example.com")
	require.NoError(t, err)
	require.True(t, dns.called, "the router's DNS must be consulted")
	require.Equal(t, "example.com", dns.asked)
	require.Equal(t, "93.184.216.34", target, "and the probe dials what it answered")
}

// The SSRF screen has to survive the move: a name that resolves to internal
// space must fail before a SYN is sent, not merely at connect time.
func TestProbeRejectsInternalResolvedAddresses(t *testing.T) {
	for name, answer := range map[string][]netip.Addr{
		"loopback":    addrs("127.0.0.1"),
		"private":     addrs("10.0.0.5"),
		"link-local":  addrs("169.254.169.254"), // cloud metadata
		"all of them": addrs("127.0.0.1", "192.168.1.1"),
	} {
		t.Run(name, func(t *testing.T) {
			n := &Inbound{dnsRouter: &stubDNSRouter{addrs: answer}}
			_, err := n.resolveProbeTarget(context.Background(), "evil.example")
			require.ErrorIs(t, err, errProbeBlockedTarget)
		})
	}
}

// A mixed answer is usable: skip the internal addresses and probe the first
// global one, rather than failing the whole host.
func TestProbeSkipsInternalAndTakesFirstGlobal(t *testing.T) {
	n := &Inbound{dnsRouter: &stubDNSRouter{addrs: addrs("10.0.0.5", "93.184.216.34", "1.1.1.1")}}
	target, err := n.resolveProbeTarget(context.Background(), "mixed.example")
	require.NoError(t, err)
	require.Equal(t, "93.184.216.34", target)
}

func TestProbeLiteralAddresses(t *testing.T) {
	n := &Inbound{dnsRouter: &stubDNSRouter{}}

	target, err := n.resolveProbeTarget(context.Background(), "93.184.216.34")
	require.NoError(t, err)
	require.Equal(t, "93.184.216.34", target)
	require.False(t, n.dnsRouter.(*stubDNSRouter).called, "a literal needs no lookup")

	_, err = n.resolveProbeTarget(context.Background(), "127.0.0.1")
	require.ErrorIs(t, err, errProbeBlockedTarget)
}

// Without a router (a standalone inbound) the name goes to the dialer unchanged
// and the system resolver handles it — the Control guard still screens the
// result, so this stays safe, just less faithful.
func TestProbeFallsBackWithoutRouterDNS(t *testing.T) {
	n := &Inbound{}
	target, err := n.resolveProbeTarget(context.Background(), "example.com")
	require.NoError(t, err)
	require.Equal(t, "example.com", target)
}
