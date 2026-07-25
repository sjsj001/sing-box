//go:build with_naive_outbound

package main

import (
	"context"
	"io"
	"net"
	"net/netip"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/group"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	socks "github.com/sagernet/sing/protocol/socks"

	"github.com/stretchr/testify/require"
)

// smart_fault_test.go is a cross-platform, deterministic fault-injection harness
// for the smart group's health machinery — the runnable equivalent of the old
// branch's nft-based sim rig (link-death / reset / black-hole scenarios), for a
// machine without netfilter. A controllable TCP forwarder sits in front of one
// member's naive server and, on command, resets or black-holes its connections;
// the test asserts the group demotes that member, keeps serving via the healthy
// member, and — the part no other test covers — READMITS the member once the
// fault clears, exercising the G3 recovery streak / flap accounting.
//
// NOTE: this validates the failure LOGIC end to end. Calibrating the concrete
// thresholds (ping/verdict/backoff timings) still needs a real degraded link
// with genuine buffer bloat, as recorded in the project plan.

const (
	faultBackendPort uint16 = 10110 // the real naive server behind the forwarder
	faultFrontPort   uint16 = 10111 // the port the "flaky" member dials (the forwarder)
	faultHealthyPort uint16 = 10112 // a second, always-healthy naive server
	faultClientPort  uint16 = 10113 // mixed inbound socks port
	faultTargetPort  uint16 = 10114 // echo target
)

type faultMode int32

const (
	modeForward faultMode = iota
	modeReset
	modeBlackhole
)

// faultForwarder is a byte-level TCP relay whose behaviour is switchable at
// runtime. In modeForward it transparently proxies to the backend; in modeReset
// it RSTs new connections and tears down live ones; in modeBlackhole it accepts
// but never responds (and drops live ones), so the peer's handshake hangs.
type faultForwarder struct {
	backend string
	mode    atomic.Int32
	mu      sync.Mutex
	conns   map[net.Conn]struct{}
}

func startFaultForwarder(t *testing.T, listenPort, backendPort uint16) *faultForwarder {
	t.Helper()
	f := &faultForwarder{
		backend: net.JoinHostPort("127.0.0.1", strconv.Itoa(int(backendPort))),
		conns:   make(map[net.Conn]struct{}),
	}
	ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(int(listenPort))))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go f.handle(conn)
		}
	}()
	return f
}

// set switches the mode and, for a fault mode, tears down all live relays so the
// change takes effect on existing connections immediately, not just new ones.
func (f *faultForwarder) set(m faultMode) {
	f.mode.Store(int32(m))
	if m == modeForward {
		return
	}
	f.mu.Lock()
	for c := range f.conns {
		_ = c.Close()
	}
	f.mu.Unlock()
}

func (f *faultForwarder) track(c net.Conn)   { f.mu.Lock(); f.conns[c] = struct{}{}; f.mu.Unlock() }
func (f *faultForwarder) untrack(c net.Conn) { f.mu.Lock(); delete(f.conns, c); f.mu.Unlock() }

func (f *faultForwarder) handle(client net.Conn) {
	switch faultMode(f.mode.Load()) {
	case modeReset:
		if tc, ok := client.(*net.TCPConn); ok {
			_ = tc.SetLinger(0) // close sends RST, not FIN → a hard connection reset
		}
		_ = client.Close()
		return
	case modeBlackhole:
		// Read and discard forever, never write: the peer's TLS handshake / probe
		// gets no response and hangs until it times out.
		f.track(client)
		go func() { _, _ = io.Copy(io.Discard, client); f.untrack(client); _ = client.Close() }()
		return
	}
	backend, err := net.Dial("tcp", f.backend)
	if err != nil {
		_ = client.Close()
		return
	}
	f.track(client)
	go f.relay(client, backend)
}

// relay proxies bytes both ways with half-close (CloseWrite) semantics, so one
// direction ending does not RST the other — essential for cronet's long-lived
// TLS/h2 mux connections, which stay open across many streams. Both connections
// are fully closed only once both directions have ended (or set() tore them
// down for a fault).
func (f *faultForwarder) relay(client, backend net.Conn) {
	done := make(chan struct{}, 2)
	cp := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		if tc, ok := dst.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
		done <- struct{}{}
	}
	go cp(backend, client)
	go cp(client, backend)
	<-done
	<-done
	_ = client.Close()
	_ = backend.Close()
	f.untrack(client)
}

// startEchoServer runs a loop-accepting echo server on port (robust to the smart
// group's bare first-connect probe) until the test ends.
func startEchoServer(t *testing.T, port uint16) {
	t.Helper()
	l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(int(port))))
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(conn, conn); _ = conn.Close() }()
		}
	}()
}

// echoOnce performs one echo exchange through the socks proxy and returns an
// error instead of failing the test, so callers can retry (the "1 blip on
// failover" is expected: naive dials optimistically, so a failing member's death
// surfaces on the first request's read, and the NEXT request recovers).
func echoOnce(clientPort, targetPort uint16) error {
	dialer := socks.NewClient(N.SystemDialer, M.ParseSocksaddrHostPort("127.0.0.1", clientPort), socks.Version5, "", "")
	conn, err := dialer.DialContext(context.Background(), "tcp", M.ParseSocksaddrHostPort("127.0.0.1", targetPort))
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err = conn.Write([]byte("hello")); err != nil {
		return err
	}
	buf := make([]byte, 5)
	if _, err = io.ReadFull(conn, buf); err != nil {
		return err
	}
	if string(buf) != "hello" {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// assertEchoRecovers asserts traffic flows (again) within the timeout, retrying
// to tolerate the expected single failed request at the moment of failover.
func assertEchoRecovers(t *testing.T, clientPort, targetPort uint16, timeout time.Duration, msg string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return echoOnce(clientPort, targetPort) == nil
	}, timeout, 400*time.Millisecond, msg)
}

// nodeHealthy reports whether a member is currently healthy with a measured
// local baseline.
func nodeHealthy(sm *group.Smart, tag string) bool {
	for _, n := range sm.DebugNodes() {
		if n.Tag == tag {
			return n.Healthy && n.BaselineMs >= 0
		}
	}
	return false
}

func nodeDown(sm *group.Smart, tag string) bool {
	for _, n := range sm.DebugNodes() {
		if n.Tag == tag {
			return !n.Healthy
		}
	}
	return false
}

func runSmartFaultScenario(t *testing.T, injected faultMode, name string) {
	caPem, certPem, keyPem := createSelfSignedCertificate(t, "example.org")
	caPemContent, err := os.ReadFile(caPem)
	require.NoError(t, err)
	caStr := string(caPemContent)

	forwarder := startFaultForwarder(t, faultFrontPort, faultBackendPort)
	startEchoServer(t, faultTargetPort)

	instance := startInstance(t, option.Options{
		Inbounds: []option.Inbound{
			{
				Type: C.TypeMixed,
				Tag:  "mixed-in",
				Options: &option.HTTPMixedInboundOptions{
					ListenOptions: option.ListenOptions{
						Listen:     common.Ptr(badoption.Addr(netip.MustParseAddr("127.0.0.1"))),
						ListenPort: faultClientPort,
					},
				},
			},
			// The flaky member's server sits behind the forwarder; the healthy one is direct.
			smartNaiveServer(t, "naive-in-flaky", faultBackendPort, certPem, keyPem),
			smartNaiveServer(t, "naive-in-healthy", faultHealthyPort, certPem, keyPem),
		},
		Outbounds: []option.Outbound{
			{Type: C.TypeDirect, Tag: "direct-out"},
			smartNaiveClient("flaky", faultFrontPort, caStr),     // dials the forwarder
			smartNaiveClient("healthy", faultHealthyPort, caStr), // dials the server directly
			{
				Type: C.TypeSmart,
				Tag:  "smart",
				Options: &option.SmartOutboundOptions{
					GroupCommonOption: option.GroupCommonOption{
						Outbounds: []string{"flaky", "healthy"},
					},
					PingInterval: badoption.Duration(time.Second), // fast detection for the test
				},
			},
		},
		Route: smartFaultRoute(),
	})

	smartOut, ok := instance.Outbound().Outbound("smart")
	require.True(t, ok)
	sm := smartOut.(*group.Smart)

	// 1. Both members come up healthy and traffic flows.
	require.Eventually(t, func() bool {
		return nodeHealthy(sm, "flaky") && nodeHealthy(sm, "healthy")
	}, 20*time.Second, 300*time.Millisecond, "both members healthy at start")
	assertEchoRecovers(t, faultClientPort, faultTargetPort, 10*time.Second, "traffic flows at start")

	// 2. Inject the fault on the flaky member: it must be demoted while the
	//    healthy member stays up, and traffic keeps flowing through the survivor
	//    (after at most the one expected failover blip).
	forwarder.set(injected)
	require.Eventually(t, func() bool {
		return nodeDown(sm, "flaky") && nodeHealthy(sm, "healthy")
	}, 25*time.Second, 300*time.Millisecond, name+": flaky member demoted, healthy survives")
	assertEchoRecovers(t, faultClientPort, faultTargetPort, 15*time.Second, name+": traffic flows via the survivor")

	// 3. Clear the fault: the recovered member must be readmitted (G3 recovery
	//    streak) — it does not stay permanently blocked/flap-penalized.
	forwarder.set(modeForward)
	require.Eventually(t, func() bool {
		return nodeHealthy(sm, "flaky") && nodeHealthy(sm, "healthy")
	}, 30*time.Second, 300*time.Millisecond, name+": flaky member readmitted after recovery")
	assertEchoRecovers(t, faultClientPort, faultTargetPort, 10*time.Second, name+": traffic flows after recovery")
}

// TestSmartFaultReset injects hard connection resets (link-death via RST).
func TestSmartFaultReset(t *testing.T) {
	runSmartFaultScenario(t, modeReset, "reset")
}

// TestSmartFaultBlackhole injects a silent black-hole (no RST, connections hang)
// — the case the G2 handshake-verdict path exists for.
func TestSmartFaultBlackhole(t *testing.T) {
	runSmartFaultScenario(t, modeBlackhole, "blackhole")
}

func smartFaultRoute() *option.RouteOptions {
	return &option.RouteOptions{
		Rules: []option.Rule{
			{
				Type: C.RuleTypeDefault,
				DefaultOptions: option.DefaultRule{
					RawDefaultRule: option.RawDefaultRule{Inbound: []string{"mixed-in"}},
					RuleAction: option.RuleAction{
						Action:       C.RuleActionTypeRoute,
						RouteOptions: option.RouteActionOptions{Outbound: "smart"},
					},
				},
			},
		},
	}
}
