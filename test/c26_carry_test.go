//go:build with_naive_outbound

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/protocol/socks"

	"github.com/stretchr/testify/require"
)

func c26Port(t *testing.T) uint16 {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	return uint16(listener.Addr().(*net.TCPAddr).Port)
}

func c26DelayProxy(t *testing.T, target string, oneWay time.Duration) uint16 {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			upstream, err := net.Dial("tcp", target)
			if err != nil {
				conn.Close()
				continue
			}
			go c26DelayCopy(upstream, conn, oneWay)
			go c26DelayCopy(conn, upstream, oneWay)
		}
	}()
	return uint16(listener.Addr().(*net.TCPAddr).Port)
}

func c26DelayCopy(dst, src net.Conn, d time.Duration) {
	type chunk struct {
		b  []byte
		at time.Time
	}
	ch := make(chan chunk, 4096)
	go func() {
		defer close(ch)
		for {
			buf := make([]byte, 64<<10)
			n, err := src.Read(buf)
			if n > 0 {
				ch <- chunk{buf[:n], time.Now().Add(d)}
			}
			if err != nil {
				return
			}
		}
	}()
	for c := range ch {
		time.Sleep(time.Until(c.at))
		if _, err := dst.Write(c.b); err != nil {
			break
		}
	}
	dst.Close()
	src.Close()
}

// c26Sink is one group's destination: a TLS server that counts what reached
// it, by the first label of the SNI.
type c26Sink struct {
	port       uint16
	accepts    atomic.Int32
	access     sync.Mutex
	hellos     map[string]int
	handshakes map[string]int
	bare       int
	aborted    map[string]int64
}

type c26CountingConn struct {
	net.Conn
	written atomic.Int64
}

func (c *c26CountingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.written.Add(int64(n))
	return n, err
}

func startC26Sink(t *testing.T, certPem, keyPem string) *c26Sink {
	cert, err := tls.LoadX509KeyPair(certPem, keyPem)
	require.NoError(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	sink := &c26Sink{
		port:       uint16(listener.Addr().(*net.TCPAddr).Port),
		hellos:     map[string]int{},
		handshakes: map[string]int{},
		aborted:    map[string]int64{},
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			sink.accepts.Add(1)
			go func() {
				counting := &c26CountingConn{Conn: conn}
				var label string
				server := tls.Server(counting, &tls.Config{
					Certificates: []tls.Certificate{cert},
					GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
						label, _, _ = strings.Cut(hello.ServerName, "-")
						sink.access.Lock()
						sink.hellos[label]++
						sink.access.Unlock()
						return nil, nil
					},
				})
				_ = server.SetDeadline(time.Now().Add(10 * time.Second))
				err := server.Handshake()
				sink.access.Lock()
				switch {
				case err == nil:
					sink.handshakes[label]++
				case label == "":
					sink.bare++
				default:
					sink.aborted[label] += counting.written.Load()
				}
				sink.access.Unlock()
				if err == nil {
					buffer := make([]byte, 1)
					_, _ = server.Read(buffer)
				}
				server.Close()
			}()
		}
	}()
	return sink
}

func startC26Box(t *testing.T, config string) *box.Box {
	ctx, cancel := context.WithCancel(globalCtx)
	options, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(config))
	require.NoError(t, err)
	instance, err := box.New(box.Options{Context: ctx, Options: options})
	require.NoError(t, err)
	require.NoError(t, instance.Start())
	t.Cleanup(func() {
		instance.Close()
		cancel()
	})
	return instance
}

var c26Race = regexp.MustCompile(`race (\S+) .*?won=(\S+) in=(\S+) (.*)`)

// TestC26CarryHello races new hosts across three real naive relays, each
// behind its own delay, and checks what reached each relay's destination: the
// client's ClientHello once from every group, one completed handshake per
// host, and no bare connection. C26_SAMPLES and C26_DELAYS (one-way
// milliseconds per group) turn it into the local measurement of round 26;
// the comparison with payload-free races is binary against binary, on r25.
func TestC26CarryHello(t *testing.T) {
	samples := 30
	if value, err := strconv.Atoi(os.Getenv("C26_SAMPLES")); err == nil {
		samples = value
	}
	caPem, certPem, keyPem := createSelfSignedCertificate(t, "example.org")
	caPemContent, err := os.ReadFile(caPem)
	require.NoError(t, err)
	caJSON, _ := json.Marshal(string(caPemContent))

	type c26Group struct {
		tag    string
		oneWay time.Duration
	}
	groups := []c26Group{{"ga", 15 * time.Millisecond}, {"gb", 25 * time.Millisecond}, {"gc", 35 * time.Millisecond}}
	if value := os.Getenv("C26_DELAYS"); value != "" {
		groups = nil
		for i, field := range strings.Split(value, ",") {
			oneWay, err := strconv.Atoi(field)
			require.NoError(t, err)
			groups = append(groups, c26Group{fmt.Sprint("g", string(rune('a'+i))), time.Duration(oneWay) * time.Millisecond})
		}
	}

	dummy, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { dummy.Close() })
	go func() {
		for {
			conn, err := dummy.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()
	dummyPort := dummy.Addr().(*net.TCPAddr).Port

	logDir := t.TempDir()
	sinks := map[string]*c26Sink{}
	var relayInbounds, relayRules, clientOutbounds, smartGroups []string
	proxyPorts := map[string]uint16{}
	for _, g := range groups {
		sinks[g.tag] = startC26Sink(t, certPem, keyPem)
		relayPort := c26Port(t)
		relayInbounds = append(relayInbounds, fmt.Sprintf(`{"type":"naive","tag":"in-%s","listen":"127.0.0.1","listen_port":%d,
			"users":[{"username":"sekai","password":"password"}],"network":"tcp",
			"tls":{"enabled":true,"server_name":"example.org","certificate_path":%q,"key_path":%q}}`, g.tag, relayPort, certPem, keyPem))
		relayRules = append(relayRules, fmt.Sprintf(`{"inbound":["in-%s"],"domain_suffix":[".test"],"action":"route-options","override_address":"127.0.0.1","override_port":%d},
			{"inbound":["in-%s"],"ip_cidr":["127.0.0.1/32"],"action":"route","outbound":"direct"},
			{"inbound":["in-%s"],"action":"route-options","override_address":"127.0.0.1","override_port":%d}`, g.tag, sinks[g.tag].port, g.tag, g.tag, dummyPort))
		proxyPorts[g.tag] = c26DelayProxy(t, fmt.Sprint("127.0.0.1:", relayPort), g.oneWay)
	}
	startC26Box(t, fmt.Sprintf(`{"log":{"level":"debug","output":%q},"inbounds":[%s],"outbounds":[{"type":"direct","tag":"direct"},{"type":"block","tag":"block"}],
		"route":{"rules":[%s],"final":"direct"}}`, filepath.Join(logDir, "relay.log"), strings.Join(relayInbounds, ","), strings.Join(relayRules, ",")))

	for _, g := range groups {
		clientOutbounds = append(clientOutbounds, fmt.Sprintf(`{"type":"naive","tag":"n-%s","server":"127.0.0.1","server_port":%d,
			"username":"sekai","password":"password","tls":{"enabled":true,"server_name":"example.org","certificate":[%s]}}`, g.tag, proxyPorts[g.tag], caJSON))
		smartGroups = append(smartGroups, fmt.Sprintf(`{"tag":%q,"outbounds":["n-%s"]}`, g.tag, g.tag))
	}
	clientLog := filepath.Join(logDir, "client.log")
	port := c26Port(t)
	startC26Box(t, fmt.Sprintf(`{"log":{"level":"debug","output":%q},"inbounds":[{"type":"mixed","listen":"127.0.0.1","listen_port":%d}],
		"outbounds":[%s,{"type":"smart","tag":"smart","groups":[%s]}],"route":{"final":"smart"}}`,
		clientLog, port, strings.Join(clientOutbounds, ","), strings.Join(smartGroups, ",")))
	client := socks.NewClient(N.SystemDialer, M.ParseSocksaddrHostPort("127.0.0.1", port), socks.Version5, "", "")

	handshake := func(host string) (time.Duration, error) {
		start := time.Now()
		conn, err := client.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddrHostPort(host, 443))
		if err != nil {
			return 0, err
		}
		defer conn.Close()
		tlsConn := tls.Client(conn, &tls.Config{ServerName: host, InsecureSkipVerify: true})
		_ = tlsConn.SetDeadline(time.Now().Add(10 * time.Second))
		if err := tlsConn.Handshake(); err != nil {
			return 0, err
		}
		elapsed := time.Since(start)
		_, _ = tlsConn.Write([]byte{1})
		return elapsed, nil
	}

	// Sessions up on every member, as the heartbeat leaves them in production.
	for i := range 3 {
		_, err := handshake(fmt.Sprintf("warm-%d.test", i))
		require.NoError(t, err)
	}
	time.Sleep(500 * time.Millisecond)
	baseline := map[string][2]int{}
	for tag, sink := range sinks {
		sink.access.Lock()
		baseline[tag] = [2]int{int(sink.accepts.Load()), sink.bare}
		sink.access.Unlock()
	}

	var fresh, cached []time.Duration
	for i := range samples {
		elapsed, err := handshake(fmt.Sprintf("new-%d.test", i))
		require.NoError(t, err)
		fresh = append(fresh, elapsed)
		time.Sleep(300 * time.Millisecond)
	}
	time.Sleep(time.Second)
	var handshakes int
	for _, g := range groups {
		sink := sinks[g.tag]
		sink.access.Lock()
		t.Logf("sink %s: tcp=%d bare=%d hellos=%d handshakes=%d aborted_bytes=%d", g.tag,
			int(sink.accepts.Load())-baseline[g.tag][0], sink.bare-baseline[g.tag][1],
			sink.hellos["new"], sink.handshakes["new"], sink.aborted["new"])
		require.Equal(t, samples, sink.hellos["new"], "group %s: one ClientHello per new host, winning or not", g.tag)
		require.Equal(t, baseline[g.tag][1], sink.bare, "group %s: no probe reached the destination without the hello", g.tag)
		handshakes += sink.handshakes["new"]
		sink.access.Unlock()
	}
	require.Equal(t, samples, handshakes, "one completed handshake per host, on the winner")

	for i := range samples {
		elapsed, err := handshake(fmt.Sprintf("new-%d.test", i))
		require.NoError(t, err)
		cached = append(cached, elapsed)
	}

	quantile := func(values []time.Duration, q float64) time.Duration {
		sorted := append([]time.Duration(nil), values...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		return sorted[int(q*float64(len(sorted)-1)+0.5)].Round(100 * time.Microsecond)
	}
	t.Logf("new p50=%v p90=%v max=%v | cached p50=%v p90=%v",
		quantile(fresh, 0.5), quantile(fresh, 0.9), quantile(fresh, 1), quantile(cached, 0.5), quantile(cached, 0.9))

	content, err := os.ReadFile(clientLog)
	require.NoError(t, err)
	wins := map[string]int{}
	var raceIn []string
	for _, line := range strings.Split(string(content), "\n") {
		match := c26Race.FindStringSubmatch(line)
		if match == nil || !strings.HasPrefix(match[1], "new-") || strings.Contains(line, " settled ") {
			continue
		}
		wins[match[2]]++
		raceIn = append(raceIn, match[3])
	}
	t.Logf("races=%d wins=%v race_in p50=%v", len(raceIn), wins, c26Median(raceIn))
}

func c26Median(values []string) time.Duration {
	var parsed []time.Duration
	for _, value := range values {
		if d, err := time.ParseDuration(value); err == nil {
			parsed = append(parsed, d)
		}
	}
	if len(parsed) == 0 {
		return 0
	}
	sort.Slice(parsed, func(i, j int) bool { return parsed[i] < parsed[j] })
	return parsed[len(parsed)/2]
}
