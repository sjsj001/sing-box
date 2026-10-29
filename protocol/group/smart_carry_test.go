package group

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing-box/route"
	"github.com/sagernet/sing/common/auth"
	"github.com/sagernet/sing/common/bufio"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/cryptobyte"
)

func goClientHello(t *testing.T) []byte {
	t.Helper()
	client, server := net.Pipe()
	defer server.Close()
	go tls.Client(client, &tls.Config{ServerName: "example.com", InsecureSkipVerify: true}).Handshake()
	buffer := make([]byte, 64<<10)
	n, err := server.Read(buffer)
	require.NoError(t, err)
	client.Close()
	return buffer[:n]
}

func craftedHello(extensions ...uint16) []byte {
	var b cryptobyte.Builder
	b.AddUint8(0x16)
	b.AddUint16(0x0301)
	b.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) {
		b.AddUint8(1)
		b.AddUint24LengthPrefixed(func(b *cryptobyte.Builder) {
			b.AddUint16(0x0303)
			b.AddBytes(make([]byte, 32))
			b.AddUint8LengthPrefixed(func(*cryptobyte.Builder) {})
			b.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) { b.AddUint16(0x1301) })
			b.AddUint8LengthPrefixed(func(b *cryptobyte.Builder) { b.AddUint8(0) })
			if extensions != nil {
				b.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) {
					for _, extension := range extensions {
						b.AddUint16(extension)
						b.AddUint16(0)
					}
				})
			}
		})
	})
	return b.BytesOrPanic()
}

func TestClassifyHello(t *testing.T) {
	t.Parallel()
	hello := goClientHello(t)
	cases := []struct {
		name    string
		opening []byte
		verdict helloVerdict
	}{
		{"go client hello", hello, helloCarriable},
		{"hello missing its last byte", hello[:len(hello)-1], helloIncomplete},
		{"record header only", hello[:5], helloIncomplete},
		{"first byte", hello[:1], helloIncomplete},
		{"hello with a byte after it", append(append([]byte(nil), hello...), 0x17), helloNotCarriable},
		{"no extensions", craftedHello(), helloCarriable},
		{"sni and groups", craftedHello(0, 10), helloCarriable},
		{"early data", craftedHello(0, 42, 10), helloNotCarriable},
		{"plain http", []byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"), helloNotCarriable},
		{"oversized record", []byte{0x16, 3, 1, 0x50, 0x00}, helloNotCarriable},
		{"server hello", func() []byte { b := craftedHello(); b[5] = 2; return b }(), helloNotCarriable},
		{"sslv2 hello", []byte{0x80, 0x2e, 0x01, 0x00, 0x02}, helloNotCarriable},
	}
	for _, c := range cases {
		n, verdict := classifyHello(c.opening)
		require.Equal(t, c.verdict, verdict, c.name)
		if verdict == helloCarriable {
			require.Equal(t, len(c.opening), n, c.name)
		}
	}
}

// recordingConn keeps every write and answers once its gate opens.
type recordingConn struct {
	answeringConn
	gate   <-chan struct{}
	access sync.Mutex
	writes [][]byte
}

func (c *recordingConn) Write(p []byte) (int, error) {
	c.access.Lock()
	c.writes = append(c.writes, append([]byte(nil), p...))
	c.access.Unlock()
	return len(p), nil
}

func (c *recordingConn) written() [][]byte {
	c.access.Lock()
	defer c.access.Unlock()
	return append([][]byte(nil), c.writes...)
}

func (c *recordingConn) Measure(ctx context.Context) (naive.ConnMeasurement, error) {
	select {
	case <-c.gate:
		return c.measurement.Validated(), nil
	case <-ctx.Done():
		return naive.ConnMeasurement{}, ctx.Err()
	}
}

type recordingOutbound struct {
	adapter.Outbound
	measurement naive.ConnMeasurement
	answer      <-chan struct{}
	dial        <-chan struct{}
	access      sync.Mutex
	handed      []*recordingConn
}

func (o *recordingOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *recordingOutbound) DialContext(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
	if o.dial != nil {
		select {
		case <-o.dial:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	conn := &recordingConn{answeringConn: answeringConn{measurement: o.measurement}, gate: o.answer}
	o.access.Lock()
	o.handed = append(o.handed, conn)
	o.access.Unlock()
	return conn, nil
}

func (o *recordingOutbound) conns() []*recordingConn {
	o.access.Lock()
	defer o.access.Unlock()
	return append([]*recordingConn(nil), o.handed...)
}

type carryFixture struct {
	smart     *Smart
	outbounds map[string]*recordingOutbound
	answer    chan struct{}
	dial      chan struct{}
}

// newCarryFixture races three groups; b scores best (0.7*40+5 = 33).
func newCarryFixture(t *testing.T, holdDials bool) *carryFixture {
	t.Helper()
	f := &carryFixture{outbounds: map[string]*recordingOutbound{}, answer: make(chan struct{})}
	if holdDials {
		f.dial = make(chan struct{})
	}
	s := &Smart{
		ctx: context.Background(), logger: logger.NOP(), alpha: testAlpha,
		cache:               newSmartCache(context.Background(), "smart"),
		raceIgnoresDeadline: true,
	}
	legs := map[string][2]time.Duration{"a": {ms(20), ms(60)}, "b": {ms(40), ms(5)}, "c": {ms(90), ms(5)}}
	for _, tag := range []string{"a", "b", "c"} {
		out := &recordingOutbound{measurement: answering(legs[tag][0], legs[tag][1]), answer: f.answer}
		if holdDials {
			out.dial = f.dial
		}
		f.outbounds[tag] = out
		member := newMember(tag+"-1", legs[tag][0])
		member.outbound = out
		s.groups = append(s.groups, &smartGroup{tag: tag, members: []*smartMember{member}})
	}
	f.smart = s
	return f
}

func (f *carryFixture) dialRace(t *testing.T, host string) (net.Conn, string) {
	t.Helper()
	destination := M.ParseSocksaddr(host + ":443")
	key := destinationKey(destination)
	conn, err := f.smart.raceOrWait(context.Background(), key, destination, nil)
	require.NoError(t, err)
	return conn, key
}

// waitWinner reads once: it returns only when the race has decided.
func waitWinner(t *testing.T, conn net.Conn) {
	t.Helper()
	_, err := conn.Read(make([]byte, 16))
	require.ErrorIs(t, err, io.EOF)
}

// awaitHello waits until every group's probe holds the hello, so the race
// cannot decide before a probe the scheduler has not run yet attaches.
func (f *carryFixture) awaitHello(t *testing.T, hello []byte) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, out := range f.outbounds {
			conns := out.conns()
			if len(conns) != 1 || len(conns[0].written()) != 1 || !bytes.Equal(conns[0].written()[0], hello) {
				return false
			}
		}
		return true
	}, 2*time.Second, time.Millisecond, "every probe has the hello")
}

// requireOnly checks what each group's single probe was sent: the winner
// exactly winnerGot, every loser exactly loserGot.
func (f *carryFixture) requireOnly(t *testing.T, winner string, winnerGot [][]byte, loserGot [][]byte) {
	t.Helper()
	for tag, out := range f.outbounds {
		conns := out.conns()
		require.Len(t, conns, 1, "one probe per group, no failover dial")
		want := loserGot
		if tag == winner {
			want = winnerGot
		}
		require.Eventually(t, func() bool {
			return len(conns[0].written()) == len(want)
		}, time.Second, time.Millisecond, tag)
		require.Equal(t, want, conns[0].written(), tag)
		if tag != winner {
			require.Eventually(t, func() bool { return conns[0].closed.Load() > 0 }, time.Second, time.Millisecond, "loser "+tag+" closed")
		}
	}
}

func TestARaceCarriesTheHelloOnEveryProbeOnce(t *testing.T) {
	t.Parallel()
	f := newCarryFixture(t, false)
	hello := goClientHello(t)
	conn, _ := f.dialRace(t, "carry.example.com")
	defer conn.Close()

	// Split as a snell record boundary might split it.
	_, err := conn.Write(hello[:100])
	require.NoError(t, err)
	_, err = conn.Write(hello[100:])
	require.NoError(t, err)
	f.awaitHello(t, hello)
	close(f.answer)
	waitWinner(t, conn)

	request := []byte("application data")
	_, err = conn.Write(request)
	require.NoError(t, err)
	f.requireOnly(t, "b", [][]byte{hello, request}, [][]byte{hello})
}

func TestAHelloOfferedBeforeAnyProbeIsUpStillReachesThem(t *testing.T) {
	t.Parallel()
	f := newCarryFixture(t, true)
	hello := goClientHello(t)
	conn, _ := f.dialRace(t, "early.example.com")
	defer conn.Close()

	_, err := conn.Write(hello)
	require.NoError(t, err)
	close(f.dial)
	f.awaitHello(t, hello)
	close(f.answer)
	waitWinner(t, conn)
	f.requireOnly(t, "b", [][]byte{hello}, [][]byte{hello})
}

func TestAnOpeningThatIsNotACarriableHelloGoesOnlyToTheWinner(t *testing.T) {
	t.Parallel()
	for name, opening := range map[string][]byte{
		"http":       []byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"),
		"early data": craftedHello(0, 42),
	} {
		f := newCarryFixture(t, false)
		conn, _ := f.dialRace(t, "plain.example.com")
		_, err := conn.Write(opening)
		require.NoError(t, err, name)
		close(f.answer)
		waitWinner(t, conn)
		f.requireOnly(t, "b", [][]byte{opening}, nil)
		conn.Close()
	}
}

func TestAHelloAfterTheRaceGoesOnlyToTheWinner(t *testing.T) {
	t.Parallel()
	f := newCarryFixture(t, false)
	hello := goClientHello(t)
	conn, _ := f.dialRace(t, "late.example.com")
	defer conn.Close()

	close(f.answer)
	waitWinner(t, conn)
	_, err := conn.Write(hello)
	require.NoError(t, err)
	f.requireOnly(t, "b", [][]byte{hello}, nil)
}

func TestClosingBeforeTheRaceDecidesLeavesNoTunnelOpen(t *testing.T) {
	t.Parallel()
	f := newCarryFixture(t, false)
	conn, key := f.dialRace(t, "gone.example.com")
	_, err := conn.Write(goClientHello(t))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	close(f.answer)

	require.Eventually(t, func() bool {
		for _, out := range f.outbounds {
			for _, c := range out.conns() {
				if c.closed.Load() == 0 {
					return false
				}
			}
		}
		return true
	}, 2*time.Second, time.Millisecond, "every probe closed, the winner included")
	require.Eventually(t, func() bool {
		entry := f.smart.cache.load(key)
		return entry != nil && entry.measured()
	}, 2*time.Second, time.Millisecond, "the round the caller left still decides the destination")
}

func TestARaceThatReachesNothingFailsTheConnection(t *testing.T) {
	t.Parallel()
	s, _ := unreachableSmart("a", "b")
	destination := M.ParseSocksaddr("nowhere.example.com:443")
	conn, err := s.raceOrWait(context.Background(), destinationKey(destination), destination, nil)
	require.NoError(t, err, "the race runs behind the connection")
	_, _ = conn.Write(goClientHello(t))
	_, err = conn.Read(make([]byte, 16))
	require.ErrorIs(t, err, naive.ErrDestinationUnreachable)
	_, err = conn.Write([]byte("more"))
	require.ErrorIs(t, err, naive.ErrDestinationUnreachable)
	require.NoError(t, conn.Close())
}

func TestABigOpeningWaitsForTheWinner(t *testing.T) {
	t.Parallel()
	f := newCarryFixture(t, false)
	conn, _ := f.dialRace(t, "bulk.example.com")
	defer conn.Close()
	opening := bytes.Repeat([]byte{1}, replayBufferLimit+1)
	written := make(chan error, 1)
	go func() {
		_, err := conn.Write(opening)
		written <- err
	}()
	select {
	case <-written:
		t.Fatal("a write past the buffer returned before there was a winner to take it")
	case <-time.After(20 * time.Millisecond):
	}
	close(f.answer)
	require.NoError(t, <-written)
	f.requireOnly(t, "b", [][]byte{opening}, nil)
}

func TestNothingIsCarriedOnceTheRaceHasDecided(t *testing.T) {
	t.Parallel()
	hello := goClientHello(t)

	winner, straggler := &recordingConn{}, &recordingConn{}
	var carrier helloCarrier
	carrier.attach(winner)
	carrier.offer(hello)
	carried, err := carrier.decide(winner)
	require.NoError(t, err)
	require.Equal(t, len(hello), carried)
	carrier.attach(straggler)

	undecided := &recordingConn{}
	var late helloCarrier
	late.attach(undecided)
	carried, err = late.decide(undecided)
	require.NoError(t, err)
	require.Zero(t, carried)
	late.offer(hello)

	time.Sleep(10 * time.Millisecond)
	require.Equal(t, [][]byte{hello}, winner.written())
	require.Empty(t, straggler.written(), "a probe attaching after the decision is a straggler")
	require.Empty(t, undecided.written(), "a hello offered after the decision belongs to the winner's flush")
}

// unmeasuredOutbound hands out tunnels that cannot report a measurement.
type unmeasuredOutbound struct {
	adapter.Outbound
	dial <-chan struct{}
	conn *plainRecordingConn
}

type plainRecordingConn struct {
	net.Conn
	writes atomic.Int32
	closed atomic.Int32
}

func (c *plainRecordingConn) Write(p []byte) (int, error) {
	c.writes.Add(1)
	return len(p), nil
}

func (c *plainRecordingConn) Close() error { c.closed.Add(1); return nil }

func (o *unmeasuredOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *unmeasuredOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	<-o.dial
	return o.conn, nil
}

func TestATunnelThatCannotReportIsNotHandedTheHello(t *testing.T) {
	t.Parallel()
	// Such a tunnel drops out of the round as soon as it is dialed, so the
	// destination must not have been sent a handshake through it first.
	f := newCarryFixture(t, true)
	plain := &unmeasuredOutbound{dial: f.dial, conn: &plainRecordingConn{}}
	member := newMember("d-1", ms(10))
	member.outbound = plain
	f.smart.groups = append(f.smart.groups, &smartGroup{tag: "d", members: []*smartMember{member}})

	conn, _ := f.dialRace(t, "unmeasured.example.com")
	defer conn.Close()
	_, err := conn.Write(goClientHello(t))
	require.NoError(t, err)
	close(f.dial)
	// Undecided until it has dropped out, so the hello was there to hand it.
	require.Eventually(t, func() bool { return plain.conn.closed.Load() > 0 }, 2*time.Second, time.Millisecond)
	close(f.answer)
	waitWinner(t, conn)
	time.Sleep(20 * time.Millisecond)
	require.Zero(t, plain.conn.writes.Load())
}

func TestARacedTunnelReadWaitsOnASmallBuffer(t *testing.T) {
	t.Parallel()
	// The copy loop creates its waiter before the race has a winner.
	conn := &raceConn{carrier: &helloCarrier{}, cancel: func() {}, ready: make(chan struct{})}
	waiter, isWaiter := bufio.CreateReadWaiter(bufio.NewExtendedReader(conn))
	require.True(t, isWaiter)
	require.False(t, waiter.InitializeReadWaiter(N.ReadWaitOptions{MTU: 16383, FrontHeadroom: 23, RearHeadroom: 16}))

	tunnel, far := net.Pipe()
	defer far.Close()
	winner := &replayConn{Conn: tunnel, member: newMember("a-1", ms(30)), settled: true}
	go conn.install(winner, tunnel, nil)
	go far.Write([]byte("hello"))
	buffer, err := waiter.WaitReadBuffer()
	require.NoError(t, err)
	require.Equal(t, "hello", string(buffer.Bytes()))
	require.LessOrEqual(t, buffer.RawCap(), idleReadBuffer)
	buffer.Release()
	require.NoError(t, conn.Close())
}

// relayTunnel is a raced tunnel as a relay's next hop: it answers the CONNECT
// with an ack, sends the server's first flight, then stays open.
type relayTunnel struct {
	recordingConn
	ack   naive.ConnectAck
	reply []byte
	read  atomic.Bool
	gone  chan struct{}
	once  sync.Once
}

func (c *relayTunnel) RemoteAck(context.Context) (naive.ConnectAck, bool, error) {
	return c.ack, true, nil
}

func (c *relayTunnel) Read(p []byte) (int, error) {
	if !c.read.Swap(true) {
		return copy(p, c.reply), nil
	}
	<-c.gone
	return 0, io.EOF
}

func (c *relayTunnel) Close() error {
	c.once.Do(func() { close(c.gone) })
	return c.recordingConn.Close()
}

func (c *relayTunnel) bytes() []byte {
	return bytes.Join(c.written(), nil)
}

type relayOutbound struct {
	adapter.Outbound
	measurement naive.ConnMeasurement
	ack         naive.ConnectAck
	answer      <-chan struct{}
	handed      atomic.Pointer[relayTunnel]
}

func (o *relayOutbound) Network() []string { return []string{N.NetworkTCP} }

func (o *relayOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	conn := &relayTunnel{
		recordingConn: recordingConn{answeringConn: answeringConn{measurement: o.measurement}, gate: o.answer},
		ack:           o.ack, reply: []byte("server flight"), gone: make(chan struct{}),
	}
	o.handed.Store(conn)
	return conn, nil
}

// relayRouter routes every connection a naive inbound accepts to one outbound,
// through the connection manager a box uses.
type relayRouter struct {
	adapter.Router
	manager  *route.ConnectionManager
	outbound N.Dialer
}

func (r relayRouter) RouteConnectionEx(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	r.manager.NewConnection(ctx, r.outbound, conn, metadata, onClose)
}

// serveRelay puts a naive inbound in front of outbound, on h2 without TLS.
func serveRelay(t *testing.T, outbound N.Dialer) string {
	t.Helper()
	router := relayRouter{manager: route.NewConnectionManager(context.Background(), logger.NOP()), outbound: outbound}
	inbound, err := naive.NewInbound(context.Background(), router, logger.NOP(), "relay", option.NaiveInboundOptions{
		Users:   []auth.User{{Username: "user", Password: "secret"}},
		Network: option.NetworkList(N.NetworkTCP),
	})
	require.NoError(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	server := &http.Server{Handler: inbound.(http.Handler), Protocols: &protocols}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	return listener.Addr().String()
}

// padded frames one of the first writes either way of a naive tunnel.
func padded(p []byte) []byte {
	return append(binary.BigEndian.AppendUint16(nil, uint16(len(p))), append([]byte{0}, p...)...)
}

// connectThrough opens a smart client's CONNECT to the relay and writes hello
// up the tunnel before the answer, as cronet does. The answer arrives on the
// channel once the relay has one.
func connectThrough(t *testing.T, relay string, hello []byte) (<-chan *http.Response, *io.PipeWriter) {
	t.Helper()
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	transport := &http.Transport{Protocols: &protocols}
	t.Cleanup(transport.CloseIdleConnections)
	upload, uploadWriter := io.Pipe()
	t.Cleanup(func() { uploadWriter.Close() })
	request, err := http.NewRequest(http.MethodConnect, "http://"+relay, upload)
	require.NoError(t, err)
	request.Host = "relayed.example.com:443"
	request.Header.Set("Padding", "x")
	request.Header.Set("-smart", "1")
	request.Header.Set("Proxy-Authorization", "Basic dXNlcjpzZWNyZXQ=")
	go uploadWriter.Write(padded(hello))
	responses := make(chan *http.Response, 1)
	go func() {
		response, err := transport.RoundTrip(request)
		if err != nil {
			t.Error(err)
			close(responses)
			return
		}
		t.Cleanup(func() { response.Body.Close() })
		responses <- response
	}()
	return responses, uploadWriter
}

func readPadded(t *testing.T, reader io.Reader) []byte {
	t.Helper()
	header := make([]byte, 3)
	_, err := io.ReadFull(reader, header)
	require.NoError(t, err)
	length := int(binary.BigEndian.Uint16(header))
	payload := make([]byte, length+int(header[2]))
	_, err = io.ReadFull(reader, payload)
	require.NoError(t, err)
	return payload[:length]
}

func TestARelayRacesWithTheHelloAndAnswersWithTheWinner(t *testing.T) {
	t.Parallel()
	// The chain the user runs: a client's naive CONNECT to a relay whose
	// outbound is smart. The client's ClientHello reaches every group's exit
	// once, the CONNECT is answered with the winner's ack, and what the client
	// sends after the answer reaches the winner only.
	answer := make(chan struct{})
	s := &Smart{
		ctx: context.Background(), logger: logger.NOP(), alpha: testAlpha,
		cache:               newSmartCache(context.Background(), "smart"),
		raceIgnoresDeadline: true,
	}
	// b scores 0.7*40+5 = 33 against a's 74, and reports a connect of 4ms.
	outbounds := map[string]*relayOutbound{}
	for _, tag := range []string{"a", "b"} {
		leg := map[string][3]time.Duration{"a": {ms(20), ms(60), ms(6)}, "b": {ms(40), ms(5), ms(4)}}[tag]
		out := &relayOutbound{
			measurement: answering(leg[0], leg[1]),
			ack:         naive.ConnectAck{Total: leg[1] + ms(1), Connect: leg[2], HasConnect: true},
			answer:      answer,
		}
		outbounds[tag] = out
		member := newMember(tag+"-1", leg[0])
		member.outbound = out
		s.groups = append(s.groups, &smartGroup{tag: tag, members: []*smartMember{member}})
	}
	hello := goClientHello(t)
	responses, upload := connectThrough(t, serveRelay(t, s), hello)

	require.Eventually(t, func() bool {
		for _, out := range outbounds {
			if conn := out.handed.Load(); conn == nil || !bytes.Equal(conn.bytes(), hello) {
				return false
			}
		}
		return true
	}, 2*time.Second, time.Millisecond, "every exit has the hello before any has answered")
	close(answer)

	response := <-responses
	require.NotNil(t, response)
	require.Equal(t, http.StatusOK, response.StatusCode)
	ack, hasAck := naive.ParseConnectAck(response.Header.Get(naive.ConnectAckHeader))
	require.True(t, hasAck)
	require.True(t, ack.HasConnect)
	require.Equal(t, ms(4), ack.Connect, "the winner's connect, passed through")
	require.Positive(t, ack.Total, "the relay's own span, measured to the winner's answer")
	require.Equal(t, "server flight", string(readPadded(t, response.Body)))

	request := []byte("GET / HTTP/1.1\r\n\r\n")
	_, err := upload.Write(padded(request))
	require.NoError(t, err)
	winner, loser := outbounds["b"].handed.Load(), outbounds["a"].handed.Load()
	require.Eventually(t, func() bool {
		return bytes.Equal(winner.bytes(), append(append([]byte(nil), hello...), request...))
	}, 2*time.Second, time.Millisecond, "the winner has the hello and what followed it")
	require.Equal(t, hello, loser.bytes(), "the loser had the hello once and nothing after")
	require.Positive(t, loser.closed.Load())
}

func TestARelayWhoseGroupsAllRefusedAnswers502(t *testing.T) {
	t.Parallel()
	s, _ := unreachableSmart("a", "b")
	responses, _ := connectThrough(t, serveRelay(t, s), goClientHello(t))
	response := <-responses
	require.NotNil(t, response)
	require.Equal(t, http.StatusBadGateway, response.StatusCode,
		"read after the race, the verdict still reaches the client as a verdict about the destination")
}
