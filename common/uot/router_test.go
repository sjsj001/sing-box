package uot

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/uot"

	"github.com/stretchr/testify/require"
)

// countingConn stands in for a naive HTTP/2 stream: no vectorised writer,
// every Write is one stream write and every Read one request body read.
type countingConn struct {
	net.Conn
	writes atomic.Int64
	reads  atomic.Int64
}

func (c *countingConn) Write(p []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(p)
}

func (c *countingConn) Read(p []byte) (int, error) {
	c.reads.Add(1)
	return c.Conn.Read(p)
}

var testDestination = M.ParseSocksaddr("1.1.1.1:443")

func newTestPair(t *testing.T, isConnect bool) (*countingConn, *uot.Conn) {
	t.Helper()
	server, client := net.Pipe()
	t.Cleanup(func() { server.Close(); client.Close() })
	request := uot.Request{IsConnect: isConnect, Destination: testDestination}
	return &countingConn{Conn: server}, uot.NewConn(client, request)
}

func packet(t *testing.T, seq int, size int) *buf.Buffer {
	t.Helper()
	b := buf.NewSize(size)
	payload := b.Extend(size)
	payload[0] = byte(seq)
	payload[1] = byte(seq >> 8)
	return b
}

func TestServerConnReleasesWrittenPackets(t *testing.T) {
	stream, client := newTestPair(t, false)
	conn := newServerConn(stream, uot.Request{Destination: testDestination})
	go func() {
		b := buf.NewPacket()
		defer b.Release()
		_, _ = client.ReadPacket(b)
	}()
	b := packet(t, 0, 1200)
	require.NoError(t, conn.WritePacket(b, testDestination))
	require.Zero(t, b.Cap(), "the packet goes back to the pool once written")
}

func TestServerConnCoalescesInOrder(t *testing.T) {
	for _, isConnect := range []bool{false, true} {
		stream, client := newTestPair(t, isConnect)
		conn := newServerConn(stream, uot.Request{IsConnect: isConnect, Destination: testDestination})
		const count = 200
		done := make(chan error, 1)
		go func() {
			for i := range count {
				if err := conn.WritePacket(packet(t, i, 1200+i%100), testDestination); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}()
		for i := range count {
			b := buf.NewPacket()
			destination, err := client.ReadPacket(b)
			require.NoError(t, err)
			require.Equal(t, testDestination, destination)
			require.Equal(t, 1200+i%100, b.Len())
			require.Equal(t, i, int(b.Byte(0))|int(b.Byte(1))<<8, "packet %d out of order", i)
			b.Release()
			if i%20 == 0 {
				time.Sleep(time.Millisecond) // a slow stream: packets pile up behind the write in flight
			}
		}
		require.NoError(t, <-done)
		require.Less(t, stream.writes.Load(), int64(count), "packets queued behind a write go out together")
	}
}

func TestServerConnCarriesAFrameLargerThanTheStage(t *testing.T) {
	stream, client := newTestPair(t, false)
	conn := newServerConn(stream, uot.Request{Destination: testDestination})
	sizes := []int{1200, 0xffff, 1300, 0xffff - 7, 1400}
	go func() {
		for i, size := range sizes {
			_ = conn.WritePacket(packet(t, i, size), testDestination)
		}
	}()
	for i, size := range sizes {
		b := buf.NewSize(0xffff)
		_, err := client.ReadPacket(b)
		require.NoError(t, err)
		require.Equal(t, size, b.Len())
		require.Equal(t, i, int(b.Byte(0)), "packet %d out of order", i)
		b.Release()
	}
}

func TestServerConnReadsAFrameInOneStreamRead(t *testing.T) {
	stream, client := newTestPair(t, false)
	conn := newServerConn(stream, uot.Request{Destination: testDestination})
	const count = 50
	go func() {
		for i := range count {
			_ = client.WritePacket(packet(t, i, 100), testDestination)
		}
	}()
	for i := range count {
		b := buf.NewPacket()
		destination, err := conn.ReadPacket(b)
		require.NoError(t, err)
		require.Equal(t, testDestination, destination)
		require.Equal(t, i, int(b.Byte(0)), "packet %d out of order", i)
		b.Release()
	}
	require.LessOrEqual(t, stream.reads.Load(), int64(count), "address, length and payload come from one read")
}

func TestServerConnCloseUnblocksWriters(t *testing.T) {
	stream, _ := newTestPair(t, false) // nobody reads: the flusher blocks in its write
	conn := newServerConn(stream, uot.Request{Destination: testDestination})
	done := make(chan error, 1)
	go func() {
		for i := 0; ; i++ {
			if err := conn.WritePacket(packet(t, i, 1200), testDestination); err != nil {
				done <- err
				return
			}
		}
	}()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, conn.Close())
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("a writer waiting for staging space is still blocked after Close")
	}
}

func TestServerConnKeepsVectorisedStreams(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	go func() {
		c, _ := listener.Accept()
		if c != nil {
			defer c.Close()
		}
	}()
	tcpConn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer tcpConn.Close()
	_, isPlain := newServerConn(tcpConn, uot.Request{}).(*uot.Conn)
	require.True(t, isPlain, "a TCP stream keeps sing's writev path")
}

// reporterConn stands in for the naive inbound's stream, which answers a
// deferred CONNECT when route reports the outcome through the Upstream chain.
type reporterConn struct {
	net.Conn
	succeeded bool
}

func (c *reporterConn) HandshakeSuccess() error          { c.succeeded = true; return nil }
func (c *reporterConn) HandshakeFailure(err error) error { return nil }

func TestServerConnKeepsTheHandshakeReporterInReach(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() { server.Close(); client.Close() })
	stream := &reporterConn{Conn: server}
	conn := newServerConn(stream, uot.Request{Destination: testDestination})
	_, isWrapped := conn.(*serverConn)
	require.True(t, isWrapped)
	require.NoError(t, N.ReportPacketConnHandshakeSuccess(conn, nil))
	require.True(t, stream.succeeded, "route's report reaches the inbound through the wrappers")
}
