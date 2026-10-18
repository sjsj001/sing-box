package snell

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

// recordingTunnel stands in for the service's packet connection: every call is
// one write of records onto the TCP stream, and each takes a while, so packets
// handed over meanwhile pile up.
type recordingTunnel struct {
	N.PacketConn
	access  sync.Mutex
	calls   int
	largest int
	got     [][2]int // sequence number and length, in the order written
	err     error
}

func (c *recordingTunnel) record(buffers []*buf.Buffer) error {
	time.Sleep(200 * time.Microsecond)
	c.access.Lock()
	defer c.access.Unlock()
	defer buf.ReleaseMulti(buffers)
	c.calls++
	c.largest = max(c.largest, len(buffers))
	if c.err != nil {
		return c.err
	}
	for _, b := range buffers {
		c.got = append(c.got, [2]int{int(b.Byte(0)) | int(b.Byte(1))<<8, b.Len()})
	}
	return nil
}

func (c *recordingTunnel) WritePacket(buffer *buf.Buffer, _ M.Socksaddr) error {
	return c.record([]*buf.Buffer{buffer})
}

func (c *recordingTunnel) WritePacketBatch(buffers []*buf.Buffer, _ []M.Socksaddr) error {
	return c.record(buffers)
}

func (c *recordingTunnel) written() int {
	c.access.Lock()
	defer c.access.Unlock()
	return len(c.got)
}

func snellPacket(seq int, size int) *buf.Buffer {
	b := buf.NewSize(size)
	payload := b.Extend(size)
	payload[0] = byte(seq)
	payload[1] = byte(seq >> 8)
	return b
}

func TestUoTPacketsHandedOverOneByOneGoOutInBatches(t *testing.T) {
	t.Parallel()
	tunnel := &recordingTunnel{}
	server, client := net.Pipe()
	defer client.Close()
	conn := &closeTunnelPacketConn{PacketConn: tunnel, tunnel: server}
	require.Equal(t, N.PacketWriter(conn), N.UnwrapPacketWriter(conn), "the copy writes through the staging, not past it")
	batchWriter, isBatchWriter := bufio.CreatePacketBatchWriter(conn)
	require.True(t, isBatchWriter)
	require.Same(t, tunnel, batchWriter, "a copy that reads in batches still reaches the service's batch writer")
	const count = 300
	destination := M.ParseSocksaddr("1.1.1.1:443")
	for i := range count {
		require.NoError(t, conn.WritePacket(snellPacket(i, 1200+i%100), destination))
	}
	require.Eventually(t, func() bool { return tunnel.written() == count }, 5*time.Second, time.Millisecond)
	tunnel.access.Lock()
	defer tunnel.access.Unlock()
	t.Logf("%d packets in %d writes, the largest %d packets", count, tunnel.calls, tunnel.largest)
	for i, packet := range tunnel.got {
		require.Equal(t, [2]int{i, 1200 + i%100}, packet, "packet %d out of order or cut", i)
	}
	require.Less(t, tunnel.calls, count, "packets that arrive during a write go out together")
	require.LessOrEqual(t, tunnel.largest, maxStagedPackets)
}

func TestAFailedUoTWriteReachesTheWriter(t *testing.T) {
	t.Parallel()
	tunnel := &recordingTunnel{err: E.New("stream reset")}
	server, client := net.Pipe()
	defer client.Close()
	conn := &closeTunnelPacketConn{PacketConn: tunnel, tunnel: server}
	done := make(chan error, 1)
	go func() {
		for i := 0; ; i++ {
			if err := conn.WritePacket(snellPacket(i, 1200), M.ParseSocksaddr("1.1.1.1:443")); err != nil {
				done <- err
				return
			}
		}
	}()
	select {
	case err := <-done:
		require.ErrorContains(t, err, "stream reset")
	case <-time.After(2 * time.Second):
		t.Fatal("a writer waiting for staging space never learned the stream failed")
	}
}
