package route

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

type recordingBatchReadWaiter struct {
	N.PacketBatchReadWaiter
	batchSize int
}

func (w *recordingBatchReadWaiter) InitializeReadWaiter(options N.ReadWaitOptions) bool {
	w.batchSize = options.BatchSize
	return false
}

// sing asks for 64, or for its default with zero; the waiter is asked for 8.
func TestCappedBatchReadWaiterCapsTheBatch(t *testing.T) {
	for asked, want := range map[int]int{0: 8, 64: 8, 8: 8, 4: 4} {
		recorder := &recordingBatchReadWaiter{}
		cappedBatchReadWaiter{recorder}.InitializeReadWaiter(N.ReadWaitOptions{BatchSize: asked})
		require.Equal(t, want, recorder.batchSize, "asked %d", asked)
	}
}

type recordingPacketWriter struct {
	access  sync.Mutex
	packets []string
}

func (w *recordingPacketWriter) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	w.access.Lock()
	w.packets = append(w.packets, string(buffer.Bytes()))
	w.access.Unlock()
	buffer.Release()
	return nil
}

func (w *recordingPacketWriter) count() int {
	w.access.Lock()
	defer w.access.Unlock()
	return len(w.packets)
}

// A copy through the cap still counts what the counters on its source count,
// and delivers every packet.
func TestBatchCappedPacketCopyKeepsCounters(t *testing.T) {
	local, err := net.ListenPacket("udp4", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { local.Close() })
	var counted atomic.Int64
	source := bufio.NewCounterPacketConn(bufio.NewPacketConn(local), []N.CountFunc{func(n int64) { counted.Add(n) }}, nil)
	destination := &recordingPacketWriter{}
	go bufio.CopyPacket(destination, batchCappedPacketReader{source})

	peer, err := net.Dial("udp4", local.LocalAddr().String())
	require.NoError(t, err)
	t.Cleanup(func() { peer.Close() })
	const packets = 20
	for i := 0; i < packets; i++ {
		_, err = peer.Write([]byte("0123456789"))
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return destination.count() == packets }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, int64(10*packets), counted.Load())
}
