package route

import (
	"github.com/sagernet/sing/common/bufio"
	N "github.com/sagernet/sing/common/network"
)

// packetCopyBatchSize caps the packets one read of a packet copy takes.
// sing's copy reads up to 64 at a time and gives every slot a buffer before
// the first read, sized for the largest datagram (about 32KB through snell),
// and keeps the ones a read did not fill for as long as the session lives,
// which is udp_timeout past its last packet: 200 idle snell UDP sessions held
// 432MB on the server. A read of 8 still takes everything a session queues
// between two reads short of a burst.
const packetCopyBatchSize = 8

// batchCappedPacketReader goes in front of a packet copy's source. sing takes
// the counters off the top of the source before anything else, so this stands
// for them: it hands them over with the source under them, wrapped in
// cappedPacketReader, which makes the batch read waiters.
type batchCappedPacketReader struct {
	N.PacketReader
}

func (r batchCappedPacketReader) UnwrapPacketReader() (N.PacketReader, []N.CountFunc) {
	reader, counters := N.UnwrapCountPacketReader(r.PacketReader, nil)
	return cappedPacketReader{reader}, counters
}

// cappedPacketReader creates its reader's batch read waiters with the batch
// capped at packetCopyBatchSize. Everything else reaches the reader through
// Upstream: its MTU, overhead and plain read waiter.
type cappedPacketReader struct {
	N.PacketReader
}

func (r cappedPacketReader) Upstream() any {
	return r.PacketReader
}

func (r cappedPacketReader) ReaderReplaceable() bool {
	return true
}

func (r cappedPacketReader) ReadCachedPacket() *N.PacketBuffer {
	if cached, isCached := r.PacketReader.(N.CachedPacketReader); isCached {
		return cached.ReadCachedPacket()
	}
	return nil
}

func (r cappedPacketReader) CreatePacketBatchReadWaiter() (N.PacketBatchReadWaiter, bool) {
	waiter, created := bufio.CreatePacketBatchReadWaiter(r.PacketReader)
	if !created {
		return nil, false
	}
	return cappedBatchReadWaiter{waiter}, true
}

func (r cappedPacketReader) CreateConnectedPacketBatchReadWaiter() (N.ConnectedPacketBatchReadWaiter, bool) {
	waiter, created := bufio.CreateConnectedPacketBatchReadWaiter(r.PacketReader)
	if !created {
		return nil, false
	}
	return cappedConnectedBatchReadWaiter{waiter}, true
}

type cappedBatchReadWaiter struct {
	N.PacketBatchReadWaiter
}

func (w cappedBatchReadWaiter) InitializeReadWaiter(options N.ReadWaitOptions) (needCopy bool) {
	options.BatchSize = cappedBatchSize(options.BatchSize)
	return w.PacketBatchReadWaiter.InitializeReadWaiter(options)
}

type cappedConnectedBatchReadWaiter struct {
	N.ConnectedPacketBatchReadWaiter
}

func (w cappedConnectedBatchReadWaiter) InitializeReadWaiter(options N.ReadWaitOptions) (needCopy bool) {
	options.BatchSize = cappedBatchSize(options.BatchSize)
	return w.ConnectedPacketBatchReadWaiter.InitializeReadWaiter(options)
}

// cappedBatchSize is size capped at packetCopyBatchSize; zero, the waiters'
// own default of 64, is capped too.
func cappedBatchSize(size int) int {
	if size <= 0 || size > packetCopyBatchSize {
		return packetCopyBatchSize
	}
	return size
}
