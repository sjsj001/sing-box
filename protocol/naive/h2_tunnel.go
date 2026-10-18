package naive

import (
	"errors"
	"io"

	"github.com/sagernet/sing-box/transport/v2rayhttp"
	"github.com/sagernet/sing/common/buf"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/rw"
)

// idleReadBuffer is the pool tier an HTTP/3 tunnel's upload waits on while it
// is quiet. 16 KiB still takes most bursts in one read; at 4 KiB a relay spent
// 7.6% more CPU on HTTP/3 uploads.
const idleReadBuffer = 16 * 1024

// h2Tunnel is v2rayhttp's HTTP2ConnWrapper, which keeps writes off the
// handler's ResponseWriter once the handler has returned, with a read waiter.
// The wrapper is no ReaderReplaceable, so route's copy stopped its search for
// one there and took its full buffer, 32 KiB and 64 KiB once 512 KB had
// passed, before blocking in the request body's Read: an idle tunnel held that
// buffer for as long as it stayed idle. Reads still go through the wrapper,
// padding and all.
//
// The copy reaches the waiter only while nothing else holds a read: an inbound
// that sniffs with a timeout leaves the deadline reader route puts in front a
// read pending, and the copy then takes its full buffer as it did before.
type h2Tunnel struct {
	*v2rayhttp.HTTP2ConnWrapper
	conn *naiveH2Conn
}

func newH2Tunnel(conn *naiveH2Conn) h2Tunnel {
	return h2Tunnel{v2rayhttp.NewHTTP2Wrapper(conn), conn}
}

func (t h2Tunnel) CreateReadWaiter() (N.ReadWaiter, bool) {
	return &tunnelReadWaiter{tunnel: t}, true
}

type tunnelReadWaiter struct {
	tunnel  h2Tunnel
	options N.ReadWaitOptions
	// full: the last read filled its buffer, so a burst is under way and the
	// next read takes the full size again. HTTP/3 only.
	full bool
}

func (w *tunnelReadWaiter) InitializeReadWaiter(options N.ReadWaitOptions) (needCopy bool) {
	w.options = options
	return false
}

func (w *tunnelReadWaiter) WaitReadBuffer() (*buf.Buffer, error) {
	for {
		var buffer *buf.Buffer
		switch {
		case w.tunnel.conn.emptyReadWaits:
			// After a full read too: a burst that ended exactly on the buffer
			// would otherwise park the idle tunnel on it.
			if err := w.tunnel.conn.waitReadable(); err != nil {
				return nil, err
			}
			buffer = w.options.NewBuffer()
		case w.full:
			buffer = w.options.NewBuffer()
		default:
			buffer = w.options.NewBufferSize(max(idleReadBuffer-w.options.FrontHeadroom-w.options.RearHeadroom, 1024))
		}
		free := buffer.FreeBytes()
		n, err := w.tunnel.Read(free)
		w.full = n == len(free)
		if n > 0 && (err == nil || errors.Is(err, io.EOF)) {
			buffer.Truncate(n)
			w.options.PostReturn(buffer)
			return buffer, nil
		}
		buffer.Release()
		if err != nil {
			return nil, err
		}
	}
}

// waitReadable returns once the next Read has data to return, or an error.
// The HTTP/2 server's request body blocks an empty Read until data or an error
// arrives and takes nothing; quic-go's returns at once, which is why HTTP/3
// waits on idleReadBuffer instead. TestHTTP2BodyEmptyReadWaitsAndTakesNothing
// holds net/http to it. Padding already due is skipped first, so that the wait
// is for the bytes the next Read would block on.
func (c *naiveH2Conn) waitReadable() error {
	if c.readRemaining == 0 && c.paddingRemaining > 0 {
		if err := rw.SkipN(c.reader, c.paddingRemaining); err != nil {
			return wrapError(err)
		}
		c.paddingRemaining = 0
	}
	_, err := c.reader.Read(nil)
	return wrapError(err)
}
