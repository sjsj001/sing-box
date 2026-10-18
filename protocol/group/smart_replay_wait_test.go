package group

import (
	"bytes"
	"io"
	"net"
	"testing"

	"github.com/sagernet/sing/common/bufio"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

func TestAnIdleTunnelReadWaitsOnASmallBuffer(t *testing.T) {
	t.Parallel()
	tunnel, far := net.Pipe()
	defer far.Close()
	conn := &replayConn{Conn: tunnel, member: newMember("a-1", ms(30))}
	defer conn.Close()
	waiter, isWaiter := bufio.CreateReadWaiter(bufio.NewExtendedReader(conn))
	require.True(t, isWaiter)
	require.False(t, waiter.InitializeReadWaiter(N.ReadWaitOptions{MTU: 16383, FrontHeadroom: 23, RearHeadroom: 16}))
	go far.Write([]byte("hello"))
	buffer, err := waiter.WaitReadBuffer()
	require.NoError(t, err)
	require.Equal(t, "hello", string(buffer.Bytes()))
	require.LessOrEqual(t, buffer.RawCap(), idleReadBuffer)
	buffer.Release()
	burst := bytes.Repeat([]byte{'x'}, 64*1024)
	go far.Write(burst)
	var got []byte
	var capacities []int
	for len(got) < len(burst) {
		buffer, err = waiter.WaitReadBuffer()
		require.NoError(t, err)
		got = append(got, buffer.Bytes()...)
		capacities = append(capacities, buffer.RawCap())
		buffer.Release()
	}
	require.Equal(t, burst, got)
	require.Greater(t, capacities[1], idleReadBuffer, "a read that filled its buffer is followed by a full-size one")
	far.Close()
	_, err = waiter.WaitReadBuffer()
	require.ErrorIs(t, err, io.EOF)
}
