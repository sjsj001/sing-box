package interrupt

import (
	"io"
	"net"
	"testing"
	"time"

	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

// The copy loop unwraps the connection it holds and half-closes what it finds
// beneath, and a group hands it this wrapper.
func TestAGroupsConnectionHalfClosesWhatItWraps(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, _ := listener.Accept()
		accepted <- conn
	}()
	dialed, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	peer := <-accepted
	require.NotNil(t, peer)
	defer peer.Close()

	conn := NewGroup().NewConn(dialed, false, false)
	defer conn.Close()
	closer, isCloser := N.UnwrapWriter(conn).(N.WriteCloser)
	require.True(t, isCloser, "the dialed connection is found beneath the wrapper")
	require.NoError(t, closer.CloseWrite())
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = peer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "the peer sees the end of the request")
	_, err = peer.Write([]byte("answer"))
	require.NoError(t, err)
	require.NoError(t, peer.Close())
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	answer, err := io.ReadAll(conn)
	require.NoError(t, err)
	require.Equal(t, "answer", string(answer), "and can still answer")
}
