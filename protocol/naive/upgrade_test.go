package naive

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type countingListener struct {
	net.Listener
	read atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: conn, read: &l.read}, nil
}

type countingConn struct {
	net.Conn
	read *atomic.Int64
}

func (c *countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.read.Add(int64(n))
	return n, err
}

// x/net's h2c read the body of an HTTP/1 request asking to upgrade to h2c
// whole into memory before anything of ours ran, unauthenticated. net/http
// takes no upgrade: the request is a plain GET for the page, and what it does
// not read of the body it gives up on and closes the connection.
func TestAnH2cUpgradeIsAPlainRequestWhoseBodyIsNotRead(t *testing.T) {
	inbound, _ := fallbackInbound(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	counted := &countingListener{Listener: listener}
	server := inbound.newHTTPServer()
	inbound.httpServer = server
	go server.Serve(counted)
	t.Cleanup(func() { server.Close() })

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	_, err = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: x\r\nUpgrade: h2c\r\nConnection: Upgrade, HTTP2-Settings\r\nHTTP2-Settings: \r\nContent-Length: 1073741824\r\n\r\n")
	require.NoError(t, err)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err)
	response.Body.Close()
	require.Equal(t, "HTTP/1.1", response.Proto)
	require.Equal(t, http.StatusOK, response.StatusCode)

	chunk := make([]byte, 64<<10)
	var sent int
	require.NoError(t, conn.SetWriteDeadline(time.Now().Add(3*time.Second)))
	for sent < 32<<20 {
		n, err := conn.Write(chunk)
		sent += n
		if err != nil {
			break
		}
	}
	// What the client got written says little: a host with large TCP buffers
	// takes tens of megabytes into the kernel that the server never reads.
	// Count what the server read, once it has let the connection go.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	io.Copy(io.Discard, conn)
	read := counted.read.Load()
	t.Logf("the client wrote %d bytes, the server read %d", sent, read)
	require.Less(t, read, int64(1<<20), "the server read an unauthenticated request body without bound")
}
