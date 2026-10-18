//go:build linux

package naive

import (
	"net"
	"syscall"
	"time"

	"github.com/sagernet/sing/common"
	M "github.com/sagernet/sing/common/metadata"

	"golang.org/x/sys/unix"
)

// wrapPaced gates conn's bulk writes on target, the queueing delay they may
// add; see pacedConn. A conn without a socket to read TCP_INFO from, a full
// path table, or a target of zero leaves conn as it is. On a kernel too old
// for tcpi_min_rtt or tcpi_delivery_rate the field reads zero and the gate
// stays open.
func wrapPaced(conn net.Conn, target time.Duration) net.Conn {
	if target <= 0 {
		return conn
	}
	// Through common.Cast: with proxy_protocol the accepted conn is sing's
	// AddrConn around the TCP conn.
	syscallConn, ok := common.Cast[syscall.Conn](conn)
	if !ok {
		return conn
	}
	rawConn, err := syscallConn.SyscallConn()
	if err != nil {
		return conn
	}
	// The socket's own peer, which TCP_INFO describes: behind proxy_protocol
	// conn reports the client instead.
	socket, ok := syscallConn.(net.Conn)
	if !ok {
		return conn
	}
	peer := M.SocksaddrFromNet(socket.RemoteAddr()).Unwrap().Addr
	if !peer.IsValid() {
		return conn
	}
	paced := &pacedConn{Conn: conn, target: target}
	paced.path = pacePathsByPeer.join(peer, paced, time.Now())
	if paced.path == nil {
		return conn
	}
	// Called under paced.access only, so one closure serves every read.
	var (
		tcpInfo *unix.TCPInfo
		infoErr error
	)
	read := func(fd uintptr) {
		tcpInfo, infoErr = unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
	}
	paced.info = func() (paceInfo, error) {
		err := rawConn.Control(read)
		if err == nil {
			err = infoErr
		}
		if err != nil {
			return paceInfo{}, err
		}
		return paceInfo{
			rtt:    time.Duration(tcpInfo.Rtt) * time.Microsecond,
			minRTT: time.Duration(tcpInfo.Min_rtt) * time.Microsecond,
			rate:   tcpInfo.Delivery_rate,
			queued: uint64(tcpInfo.Unacked)*uint64(tcpInfo.Snd_mss) + uint64(tcpInfo.Notsent_bytes),
		}, nil
	}
	return paced
}
