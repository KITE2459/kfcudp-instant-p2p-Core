//go:build linux || darwin || freebsd

package kcpbridge

import (
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

// newReusePortConn - SO_REUSEPORT로 UDP 소켓 생성.
// 같은 포트에 소켓 N개 바인딩 → OS RSS로 패킷 분산.
// Linux 3.9+, macOS 10.12+ 지원.
func newReusePortConn(addr string) (net.PacketConn, error) {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var setsockoptErr error
			err := c.Control(func(fd uintptr) {
				setsockoptErr = unix.SetsockoptInt(
					int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1,
				)
			})
			if err != nil {
				return err
			}
			return setsockoptErr
		},
	}
	return lc.ListenPacket(nil, "udp", addr) //nolint:staticcheck
}