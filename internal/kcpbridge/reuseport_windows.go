//go:build windows

package kcpbridge

import (
	"net"
)

// newReusePortConn - Windows는 SO_REUSEPORT 없음.
// SO_REUSEADDR로 대체 (net.ListenPacket 기본값).
// Windows 클라이언트(join)에서 사용.
func newReusePortConn(addr string) (net.PacketConn, error) {
	return net.ListenPacket("udp", addr)
}