package netutil

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/windows"
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
)

func EndpointAddress(host string, port int) string {
	if port <= 0 {
		return strings.TrimSpace(host)
	}
	return net.JoinHostPort(strings.TrimSpace(host), strconv.Itoa(port))
}

func SplitHostPort(address string) (string, int, error) {
	host, rawPort, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, fmt.Errorf("invalid rpc address %q: %w", address, err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil || port <= 0 {
		return "", 0, fmt.Errorf("invalid rpc address port %q", rawPort)
	}
	return host, port, nil
}

// setReuseSocketOpt 设置端口复用参数
// reusePort: 是否开启 SO_REUSEPORT（多进程同端口监听）
func setReuseSocketOpt(c syscall.RawConn) error {
	return c.Control(func(fd uintptr) {
		_ = applySocketReuseOptions(fd)
	})
}

// NewReuseTcpListener 创建支持端口复用的TCP监听器
// addr: 监听地址 如 ":8080"、"127.0.0.1:9000"
// reusePort: true=多进程共享端口(仅linux/mac生效)
func NewReuseTcpListener(ctx context.Context, network, addr string) (net.Listener, error) {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			return setReuseSocketOpt(c)
		},
	}
	return lc.Listen(ctx, network, addr)
}

// NewReuseUdpListener 创建支持端口复用的UDP PacketConn
func NewReuseUdpListener(ctx context.Context, network, addr string) (net.PacketConn, error) {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			return setReuseSocketOpt(c)
		},
	}
	return lc.ListenPacket(ctx, network, addr)
}

func CloneUDPAddr(addr *net.UDPAddr) *net.UDPAddr {
	if addr == nil {
		return nil
	}
	c := *addr
	c.IP = append(net.IP(nil), addr.IP...)
	return &c
}

func IsConnectionReset(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	// 1. 直接比较 syscall 错误
	if errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	// 2. Windows 特有错误码
	if errors.Is(err, windows.WSAECONNRESET) {
		return true
	}
	// 3. 若被包装在 net.OpError 中
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		// 检查底层错误
		if errors.Is(opErr.Err, syscall.ECONNRESET) ||
			errors.Is(opErr.Err, windows.WSAECONNRESET) {
			return true
		}
	}
	return false
}
