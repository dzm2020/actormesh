//go:build windows

package netutil

import "syscall"

func applySocketReuseOptions(fd uintptr) error {
	// Windows 不支持 SO_REUSEPORT，仅设置 SO_REUSEADDR。
	return syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
}
