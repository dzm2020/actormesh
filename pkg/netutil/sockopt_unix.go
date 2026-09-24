//go:build !windows

package netutil

import "golang.org/x/sys/unix"

func applySocketReuseOptions(fd uintptr) error {
	fdInt := int(fd)
	if err := unix.SetsockoptInt(fdInt, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		return err
	}
	return nil
}
