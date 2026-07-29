//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package devlock

import (
	"fmt"
	"os"
	"syscall"
)

func filesystemIdentity(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", fmt.Errorf("unsupported file metadata for %q", path)
	}
	return fmt.Sprintf("%d:%d", stat.Dev, stat.Ino), nil
}
