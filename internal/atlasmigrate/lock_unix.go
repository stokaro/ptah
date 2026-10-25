//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package atlasmigrate

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func tryLockFile(file *os.File) (bool, error) {
	err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock migration directory: %w", err)
	}
	return true, nil
}

func unlockFile(file *os.File) error {
	if err := unix.Flock(int(file.Fd()), unix.LOCK_UN); err != nil {
		return fmt.Errorf("unlock migration directory: %w", err)
	}
	return nil
}

// The lock file is removed on release; see (*dirLock).release for the order.
func removeLockFileBeforeUnlock(path string) { _ = os.Remove(path) }

func removeLockFileAfterClose(string) {}

// lockFileInUse is false: opening a file another process holds never fails
// here.
func lockFileInUse(error) bool { return false }
