//go:build windows

package atlasmigrate

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func tryLockFile(file *os.File) (bool, error) {
	var overlapped windows.Overlapped
	err := windows.LockFileEx(
		windows.Handle(file.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		1,
		0,
		&overlapped,
	)
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock migration directory: %w", err)
	}
	return true, nil
}

func unlockFile(file *os.File) error {
	var overlapped windows.Overlapped
	if err := windows.UnlockFileEx(
		windows.Handle(file.Fd()),
		0,
		1,
		0,
		&overlapped,
	); err != nil {
		return fmt.Errorf("unlock migration directory: %w", err)
	}
	return nil
}

// lockFileInUse reports an open of the lock file that another handle refused.
// Removing a file opens it without sharing it, so an open in the instant a
// release removes the lock file fails with ERROR_SHARING_VIOLATION; the lock
// is held then as surely as when LockFileEx refuses it, and the caller waits
// for it the same way.
func lockFileInUse(err error) bool {
	return errors.Is(err, windows.ERROR_SHARING_VIOLATION)
}

// The lock file is removed on release; see (*dirLock).release for the order.
func removeLockFileBeforeUnlock(string) {}

func removeLockFileAfterClose(path string) { _ = os.Remove(path) }
