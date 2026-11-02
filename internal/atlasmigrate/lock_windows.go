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

// lockFileClosing reports an open of the lock file that a removal in progress
// refused. A file removed while another handle still has it open stays until
// that handle closes, and Windows refuses every open of it in the meantime with
// ERROR_ACCESS_DENIED: a release that closes and removes the file while
// another process is opening it leaves that process this answer. Without it
// the waiting run failed with `Access is denied` instead of taking the lock a
// moment later. The same answer also means a directory the process may not
// write, so [acquireDirLock] waits it out only briefly.
func lockFileClosing(err error) bool {
	return errors.Is(err, windows.ERROR_ACCESS_DENIED)
}

// The lock file is removed on release; see (*dirLock).release for the order.
func removeLockFileBeforeUnlock(string) {}

func removeLockFileAfterClose(path string) { _ = os.Remove(path) }
