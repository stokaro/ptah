//go:build windows

package providerinstall

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

// tryLock takes an exclusive lock on the first byte of file without waiting,
// and reports false when another handle holds it.
func tryLock(file *os.File) (bool, error) {
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
	return err == nil, err
}

// unlock releases the lock explicitly: Windows releases the locks of a closed
// handle, but not at a moment it promises.
func unlock(file *os.File) error {
	var overlapped windows.Overlapped
	return windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, &overlapped)
}
