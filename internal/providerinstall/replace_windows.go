//go:build windows

package providerinstall

import (
	"errors"
	"os"
	"time"

	"golang.org/x/sys/windows"
)

// replaceBudget bounds how long a record replacement waits for a reader that
// holds the record open.
const replaceBudget = 2 * time.Second

// replaceRecord renames temp over the record at path. Windows refuses to
// replace a file another handle holds open, with ERROR_ACCESS_DENIED or
// ERROR_SHARING_VIOLATION, and Go opens every file without delete sharing
// there. The install lock keeps other installs away from the record, but a
// provider selection in another process reads it without the lock, and so may
// a virus scanner. Such a reader lets go within moments, so the rename is
// retried for at most replaceBudget, with waits that start at a millisecond and
// double up to 50 milliseconds. Any other error is returned at once.
func replaceRecord(temp, path string) error {
	deadline := time.Now().Add(replaceBudget)
	wait := time.Millisecond
	for {
		err := os.Rename(temp, path)
		if err == nil || !heldOpen(err) || time.Now().After(deadline) {
			return err
		}
		time.Sleep(wait)
		wait = min(2*wait, 50*time.Millisecond)
	}
}

// heldOpen reports whether err is how Windows refuses to replace a file another
// handle holds open.
func heldOpen(err error) bool {
	return errors.Is(err, windows.ERROR_ACCESS_DENIED) || errors.Is(err, windows.ERROR_SHARING_VIOLATION)
}
