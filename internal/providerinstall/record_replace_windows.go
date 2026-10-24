//go:build windows

package providerinstall

import (
	"errors"
	"os"
	"time"

	"golang.org/x/sys/windows"

	"ptah.run/internal/fsretry"
)

// recordReplaceWindow bounds how long a record replacement waits for another
// install that holds the record open, as a parallel install reading it does.
const recordReplaceWindow = 2 * time.Second

// replaceRecord moves the record written at temp over the one at path.
// Windows refuses the move while another install holds the record open, so
// it is retried for a moment; see [fsretry].
func replaceRecord(temp, path string) error {
	return fsretry.Replace(func() error { return os.Rename(temp, path) }, heldByAnotherHandle, recordReplaceWindow)
}

func heldByAnotherHandle(err error) bool {
	return errors.Is(err, windows.ERROR_ACCESS_DENIED) || errors.Is(err, windows.ERROR_SHARING_VIOLATION)
}
