package fsdurable

import (
	"errors"
	"time"

	"golang.org/x/sys/windows"
)

// ReplaceWindow bounds how long ReplaceFile waits out a destination another
// handle holds. Windows refuses to replace a file that is open without
// FILE_SHARE_DELETE, as every file os.Open opens is, or that another replace
// has open, with ERROR_ACCESS_DENIED or ERROR_SHARING_VIOLATION. Both last as
// long as the other handle, which a reader or a concurrent writer of the same
// small file closes within milliseconds; cmd/go waits them out the same way.
const ReplaceWindow = 2 * time.Second

// ReplaceFile atomically replaces newPath with oldPath and asks Windows to
// flush the move to stable storage before returning. A destination another
// handle holds is waited out for up to ReplaceWindow; any other error is
// returned at once.
func ReplaceFile(oldPath, newPath string) error {
	oldPathPtr, err := windows.UTF16PtrFromString(oldPath)
	if err != nil {
		return err
	}
	newPathPtr, err := windows.UTF16PtrFromString(newPath)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(ReplaceWindow)
	delay := time.Millisecond
	for {
		err = windows.MoveFileEx(
			oldPathPtr,
			newPathPtr,
			windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH,
		)
		if err == nil || !heldByAnotherHandle(err) || !time.Now().Before(deadline) {
			return err
		}
		time.Sleep(delay)
		delay = min(2*delay, 100*time.Millisecond)
	}
}

func heldByAnotherHandle(err error) bool {
	return errors.Is(err, windows.ERROR_ACCESS_DENIED) || errors.Is(err, windows.ERROR_SHARING_VIOLATION)
}
