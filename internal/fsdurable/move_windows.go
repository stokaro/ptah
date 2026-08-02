//go:build windows

package fsdurable

import (
	"errors"
	"io/fs"

	"golang.org/x/sys/windows"
)

// MoveFileNoReplace atomically publishes oldPath at newPath without replacing
// an existing entry and asks Windows to flush the move before returning.
func MoveFileNoReplace(oldPath, newPath string) error {
	oldPathPtr, err := windows.UTF16PtrFromString(oldPath)
	if err != nil {
		return err
	}
	newPathPtr, err := windows.UTF16PtrFromString(newPath)
	if err != nil {
		return err
	}
	err = windows.MoveFileEx(
		oldPathPtr,
		newPathPtr,
		windows.MOVEFILE_WRITE_THROUGH,
	)
	if errors.Is(err, windows.ERROR_ALREADY_EXISTS) || errors.Is(err, windows.ERROR_FILE_EXISTS) {
		return errors.Join(fs.ErrExist, err)
	}
	return err
}
