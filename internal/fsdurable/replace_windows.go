package fsdurable

import "golang.org/x/sys/windows"

// ReplaceFile atomically replaces newPath with oldPath and asks Windows to
// flush the move to stable storage before returning.
func ReplaceFile(oldPath, newPath string) error {
	oldPathPtr, err := windows.UTF16PtrFromString(oldPath)
	if err != nil {
		return err
	}
	newPathPtr, err := windows.UTF16PtrFromString(newPath)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(
		oldPathPtr,
		newPathPtr,
		windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH,
	)
}
