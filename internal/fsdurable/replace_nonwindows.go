//go:build !windows

package fsdurable

import "os"

// ReplaceFile atomically replaces newPath with oldPath.
func ReplaceFile(oldPath, newPath string) error {
	return os.Rename(oldPath, newPath)
}
