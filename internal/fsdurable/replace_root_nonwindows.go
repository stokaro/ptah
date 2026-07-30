//go:build !windows

package fsdurable

import "os"

// ReplaceFileAt atomically replaces newName with oldName through root.
func ReplaceFileAt(root *os.Root, oldName, newName string) error {
	return root.Rename(oldName, newName)
}
