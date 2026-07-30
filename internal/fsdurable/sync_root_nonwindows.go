//go:build !windows

package fsdurable

import (
	"errors"
	"fmt"
	"os"
)

// SyncRoot flushes root directory-entry changes to stable storage.
func SyncRoot(root *os.Root) error {
	handle, err := root.Open(".")
	if err != nil {
		return fmt.Errorf("open rooted directory for sync: %w", err)
	}
	return errors.Join(handle.Sync(), handle.Close())
}
