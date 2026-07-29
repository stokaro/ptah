package fsdurable

import (
	"errors"
	"fmt"
	"os"
)

// SyncDir flushes directory-entry changes for dir to stable storage.
func SyncDir(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open directory for sync: %w", err)
	}
	return errors.Join(handle.Sync(), handle.Close())
}
