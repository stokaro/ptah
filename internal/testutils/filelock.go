package testutils

import (
	"errors"
	"fmt"
	"os"
)

// AcquireExclusiveFileLock acquires an OS-backed lock for cross-process lock
// behavior tests. The returned function unlocks and closes the file.
func AcquireExclusiveFileLock(path string) (func() error, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	if err := lockFile(file); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return func() error {
		if err := unlockFile(file); err != nil {
			return errors.Join(err, file.Close())
		}
		if err := file.Close(); err != nil {
			return err
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove test lock file: %w", err)
		}
		return nil
	}, nil
}
