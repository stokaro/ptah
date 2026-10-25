package providerinstall

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// lockName is the file in a provider's directory that installs into that
// directory lock. The file stays after the install: removing it while another
// install has it open would let a third install lock a new file of the same
// name, and the two would run at once.
const lockName = ".install.lock"

// lockRetryInterval is how long an install waits before it asks again for a
// lock another install holds.
const lockRetryInterval = 25 * time.Millisecond

// storeLock is the exclusive install lock of one provider directory. The
// operating system locks the file per open handle, so the lock excludes an
// install in another process and one in this process alike.
type storeLock struct {
	file *os.File
}

// lockProvider takes the install lock of the provider directory dir, creating
// the directory and the lock file when they are missing. While another install
// holds the lock it waits, until ctx ends. The caller releases the lock with
// [storeLock.Close].
func lockProvider(ctx context.Context, dir string) (*storeLock, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, lockName)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	for {
		locked, err := tryLock(file)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("lock %s: %w", path, err), file.Close())
		}
		if locked {
			return &storeLock{file: file}, nil
		}
		select {
		case <-ctx.Done():
			return nil, errors.Join(fmt.Errorf("another install holds %s: %w", path, ctx.Err()), file.Close())
		case <-time.After(lockRetryInterval):
		}
	}
}

// Close releases the lock, then closes its file. Closing a lock that is
// already closed, or a nil one, does nothing.
func (l *storeLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	file := l.file
	l.file = nil
	return errors.Join(unlock(file), file.Close())
}
