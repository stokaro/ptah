package atlasmigrate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

var errDirLocked = errors.New("migration directory is locked")

// errDirLockClosing reports an open of the lock file that the platform refused
// while another process removes it; see [lockFileClosing].
var errDirLockClosing = errors.New("migration directory lock file is being removed")

// closingLockError is an open of the lock file that [lockFileClosing] refused.
// It reads as the open's own failure, which is what a refusal that outlasts
// [lockFileClosingGrace] reports, and matches [errDirLockClosing].
type closingLockError struct{ err error }

func (e closingLockError) Error() string { return "open migration directory lock: " + e.err.Error() }

func (e closingLockError) Unwrap() error { return e.err }

func (closingLockError) Is(target error) bool { return target == errDirLockClosing }

// lockFileClosingGrace is how long an open the platform refuses as
// [lockFileClosing] is waited out before its error is reported. A removal ends
// within it; a refusal that outlasts it is a fault of its own, such as a
// directory the process may not write, and is reported rather than waited on
// for as long as the lock timeout allows, which may be forever.
const lockFileClosingGrace = 2 * time.Second

type dirLock struct {
	file *os.File
	path string
}

type heldMigrationDirectoryLock struct {
	active       atomic.Bool
	canonicalDir string
	parent       *heldMigrationDirectoryLock
}

type heldMigrationDirectoryLockContextKey struct{}

// WithMigrationDirectoryLock invokes consume while holding the cross-process
// lock shared by migration directory readers and publishers. The callback must
// use its scoped context for nested operations that may acquire the same lock.
func WithMigrationDirectoryLock(
	ctx context.Context,
	migrationsDir string,
	timeout time.Duration,
	consume func(context.Context) error,
) (resultErr error) {
	if consume == nil {
		return errors.New("migration directory lock callback is nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("acquire migration directory lock: %w", err)
	}
	canonicalDir, err := canonicalMigrationDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("resolve migration directory lock path: %w", err)
	}
	held, _ := ctx.Value(heldMigrationDirectoryLockContextKey{}).(*heldMigrationDirectoryLock)
	lock, err := acquireDirLock(ctx, canonicalDir, timeout)
	if err != nil {
		return err
	}
	current := &heldMigrationDirectoryLock{
		canonicalDir: canonicalDir,
		parent:       held,
	}
	current.active.Store(true)
	lockedCtx := context.WithValue(ctx, heldMigrationDirectoryLockContextKey{}, current)
	defer func() {
		current.active.Store(false)
		resultErr = errors.Join(resultErr, lock.release())
	}()
	return consume(lockedCtx)
}

func migrationDirectoryLockHeld(ctx context.Context, migrationsDir string) (bool, error) {
	canonicalDir, err := canonicalMigrationDir(migrationsDir)
	if err != nil {
		return false, err
	}
	held, _ := ctx.Value(heldMigrationDirectoryLockContextKey{}).(*heldMigrationDirectoryLock)
	for current := held; current != nil; current = current.parent {
		if current.active.Load() && current.canonicalDir == canonicalDir {
			return true, nil
		}
	}
	return false, nil
}

func acquireDirLock(ctx context.Context, migrationsDir string, timeout time.Duration) (*dirLock, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("acquire migration directory lock: %w", err)
	}
	canonicalDir, err := canonicalMigrationDir(migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("resolve migration directory lock path: %w", err)
	}
	lockPath := migrationDirLockPath(canonicalDir)
	startedAt := time.Now()
	var closingSince time.Time
	for {
		lock, err := tryAcquireDirLock(lockPath)
		if errors.Is(err, errDirLockClosing) {
			if closingSince.IsZero() {
				closingSince = time.Now()
			}
			if timeout < 0 || time.Since(closingSince) >= lockFileClosingGrace ||
				timeout > 0 && time.Since(startedAt) >= timeout {
				return nil, err
			}
			if err := waitForDirLockRetry(ctx, startedAt, timeout); err != nil {
				return nil, err
			}
			continue
		}
		closingSince = time.Time{}
		if err == nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return nil, errors.Join(
					fmt.Errorf("acquire migration directory lock: %w", contextErr),
					lock.release(),
				)
			}
			return lock, nil
		}
		if !errors.Is(err, errDirLocked) {
			return nil, err
		}
		if timeout < 0 {
			return nil, fmt.Errorf("migration directory lock is held by another process: %s", lockPath)
		}
		if timeout > 0 && time.Since(startedAt) >= timeout {
			return nil, fmt.Errorf("migration directory lock timeout after %s: %s", timeout, lockPath)
		}
		if err := waitForDirLockRetry(ctx, startedAt, timeout); err != nil {
			return nil, err
		}
	}
}

func canonicalMigrationDir(migrationsDir string) (string, error) {
	absoluteDir, err := filepath.Abs(migrationsDir)
	if err != nil {
		return "", err
	}
	resolvedDir, err := filepath.EvalSymlinks(absoluteDir)
	if err == nil {
		return resolvedDir, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	resolvedParent, err := filepath.EvalSymlinks(filepath.Dir(absoluteDir))
	if err != nil {
		return "", err
	}
	return filepath.Join(resolvedParent, filepath.Base(absoluteDir)), nil
}

func migrationDirLockPath(migrationsDir string) string {
	cleanDir := filepath.Clean(migrationsDir)
	return filepath.Join(
		filepath.Dir(cleanDir),
		"."+filepath.Base(cleanDir)+lockFileName,
	)
}

func tryAcquireDirLock(lockPath string) (*dirLock, error) {
	file, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0600)
	if lockFileInUse(err) {
		return nil, errDirLocked
	}
	if lockFileClosing(err) {
		return nil, closingLockError{err: err}
	}
	if err != nil {
		return nil, fmt.Errorf("open migration directory lock: %w", err)
	}
	locked, err := tryLockFile(file)
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if !locked {
		return nil, errors.Join(errDirLocked, file.Close())
	}
	current, err := file.Stat()
	if err != nil {
		return nil, errors.Join(err, unlockFile(file), file.Close())
	}
	pathInfo, err := os.Stat(lockPath)
	if err != nil || !os.SameFile(current, pathInfo) {
		return nil, errors.Join(errDirLocked, unlockFile(file), file.Close())
	}
	return &dirLock{file: file, path: lockPath}, nil
}

func waitForDirLockRetry(ctx context.Context, startedAt time.Time, timeout time.Duration) error {
	wait := 25 * time.Millisecond
	if timeout > 0 {
		remaining := timeout - time.Since(startedAt)
		if remaining <= 0 {
			return nil
		}
		wait = min(wait, remaining)
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("acquire migration directory lock: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}

// release gives up the lock and removes the lock file, so a run leaves no file
// beside the migration directory. The file sat in the repository after every
// `migrate diff`, next to the directory it guarded, where Atlas leaves nothing.
//
// The exclusion holds because the file is removed only where no other process
// can hold it. On Unix it is unlinked while the lock is still held: a process
// that opened it before then can lock it only after this one unlocks, and then
// finds the path gone or naming a newer file, which [tryAcquireDirLock] answers
// as still locked, so it opens the path again. On Windows a file another
// process has open cannot be removed at all, so it is removed after it is
// closed, and stays when someone has already opened it. A removal that fails
// leaves the file for the next run to reuse, as before.
func (l *dirLock) release() error {
	if l == nil {
		return nil
	}
	removeLockFileBeforeUnlock(l.path)
	unlockErr := unlockFile(l.file)
	closeErr := l.file.Close()
	if err := errors.Join(unlockErr, closeErr); err != nil {
		return fmt.Errorf("release migration directory lock: %w", err)
	}
	removeLockFileAfterClose(l.path)
	return nil
}
