package atlasmigrate_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"golang.org/x/sys/windows"

	"ptah.run/internal/atlasmigrate"
)

// holdUnshared opens the migration directory's lock file the way removing it
// does, sharing it with nobody, and returns the handle.
func holdUnshared(c *qt.C, dir string) windows.Handle {
	// The lock beside the directory's resolved path, which is the one the
	// lock uses.
	dir, err := filepath.EvalSymlinks(dir)
	c.Assert(err, qt.IsNil)
	path, err := windows.UTF16PtrFromString(filepath.Join(filepath.Dir(dir), "."+filepath.Base(dir)+".ptah-migrate-diff.lock"))
	c.Assert(err, qt.IsNil)
	handle, err := windows.CreateFile(path, windows.GENERIC_READ|windows.DELETE, 0, nil,
		windows.OPEN_ALWAYS, windows.FILE_ATTRIBUTE_NORMAL, 0)
	c.Assert(err, qt.IsNil)
	return handle
}

// A lock file another handle holds unshared, as a release removing it does,
// is a held lock: tried once, the lock is refused as held rather than failed.
func TestWithMigrationDirectoryLock_TreatsAnUnsharedLockFileAsHeld(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	handle := holdUnshared(c, dir)
	t.Cleanup(func() { _ = windows.CloseHandle(handle) })

	err := atlasmigrate.WithMigrationDirectoryLock(t.Context(), dir, -1, func(context.Context) error { return nil })

	c.Assert(err, qt.ErrorMatches, "migration directory lock is held by another process: .*")
}

// Waited for, it is acquired once the other handle is closed.
func TestWithMigrationDirectoryLock_WaitsOutAnUnsharedLockFile(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	handle := holdUnshared(c, dir)
	closed := make(chan error, 1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		closed <- windows.CloseHandle(handle)
	}()
	ran := false

	err := atlasmigrate.WithMigrationDirectoryLock(t.Context(), dir, 10*time.Second, func(context.Context) error {
		ran = true
		return nil
	})

	c.Assert(err, qt.IsNil)
	c.Assert(ran, qt.IsTrue)
	c.Assert(<-closed, qt.IsNil)
}
