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

// holdRemoved creates the migration directory's lock file, opens it sharing
// its removal, and marks it for removal, so the file stays pending removal
// until the returned handle closes, as a release that removes the file while
// another process has it open leaves it.
//
// The mark is the classic disposition, which keeps the name until the last
// handle closes. DeleteFile asks for POSIX semantics where the volume offers
// them, and those take the name away at once, so a test built on it passes
// whether or not the refusal is handled.
func holdRemoved(c *qt.C, dir string) windows.Handle {
	dir, err := filepath.EvalSymlinks(dir)
	c.Assert(err, qt.IsNil)
	path := filepath.Join(filepath.Dir(dir), "."+filepath.Base(dir)+".ptah-migrate-diff.lock")
	name, err := windows.UTF16PtrFromString(path)
	c.Assert(err, qt.IsNil)
	handle, err := windows.CreateFile(name, windows.GENERIC_READ|windows.DELETE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil,
		windows.OPEN_ALWAYS, windows.FILE_ATTRIBUTE_NORMAL, 0)
	c.Assert(err, qt.IsNil)
	deleteFile := byte(1)
	c.Assert(windows.SetFileInformationByHandle(handle, windows.FileDispositionInfo, &deleteFile, 1), qt.IsNil)
	return handle
}

// A lock file pending removal is a lock on its way out: waited for, it is
// acquired once the other handle closes and the file is gone. Opening it in
// the meantime is refused with ERROR_ACCESS_DENIED, which a run reported as
// its own failure.
func TestWithMigrationDirectoryLock_WaitsOutALockFilePendingRemoval(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	handle := holdRemoved(c, dir)
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

// The same refusal is not waited on past a short grace, because it is also
// the answer for a directory the process may not write, and the lock timeout
// may be no limit at all; tried once, it is not waited on at all. Either way
// the run reports the open's own failure.
func TestWithMigrationDirectoryLock_ReportsARefusalThatOutlastsTheGrace(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
	}{
		{name: "waiting without a limit", timeout: 0},
		{name: "tried once", timeout: -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			handle := holdRemoved(c, dir)
			t.Cleanup(func() { _ = windows.CloseHandle(handle) })

			err := atlasmigrate.WithMigrationDirectoryLock(t.Context(), dir, test.timeout, func(context.Context) error { return nil })

			c.Assert(err, qt.ErrorIs, windows.ERROR_ACCESS_DENIED)
			c.Assert(err, qt.ErrorMatches, "open migration directory lock: .*")
		})
	}
}
