//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package devlock_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/devlock"
)

func TestAcquire_SQLiteSerializesSymlinkAliases(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	databasePath := filepath.Join(dir, "dev.db")
	aliasPath := filepath.Join(dir, "dev-alias.db")
	firstConn, err := dbschema.ConnectToDatabase(
		t.Context(),
		"sqlite://"+databasePath,
	)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(firstConn)
	_, err = firstConn.ExecContext(t.Context(), "CREATE TABLE identity_probe (id INTEGER)")
	c.Assert(err, qt.IsNil)
	c.Assert(os.Symlink(databasePath, aliasPath), qt.IsNil)
	secondConn, err := dbschema.ConnectToDatabase(
		t.Context(),
		"sqlite://"+aliasPath,
	)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(secondConn)

	firstLock, err := devlock.Acquire(t.Context(), firstConn, 0)
	c.Assert(err, qt.IsNil)
	waitCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	blockedLock, err := devlock.Acquire(waitCtx, secondConn, 0)

	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
	c.Assert(blockedLock, qt.IsNil)
	c.Assert(firstLock.Release(), qt.IsNil)
}
