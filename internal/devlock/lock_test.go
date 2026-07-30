package devlock_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasurl"
	"github.com/stokaro/ptah/internal/devlock"
)

func TestAcquire_SQLiteSerializesSameRealm(t *testing.T) {
	c := qt.New(t)
	devURL := atlasurl.SQLiteURLFromPath(filepath.Join(t.TempDir(), "dev.db"))
	firstConn, err := dbschema.ConnectToDatabase(t.Context(), devURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(firstConn)
	secondConn, err := dbschema.ConnectToDatabase(t.Context(), devURL)
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
	secondLock, err := devlock.Acquire(t.Context(), secondConn, 0)
	c.Assert(err, qt.IsNil)
	c.Assert(secondLock.Release(), qt.IsNil)
}

func TestAcquire_SQLiteSeparatesDifferentRealms(t *testing.T) {
	c := qt.New(t)
	firstConn, err := dbschema.ConnectToDatabase(
		t.Context(),
		atlasurl.SQLiteURLFromPath(filepath.Join(t.TempDir(), "first.db")),
	)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(firstConn)
	secondConn, err := dbschema.ConnectToDatabase(
		t.Context(),
		atlasurl.SQLiteURLFromPath(filepath.Join(t.TempDir(), "second.db")),
	)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(secondConn)

	firstLock, err := devlock.Acquire(t.Context(), firstConn, 0)
	c.Assert(err, qt.IsNil)
	secondLock, err := devlock.Acquire(t.Context(), secondConn, 0)
	c.Assert(err, qt.IsNil)

	c.Assert(secondLock.Release(), qt.IsNil)
	c.Assert(firstLock.Release(), qt.IsNil)
}

func TestSameRealm_SQLite(t *testing.T) {
	c := qt.New(t)
	sharedURL := atlasurl.SQLiteURLFromPath(filepath.Join(t.TempDir(), "shared.db"))
	firstConn, err := dbschema.ConnectToDatabase(t.Context(), sharedURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(firstConn)
	secondConn, err := dbschema.ConnectToDatabase(t.Context(), sharedURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(secondConn)
	distinctConn, err := dbschema.ConnectToDatabase(
		t.Context(),
		atlasurl.SQLiteURLFromPath(filepath.Join(t.TempDir(), "distinct.db")),
	)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(distinctConn)

	same, err := devlock.SameRealm(t.Context(), firstConn, secondConn)
	c.Assert(err, qt.IsNil)
	c.Assert(same, qt.IsTrue)

	same, err = devlock.SameRealm(t.Context(), firstConn, distinctConn)
	c.Assert(err, qt.IsNil)
	c.Assert(same, qt.IsFalse)
}

func TestSameRealm_FailurePathRequiresConnections(t *testing.T) {
	c := qt.New(t)

	same, err := devlock.SameRealm(t.Context(), nil, nil)

	c.Assert(err, qt.ErrorMatches, `compare dev database realms requires two database connections`)
	c.Assert(same, qt.IsFalse)
}

func TestAcquire_SQLiteSerializesHardLinkAliases(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	databasePath := filepath.Join(dir, "dev.db")
	aliasPath := filepath.Join(dir, "dev-alias.db")
	firstConn, err := dbschema.ConnectToDatabase(
		t.Context(),
		atlasurl.SQLiteURLFromPath(databasePath),
	)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(firstConn)
	_, err = firstConn.ExecContext(t.Context(), "CREATE TABLE identity_probe (id INTEGER)")
	c.Assert(err, qt.IsNil)
	c.Assert(os.Link(databasePath, aliasPath), qt.IsNil)
	secondConn, err := dbschema.ConnectToDatabase(
		t.Context(),
		atlasurl.SQLiteURLFromPath(aliasPath),
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
