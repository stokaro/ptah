package devlock_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/dblock"
	"ptah.run/internal/devlock"
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

// TestAcquire_SQLiteRefusesAHeldRealmAtOnceUnderNoWait: a negative timeout
// tries the realm lock once. The context here would outlast any wait, so a
// refusal that came from it would name the deadline instead.
func TestAcquire_SQLiteRefusesAHeldRealmAtOnceUnderNoWait(t *testing.T) {
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
	defer func() { c.Check(firstLock.Release(), qt.IsNil) }()

	waitCtx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	blockedLock, err := devlock.Acquire(waitCtx, secondConn, dblock.NoWait)

	c.Assert(err, qt.ErrorMatches, `acquire sqlite dev database realm lock: lock is held by another process`)
	c.Assert(blockedLock, qt.IsNil)
	c.Assert(waitCtx.Err(), qt.IsNil)
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

// TestSameRealm_SQLiteInMemory compares connections opened separately to
// databases in memory, which name no file. Two private databases are two
// realms whatever their URLs say, and two connections to one shared-cache
// name reach one database.
func TestSameRealm_SQLiteInMemory(t *testing.T) {
	tests := []struct {
		name  string
		left  string
		right string
		want  bool
	}{
		{name: "two private databases under one URL", left: "sqlite://dev?mode=memory", right: "sqlite://dev?mode=memory", want: false},
		{name: "two unnamed private databases", left: "sqlite://:memory:", right: "sqlite://:memory:", want: false},
		{name: "two private names", left: "sqlite://target?mode=memory", right: "sqlite://dev?mode=memory", want: false},
		{name: "one shared-cache name", left: "sqlite://dev?mode=memory&cache=shared", right: "sqlite://dev?mode=memory&cache=shared", want: true},
		{name: "two shared-cache names", left: "sqlite://target?mode=memory&cache=shared", right: "sqlite://dev?mode=memory&cache=shared", want: false},
		{name: "a shared-cache name and a private one", left: "sqlite://dev?mode=memory&cache=shared", right: "sqlite://dev?mode=memory", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			left, err := dbschema.ConnectToDatabase(c.Context(), test.left)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(left) })
			right, err := dbschema.ConnectToDatabase(c.Context(), test.right)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(right) })

			same, err := devlock.SameRealm(c.Context(), left, right)

			c.Assert(err, qt.IsNil)
			c.Assert(same, qt.Equals, test.want)
		})
	}
}

// TestSameRealm_SQLiteInMemoryOnePool compares a private in-memory database's
// connection with itself, with the copy WithExecutor returns, and with the
// session copy WithSession hands its callback, which reach the one database
// the pool holds. The session copy pins the pool's one session, so asking the
// connection itself which file it holds would wait for that session forever.
func TestSameRealm_SQLiteInMemoryOnePool(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://dev?mode=memory")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	ctx, cancel := context.WithTimeout(c.Context(), 10*time.Second)
	c.Cleanup(cancel)
	var scopedSame bool
	var scopedErr error

	err = conn.WithSession(ctx, func(scoped *dbschema.DatabaseConnection) error {
		scopedSame, scopedErr = devlock.SameRealm(ctx, conn, scoped)
		return nil
	})
	sameCopy, copyErr := devlock.SameRealm(ctx, conn, conn.WithExecutor(conn.Writer()))
	same, sameErr := devlock.SameRealm(ctx, conn, conn)

	c.Assert(err, qt.IsNil)
	c.Assert(scopedErr, qt.IsNil)
	c.Assert(scopedSame, qt.IsTrue)
	c.Assert(copyErr, qt.IsNil)
	c.Assert(sameCopy, qt.IsTrue)
	c.Assert(sameErr, qt.IsNil)
	c.Assert(same, qt.IsTrue)
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

// A cleanup may run under a context no realm lock guards, and under one a lock
// that cannot be lost guards: a file lock, which SQLite takes. settle then
// returns the work's own answer.
func TestMayClean_WithoutALoss(t *testing.T) {
	c := qt.New(t)
	devURL := atlasurl.SQLiteURLFromPath(filepath.Join(t.TempDir(), "dev.db"))
	conn, err := dbschema.ConnectToDatabase(t.Context(), devURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	lock, err := devlock.Acquire(t.Context(), conn, 0)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(lock.Release(), qt.IsNil) }()

	guarded, settle := lock.Guard(t.Context())

	c.Assert(devlock.MayClean(t.Context()), qt.IsTrue)
	c.Assert(devlock.MayClean(guarded), qt.IsTrue)
	c.Assert(settle(nil), qt.IsNil)
}
