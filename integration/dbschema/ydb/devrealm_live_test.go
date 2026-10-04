//go:build integration

package ydb_test

import (
	"context"
	"path"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devclean"
	"ptah.run/internal/devlock"
	"ptah.run/internal/migrateclean"
	"ptah.run/internal/ydbrealm"
	"ptah.run/internal/ydburl"
)

// enterRealm creates a dev realm in the test database and returns its URL,
// removing the realm when the test ends.
func enterRealm(c *qt.C) string {
	c.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	realmURL, release, err := ydbrealm.Enter(ctx, dbtarget.URL(c, dbtarget.YDB))
	c.Assert(err, qt.IsNil)
	c.Cleanup(release)
	return realmURL
}

// connect opens rawURL for the test.
func connect(c *qt.C, rawURL string) *dbschema.DatabaseConnection {
	c.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(ctx, rawURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// realmPath is the realm's directory relative to the database root.
func realmPath(c *qt.C, realmURL string) string {
	c.Helper()
	parsed, err := ydburl.Parse(realmURL)
	c.Assert(err, qt.IsNil)
	return parsed.RealmPath()
}

// A dev realm is an empty database of its own: a connection to it creates and
// reads tables under the realm's directory, which the database's own
// connection leaves out of what it reads, and its clean check sees what the
// run put there.
func TestYDBDevRealm_IsADatabaseOfItsOwn(t *testing.T) {
	c := qt.New(t)
	ctx := c.Context()
	realmURL := enterRealm(c)
	realm := connect(c, realmURL)
	database := openYDB(c)

	cleanBefore := migrateclean.DevRefusal(ctx, realm)
	c.Assert(realm.SchemaWriter().ExecuteSQL(ctx,
		"CREATE TABLE `app/t` (`id` Int64 NOT NULL, `v` Utf8, PRIMARY KEY (`id`))"), qt.IsNil)
	inRealm, readErr := dbschema.ReadSchemaWithSchemasContext(ctx, realm, nil)
	inDatabase, databaseErr := dbschema.ReadSchemaWithSchemasContext(ctx, database, nil)
	var rows int64
	countErr := database.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM `"+path.Join(realmPath(c, realmURL), "app", "t")+"`").Scan(&rows)
	cleanAfter := migrateclean.DevRefusal(ctx, realm)

	c.Assert(cleanBefore, qt.IsNil)
	c.Assert(readErr, qt.IsNil)
	c.Assert(inRealm.Tables, qt.HasLen, 1)
	c.Assert(inRealm.Tables[0].Schema+"|"+inRealm.Tables[0].Name, qt.Equals, "app|t")
	c.Assert(databaseErr, qt.IsNil)
	for _, table := range inDatabase.Tables {
		c.Assert(table.Schema, qt.Not(qt.Matches), ydburl.RealmDirectory+"(/.*)?")
	}
	c.Assert(countErr, qt.IsNil)
	c.Assert(rows, qt.Equals, int64(0))
	var notClean *migrateclean.NotCleanError
	c.Assert(cleanAfter, qt.ErrorAs, &notClean)
	c.Assert(notClean.Reason, qt.Equals, `found table "t" in schema "app"`)
}

// The release removes the realm with everything in it, and the directory of
// the realms with the last one.
func TestYDBDevRealm_ReleaseRemovesIt(t *testing.T) {
	c := qt.New(t)
	ctx := c.Context()
	realmURL, release, err := ydbrealm.Enter(ctx, dbtarget.URL(c, dbtarget.YDB))
	c.Assert(err, qt.IsNil)
	realm := connect(c, realmURL)
	c.Assert(realm.SchemaWriter().ExecuteSQL(ctx,
		"CREATE TABLE `app/t` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))"), qt.IsNil)
	c.Assert(realm.SchemaWriter().ExecuteSQL(ctx,
		"CREATE VIEW `v` WITH (security_invoker = TRUE) AS SELECT * FROM `app/t`"), qt.IsNil)
	before := directoryNames(c, ctx, ydburl.RealmDirectory)

	release()

	c.Assert(before, qt.Contains, path.Base(realmPath(c, realmURL)))
	c.Assert(directoryNames(c, ctx), qt.Not(qt.Contains), ydburl.RealmDirectory)
}

// A reset of a realm the run claimed empties it, a view and a directory
// included, and the claim refuses a realm a run left something in.
func TestYDBDevRealm_ClaimAndReset(t *testing.T) {
	c := qt.New(t)
	ctx := c.Context()
	realm := connect(c, enterRealm(c))

	baseline, claimErr := devclean.Claim(ctx, realm)
	c.Assert(claimErr, qt.IsNil)
	for _, statement := range []string{
		"CREATE TABLE `app/t` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))",
		"CREATE TABLE `u` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))",
		"CREATE VIEW `app/v` WITH (security_invoker = TRUE) AS SELECT * FROM `u`",
	} {
		c.Assert(realm.SchemaWriter().ExecuteSQL(ctx, statement), qt.IsNil)
	}
	_, reclaimErr := devclean.Claim(ctx, realm)
	resetErr := devclean.DatabaseRealmKeeping(ctx, realm, baseline)
	lister, ok := realm.SchemaWriter().(interface {
		ResetObjects(context.Context, dbreset.Scope) ([]dbreset.Object, error)
	})
	c.Assert(ok, qt.IsTrue)
	left, listErr := lister.ResetObjects(ctx, dbreset.Scope{})

	c.Assert(reclaimErr, qt.ErrorMatches, `connected database is not clean: found table "t" in schema "app"; .*`)
	c.Assert(resetErr, qt.IsNil)
	c.Assert(listErr, qt.IsNil)
	c.Assert(left, qt.HasLen, 0)
}

// Two realms of one database are two realms, a realm is not the database that
// holds it, and two connections to one realm are one realm: the lock a replay
// takes on a realm keeps a second replay out of it and no other.
func TestYDBDevRealm_Identity(t *testing.T) {
	c := qt.New(t)
	ctx := c.Context()
	firstURL := enterRealm(c)
	first := connect(c, firstURL)
	firstAgain := connect(c, firstURL)
	second := connect(c, enterRealm(c))
	database := openYDB(c)

	sameRealm, sameErr := devlock.SameRealm(ctx, first, firstAgain)
	twoRealms, twoErr := devlock.SameRealm(ctx, first, second)
	realmAndDatabase, databaseErr := devlock.SameRealm(ctx, first, database)
	held, holdErr := devlock.Acquire(ctx, first, 0)
	c.Assert(holdErr, qt.IsNil)
	c.Cleanup(func() { c.Check(held.Release(), qt.IsNil) })
	_, waitedErr := devlock.Acquire(ctx, firstAgain, -1)
	other, otherErr := devlock.Acquire(ctx, second, -1)
	c.Assert(otherErr, qt.IsNil)
	c.Check(other.Release(), qt.IsNil)

	c.Assert(sameErr, qt.IsNil)
	c.Assert(sameRealm, qt.IsTrue)
	c.Assert(twoErr, qt.IsNil)
	c.Assert(twoRealms, qt.IsFalse)
	c.Assert(databaseErr, qt.IsNil)
	c.Assert(realmAndDatabase, qt.IsFalse)
	c.Assert(waitedErr, qt.ErrorMatches, `acquire dev database realm lock: .*`)
}
