//go:build integration

package ydb_test

import (
	"context"
	"net/url"
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
	"ptah.run/internal/schemaclean"
	"ptah.run/internal/ydbrealm"
	"ptah.run/internal/ydburl"
)

// enterRealm creates a dev realm in the line's test database and returns its
// URL, removing the realm when the test ends.
func enterRealm(c *qt.C, line ydbLine) string {
	c.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	realmURL, release, err := ydbrealm.Enter(ctx, dbtarget.URL(c, line.engine))
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
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := c.Context()
			realmURL := enterRealm(c, line)
			realm := connect(c, realmURL)
			database := openYDB(c, line)

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
		})
	}
}

// The release removes the realm with everything in it, a topic included, and
// the directory of the realms with the last one.
func TestYDBDevRealm_ReleaseRemovesIt(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := c.Context()
			realmURL, release, err := ydbrealm.Enter(ctx, dbtarget.URL(c, line.engine))
			c.Assert(err, qt.IsNil)
			realm := connect(c, realmURL)
			c.Assert(realm.SchemaWriter().ExecuteSQL(ctx,
				"CREATE TABLE `app/t` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))"), qt.IsNil)
			c.Assert(realm.SchemaWriter().ExecuteSQL(ctx,
				"CREATE VIEW `v` WITH (security_invoker = TRUE) AS SELECT * FROM `app/t`"), qt.IsNil)
			c.Assert(realm.SchemaWriter().ExecuteSQL(ctx, "CREATE TOPIC `app/events` (CONSUMER `reader`)"), qt.IsNil)
			before := directoryNames(c, ctx, line, ydburl.RealmDirectory)

			release()

			c.Assert(before, qt.Contains, path.Base(realmPath(c, realmURL)))
			c.Assert(directoryNames(c, ctx, line), qt.Not(qt.Contains), ydburl.RealmDirectory)
		})
	}
}

// A reset of a realm the run claimed empties it, a view, a topic and a
// directory included, and the claim refuses a realm a run left something in.
func TestYDBDevRealm_ClaimAndReset(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := c.Context()
			realm := connect(c, enterRealm(c, line))

			baseline, claimErr := devclean.Claim(ctx, realm)
			c.Assert(claimErr, qt.IsNil)
			for _, statement := range []string{
				"CREATE TABLE `app/t` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))",
				"CREATE TABLE `u` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))",
				"CREATE VIEW `app/v` WITH (security_invoker = TRUE) AS SELECT * FROM `u`",
				"CREATE TOPIC `app/events` (CONSUMER `reader`)",
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

			c.Assert(reclaimErr, qt.ErrorMatches, `connected database is not clean: found topic "events" in schema "app"; .*`)
			c.Assert(resetErr, qt.IsNil)
			c.Assert(listErr, qt.IsNil)
			c.Assert(left, qt.HasLen, 0)
		})
	}
}

// Two realms of one database are two realms, a realm is not the database that
// holds it, and two connections to one realm are one realm: the lock a replay
// takes on a realm keeps a second replay out of it and no other.
func TestYDBDevRealm_Identity(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := c.Context()
			firstURL := enterRealm(c, line)
			first := connect(c, firstURL)
			firstAgain := connect(c, firstURL)
			second := connect(c, enterRealm(c, line))
			database := openYDB(c, line)

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
		})
	}
}

// A cleanup plan names every table the writer's DropAllTables drops: the
// migrator's tables, which the reader leaves out of every directory, in each
// directory that holds one. Without the probe the plan names users alone, and
// the cleanup drops four tables.
func TestYDBSchemaClean_NamesTheMigratorsTables(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := c.Context()
			realm := connect(c, enterRealm(c, line))
			for _, table := range []string{"users", "schema_migrations", "app/schema_migrations", "app/ptah_migration_tags"} {
				c.Assert(realm.SchemaWriter().ExecuteSQL(ctx,
					"CREATE TABLE `"+table+"` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))"), qt.IsNil)
			}

			plan, inspectErr := schemaclean.Inspect(ctx, realm)
			executed, executeErr := schemaclean.Execute(ctx, realm, schemaclean.Options{})
			left, readErr := dbschema.ReadSchemaWithSchemasContext(ctx, realm, nil)
			lister, ok := realm.SchemaWriter().(interface {
				TablesNamed(context.Context, []string) ([]dbreset.Object, error)
			})
			c.Assert(ok, qt.IsTrue)
			bookkeeping, findErr := lister.TablesNamed(ctx, []string{"schema_migrations", "ptah_migration_tags"})

			c.Assert(inspectErr, qt.IsNil)
			var planned []string
			for _, object := range plan.Objects {
				planned = append(planned, object.Type+" "+path.Join(object.Schema, object.Name))
			}
			c.Assert(planned, qt.ContentEquals, []string{
				"table users", "table schema_migrations", "table app/schema_migrations", "table app/ptah_migration_tags",
			})
			c.Assert(executeErr, qt.IsNil)
			c.Assert(executed.Objects, qt.HasLen, 4)
			c.Assert(readErr, qt.IsNil)
			c.Assert(left.Tables, qt.HasLen, 0)
			c.Assert(findErr, qt.IsNil)
			c.Assert(bookkeeping, qt.HasLen, 0)
		})
	}
}

// A realm is a directory, which the scheme service creates only for a user
// who holds ydb.granular.create_directory: one who may create tables is
// refused the directory with the right named, and creates a realm once
// granted it.
//
// DROP USER leaves the user's rights on /local behind, and a user created
// again under the name holds them, so the rights are revoked before the test
// and after it.
func TestYDBDevRealm_NamesTheRightItNeeds(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			admin := openYDB(c, line)
			const user, password = "ptahrealmtest", "realmrights1"
			forget := func(ctx context.Context) error {
				return admin.Writer().ExecuteSQL(ctx, "REVOKE ALL ON `/local` FROM "+user+";\nDROP USER IF EXISTS "+user)
			}
			c.Assert(admin.Writer().ExecuteSQL(c.Context(), "DROP USER IF EXISTS "+user), qt.IsNil)
			c.Assert(admin.Writer().ExecuteSQL(c.Context(), "CREATE USER "+user+" PASSWORD '"+password+"'"), qt.IsNil)
			c.Assert(admin.Writer().ExecuteSQL(c.Context(), "REVOKE ALL ON `/local` FROM "+user), qt.IsNil)
			c.Cleanup(func() { c.Check(forget(context.Background()), qt.IsNil) })
			grant := func(rights string) {
				c.Assert(admin.Writer().ExecuteSQL(c.Context(), "GRANT "+rights+" ON `/local` TO "+user), qt.IsNil)
			}
			grant("CONNECT, 'ydb.granular.create_table', 'ydb.granular.describe_schema', 'ydb.granular.remove_schema'")
			parsed, err := url.Parse(dbtarget.URL(c, line.engine))
			c.Assert(err, qt.IsNil)
			parsed.User = url.UserPassword(user, password)

			_, refusedRelease, refused := ydbrealm.Enter(c.Context(), parsed.String())
			refusedRelease()
			grant("'ydb.granular.create_directory'")
			realmURL, release, created := ydbrealm.Enter(c.Context(), parsed.String())
			c.Cleanup(release)

			c.Assert(refused, qt.ErrorMatches, `create the dev realm [0-9a-f]+ in /local: ydb: create directory `+
				`/local/ptah_dev/[0-9a-f]+: UNAUTHORIZED: .*; creating a directory needs the `+
				`ydb.granular.create_directory right on the database`)
			c.Assert(created, qt.IsNil)
			c.Assert(directoryNames(c, c.Context(), line, ydburl.RealmDirectory), qt.Contains, path.Base(realmPath(c, realmURL)))
		})
	}
}
