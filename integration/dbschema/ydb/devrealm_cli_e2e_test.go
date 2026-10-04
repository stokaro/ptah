//go:build integration

package ydb_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
	"ptah.run/internal/ydburl"
)

// devRealmEntities declares a table in a directory of its own, for the
// shadow verification below.
const devRealmEntities = `package entities

//ptah:schema:table name="items" schema="ptah_ydb_devrealm"
type Item struct {
	//ptah:schema:field name="id" type="BIGINT" primary
	ID int64
	//ptah:schema:field name="title" type="VARCHAR(80)" not_null default="untitled"
	Title string
}
`

// writeFiles writes each body into dir under its name.
func writeFiles(c *qt.C, dir string, files map[string]string) {
	c.Helper()
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
}

// devRealmKept is a directory of the test database that holds a table, so
// the database is not empty: a dev database that were the database itself
// would be refused as not clean, or emptied.
const devRealmKept = "ptah_ydb_devrealm_kept"

// TestYDBBinary_DevDatabaseIsARealm drives the shipped binary's dev database
// verbs with the test database as the dev URL. Each run gets a dev realm of
// its own in that database, so the replay, the shadow verification and every
// parallel test case run against an empty database, nothing they create
// reaches the database's own schema, what the database holds stays, and the
// realms are gone when the binary exits.
func TestYDBBinary_DevDatabaseIsARealm(t *testing.T) {
	c := qt.New(t)
	binary := buildBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			dropDirectory(c, conn, devRealmKept, "t")
			c.Cleanup(func() { dropDirectory(c, conn, devRealmKept, "t") })
			c.Assert(conn.Writer().ExecuteSQL(ctx,
				"CREATE TABLE `"+devRealmKept+"/t` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))"), qt.IsNil)

			t.Run("a replay runs in a realm", func(t *testing.T) {
				c := qt.New(t)
				migrations := filepath.Join(c.TempDir(), "migrations")
				writeFiles(c, migrations, map[string]string{
					"0000000001_users.up.sql":   "CREATE TABLE `users` (`id` Int64 NOT NULL, `name` Utf8, PRIMARY KEY (`id`));\n",
					"0000000001_users.down.sql": "DROP TABLE `users`;\n",
				})
				hashed, hashErr := runBinary(ctx, binary, "migrations", "hash", "--dir", migrations)
				c.Assert(hashErr, qt.IsNil, qt.Commentf("%s", hashed))

				validated, err := runBinary(ctx, binary, "migrations", "validate", "--dir", migrations, "--dev-url", url)

				c.Assert(err, qt.IsNil, qt.Commentf("%s", validated))
				c.Assert(validated, qt.Contains, "OK: migration SQL validated on dev database")
				c.Assert(directoryNames(c, ctx, line), qt.Not(qt.Contains), "users")
				c.Assert(directoryNames(c, ctx, line), qt.Not(qt.Contains), ydburl.RealmDirectory)
			})

			t.Run("lint replays in a realm and reads its baseline there", func(t *testing.T) {
				c := qt.New(t)
				migrations := filepath.Join(c.TempDir(), "migrations")
				writeFiles(c, migrations, map[string]string{
					"0000000001_users.up.sql":   "CREATE TABLE `shop/users` (`id` Uint64 NOT NULL, PRIMARY KEY (`id`));\n",
					"0000000001_users.down.sql": "DROP TABLE `shop/users`;\n",
					"0000000002_score.up.sql":   "ALTER TABLE `shop/users` ADD COLUMN `score` Int32;\n",
					"0000000002_score.down.sql": "ALTER TABLE `shop/users` DROP COLUMN `score`;\n",
				})
				hashed, hashErr := runBinary(ctx, binary, "migrations", "hash", "--dir", migrations)
				c.Assert(hashErr, qt.IsNil, qt.Commentf("%s", hashed))

				linted, err := runBinary(ctx, binary, "migrations", "lint", "--dir", migrations, "--dev-url", url,
					"--fail-on", "none")

				c.Assert(err, qt.IsNil, qt.Commentf("%s", linted))
				c.Assert(linted, qt.Contains, "No lint findings.")
				c.Assert(linted, qt.Not(qt.Contains), "ran without the baseline schema")
				c.Assert(directoryNames(c, ctx, line), qt.Not(qt.Contains), "shop")
				c.Assert(directoryNames(c, ctx, line), qt.Not(qt.Contains), ydburl.RealmDirectory)
			})

			t.Run("a statement that reaches the whole database is refused", func(t *testing.T) {
				c := qt.New(t)
				migrations := filepath.Join(c.TempDir(), "migrations")
				writeFiles(c, migrations, map[string]string{
					"0000000001_reader.up.sql":   "CREATE USER ptahydbdevrealmreader PASSWORD 'secret';\n",
					"0000000001_reader.down.sql": "DROP USER ptahydbdevrealmreader;\n",
				})
				hashed, hashErr := runBinary(ctx, binary, "migrations", "hash", "--dir", migrations)
				c.Assert(hashErr, qt.IsNil, qt.Commentf("%s", hashed))

				refused, err := runBinary(ctx, binary, "migrations", "validate", "--dir", migrations, "--dev-url", url)
				var users int64
				countErr := conn.QueryRowContext(ctx,
					"SELECT COUNT(*) FROM `.sys/auth_users` WHERE Sid = 'ptahydbdevrealmreader'").Scan(&users)

				c.Assert(err, qt.IsNotNil)
				c.Assert(refused, qt.Contains, "ydb migration replay rejects a user of the whole database because its "+
					"effects cannot be confined to the disposable database realm")
				c.Assert(countErr, qt.IsNil)
				c.Assert(users, qt.Equals, int64(0))
				c.Assert(directoryNames(c, ctx, line), qt.Not(qt.Contains), ydburl.RealmDirectory)
			})

			t.Run("parallel test cases each get a realm", func(t *testing.T) {
				c := qt.New(t)
				root := c.TempDir()
				migrations := filepath.Join(root, "migrations")
				tests := filepath.Join(root, "tests")
				writeFiles(c, migrations, map[string]string{
					"0000000001_users.up.sql":   "CREATE TABLE `users` (`id` Int64 NOT NULL, `name` Utf8, PRIMARY KEY (`id`));\n",
					"0000000001_users.down.sql": "DROP TABLE `users`;\n",
				})
				writeFiles(c, tests, map[string]string{"cases.yaml": `cases:
  - name: one row
    parallel: true
    steps:
      - migrate_to: latest
      - exec: "UPSERT INTO ` + "`users`" + ` (id, name) VALUES (1, 'a'u)"
      - assert:
          query: "SELECT COUNT(*) FROM ` + "`users`" + `"
          scalar: "1"
  - name: two rows
    parallel: true
    steps:
      - migrate_to: latest
      - exec: "UPSERT INTO ` + "`users`" + ` (id, name) VALUES (2, 'b'u)"
      - exec: "UPSERT INTO ` + "`users`" + ` (id, name) VALUES (3, 'c'u)"
      - assert:
          query: "SELECT COUNT(*) FROM ` + "`users`" + `"
          scalar: "2"
`})

				tested, err := runBinary(ctx, binary, "migrations", "test", "--db-url", url, "--dir", tests,
					"--migrations-dir", migrations)

				c.Assert(err, qt.IsNil, qt.Commentf("%s", tested))
				c.Assert(tested, qt.Contains, `PASS  case "one row"`)
				c.Assert(tested, qt.Contains, `PASS  case "two rows"`)
				c.Assert(directoryNames(c, ctx, line), qt.Not(qt.Contains), "users")
				c.Assert(directoryNames(c, ctx, line), qt.Not(qt.Contains), ydburl.RealmDirectory)
			})

			t.Run("a shadow database that is the target is a realm in it", func(t *testing.T) {
				c := qt.New(t)
				root := c.TempDir()
				entities := filepath.Join(root, "entities")
				migrations := filepath.Join(root, "migrations")
				writeFiles(c, entities, map[string]string{"items.go": devRealmEntities})
				c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)

				generated, err := runBinary(ctx, binary, "migrations", "generate", "--db-url", url, "--root-dir", entities,
					"--migrations-dir", migrations, "--schemas", "ptah_ydb_devrealm", "--shadow-db", url, "--name", "items")
				written, globErr := filepath.Glob(filepath.Join(migrations, "*_items.up.sql"))

				c.Assert(err, qt.IsNil, qt.Commentf("%s", generated))
				c.Assert(globErr, qt.IsNil)
				c.Assert(written, qt.HasLen, 1)
				c.Assert(tableNames(readScoped(c, conn, []string{"ptah_ydb_devrealm"})), qt.HasLen, 0)
				c.Assert(directoryNames(c, ctx, line), qt.Not(qt.Contains), ydburl.RealmDirectory)
			})

			t.Run("a rehearsal on the target's URL runs in a realm", func(t *testing.T) {
				c := qt.New(t)
				entities := filepath.Join(c.TempDir(), "entities")
				writeFiles(c, entities, map[string]string{"items.go": devRealmEntities})
				dropDirectory(c, conn, "ptah_ydb_devrealm", "items")
				c.Cleanup(func() { dropDirectory(c, conn, "ptah_ydb_devrealm", "items") })

				applied, err := runBinary(ctx, binary, "schema", "apply", "--db-url", url, "--root-dir", entities,
					"--dev-url", url, "--auto-approve")

				c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
				c.Assert(applied, qt.Contains, "Schema apply completed successfully.")
				c.Assert(directoryNames(c, ctx, line, "ptah_ydb_devrealm"), qt.DeepEquals, []string{"items"})
				c.Assert(directoryNames(c, ctx, line), qt.Not(qt.Contains), ydburl.RealmDirectory)
			})

			c.Assert(directoryNames(c, ctx, line, devRealmKept), qt.DeepEquals, []string{"t"})
		})
	}
}

// devRealmRollback is the directory the rollback migrations below write to,
// with their revision table, so a cleanup that drops it leaves the test
// database as it was.
const devRealmRollback = "ptah_ydb_devrealm_rollback"

// TestYDBBinary_DevDatabaseOnAnotherServer points the dev and shadow URLs at
// the other line's server. Both servers name their database /local, so the
// database path cannot tell the two URLs apart, and the host cannot either:
// one YDB database answers on every node of its cluster. Each URL a run
// compares with the target therefore names a dev realm on the other server,
// which no comparison confuses with the target's database, and the rollback
// verification, the derived rollback and the rehearsal all run there.
func TestYDBBinary_DevDatabaseOnAnotherServer(t *testing.T) {
	c := qt.New(t)
	binary := buildBinary(c, c.Context())
	for i, line := range ydbLines {
		other := ydbLines[(i+1)%len(ydbLines)]
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			devURL := dbtarget.URL(t, other.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			dropDirectory(c, conn, "ptah_ydb_devrealm", "items")
			dropDirectory(c, conn, devRealmRollback, "notes")
			c.Cleanup(func() {
				dropDirectory(c, conn, "ptah_ydb_devrealm", "items")
				dropDirectory(c, conn, devRealmRollback, "notes")
			})
			root := c.TempDir()
			entities := filepath.Join(root, "entities")
			migrations := filepath.Join(root, "migrations")
			writeFiles(c, entities, map[string]string{"items.go": devRealmEntities})
			writeFiles(c, migrations, map[string]string{
				"0000000001_notes.up.sql": "CREATE TABLE `" + devRealmRollback + "/notes` " +
					"(`id` Int64 NOT NULL, PRIMARY KEY (`id`));\n",
				"0000000001_notes.down.sql": "DROP TABLE `" + devRealmRollback + "/notes`;\n",
			})
			migrationFlags := []string{"--db-url", url, "--migrations-dir", migrations,
				"--migrations-schema", devRealmRollback}

			up, upErr := runBinary(ctx, binary, append([]string{"migrations", "up"}, migrationFlags...)...)
			verified, verifyErr := runBinary(ctx, binary, append([]string{"migrations", "down", "--target", "0",
				"--shadow-db", devURL, "--confirm"}, migrationFlags...)...)
			upAgain, upAgainErr := runBinary(ctx, binary, append([]string{"migrations", "up"}, migrationFlags...)...)
			derived, deriveErr := runBinary(ctx, binary, append([]string{"migrations", "down", "--target", "0",
				"--plan", "--shadow-db", devURL, "--confirm"}, migrationFlags...)...)
			applied, applyErr := runBinary(ctx, binary, "schema", "apply", "--db-url", url, "--root-dir", entities,
				"--dev-url", devURL, "--auto-approve")

			c.Assert(upErr, qt.IsNil, qt.Commentf("%s", up))
			c.Assert(verifyErr, qt.IsNil, qt.Commentf("%s", verified))
			c.Assert(verified, qt.Contains, "Rollback plan verified on shadow database")
			c.Assert(verified, qt.Contains, "Database is now at version: 0")
			c.Assert(upAgainErr, qt.IsNil, qt.Commentf("%s", upAgain))
			c.Assert(deriveErr, qt.IsNil, qt.Commentf("%s", derived))
			c.Assert(derived, qt.Contains, "DROP TABLE `"+devRealmRollback+"/notes`")
			c.Assert(applyErr, qt.IsNil, qt.Commentf("%s", applied))
			c.Assert(applied, qt.Contains, "Schema apply completed successfully.")
			c.Assert(directoryNames(c, ctx, line, "ptah_ydb_devrealm"), qt.DeepEquals, []string{"items"})
			c.Assert(directoryNames(c, ctx, line, devRealmRollback), qt.Not(qt.Contains), "notes")
			c.Assert(directoryNames(c, ctx, other), qt.Not(qt.Contains), ydburl.RealmDirectory)
			c.Assert(tableNames(readScoped(c, openYDB(c, other), []string{"ptah_ydb_devrealm"})), qt.HasLen, 0)
		})
	}
}
