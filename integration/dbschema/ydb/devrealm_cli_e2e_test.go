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

// TestYDBBinary_DevDatabaseIsARealm drives the shipped binary's dev database
// verbs with the test database as the dev URL. Each run gets a dev realm of
// its own in that database, so the replay, the shadow verification and every
// parallel test case run against an empty database, nothing they create
// reaches the database's own schema, and the realms are gone when the binary
// exits.
func TestYDBBinary_DevDatabaseIsARealm(t *testing.T) {
	url := dbtarget.URL(t, dbtarget.YDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	binary := buildBinary(c, ctx)
	conn := openYDB(c)

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
		c.Assert(directoryNames(c, ctx), qt.Not(qt.Contains), "users")
		c.Assert(directoryNames(c, ctx), qt.Not(qt.Contains), ydburl.RealmDirectory)
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
		c.Assert(directoryNames(c, ctx), qt.Not(qt.Contains), ydburl.RealmDirectory)
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
		c.Assert(directoryNames(c, ctx), qt.Not(qt.Contains), "users")
		c.Assert(directoryNames(c, ctx), qt.Not(qt.Contains), ydburl.RealmDirectory)
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
		c.Assert(directoryNames(c, ctx), qt.Not(qt.Contains), "ptah_ydb_devrealm")
		c.Assert(directoryNames(c, ctx), qt.Not(qt.Contains), ydburl.RealmDirectory)
	})
}
