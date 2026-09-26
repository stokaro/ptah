package atlasschema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
)

// seedSQLiteDatabase creates a SQLite database holding one table and returns
// its path.
func seedSQLiteDatabase(c *qt.C, table string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "held.db")
	conn := connectSQLite(c, path)
	defer dbschema.CloseAndWarn(conn)
	_, err := conn.ExecContext(c.Context(), "CREATE TABLE "+table+" (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	return path
}

// writeIsolationSources writes a hashed migration directory and a schema file,
// each creating one table, and returns their paths.
func writeIsolationSources(c *qt.C) (dir, schema string) {
	c.Helper()
	root := c.TempDir()
	dir = filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_t.sql"),
		[]byte("CREATE TABLE t (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	schema = filepath.Join(root, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE t (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	return dir, schema
}

// A diff that resets the dev database refuses a --to database the dev URL
// also names, before either is opened, and the database keeps its table.
// Without the refusal the reset dropped every table in the --to database and
// the diff planned to create them again.
func TestDiff_FailurePath_RefusesADevURLThatNamesTheTarget(t *testing.T) {
	tests := []struct {
		name string
		from string
	}{
		{name: "a migration directory replayed on the dev database", from: "directory"},
		{name: "a schema file materialized on the dev database", from: "schema file"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, schema := writeIsolationSources(c)
			sources := map[string]string{"directory": "file://" + dir, "schema file": "file://" + schema}
			held := seedSQLiteDatabase(c, "keepme")
			url := atlasurl.SQLiteURLFromPath(held)

			report, err := atlasschema.Diff(c.Context(), atlasschema.DiffOptions{
				FromURLs: []string{sources[test.from]},
				ToURLs:   []string{url},
				DevURL:   url,
			})

			c.Assert(err, qt.ErrorMatches, `--to database must differ from --dev-url because the dev database is reset during planning`)
			c.Assert(report.Changes, qt.HasLen, 0)
			c.Assert(sqliteTableExists(c, held, "keepme"), qt.IsTrue)
		})
	}
}

// The control: a diff between two databases never resets the dev database, so
// a dev URL naming one of them is not refused.
func TestDiff_ADevURLNamingADatabaseIsAcceptedWhenNothingIsReset(t *testing.T) {
	c := qt.New(t)
	held := seedSQLiteDatabase(c, "keepme")
	other := seedSQLiteDatabase(c, "keepme")
	url := atlasurl.SQLiteURLFromPath(held)

	report, err := atlasschema.Diff(c.Context(), atlasschema.DiffOptions{
		FromURLs: []string{url},
		ToURLs:   []string{atlasurl.SQLiteURLFromPath(other)},
		DevURL:   url,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Changes, qt.HasLen, 0)
	c.Assert(sqliteTableExists(c, held, "keepme"), qt.IsTrue)
}
