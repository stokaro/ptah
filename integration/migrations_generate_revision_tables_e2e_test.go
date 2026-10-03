//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// `ptah migrations generate` compares the database it reads with the desired
// state, and the migrator's revision tables are bookkeeping in that database.
// A plan that read them as tables no declaration has dropped the record of
// every applied migration: on ClickHouse 26.9.8.3 the up file ended with
// `DROP TABLE IF EXISTS schema_migrations;` and the same for the log, and on
// Oracle 23.26 with `DROP TABLE IF EXISTS PTAH.schema_migrations PURGE;`
// (stokaro/ptah#4029). A revision table under a custom name, or in the Atlas
// format, was dropped the same way on every dialect.
//
// Each row generates and applies one migration, generates again against a
// declaration that adds a column, and reads the second up file: it adds the
// column and names no revision table.

// revisionTablesCase is one engine and one revision-table setting.
type revisionTablesCase struct {
	name string
	// url opens a scratch database for the row.
	url func(c *qt.C) string
	// first and second are the declarations; second adds column m.
	first, second string
	// flags are the revision settings, passed to up and to generate alike.
	flags []string
	// addColumn is a fragment of the statement that adds m.
	addColumn string
	// tables are the bookkeeping tables the migrator writes under flags.
	tables []string
}

// sqliteScratchURL is a SQLite file in a temporary directory.
func sqliteScratchURL(c *qt.C) string {
	c.Helper()
	return "sqlite://" + filepath.ToSlash(filepath.Join(c.TempDir(), "app.db"))
}

// clickHouseScratchURL is a scratch ClickHouse database.
func clickHouseScratchURL(c *qt.C) string {
	c.Helper()
	return clickHouseDevDatabase(c).url
}

// oracleScratchURL is a scratch Oracle account.
func oracleScratchURL(c *qt.C) string {
	c.Helper()
	return oracleDevDatabase(c).url
}

const (
	revisionClickHouseFirst  = "CREATE TABLE asn (id Int32, n Int32) ENGINE = MergeTree ORDER BY id;\n"
	revisionClickHouseSecond = "CREATE TABLE asn (id Int32, n Int32, m Int32) ENGINE = MergeTree ORDER BY id;\n"
	revisionSQLiteFirst      = "CREATE TABLE asn (id INTEGER PRIMARY KEY, n INTEGER);\n"
	revisionSQLiteSecond     = "CREATE TABLE asn (id INTEGER PRIMARY KEY, n INTEGER, m INTEGER);\n"
	revisionOracleFirst      = "CREATE TABLE asn (id NUMBER(10) PRIMARY KEY, n NUMBER(10));\n"
	revisionOracleSecond     = "CREATE TABLE asn (id NUMBER(10) PRIMARY KEY, n NUMBER(10), m NUMBER(10));\n"
)

var revisionTablesCases = []revisionTablesCase{
	{
		name: "ClickHouse native", url: clickHouseScratchURL,
		first: revisionClickHouseFirst, second: revisionClickHouseSecond,
		addColumn: "ADD COLUMN m Int32", tables: []string{"schema_migrations", "schema_migrations_log"},
	},
	{
		name: "ClickHouse Atlas format", url: clickHouseScratchURL,
		first: revisionClickHouseFirst, second: revisionClickHouseSecond,
		flags:     []string{"--revision-format", "atlas"},
		addColumn: "ADD COLUMN m Int32", tables: []string{"atlas_schema_revisions"},
	},
	{
		name: "Oracle native", url: oracleScratchURL,
		first: revisionOracleFirst, second: revisionOracleSecond,
		addColumn: "ADD (m NUMBER(10))", tables: []string{"schema_migrations", "schema_migrations_log"},
	},
	{
		name: "SQLite Atlas format", url: sqliteScratchURL,
		first: revisionSQLiteFirst, second: revisionSQLiteSecond,
		flags:     []string{"--revision-format", "atlas"},
		addColumn: `ADD COLUMN "m" INTEGER`, tables: []string{"atlas_schema_revisions"},
	},
	{
		name: "SQLite custom table", url: sqliteScratchURL,
		first: revisionSQLiteFirst, second: revisionSQLiteSecond,
		flags:     []string{"--migrations-table", "custom_revs"},
		addColumn: `ADD COLUMN "m" INTEGER`, tables: []string{"custom_revs", "custom_revs_log"},
	},
}

func TestMigrationsGenerateLeavesTheRevisionTablesOutE2E(t *testing.T) {
	for _, test := range revisionTablesCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url := test.url(c)
			dir := c.TempDir()
			migrations := filepath.Join(dir, "migrations")
			first := filepath.Join(dir, "first.sql")
			second := filepath.Join(dir, "second.sql")
			c.Assert(os.WriteFile(first, []byte(test.first), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(second, []byte(test.second), 0o600), qt.IsNil)
			generate := func(schemaFile, name string) {
				args := append([]string{"migrations", "generate", "--db-url", url, "--schema-file", schemaFile,
					"--migrations-dir", migrations, "--name", name}, test.flags...)
				out, err := runPtahNativeWithError(args...)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			}

			generate(first, "first")
			out, err := runPtahNativeWithError(append([]string{"migrations", "up", "--db-url", url,
				"--migrations-dir", migrations}, test.flags...)...)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			generate(second, "second")

			ups, err := filepath.Glob(filepath.Join(migrations, "*_second.up.sql"))
			c.Assert(err, qt.IsNil)
			c.Assert(ups, qt.HasLen, 1)
			body, err := os.ReadFile(ups[0])
			c.Assert(err, qt.IsNil)
			c.Assert(string(body), qt.Contains, test.addColumn)
			for _, table := range test.tables {
				c.Assert(string(body), qt.Not(qt.Contains), table)
			}
		})
	}
}

// TestDBReadLeavesPtahsRevisionTablesOutE2E reads a database after `migrations
// up` with `ptah db read`, which takes no revision setting, so what it reports
// is the reader's alone. Ptah's own tables under their default names are left
// out; `migrations generate` relies on that for every other caller of a reader,
// such as `schema apply`.
func TestDBReadLeavesPtahsRevisionTablesOutE2E(t *testing.T) {
	for _, test := range []revisionTablesCase{revisionTablesCases[0], revisionTablesCases[2]} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url := test.url(c)
			dir := c.TempDir()
			migrations := filepath.Join(dir, "migrations")
			schemaFile := filepath.Join(dir, "first.sql")
			c.Assert(os.WriteFile(schemaFile, []byte(test.first), 0o600), qt.IsNil)
			out, err := runPtahNativeWithError("migrations", "generate", "--db-url", url,
				"--schema-file", schemaFile, "--migrations-dir", migrations, "--name", "first")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			out, err = runPtahNativeWithError("migrations", "up", "--db-url", url, "--migrations-dir", migrations)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

			out, err = runPtahNativeWithError("db", "read", "--db-url", url)

			// Oracle folds the unquoted table name to ASN, and an unquoted
			// revision table would fold the same way.
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(strings.ToLower(out), qt.Contains, "asn")
			c.Assert(strings.ToLower(out), qt.Not(qt.Contains), "schema_migrations")
		})
	}
}

// TestMigrationsPlanLeavesTheRevisionTablesOutE2E is the preview of the same
// plan: `migrations plan` reads the database on its own, and given the same
// revision settings it shows what `generate` writes, with no DROP for them.
func TestMigrationsPlanLeavesTheRevisionTablesOutE2E(t *testing.T) {
	for _, test := range []revisionTablesCase{revisionTablesCases[3], revisionTablesCases[4]} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url := test.url(c)
			dir := c.TempDir()
			migrations := filepath.Join(dir, "migrations")
			first := filepath.Join(dir, "first.sql")
			second := filepath.Join(dir, "second.sql")
			c.Assert(os.WriteFile(first, []byte(test.first), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(second, []byte(test.second), 0o600), qt.IsNil)
			out, err := runPtahNativeWithError(append([]string{"migrations", "generate", "--db-url", url,
				"--schema-file", first, "--migrations-dir", migrations, "--name", "first"}, test.flags...)...)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			out, err = runPtahNativeWithError(append([]string{"migrations", "up", "--db-url", url,
				"--migrations-dir", migrations}, test.flags...)...)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

			out, err = runPtahNativeWithError(append([]string{"migrations", "plan", "--db-url", url,
				"--schema-file", second}, test.flags...)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, test.addColumn)
			for _, table := range test.tables {
				c.Assert(out, qt.Not(qt.Contains), table)
			}
		})
	}
}
