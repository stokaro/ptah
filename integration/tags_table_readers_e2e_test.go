//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// The table `migrations tag` records in is Ptah's bookkeeping, as its revision
// tables are, and every reader leaves it out. Read as an ordinary table, it is
// one no declaration has: `schema apply` planned `DROP TABLE IF EXISTS
// "ptah_migration_tags"` against a database whose only difference from the
// declaration was a recorded tag, measured on SQLite, PostgreSQL 18 and MySQL
// 8.4, and `db read` listed it.
//
// Each row applies one migration, records a tag, and then asks `schema apply
// --dry-run` for the declaration the migration created: the schema is synced,
// so the plan is empty, and `db read` names the declared table and not the
// tags table.
func TestReadersLeaveTheTagsTableOutE2E(t *testing.T) {
	for _, test := range tagsTableCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url := test.url(c)
			dir := c.TempDir()
			migrations := filepath.Join(dir, "migrations")
			schemaFile := filepath.Join(dir, "schema.sql")
			c.Assert(os.WriteFile(schemaFile, []byte(revisionSQLiteFirst), 0o600), qt.IsNil)
			out, err := runPtahNativeWithError("migrations", "generate", "--db-url", url,
				"--schema-file", schemaFile, "--migrations-dir", migrations, "--name", "first")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			out, err = runPtahNativeWithError("migrations", "up", "--db-url", url, "--migrations-dir", migrations)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			out, err = runPtahNativeWithError("migrations", "tag", "release-1", "--db-url", url)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

			planned, planErr := runPtahNativeWithError("schema", "apply", "--db-url", url,
				"--schema-file", schemaFile, "--dry-run")
			read, readErr := runPtahNativeWithError("db", "read", "--db-url", url)

			c.Assert(planErr, qt.IsNil, qt.Commentf("%s", planned))
			c.Assert(planned, qt.Not(qt.Contains), "ptah_migration_tags")
			c.Assert(planned, qt.Not(qt.Contains), "DROP")
			c.Assert(readErr, qt.IsNil, qt.Commentf("%s", read))
			c.Assert(read, qt.Contains, "asn")
			c.Assert(read, qt.Not(qt.Contains), "ptah_migration_tags")
		})
	}
}

// tagsTableCase is one engine the tags table is read on.
type tagsTableCase struct {
	name string
	// url opens a scratch database for the row.
	url func(c *qt.C) string
}

// tagsTableCases are the engines TestReadersLeaveTheTagsTableOutE2E reads.
var tagsTableCases = []tagsTableCase{
	{name: "SQLite", url: sqliteScratchURL},
	{name: "PostgreSQL", url: postgresTagsScratchURL},
	{name: "MySQL", url: mySQLTagsScratchURL},
}

// postgresTagsScratchURL is a scratch database on PostgreSQL.
func postgresTagsScratchURL(c *qt.C) string {
	c.Helper()
	return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL),
		"DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
}

// mySQLTagsScratchURL is a scratch database on MySQL, reached with the
// administrative account that created it.
func mySQLTagsScratchURL(c *qt.C) string {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.MySQLAdmin)
	admin := connectDevDialect(c, adminURL)
	name := devDialectScratchName()
	_, err := admin.ExecContext(c.Context(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name)
		c.Check(dropErr, qt.IsNil)
	})
	return replaceMySQLDatabaseName(c, asMySQLURL(adminURL), name)
}
