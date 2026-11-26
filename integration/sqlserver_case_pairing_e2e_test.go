//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A SQL Server database compares object names under its collation. Under a
// case-insensitive one, MixedCase and MIXEDCASE are one procedure, so a schema
// that declares MIXEDCASE with another body changes MixedCase. Planned as an
// addition and a removal, `CREATE OR ALTER PROCEDURE [MIXEDCASE]` changed the
// procedure and `DROP PROCEDURE [dbo].[MixedCase]` then removed it, the down
// migration did the same in reverse, and every step reported success
// (stokaro/ptah#6722). Each test reads the database back after each step.

// sqlServerCollatedDatabase is an empty database on the suite's SQL Server,
// created under collation and dropped when the test ends.
func sqlServerCollatedDatabase(c *qt.C, collation string) string {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.SQLServer)
	admin := connectDevDialect(c, adminURL)
	name := devDialectScratchName()
	_, err := admin.ExecContext(c.Context(), "CREATE DATABASE "+name+" COLLATE "+collation)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(),
			fmt.Sprintf("ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s", name))
		c.Check(dropErr, qt.IsNil)
	})
	return replaceSQLServerDatabase(c, adminURL, name)
}

// caseOnlyChange is an object the first migration creates, the down file of
// that migration, a schema file declaring the object in another case with a
// changed definition, and a query that reads back what the server holds,
// with what it reads before and after the change.
type caseOnlyChange struct {
	name       string
	created    string
	removed    string
	schemaFile string
	schema     string
	read       string
	before     string
	after      string
}

// caseOnlyChanges is one row for each kind of object the case-insensitive
// test changes. Each query compares names under the database collation, so
// it reads the object whichever case the server stores it in.
var caseOnlyChanges = []caseOnlyChange{
	{
		name:       "procedure",
		created:    "CREATE OR ALTER PROCEDURE [MixedCase]\nAS\nBEGIN SELECT 1 END;\n",
		removed:    "DROP PROCEDURE [MixedCase];\n",
		schemaFile: "schema.hcl",
		schema:     "procedure \"MIXEDCASE\" {\n  as = \"BEGIN SELECT 2 END\"\n}\n",
		read: `SELECT CONCAT(COUNT(*), N' ', MAX(CASE WHEN m.definition LIKE N'%SELECT 2%' THEN N'new' ELSE N'old' END))
			FROM sys.procedures AS p JOIN sys.sql_modules AS m ON m.object_id = p.object_id
			WHERE p.name = N'mixedcase'`,
		before: "1 old",
		after:  "1 new",
	},
	{
		name:       "view",
		created:    "CREATE VIEW [MixedView] AS SELECT 1 AS a;\n",
		removed:    "DROP VIEW [MixedView];\n",
		schemaFile: "schema.sql",
		schema:     "CREATE VIEW MIXEDVIEW AS SELECT 2 AS a;\n",
		read: `SELECT CONCAT(COUNT(*), N' ', MAX(CASE WHEN m.definition LIKE N'%SELECT 2%' THEN N'new' ELSE N'old' END))
			FROM sys.views AS v JOIN sys.sql_modules AS m ON m.object_id = v.object_id
			WHERE v.name = N'mixedview'`,
		before: "1 old",
		after:  "1 new",
	},
	{
		name: "table",
		created: "CREATE TABLE [dbo].[MixedTable] ([id] INT NOT NULL PRIMARY KEY, [note] NVARCHAR(10) NULL);\nGO\n" +
			"INSERT INTO [dbo].[MixedTable] ([id], [note]) VALUES (1, N'kept');\n",
		removed:    "DROP TABLE [MixedTable];\n",
		schemaFile: "schema.sql",
		schema:     "CREATE TABLE MIXEDTABLE (id INT NOT NULL PRIMARY KEY, note NVARCHAR(20) NULL);\n",
		read: `SELECT CONCAT(
			(SELECT COUNT(*) FROM sys.tables WHERE name = N'mixedtable'), N' ',
			(SELECT max_length FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.mixedtable') AND name = N'note'), N' ',
			(SELECT note FROM dbo.mixedtable WHERE id = 1))`,
		before: "1 20 kept",
		after:  "1 40 kept",
	},
	{
		name:       "sequence",
		created:    "CREATE SEQUENCE [dbo].[MixedSeq] START WITH 1 INCREMENT BY 1;\n",
		removed:    "DROP SEQUENCE [MixedSeq];\n",
		schemaFile: "schema.sql",
		schema:     "CREATE SEQUENCE MIXEDSEQ START WITH 1 INCREMENT BY 2;\n",
		read: `SELECT CONCAT(COUNT(*), N' ', MAX(CONVERT(nvarchar(20), increment)))
			FROM sys.sequences WHERE name = N'mixedseq'`,
		before: "1 1",
		after:  "1 2",
	},
	{
		name: "trigger",
		created: "CREATE TABLE [dbo].[orders] ([id] INT NOT NULL PRIMARY KEY);\nGO\n" +
			"CREATE TRIGGER [MixedTrg] ON [dbo].[orders] AFTER INSERT AS BEGIN SET NOCOUNT ON; END;\n",
		removed:    "DROP TRIGGER [MixedTrg];\nDROP TABLE [orders];\n",
		schemaFile: "schema.sql",
		schema: "CREATE TABLE orders (id INT NOT NULL PRIMARY KEY);\n" +
			"CREATE TRIGGER MIXEDTRG ON orders AFTER INSERT AS BEGIN SET NOCOUNT ON; SELECT 2; END;\n",
		read: `SELECT CONCAT(COUNT(*), N' ', MAX(CASE WHEN m.definition LIKE N'%SELECT 2%' THEN N'new' ELSE N'old' END))
			FROM sys.triggers AS t JOIN sys.sql_modules AS m ON m.object_id = t.object_id
			WHERE t.name = N'mixedtrg'`,
		before: "1 old",
		after:  "1 new",
	},
}

// caseOnlyRun is a migration directory on one database, with the schema file
// the next migration is generated from.
type caseOnlyRun struct {
	url    string
	dir    string
	schema string
}

// newCaseOnlyRun writes the first migration and the schema file.
func newCaseOnlyRun(c *qt.C, url, created, removed, schemaFile, schema string) caseOnlyRun {
	c.Helper()
	run := caseOnlyRun{url: url, dir: c.TempDir(), schema: filepath.Join(c.TempDir(), schemaFile)}
	writeRoutineMigration(c, run.dir, "1700000000_init.up.sql", created)
	writeRoutineMigration(c, run.dir, "1700000000_init.down.sql", removed)
	c.Assert(os.WriteFile(run.schema, []byte(schema), 0o600), qt.IsNil)
	return run
}

// ptah runs the shipped binary with args against the run's database and
// directory and asserts that it succeeds.
func (r caseOnlyRun) ptah(c *qt.C, args ...string) clirun.Result {
	c.Helper()
	result := clirun.Run(c, clirun.Ptah, clirun.Options{}, args...)
	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("ptah %v\nstdout:\n%s\nstderr:\n%s", args, result.Stdout, result.Stderr))
	return result
}

func (r caseOnlyRun) up(c *qt.C) {
	c.Helper()
	r.ptah(c, "migrations", "up", "--db-url", r.url, "--migrations-dir", r.dir, "--allow-destructive")
}

func (r caseOnlyRun) generate(c *qt.C) {
	c.Helper()
	r.ptah(c, "migrations", "generate", "--db-url", r.url, "--migrations-dir", r.dir, "--schema-file", r.schema, "--name", "casing")
}

// plan is what the next migration would run.
func (r caseOnlyRun) plan(c *qt.C) string {
	c.Helper()
	return r.ptah(c, "migrations", "plan", "--db-url", r.url, "--schema-file", r.schema).Stdout
}

func (r caseOnlyRun) down(c *qt.C) {
	c.Helper()
	r.ptah(c, "migrations", "down", "--db-url", r.url, "--migrations-dir", r.dir, "--limit", "1", "--confirm")
}

// readCaseOnlyFact runs query on the database at url and returns its one value.
func readCaseOnlyFact(c *qt.C, url, query string) string {
	c.Helper()
	conn := connectDevDialect(c, url)
	var value string
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&value), qt.IsNil, qt.Commentf("%s", query))
	return value
}

// TestMigrationsChangeACaseOnlyNamesakeUnderACaseInsensitiveSQLServerCollationE2E
// declares each kind of object in another case than the database holds it,
// on a database whose collation is case-insensitive. The generated migration
// changes the object the database holds: after up it holds the new
// definition, a second plan is empty, and after down it holds the old one.
func TestMigrationsChangeACaseOnlyNamesakeUnderACaseInsensitiveSQLServerCollationE2E(t *testing.T) {
	for _, test := range caseOnlyChanges {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url := sqlServerCollatedDatabase(c, "SQL_Latin1_General_CP1_CI_AS")
			run := newCaseOnlyRun(c, url, test.created, test.removed, test.schemaFile, test.schema)
			run.up(c)
			before := readCaseOnlyFact(c, url, test.read)

			run.generate(c)
			run.up(c)
			after := readCaseOnlyFact(c, url, test.read)
			plan := run.plan(c)
			run.down(c)

			c.Assert(before, qt.Equals, test.before)
			c.Assert(after, qt.Equals, test.after)
			c.Assert(plan, qt.Contains, "Safety: no executable migration statements")
			c.Assert(readCaseOnlyFact(c, url, test.read), qt.Equals, test.before)
		})
	}
}

// TestMigrationsKeepCaseOnlyNamesakesApartUnderACaseSensitiveSQLServerCollationE2E
// declares two procedures whose names differ only in case on a database
// holding one of them, under a case-sensitive collation. They are two
// procedures: after up the database holds both, a second plan is empty, and
// after down it holds the first alone.
func TestMigrationsKeepCaseOnlyNamesakesApartUnderACaseSensitiveSQLServerCollationE2E(t *testing.T) {
	c := qt.New(t)
	url := sqlServerCollatedDatabase(c, "Latin1_General_100_CS_AS")
	run := newCaseOnlyRun(c, url,
		"CREATE OR ALTER PROCEDURE [MixedCase]\nAS\nBEGIN SELECT 1 END;\n",
		"DROP PROCEDURE [MixedCase];\n",
		"schema.hcl",
		"procedure \"MixedCase\" {\n  as = \"BEGIN SELECT 1 END\"\n}\nprocedure \"MIXEDCASE\" {\n  as = \"BEGIN SELECT 2 END\"\n}\n")
	const procedures = `SELECT STRING_AGG(CONVERT(nvarchar(max), p.name + N':' +
			CASE WHEN m.definition LIKE N'%SELECT 2%' THEN N'2' ELSE N'1' END), N',')
		WITHIN GROUP (ORDER BY p.name COLLATE Latin1_General_BIN)
		FROM sys.procedures AS p JOIN sys.sql_modules AS m ON m.object_id = p.object_id`
	run.up(c)

	run.generate(c)
	run.up(c)
	after := readCaseOnlyFact(c, url, procedures)
	plan := run.plan(c)
	run.down(c)

	c.Assert(after, qt.Equals, "MIXEDCASE:2,MixedCase:1")
	c.Assert(plan, qt.Contains, "Safety: no executable migration statements")
	c.Assert(readCaseOnlyFact(c, url, procedures), qt.Equals, "MixedCase:1")
}
