//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// Only a SQL Server database can say whether two names are one: its collation
// decides, and Ptah's offline rules know no collation. A diff of two schema
// files asks the dev database, whether its URL is a flag or the project
// file's, and a command that runs a change on a dev or shadow database before
// the target refuses one whose collation reads names otherwise than the
// target's.

// schemaFilePath writes content to a schema file named name and returns its
// path, with forward slashes so that it reads as a file:// URL too.
func schemaFilePath(c *qt.C, name, content string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), name)
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	return filepath.ToSlash(path)
}

// withDatabases replaces each placeholder in args with its value, given as
// pairs.
func withDatabases(args []string, replacements ...string) []string {
	replacer := strings.NewReplacer(replacements...)
	replaced := make([]string, len(args))
	for position, arg := range args {
		replaced[position] = replacer.Replace(arg)
	}
	return replaced
}

// TestSchemaDiffOfTwoSchemaFilesPairsNamesAsTheSQLServerDevDatabaseDoesE2E
// compares two schema files on a SQL Server dev database. Two unrelated names
// with non-ASCII letters are two objects, and two names that differ only in
// case are one object under a case-insensitive collation and two under a
// case-sensitive one. Two ASCII names that differ beyond case are one name
// under a collation with contractions: Hungarian_CI_AS reads `ccs` as `cscs`,
// so the plan replaces the one function, as it replaces a function whose body
// changed, rather than creating `cscs` and dropping `ccs`, which on that
// database would leave neither.
func TestSchemaDiffOfTwoSchemaFilesPairsNamesAsTheSQLServerDevDatabaseDoesE2E(t *testing.T) {
	const (
		grosse     = "CREATE TABLE [Größe] (id int NOT NULL);\n"
		zaehler    = "CREATE TABLE [Zähler] (id int NOT NULL);\n"
		views      = "CREATE VIEW [Größe] AS SELECT 1 AS a;\nGO\nCREATE VIEW [v2] AS SELECT 2 AS a;\nGO\n"
		mixed      = "CREATE VIEW [MixedView] AS SELECT 1 AS a;\n"
		mixedNew   = "CREATE VIEW [MIXEDVIEW] AS SELECT 2 AS a;\n"
		contracted = "CREATE FUNCTION [dbo].[ccs]() RETURNS int AS BEGIN RETURN 1 END;\nGO\n"
		expanded   = "CREATE FUNCTION [dbo].[cscs]() RETURNS int AS BEGIN RETURN 2 END;\nGO\n"
	)
	tests := []struct {
		name      string
		binary    clirun.Target
		collation string
		from, to  string
		want      string
	}{
		{
			name: "two non-ASCII tables, native", binary: clirun.Ptah, collation: "SQL_Latin1_General_CP1_CI_AS",
			from: grosse, to: zaehler,
			want: "CREATE TABLE [Zähler] (\n  [id] int NOT NULL\n);\nGO\nDROP TABLE [Größe];\nGO\n",
		},
		{
			name: "two non-ASCII tables, compatibility", binary: clirun.Compat, collation: "SQL_Latin1_General_CP1_CI_AS",
			from: grosse, to: zaehler,
			want: "-- Create \"Zähler\" table\nCREATE TABLE [Zähler] ([id] int NOT NULL);\nGO\n" +
				"-- Drop \"Größe\" table\nDROP TABLE [Größe];\nGO\n",
		},
		{
			name: "a non-ASCII view beside another, native", binary: clirun.Ptah, collation: "SQL_Latin1_General_CP1_CI_AS",
			to:   views,
			want: "CREATE VIEW [Größe] AS\nSELECT 1 AS a;\nGO\nCREATE VIEW [v2] AS\nSELECT 2 AS a;\nGO\n",
		},
		{
			name: "a non-ASCII view beside another, compatibility", binary: clirun.Compat, collation: "SQL_Latin1_General_CP1_CI_AS",
			to: views,
			want: "-- Create \"Größe\" view\nCREATE VIEW [Größe] AS\nSELECT 1 AS a;\nGO\n" +
				"-- Create \"v2\" view\nCREATE VIEW [v2] AS\nSELECT 2 AS a;\nGO\n",
		},
		{
			name: "a case-only view under a case-insensitive collation", binary: clirun.Ptah, collation: "SQL_Latin1_General_CP1_CI_AS",
			from: mixed, to: mixedNew,
			want: "CREATE OR ALTER VIEW [MIXEDVIEW] AS\nSELECT 2 AS a;\nGO\n",
		},
		{
			name: "a case-only view under a case-sensitive collation", binary: clirun.Ptah, collation: "Latin1_General_100_CS_AS",
			from: mixed, to: mixedNew,
			want: "CREATE VIEW [MIXEDVIEW] AS\nSELECT 2 AS a;\nGO\nDROP VIEW [MixedView];\nGO\n",
		},
		{
			name: "a contraction under a Hungarian collation", binary: clirun.Ptah, collation: "Hungarian_CI_AS",
			from: contracted, to: expanded,
			want: "DROP FUNCTION [dbo].[cscs];\nGO\nCREATE OR ALTER FUNCTION [dbo].[cscs]()\nRETURNS int\nAS\nBEGIN RETURN 2 END;\nGO\n",
		},
		{
			name: "a contraction under a Latin collation", binary: clirun.Ptah, collation: "SQL_Latin1_General_CP1_CI_AS",
			from: contracted, to: expanded,
			want: "CREATE OR ALTER FUNCTION [dbo].[cscs]()\nRETURNS int\nAS\nBEGIN RETURN 2 END;\nGO\n" +
				"DROP FUNCTION [dbo].[ccs];\nGO\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dev := sqlServerCollatedDatabase(c, test.collation)
			from := "file://" + schemaFilePath(c, "from.sql", test.from)
			to := "file://" + schemaFilePath(c, "to.sql", test.to)

			result := clirun.Run(c, test.binary, clirun.Options{},
				"schema", "diff", "--from", from, "--to", to, "--dev-url", dev)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			c.Assert(result.Stdout, qt.Equals, test.want)
		})
	}
}

// TestSchemaDiffAsksTheSQLServerDevDatabaseOfTheProjectFileE2E compares two
// schema files with the dev database named only in the project file, beside
// a --server-version that pins what the plan may use. The names are still
// the server's to compare, so the diff asks the dev database for them: two
// unrelated non-ASCII tables are two objects, and the Hungarian contraction
// is one function.
func TestSchemaDiffAsksTheSQLServerDevDatabaseOfTheProjectFileE2E(t *testing.T) {
	tests := []struct {
		name      string
		collation string
		from, to  string
		want      string
	}{
		{
			name: "two non-ASCII tables", collation: "SQL_Latin1_General_CP1_CI_AS",
			from: "CREATE TABLE [Größe] (id int NOT NULL);\n", to: "CREATE TABLE [Zähler] (id int NOT NULL);\n",
			want: "CREATE TABLE [Zähler] (\n  [id] int NOT NULL\n);\nGO\nDROP TABLE [Größe];\nGO\n",
		},
		{
			name: "a contraction under a Hungarian collation", collation: "Hungarian_CI_AS",
			from: "CREATE FUNCTION [dbo].[ccs]() RETURNS int AS BEGIN RETURN 1 END;\nGO\n",
			to:   "CREATE FUNCTION [dbo].[cscs]() RETURNS int AS BEGIN RETURN 2 END;\nGO\n",
			want: "DROP FUNCTION [dbo].[cscs];\nGO\nCREATE OR ALTER FUNCTION [dbo].[cscs]()\nRETURNS int\nAS\nBEGIN RETURN 2 END;\nGO\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			dev := sqlServerCollatedDatabase(c, test.collation)
			c.Assert(os.WriteFile(filepath.Join(dir, "ptah.yaml"), []byte("dev: "+strconv.Quote(dev)+"\n"), 0o600), qt.IsNil)
			from := "file://" + schemaFilePath(c, "from.sql", test.from)
			to := "file://" + schemaFilePath(c, "to.sql", test.to)

			result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
				"schema", "diff", "--from", from, "--to", to, "--server-version", "17")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			c.Assert(result.Stdout, qt.Equals, test.want)
		})
	}
}

// TestSchemaDiffFailsWhenTheSQLServerDevDatabaseOfTheProjectFileDoesNotAnswerE2E
// compares two SQL Server schema files with a dev database in the project
// file that nothing answers on. Without the server nothing says how the names
// compare, so the run fails where a PostgreSQL diff plans for the dialect
// default (TestDevURLInTheProjectFileThatDoesNotAnswerIsNotedByADiffOfTwoDocumentsE2E),
// with or without a --server-version.
func TestSchemaDiffFailsWhenTheSQLServerDevDatabaseOfTheProjectFileDoesNotAnswerE2E(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "without a server version"},
		{name: "beside a server version", args: []string{"--server-version", "17"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			closed := "sqlserver://sa:closed-port-secret@127.0.0.1:1?database=dev&encrypt=disable"
			c.Assert(os.WriteFile(filepath.Join(dir, "ptah.yaml"), []byte("dev: "+strconv.Quote(closed)+"\n"), 0o600), qt.IsNil)
			from := "file://" + schemaFilePath(c, "from.sql", "CREATE TABLE [ccs] (id int NOT NULL);\n")
			to := "file://" + schemaFilePath(c, "to.sql", "CREATE TABLE [cscs] (id int NOT NULL);\n")

			result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
				append([]string{"schema", "diff", "--from", from, "--to", to}, test.args...)...)

			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(result.Stderr, qt.Contains, "two schema files planned for sqlserver are compared only under the names "+
				"a connected server compares, so the dev database named in the project file must answer: connect to the dev "+
				"database named in the project file: ")
			c.Assert(result.Stdout, qt.Equals, "")
		})
	}
}

// sqlServerApplyCommands are `schema apply` in each binary, with the target
// and the dev database to rehearse on.
var sqlServerApplyCommands = []struct {
	name   string
	binary clirun.Target
	args   []string
}{
	{
		name: "native", binary: clirun.Ptah,
		args: []string{"schema", "apply", "--db-url", "{target}", "--dev-url", "{dev}", "--schema-file", "{schema}", "--auto-approve"},
	},
	{
		name: "compatibility", binary: clirun.Compat,
		args: []string{"schema", "apply", "--url", "{target}", "--dev-url", "{dev}", "--to", "file://{schema}", "--auto-approve"},
	},
}

// tableCount reads how many tables named name the database at url holds.
func tableCount(c *qt.C, url, name string) string {
	c.Helper()
	return readCaseOnlyFact(c, url, "SELECT CONCAT(COUNT(*), N'') FROM sys.tables WHERE name = N'"+name+"'")
}

// TestSchemaApplyRehearsesOnASQLServerDevDatabaseOfTheTargetsCollationE2E is
// the control for the refusal below: a dev database whose collation has
// another name but agrees with the target's on case, accent, width and kana
// is a rehearsal, and the table reaches the target.
func TestSchemaApplyRehearsesOnASQLServerDevDatabaseOfTheTargetsCollationE2E(t *testing.T) {
	for _, test := range sqlServerApplyCommands {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := sqlServerCollatedDatabase(c, "SQL_Latin1_General_CP1_CI_AS")
			dev := sqlServerCollatedDatabase(c, "Latin1_General_100_CI_AS")
			schema := schemaFilePath(c, "schema.sql", "CREATE TABLE rehearsed (id int NOT NULL);\n")

			result := clirun.Run(c, test.binary, clirun.Options{},
				withDatabases(test.args, "{target}", target, "{dev}", dev, "{schema}", schema)...)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			c.Assert(tableCount(c, target, "rehearsed"), qt.Equals, "1")
		})
	}
}

// TestSchemaApplyRefusesASQLServerDevDatabaseOfAnotherCollationE2E rehearses
// a change for a case-insensitive target on a case-sensitive dev database.
// The dev database would hold MixedCase and MIXEDCASE as two objects where the
// target holds one, so the rehearsal proves nothing about the target; the
// command refuses, naming both collations, and the target is left as it was.
func TestSchemaApplyRefusesASQLServerDevDatabaseOfAnotherCollationE2E(t *testing.T) {
	for _, test := range sqlServerApplyCommands {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := sqlServerCollatedDatabase(c, "SQL_Latin1_General_CP1_CI_AS")
			dev := sqlServerCollatedDatabase(c, "Latin1_General_100_CS_AS")
			schema := schemaFilePath(c, "schema.sql", "CREATE TABLE rehearsed (id int NOT NULL);\n")

			result := clirun.Run(c, test.binary, clirun.Options{},
				withDatabases(test.args, "{target}", target, "{dev}", dev, "{schema}", schema)...)

			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(result.Stderr, qt.Contains,
				"the --dev-url database compares names under the collation Latin1_General_100_CS_AS and the target under "+
					"SQL_Latin1_General_CP1_CI_AS, which differ in case sensitivity")
			c.Assert(result.Stderr, qt.Contains, "create the --dev-url database with COLLATE SQL_Latin1_General_CP1_CI_AS")
			c.Assert(tableCount(c, target, "rehearsed"), qt.Equals, "0")
		})
	}
}

// TestSchemaApplyRefusesASQLServerDevDatabaseOfAnotherWidthOrKanaSensitivityE2E
// rehearses on a dev database that agrees with the target on case and accent
// and differs in width or kana sensitivity: under Latin1_General_100_CI_AS_WS
// the full-width Ｍ is another letter than M, and under _KS the katakana ア
// another syllable than the hiragana あ, where the target reads each pair as
// one name.
func TestSchemaApplyRefusesASQLServerDevDatabaseOfAnotherWidthOrKanaSensitivityE2E(t *testing.T) {
	tests := []struct {
		name        string
		dev         string
		sensitivity string
	}{
		{name: "width", dev: "Latin1_General_100_CI_AS_WS", sensitivity: "width"},
		{name: "kana", dev: "Latin1_General_100_CI_AS_KS", sensitivity: "kana"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := sqlServerCollatedDatabase(c, "Latin1_General_100_CI_AS")
			dev := sqlServerCollatedDatabase(c, test.dev)
			schema := schemaFilePath(c, "schema.sql", "CREATE TABLE rehearsed (id int NOT NULL);\n")

			result := clirun.Run(c, clirun.Ptah, clirun.Options{},
				"schema", "apply", "--db-url", target, "--dev-url", dev, "--schema-file", schema, "--auto-approve")

			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(result.Stderr, qt.Contains, "the --dev-url database compares names under the collation "+test.dev+
				" and the target under Latin1_General_100_CI_AS, which differ in "+test.sensitivity+" sensitivity")
			c.Assert(tableCount(c, target, "rehearsed"), qt.Equals, "0")
		})
	}
}

// sqlServerShadowCommands are the commands that verify a migration on a
// shadow database before they change the target, with the refusal each gives
// for a shadow database of another collation, the table it would change, and
// how many of that table the target holds after a refusal and after a
// verified run.
var sqlServerShadowCommands = []struct {
	name     string
	args     []string
	want     string
	table    string
	refused  string
	verified string
}{
	{
		name: "a verified rollback",
		args: []string{"migrations", "down", "--db-url", "{target}", "--migrations-dir", "{dir}",
			"--shadow-db", "{shadow}", "--limit", "1", "--confirm"},
		want:  "rollback verification failed: the shadow database ",
		table: "kept", refused: "1", verified: "0",
	},
	{
		name: "a rollback planned from the schema difference",
		args: []string{"migrations", "down", "--db-url", "{target}", "--migrations-dir", "{dir}",
			"--shadow-db", "{shadow}", "--plan", "--limit", "1", "--confirm"},
		want:  "dynamic rollback planning failed: the dev database ",
		table: "kept", refused: "1", verified: "0",
	},
	{
		name: "a drift-checked up",
		args: []string{"migrations", "up", "--db-url", "{target}", "--migrations-dir", "{dir}",
			"--shadow-db", "{shadow}", "--check-drift"},
		want:  "applied history shadow check failed: the shadow database ",
		table: "pending", refused: "0", verified: "1",
	},
}

// shadowRun is a case-insensitive target at the first of two migrations, with
// the second pending, and the arguments of a command against it and shadow.
func shadowRun(c *qt.C, args []string, shadow string) (target string, ran []string) {
	c.Helper()
	target = sqlServerCollatedDatabase(c, "SQL_Latin1_General_CP1_CI_AS")
	run := newCaseOnlyRun(c, target, "CREATE TABLE [kept] ([id] INT NOT NULL);\n", "DROP TABLE [kept];\n", "schema.sql", "")
	run.up(c)
	writeRoutineMigration(c, run.dir, "1700000001_pending.up.sql", "CREATE TABLE [pending] ([id] INT NOT NULL);\n")
	writeRoutineMigration(c, run.dir, "1700000001_pending.down.sql", "DROP TABLE [pending];\n")
	return target, withDatabases(args, "{target}", target, "{dir}", run.dir, "{shadow}", shadow)
}

// TestMigrationsVerifyOnASQLServerShadowDatabaseOfTheTargetsCollationE2E is
// the control for the refusal below: a shadow database whose collation has
// another name but agrees with the target's on case, accent, width and kana
// verifies the change, and the command changes the target.
func TestMigrationsVerifyOnASQLServerShadowDatabaseOfTheTargetsCollationE2E(t *testing.T) {
	for _, test := range sqlServerShadowCommands {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, args := shadowRun(c, test.args, sqlServerCollatedDatabase(c, "Latin1_General_100_CI_AS"))

			result := clirun.Run(c, clirun.Ptah, clirun.Options{}, args...)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			c.Assert(tableCount(c, target, test.table), qt.Equals, test.verified)
		})
	}
}

// TestMigrationsRefuseASQLServerShadowDatabaseOfAnotherCollationE2E verifies
// a migration of a case-insensitive target on a case-sensitive shadow
// database. The verification is refused before the shadow database is
// written, naming both collations, and the target keeps the state it had: the
// table a rollback would drop stays, and the pending migration does not run.
func TestMigrationsRefuseASQLServerShadowDatabaseOfAnotherCollationE2E(t *testing.T) {
	for _, test := range sqlServerShadowCommands {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, args := shadowRun(c, test.args, sqlServerCollatedDatabase(c, "Latin1_General_100_CS_AS"))

			result := clirun.Run(c, clirun.Ptah, clirun.Options{}, args...)

			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(result.Stderr, qt.Contains, test.want+"compares names under the collation Latin1_General_100_CS_AS "+
				"and the target under SQL_Latin1_General_CP1_CI_AS, which differ in case sensitivity")
			c.Assert(tableCount(c, target, test.table), qt.Equals, test.refused)
		})
	}
}
