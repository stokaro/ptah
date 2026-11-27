//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// TestSchemaDiffFromAFileToASQLServerDatabaseE2E compares a schema file with
// a SQL Server database in both binaries. The names of both sides are
// resolved through the --to database, under its collation: identical sides
// plan nothing, a table that differs only in case is the same table under a
// case-insensitive collation, and two tables under a case-sensitive one.
// Without that, every such diff failed with an invalid identifier semantics
// snapshot (stokaro/ptah#6749).
func TestSchemaDiffFromAFileToASQLServerDatabaseE2E(t *testing.T) {
	const synced = "Schemas are synced, no changes to be made.\n"
	tests := []struct {
		name      string
		binary    clirun.Target
		collation string
		file      string
		want      string
	}{
		{
			name: "identical sides, native", binary: clirun.Ptah, collation: "SQL_Latin1_General_CP1_CI_AS",
			file: "CREATE TABLE [Orders] (id int NOT NULL);\n", want: synced,
		},
		{
			name: "identical sides, compatibility", binary: clirun.Compat, collation: "SQL_Latin1_General_CP1_CI_AS",
			file: "CREATE TABLE [Orders] (id int NOT NULL);\n", want: synced,
		},
		{
			name: "a case-only rename under a case-insensitive collation, native", binary: clirun.Ptah,
			collation: "SQL_Latin1_General_CP1_CI_AS", file: "CREATE TABLE [ORDERS] (id int NOT NULL);\n", want: synced,
		},
		{
			name: "a case-only rename under a case-insensitive collation, compatibility", binary: clirun.Compat,
			collation: "SQL_Latin1_General_CP1_CI_AS", file: "CREATE TABLE [ORDERS] (id int NOT NULL);\n", want: synced,
		},
		{
			name: "a case-only rename under a case-sensitive collation, native", binary: clirun.Ptah,
			collation: "Latin1_General_100_CS_AS", file: "CREATE TABLE [ORDERS] (id int NOT NULL);\n",
			want: "CREATE TABLE [dbo].[Orders] (\n  [id] INT NOT NULL\n);\nGO\nDROP TABLE [dbo].[ORDERS];\nGO\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := sqlServerDatabaseHolding(c, test.collation, "CREATE TABLE [Orders] (id int NOT NULL)")
			dev := sqlServerCollatedDatabase(c, test.collation)
			from := "file://" + schemaFilePath(c, "schema.sql", test.file)

			result := clirun.Run(c, test.binary, clirun.Options{},
				"schema", "diff", "--from", from, "--to", target, "--dev-url", dev)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			c.Assert(result.Stdout, qt.Equals, test.want)
			c.Assert(sqlServerObjects(c, target), qt.Equals, "Orders U")
		})
	}
}

// TestSchemaDiffFromAFileToASQLServerDatabaseWithoutADevDatabaseE2E is the
// native diff with no dev database at all. The --to database answers for
// the names, so the diff needs none, and it says nothing of a project file
// that is not there.
func TestSchemaDiffFromAFileToASQLServerDatabaseWithoutADevDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	target := sqlServerDatabaseHolding(c, "SQL_Latin1_General_CP1_CI_AS", "CREATE TABLE [Orders] (id int NOT NULL)")
	from := "file://" + schemaFilePath(c, "schema.sql", "CREATE TABLE [ORDERS] (id int NOT NULL);\n")

	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, "schema", "diff", "--from", from, "--to", target)

	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
	c.Assert(result.Stdout, qt.Equals, "Schemas are synced, no changes to be made.\n")
}
