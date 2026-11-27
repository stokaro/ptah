//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// A SQL Server role name, and the name of a table, view, routine or sequence
// in a schema, is compared under the database collation. Measured on SQL
// Server 2025 under a case-insensitive collation: CREATE ROLE [APP] beside the
// role App fails with error 15023, and CREATE OR ALTER PROCEDURE
// [dbo].[REPORT] beside the view dbo.Report fails with error 2010
// (stokaro/ptah#6741).

// sqlServerRoles reads the roles the database at url holds that a user
// created, by their stored names.
func sqlServerRoles(c *qt.C, url string) string {
	c.Helper()
	return readCaseOnlyFact(c, url, `SELECT ISNULL(STRING_AGG(name COLLATE Latin1_General_BIN, N',')
			WITHIN GROUP (ORDER BY name COLLATE Latin1_General_BIN), N'')
		FROM sys.database_principals WHERE type = 'R' AND is_fixed_role = 0 AND name <> N'public'`)
}

// TestSchemaApplyPairsASQLServerRoleByCollationE2E declares the role APP on
// a database holding App. Under a case-insensitive collation they are one
// role, so nothing is planned; under a case-sensitive one APP is created.
func TestSchemaApplyPairsASQLServerRoleByCollationE2E(t *testing.T) {
	tests := []struct {
		collation string
		want      string
	}{
		{collation: "SQL_Latin1_General_CP1_CI_AS", want: "App"},
		{collation: "Latin1_General_100_CS_AS", want: "APP,App"},
	}
	for _, test := range tests {
		t.Run(test.collation, func(t *testing.T) {
			c := qt.New(t)
			target := sqlServerDatabaseHolding(c, test.collation, "CREATE TABLE t1 (id INT NOT NULL)", "CREATE ROLE [App]")
			schema := schemaFilePath(c, "schema.sql", "CREATE TABLE t1 (id INT NOT NULL);\nGO\nCREATE ROLE [APP];\nGO\n")

			result := clirun.Run(c, clirun.Ptah, clirun.Options{},
				"schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			c.Assert(sqlServerRoles(c, target), qt.Equals, test.want)
		})
	}
}

// TestSchemaApplyRefusesASQLServerNamesakeOfAnotherKindE2E declares
// the procedure dbo.REPORT on a case-insensitive database holding the view
// dbo.Report, alone and beside that view. The plan is refused before anything
// runs, naming both objects, and the database keeps the view.
func TestSchemaApplyRefusesASQLServerNamesakeOfAnotherKindE2E(t *testing.T) {
	const procedure = "CREATE PROCEDURE [dbo].[REPORT] AS BEGIN SELECT 2 END;\nGO\n"
	tests := []struct {
		name   string
		schema string
		want   string
	}{
		{
			name: "the procedure in place of the view", schema: procedure,
			want: "the plan creates procedure dbo.REPORT and drops view dbo.Report, and sqlserver keeps both kinds " +
				"in one namespace in a schema",
		},
		{
			name: "the procedure beside the view", schema: "CREATE VIEW [dbo].[Report] AS SELECT 1 AS a;\nGO\n" + procedure,
			want: "target view dbo.Report and procedure dbo.REPORT may have the same catalog identity, and sqlserver " +
				"keeps both kinds in one namespace in a schema",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := sqlServerDatabaseHolding(c, "SQL_Latin1_General_CP1_CI_AS", "CREATE VIEW [dbo].[Report] AS SELECT 1 AS a")
			schema := schemaFilePath(c, "schema.sql", test.schema)

			result := clirun.Run(c, clirun.Ptah, clirun.Options{},
				"schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(result.Stderr, qt.Contains, test.want)
			c.Assert(sqlServerObjects(c, target), qt.Equals, "Report V")
		})
	}
}

// TestSchemaApplyKeepsSQLServerNamesOfTwoKindsApartE2E is the control: a
// case-sensitive database reads REPORT and Report as two names, so the
// procedure is created beside the view.
func TestSchemaApplyKeepsSQLServerNamesOfTwoKindsApartE2E(t *testing.T) {
	c := qt.New(t)
	target := sqlServerDatabaseHolding(c, "Latin1_General_100_CS_AS", "CREATE VIEW [dbo].[Report] AS SELECT 1 AS a")
	schema := schemaFilePath(c, "schema.sql",
		"CREATE VIEW [dbo].[Report] AS SELECT 1 AS a;\nGO\nCREATE PROCEDURE [dbo].[REPORT] AS BEGIN SELECT 2 END;\nGO\n")

	result := clirun.Run(c, clirun.Ptah, clirun.Options{},
		"schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
	c.Assert(sqlServerObjects(c, target), qt.Equals, "REPORT P,Report V")
}
