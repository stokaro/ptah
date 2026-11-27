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

// TestSchemaApplyRefusesASQLServerNamesakeOfAnotherKindE2E declares an
// object of one kind on a case-insensitive database holding an object of
// another kind under a name the server reads as the same name, alone and
// beside the held object: the procedure dbo.REPORT where the view dbo.Report
// is, and the procedure dbo.x where the function dbo.x is. The plan is refused
// before anything runs, naming both objects, and the database keeps what it
// held.
func TestSchemaApplyRefusesASQLServerNamesakeOfAnotherKindE2E(t *testing.T) {
	const (
		view      = "CREATE VIEW [dbo].[Report] AS SELECT 1 AS a"
		report    = "CREATE PROCEDURE [dbo].[REPORT] AS BEGIN SELECT 2 END;\nGO\n"
		function  = "CREATE FUNCTION [dbo].[x]() RETURNS int AS BEGIN RETURN 1 END"
		procedure = "CREATE PROCEDURE [dbo].[x] AS BEGIN SELECT 1 END;\nGO\n"
	)
	tests := []struct {
		name   string
		held   string
		schema string
		want   string
		kept   string
	}{
		{
			name: "the procedure in place of the view", held: view, schema: report,
			want: "the plan creates procedure dbo.REPORT and drops view dbo.Report, and sqlserver keeps both kinds " +
				"in one namespace in a schema",
			kept: "Report V",
		},
		{
			name: "the procedure beside the view", held: view, schema: view + ";\nGO\n" + report,
			want: "target view dbo.Report and procedure dbo.REPORT may have the same catalog identity, and sqlserver " +
				"keeps both kinds in one namespace in a schema",
			kept: "Report V",
		},
		{
			name: "a procedure in place of a function of one name", held: function, schema: procedure,
			want: "the plan creates procedure dbo.x and drops function dbo.x, and sqlserver keeps both kinds " +
				"in one namespace in a schema, where the two are one name, so the create would fail while the function " +
				"exists; make the change in two migrations, the first dropping the function dbo.x and the second " +
				"creating the procedure dbo.x",
			kept: "x FN",
		},
		{
			name: "a procedure beside a function of one name", held: function, schema: function + ";\nGO\n" + procedure,
			want: "target function dbo.x and procedure dbo.x are one name, and sqlserver keeps both kinds in one " +
				"namespace in a schema",
			kept: "x FN",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := sqlServerDatabaseHolding(c, "SQL_Latin1_General_CP1_CI_AS", test.held)
			schema := schemaFilePath(c, "schema.sql", test.schema)

			result := clirun.Run(c, clirun.Ptah, clirun.Options{},
				"schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(result.Stderr, qt.Contains, test.want)
			c.Assert(sqlServerObjects(c, target), qt.Equals, test.kept)
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

// TestSchemaApplyRefusesTwoSQLServerRolesOfOneNameE2E declares the roles App
// and APP on a case-insensitive database holding App. The comparison pairs one
// of them with App and would plan CREATE ROLE for the other, which fails with
// error 15023, so the declaration is refused before anything runs.
func TestSchemaApplyRefusesTwoSQLServerRolesOfOneNameE2E(t *testing.T) {
	c := qt.New(t)
	target := sqlServerDatabaseHolding(c, "SQL_Latin1_General_CP1_CI_AS", "CREATE TABLE t1 (id INT NOT NULL)", "CREATE ROLE [App]")
	schema := schemaFilePath(c, "schema.sql",
		"CREATE TABLE t1 (id INT NOT NULL);\nGO\nCREATE ROLE [App];\nGO\nCREATE ROLE [APP];\nGO\n")

	result := clirun.Run(c, clirun.Ptah, clirun.Options{},
		"schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

	c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(result.Stderr, qt.Contains, "target roles App and APP may have the same catalog identity")
	c.Assert(sqlServerRoles(c, target), qt.Equals, "App")
}

// TestSchemaApplyReplacesASQLServerFunctionByAProcedureInTwoMigrationsE2E
// makes the change the refusal of a procedure in place of a function names:
// the first apply drops the function dbo.x, and the second creates the
// procedure dbo.x, which then holds the name.
func TestSchemaApplyReplacesASQLServerFunctionByAProcedureInTwoMigrationsE2E(t *testing.T) {
	c := qt.New(t)
	target := sqlServerDatabaseHolding(c, "SQL_Latin1_General_CP1_CI_AS",
		"CREATE TABLE t1 (id INT NOT NULL)", "CREATE FUNCTION [dbo].[x]() RETURNS int AS BEGIN RETURN 1 END")
	first := schemaFilePath(c, "first.sql", "CREATE TABLE t1 (id INT NOT NULL);\nGO\n")
	second := schemaFilePath(c, "second.sql",
		"CREATE TABLE t1 (id INT NOT NULL);\nGO\nCREATE PROCEDURE [dbo].[x] AS BEGIN SELECT 1 END;\nGO\n")

	dropped := clirun.Run(c, clirun.Ptah, clirun.Options{},
		"schema", "apply", "--db-url", target, "--schema-file", first, "--auto-approve")
	c.Assert(dropped.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", dropped.Stderr))
	c.Assert(sqlServerObjects(c, target), qt.Equals, "t1 U")
	created := clirun.Run(c, clirun.Ptah, clirun.Options{},
		"schema", "apply", "--db-url", target, "--schema-file", second, "--auto-approve")

	c.Assert(created.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", created.Stderr))
	c.Assert(sqlServerObjects(c, target), qt.Equals, "t1 U,x P")
}
