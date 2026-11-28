//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A SQL Server database role lives in the database that holds it, so a
// rehearsal on a dev database runs its CREATE ROLE, and the cleanup after the
// rehearsal drops each role the claim of the dev database did not find. A
// dev database that already held a role before the run is still refused: the
// cleanup cannot tell whose the role is. A GRANT, DENY or REVOKE to a role the
// run created is rehearsed too, since dropping the role removes every
// permission granted or denied to it.

// sqlServerHeldRoles reads the database roles beyond the fixed ones, by name.
const sqlServerHeldRoles = "SELECT COALESCE(STRING_AGG(name, ',') WITHIN GROUP (ORDER BY name), '') " +
	"FROM sys.database_principals WHERE type = 'R' AND is_fixed_role = 0 AND principal_id > 4"

// sqlServerHeldTables reads the user tables of dbo beside the apply log, by
// name.
const sqlServerHeldTables = "SELECT COALESCE(STRING_AGG(name, ',') WITHIN GROUP (ORDER BY name), '') " +
	"FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo') AND name <> 'ptah_schema_apply_log'"

// sqlServerRoleScratchDatabase is a scratch database on the SQL Server the
// suite names, dropped when the test ends.
func sqlServerRoleScratchDatabase(c *qt.C) string {
	c.Helper()
	return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer),
		"ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s", replaceSQLServerDatabase)
}

// applyRoleSchema runs ptah schema apply of schema on target, rehearsed on
// dev.
func applyRoleSchema(c *qt.C, target, dev, schema string) clirun.Result {
	c.Helper()
	dir := c.TempDir()
	file := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(file, []byte(schema), 0o600), qt.IsNil)
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"schema", "apply", "--db-url", target, "--dev-url", dev, "--schema-file", file, "--auto-approve")
}

// TestSchemaApplyRehearsesADatabaseRoleOnSQLServerE2E applies a role twice:
// the first run creates it on the dev database through the plan, the second
// through the state of the target the rehearsal recreates there. Each run
// leaves the role on the target and none on the dev database.
func TestSchemaApplyRehearsesADatabaseRoleOnSQLServerE2E(t *testing.T) {
	c := qt.New(t)
	target, dev := sqlServerRoleScratchDatabase(c), sqlServerRoleScratchDatabase(c)

	first := applyRoleSchema(c, target, dev, "CREATE TABLE t1 (id INT NOT NULL);\nCREATE ROLE [APP];\n")

	c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
	c.Assert(heldColumns(c, target, sqlServerHeldRoles), qt.Equals, "APP")
	c.Assert(heldColumns(c, target, sqlServerHeldTables), qt.Equals, "t1")
	c.Assert(heldColumns(c, dev, sqlServerHeldRoles), qt.Equals, "")
	c.Assert(heldColumns(c, dev, sqlServerHeldTables), qt.Equals, "")

	second := applyRoleSchema(c, target, dev,
		"CREATE TABLE t1 (id INT NOT NULL);\nCREATE TABLE t2 (id INT NOT NULL);\nCREATE ROLE [APP];\nCREATE ROLE [App Readers];\n")

	c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", second.Stdout, second.Stderr))
	c.Assert(heldColumns(c, target, sqlServerHeldRoles), qt.Equals, "APP,App Readers")
	c.Assert(heldColumns(c, target, sqlServerHeldTables), qt.Equals, "t1,t2")
	c.Assert(heldColumns(c, dev, sqlServerHeldRoles), qt.Equals, "")
	c.Assert(heldColumns(c, dev, sqlServerHeldTables), qt.Equals, "")
}

// TestSchemaApplyRefusesADevDatabaseHoldingARoleOnSQLServerE2E is the control:
// a role the dev database held before the run is not the run's, so the cleanup
// refuses the database, leaves the role where it is and applies nothing.
func TestSchemaApplyRefusesADevDatabaseHoldingARoleOnSQLServerE2E(t *testing.T) {
	c := qt.New(t)
	target, dev := sqlServerRoleScratchDatabase(c), sqlServerRoleScratchDatabase(c)
	_, err := connectDevDialect(c, dev).ExecContext(c.Context(), "CREATE ROLE [PREEXISTING]")
	c.Assert(err, qt.IsNil)

	got := applyRoleSchema(c, target, dev, "CREATE TABLE t1 (id INT NOT NULL);\nCREATE ROLE [APP];\n")

	c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains,
		"refusing to clean database realm with unsupported database-scoped database principal [PREEXISTING] (DATABASE_ROLE)")
	c.Assert(heldColumns(c, target, sqlServerHeldRoles), qt.Equals, "")
	c.Assert(heldColumns(c, target, sqlServerHeldTables), qt.Equals, "")
	c.Assert(heldColumns(c, dev, sqlServerHeldRoles), qt.Equals, "PREEXISTING")
}

// sqlServerHeldPermissions reads the permissions held by database roles beyond
// the fixed ones, as role:state permission object.
const sqlServerHeldPermissions = "SELECT COALESCE(STRING_AGG(held, ',') WITHIN GROUP (ORDER BY held), '') FROM (" +
	"SELECT CONVERT(nvarchar(400), CONCAT(d.name, ':', p.state_desc, ' ', p.permission_name, ' ', " +
	"OBJECT_SCHEMA_NAME(p.major_id), '.', OBJECT_NAME(p.major_id))) COLLATE Latin1_General_BIN AS held " +
	"FROM sys.database_permissions AS p JOIN sys.database_principals AS d ON d.principal_id = p.grantee_principal_id " +
	"WHERE d.type = 'R' AND d.is_fixed_role = 0 AND d.principal_id > 4) AS permissions"

// TestSchemaApplyRehearsesGrantsToADatabaseRoleOnSQLServerE2E applies roles
// with their grants and a deny twice: the first run grants on the dev database
// through the plan, the second through the state of the target the rehearsal
// recreates there, and then grants once more. Each run leaves the permissions
// on the target, and no role and no permission on the dev database.
func TestSchemaApplyRehearsesGrantsToADatabaseRoleOnSQLServerE2E(t *testing.T) {
	c := qt.New(t)
	target, dev := sqlServerRoleScratchDatabase(c), sqlServerRoleScratchDatabase(c)
	const roles = "CREATE SCHEMA app;\nGO\nCREATE ROLE [APP];\nCREATE ROLE [App Readers];\n" +
		"GRANT SELECT ON t1 TO [APP];\nGRANT SELECT, INSERT ON app.t2 TO [App Readers] WITH GRANT OPTION;\n" +
		"DENY UPDATE ON t1 TO [App Readers];\n"

	first := applyRoleSchema(c, target, dev,
		"CREATE TABLE t1 (id INT NOT NULL);\nCREATE TABLE app.t2 (id INT NOT NULL);\n"+roles)

	c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
	c.Assert(heldColumns(c, target, sqlServerHeldPermissions), qt.Equals,
		"APP:GRANT SELECT dbo.t1,App Readers:DENY UPDATE dbo.t1,"+
			"App Readers:GRANT_WITH_GRANT_OPTION INSERT app.t2,App Readers:GRANT_WITH_GRANT_OPTION SELECT app.t2")
	c.Assert(heldColumns(c, dev, sqlServerHeldRoles), qt.Equals, "")
	c.Assert(heldColumns(c, dev, sqlServerHeldPermissions), qt.Equals, "")

	second := applyRoleSchema(c, target, dev,
		"CREATE TABLE t1 (id INT NOT NULL, v INT NULL);\nCREATE TABLE app.t2 (id INT NOT NULL);\n"+roles+
			"GRANT SELECT ON app.t2 TO [APP];\n")

	c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", second.Stdout, second.Stderr))
	c.Assert(heldColumns(c, target, sqlServerHeldPermissions), qt.Equals,
		"APP:GRANT SELECT app.t2,APP:GRANT SELECT dbo.t1,App Readers:DENY UPDATE dbo.t1,"+
			"App Readers:GRANT_WITH_GRANT_OPTION INSERT app.t2,App Readers:GRANT_WITH_GRANT_OPTION SELECT app.t2")
	c.Assert(heldColumns(c, dev, sqlServerHeldRoles), qt.Equals, "")
	c.Assert(heldColumns(c, dev, sqlServerHeldPermissions), qt.Equals, "")
}

// TestSchemaApplyRefusesAGrantToPublicInARehearsalOnSQLServerE2E is the
// control: public outlives the run, so a grant to it is not rehearsed, and the
// run applies nothing.
func TestSchemaApplyRefusesAGrantToPublicInARehearsalOnSQLServerE2E(t *testing.T) {
	c := qt.New(t)
	target, dev := sqlServerRoleScratchDatabase(c), sqlServerRoleScratchDatabase(c)

	got := applyRoleSchema(c, target, dev,
		"CREATE TABLE t1 (id INT NOT NULL);\nCREATE ROLE [APP];\nGRANT SELECT ON t1 TO [public];\n")

	c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, "sqlserver migration replay rejects permission mutation")
	c.Assert(heldColumns(c, target, sqlServerHeldRoles), qt.Equals, "")
	c.Assert(heldColumns(c, target, sqlServerHeldTables), qt.Equals, "")
}
