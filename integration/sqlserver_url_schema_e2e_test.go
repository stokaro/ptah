//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// A SQL Server URL's schema parameter selects the schema a connection reads
// and compares, while the server creates a name written without a schema in
// the default schema of the connection's user. Where the two differ, a plan
// would create an unqualified object where the next run does not read it, so
// the comparison refuses a declaration that writes one.

// sqlServerUnqualifiedSchema is a schema file declaring an unqualified table
// and function.
const sqlServerUnqualifiedSchema = "CREATE TABLE t1 (id INT NOT NULL);\nGO\n" +
	"CREATE FUNCTION f() RETURNS int AS BEGIN RETURN 1 END;\nGO\n"

// applySchemaText runs ptah schema apply of schema on target.
func applySchemaText(c *qt.C, target, schema string) clirun.Result {
	c.Helper()
	file := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte(schema), 0o600), qt.IsNil)
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
		"schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")
}

// TestSchemaApplySQLServerRefusesUnqualifiedNamesOutsideTheURLSchemaE2E
// applies unqualified names as sa through a URL naming app. The server would
// create them in dbo, where the next run does not read, so the run is refused
// and the database holds neither.
func TestSchemaApplySQLServerRefusesUnqualifiedNamesOutsideTheURLSchemaE2E(t *testing.T) {
	c := qt.New(t)
	dbURL := sqlServerRoleScratchDatabase(c)
	_, err := connectDevDialect(c, dbURL).ExecContext(c.Context(), "CREATE SCHEMA app")
	c.Assert(err, qt.IsNil)

	got := applySchemaText(c, dbURL+"&schema=app", sqlServerUnqualifiedSchema)

	c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, `the desired schema writes "f", "t1" without a schema: this connection reads schema "app", `+
		`and the server creates such a name in "dbo", the default schema of the connection's user`)
	c.Assert(heldColumns(c, dbURL, sqlServerObjectsBySchema), qt.Equals, "")
}

// TestSchemaApplySQLServerRefusesUnqualifiedNamesOfAnAccountNamingDboE2E is
// the same for an account whose default schema is app, through a URL naming
// dbo: the server would create the names in app.
func TestSchemaApplySQLServerRefusesUnqualifiedNamesOfAnAccountNamingDboE2E(t *testing.T) {
	c := qt.New(t)
	dbURL := sqlServerRoleScratchDatabase(c)
	account := sqlServerAccount(c, dbURL, "app")

	got := applySchemaText(c, account+"&schema=dbo", sqlServerUnqualifiedSchema)

	c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, `this connection reads schema "dbo", and the server creates such a name in "app"`)
	c.Assert(heldColumns(c, dbURL, sqlServerObjectsBySchema), qt.Equals, "")
}

// TestSchemaApplySQLServerAppliesQualifiedNamesInTheURLSchemaE2E is the
// control: the same objects qualified with app are created there, and the
// second run finds them.
func TestSchemaApplySQLServerAppliesQualifiedNamesInTheURLSchemaE2E(t *testing.T) {
	c := qt.New(t)
	dbURL := sqlServerRoleScratchDatabase(c)
	_, err := connectDevDialect(c, dbURL).ExecContext(c.Context(), "CREATE SCHEMA app")
	c.Assert(err, qt.IsNil)
	const qualified = "CREATE TABLE app.t1 (id INT NOT NULL);\nGO\n" +
		"CREATE FUNCTION app.f() RETURNS int AS BEGIN RETURN 1 END;\nGO\n"

	applied := applySchemaText(c, dbURL+"&schema=app", qualified)
	again := applySchemaText(c, dbURL+"&schema=app", qualified)

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(heldColumns(c, dbURL, sqlServerObjectsBySchema), qt.Equals, "app.f,app.t1")
	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
	c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
}

// TestSchemaApplySQLServerAppliesUnqualifiedNamesInTheSchemaTheURLNamesE2E is
// the other control: a URL naming the user's default schema, as the server
// compares the names, leaves unqualified names where they are read. The
// account's app is spelled APP, and sa's dbo is spelled DBO.
func TestSchemaApplySQLServerAppliesUnqualifiedNamesInTheSchemaTheURLNamesE2E(t *testing.T) {
	c := qt.New(t)
	dbURL := sqlServerRoleScratchDatabase(c)
	account := sqlServerAccount(c, dbURL, "app")

	asAccount := applySchemaText(c, account+"&schema=APP", sqlServerUnqualifiedSchema)
	asSA := applySchemaText(c, dbURL+"&schema=DBO", "CREATE TABLE t2 (id INT NOT NULL);\nGO\n")
	again := applySchemaText(c, account+"&schema=APP", sqlServerUnqualifiedSchema)

	c.Assert(asAccount.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", asAccount.Stdout, asAccount.Stderr))
	c.Assert(asSA.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", asSA.Stdout, asSA.Stderr))
	c.Assert(heldColumns(c, dbURL, sqlServerObjectsBySchema), qt.Equals, "app.f,app.t1,dbo.t2")
	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
	c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
}

// TestSchemaApplySQLServerRefusesADevURLPlacingNamesApartE2E rehearses a
// target account whose default schema is app on a dev database reached as sa.
// Without a schema parameter the dev URL reads dbo. With schema=app it reads
// app but creates the plan's unqualified names in dbo, where the target
// creates them in app. Each pair is refused before anything is applied.
func TestSchemaApplySQLServerRefusesADevURLPlacingNamesApartE2E(t *testing.T) {
	tests := []struct {
		name     string
		devQuery string
		want     string
	}{
		{"a dev URL reading dbo", "",
			`--dev-url schema scope "dbo" does not match --db-url schema scope "app"; point --dev-url at a dev database using the same schema`},
		{"a dev URL naming app", "&schema=app",
			`--dev-url default schema "dbo" does not match --db-url default schema "app"; give --dev-url a user whose default schema is "app"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, dev := sqlServerRoleScratchDatabase(c), sqlServerRoleScratchDatabase(c)
			account := sqlServerAccount(c, target, "app")

			got := applyRoleSchema(c, account, dev+test.devQuery, "CREATE TABLE t1 (id INT NOT NULL);\n")

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, test.want)
			c.Assert(heldColumns(c, target, sqlServerObjectsBySchema), qt.Equals, "")
		})
	}
}

// TestSchemaApplySQLServerRehearsesAsADevUserOfTheTargetsDefaultSchemaE2E is
// the control: a dev database reached as a user whose default schema is app
// runs the plan as the target does, a role and its grant among it, and the
// cleanup after the rehearsal leaves that database empty. The cleanup passes
// over dbo's membership of db_owner, which every database holds.
func TestSchemaApplySQLServerRehearsesAsADevUserOfTheTargetsDefaultSchemaE2E(t *testing.T) {
	c := qt.New(t)
	target, dev := sqlServerRoleScratchDatabase(c), sqlServerRoleScratchDatabase(c)
	account, devAccount := sqlServerAccount(c, target, "app"), sqlServerAccount(c, dev, "app")

	got := applyRoleSchema(c, account, devAccount,
		"CREATE TABLE t1 (id INT NOT NULL);\nCREATE ROLE [APP];\nGRANT SELECT ON t1 TO [APP];\n")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(heldColumns(c, target, sqlServerObjectsBySchema), qt.Equals, "app.t1")
	c.Assert(heldColumns(c, target, sqlServerHeldPermissions), qt.Equals, "APP:GRANT SELECT app.t1")
	c.Assert(heldColumns(c, dev, sqlServerObjectsBySchema), qt.Equals, "")
	c.Assert(heldColumns(c, dev, sqlServerHeldRoles), qt.Equals, "")
}
