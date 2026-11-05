//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// declaredSQLServerDatabase is a scratch SQL Server database and a connection to
// it that reads its catalog.
func declaredSQLServerDatabase(c *qt.C) (string, *sql.DB) {
	c.Helper()
	target := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer),
		"ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s",
		replaceSQLServerDatabase)
	db, err := sql.Open("sqlserver", target)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Assert(db.Close(), qt.IsNil) })
	return target, db
}

// declaredSQLServerExec runs each statement on db.
func declaredSQLServerExec(c *qt.C, db *sql.DB, statements ...string) {
	c.Helper()
	for _, statement := range statements {
		_, err := db.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// sqlServerSchemasNamed reads the catalog's spelling of the schemas whose name
// equals name under the database's collation.
func sqlServerSchemasNamed(c *qt.C, db *sql.DB, name string) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), `SELECT name FROM sys.schemas WHERE name = @p1`, name)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	names := make([]string, 0)
	for rows.Next() {
		var held string
		c.Assert(rows.Scan(&held), qt.IsNil)
		names = append(names, held)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// TestDeclaredEmptySchemaOnSQLServerE2E declares a schema with nothing in it, in
// HCL and in SQL. The plan creates it, the catalog holds it, and the next plan is
// empty.
func TestDeclaredEmptySchemaOnSQLServerE2E(t *testing.T) {
	documents := []struct{ name, body string }{
		{name: "schema.hcl", body: "schema \"es_app\" {\n}\n"},
		{name: "schema.sql", body: "CREATE SCHEMA es_app;\n"},
	}
	for _, document := range documents {
		t.Run(document.name, func(t *testing.T) {
			c := qt.New(t)
			target, db := declaredSQLServerDatabase(c)
			file := writeDeclaredSchemaFile(c, document.name, document.body)

			planned := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")

			c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
			c.Assert(planned.Stdout, qt.Contains, "CREATE SCHEMA [es_app]")
			c.Assert(sqlServerSchemasNamed(c, db, "es_app"), qt.HasLen, 0)

			applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(sqlServerSchemasNamed(c, db, "es_app"), qt.DeepEquals, []string{"es_app"})

			settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")

			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// TestDeclaredEmptySchemaTakesItsPrivilegeOnSQLServerE2E applies a privilege on a
// schema with nothing in it. A plan that grants it without creating the schema is
// a GRANT the server refuses. The privilege is read back from the catalog.
func TestDeclaredEmptySchemaTakesItsPrivilegeOnSQLServerE2E(t *testing.T) {
	c := qt.New(t)
	target, db := declaredSQLServerDatabase(c)
	declaredSQLServerExec(c, db, "CREATE ROLE es_reader")
	file := writeDeclaredSchemaFile(c, "schema.hcl",
		"schema \"es_app\" {\n}\n\npermission {\n  to = \"es_reader\"\n  for = schema.es_app\n  privileges = [\"SELECT\"]\n}\n")

	applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	var granted int
	c.Assert(db.QueryRowContext(c.Context(), `SELECT count(*) FROM sys.database_permissions AS p
		JOIN sys.database_principals AS r ON r.principal_id = p.grantee_principal_id
		WHERE p.class_desc = 'SCHEMA' AND SCHEMA_NAME(p.major_id) = 'es_app' AND r.name = 'es_reader' AND p.permission_name = 'SELECT'`,
	).Scan(&granted), qt.IsNil)
	c.Assert(granted, qt.Equals, 1)
}

// TestDeclaredSchemaOnSQLServerPlansNothingWhereItShouldNotE2E is the control.
// Each row is a schema the plan must leave alone: one the database holds, under
// another spelling the catalog's collation treats as the same name, the connected
// schema, one recorded as not described, one outside the operator's selection, and
// one the document does not mention, which Ptah does not drop. The default
// collation of a SQL Server database compares case-insensitively.
func TestDeclaredSchemaOnSQLServerPlansNothingWhereItShouldNotE2E(t *testing.T) {
	tests := []struct {
		name     string
		setup    []string
		document string
		args     []string
		want     []string
	}{
		{
			name:     "declared and held",
			setup:    []string{"CREATE SCHEMA es_app"},
			document: "schema \"es_app\" {\n}\n",
			want:     []string{"es_app"},
		},
		{
			name:     "declared in another spelling of what the database holds",
			setup:    []string{"CREATE SCHEMA es_app"},
			document: "schema \"ES_APP\" {\n}\n",
			want:     []string{"es_app"},
		},
		{
			name:     "the connected schema",
			document: "schema \"dbo\" {\n}\n",
			want:     make([]string, 0),
		},
		{
			name:     "recorded as not described, and declared",
			document: "// ptah:not-described schema \"es_app\"\n\nschema \"es_app\" {\n}\n",
			want:     make([]string, 0),
		},
		{
			name:     "outside the schemas the operator selected",
			document: "schema \"dbo\" {\n}\n\nschema \"es_app\" {\n}\n",
			args:     []string{"--schemas", "dbo"},
			want:     make([]string, 0),
		},
		{
			name:     "held and empty, and the document does not mention it",
			setup:    []string{"CREATE SCHEMA es_app"},
			document: "schema \"dbo\" {\n}\n",
			want:     []string{"es_app"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, db := declaredSQLServerDatabase(c)
			declaredSQLServerExec(c, db, test.setup...)
			args := append([]string{"schema", "apply", "--db-url", target, "--auto-approve",
				"--schema-file", writeDeclaredSchemaFile(c, "schema.hcl", test.document)}, test.args...)

			got := runPtahIn(c, args...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(sqlServerSchemasNamed(c, db, "es_app"), qt.DeepEquals, test.want)
		})
	}
}

// TestDeclaredEmptySchemaSurvivesAnInspectOnSQLServerE2E reads a database holding
// an empty schema into HCL and plans from it against the same database. A read of
// that schema declares it, and the document is synced. A read that does not select
// it does not declare it, and the plan leaves it where it is.
func TestDeclaredEmptySchemaSurvivesAnInspectOnSQLServerE2E(t *testing.T) {
	tests := []struct {
		name          string
		selection     []string
		want, wantOut string
	}{
		{name: "selected", selection: []string{"--schemas", "es_app"}, want: `schema "es_app"`, wantOut: `schema "dbo"`},
		{name: "not selected", want: `schema "dbo"`, wantOut: `schema "es_app"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, db := declaredSQLServerDatabase(c)
			declaredSQLServerExec(c, db, "CREATE SCHEMA es_app")

			inspected := runPtahIn(c, append([]string{"schema", "inspect", "--db-url", target}, test.selection...)...)

			c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))
			c.Assert(inspected.Stdout, qt.Contains, test.want)
			c.Assert(inspected.Stdout, qt.Not(qt.Contains), test.wantOut)
			planned := runPtahIn(c, append([]string{"schema", "apply", "--db-url", target, "--dry-run",
				"--schema-file", writeDeclaredSchemaFile(c, "schema.hcl", inspected.Stdout)}, test.selection...)...)
			c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
			c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(sqlServerSchemasNamed(c, db, "es_app"), qt.DeepEquals, []string{"es_app"})
		})
	}
}

// TestDeclaredEmptySchemaSurvivesASQLDescriptionOnSQLServerE2E reads a database into
// SQL and plans from it. A description of that schema writes the guarded creation,
// which a schema file reads back, so the document is synced. One that does not
// select it writes no schema at all, not even the connected one, so it is a document
// a schema file can read whatever the statement looks like.
func TestDeclaredEmptySchemaSurvivesASQLDescriptionOnSQLServerE2E(t *testing.T) {
	tests := []struct {
		name      string
		selection []string
		want      string
	}{
		{name: "selected", selection: []string{"--schemas", "es_app"}, want: "CREATE SCHEMA [es_app]"},
		{name: "not selected"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, db := declaredSQLServerDatabase(c)
			declaredSQLServerExec(c, db, "CREATE SCHEMA es_app")

			read := runPtahIn(c, append([]string{"schema", "inspect", "--db-url", target, "--format", "sql"}, test.selection...)...)

			c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", read.Stderr))
			c.Assert(read.Stdout, qt.Contains, test.want)
			c.Assert(read.Stdout, qt.Not(qt.Contains), "CREATE SCHEMA [dbo]")
			planned := runPtahIn(c, append([]string{"schema", "apply", "--db-url", target, "--dry-run",
				"--schema-file", writeDeclaredSchemaFile(c, "schema.sql", read.Stdout)}, test.selection...)...)
			c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
			c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// TestDeclaredEmptySchemaRollsBackOnSQLServerE2E generates the migration that
// creates a schema. Its down file drops the schema, read back from the catalog.
func TestDeclaredEmptySchemaRollsBackOnSQLServerE2E(t *testing.T) {
	c := qt.New(t)
	target, db := declaredSQLServerDatabase(c)
	dir := c.TempDir()
	file := writeDeclaredSchemaFile(c, "schema.hcl", "schema \"es_app\" {\n}\n")

	generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", file, "--migrations-dir", dir)

	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	c.Assert(readFirstMatchingFile(c, dir, "*.down.sql"), qt.Contains, "DROP SCHEMA [es_app]")

	up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir)

	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(sqlServerSchemasNamed(c, db, "es_app"), qt.DeepEquals, []string{"es_app"})

	down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
	c.Assert(sqlServerSchemasNamed(c, db, "es_app"), qt.HasLen, 0)
}

// TestDeclaredEmptySchemaRollbackStopsAtATableOnSQLServerE2E puts a table in the
// schema after the migration that created it. The server refuses the drop, so the
// rollback stops and the table survives. Once the operator moves the table, the
// repair the status names finishes the rollback.
func TestDeclaredEmptySchemaRollbackStopsAtATableOnSQLServerE2E(t *testing.T) {
	c := qt.New(t)
	target, db := declaredSQLServerDatabase(c)
	dir := c.TempDir()
	file := writeDeclaredSchemaFile(c, "schema.hcl", "schema \"es_app\" {\n}\n")
	generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", file, "--migrations-dir", dir)
	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir)
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	declaredSQLServerExec(c, db, "CREATE TABLE es_app.since (id int PRIMARY KEY)")

	stopped := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(stopped.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stopped.Stdout, stopped.Stderr))
	c.Assert(sqlServerSchemasNamed(c, db, "es_app"), qt.DeepEquals, []string{"es_app"})
	declaredSQLServerExec(c, db, "DROP TABLE es_app.since")

	repaired := runPtahIn(c, "migrations", "repair", "--db-url", target, "--migrations-dir", dir,
		"--version", generatedMigrationVersion(c, dir), "--resume-from", "1")

	c.Assert(repaired.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", repaired.Stdout, repaired.Stderr))
	c.Assert(sqlServerSchemasNamed(c, db, "es_app"), qt.HasLen, 0)
}

// TestDeclaredSchemaIsRehearsedOnSQLServerE2E rehearses a plan twice on one dev
// database, on both surfaces. A rehearsal has to take back every schema it created,
// with or without something in it: the next claim of the dev database refuses a
// schema or a table a reset would drop, so one left behind would refuse the second
// run. The dev database holds neither afterward.
func TestDeclaredSchemaIsRehearsedOnSQLServerE2E(t *testing.T) {
	tests := []struct {
		name, document string
	}{
		{name: "a schema alone", document: "schema \"dbo\" {\n}\n\nschema \"es_app\" {\n}\n"},
		{
			name: "a schema with a table",
			document: "schema \"dbo\" {\n}\n\nschema \"es_app\" {\n}\n\ntable \"t\" {\n  schema = schema.es_app\n" +
				"  column \"id\" {\n    type = int\n    null = false\n  }\n}\n",
		},
	}
	for _, surface := range devSchemaStateSurfaces {
		for _, test := range tests {
			t.Run(surface.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				target, targetDB := declaredSQLServerDatabase(c)
				dev, devDB := declaredSQLServerDatabase(c)
				document := writeDeclaredSchemaFile(c, "schema.hcl", test.document)

				first := clirun.Run(c, surface.target, clirun.Options{Dir: c.TempDir()}, surface.apply(target, document, dev)...)

				c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
				c.Assert(sqlServerSchemasNamed(c, targetDB, "es_app"), qt.DeepEquals, []string{"es_app"})
				c.Assert(sqlServerSchemasNamed(c, devDB, "es_app"), qt.HasLen, 0)
				// A plan that changes nothing rehearses nothing, so the second
				// run has this table to drop.
				declaredSQLServerExec(c, targetDB, "CREATE TABLE dbo.unplanned (id int PRIMARY KEY)")

				second := clirun.Run(c, surface.target, clirun.Options{Dir: c.TempDir()}, surface.apply(target, document, dev)...)

				c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", second.Stdout, second.Stderr))
				c.Assert(sqlServerSchemasNamed(c, devDB, "es_app"), qt.HasLen, 0)
			})
		}
	}
}

// TestDeclaredSchemaIsRehearsedOnADottedSQLServerDatabaseE2E rehearses on a dev
// database whose name holds a dot. The reset asks the server what it may do to the
// database by name, and an unquoted dotted name is a qualified one, so the server
// answers that it may do nothing and the reset refuses.
func TestDeclaredSchemaIsRehearsedOnADottedSQLServerDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	target, _ := declaredSQLServerDatabase(c)
	admin, err := sql.Open("sqlserver", dbtarget.URL(c, dbtarget.SQLServer))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Assert(admin.Close(), qt.IsNil) })
	const dotted = "ptah_dotted.dev"
	declaredSQLServerExec(c, admin, "CREATE DATABASE ["+dotted+"]")
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(),
			"ALTER DATABASE ["+dotted+"] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE ["+dotted+"]")
		c.Check(err, qt.IsNil)
	})
	dev := replaceSQLServerDatabase(c, target, dotted)
	document := writeDeclaredSchemaFile(c, "schema.hcl", "schema \"dbo\" {\n}\n\nschema \"es_app\" {\n}\n")

	got := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", document, "--dev-url", dev, "--auto-approve")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
}
