//go:build integration

package integration_test

import (
	"database/sql"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// sqlServerSchemaComment reads the comment the catalog holds for schema, and "" for
// none.
func sqlServerSchemaComment(c *qt.C, db *sql.DB, schema string) string {
	c.Helper()
	var comment string
	c.Assert(db.QueryRowContext(c.Context(), `SELECT COALESCE(CAST(MAX(CAST(ep.value AS nvarchar(4000))) AS nvarchar(4000)), N'')
		FROM sys.extended_properties AS ep
		WHERE ep.class = 3 AND ep.major_id = SCHEMA_ID(@p1) AND ep.minor_id = 0 AND ep.name = N'MS_Description'`, schema,
	).Scan(&comment), qt.IsNil)
	return comment
}

// sqlServerCommentDocument declares a schema with a comment.
func sqlServerCommentDocument(schema, comment string) string {
	return fmt.Sprintf("schema %q {\n  comment = %q\n}\n", schema, comment)
}

// TestSQLServerSchemaCommentReachesTheCatalogE2E applies a comment on a schema and
// reads it back from sys.extended_properties. A schema the plan creates takes it
// with the creation, a schema the database holds takes it on its own, and a changed
// one is replaced. The next plan is empty each time, and it can only be empty if the
// reader sees what the plan wrote.
func TestSQLServerSchemaCommentReachesTheCatalogE2E(t *testing.T) {
	tests := []struct {
		name, schema string
		setup        []string
		wantPlan     string
	}{
		{name: "a schema the plan creates", schema: "es_app", wantPlan: "sp_addextendedproperty"},
		{name: "a schema the database holds", schema: "es_app", setup: []string{"CREATE SCHEMA es_app"}, wantPlan: "sp_addextendedproperty"},
		{name: "the connected schema", schema: "dbo", wantPlan: "sp_addextendedproperty"},
		{
			name: "a schema holding another comment", schema: "es_app",
			setup: []string{
				"CREATE SCHEMA es_app",
				"EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'before', @level0type = N'SCHEMA', @level0name = N'es_app'",
			},
			wantPlan: "sp_updateextendedproperty",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, db := declaredSQLServerDatabase(c)
			declaredSQLServerExec(c, db, test.setup...)
			file := writeDeclaredSchemaFile(c, "schema.hcl", sqlServerCommentDocument(test.schema, "it's the application"))

			applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, test.wantPlan)
			c.Assert(sqlServerSchemaComment(c, db, test.schema), qt.Equals, "it's the application")

			settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")

			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// TestSQLServerSchemaCommentStaysWhereTheDeclarationIsSilentE2E is the control. A
// declaration that names a schema and states no comment says nothing about one, as
// on PostgreSQL, so the comment stays.
func TestSQLServerSchemaCommentStaysWhereTheDeclarationIsSilentE2E(t *testing.T) {
	c := qt.New(t)
	target, db := declaredSQLServerDatabase(c)
	declaredSQLServerExec(c, db, "CREATE SCHEMA es_app",
		"EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'kept', @level0type = N'SCHEMA', @level0name = N'es_app'")
	file := writeDeclaredSchemaFile(c, "schema.hcl", "schema \"es_app\" {\n}\n")

	got := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
	c.Assert(sqlServerSchemaComment(c, db, "es_app"), qt.Equals, "kept")
}

// TestSQLServerSchemaCommentSurvivesADescriptionE2E reads a commented schema into HCL
// and into SQL and plans from each against the same database. Both carry the comment,
// so both are synced. A document that loses it would still be synced here, so the
// description is also applied to an empty database, and that one reads the comment
// back.
func TestSQLServerSchemaCommentSurvivesADescriptionE2E(t *testing.T) {
	tests := []struct {
		name, file string
		read       []string
	}{
		{name: "hcl", file: "schema.hcl", read: []string{"schema", "inspect", "--schemas", "es_app", "--db-url"}},
		{name: "sql", file: "schema.sql", read: []string{"db", "read", "--schemas", "es_app", "--db-url"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, db := declaredSQLServerDatabase(c)
			empty, emptyDB := declaredSQLServerDatabase(c)
			declaredSQLServerExec(c, db, "CREATE SCHEMA es_app",
				"EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'application', @level0type = N'SCHEMA', @level0name = N'es_app'")

			read := runPtahIn(c, append(test.read, target)...)

			c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", read.Stderr))
			document := writeDeclaredSchemaFile(c, test.file, read.Stdout)
			planned := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", document, "--schemas", "es_app", "--dry-run")
			c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("description:\n%s\nstdout:\n%s\nstderr:\n%s", read.Stdout, planned.Stdout, planned.Stderr))
			c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")

			applied := runPtahIn(c, "schema", "apply", "--db-url", empty, "--schema-file", document, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(sqlServerSchemaComment(c, emptyDB, "es_app"), qt.Equals, "application")
		})
	}
}

// TestSQLServerSchemaCommentRollsBackE2E generates the migration that changes a
// comment. Its down file puts the comment back, read from the catalog.
func TestSQLServerSchemaCommentRollsBackE2E(t *testing.T) {
	c := qt.New(t)
	target, db := declaredSQLServerDatabase(c)
	declaredSQLServerExec(c, db, "CREATE SCHEMA es_app",
		"EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'before', @level0type = N'SCHEMA', @level0name = N'es_app'")
	dir := c.TempDir()
	file := writeDeclaredSchemaFile(c, "schema.hcl", sqlServerCommentDocument("es_app", "after"))

	generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", file, "--migrations-dir", dir)

	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))

	up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir)

	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(sqlServerSchemaComment(c, db, "es_app"), qt.Equals, "after")

	down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
	c.Assert(sqlServerSchemaComment(c, db, "es_app"), qt.Equals, "before")
}

// TestSQLServerSchemaCommentIsRehearsedE2E rehearses a plan that comments on a schema
// and on dbo, twice on one dev database, on both surfaces. The target holds its
// comment afterward and the dev database holds none: a reset drops the schema with
// its comment and takes dbo's away, and a comment left on dbo would be read as the
// dev database's state by the next run.
func TestSQLServerSchemaCommentIsRehearsedE2E(t *testing.T) {
	const document = "schema \"dbo\" {\n  comment = \"default\"\n}\n\nschema \"es_app\" {\n  comment = \"application\"\n}\n"
	for _, surface := range devSchemaStateSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)
			target, targetDB := declaredSQLServerDatabase(c)
			dev, devDB := declaredSQLServerDatabase(c)
			file := writeDeclaredSchemaFile(c, "schema.hcl", document)

			first := clirun.Run(c, surface.target, clirun.Options{Dir: c.TempDir()}, surface.apply(target, file, dev)...)

			c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
			c.Assert(sqlServerSchemaComment(c, targetDB, "es_app"), qt.Equals, "application")
			c.Assert(sqlServerSchemaComment(c, targetDB, "dbo"), qt.Equals, "default")
			c.Assert(sqlServerSchemasNamed(c, devDB, "es_app"), qt.HasLen, 0)
			c.Assert(sqlServerSchemaComment(c, devDB, "dbo"), qt.Equals, "")
			// A plan that changes nothing rehearses nothing, so the second run has a
			// comment to change.
			changed := writeDeclaredSchemaFile(c, "schema.hcl", "schema \"dbo\" {\n  comment = \"changed\"\n}\n\nschema \"es_app\" {\n  comment = \"application\"\n}\n")

			second := clirun.Run(c, surface.target, clirun.Options{Dir: c.TempDir()}, surface.apply(target, changed, dev)...)

			c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", second.Stdout, second.Stderr))
			c.Assert(sqlServerSchemaComment(c, targetDB, "dbo"), qt.Equals, "changed")
			c.Assert(sqlServerSchemaComment(c, devDB, "dbo"), qt.Equals, "")
		})
	}
}
