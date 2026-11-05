//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// `ptah schema diff --from <file> --to <database>` with no --dev-url compares
// the file, as the database a server builds from it, with the database. That
// database holds schema public as PostgreSQL creates it in every database:
// USAGE for PUBLIC and the comment `standard public schema`. Each test reads
// both back from the catalog.

// TestSchemaDiffTakesTheDefaultSchemaAsTheServerCreatedItE2E diffs a SQL file
// and an HCL file that say nothing about public's privileges or comment to a
// database built from the same table. The diff plans neither a GRANT nor a
// COMMENT for the schema.
func TestSchemaDiffTakesTheDefaultSchemaAsTheServerCreatedItE2E(t *testing.T) {
	for _, document := range []struct{ name, body string }{
		{name: "schema.sql", body: "CREATE TABLE t1 (id integer PRIMARY KEY);\n"},
		{name: "schema.hcl", body: "schema \"public\" {}\ntable \"t1\" {\n  schema = schema.public\n  column \"id\" {\n" +
			"    type = integer\n    null = false\n  }\n  primary_key {\n    columns = [column.id]\n  }\n}\n"},
	} {
		t.Run(document.name, func(t *testing.T) {
			c := qt.New(t)
			database := newEmptyReadScopeDatabase(c, "default_schema")
			execPostgresFamilySQL(c, c.Context(), database, "CREATE TABLE t1 (id integer PRIMARY KEY)")
			file := filepath.Join(c.TempDir(), document.name)
			c.Assert(os.WriteFile(file, []byte(document.body), 0o600), qt.IsNil)

			diff := runPtahIn(c, "schema", "diff", "--from", "file://"+file, "--to", database)

			c.Assert(diff.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", diff.Stdout, diff.Stderr))
			c.Assert(defaultSchemaState(c, database), qt.Equals, "PUBLIC USAGE true; comment standard public schema")
			c.Assert(diff.Stdout, qt.Not(qt.Contains), `ON SCHEMA "public"`)
		})
	}
}

// TestSchemaDiffComparesWhatAFileSaysAboutTheDefaultSchemaE2E is the control:
// a file that revokes PUBLIC's USAGE on public, or gives the schema a comment
// of its own, differs from the database there, and the diff plans the
// statement that gives the database's state back.
func TestSchemaDiffComparesWhatAFileSaysAboutTheDefaultSchemaE2E(t *testing.T) {
	for _, document := range []struct{ name, body, want string }{
		{
			name: "schema.sql",
			body: "CREATE TABLE t1 (id integer PRIMARY KEY);\nREVOKE USAGE ON SCHEMA public FROM PUBLIC;\n",
			want: `GRANT USAGE ON SCHEMA "public" TO PUBLIC;`,
		},
		{
			name: "schema.hcl",
			body: "schema \"public\" {\n  comment = \"mine\"\n}\ntable \"t1\" {\n  schema = schema.public\n  column \"id\" {\n" +
				"    type = integer\n    null = false\n  }\n  primary_key {\n    columns = [column.id]\n  }\n}\n",
			want: `COMMENT ON SCHEMA "public" IS 'standard public schema';`,
		},
	} {
		t.Run(document.name, func(t *testing.T) {
			c := qt.New(t)
			database := newEmptyReadScopeDatabase(c, "default_schema")
			execPostgresFamilySQL(c, c.Context(), database, "CREATE TABLE t1 (id integer PRIMARY KEY)")
			file := filepath.Join(c.TempDir(), document.name)
			c.Assert(os.WriteFile(file, []byte(document.body), 0o600), qt.IsNil)

			diff := runPtahIn(c, "schema", "diff", "--from", "file://"+file, "--to", database)

			c.Assert(diff.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", diff.Stdout, diff.Stderr))
			c.Assert(defaultSchemaState(c, database), qt.Equals, "PUBLIC USAGE true; comment standard public schema")
			c.Assert(diff.Stdout, qt.Contains, document.want)
		})
	}
}

// defaultSchemaState reads whether PUBLIC holds USAGE on schema public, and the
// schema's comment.
func defaultSchemaState(c *qt.C, database string) string {
	c.Helper()
	var state string
	err := openReadScopeDatabase(c, database).QueryRowContext(c.Context(), `
		SELECT 'PUBLIC USAGE ' || has_schema_privilege('public', 'public', 'USAGE')::text ||
			'; comment ' || COALESCE(obj_description('public'::regnamespace, 'pg_namespace'), '')`).Scan(&state)
	c.Assert(err, qt.IsNil)
	return state
}
