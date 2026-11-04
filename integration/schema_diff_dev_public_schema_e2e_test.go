//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// `ptah schema diff --from <file> --to <database> --dev-url <dev>` creates the
// file on the dev database and resets it before and after. A document that
// revokes USAGE on public from PUBLIC, or gives public a comment, changes the
// schema itself, which the reset keeps. Each test reads public back from the
// dev database before the first diff and after each one.

// TestSchemaDiffLeavesTheDevDatabasePublicSchemaAsItFoundItE2E runs two diffs
// back to back on one dev database: the first creates a document that changes
// public, and the second one that says nothing about it, against a target
// holding the same table. Public holds after each diff what it held before
// the first, and the second diff finds nothing to change. A dev database that
// holds an extension in public has the schema emptied where it stands rather
// than dropped and created again, and gets the same answer.
func TestSchemaDiffLeavesTheDevDatabasePublicSchemaAsItFoundItE2E(t *testing.T) {
	const table = "CREATE TABLE t1 (id integer PRIMARY KEY);\n"
	tests := []struct {
		name     string
		document string
		body     string
		devSetup []string
		pin      []string
	}{
		{
			name:     "a revoke, public created again",
			document: "schema.sql",
			body:     table + "REVOKE USAGE ON SCHEMA public FROM PUBLIC;\n",
		},
		{
			name:     "a comment, public created again under a URL that pins it",
			document: "schema.hcl",
			body: "schema \"public\" {\n  comment = \"mine\"\n}\ntable \"t1\" {\n  schema = schema.public\n  column \"id\" {\n" +
				"    type = integer\n    null = false\n  }\n  primary_key {\n    columns = [column.id]\n  }\n}\n",
			pin: []string{"public"},
		},
		{
			name:     "a revoke, public emptied in place",
			document: "schema.sql",
			body:     table + "REVOKE USAGE ON SCHEMA public FROM PUBLIC;\n",
			devSetup: []string{"CREATE EXTENSION citext SCHEMA public"},
		},
		{
			name:     "a comment, public emptied in place",
			document: "schema.hcl",
			body: "schema \"public\" {\n  comment = \"mine\"\n}\ntable \"t1\" {\n  schema = schema.public\n  column \"id\" {\n" +
				"    type = integer\n    null = false\n  }\n  primary_key {\n    columns = [column.id]\n  }\n}\n",
			devSetup: []string{"CREATE EXTENSION citext SCHEMA public"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newEmptyReadScopeDatabase(c, "dev_public_target")
			execPostgresFamilySQL(c, c.Context(), target, "CREATE TABLE t1 (id integer PRIMARY KEY)")
			dev := newEmptyReadScopeDatabase(c, "dev_public_dev")
			for _, statement := range test.devSetup {
				execPostgresFamilySQL(c, c.Context(), dev, statement)
			}
			devURL := dev
			for _, schema := range test.pin {
				devURL = planE2ESearchPath(c, devURL, schema)
			}
			dir := c.TempDir()
			changing := filepath.Join(dir, test.document)
			c.Assert(os.WriteFile(changing, []byte(test.body), 0o600), qt.IsNil)
			bare := filepath.Join(dir, "bare.sql")
			c.Assert(os.WriteFile(bare, []byte(table), 0o600), qt.IsNil)
			claimed := devPublicSchemaState(c, dev)

			first := runPtahIn(c, "schema", "diff", "--from", "file://"+changing, "--to", target, "--dev-url", devURL)
			afterFirst := devPublicSchemaState(c, dev)
			second := runPtahIn(c, "schema", "diff", "--from", "file://"+bare, "--to", target, "--dev-url", devURL)

			c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
			c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", second.Stdout, second.Stderr))
			c.Assert(afterFirst, qt.Equals, claimed)
			c.Assert(devPublicSchemaState(c, dev), qt.Equals, claimed)
			c.Assert(second.Stdout, qt.Contains, "Schemas are synced")
		})
	}
}

// devPublicSchemaState reads public's owner, its comment, and the privileges
// PUBLIC holds on it.
func devPublicSchemaState(c *qt.C, database string) string {
	c.Helper()
	var state string
	err := openReadScopeDatabase(c, database).QueryRowContext(c.Context(), `
		SELECT 'owner ' || pg_get_userbyid(nspowner) ||
			'; comment ' || COALESCE(obj_description(oid, 'pg_namespace'), '') ||
			'; PUBLIC USAGE ' || has_schema_privilege('public', 'public', 'USAGE')::text ||
			', CREATE ' || has_schema_privilege('public', 'public', 'CREATE')::text
		FROM pg_namespace WHERE nspname = 'public'`).Scan(&state)
	c.Assert(err, qt.IsNil)
	return state
}
