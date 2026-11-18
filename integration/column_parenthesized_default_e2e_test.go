//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// writeParenthesizedDefaultSchema writes one schema file and returns its path.
func writeParenthesizedDefaultSchema(c *qt.C, dir, name, schema string) string {
	c.Helper()
	path := filepath.Join(dir, name)
	c.Assert(os.WriteFile(path, []byte(schema), 0o600), qt.IsNil)
	return path
}

// TestSchemaApplyRoundTripsAParenthesizedDefaultE2E applies a schema file
// whose default is written the way pg_get_expr prints it, and pg_dump writes
// it, on PostgreSQL.
//
// The server stores the expression in the spelling the file uses, so the
// catalog reads back the default the file states, and a second comparison of
// the same file finds nothing to do. The rows with a cast after the group are
// what PostgreSQL prints for a default the author cast: now()::date reads back
// as (now())::date, and ('X-' || gen_random_uuid()::text)::varchar as the
// varchar row.
func TestSchemaApplyRoundTripsAParenthesizedDefaultE2E(t *testing.T) {
	tests := []struct {
		name         string
		schema       string
		column       string
		wantDefault  string
		wantNullable string
	}{
		{
			name:         "a default in the column declaration",
			schema:       "CREATE TABLE tags (id bigint PRIMARY KEY, tag text DEFAULT ('X-'::text || (gen_random_uuid())::text));\n",
			column:       "tag",
			wantDefault:  "('X-'::text || (gen_random_uuid())::text)",
			wantNullable: "YES",
		},
		{
			name:         "a cast after a call in a group",
			schema:       "CREATE TABLE tags (id bigint PRIMARY KEY, day date DEFAULT (now())::date);\n",
			column:       "day",
			wantDefault:  "(now())::date",
			wantNullable: "YES",
		},
		{
			name:         "a cast after the group, then NOT NULL",
			schema:       "CREATE TABLE tags (id bigint PRIMARY KEY, code character varying(40) DEFAULT (('X-'::text || (gen_random_uuid())::text))::character varying NOT NULL);\n",
			column:       "code",
			wantDefault:  "(('X-'::text || (gen_random_uuid())::text))::character varying",
			wantNullable: "NO",
		},
		{
			name: "a default set by ALTER COLUMN",
			schema: "CREATE TABLE tags (id bigint PRIMARY KEY, tag text);\n" +
				"ALTER TABLE tags ALTER COLUMN tag SET DEFAULT ('X-'::text || (gen_random_uuid())::text);\n",
			column:       "tag",
			wantDefault:  "('X-'::text || (gen_random_uuid())::text)",
			wantNullable: "YES",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := scratchReplayDatabase(c)
			schema := writeParenthesizedDefaultSchema(c, c.TempDir(), "schema.sql", test.schema)

			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")
			again := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

			columnDefault, nullable := readColumnDefinition(c, target, "tags", test.column)
			c.Assert(columnDefault, qt.DeepEquals, new(test.wantDefault))
			c.Assert(nullable, qt.Equals, test.wantNullable)
			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyChangesAParenthesizedDefaultE2E is the control the test above
// needs: a comparison that ignored the default would also find nothing to do.
// A changed expression is planned as one SET DEFAULT that carries the group as
// written, and the server holds the new default after the apply.
func TestSchemaApplyChangesAParenthesizedDefaultE2E(t *testing.T) {
	c := qt.New(t)
	target, _ := scratchReplayDatabase(c)
	dir := c.TempDir()
	before := writeParenthesizedDefaultSchema(c, dir, "before.sql",
		"CREATE TABLE tags (id bigint PRIMARY KEY, tag text DEFAULT ('X-'::text || (gen_random_uuid())::text));\n")
	after := writeParenthesizedDefaultSchema(c, dir, "after.sql",
		"CREATE TABLE tags (id bigint PRIMARY KEY, tag text DEFAULT ('Y-'::text || (gen_random_uuid())::text));\n")
	runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", before, "--auto-approve")

	plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", after, "--dry-run")
	runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", after, "--auto-approve")

	c.Assert(plan, qt.Contains, `ALTER TABLE "public"."tags" ALTER COLUMN "tag" SET DEFAULT ('Y-'::text || (gen_random_uuid())::text);`)
	columnDefault, _ := readColumnDefinition(c, target, "tags", "tag")
	c.Assert(columnDefault, qt.DeepEquals, new("('Y-'::text || (gen_random_uuid())::text)"))
}
