//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// writeColumnChangeSchema writes one schema file for the table the tests
// change.
func writeColumnChangeSchema(c *qt.C, dir, name, columns string) string {
	c.Helper()
	path := filepath.Join(dir, name)
	c.Assert(os.WriteFile(path, []byte("CREATE TABLE flags3645 (id bigint PRIMARY KEY, "+columns+");\n"), 0o600), qt.IsNil)
	return path
}

// TestSchemaApplyChangesOnlyTheDefaultLive applies a change to one column's
// default on PostgreSQL and reads the column back.
//
// Restating the column for it writes `ALTER COLUMN TYPE` naming the type it
// already has, which locks the table, a NULL backfill in a DO block, `SET NOT
// NULL` for a column that is NOT NULL, and then the default. Atlas CE v1.3.0
// writes the SET DEFAULT or DROP DEFAULT alone, and so does the plan
// (stokaro/ptah#3645). The server is what says the one statement is enough:
// the catalog holds the new default and nullability, and a second comparison
// finds nothing left to do.
func TestSchemaApplyChangesOnlyTheDefaultLive(t *testing.T) {
	tests := []struct {
		name         string
		before       string
		after        string
		wantPlan     string
		wantDefault  *string
		wantNullable string
	}{
		{
			name:         "a default set on a NOT NULL column",
			before:       "fresh boolean NOT NULL",
			after:        "fresh boolean NOT NULL DEFAULT true",
			wantPlan:     `ALTER TABLE "flags3645" ALTER COLUMN "fresh" SET DEFAULT true;`,
			wantDefault:  new("true"),
			wantNullable: "NO",
		},
		{
			name:         "a default dropped from a nullable column",
			before:       "fresh integer DEFAULT 7",
			after:        "fresh integer",
			wantPlan:     `ALTER TABLE "flags3645" ALTER COLUMN "fresh" DROP DEFAULT;`,
			wantDefault:  nil,
			wantNullable: "YES",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := scratchReplayDatabase(c)
			dir := c.TempDir()
			before := writeColumnChangeSchema(c, dir, "before.sql", test.before)
			after := writeColumnChangeSchema(c, dir, "after.sql", test.after)
			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", before, "--auto-approve")

			plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", after, "--dry-run")
			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", after, "--auto-approve")
			again := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", after, "--dry-run")

			c.Assert(plan, qt.Contains, test.wantPlan)
			c.Assert(plan, qt.Not(qt.Contains), `ALTER COLUMN "fresh" TYPE`)
			c.Assert(plan, qt.Not(qt.Contains), "NOT NULL;")
			c.Assert(plan, qt.Not(qt.Contains), "DO $$")
			c.Assert(plan, qt.Not(qt.Contains), "--;")
			columnDefault, nullable := readColumnDefinition(c, target, "flags3645", "fresh")
			c.Assert(columnDefault, qt.DeepEquals, test.wantDefault)
			c.Assert(nullable, qt.Equals, test.wantNullable)
			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}

// readColumnDefinition answers the column's default as the catalog prints it,
// nil when it has none, and whether it is nullable.
func readColumnDefinition(c *qt.C, url, table, column string) (*string, string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var columnDefault *string
	var nullable string
	err = conn.QueryRowContext(c.Context(),
		`SELECT column_default, is_nullable FROM information_schema.columns
		 WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2`,
		table, column,
	).Scan(&columnDefault, &nullable)
	c.Assert(err, qt.IsNil)
	return columnDefault, nullable
}
