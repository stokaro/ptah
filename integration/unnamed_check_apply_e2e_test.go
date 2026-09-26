//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// checkConstraintsQuery lists every CHECK of the public schema with its table,
// name and definition as the server prints them.
const checkConstraintsQuery = `SELECT conrelid::regclass::text || ' ' || conname || ': ' || pg_get_constraintdef(oid)
FROM pg_constraint
WHERE contype = 'c' AND connamespace = 'public'::regnamespace
ORDER BY 1`

// checkConstraintsOf reads back the CHECKs a database holds.
func checkConstraintsOf(c *qt.C, databaseURL string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	rows, err := conn.QueryContext(c.Context(), checkConstraintsQuery)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var checks []string
	for rows.Next() {
		var check string
		c.Assert(rows.Scan(&check), qt.IsNil)
		checks = append(checks, check)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return checks
}

// TestSchemaApplyCreatesTheChecksTheFileCreatesE2E applies a schema file with
// unnamed CHECKs to a database holding its tables, and reads the CHECKs back:
// they are the ones the file's own SQL creates, name and condition alike.
//
// The expected set is read from a second database the file's SQL was run on,
// so the assertion is the server's answer and not a list restating Ptah's
// rule. A plan that drops a CHECK the file declares fails it: were two unnamed
// CHECKs on one table one to the comparison, the plan would drop both of the
// database's CHECKs and add back only the later one (stokaro/ptah#3729).
func TestSchemaApplyCreatesTheChecksTheFileCreatesE2E(t *testing.T) {
	tests := []struct {
		name     string
		existing string
		schema   string
	}{
		{
			name:     "a table without its CHECKs",
			existing: `CREATE TABLE d (lo int, hi int);`,
			schema:   `CREATE TABLE d (lo int CHECK (lo > 0), hi int, CHECK (lo < hi), CHECK (hi > 0));`,
		},
		{
			name: "a table holding the CHECKs under other names",
			existing: `CREATE TABLE d (lo int CONSTRAINT old_lo CHECK (lo > 0), hi int,
  CONSTRAINT old_range CHECK (lo < hi), CONSTRAINT old_hi CHECK (hi > 0));`,
			schema: `CREATE TABLE d (lo int CHECK (lo > 0), hi int, CHECK (lo < hi), CHECK (hi > 0));`,
		},
		{
			name:     "two CHECKs on one column",
			existing: `CREATE TABLE h2 (a int CONSTRAINT old_low CHECK (a > 0), CONSTRAINT old_high CHECK (a < 10));`,
			schema:   `CREATE TABLE h2 (a int CHECK (a > 0) CHECK (a < 10));`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			want := checkConstraintsOf(c, databaseBuiltFrom(c, test.schema))
			target := databaseBuiltFrom(c, test.existing)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(test.schema+"\n"), 0o600), qt.IsNil)

			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			c.Assert(checkConstraintsOf(c, target), qt.DeepEquals, want)
			out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
			c.Assert(out, qt.Contains, "Schema is synced")
		})
	}
}
