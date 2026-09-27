//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// slotsSchema is a table whose UNIQUE over pos is declared with clause, which
// may be empty.
func slotsSchema(clause string) string {
	return "CREATE TABLE slots (id int PRIMARY KEY, pos int NOT NULL, CONSTRAINT slots_pos_key UNIQUE (pos)" + clause + ");\n"
}

// appliedSlots applies [slotsSchema] with clause to an empty database through
// `ptah schema apply`, fills it with two rows, and swaps their positions in
// one statement. It returns the error the swap met.
func appliedSlots(c *qt.C, clause string) error {
	c.Helper()
	target, _ := scratchReplayDatabase(c)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(slotsSchema(clause)), 0o600), qt.IsNil)
	runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")
	c.Assert(runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run"),
		qt.Contains, "Schema is synced")
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), "INSERT INTO slots VALUES (1, 1), (2, 2)")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(c.Context(), "UPDATE slots SET pos = 3 - pos")
	return err
}

// TestADeferrableKeyChecksAtTheEndOfTheStatementE2E applies a schema file whose
// UNIQUE is DEFERRABLE, and swaps two values of the key in one UPDATE. The
// statement passes through a state where both rows hold the same value;
// PostgreSQL 18.6 checks a deferrable key at the end of the statement, so the
// swap succeeds. Written plain, the key checks each row and the swap fails
// (stokaro/ptah#3824).
func TestADeferrableKeyChecksAtTheEndOfTheStatementE2E(t *testing.T) {
	for _, clause := range []string{" DEFERRABLE", " DEFERRABLE INITIALLY DEFERRED"} {
		t.Run(clause, func(t *testing.T) {
			c := qt.New(t)

			err := appliedSlots(c, clause)

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestAKeyThatDoesNotDeferChecksEachRowE2E is the control: the same swap
// against the key declared without the clause is refused.
func TestAKeyThatDoesNotDeferChecksEachRowE2E(t *testing.T) {
	c := qt.New(t)

	err := appliedSlots(c, "")

	c.Assert(err, qt.ErrorMatches, `(?s).*duplicate key value violates unique constraint "slots_pos_key".*`)
}
