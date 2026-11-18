//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// oraclePredicateDefaultSchema declares defaults that continue an operator
// expression with a predicate, outside parentheses, which Oracle takes. After
// IS the NOT NULL is the test's; the NOT NULL after the complete predicate is
// the column's. Read as the column's, the first NOT NULL would end the default
// at 1 + 2 IS, which Oracle refuses.
const oraclePredicateDefaultSchema = "CREATE TABLE ptah_predicate_defaults (\n" +
	"  id NUMBER PRIMARY KEY,\n" +
	"  a BOOLEAN DEFAULT 1 + 2 IS NOT NULL NOT NULL,\n" +
	"  b BOOLEAN DEFAULT 1 + 2 IS NULL OR 1 = 2\n" +
	");\n"

// TestApplyOraclePredicateDefaultE2E applies the predicate defaults through
// `ptah schema apply`, inserts a row that takes both, and reads the row back:
// Oracle evaluates a to true and b to false, and a is NOT NULL. A second apply
// plans nothing, so the comparison reads each default Oracle stores as the one
// the file declares.
func TestApplyOraclePredicateDefaultE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	dbURL := dbtarget.URL(c, dbtarget.Oracle)
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	defer func() {
		c.Check(conn.SchemaWriter().DropAllTables(context.WithoutCancel(ctx)), qt.IsNil)
	}()
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(oraclePredicateDefaultSchema), 0o600), qt.IsNil)
	apply := []string{"schema", "apply", "--db-url", dbURL, "--schema-file", schema, "--auto-approve"}

	applied, appliedErr, err := runPtahSplitStreams(ctx, apply)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied, appliedErr))
	again, againErr, err := runPtahSplitStreams(ctx, apply)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", again, againErr))

	c.Assert(again, qt.Equals, "Schema is synced, no changes to be made.\n")
	_, err = conn.ExecContext(ctx, "INSERT INTO ptah_predicate_defaults (id) VALUES (1)")
	c.Assert(err, qt.IsNil)
	var got string
	err = conn.QueryRowContext(ctx, "SELECT a || ',' || b || ',' || "+
		"(SELECT nullable FROM user_tab_columns WHERE table_name = 'PTAH_PREDICATE_DEFAULTS' AND column_name = 'A') "+
		"FROM ptah_predicate_defaults").Scan(&got)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "1,0,N")
}
