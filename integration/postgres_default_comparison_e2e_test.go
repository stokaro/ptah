//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dbtarget"
)

// postgresComparisonDefaultSchema declares the defaults PostgreSQL 18 reads
// without parentheses beyond arithmetic: a comparison, IS DISTINCT FROM, and
// a minus before a call or a group. PostgreSQL stores each with the
// parentheses pg_get_expr prints, such as ((1 + 1) = 2).
const postgresComparisonDefaultSchema = "CREATE TABLE marks (\n" +
	"  id int NOT NULL,\n" +
	"  lt boolean DEFAULT 1 < 2,\n" +
	"  sum_eq boolean DEFAULT 1 + 1 = 2,\n" +
	"  ne boolean DEFAULT 2 <> 2 NOT NULL,\n" +
	"  distinct_from boolean DEFAULT 1 IS DISTINCT FROM 2,\n" +
	"  negated_call int DEFAULT -abs(5),\n" +
	"  negated_group int DEFAULT -(2 + 3)\n" +
	");\n"

// TestApplyPostgresComparisonDefaultE2E applies postgresComparisonDefaultSchema
// twice through `ptah schema apply`. The second apply plans nothing, and a row
// that takes every default reads back what the server computed for each.
func TestApplyPostgresComparisonDefaultE2E(t *testing.T) {
	c := qt.New(t)
	database := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL),
		"DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(postgresComparisonDefaultSchema), 0o600), qt.IsNil)
	apply := []string{"schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve"}

	applied := runPtahIn(c, apply...)
	again := runPtahIn(c, apply...)

	c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
	c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
	c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
	applyEngineFile(c, database, platform.Postgres, "INSERT INTO marks (id) VALUES (1);\n")
	read := "SELECT concat_ws(',', lt, sum_eq, ne, distinct_from, negated_call, negated_group) FROM marks"
	c.Assert(catalogReadBack(c, database, []string{read}), qt.DeepEquals, [][]string{{"t,t,f,t,-5,-5"}})
}
