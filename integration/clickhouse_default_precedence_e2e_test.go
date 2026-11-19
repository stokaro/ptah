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

// clickHousePrecedenceDefaultSchema declares expression defaults ClickHouse
// stores with every operation it groups in parentheses: ClickHouse 26.10
// reports 1 + 2 * 3 as 1 + (2 * 3), 1 + 2 > 2 as (1 + 2) > 2, a - b - c as
// (a - b) - c, the predicate a + 1 IS NOT NULL AND NOT b = 1 as
// ((a + 1) IS NOT NULL) AND (NOT (b = 1)), and 1.50 * a as 1.5 * a in
// system.columns.default_expression. q and u hold the parentheses their
// grouping needs. ClickHouse 24.10 and 26.10 store a = 1 OR b = 3 as
// (a = 1) OR (b = 3), c BETWEEN 1 AND 3 as (c >= 1) AND (c <= 3),
// a NOT BETWEEN 1 AND 3 as (a < 1) OR (a > 3), and NOT a and -a as written.
const clickHousePrecedenceDefaultSchema = "CREATE TABLE tallies (\n" +
	"  id UInt64,\n" +
	"  a Int64 DEFAULT 9,\n" +
	"  b Int64 DEFAULT 3,\n" +
	"  c Int64 DEFAULT 2,\n" +
	"  p Int64 DEFAULT 1 + 2 * 3,\n" +
	"  q Int64 DEFAULT (1 + 2) * 3,\n" +
	"  r UInt8 DEFAULT 1 + 2 > 2,\n" +
	"  s Int64 DEFAULT a - b - c,\n" +
	"  u Int64 DEFAULT a - (b - c),\n" +
	"  v UInt8 DEFAULT a + 1 IS NOT NULL AND NOT b = 1,\n" +
	"  w UInt8 DEFAULT a = 1 OR b = 3 AND c = 2,\n" +
	"  x Float64 DEFAULT 1.50 * a,\n" +
	"  o UInt8 DEFAULT a = 1 OR b = 3,\n" +
	"  y UInt8 DEFAULT c BETWEEN 1 AND 3,\n" +
	"  z UInt8 DEFAULT a NOT BETWEEN 1 AND 3,\n" +
	"  g UInt8 DEFAULT NOT a,\n" +
	"  n Int64 DEFAULT -a\n" +
	") ENGINE = MergeTree ORDER BY id;\n"

// TestApplyConvergesOnAClickHousePrecedenceDefaultE2E applies
// clickHousePrecedenceDefaultSchema twice through `ptah schema apply`. The
// second apply plans nothing, which says the comparison reads the spelling
// the server stores as the default the file declares, and a row that takes
// every default reads back what the server computed for each. Compared as
// text, the second apply plans to modify every default but q, u, g and n.
func TestApplyConvergesOnAClickHousePrecedenceDefaultE2E(t *testing.T) {
	c := qt.New(t)
	database := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.ClickHouse), "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(clickHousePrecedenceDefaultSchema), 0o600), qt.IsNil)
	apply := []string{"schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve"}

	applied := runPtahIn(c, apply...)
	again := runPtahIn(c, apply...)

	c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
	c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
	c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
	applyEngineFile(c, database, platform.ClickHouse, "INSERT INTO tallies (id) VALUES (1);\n")
	read := "SELECT concat(toString(p), ',', toString(q), ',', toString(r), ',', toString(s), ',', toString(u), ',', " +
		"toString(v), ',', toString(w), ',', toString(x), ',', toString(o), ',', toString(y), ',', toString(z), ',', " +
		"toString(g), ',', toString(n)) FROM tallies"
	c.Assert(catalogReadBack(c, database, []string{read}), qt.DeepEquals, [][]string{{"7,9,1,4,8,1,1,13.5,1,1,1,0,-9"}})
}
