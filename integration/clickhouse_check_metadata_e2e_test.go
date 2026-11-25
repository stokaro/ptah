//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// A failed CHECK scan must keep the old constraint when the accompanying
// column change removes a column or changes only its comment. Both leave the
// values read by the new condition available before any ALTER runs.
func TestApplyClickHouseCheck_KeepsConstraintForColumnMetadata(t *testing.T) {
	for _, test := range []struct {
		name, columns string
	}{
		{name: "removed column", columns: "id UInt64, v Int32"},
		{name: "column comment", columns: "id UInt64, v Int32, s String COMMENT 'a note'"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.ClickHouse), "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
			conn := connectDevDialect(c, url)
			rebuildExec(c, conn,
				"CREATE TABLE t (id UInt64, v Int32, s String, CONSTRAINT t_held CHECK (v > 0)) ENGINE = MergeTree ORDER BY id",
				"INSERT INTO t VALUES (1, 3, 'a')",
			)
			file := filepath.Join(c.TempDir(), "schema.sql")
			desired := "CREATE TABLE t (" + test.columns + ", CONSTRAINT t_held CHECK (v > 5)) ENGINE = MergeTree ORDER BY id;"
			c.Assert(os.WriteFile(file, []byte(desired), 0o600), qt.IsNil)

			applied := runPtahIn(c, "schema", "apply", "--db-url", url, "--schema-file", file, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(applied.Stderr, qt.Matches, "(?s).*code: 395, message: t_held is broken by rows t holds.*")
			c.Assert(catalogReadBack(c, url, []string{checkConstraints, "SELECT count() FROM t"}), qt.DeepEquals,
				[][]string{{"CONSTRAINT t_held CHECK"}, {"1"}},
			)
			c.Assert(checkInsert(c, conn, "(2, -1, 'b')"), qt.ErrorMatches, "(?s).*code: 469, message: Constraint `t_held` .*")
			c.Assert(checkInsert(c, conn, "(2, 3, 'b')"), qt.IsNil)
		})
	}
}
