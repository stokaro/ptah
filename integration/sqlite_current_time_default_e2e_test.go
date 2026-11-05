//go:build integration

package integration_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestCompatMigrateDiffE2E_SQLiteKeepsACurrentTimeDefault diffs a table with each
// of SQLite's current-time keywords as a default through two steps. SQLite answers
// `near "(": syntax error` to CURRENT_TIMESTAMP(), so a file that spells the
// keyword as a call stops the second diff when it replays the first file. The row
// read back carries a value the default filled.
func TestCompatMigrateDiffE2E_SQLiteKeepsACurrentTimeDefault(t *testing.T) {
	c := qt.New(t)
	schemas := []string{
		"CREATE TABLE t (id integer PRIMARY KEY, ts datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, d date DEFAULT CURRENT_DATE, h time DEFAULT CURRENT_TIME);\n",
		"CREATE TABLE t (id integer PRIMARY KEY, ts datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, d date DEFAULT CURRENT_DATE, h time DEFAULT CURRENT_TIME, y int);\n",
	}
	const dev = "sqlite://dev?mode=memory&_fk=1"

	pairs := readMigrationPairs(c, diffEngineSteps(c, "", dev, schemas))
	target := sqliteScratchURL(c)

	c.Assert(pairs, qt.HasLen, 2)
	c.Assert(pairs[0].Up, qt.Contains, "DEFAULT CURRENT_TIMESTAMP,")
	c.Assert(pairs[0].Up, qt.Not(qt.Contains), "CURRENT_TIMESTAMP()")
	for _, pair := range pairs {
		applyEngineFile(c, target, "sqlite", pair.Up)
	}
	conn := connectDevDialect(c, target)
	_, err := conn.ExecContext(c.Context(), "INSERT INTO t (id) VALUES (1)")
	c.Assert(err, qt.IsNil)
	var ts string
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT ts FROM t").Scan(&ts), qt.IsNil)
	c.Assert(strings.TrimSpace(ts), qt.Not(qt.Equals), "")
}
