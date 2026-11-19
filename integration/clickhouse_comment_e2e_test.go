//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// TestApplyKeepsAndChangesAClickHouseCommentE2E applies a schema whose table
// and column carry comments, twice, then a schema that changes both, twice,
// and reads the comments the server holds. A reader that drops the column
// comment plans the column on every apply, and a plan that restates the
// column with MODIFY COLUMN leaves the old comment in place and plans the
// change again.
func TestApplyKeepsAndChangesAClickHouseCommentE2E(t *testing.T) {
	c := qt.New(t)
	database := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.ClickHouse), "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
	write := func(name, schema string) string {
		file := filepath.Join(c.TempDir(), name)
		c.Assert(os.WriteFile(file, []byte(schema), 0o600), qt.IsNil)
		return file
	}
	first := write("first.sql", "CREATE TABLE notes (id Int32, a String COMMENT 'one') ENGINE = MergeTree ORDER BY id COMMENT 'table one';\n")
	second := write("second.sql", "CREATE TABLE notes (id Int32, a String COMMENT 'it''s \\\\ two') ENGINE = MergeTree ORDER BY id COMMENT 'table two';\n")
	apply := func(file string) []string {
		return []string{"schema", "apply", "--db-url", database, "--schema-file", file, "--auto-approve"}
	}

	created := runPtahIn(c, apply(first)...)
	kept := runPtahIn(c, apply(first)...)
	changed := runPtahIn(c, apply(second)...)
	again := runPtahIn(c, apply(second)...)

	c.Assert(created.ExitCode, qt.Equals, 0, historyComment(created))
	c.Assert(kept.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n", historyComment(kept))
	c.Assert(changed.ExitCode, qt.Equals, 0, historyComment(changed))
	c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n", historyComment(again))
	reads := []string{
		"SELECT hex(comment) FROM system.columns WHERE database = currentDatabase() AND table = 'notes' AND name = 'a'",
		"SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = 'notes'",
	}
	c.Assert(catalogReadBack(c, database, reads), qt.DeepEquals, [][]string{{"69742773205C2074776F"}, {"table two"}})
}
