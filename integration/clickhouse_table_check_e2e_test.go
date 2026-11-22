//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// ClickHouse checks each row an INSERT writes against the CHECK constraints of
// the table, refuses a CHECK without a name, and holds a table's constraints
// only in its CREATE TABLE statement. These tests declare table CHECKs through
// Go annotations, one of them without a name, apply them, and ask the server
// which constraints the table holds and which rows it refuses. A plan that
// drops a declared CHECK applies cleanly and plans nothing the next time, so
// only a row the server accepts shows that the CHECK is missing.

// checkEntity is the Go declaration of table t: the table annotation with
// attrs, the given constraint annotations, and the columns id, v with the
// attributes vAttrs, s, and the extra ones.
func checkEntity(attrs string, constraints []string, vAttrs, extra string) string {
	return "package checks\n\n" +
		`//ptah:schema:table name="t"` + attrs + "\n" +
		strings.Join(constraints, "\n") + "\n" +
		"type T struct {\n" +
		"\t//ptah:schema:field name=\"id\" type=\"UInt64\" primary=\"true\"\n\tID uint64\n" +
		"\t//ptah:schema:field name=\"v\" type=\"Int32\" not_null=\"true\"" + vAttrs + "\n\tV int32\n" +
		"\t//ptah:schema:field name=\"s\" type=\"String\" not_null=\"true\"\n\tS string\n" +
		extra +
		"}\n"
}

// checkEntityDir writes the declaration to a directory of its own.
func checkEntityDir(c *qt.C, entity string) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "t.go"), []byte(entity), 0o600), qt.IsNil)
	return dir
}

// checkApply applies the declaration, then applies it again without
// extraArgs, and returns the output of both.
func checkApply(c *qt.C, url, entity string, extraArgs ...string) (applied, again string) {
	c.Helper()
	dir := checkEntityDir(c, entity)
	args := append([]string{"schema", "apply", "--db-url", url, "--root-dir", dir, "--auto-approve"}, extraArgs...)
	first := runPtahIn(c, args...)
	c.Assert(first.ExitCode, qt.Equals, 0, historyComment(first))
	second := runPtahIn(c, "schema", "apply", "--db-url", url, "--root-dir", dir, "--auto-approve")
	c.Assert(second.ExitCode, qt.Equals, 0, historyComment(second))
	return first.Stdout, second.Stdout
}

// checkConstraints reads the name and the kind of each constraint of table t
// in the order the server prints them, which is the order they were added in.
const checkConstraints = "SELECT arrayStringConcat(extractAll(create_table_query, 'CONSTRAINT [^ ]+ (?:CHECK|ASSUME)'), ', ') " +
	"FROM system.tables WHERE database = currentDatabase() AND name = 't'"

// checkInsert runs an INSERT and returns the error the server answers, nil
// when it takes the row.
func checkInsert(c *qt.C, conn *dbschema.DatabaseConnection, row string) error {
	c.Helper()
	_, err := conn.ExecContext(c.Context(), "INSERT INTO t VALUES "+row)
	return err
}

var (
	createdChecks = checkEntity("", []string{
		`//ptah:schema:constraint type="CHECK" table="t" check="v > 0"`,
		`//ptah:schema:constraint name="t_named" type="CHECK" table="t" check="length(s) < 10"`,
	}, ` check="v != 7"`, "")
	changedChecks = checkEntity("", []string{`//ptah:schema:constraint type="CHECK" table="t" check="v > 1"`}, "",
		"\t//ptah:schema:field name=\"w\" type=\"Int32\" not_null=\"true\" default=\"1\" check=\"w > 0\"\n\tW int32\n")
	droppedColumn = checkEntity("", []string{`//ptah:schema:constraint type="CHECK" table="t" check="v > 1"`}, "", "")
	rebuiltChecks = checkEntity(` platform.clickhouse.engine="ReplacingMergeTree"`,
		[]string{`//ptah:schema:constraint type="CHECK" table="t" check="v > 2"`}, "", "")
)

// TestApplyClickHouseTableChecksE2E creates table t with an unnamed table
// CHECK, a named one and a column CHECK, then changes the unnamed one, drops
// the named one and the column CHECK, adds a column with a CHECK of its own,
// drops that column, and rebuilds the table on another engine. Each apply is
// followed by one that plans nothing, and the server refuses each row the
// declared CHECKs refuse.
func TestApplyClickHouseTableChecksE2E(t *testing.T) {
	c := qt.New(t)
	for _, server := range clickHouseServers(c) {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			url := createdDevDialectDatabase(c, server.url, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
			conn := connectDevDialect(c, url)

			_, created := checkApply(c, url, createdChecks)
			c.Assert(created, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(catalogReadBack(c, url, []string{checkConstraints}), qt.DeepEquals,
				[][]string{{"CONSTRAINT t_v_check CHECK, CONSTRAINT t_check CHECK, CONSTRAINT t_named CHECK"}})
			c.Assert(checkInsert(c, conn, "(1, 0, 'a')"), qt.ErrorMatches, "(?s).*code: 469, message: Constraint `t_check` .*")
			c.Assert(checkInsert(c, conn, "(1, 7, 'a')"), qt.ErrorMatches, "(?s).*code: 469, message: Constraint `t_v_check` .*")
			c.Assert(checkInsert(c, conn, "(1, 3, 'a long text')"), qt.ErrorMatches, "(?s).*code: 469, message: Constraint `t_named` .*")
			c.Assert(checkInsert(c, conn, "(1, 3, 'a')"), qt.IsNil)

			plan, changed := checkApply(c, url, changedChecks)
			c.Assert(changed, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(plan, qt.Contains, "-- CLICKHOUSE: ADD CONSTRAINT checks the rows written after it, not the rows t holds\n"+
				"ALTER TABLE t ADD CONSTRAINT t_check CHECK (v > 1);")
			c.Assert(catalogReadBack(c, url, []string{checkConstraints}), qt.DeepEquals,
				[][]string{{"CONSTRAINT t_w_check CHECK, CONSTRAINT t_check CHECK"}})
			c.Assert(checkInsert(c, conn, "(2, 1, 'b', 1)"), qt.ErrorMatches, "(?s).*code: 469, message: Constraint `t_check` .*")
			c.Assert(checkInsert(c, conn, "(2, 5, 'b', 0)"), qt.ErrorMatches, "(?s).*code: 469, message: Constraint `t_w_check` .*")
			c.Assert(checkInsert(c, conn, "(2, 7, 'a long text', 1)"), qt.IsNil)

			_, dropped := checkApply(c, url, droppedColumn)
			c.Assert(dropped, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(catalogReadBack(c, url, []string{checkConstraints}), qt.DeepEquals, [][]string{{"CONSTRAINT t_check CHECK"}})

			rebuildExec(c, conn, "ALTER TABLE t ADD CONSTRAINT a_s ASSUME length(s) < 100")
			plan, rebuilt := checkApply(c, url, rebuiltChecks, "--allow-table-rebuild")
			c.Assert(rebuilt, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(plan, qt.Contains, "-- The new table keeps what no declaration states and t has: the ASSUME constraints a_s\n")
			c.Assert(catalogReadBack(c, url, []string{checkConstraints}), qt.DeepEquals,
				[][]string{{"CONSTRAINT t_check CHECK, CONSTRAINT a_s ASSUME"}})
			c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "ReplacingMergeTree")
			c.Assert(rebuildRows(c, conn, "v"), qt.DeepEquals, [][2]string{{"1", "3"}, {"2", "7"}})
			c.Assert(checkInsert(c, conn, "(3, 2, 'c')"), qt.ErrorMatches, "(?s).*code: 469, message: Constraint `t_check` .*")
		})
	}
}

// TestApplyClickHouseTableChecks_ARebuildStopsOnARowTheCheckRefuses rebuilds a
// table that holds a row its declared CHECK refuses. ALTER TABLE ... ADD
// CONSTRAINT leaves such a row in place, and the copy into the new table
// checks every row, so the plan stops before the swap: the table keeps its
// engine and its rows, and the copy is left for the operator to drop.
func TestApplyClickHouseTableChecks_ARebuildStopsOnARowTheCheckRefuses(t *testing.T) {
	c := qt.New(t)
	for _, server := range clickHouseServers(c) {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			url := createdDevDialectDatabase(c, server.url, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
			conn := connectDevDialect(c, url)
			rebuildExec(c, conn,
				"CREATE TABLE t (id UInt64, v Int32, s String, CONSTRAINT t_check CHECK v > 0) ENGINE = MergeTree ORDER BY id",
				"INSERT INTO t VALUES (1, 2, 'a'), (2, 5, 'b')")
			dir := checkEntityDir(c, rebuiltChecks)

			applied := runPtahIn(c, "schema", "apply", "--db-url", url, "--root-dir", dir, "--auto-approve", "--allow-table-rebuild")

			c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(applied.Stderr, qt.Matches, "(?s).*code: 469, message: Constraint `t_check` for table [^ ]+ptah_rebuild_t .*")
			c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "MergeTree")
			c.Assert(rebuildRows(c, conn, "v"), qt.DeepEquals, [][2]string{{"1", "2"}, {"2", "5"}})
			c.Assert(rebuildTables(c, conn), qt.DeepEquals, []string{"ptah_rebuild_t", "t"})
		})
	}
}
