//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
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
	annotations := append([]string{`//ptah:schema:table name="t"` + attrs}, constraints...)
	return "package checks\n\n" +
		strings.Join(annotations, "\n") + "\n" +
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

// heldRowScan is the SELECT that stops the plan when table t holds a row the
// CHECK name with condition refuses.
func heldRowScan(name, condition string) string {
	return fmt.Sprintf("SELECT throwIf(count() > 0, '%s is broken by rows t holds') FROM `t` WHERE NOT ifNull((%s), 0);\n", name, condition)
}

// addedCheck is the plan text that adds the CHECK name with condition to
// table t: the note, the ADD and the scan after it. The scan ahead of it comes
// earlier in the plan.
func addedCheck(name, condition string) string {
	return "-- CLICKHOUSE: ADD CONSTRAINT " + name + " checks only the rows written after it; " +
		"a SELECT ahead of it and one after it scan t for held rows the CHECK refuses\n" +
		fmt.Sprintf("ALTER TABLE `t` ADD CONSTRAINT `%s` CHECK (%s);\n", name, condition) + heldRowScan(name, condition)
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
			c.Assert(plan, qt.Contains, heldRowScan("t_check", "v > 1")+addedCheck("t_check", "v > 1"))
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

// tableChecks is table t with one table CHECK annotation per condition, each
// with the name the condition's entry gives it or none, and with the extra
// columns.
func tableChecks(extra string, checks ...[2]string) string {
	annotations := make([]string, 0, len(checks))
	for _, check := range checks {
		name := ""
		if check[0] != "" {
			name = ` name="` + check[0] + `"`
		}
		annotations = append(annotations, `//ptah:schema:constraint`+name+` type="CHECK" table="t" check="`+check[1]+`"`)
	}
	return checkEntity("", annotations, "", extra)
}

// upperV is a column V beside v, which ClickHouse holds as another column.
const upperV = "\t//ptah:schema:field name=\"V\" type=\"Int32\" not_null=\"true\" default=\"1\"\n\tUpperV int32\n"

// TestApplyClickHouseTableChecks_AReformattedCheckPlansNothing declares CHECKs
// in spellings each line stores reformatted: both lines store a cast as
// `CAST(v, 'Int64')` and group the operands of OR, and 24.10 stores
// `equals(v, v)` as `v = v` and `greater(v, 0)` as `v > 0`. The apply after the
// one that creates them plans nothing.
func TestApplyClickHouseTableChecks_AReformattedCheckPlansNothing(t *testing.T) {
	c := qt.New(t)
	for _, server := range clickHouseServers(c) {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			url := createdDevDialectDatabase(c, server.url, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
			entity := tableChecks("", [2]string{"", "CAST(v AS Int64) > 0"}, [2]string{"", "v::Int64 > 0"},
				[2]string{"", "s ILIKE 'a%' OR s = ''"}, [2]string{"", "equals(v, v)"}, [2]string{"", "greater(v, 0)"})

			_, again := checkApply(c, url, entity)

			c.Assert(again, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(catalogReadBack(c, url, []string{checkConstraints}), qt.DeepEquals, [][]string{{
				"CONSTRAINT t_check CHECK, CONSTRAINT t_check1 CHECK, CONSTRAINT t_check2 CHECK, CONSTRAINT t_check3 CHECK, CONSTRAINT t_check4 CHECK",
			}})
		})
	}
}

// TestApplyClickHouseTableChecks_AnUnnamedCheckKeepsItsName swaps two unnamed
// CHECKs, which plans nothing, and then names a third CHECK t_check, the name
// the first unnamed one holds: only that one is replaced, under the next free
// name, and the other keeps t_check1. Each CHECK still refuses its rows.
func TestApplyClickHouseTableChecks_AnUnnamedCheckKeepsItsName(t *testing.T) {
	c := qt.New(t)
	for _, server := range clickHouseServers(c) {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			url := createdDevDialectDatabase(c, server.url, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
			conn := connectDevDialect(c, url)
			checkApply(c, url, tableChecks("", [2]string{"", "v < 10"}, [2]string{"", "v != 5"}))

			swapped, _ := checkApply(c, url, tableChecks("", [2]string{"", "v != 5"}, [2]string{"", "v < 10"}))
			plan, again := checkApply(c, url, tableChecks("", [2]string{"t_check", "v != 7"}, [2]string{"", "v < 10"}, [2]string{"", "v != 5"}))

			c.Assert(swapped, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(again, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(plan, qt.Contains, heldRowScan("t_check", "v != 7")+heldRowScan("t_check2", "v < 10")+
				"ALTER TABLE `t` DROP CONSTRAINT `t_check`;\n"+addedCheck("t_check", "v != 7")+addedCheck("t_check2", "v < 10"))
			c.Assert(plan, qt.Not(qt.Contains), "t_check1")
			c.Assert(catalogReadBack(c, url, []string{checkConstraints}), qt.DeepEquals,
				[][]string{{"CONSTRAINT t_check1 CHECK, CONSTRAINT t_check CHECK, CONSTRAINT t_check2 CHECK"}})
			c.Assert(checkInsert(c, conn, "(1, 7, 'a')"), qt.ErrorMatches, "(?s).*code: 469, message: Constraint `t_check` .*")
			c.Assert(checkInsert(c, conn, "(1, 5, 'a')"), qt.ErrorMatches, "(?s).*code: 469, message: Constraint `t_check1` .*")
			c.Assert(checkInsert(c, conn, "(1, 11, 'a')"), qt.ErrorMatches, "(?s).*code: 469, message: Constraint `t_check2` .*")
			c.Assert(checkInsert(c, conn, "(1, 3, 'a')"), qt.IsNil)
		})
	}
}

// TestApplyClickHouseTableChecks_ACheckOnAColumnInAnotherCaseIsAnotherCheck
// moves a CHECK from column v to column V, which ClickHouse holds as another
// column: the plan replaces the CHECK, and the server then refuses rows by V.
func TestApplyClickHouseTableChecks_ACheckOnAColumnInAnotherCaseIsAnotherCheck(t *testing.T) {
	c := qt.New(t)
	for _, server := range clickHouseServers(c) {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			url := createdDevDialectDatabase(c, server.url, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
			conn := connectDevDialect(c, url)
			checkApply(c, url, tableChecks(upperV, [2]string{"t_check", "v > 0"}))

			plan, again := checkApply(c, url, tableChecks(upperV, [2]string{"t_check", "V > 0"}))

			c.Assert(again, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(plan, qt.Contains, heldRowScan("t_check", "V > 0")+"ALTER TABLE `t` DROP CONSTRAINT `t_check`;\n"+addedCheck("t_check", "V > 0"))
			c.Assert(checkInsert(c, conn, "(1, 1, 'a', 0)"), qt.ErrorMatches, "(?s).*code: 469, message: Constraint `t_check` .*")
			c.Assert(checkInsert(c, conn, "(1, 0, 'a', 1)"), qt.IsNil)
		})
	}
}

// TestApplyClickHouseTableChecks_AnAddedCheckStopsOnAHeldRowItRefuses adds a
// CHECK to a table that holds a row it refuses: by its value, by a NULL, which
// an INSERT refuses too, and by a string with a quote in it. ADD CONSTRAINT
// checks only the rows written after it, so the plan scans the table before
// the ADD, and the scan stops the apply with code 395 naming the constraint.
// The constraint stays absent and the row stays, so a retried apply stops the
// same way. Once the row is gone, the apply adds the constraint, and the next
// one plans nothing.
func TestApplyClickHouseTableChecks_AnAddedCheckStopsOnAHeldRowItRefuses(t *testing.T) {
	c := qt.New(t)
	nullable := "\t//ptah:schema:field name=\"n\" type=\"Int32\"\n\tN *int32\n"
	refused := "(?s).*code: 395, message: t_held is broken by rows t holds.*"
	for _, server := range clickHouseServers(c) {
		for _, test := range []struct {
			name, row, check string
		}{
			{name: "a value", row: "(1, 0, 'a', 1)", check: "v > 0"},
			{name: "a NULL", row: "(1, 3, 'a', NULL)", check: "n > 0"},
			{name: "a quoted string", row: "(1, 3, 'it''s', 1)", check: "s != 'it''s' AND s != 'x\\\\y'"},
		} {
			t.Run(server.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				url := createdDevDialectDatabase(c, server.url, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
				conn := connectDevDialect(c, url)
				checkApply(c, url, tableChecks(nullable))
				c.Assert(checkInsert(c, conn, test.row), qt.IsNil)
				dir := checkEntityDir(c, tableChecks(nullable, [2]string{"t_held", test.check}))
				apply := []string{"schema", "apply", "--db-url", url, "--root-dir", dir, "--auto-approve"}

				applied := runPtahIn(c, apply...)
				afterApplied := catalogReadBack(c, url, []string{checkConstraints, "SELECT count() FROM t"})
				retried := runPtahIn(c, apply...)
				afterRetried := catalogReadBack(c, url, []string{checkConstraints, "SELECT count() FROM t"})
				_, emptied := conn.ExecContext(c.Context(), "TRUNCATE TABLE t")
				fixed := runPtahIn(c, apply...)
				afterFixed := catalogReadBack(c, url, []string{checkConstraints})
				again := runPtahIn(c, apply...)

				c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0)
				c.Assert(applied.Stderr, qt.Matches, refused)
				c.Assert(afterApplied, qt.DeepEquals, [][]string{{""}, {"1"}})
				c.Assert(retried.ExitCode, qt.Not(qt.Equals), 0)
				c.Assert(retried.Stderr, qt.Matches, refused)
				c.Assert(afterRetried, qt.DeepEquals, [][]string{{""}, {"1"}})
				c.Assert(emptied, qt.IsNil)
				c.Assert(fixed.ExitCode, qt.Equals, 0, historyComment(fixed))
				c.Assert(afterFixed, qt.DeepEquals, [][]string{{"CONSTRAINT t_held CHECK"}})
				c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
				c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			})
		}
	}
}

// TestApplyClickHouseTableChecks_AChangedCheckStopsAheadOfItsDrop changes the
// condition of a CHECK while table t holds a row the new condition refuses and
// the old one takes. The plan changes no column of t, so the scan runs ahead
// of DROP CONSTRAINT: the apply stops with code 395 and reports the scan alone,
// and the table keeps the old CHECK, which still refuses a row and still takes
// the held value.
func TestApplyClickHouseTableChecks_AChangedCheckStopsAheadOfItsDrop(t *testing.T) {
	c := qt.New(t)
	for _, server := range clickHouseServers(c) {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			url := createdDevDialectDatabase(c, server.url, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
			conn := connectDevDialect(c, url)
			checkApply(c, url, tableChecks("", [2]string{"t_held", "v > 0"}))
			c.Assert(checkInsert(c, conn, "(1, 3, 'a')"), qt.IsNil)
			dir := checkEntityDir(c, tableChecks("", [2]string{"t_held", "v > 5"}))

			applied := runPtahIn(c, "schema", "apply", "--db-url", url, "--root-dir", dir, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(applied.Stderr, qt.Matches, "(?s).*code: 395, message: t_held is broken by rows t holds.*")
			c.Assert(applied.Stderr, qt.Contains,
				"\nSQL: SELECT throwIf(count() > 0, 't_held is broken by rows t holds') FROM `t` WHERE NOT ifNull((v > 5), 0)\n")
			c.Assert(catalogReadBack(c, url, []string{checkConstraints, "SELECT count() FROM t"}), qt.DeepEquals,
				[][]string{{"CONSTRAINT t_held CHECK"}, {"1"}})
			c.Assert(checkInsert(c, conn, "(9, -7, 'b')"), qt.ErrorMatches, "(?s).*code: 469, message: Constraint `t_held` .*")
			c.Assert(checkInsert(c, conn, "(2, 3, 'b')"), qt.IsNil)
		})
	}
}

// createdT is table t as a schema file, and checkedT is the same table with
// the CHECK t_held.
const (
	createdT = "CREATE TABLE `t` (`id` UInt64, `v` Int32) ENGINE = MergeTree ORDER BY id;\n"
	checkedT = "CREATE TABLE `t` (`id` UInt64, `v` Int32, CONSTRAINT `t_held` CHECK (v > 0)) ENGINE = MergeTree ORDER BY id;\n"
)

// unscannedWarning is the run warning of a plan that adds CHECK t_held to t
// without the scans.
const unscannedWarning = "warning: CHECK t_held on t is added without a scan of the rows t holds: " +
	"the server checks only the rows written after ADD CONSTRAINT, so a held row the CHECK refuses stays\n"

// TestApplyClickHouseTableChecks_WithoutTheScansAHeldRowStays adds a CHECK to
// table t while it holds a row the CHECK refuses, with the scans skipped by the
// flag and by its variable alone. The plan writes no scan and says so above
// the ADD, the run warns, and the apply succeeds: the row stays, and the CHECK
// refuses the rows written after it.
func TestApplyClickHouseTableChecks_WithoutTheScansAHeldRowStays(t *testing.T) {
	c := qt.New(t)
	unscanned := "-- CLICKHOUSE: ADD CONSTRAINT t_held checks only the rows written after it, " +
		"and the plan has no scan: the rows t already holds are not checked\nALTER TABLE `t` ADD CONSTRAINT `t_held` CHECK (v > 0);\n"
	for _, server := range clickHouseServers(c) {
		for _, test := range []struct {
			name string
			args []string
			env  []string
		}{
			{name: "the flag", args: []string{"--skip-check-scan"}},
			{name: "the variable", env: []string{"PTAH_SKIP_CHECK_SCAN=1"}},
		} {
			t.Run(server.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				url := createdDevDialectDatabase(c, server.url, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
				conn := connectDevDialect(c, url)
				checkApply(c, url, tableChecks(""))
				c.Assert(checkInsert(c, conn, "(1, 0, 'a')"), qt.IsNil)
				dir := checkEntityDir(c, tableChecks("", [2]string{"t_held", "v > 0"}))

				applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Env: append(environmentWithoutPtahVariables(), test.env...)},
					append([]string{"schema", "apply", "--db-url", url, "--root-dir", dir, "--auto-approve"}, test.args...)...)

				c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
				c.Assert(applied.Stdout, qt.Contains, unscanned)
				c.Assert(applied.Stdout, qt.Not(qt.Contains), "throwIf")
				c.Assert(applied.Stderr, qt.Contains, unscannedWarning)
				c.Assert(catalogReadBack(c, url, []string{checkConstraints, "SELECT count() FROM t"}), qt.DeepEquals,
					[][]string{{"CONSTRAINT t_held CHECK"}, {"1"}})
				c.Assert(checkInsert(c, conn, "(2, 0, 'b')"), qt.ErrorMatches, "(?s).*code: 469, message: Constraint `t_held` .*")
			})
		}
	}
}

// TestApplyClickHouseTableChecks_CompatReadsTheScanVariable adds a CHECK
// through ptah-compat while table t holds a row the CHECK refuses. Without the
// variable the scan stops the apply and the CHECK stays absent; with it the
// apply succeeds, warns, and leaves the row.
func TestApplyClickHouseTableChecks_CompatReadsTheScanVariable(t *testing.T) {
	c := qt.New(t)
	for _, server := range clickHouseServers(c) {
		for _, test := range []struct {
			name       string
			env        []string
			wantExit   int
			wantStderr string
			wantChecks string
		}{
			{name: "without the variable", wantExit: 1, wantStderr: "code: 395, message: t_held is broken by rows t holds"},
			{name: "with the variable", env: []string{"PTAH_SKIP_CHECK_SCAN=1"}, wantStderr: unscannedWarning, wantChecks: "CONSTRAINT t_held CHECK"},
		} {
			t.Run(server.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				url := createdDevDialectDatabase(c, server.url, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
				devURL := createdDevDialectDatabase(c, server.url, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
				conn := connectDevDialect(c, url)
				dir := c.TempDir()
				project := "env \"local\" {\n  url = \"" + url + "\"\n  src = \"file://schema.sql\"\n  dev = \"" + devURL + "\"\n}\n"
				c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)
				c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(createdT), 0o600), qt.IsNil)
				first := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
					"schema", "apply", "--env", "local", "--auto-approve")
				c.Assert(first.ExitCode, qt.Equals, 0, historyComment(first))
				c.Assert(checkInsert(c, conn, "(1, 0)"), qt.IsNil)
				c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(checkedT), 0o600), qt.IsNil)

				applied := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: append(environmentWithoutPtahVariables(), test.env...)},
					"schema", "apply", "--env", "local", "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, test.wantExit, historyComment(applied))
				c.Assert(applied.Stderr, qt.Contains, test.wantStderr)
				c.Assert(catalogReadBack(c, url, []string{checkConstraints, "SELECT count() FROM t"}), qt.DeepEquals,
					[][]string{{test.wantChecks}, {"1"}})
			})
		}
	}
}

// TestGenerateClickHouseTableChecks_TheMigrationKeepsOrSkipsTheScans generates
// a migration whose up file adds a CHECK to table t, and one whose down file
// adds it back, with and without --skip-check-scan. By default the file scans t
// for the held rows the CHECK refuses, ahead of the ADD and after it; with the
// flag it writes no scan and says so above the ADD. Each row pins the whole
// body after the generated header. The rollback adds the CHECK the database
// holds, and its scans read the condition as the server stores it: 26.10 wraps
// it in parentheses, and 24.10 does not.
func TestGenerateClickHouseTableChecks_TheMigrationKeepsOrSkipsTheScans(t *testing.T) {
	c := qt.New(t)
	const up, down = "-- Migration generated from schema differences\n", "-- Migration rollback\n"
	scanned := strings.TrimSuffix(heldRowScan("t_held", "v > 0")+addedCheck("t_held", "v > 0"), "\n")
	stored := map[string]string{"current": "(v > 0)", "24.10": "v > 0"}
	readBack := func(server string) string {
		return strings.TrimSuffix(heldRowScan("t_held", stored[server])+
			strings.TrimSuffix(addedCheck("t_held", "v > 0"), heldRowScan("t_held", "v > 0"))+heldRowScan("t_held", stored[server]), "\n")
	}
	unscanned := "-- CLICKHOUSE: ADD CONSTRAINT t_held checks only the rows written after it, " +
		"and the plan has no scan: the rows t already holds are not checked\nALTER TABLE `t` ADD CONSTRAINT `t_held` CHECK (v > 0);"
	held := tableChecks("", [2]string{"t_held", "v > 0"})
	for _, server := range clickHouseServers(c) {
		for _, test := range []struct {
			name             string
			current, desired string
			file, header     string
			args             []string
			want             string
		}{
			{name: "up, scanned", current: tableChecks(""), desired: held, file: "*.up.sql", header: up, want: scanned},
			{name: "up, unscanned", current: tableChecks(""), desired: held, file: "*.up.sql", header: up, args: []string{"--skip-check-scan"}, want: unscanned},
			{name: "down, scanned", current: held, desired: tableChecks(""), file: "*.down.sql", header: down, want: readBack(server.name)},
			{name: "down, unscanned", current: held, desired: tableChecks(""), file: "*.down.sql", header: down, args: []string{"--skip-check-scan"}, want: unscanned},
		} {
			t.Run(server.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				url := createdDevDialectDatabase(c, server.url, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
				checkApply(c, url, test.current)
				dir := checkEntityDir(c, test.desired)
				migrations := c.TempDir()

				generated := runPtahIn(c, append([]string{"migrations", "generate", "--db-url", url, "--root-dir", dir, "--migrations-dir", migrations}, test.args...)...)
				written, err := filepath.Glob(filepath.Join(migrations, test.file))

				c.Assert(generated.ExitCode, qt.Equals, 0, historyComment(generated))
				c.Assert(err, qt.IsNil)
				c.Assert(written, qt.HasLen, 1)
				body, err := os.ReadFile(written[0])
				c.Assert(err, qt.IsNil)
				c.Assert(string(body), qt.Matches, `(?s)`+regexp.QuoteMeta(test.header)+`.*\n\n`+regexp.QuoteMeta(test.want))
			})
		}
	}
}

// planningProject is a directory whose schema.sql holds schema. Its atlas.hcl
// env local names url, schema.sql, devURL and the migration directory
// migrations, whose URL ends in query.
func planningProject(c *qt.C, url, devURL, schema, query string) string {
	c.Helper()
	dir := c.TempDir()
	project := "env \"local\" {\n  url = \"" + url + "\"\n  src = \"file://schema.sql\"\n  dev = \"" + devURL + "\"\n" +
		"  migration {\n    dir = \"file://migrations" + query + "\"\n  }\n}\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(schema), 0o600), qt.IsNil)
	return dir
}

// Markers of the two plans, free of the characters JSON escapes, so they match
// a plan document too: the scan's message, and the note above an ADD the plan
// does not scan for.
const (
	scanMarker = "t_held is broken by rows t holds"
	noteMarker = "ADD CONSTRAINT t_held checks only the rows written after it, " +
		"and the plan has no scan: the rows t already holds are not checked"
)

// plannedChecks runs a planning verb in dir and returns what it planned: its
// standard output and error, followed by the migration files it wrote for the
// change named add.
func plannedChecks(c *qt.C, target clirun.Target, dir string, env, args []string) string {
	c.Helper()
	got := clirun.Run(c, target, clirun.Options{Dir: dir, Env: append(environmentWithoutPtahVariables(), env...)}, args...)
	c.Assert(got.ExitCode, qt.Equals, 0, historyComment(got))
	written, err := filepath.Glob(filepath.Join(dir, "migrations", "*_add*.sql"))
	c.Assert(err, qt.IsNil)
	planned := []string{got.Stdout, got.Stderr}
	for _, path := range written {
		body, err := os.ReadFile(path)
		c.Assert(err, qt.IsNil)
		planned = append(planned, string(body))
	}
	return strings.Join(planned, "\n")
}

// TestPlanClickHouseTableChecks_EachPlanningVerbReadsTheOptOut plans the CHECK
// t_held onto table t through each verb that plans without applying, once as
// it is and once with the opt-out: --skip-check-scan on a native verb, and
// PTAH_SKIP_CHECK_SCAN=1 on a ptah-compat verb, which takes no flag. The plan
// scans t without the opt-out; with it the plan has the note and no scan.
// The apply and generate tests above cover the verbs that write, and the next
// test covers ptah-compat migrate diff.
func TestPlanClickHouseTableChecks_EachPlanningVerbReadsTheOptOut(t *testing.T) {
	c := qt.New(t)
	for _, server := range clickHouseServers(c) {
		for _, verb := range []struct {
			name      string
			target    clirun.Target
			args      []string
			optOutArg []string
			optOutEnv []string
		}{
			{name: "schema compare", target: clirun.Ptah, args: []string{"schema", "compare", "--db-url", "{url}", "--schema-file", "schema.sql"},
				optOutArg: []string{"--skip-check-scan"}},
			{name: "schema diff", target: clirun.Ptah, args: []string{"schema", "diff", "--from", "{url}", "--to", "schema.sql"},
				optOutArg: []string{"--skip-check-scan"}},
			{name: "schema plan", target: clirun.Ptah, args: []string{"schema", "plan", "--db-url", "{url}", "--schema-file", "schema.sql", "--dry-run"},
				optOutArg: []string{"--skip-check-scan"}},
			{name: "migrations plan", target: clirun.Ptah, args: []string{"migrations", "plan", "--db-url", "{url}", "--schema-file", "schema.sql"},
				optOutArg: []string{"--skip-check-scan"}},
			{name: "compat schema diff", target: clirun.Compat, args: []string{"schema", "diff", "--env", "local", "--from", "{url}", "--to", "file://schema.sql"},
				optOutEnv: []string{"PTAH_SKIP_CHECK_SCAN=1"}},
			{name: "compat schema plan", target: clirun.Compat, args: []string{"schema", "plan", "--env", "local", "--dry-run"},
				optOutEnv: []string{"PTAH_SKIP_CHECK_SCAN=1"}},
		} {
			t.Run(server.name+"/"+verb.name, func(t *testing.T) {
				c := qt.New(t)
				url := createdDevDialectDatabase(c, server.url, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
				devURL := createdDevDialectDatabase(c, server.url, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
				args := strings.Split(strings.ReplaceAll(strings.Join(verb.args, "\x00"), "{url}", url), "\x00")
				_, err := connectDevDialect(c, url).ExecContext(c.Context(), createdT)
				c.Assert(err, qt.IsNil)
				dir := planningProject(c, url, devURL, checkedT, "")

				plain := plannedChecks(c, verb.target, dir, nil, args)
				optedOut := plannedChecks(c, verb.target, dir, verb.optOutEnv, append(args, verb.optOutArg...))

				c.Assert(plain, qt.Contains, scanMarker)
				c.Assert(plain, qt.Not(qt.Contains), noteMarker)
				c.Assert(optedOut, qt.Contains, noteMarker)
				c.Assert(optedOut, qt.Not(qt.Contains), scanMarker)
				c.Assert(optedOut, qt.Contains, unscannedWarning)
			})
		}
	}
}

// TestPlanClickHouseTableChecks_CompatMigrateDiffReadsTheOptOut writes a
// migration directory that creates table t, then has ptah-compat migrate diff
// add the CHECK t_held to it, with PTAH_SKIP_CHECK_SCAN=1 and without, in the
// Atlas layout and in golang-migrate's, which plans a rollback too. The
// migration file scans t without the variable; with it the file has the note
// and no scan. It runs on the current line alone: migrate diff replays the
// directory on the dev database and cleans it, and Ptah refuses that cleanup
// on a server older than 24.11, which cannot prove it sees the whole catalog.
func TestPlanClickHouseTableChecks_CompatMigrateDiffReadsTheOptOut(t *testing.T) {
	for _, test := range []struct {
		name       string
		query      string
		env        []string
		want, omit string
	}{
		{name: "atlas, without the variable", want: scanMarker, omit: noteMarker},
		{name: "atlas, with the variable", env: []string{"PTAH_SKIP_CHECK_SCAN=1"}, want: noteMarker, omit: scanMarker},
		{name: "golang-migrate, without the variable", query: "?format=golang-migrate", want: scanMarker, omit: noteMarker},
		{name: "golang-migrate, with the variable", query: "?format=golang-migrate", env: []string{"PTAH_SKIP_CHECK_SCAN=1"}, want: noteMarker, omit: scanMarker},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server := dbtarget.URL(c, dbtarget.ClickHouse)
			url := createdDevDialectDatabase(c, server, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
			devURL := createdDevDialectDatabase(c, server, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
			dir := planningProject(c, url, devURL, createdT, test.query)
			initial := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}, "migrate", "diff", "--env", "local", "init")
			c.Assert(initial.ExitCode, qt.Equals, 0, historyComment(initial))
			c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(checkedT), 0o600), qt.IsNil)

			planned := plannedChecks(c, clirun.Compat, dir, test.env, []string{"migrate", "diff", "--env", "local", "add"})

			c.Assert(planned, qt.Contains, test.want)
			c.Assert(planned, qt.Not(qt.Contains), test.omit)
		})
	}
}
