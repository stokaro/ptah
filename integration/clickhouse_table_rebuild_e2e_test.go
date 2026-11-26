//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	neturl "net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/chcolumn"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// ClickHouse has no ALTER TABLE that changes a table's engine, PARTITION BY or
// PRIMARY KEY, or an ORDER BY that MODIFY ORDER BY cannot make. Ptah creates
// the table again under another name, copies its rows into it and swaps the
// two with EXCHANGE TABLES, only when the operator asks with
// --allow-table-rebuild. These tests read the table definition and the rows
// back from the server, since a plan that reads correctly can still be refused
// by it, and then plan again to see that nothing is left to do.

// rebuildFixtureColumns is the column list every declaration below shares.
const rebuildFixtureColumns = `    columns:
      id: {type: UInt64, not_null: true}
      name: {type: String, not_null: true}
      ver: {type: UInt32, not_null: true}
      d: {type: Date, not_null: true}
`

const rebuildFixtureRows = "INSERT INTO t VALUES (1, 'a', 1, '2024-01-05'), (2, 'b', 2, '2024-02-05'), (3, 'c', 3, '2024-02-06')"

// rebuildScratch creates a database on the ClickHouse server, drops it when the
// test ends, and returns its URL and a connection to it.
func rebuildScratch(c *qt.C) (string, *dbschema.DatabaseConnection) {
	c.Helper()
	url := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.ClickHouse), "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
	return url, connectDevDialect(c, url)
}

// rebuildScratchWithEngine is rebuildScratch for a database of the given
// engine.
func rebuildScratchWithEngine(c *qt.C, engine string) (string, *dbschema.DatabaseConnection) {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.ClickHouse)
	admin := connectDevDialect(c, adminURL)
	name := devDialectScratchName()
	rebuildExec(c, admin, "CREATE DATABASE "+name+" ENGINE = "+engine)
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.WithoutCancel(c.Context()), "DROP DATABASE IF EXISTS "+name+" SYNC")
		c.Check(err, qt.IsNil)
	})
	url := renameInPath(c, adminURL, name)
	return url, connectDevDialect(c, url)
}

// rebuildPolicy creates a row policy, and drops it when the test ends: a row
// policy outlives the database that holds its table. The test's context is
// canceled before a cleanup runs.
func rebuildPolicy(c *qt.C, conn *dbschema.DatabaseConnection, name, table, using string) {
	c.Helper()
	rebuildExec(c, conn, "CREATE ROW POLICY "+name+" ON "+table+" FOR SELECT USING "+using+" TO ALL")
	c.Cleanup(func() {
		_, err := conn.ExecContext(context.WithoutCancel(c.Context()), "DROP ROW POLICY IF EXISTS "+name+" ON "+table)
		c.Check(err, qt.IsNil)
	})
}

// rebuildUser creates a user that holds every privilege on the database the
// connection uses except SYSTEM MERGES, drops it when the test ends, and
// returns the URL that connects as it: a user outlives the database.
func rebuildUser(c *qt.C, url string, conn *dbschema.DatabaseConnection) string {
	c.Helper()
	var database string
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT currentDatabase()").Scan(&database), qt.IsNil)
	name := "ptah_" + database
	rebuildExec(c, conn,
		"CREATE USER "+name+" IDENTIFIED BY 'merges'",
		"GRANT ALL ON "+database+".* TO "+name,
		"REVOKE SYSTEM MERGES ON "+database+".* FROM "+name)
	c.Cleanup(func() {
		_, err := conn.ExecContext(context.WithoutCancel(c.Context()), "DROP USER IF EXISTS "+name)
		c.Check(err, qt.IsNil)
	})
	parsed, err := neturl.Parse(url)
	c.Assert(err, qt.IsNil)
	parsed.User = neturl.UserPassword(name, "merges")
	return parsed.String()
}

func rebuildExec(c *qt.C, conn *dbschema.DatabaseConnection, statements ...string) {
	c.Helper()
	for _, statement := range statements {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// rebuildDefinition reads the storage clauses of table t, with the parentheses
// and spaces of a key left out: 24.10 and 26.10 spell the same key differently.
func rebuildDefinition(c *qt.C, conn *dbschema.DatabaseConnection) [4]string {
	c.Helper()
	var definition [4]string
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT engine, partition_key, replaceRegexpAll(sorting_key, '[() ]', ''), replaceRegexpAll(primary_key, '[() ]', '') "+
			"FROM system.tables WHERE database = currentDatabase() AND name = 't'",
	).Scan(&definition[0], &definition[1], &definition[2], &definition[3]), qt.IsNil)
	return definition
}

// rebuildRows reads the id and the second column of every row of table t, in id
// order.
func rebuildRows(c *qt.C, conn *dbschema.DatabaseConnection, secondColumn string) [][2]string {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(), "SELECT toString(id), toString("+secondColumn+") FROM t ORDER BY id")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var result [][2]string
	for rows.Next() {
		var row [2]string
		c.Assert(rows.Scan(&row[0], &row[1]), qt.IsNil)
		result = append(result, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return result
}

func rebuildTables(c *qt.C, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(),
		"SELECT name FROM system.tables WHERE database = currentDatabase() AND name NOT LIKE 'ptah_schema%' ORDER BY name")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// rebuildApply writes the declaration to a file and applies it.
func rebuildApply(c *qt.C, url, declaration string, extraArgs ...string) clirun.Result {
	c.Helper()
	dir := c.TempDir()
	file := filepath.Join(dir, "schema.yaml")
	c.Assert(os.WriteFile(file, []byte(declaration), 0o600), qt.IsNil)
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
		append([]string{"schema", "apply", "--db-url", url, "--schema-file", file, "--auto-approve"}, extraArgs...)...)
}

func rebuildDeclaration(properties string) string {
	return "tables:\n  t:\n    platform:\n      clickhouse:\n" + properties + rebuildFixtureColumns
}

// rebuildSynced applies the declaration again, without the request, and
// asserts there is nothing left to do. A plan that still had a rebuild in it
// would be refused without the request.
func rebuildSynced(c *qt.C, url, declaration string, extraArgs ...string) {
	c.Helper()
	again := rebuildApply(c, url, declaration, extraArgs...)
	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
	c.Assert(again.Stdout+again.Stderr, qt.Contains, "Schema is synced, no changes to be made.")
}

// rebuildCount reads one number.
func rebuildCount(c *qt.C, conn *dbschema.DatabaseConnection, query string) string {
	c.Helper()
	var count string
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&count), qt.IsNil)
	return count
}

// rebuildEvents is the declaration most tests apply: the engine changes, which
// only a rebuild can do.
var rebuildEvents = rebuildDeclaration("        engine: ReplacingMergeTree(ver)\n        order_by: id\n")

// rebuildCreate is the table most tests start from, before rebuildFixtureRows.
const rebuildCreate = "CREATE TABLE t (id UInt64, name String, ver UInt32, d Date) ENGINE = MergeTree ORDER BY id"

// rebuildFixtureRowsRead is what rebuildRows reads after rebuildFixtureRows.
var rebuildFixtureRowsRead = [][2]string{{"1", "a"}, {"2", "b"}, {"3", "c"}}

func TestClickHouseTableRebuild_HappyPath(t *testing.T) {
	tests := []struct {
		name           string
		create         string
		properties     string
		wantDefinition [4]string
	}{
		{
			name:           "the engine changes",
			create:         rebuildCreate,
			properties:     "        engine: ReplacingMergeTree(ver)\n        order_by: id\n",
			wantDefinition: [4]string{"ReplacingMergeTree", "", "id", "id"},
		},
		{
			name:           "PARTITION BY changes",
			create:         rebuildCreate,
			properties:     "        engine: MergeTree\n        order_by: id\n        partition_by: toYYYYMM(d)\n",
			wantDefinition: [4]string{"MergeTree", "toYYYYMM(d)", "id", "id"},
		},
		{
			name:           "PRIMARY KEY changes",
			create:         "CREATE TABLE t (id UInt64, name String, ver UInt32, d Date) ENGINE = MergeTree ORDER BY (id, name) PRIMARY KEY id",
			properties:     "        engine: MergeTree\n        order_by: (id, name)\n        primary_key: (id, name)\n",
			wantDefinition: [4]string{"MergeTree", "", "id,name", "id,name"},
		},
		{
			name:           "ORDER BY changes to a key MODIFY ORDER BY cannot make",
			create:         "CREATE TABLE t (id UInt64, name String, ver UInt32, d Date) ENGINE = MergeTree ORDER BY (id, name)",
			properties:     "        engine: MergeTree\n        order_by: (name, id)\n",
			wantDefinition: [4]string{"MergeTree", "", "name,id", "name,id"},
		},
		{
			// The merges of the copy stop while it is counted, so none of
			// its rows merges away before the count.
			name:           "the engine changes and the table declares a TTL",
			create:         rebuildCreate,
			properties:     "        engine: ReplacingMergeTree(ver)\n        order_by: id\n        ttl: d + toIntervalYear(100)\n",
			wantDefinition: [4]string{"ReplacingMergeTree", "", "id", "id"},
		},
		{
			name:           "a Log table becomes a MergeTree table",
			create:         "CREATE TABLE t (id UInt64, name String, ver UInt32, d Date) ENGINE = Log",
			properties:     "        engine: MergeTree\n        order_by: id\n",
			wantDefinition: [4]string{"MergeTree", "", "id", "id"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url, conn := rebuildScratch(c)
			rebuildExec(c, conn, test.create, rebuildFixtureRows)

			applied := rebuildApply(c, url, rebuildDeclaration(test.properties), "--allow-table-rebuild")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(rebuildDefinition(c, conn), qt.Equals, test.wantDefinition)
			c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, rebuildFixtureRowsRead)
			c.Assert(rebuildTables(c, conn), qt.DeepEquals, []string{"t"})
			rebuildSynced(c, url, rebuildDeclaration(test.properties))
		})
	}
}

// The table keeps its row policy and its grant, both of which name it, and gets
// its skipping indexes with the rows already written.
func TestClickHouseTableRebuild_KeepsWhatAttachesToTheTable(t *testing.T) {
	c := qt.New(t)
	url, conn := rebuildScratch(c)
	rebuildExec(c, conn,
		"CREATE TABLE t (id UInt64, name String, ver UInt32, d Date, INDEX name_idx name TYPE bloom_filter(0.01) GRANULARITY 3) ENGINE = MergeTree ORDER BY id",
		rebuildFixtureRows,
	)
	rebuildPolicy(c, conn, "mine", "t", "id > 0")
	declaration := "tables:\n  t:\n    platform:\n      clickhouse:\n        engine: ReplacingMergeTree(ver)\n        order_by: id\n" +
		rebuildFixtureColumns +
		"    indexes:\n      name_idx:\n        fields: [name]\n        platform:\n          clickhouse:\n            type: bloom_filter(0.01)\n            granularity: \"3\"\n" +
		"row_policies:\n  mine:\n    table: t\n    using: id > 0\n    to: ALL\n"

	applied := rebuildApply(c, url, declaration, "--allow-table-rebuild")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "ReplacingMergeTree")
	c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, rebuildFixtureRowsRead)
	c.Assert(rebuildCount(c, conn,
		"SELECT name || ' ' || type_full || ' ' || toString(granularity) FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't'",
	), qt.Equals, "name_idx bloom_filter(0.01) 3")
	c.Assert(rebuildCount(c, conn,
		"SELECT short_name FROM system.row_policies WHERE database = currentDatabase() AND table = 't'",
	), qt.Equals, "mine")
	rebuildSynced(c, url, declaration)
}

// A column the declaration adds takes its default, one it drops is not copied,
// and a column whose type changes is checked on the way.
func TestClickHouseTableRebuild_ReshapesTheColumns(t *testing.T) {
	c := qt.New(t)
	url, conn := rebuildScratch(c)
	rebuildExec(c, conn,
		"CREATE TABLE t (id UInt64, name String, ver UInt32, obsolete String) ENGINE = MergeTree ORDER BY id",
		"INSERT INTO t VALUES (1, 'a', 1, 'x'), (2, 'b', 2, 'y')",
	)
	declaration := "tables:\n  t:\n    platform:\n      clickhouse:\n        engine: ReplacingMergeTree(ver)\n        order_by: id\n" +
		"    columns:\n      id: {type: UInt64, not_null: true}\n      name: {type: String, not_null: true}\n" +
		"      ver: {type: UInt16, not_null: true}\n      note: {type: String, not_null: true, default: \"n\"}\n"

	applied := rebuildApply(c, url, declaration, "--allow-table-rebuild")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(rebuildRows(c, conn, "note"), qt.DeepEquals, [][2]string{{"1", "n"}, {"2", "n"}})
	c.Assert(rebuildCount(c, conn,
		"SELECT arrayStringConcat(groupArray(name || ' ' || type), ', ') FROM (SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't' ORDER BY position)",
	), qt.Equals, "id UInt64, name String, ver UInt16, note String")
	c.Assert(rebuildRows(c, conn, "ver"), qt.DeepEquals, [][2]string{{"1", "1"}, {"2", "2"}})
	rebuildSynced(c, url, declaration)
}

// Without the request the plan is refused naming it, and the table is untouched.
func TestClickHouseTableRebuild_FailurePath(t *testing.T) {
	t.Run("a plan that creates the table again is refused without the request", func(t *testing.T) {
		c := qt.New(t)
		url, conn := rebuildScratch(c)
		rebuildExec(c, conn, rebuildCreate, rebuildFixtureRows)

		applied := rebuildApply(c, url, rebuildEvents)

		c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
		c.Assert(applied.Stderr, qt.Contains, "ClickHouse has no ALTER TABLE that changes a table's ENGINE; Ptah creates the table again and keeps its rows when asked with --allow-table-rebuild")
		c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "MergeTree")
		c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, rebuildFixtureRowsRead)
		c.Assert(rebuildTables(c, conn), qt.DeepEquals, []string{"t"})
	})

	// The conversion is checked while the copy is written, and the table is
	// not touched until the swap. The next plan finds the copy's name taken
	// and says the table holds every row.
	t.Run("a value the new type cannot hold stops the rebuild with the table untouched", func(t *testing.T) {
		c := qt.New(t)
		url, conn := rebuildScratch(c)
		rebuildExec(c, conn,
			"CREATE TABLE t (id UInt64, ver Int64) ENGINE = MergeTree ORDER BY id",
			"INSERT INTO t VALUES (1, 5), (2, 300)")
		declaration := "tables:\n  t:\n    platform:\n      clickhouse:\n        engine: ReplacingMergeTree(ver)\n        order_by: id\n" +
			"    columns:\n      id: {type: UInt64, not_null: true}\n      ver: {type: UInt8, not_null: true}\n"

		applied := rebuildApply(c, url, declaration, "--allow-table-rebuild")

		c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
		c.Assert(applied.Stderr, qt.Contains, "cannot be safely converted into type UInt8")
		c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "MergeTree")
		c.Assert(rebuildRows(c, conn, "ver"), qt.DeepEquals, [][2]string{{"1", "5"}, {"2", "300"}})
		c.Assert(rebuildTables(c, conn), qt.DeepEquals, []string{"ptah_rebuild_t", "t"})

		again := rebuildApply(c, url, declaration, "--allow-table-rebuild")

		c.Assert(again.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
		c.Assert(again.Stderr, qt.Contains, "the database holds a table named ptah_rebuild_t, the name the new table takes until the swap. "+
			"A rebuild of t that stopped leaves it there, and t then holds every row that rebuild read, whichever statement it stopped at. "+
			"Drop ptah_rebuild_t and run the plan again")
		c.Assert(rebuildRows(c, conn, "ver"), qt.DeepEquals, [][2]string{{"1", "5"}, {"2", "300"}})
	})

	tests := []struct {
		name       string
		extraArgs  []string
		wantStderr string
	}{
		{name: "the plan sees it", wantStderr: "the database holds a table named ptah_rebuild_t, the name the new table takes until the swap"},
		{name: "the plan does not see it, and the server refuses to create it", extraArgs: []string{"--exclude", "ptah_rebuild_t"}, wantStderr: "already exists"},
	}
	for _, test := range tests {
		t.Run("a table of the copy's name is not overwritten: "+test.name, func(t *testing.T) {
			c := qt.New(t)
			url, conn := rebuildScratch(c)
			rebuildExec(c, conn, rebuildCreate, rebuildFixtureRows,
				"CREATE TABLE ptah_rebuild_t (id UInt64) ENGINE = MergeTree ORDER BY id",
				"INSERT INTO ptah_rebuild_t VALUES (99)")

			applied := rebuildApply(c, url, rebuildEvents, append([]string{"--allow-table-rebuild"}, test.extraArgs...)...)

			c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stderr, qt.Contains, test.wantStderr)
			c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "MergeTree")
			c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, rebuildFixtureRowsRead)
			c.Assert(rebuildCount(c, conn, "SELECT toString(groupArray(id)) FROM ptah_rebuild_t"), qt.Equals, "[99]")
		})
	}

	// The engine is read before anything runs. A copy into a Null table would
	// drop every row, and the swap would then put the empty table in place.
	t.Run("an engine that keeps no rows here is refused with the request", func(t *testing.T) {
		c := qt.New(t)
		url, conn := rebuildScratch(c)
		rebuildExec(c, conn, rebuildCreate, rebuildFixtureRows)

		applied := rebuildApply(c, url, rebuildDeclaration("        engine: \"Null\"\n"), "--allow-table-rebuild")

		c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
		c.Assert(applied.Stderr, qt.Contains, "Ptah does not create table t again, because its engine Null does not keep the table's rows on this server.")
		c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "MergeTree")
		c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, rebuildFixtureRowsRead)
		c.Assert(rebuildTables(c, conn), qt.DeepEquals, []string{"t"})
	})

	// The first statement of the rebuild fails before anything is created.
	t.Run("a database that cannot swap two tables is refused before the copy", func(t *testing.T) {
		c := qt.New(t)
		url, conn := rebuildScratchWithEngine(c, "Memory")
		rebuildExec(c, conn, rebuildCreate, rebuildFixtureRows)

		applied := rebuildApply(c, url, rebuildEvents, "--allow-table-rebuild")

		c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
		c.Assert(applied.Stderr, qt.Contains, "code: 395, message: cannot create t again: EXCHANGE TABLES swaps two tables only in an Atomic database, and the database of t is not one")
		c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "MergeTree")
		c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, rebuildFixtureRowsRead)
		c.Assert(rebuildTables(c, conn), qt.DeepEquals, []string{"t"})
	})

	// The copy's merges stop while it is filled, which takes SYSTEM MERGES. A
	// statement before CREATE TABLE checks the privilege, so a user without it
	// is refused with nothing created rather than with an empty copy left
	// behind by the stop.
	t.Run("a user without SYSTEM MERGES is refused before the copy", func(t *testing.T) {
		c := qt.New(t)
		url, conn := rebuildScratch(c)
		rebuildExec(c, conn, rebuildCreate, rebuildFixtureRows)

		applied := rebuildApply(c, rebuildUser(c, url, conn), rebuildEvents, "--allow-table-rebuild")

		c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
		c.Assert(applied.Stderr, qt.Contains, "code: 497")
		c.Assert(applied.Stderr, qt.Contains, "necessary to have the grant SYSTEM MERGES ON ")
		c.Assert(rebuildTables(c, conn), qt.DeepEquals, []string{"t"})
		c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "MergeTree")
		c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, rebuildFixtureRowsRead)
	})

	// The copy reads the table as the user running the plan. A row policy that
	// hides a row from that user would leave it out of the copy, and it would
	// go with the old table.
	t.Run("a row policy that hides rows from the user is refused before the copy", func(t *testing.T) {
		c := qt.New(t)
		url, conn := rebuildScratch(c)
		rebuildExec(c, conn, rebuildCreate, rebuildFixtureRows)
		rebuildPolicy(c, conn, "some", "t", "id > 1")
		declaration := rebuildEvents + "row_policies:\n  some:\n    table: t\n    using: id > 1\n    to: ALL\n"

		applied := rebuildApply(c, url, declaration, "--allow-table-rebuild")

		c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
		c.Assert(applied.Stderr, qt.Contains, "code: 395, message: cannot create t again: the user running the plan does not read every row t stores, "+
			"so the copy would leave rows out and they would go with the old table.")
		c.Assert(rebuildTables(c, conn), qt.DeepEquals, []string{"t"})
		rebuildExec(c, conn, "DROP ROW POLICY some ON t")
		c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "MergeTree")
		c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, rebuildFixtureRowsRead)
	})
}

// The copy is counted against the table before the swap. A write to the table
// during the copy cannot be timed in a test, so a row policy on the copy's name
// makes the two counts differ the same way: the plan stops with the table
// untouched, the next plan finds the copy's name taken, and once the copy is
// dropped the rebuild runs.
func TestClickHouseTableRebuild_ACopyThatDiffersStopsBeforeTheSwap(t *testing.T) {
	c := qt.New(t)
	url, conn := rebuildScratch(c)
	rebuildExec(c, conn, rebuildCreate, rebuildFixtureRows)
	// A table filter of the session's settings hides a row of the copy from
	// the count, the way a row written to the table during the copy would
	// make the table hold one more.
	filtered, err := neturl.Parse(url)
	c.Assert(err, qt.IsNil)
	query := filtered.Query()
	query.Set("additional_table_filters", "{'ptah_rebuild_t': 'id > 1'}")
	filtered.RawQuery = query.Encode()

	applied := rebuildApply(c, filtered.String(), rebuildEvents, "--allow-table-rebuild")

	c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stderr, qt.Contains, "code: 395, message: cannot create t again: the copy ptah_rebuild_t holds fewer rows than t")
	c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "MergeTree")
	c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, rebuildFixtureRowsRead)
	c.Assert(rebuildTables(c, conn), qt.DeepEquals, []string{"ptah_rebuild_t", "t"})

	again := rebuildApply(c, url, rebuildEvents, "--allow-table-rebuild")

	c.Assert(again.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
	c.Assert(again.Stderr, qt.Contains, "the database holds a table named ptah_rebuild_t, the name the new table takes until the swap")

	rebuildExec(c, conn, "DROP TABLE ptah_rebuild_t SYNC")
	resumed := rebuildApply(c, url, rebuildEvents, "--allow-table-rebuild")

	c.Assert(resumed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", resumed.Stdout, resumed.Stderr))
	c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "ReplacingMergeTree")
	c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, rebuildFixtureRowsRead)
	rebuildSynced(c, url, rebuildEvents)
}

// A rebuild that stops after the swap leaves the new table in place, holding
// every row, and the old one under the copy's name. A dictionary that loads
// from that name makes the server refuse the last DROP TABLE. Once nothing
// depends on it, the next plan drops it like any table the declaration does not
// hold, with or without the request, and then has nothing left to do.
func TestClickHouseTableRebuild_AStopAfterTheSwapLeavesTheNewTable(t *testing.T) {
	tests := []struct {
		name      string
		extraArgs []string
	}{
		{name: "the next plan has the request", extraArgs: []string{"--allow-table-rebuild"}},
		{name: "the next plan has no request"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url, conn := rebuildScratch(c)
			rebuildExec(c, conn, rebuildCreate, rebuildFixtureRows)
			rebuildExec(c, conn, rebuildInDatabase(c, conn, []string{
				"CREATE DICTIONARY hold (id UInt64, name String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'ptah_rebuild_t' DB '{db}')) LAYOUT(FLAT()) LIFETIME(0)",
			})...)

			stopped := rebuildApply(c, url, rebuildEvents, "--allow-table-rebuild")

			c.Assert(stopped.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", stopped.Stdout, stopped.Stderr))
			c.Assert(stopped.Stderr, qt.Contains, "code: 630")
			c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "ReplacingMergeTree")
			c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, rebuildFixtureRowsRead)
			c.Assert(rebuildCount(c, conn,
				"SELECT engine || ' ' || toString(total_rows) FROM system.tables WHERE database = currentDatabase() AND name = 'ptah_rebuild_t'",
			), qt.Equals, "MergeTree 3")

			rebuildExec(c, conn, "DROP DICTIONARY hold")
			again := rebuildApply(c, url, rebuildEvents, test.extraArgs...)

			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(rebuildTables(c, conn), qt.DeepEquals, []string{"t"})
			c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, rebuildFixtureRowsRead)
			rebuildSynced(c, url, rebuildEvents)
		})
	}
}

// Everything that finds the table by name uses the new table after the swap,
// and none of them sees the copy being written: a materialized view that reads
// the table is not fed the copied rows and fires for a row written after, one
// that writes to the table reaches the new one, a Buffer table flushes into
// it, and a dictionary loads from it.
func TestClickHouseTableRebuild_DependentsUseTheNewTable(t *testing.T) {
	tests := []struct {
		name       string
		dependents []string
		// extraArgs keep a materialized view, and the table it reads, out of
		// the plan, so that the plan does not drop them. Ptah reads no
		// dictionary and no Buffer table.
		extraArgs []string
		after     []string
		probe     string
		want      string
	}{
		{
			// The view's target counts the rows the view is fed: four, had the
			// copy gone through it, and zero, had it stayed on the old table.
			name:       "a materialized view that reads the table",
			dependents: []string{"CREATE MATERIALIZED VIEW mv ENGINE = SummingMergeTree ORDER BY k AS SELECT 1 AS k, toUInt64(1) AS n FROM t"},
			extraArgs:  []string{"--exclude", "mv"},
			after:      []string{"INSERT INTO t VALUES (4, 'd', 4, '2024-03-01')"},
			probe:      "SELECT toString(sum(n)) FROM mv",
			want:       "1",
		},
		{
			name: "a materialized view that writes to the table",
			dependents: []string{
				"CREATE TABLE src (id UInt64, name String, ver UInt32, d Date) ENGINE = MergeTree ORDER BY id",
				"CREATE MATERIALIZED VIEW mv TO t AS SELECT id, name, ver, d FROM src",
			},
			extraArgs: []string{"--exclude", "mv", "--exclude", "src"},
			after:     []string{"INSERT INTO src VALUES (4, 'd', 4, '2024-03-01')"},
			probe:     "SELECT toString(count()) FROM t",
			want:      "4",
		},
		{
			// The row waits in the buffer through the rebuild, and DROP TABLE
			// flushes it.
			name: "a Buffer table that flushes into the table",
			dependents: []string{
				"CREATE TABLE b (id UInt64, name String, ver UInt32, d Date) ENGINE = Buffer(currentDatabase(), t, 1, 3600, 3600, 1000000, 1000000, 100000000, 100000000)",
				"INSERT INTO b VALUES (4, 'd', 4, '2024-03-01')",
			},
			after: []string{"DROP TABLE b SYNC"},
			probe: "SELECT toString(count()) FROM t",
			want:  "4",
		},
		{
			name: "a dictionary that loads from the table",
			dependents: []string{"CREATE DICTIONARY dict (id UInt64, name String) PRIMARY KEY id " +
				"SOURCE(CLICKHOUSE(TABLE 't' DB '{db}' USER {user} PASSWORD {password})) LAYOUT(FLAT()) LIFETIME(0)"},
			after: []string{"INSERT INTO t VALUES (4, 'd', 4, '2024-03-01')", "SYSTEM RELOAD DICTIONARY {db}.dict"},
			probe: "SELECT dictGet('{db}.dict', 'name', toUInt64(4))",
			want:  "d",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url, conn := rebuildScratch(c)
			rebuildExec(c, conn, rebuildCreate, rebuildFixtureRows)
			rebuildExec(c, conn, rebuildInDatabase(c, conn, test.dependents)...)

			applied := rebuildApply(c, url, rebuildEvents, append([]string{"--allow-table-rebuild"}, test.extraArgs...)...)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "ReplacingMergeTree")
			c.Assert(rebuildTables(c, conn), qt.Not(qt.Contains), "ptah_rebuild_t")
			rebuildExec(c, conn, rebuildInDatabase(c, conn, test.after)...)
			c.Assert(rebuildCount(c, conn, rebuildInDatabase(c, conn, []string{test.probe})[0]), qt.Equals, test.want)
			rebuildSynced(c, url, rebuildEvents, test.extraArgs...)
		})
	}
}

// A view, a Merge table and a remote() query only read the table, so they do
// not stop the rebuild, and read the new table after it.
func TestClickHouseTableRebuild_ReadersReadTheNewTable(t *testing.T) {
	tests := []struct {
		name   string
		reader string
		// extraArgs keep a view out of the plan. Ptah reads no Merge table,
		// so the plan leaves it alone.
		extraArgs []string
		probe     string
		wantCount string
	}{
		{
			name:      "a plain view",
			reader:    "CREATE VIEW v AS SELECT id FROM t",
			extraArgs: []string{"--exclude", "v"},
			probe:     "SELECT toString(count()) FROM v",
			wantCount: "3",
		},
		{
			name:      "a view that joins the table by a comma",
			reader:    "CREATE VIEW v AS SELECT a.dummy FROM system.one AS a, t",
			extraArgs: []string{"--exclude", "v"},
			probe:     "SELECT toString(count()) FROM v",
			wantCount: "3",
		},
		{
			name:      "a view that tests membership in the table",
			reader:    "CREATE VIEW v AS SELECT 1 AS x WHERE (toUInt64(1), 'a', toUInt32(1), toDate('2024-01-05')) IN t",
			extraArgs: []string{"--exclude", "v"},
			probe:     "SELECT toString(count()) FROM v",
			wantCount: "1",
		},
		{
			name:      "a view that reads the table through a table function",
			reader:    "CREATE VIEW v AS SELECT * FROM remote('127.0.0.1', currentDatabase(), t, {user}, {password})",
			extraArgs: []string{"--exclude", "v"},
			probe:     "SELECT toString(count()) FROM v",
			wantCount: "3",
		},
		{
			name:      "a Merge table",
			reader:    "CREATE TABLE m (id UInt64) ENGINE = Merge(currentDatabase(), '^t$')",
			probe:     "SELECT toString(count()) FROM m",
			wantCount: "3",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url, conn := rebuildScratch(c)
			rebuildExec(c, conn, rebuildCreate, rebuildFixtureRows)
			rebuildExec(c, conn, rebuildInDatabase(c, conn, []string{test.reader})...)

			applied := rebuildApply(c, url, rebuildEvents, append([]string{"--allow-table-rebuild"}, test.extraArgs...)...)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "ReplacingMergeTree")
			c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, rebuildFixtureRowsRead)
			c.Assert(rebuildCount(c, conn, test.probe), qt.Equals, test.wantCount)
			rebuildSynced(c, url, rebuildEvents, test.extraArgs...)
		})
	}
}

// rebuildInDatabase writes the scratch database's name where a statement says
// {db}, for a definition that takes the name as a string, and the user and the
// password the tests connect with, as string literals, where it says {user} and
// {password}: a dictionary or a remote() query connects again on its own.
func rebuildInDatabase(c *qt.C, conn *dbschema.DatabaseConnection, statements []string) []string {
	c.Helper()
	database := rebuildCount(c, conn, "SELECT currentDatabase()")
	target, err := neturl.Parse(dbtarget.URL(c, dbtarget.ClickHouse))
	c.Assert(err, qt.IsNil)
	password, _ := target.User.Password()
	fill := strings.NewReplacer("{db}", database,
		"{user}", chcolumn.StringLiteral(target.User.Username()), "{password}", chcolumn.StringLiteral(password))
	written := make([]string, len(statements))
	for i, statement := range statements {
		written[i] = fill.Replace(statement)
	}
	return written
}

// ClickHouse refuses a block of more than max_partitions_per_insert_block
// partitions, 100 by default, so the copy lifts the limit.
func TestClickHouseTableRebuild_CopiesManyPartitions(t *testing.T) {
	c := qt.New(t)
	url, conn := rebuildScratch(c)
	rebuildExec(c, conn, rebuildCreate,
		"INSERT INTO t SELECT number, toString(number), 1, toDate('2020-01-01') + number FROM numbers(300)")
	declaration := rebuildDeclaration("        engine: MergeTree\n        order_by: id\n        partition_by: d\n")

	applied := rebuildApply(c, url, declaration, "--allow-table-rebuild")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(rebuildCount(c, conn, "SELECT toString(count()) FROM t"), qt.Equals, "300")
	c.Assert(rebuildCount(c, conn,
		"SELECT toString(uniqExact(partition)) FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active",
	), qt.Equals, "300")
	rebuildSynced(c, url, declaration)
}

// A ReplacingMergeTree or CollapsingMergeTree table merges the rows of a block
// as it writes them unless told otherwise, so a copy that let it would hold
// fewer rows than the table, and the count before the swap would stop the
// plan. The copy writes the rows as they are, and a later merge is the
// engine's business.
func TestClickHouseTableRebuild_CopiesRowsAnEngineWouldMerge(t *testing.T) {
	c := qt.New(t)
	url, conn := rebuildScratch(c)
	rebuildExec(c, conn,
		"CREATE TABLE t (id UInt64, name String, ver UInt32, d Date) ENGINE = ReplacingMergeTree(ver) ORDER BY id",
		"INSERT INTO t VALUES (1, 'a', 1, '2024-01-05')",
		"INSERT INTO t VALUES (1, 'b', 2, '2024-01-05')",
	)
	declaration := rebuildDeclaration("        engine: ReplacingMergeTree(ver)\n        order_by: id\n        partition_by: toYYYYMM(d)\n")

	applied := rebuildApply(c, url, declaration, "--allow-table-rebuild")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(rebuildDefinition(c, conn)[1], qt.Equals, "toYYYYMM(d)")
	c.Assert(rebuildCount(c, conn, "SELECT toString(count()) FROM t"), qt.Equals, "2")
	rebuildSynced(c, url, declaration)
}

// A lightweight DELETE hides a row the table still stores. The rows guard
// counts the stored rows with the deleted ones, as system.tables does, so it
// lets the rebuild run, and the copy reads only the rows that are not deleted.
func TestClickHouseTableRebuild_LeavesDeletedRowsOut(t *testing.T) {
	c := qt.New(t)
	url, conn := rebuildScratch(c)
	rebuildExec(c, conn, rebuildCreate, rebuildFixtureRows, "DELETE FROM t WHERE id = 3")

	applied := rebuildApply(c, url, rebuildEvents, "--allow-table-rebuild")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "ReplacingMergeTree")
	c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, [][2]string{{"1", "a"}, {"2", "b"}})
	c.Assert(rebuildCount(c, conn, "SELECT toString(count()) FROM t SETTINGS apply_deleted_mask = 0"), qt.Equals, "2")
	rebuildSynced(c, url, rebuildEvents)
}

// rebuildStorageColumns declare the columns of rebuildStorageCreate, with and
// without their comments.
const (
	rebuildStorageColumns = "    columns:\n      id: {type: UInt64, not_null: true}\n      name: {type: String, not_null: true, comment: the name}\n" +
		"      ver: {type: UInt32, not_null: true}\n      d: {type: Date, not_null: true}\n"
	rebuildStorageCreate = "CREATE TABLE t (id UInt64, name String COMMENT 'the name' CODEC(ZSTD(3)), ver UInt32, d Date, " +
		"PROJECTION p_by_name (SELECT * ORDER BY name)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1024 COMMENT 'tbl comment'"
	rebuildStorageProperties = "        engine: ReplacingMergeTree(ver)\n        order_by: id\n" +
		"        settings: index_granularity = 1024, deduplicate_merge_projection_mode = 'rebuild'\n"
)

// No declaration states a column codec or a projection, so the new table takes
// both from the table, and the plan names them. A comment is declared, so it
// follows the declaration, as it does on the ALTER TABLE path, and the plan
// names what changes.
func TestClickHouseTableRebuild_KeepsWhatNoDeclarationStates(t *testing.T) {
	tests := []struct {
		name             string
		declaration      string
		wantColumn       string
		wantTableComment string
		wantPlan         string
	}{
		{
			name: "the declaration states the comments",
			declaration: "tables:\n  t:\n    comment: tbl comment\n    platform:\n      clickhouse:\n" + rebuildStorageProperties +
				rebuildStorageColumns,
			wantColumn:       "CODEC(ZSTD(3)) the name",
			wantTableComment: "tbl comment",
			wantPlan:         "-- The new table keeps what no declaration states and t has: the codecs of columns name CODEC(ZSTD(3)); the projections p_by_name",
		},
		{
			name: "the declaration leaves the comments out",
			declaration: "tables:\n  t:\n    platform:\n      clickhouse:\n" + rebuildStorageProperties +
				strings.Replace(rebuildStorageColumns, ", comment: the name", "", 1),
			wantColumn:       "CODEC(ZSTD(3)) ",
			wantTableComment: "",
			wantPlan:         "-- The new table takes what the declaration states, which changes: the table comment 'tbl comment' -> none; column name: comment 'the name' -> none",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url, conn := rebuildScratch(c)
			rebuildExec(c, conn, rebuildStorageCreate, rebuildFixtureRows)

			applied := rebuildApply(c, url, test.declaration, "--allow-table-rebuild")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, test.wantPlan)
			c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "ReplacingMergeTree")
			c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, rebuildFixtureRowsRead)
			c.Assert(rebuildCount(c, conn,
				"SELECT compression_codec || ' ' || comment FROM system.columns WHERE database = currentDatabase() AND table = 't' AND name = 'name'",
			), qt.Equals, test.wantColumn)
			c.Assert(rebuildCount(c, conn, "SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = 't'"), qt.Equals, test.wantTableComment)
			c.Assert(rebuildCount(c, conn,
				"SELECT arrayStringConcat(groupArray(name || ' ' || query), ', ') FROM system.projections WHERE database = currentDatabase() AND table = 't'",
			), qt.Equals, "p_by_name SELECT * ORDER BY name")
			rebuildSynced(c, url, test.declaration)
		})
	}
}

// A codec fits the type it was chosen for: measured on 26.10, the codec of a
// UInt64 column carried onto a String one is one that CREATE TABLE refuses with
// code 36. So the new table does not take the codec of a column whose type
// changes, and the plan says so.
func TestClickHouseTableRebuild_LeavesTheCodecOfARetypedColumn(t *testing.T) {
	c := qt.New(t)
	url, conn := rebuildScratch(c)
	rebuildExec(c, conn,
		"CREATE TABLE t (id UInt64, x UInt64 CODEC(Delta, ZSTD), ver UInt32) ENGINE = MergeTree ORDER BY id",
		"INSERT INTO t VALUES (1, 10, 1), (2, 20, 2)")
	declaration := "tables:\n  t:\n    platform:\n      clickhouse:\n        engine: ReplacingMergeTree(ver)\n        order_by: id\n" +
		"    columns:\n      id: {type: UInt64, not_null: true}\n      x: {type: String, not_null: true}\n      ver: {type: UInt32, not_null: true}\n"

	applied := rebuildApply(c, url, declaration, "--allow-table-rebuild")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stdout, qt.Contains, "column x: codec Delta(8), ZSTD(1) -> none, the type changes")
	c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "ReplacingMergeTree")
	c.Assert(rebuildCount(c, conn,
		"SELECT type || ' ' || compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 't' AND name = 'x'",
	), qt.Equals, "String ")
	c.Assert(rebuildRows(c, conn, "x"), qt.DeepEquals, [][2]string{{"1", "10"}, {"2", "20"}})
	rebuildSynced(c, url, declaration)
}

// A rebuild that would lose something no declaration states, or could not
// prove that the copy holds every row, is refused, and the table is untouched.
func TestClickHouseTableRebuild_RefusesWhatTheNewTableWouldLose(t *testing.T) {
	tests := []struct {
		name        string
		create      string
		declaration string
		wantStderr  string
	}{
		{
			name:        "a column TTL",
			create:      "CREATE TABLE t (id UInt64, name String, ver UInt32, d Date TTL d + INTERVAL 100 YEAR) ENGINE = MergeTree ORDER BY id",
			declaration: rebuildEvents,
			wantStderr: "cannot create table t again: column d has TTL, which no declaration states and Ptah does not carry into the new table, " +
				"so the new table would lose it: TTL d + toIntervalYear(100).",
		},
		{
			name:   "a projection over a column the declaration drops",
			create: "CREATE TABLE t (id UInt64, name String, ver UInt32, d Date, PROJECTION p_by_d (SELECT d, count() GROUP BY d)) ENGINE = MergeTree ORDER BY id",
			declaration: "tables:\n  t:\n    platform:\n      clickhouse:\n        engine: MergeTree\n        order_by: id\n        partition_by: id % 4\n" +
				"    columns:\n      id: {type: UInt64, not_null: true}\n      name: {type: String, not_null: true}\n      ver: {type: UInt32, not_null: true}\n",
			wantStderr: "cannot create table t again: its projection p_by_d uses column d, which the declaration drops, so the new table cannot have the projection.",
		},
		{
			// The guard reads the server's default before anything is
			// created.
			name:        "projections on an engine that merges rows, without deduplicate_merge_projection_mode",
			create:      rebuildStorageCreate,
			declaration: "tables:\n  t:\n    comment: tbl comment\n    platform:\n      clickhouse:\n        engine: ReplacingMergeTree(ver)\n        order_by: id\n        settings: index_granularity = 1024\n" + rebuildStorageColumns,
			wantStderr: "code: 395, message: cannot create t again: t has projections, and ClickHouse keeps a projection on a ReplacingMergeTree table " +
				"only when deduplicate_merge_projection_mode is drop or rebuild, which neither the declaration nor the server sets.",
		},
		{
			name:        "a TinyLog table, which reports no row count",
			create:      "CREATE TABLE t (id UInt64, name String, ver UInt32, d Date) ENGINE = TinyLog",
			declaration: rebuildDeclaration("        engine: MergeTree\n        order_by: id\n"),
			wantStderr:  "Ptah does not create table t again, because its engine TinyLog reports no row count in system.tables.",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url, conn := rebuildScratch(c)
			rebuildExec(c, conn, test.create, rebuildFixtureRows)
			before := rebuildCount(c, conn, "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 't'")

			applied := rebuildApply(c, url, test.declaration, "--allow-table-rebuild")

			c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stderr, qt.Contains, test.wantStderr)
			c.Assert(rebuildCount(c, conn, "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 't'"), qt.Equals, before)
			c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, rebuildFixtureRowsRead)
			c.Assert(rebuildTables(c, conn), qt.DeepEquals, []string{"t"})
		})
	}
}

// The copy's merges stop while it is filled and counted. Without the stop, a
// ReplacingMergeTree copy written in blocks of 10,000 merges 600,000 rows over
// 1,000 keys down to about 200,000 before the count, measured on 24.10 and
// 26.10, and the count refuses the plan for a reason the user cannot act on.
func TestClickHouseTableRebuild_StopsTheMergesOfTheCopy(t *testing.T) {
	c := qt.New(t)
	url, conn := rebuildScratch(c)
	rebuildExec(c, conn,
		"CREATE TABLE t (k UInt64, ver UInt32, payload String) ENGINE = MergeTree ORDER BY k",
		"INSERT INTO t SELECT number % 1000, number, 'x' FROM numbers(600000)")
	declaration := "tables:\n  t:\n    platform:\n      clickhouse:\n        engine: ReplacingMergeTree(ver)\n        order_by: k\n" +
		"    columns:\n      k: {type: UInt64, not_null: true}\n      ver: {type: UInt32, not_null: true}\n      payload: {type: String, not_null: true}\n"
	small, err := neturl.Parse(url)
	c.Assert(err, qt.IsNil)
	query := small.Query()
	query.Set("max_insert_block_size", "10000")
	query.Set("min_insert_block_size_rows", "10000")
	query.Set("min_insert_block_size_bytes", "0")
	small.RawQuery = query.Encode()

	applied := rebuildApply(c, small.String(), declaration, "--allow-table-rebuild")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "ReplacingMergeTree")
	rebuildExec(c, conn, "OPTIMIZE TABLE t FINAL")
	c.Assert(rebuildCount(c, conn, "SELECT toString(count()) FROM t"), qt.Equals, "1000")
	rebuildSynced(c, url, declaration)
}

// A declared TTL removes expired rows when parts merge, and the copy's merges
// stop, so the copy is counted with every row, expired or not. The merges of
// the new table remove the expired rows after the swap, as the declaration asks.
func TestClickHouseTableRebuild_CountsACopyWithATTL(t *testing.T) {
	c := qt.New(t)
	url, conn := rebuildScratch(c)
	rebuildExec(c, conn,
		"CREATE TABLE t (id UInt64, name String, ver UInt32, d Date) ENGINE = MergeTree ORDER BY id",
		"INSERT INTO t SELECT number, toString(number), 1, if(number % 2 = 0, today() - 10, today()) FROM numbers(1000)")
	declaration := rebuildDeclaration("        engine: MergeTree\n        order_by: id\n        partition_by: intDiv(id, 100)\n        ttl: d + toIntervalDay(1)\n")

	applied := rebuildApply(c, url, declaration, "--allow-table-rebuild")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stdout, qt.Contains, "because its TTL removes expired rows when parts merge")
	c.Assert(applied.Stdout, qt.Contains, "throwIf(copied > source, ")
	c.Assert(rebuildDefinition(c, conn)[1], qt.Equals, "intDiv(id, 100)")
	rebuildExec(c, conn, "OPTIMIZE TABLE t FINAL")
	c.Assert(rebuildCount(c, conn, "SELECT toString(count()) FROM t"), qt.Equals, "500")
	rebuildSynced(c, url, declaration)
}

// A table name ClickHouse reads an escape in is quoted in every statement of
// the rebuild, the new table's name included.
func TestClickHouseTableRebuild_QuotesTheTableName(t *testing.T) {
	for _, test := range []struct{ name, quoted string }{
		{name: `sl\x`, quoted: "`sl\\\\x`"},
		{name: "we'ird", quoted: "`we'ird`"},
	} {
		name, quoted := test.name, test.quoted
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			url, conn := rebuildScratch(c)
			rebuildExec(c, conn,
				"CREATE TABLE "+quoted+" (id UInt64, name String, ver UInt32, d Date) ENGINE = MergeTree ORDER BY id",
				"INSERT INTO "+quoted+" VALUES (1, 'a', 1, '2024-01-05'), (2, 'b', 2, '2024-02-05')")
			declaration := strings.Replace(rebuildEvents, "\n  t:\n", "\n  "+strconv.Quote(name)+":\n", 1)

			applied := rebuildApply(c, url, declaration, "--allow-table-rebuild")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(rebuildCount(c, conn,
				"SELECT engine || ' ' || toString(total_rows) FROM system.tables WHERE database = currentDatabase() AND name = "+chcolumn.StringLiteral(name),
			), qt.Equals, "ReplacingMergeTree 2")
			c.Assert(rebuildTables(c, conn), qt.DeepEquals, []string{name})
			rebuildSynced(c, url, declaration)
		})
	}
}

// An unnamed CHECK takes its name from the table, and keeps it through a
// rebuild, rather than taking the name of the copy it is created on.
func TestClickHouseTableRebuild_KeepsTheNameOfAnUnnamedCheck(t *testing.T) {
	c := qt.New(t)
	url, conn := rebuildScratch(c)
	rebuildExec(c, conn,
		"CREATE TABLE t (id UInt64, name String, ver UInt32, d Date, CONSTRAINT t_ver_check CHECK ver > 0) ENGINE = MergeTree ORDER BY id",
		rebuildFixtureRows)
	declaration := strings.Replace(rebuildEvents, "ver: {type: UInt32, not_null: true}", "ver: {type: UInt32, not_null: true, check: ver > 0}", 1)

	applied := rebuildApply(c, url, declaration, "--allow-table-rebuild")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(rebuildCount(c, conn,
		"SELECT extract(create_table_query, 'CONSTRAINT ([^ ]+) CHECK') FROM system.tables WHERE database = currentDatabase() AND name = 't'",
	), qt.Equals, "t_ver_check")
	rebuildSynced(c, url, declaration)
}

// A replicated table is not rebuilt, whatever the request says, and the plan
// says what is unsafe.
func TestClickHouseTableRebuild_RefusesAReplicatedEngine(t *testing.T) {
	c := qt.New(t)
	url, conn := rebuildScratch(c)
	rebuildExec(c, conn, rebuildCreate, rebuildFixtureRows)

	applied := rebuildApply(c, url,
		rebuildDeclaration("        engine: ReplicatedMergeTree('/clickhouse/tables/{shard}/t', '{replica}')\n        order_by: id\n"), "--allow-table-rebuild")

	c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stderr, qt.Contains, "because its engine ReplicatedMergeTree('/clickhouse/tables/{shard}/t', '{replica}') is replicated. The rebuild is not safe there.")
	c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "MergeTree")
	c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, rebuildFixtureRowsRead)
	c.Assert(rebuildTables(c, conn), qt.DeepEquals, []string{"t"})
}

// The plan marks the rebuild destructive, so a review that reads only the
// document sees it, and says what the operator accepts.
func TestClickHouseTableRebuild_PlanIsMarkedDestructive(t *testing.T) {
	c := qt.New(t)
	url, conn := rebuildScratch(c)
	rebuildExec(c, conn, rebuildCreate, rebuildFixtureRows)
	dir := c.TempDir()
	file := filepath.Join(dir, "schema.yaml")
	c.Assert(os.WriteFile(file, []byte(rebuildEvents), 0o600), qt.IsNil)

	planned := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
		"schema", "plan", "--db-url", url, "--schema-file", file, "--allow-table-rebuild", "--dry-run", "--json")

	c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
	var document struct {
		Plan struct {
			Destructive bool `json:"destructive"`
			Statements  []struct {
				SQL      string `json:"sql"`
				Severity string `json:"severity"`
			} `json:"statements"`
		} `json:"plan"`
	}
	c.Assert(json.Unmarshal([]byte(planned.Stdout), &document), qt.IsNil)
	c.Assert(document.Plan.Destructive, qt.IsTrue)
	c.Assert(document.Plan.Statements, qt.HasLen, 10)
	c.Assert(document.Plan.Statements[0].SQL, qt.Contains, "WARNING: the steps are not atomic, and rows written to the table while they run are lost")
	c.Assert(document.Plan.Statements[8].SQL, qt.Equals, "EXCHANGE TABLES `t` AND `ptah_rebuild_t`")
	c.Assert(document.Plan.Statements[9].Severity, qt.Equals, "destructive")
	c.Assert(document.Plan.Statements[9].SQL, qt.Equals, "DROP TABLE `ptah_rebuild_t` SYNC")
	c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "MergeTree")
	c.Assert(rebuildTables(c, conn), qt.DeepEquals, []string{"t"})
}

// ptah-compat reads `diff "clickhouse" { allow_recreate = true }` at the top
// level or in the env, or the variable, and refuses without any. The env block
// merges over the top-level one, and the variable asks on its own: set, it
// allows a rebuild a block set to false refuses, and set to false it withdraws
// nothing a block asked for. Atlas Community Edition has no ClickHouse driver,
// and under its strict policy each block is an ignored construct and the
// variable a capability it lacks, so that policy refuses all three.
func TestClickHouseTableRebuild_CompatSurface(t *testing.T) {
	const declaration = "CREATE TABLE `t` (`id` UInt64, `name` String, `ver` UInt32, `d` Date) ENGINE = ReplacingMergeTree ORDER BY id;\n"
	const block = "diff \"clickhouse\" {\n  allow_recreate = true\n}\n"
	const refusal = "diff \"clickhouse\" {\n  allow_recreate = false\n}\n"
	const refused = "Ptah creates the table again and keeps its rows when asked with PTAH_ALLOW_TABLE_REBUILD=1"
	tests := []struct {
		name       string
		project    string
		envBody    string
		env        []string
		wantExit   int
		wantStderr string
		wantEngine string
	}{
		{name: "the block allows it", project: block, wantExit: 0, wantEngine: "ReplacingMergeTree"},
		{name: "the env block allows it", envBody: block, wantExit: 0, wantEngine: "ReplacingMergeTree"},
		{name: "the variable allows it", env: []string{"PTAH_ALLOW_TABLE_REBUILD=1"}, wantExit: 0, wantEngine: "ReplacingMergeTree"},
		{name: "the env block allows it over a top-level refusal", project: refusal, envBody: block, wantExit: 0, wantEngine: "ReplacingMergeTree"},
		{name: "the variable allows it over a block set to false", project: refusal, envBody: refusal,
			env: []string{"PTAH_ALLOW_TABLE_REBUILD=1"}, wantExit: 0, wantEngine: "ReplacingMergeTree"},
		{name: "a variable set to false leaves the block's request", project: block,
			env: []string{"PTAH_ALLOW_TABLE_REBUILD=0"}, wantExit: 0, wantEngine: "ReplacingMergeTree"},
		{name: "a block set to false refuses", project: refusal, wantExit: 1, wantStderr: refused, wantEngine: "MergeTree"},
		{name: "the env block refuses over a top-level request", project: block, envBody: refusal, wantExit: 1,
			wantStderr: refused, wantEngine: "MergeTree"},
		{name: "no request refuses", wantExit: 1, wantStderr: refused, wantEngine: "MergeTree"},
		{name: "strict compatibility refuses the block", project: block, env: []string{"PTAH_ATLAS_STRICT_COMPAT=1"}, wantExit: 1,
			wantStderr: `strict compatibility refuses ignored atlas.hcl block "diff"`, wantEngine: "MergeTree"},
		{name: "strict compatibility refuses the env block", envBody: block, env: []string{"PTAH_ATLAS_STRICT_COMPAT=1"}, wantExit: 1,
			wantStderr: `strict compatibility refuses ignored atlas.hcl block "diff"`, wantEngine: "MergeTree"},
		{name: "strict compatibility refuses the variable", env: []string{"PTAH_ATLAS_STRICT_COMPAT=1", "PTAH_ALLOW_TABLE_REBUILD=1"}, wantExit: 1,
			wantStderr: "PTAH_ATLAS_STRICT_COMPAT does not allow PTAH_ALLOW_TABLE_REBUILD", wantEngine: "MergeTree"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url, conn := rebuildScratch(c)
			devURL, _ := rebuildScratch(c)
			rebuildExec(c, conn, rebuildCreate, rebuildFixtureRows)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(declaration), 0o600), qt.IsNil)
			project := test.project + "env \"local\" {\n  url = \"" + url + "\"\n  src = \"file://schema.sql\"\n  dev = \"" + devURL + "\"\n" +
				test.envBody + "}\n"
			c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)

			applied := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: append(environmentWithoutPtahVariables(), test.env...)},
				"schema", "apply", "--env", "local", "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, test.wantExit, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stderr, qt.Contains, test.wantStderr)
			c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, test.wantEngine)
			c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, rebuildFixtureRowsRead)
		})
	}
}
