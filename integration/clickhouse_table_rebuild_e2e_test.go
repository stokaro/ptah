//go:build integration

package integration_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// ClickHouse has no ALTER TABLE that changes a table's engine, PARTITION BY or
// PRIMARY KEY, or an ORDER BY that MODIFY ORDER BY cannot make. Ptah creates
// the table again, and copies its rows through a holding table, only when the
// operator asks with --allow-table-rebuild. These tests read the table
// definition and the rows back from the server, since a plan that reads
// correctly can still be refused by it.

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

func TestClickHouseTableRebuild_HappyPath(t *testing.T) {
	tests := []struct {
		name           string
		create         string
		properties     string
		wantDefinition [4]string
	}{
		{
			name:           "the engine changes",
			create:         "CREATE TABLE t (id UInt64, name String, ver UInt32, d Date) ENGINE = MergeTree ORDER BY id",
			properties:     "        engine: ReplacingMergeTree(ver)\n        order_by: id\n",
			wantDefinition: [4]string{"ReplacingMergeTree", "", "id", "id"},
		},
		{
			name:           "PARTITION BY changes",
			create:         "CREATE TABLE t (id UInt64, name String, ver UInt32, d Date) ENGINE = MergeTree ORDER BY id",
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
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url, conn := rebuildScratch(c)
			rebuildExec(c, conn, test.create, rebuildFixtureRows)

			applied := rebuildApply(c, url, rebuildDeclaration(test.properties), "--allow-table-rebuild")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(rebuildDefinition(c, conn), qt.Equals, test.wantDefinition)
			c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, [][2]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
			c.Assert(rebuildTables(c, conn), qt.DeepEquals, []string{"t"})
		})
	}
}

// The table keeps its row policy and gets its skipping indexes with the rows
// already written, and a second apply has nothing left to do.
func TestClickHouseTableRebuild_KeepsWhatAttachesToTheTable(t *testing.T) {
	c := qt.New(t)
	url, conn := rebuildScratch(c)
	rebuildExec(c, conn,
		"CREATE TABLE t (id UInt64, name String, ver UInt32, d Date, INDEX name_idx name TYPE bloom_filter(0.01) GRANULARITY 3) ENGINE = MergeTree ORDER BY id",
		rebuildFixtureRows,
		"CREATE ROW POLICY mine ON t FOR SELECT USING id > 0 TO ALL",
	)
	c.Cleanup(func() { _, _ = conn.ExecContext(c.Context(), "DROP ROW POLICY IF EXISTS mine ON t") })
	declaration := "tables:\n  t:\n    platform:\n      clickhouse:\n        engine: ReplacingMergeTree(ver)\n        order_by: id\n" +
		rebuildFixtureColumns +
		"    indexes:\n      name_idx:\n        fields: [name]\n        platform:\n          clickhouse:\n            type: bloom_filter(0.01)\n            granularity: \"3\"\n" +
		"row_policies:\n  mine:\n    table: t\n    using: id > 0\n    to: ALL\n"

	applied := rebuildApply(c, url, declaration, "--allow-table-rebuild")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "ReplacingMergeTree")
	c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, [][2]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
	var index, policy string
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT name || ' ' || type_full || ' ' || toString(granularity) FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't'",
	).Scan(&index), qt.IsNil)
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT short_name FROM system.row_policies WHERE database = currentDatabase() AND table = 't'",
	).Scan(&policy), qt.IsNil)
	c.Assert(index, qt.Equals, "name_idx bloom_filter(0.01) 3")
	c.Assert(policy, qt.Equals, "mine")

	again := rebuildApply(c, url, declaration, "--allow-table-rebuild")

	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
	c.Assert(again.Stdout+again.Stderr, qt.Contains, "Schema is synced, no changes to be made.")
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
	var columns string
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT arrayStringConcat(groupArray(name || ' ' || type), ', ') FROM (SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't' ORDER BY position)",
	).Scan(&columns), qt.IsNil)
	c.Assert(columns, qt.Equals, "id UInt64, name String, ver UInt16, note String")
	c.Assert(rebuildRows(c, conn, "ver"), qt.DeepEquals, [][2]string{{"1", "1"}, {"2", "2"}})
}

// Without the request the plan is refused naming it, and the table is untouched.
func TestClickHouseTableRebuild_FailurePath(t *testing.T) {
	t.Run("a plan that creates the table again is refused without the request", func(t *testing.T) {
		c := qt.New(t)
		url, conn := rebuildScratch(c)
		rebuildExec(c, conn,
			"CREATE TABLE t (id UInt64, name String, ver UInt32, d Date) ENGINE = MergeTree ORDER BY id", rebuildFixtureRows)

		applied := rebuildApply(c, url, rebuildDeclaration("        engine: ReplacingMergeTree(ver)\n        order_by: id\n"))

		c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
		c.Assert(applied.Stderr, qt.Contains, "ClickHouse has no ALTER TABLE that changes a table's ENGINE; Ptah creates the table again and keeps its rows when asked with --allow-table-rebuild")
		c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "MergeTree")
		c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, [][2]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
		c.Assert(rebuildTables(c, conn), qt.DeepEquals, []string{"t"})
	})

	// The conversion is checked while the table is whole: the statement that
	// copies the rows to the holding table fails, and nothing was dropped.
	t.Run("a value the new type cannot hold stops the rebuild before the table is dropped", func(t *testing.T) {
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

		// The holding table is a copy of rows the table still holds, so the next
		// plan asks for a comparison, and copying back would double the rows.
		again := rebuildApply(c, url, declaration, "--allow-table-rebuild")

		c.Assert(again.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
		c.Assert(again.Stderr, qt.Contains, "table ptah_rebuild_t is the holding table of a rebuild of table t that stopped")
		c.Assert(again.Stderr, qt.Contains, "Table t exists.")
		c.Assert(again.Stderr, qt.Contains, "Compare SELECT count() FROM `t` with SELECT count() FROM `ptah_rebuild_t`.")
		c.Assert(rebuildRows(c, conn, "ver"), qt.DeepEquals, [][2]string{{"1", "5"}, {"2", "300"}})
		var held uint64
		c.Assert(conn.QueryRowContext(c.Context(), "SELECT count() FROM ptah_rebuild_t").Scan(&held), qt.IsNil)
		c.Assert(held, qt.Equals, uint64(0))
	})

	tests := []struct {
		name       string
		extraArgs  []string
		wantStderr string
	}{
		{name: "the plan sees it", wantStderr: "the database holds a table named ptah_rebuild_t, where the rebuild keeps the rows"},
		{name: "the plan does not see it, and the server refuses to create it", extraArgs: []string{"--exclude", "ptah_rebuild_t"}, wantStderr: "already exists"},
	}
	for _, test := range tests {
		t.Run("a holding table left by an earlier run is not overwritten: "+test.name, func(t *testing.T) {
			c := qt.New(t)
			url, conn := rebuildScratch(c)
			rebuildExec(c, conn,
				"CREATE TABLE t (id UInt64, name String, ver UInt32, d Date) ENGINE = MergeTree ORDER BY id", rebuildFixtureRows,
				"CREATE TABLE ptah_rebuild_t (id UInt64) ENGINE = MergeTree ORDER BY id",
				"INSERT INTO ptah_rebuild_t VALUES (99)")

			applied := rebuildApply(c, url, rebuildDeclaration("        engine: ReplacingMergeTree(ver)\n        order_by: id\n"),
				append([]string{"--allow-table-rebuild"}, test.extraArgs...)...)

			c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stderr, qt.Contains, test.wantStderr)
			c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "MergeTree")
			c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, [][2]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
		})
	}
}

// A rebuild that stops after its DROP TABLE leaves the rows only in the holding
// table. The CHECK below rejects the rows on the way back, which is a failure
// that cannot be known when the plan is made. The next plan, which sees the
// holding table as a table the declaration does not hold, must refuse to drop it
// and say how to get the rows back.
func TestClickHouseTableRebuild_AStoppedRebuildIsNeverDropped(t *testing.T) {
	c := qt.New(t)
	url, conn := rebuildScratch(c)
	rebuildExec(c, conn,
		"CREATE TABLE t (id UInt64, name String, ver UInt32, d Date) ENGINE = MergeTree ORDER BY id", rebuildFixtureRows)
	declaration := "tables:\n  t:\n    platform:\n      clickhouse:\n        engine: ReplacingMergeTree(ver)\n        order_by: id\n" +
		"    columns:\n      id: {type: UInt64, not_null: true}\n      name: {type: String, not_null: true}\n" +
		"      ver: {type: UInt32, not_null: true, check: \"ver < 2\"}\n      d: {type: Date, not_null: true}\n"

	stopped := rebuildApply(c, url, declaration, "--allow-table-rebuild")

	c.Assert(stopped.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", stopped.Stdout, stopped.Stderr))
	c.Assert(rebuildTables(c, conn), qt.DeepEquals, []string{"ptah_rebuild_t", "t"})
	c.Assert(rebuildRows(c, conn, "name"), qt.HasLen, 0)
	var held uint64
	var comment string
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT count() FROM ptah_rebuild_t").Scan(&held), qt.IsNil)
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = 'ptah_rebuild_t'").Scan(&comment), qt.IsNil)
	c.Assert(held, qt.Equals, uint64(3))
	c.Assert(comment, qt.Equals, "ptah rebuild holding table for t")

	tests := []struct {
		name      string
		extraArgs []string
	}{
		{name: "with the request"},
		{name: "without the request"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			again := rebuildApply(c, url, declaration, test.extraArgs...)

			c.Assert(again.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stderr, qt.Contains, "table ptah_rebuild_t is the holding table of a rebuild of table t that stopped")
			c.Assert(again.Stderr, qt.Contains, "Table t exists.")
			c.Assert(again.Stderr, qt.Contains, "Compare SELECT count() FROM `t` with SELECT count() FROM `ptah_rebuild_t`.")
			c.Assert(again.Stderr, qt.Contains, "Copying it back would double them.")
			c.Assert(again.Stderr, qt.Contains, "copy them back: INSERT INTO `t` (`id`, `name`, `ver`, `d`) SELECT `id`, `name`, `ver`, `d` FROM `ptah_rebuild_t`.")
			c.Assert(rebuildTables(c, conn), qt.DeepEquals, []string{"ptah_rebuild_t", "t"})
		})
	}
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT count() FROM ptah_rebuild_t").Scan(&held), qt.IsNil)
	c.Assert(held, qt.Equals, uint64(3))
}

// A materialized view reads every row written to its source, so the copy back
// would feed the rows through it a second time, and a SummingMergeTree target
// would count them twice. A view, a dictionary and a Merge, Buffer or
// Distributed table break while the table is gone. The first statement of the
// rebuild fails when any of them names the table, before a table is created.
func TestClickHouseTableRebuild_RefusesATableSomethingReads(t *testing.T) {
	tests := []struct {
		name       string
		dependents []string
		// extraArgs keep the dependent out of the plan, so that the plan does
		// not drop it before the rebuild is asked. A Merge table is not read.
		extraArgs []string
	}{
		{
			name:       "a materialized view",
			dependents: []string{"CREATE MATERIALIZED VIEW mv ENGINE = SummingMergeTree ORDER BY k AS SELECT 1 AS k, 1 AS n FROM t"},
			extraArgs:  []string{"--exclude", "mv"},
		},
		{
			name:       "a plain view",
			dependents: []string{"CREATE VIEW v AS SELECT id FROM t"},
			extraArgs:  []string{"--exclude", "v"},
		},
		{
			name:       "a materialized view that writes to the table",
			dependents: []string{"CREATE TABLE src (id UInt64, name String, ver UInt32, d Date) ENGINE = MergeTree ORDER BY id", "CREATE MATERIALIZED VIEW mv TO t AS SELECT id, name, ver, d FROM src"},
			extraArgs:  []string{"--exclude", "mv", "--exclude", "src"},
		},
		{
			name:       "a view that joins the table by a comma",
			dependents: []string{"CREATE VIEW v AS SELECT a.dummy FROM system.one AS a, t"},
			extraArgs:  []string{"--exclude", "v"},
		},
		{
			name:       "a view that tests membership in the table",
			dependents: []string{"CREATE VIEW v AS SELECT 1 AS x WHERE (toUInt64(1), 'a', toUInt32(1), toDate('2024-01-05')) IN t"},
			extraArgs:  []string{"--exclude", "v"},
		},
		{
			name:       "a view that reads the table through a table function",
			dependents: []string{"CREATE VIEW v AS SELECT * FROM remote('127.0.0.1', currentDatabase(), t)"},
			extraArgs:  []string{"--exclude", "v"},
		},
		{
			name:       "a Merge table",
			dependents: []string{"CREATE TABLE m (id UInt64) ENGINE = Merge(currentDatabase(), '^t$')"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url, conn := rebuildScratch(c)
			rebuildExec(c, conn,
				"CREATE TABLE t (id UInt64, name String, ver UInt32, d Date) ENGINE = MergeTree ORDER BY id", rebuildFixtureRows)
			rebuildExec(c, conn, test.dependents...)
			declaration := rebuildDeclaration("        engine: ReplacingMergeTree(ver)\n        order_by: id\n")

			applied := rebuildApply(c, url, declaration, append([]string{"--allow-table-rebuild"}, test.extraArgs...)...)

			c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stderr, qt.Contains, "cannot create t again: it is read by ")
			c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "MergeTree")
			c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, [][2]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
			c.Assert(rebuildTables(c, conn), qt.Not(qt.Contains), "ptah_rebuild_t")
		})
	}
}

// ClickHouse refuses a block of more than max_partitions_per_insert_block
// partitions, 100 by default, and the copy back runs after the table is gone,
// so it lifts the limit.
func TestClickHouseTableRebuild_CopiesBackManyPartitions(t *testing.T) {
	c := qt.New(t)
	url, conn := rebuildScratch(c)
	rebuildExec(c, conn,
		"CREATE TABLE t (id UInt64, name String, ver UInt32, d Date) ENGINE = MergeTree ORDER BY id",
		"INSERT INTO t SELECT number, toString(number), 1, toDate('2020-01-01') + number FROM numbers(300)")

	applied := rebuildApply(c, url, rebuildDeclaration("        engine: MergeTree\n        order_by: id\n        partition_by: d\n"), "--allow-table-rebuild")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	var rows, partitions uint64
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT count() FROM t").Scan(&rows), qt.IsNil)
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT uniqExact(partition) FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active").Scan(&partitions), qt.IsNil)
	c.Assert(rows, qt.Equals, uint64(300))
	c.Assert(partitions, qt.Equals, uint64(300))
}

// A replicated table is not rebuilt, whatever the request says, and the plan
// says what is unsafe.
func TestClickHouseTableRebuild_RefusesAReplicatedEngine(t *testing.T) {
	c := qt.New(t)
	url, conn := rebuildScratch(c)
	rebuildExec(c, conn,
		"CREATE TABLE t (id UInt64, name String, ver UInt32, d Date) ENGINE = MergeTree ORDER BY id", rebuildFixtureRows)

	applied := rebuildApply(c, url,
		rebuildDeclaration("        engine: ReplicatedMergeTree('/clickhouse/tables/{shard}/t', '{replica}')\n        order_by: id\n"), "--allow-table-rebuild")

	c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stderr, qt.Contains, "because its engine ReplicatedMergeTree('/clickhouse/tables/{shard}/t', '{replica}') is replicated. The rebuild is not safe there.")
	c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "MergeTree")
	c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, [][2]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
	c.Assert(rebuildTables(c, conn), qt.DeepEquals, []string{"t"})
}

// The plan marks the rebuild destructive, so a review that reads only the
// document sees it, and says what the operator accepts.
func TestClickHouseTableRebuild_PlanIsMarkedDestructive(t *testing.T) {
	c := qt.New(t)
	url, conn := rebuildScratch(c)
	rebuildExec(c, conn,
		"CREATE TABLE t (id UInt64, name String, ver UInt32, d Date) ENGINE = MergeTree ORDER BY id", rebuildFixtureRows)
	dir := c.TempDir()
	file := filepath.Join(dir, "schema.yaml")
	c.Assert(os.WriteFile(file, []byte(rebuildDeclaration("        engine: ReplacingMergeTree(ver)\n        order_by: id\n")), 0o600), qt.IsNil)

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
	c.Assert(document.Plan.Statements, qt.HasLen, 7)
	c.Assert(document.Plan.Statements[0].SQL, qt.Contains, "WARNING: the steps are not atomic, and rows written to the table while they run are lost")
	c.Assert(document.Plan.Statements[3].Severity, qt.Equals, "destructive")
	c.Assert(document.Plan.Statements[3].SQL, qt.Equals, "DROP TABLE `t` SYNC")
	c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, "MergeTree")
}

// ptah-compat reads `diff "clickhouse" { allow_recreate = true }`, or the
// variable, and refuses without either. Atlas Community Edition has no
// ClickHouse driver, and under its strict policy the block is an ignored
// construct, which that policy refuses.
func TestClickHouseTableRebuild_CompatSurface(t *testing.T) {
	const declaration = "CREATE TABLE `t` (`id` UInt64, `name` String, `ver` UInt32, `d` Date) ENGINE = ReplacingMergeTree ORDER BY id;\n"
	const create = "CREATE TABLE t (id UInt64, name String, ver UInt32, d Date) ENGINE = MergeTree ORDER BY id"
	const block = "diff \"clickhouse\" {\n  allow_recreate = true\n}\n"
	tests := []struct {
		name       string
		project    string
		env        []string
		wantExit   int
		wantStderr string
		wantEngine string
	}{
		{name: "the block allows it", project: block, wantExit: 0, wantEngine: "ReplacingMergeTree"},
		{name: "the variable allows it", env: []string{"PTAH_ALLOW_TABLE_REBUILD=1"}, wantExit: 0, wantEngine: "ReplacingMergeTree"},
		{name: "a block set to false refuses", project: "diff \"clickhouse\" {\n  allow_recreate = false\n}\n", wantExit: 1,
			wantStderr: "Ptah creates the table again and keeps its rows when asked with PTAH_ALLOW_TABLE_REBUILD=1", wantEngine: "MergeTree"},
		{name: "no request refuses", wantExit: 1,
			wantStderr: "Ptah creates the table again and keeps its rows when asked with PTAH_ALLOW_TABLE_REBUILD=1", wantEngine: "MergeTree"},
		{name: "strict compatibility refuses the block", project: block, env: []string{"PTAH_ATLAS_STRICT_COMPAT=1"}, wantExit: 1,
			wantStderr: `strict compatibility refuses ignored atlas.hcl block "diff"`, wantEngine: "MergeTree"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url, conn := rebuildScratch(c)
			devURL, _ := rebuildScratch(c)
			rebuildExec(c, conn, create, rebuildFixtureRows)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(declaration), 0o600), qt.IsNil)
			project := test.project + "env \"local\" {\n  url = \"" + url + "\"\n  src = \"file://schema.sql\"\n  dev = \"" + devURL + "\"\n}\n"
			c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)

			applied := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: append(environmentWithoutPtahVariables(), test.env...)},
				"schema", "apply", "--env", "local", "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, test.wantExit, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stderr, qt.Contains, test.wantStderr)
			c.Assert(rebuildDefinition(c, conn)[0], qt.Equals, test.wantEngine)
			c.Assert(rebuildRows(c, conn, "name"), qt.DeepEquals, [][2]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
		})
	}
}
