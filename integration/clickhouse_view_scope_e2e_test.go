//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/dbtarget"
)

type clickHouseViewScope struct {
	url  string
	name string
	conn *dbschema.DatabaseConnection
}

func newClickHouseViewScope(c *qt.C) clickHouseViewScope {
	c.Helper()
	url := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.ClickHouse), "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
	conn := connectDevDialect(c, url)
	var name string
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT currentDatabase()").Scan(&name), qt.IsNil)
	return clickHouseViewScope{url: url, name: name, conn: conn}
}

func (s clickHouseViewScope) source(c *qt.C, values string) {
	c.Helper()
	rebuildExec(c, s.conn, "CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id", "INSERT INTO src VALUES "+values)
}

func (s clickHouseViewScope) views(c *qt.C, foreign string) {
	c.Helper()
	rebuildExec(c, s.conn,
		"CREATE VIEW v AS SELECT "+s.name+".id FROM "+s.name+".src AS "+s.name,
		"CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY tuple() AS SELECT id FROM "+s.name+".src",
		"CREATE VIEW foreign_v AS SELECT id FROM "+foreign+".src",
		"CREATE ROW POLICY view_rows ON v USING id > 0 TO ALL")
}

func (s clickHouseViewScope) applySQL(c *qt.C, script string) {
	c.Helper()
	rebuildExec(c, s.conn, atlasschema.SplitApplyStatements(script, s.conn.Info())...)
}

func (s clickHouseViewScope) sum(c *qt.C, relation string) uint64 {
	c.Helper()
	var value uint64
	c.Assert(s.conn.QueryRowContext(c.Context(), fmt.Sprintf("SELECT sum(id) FROM %s", relation)).Scan(&value), qt.IsNil)
	return value
}

func (s clickHouseViewScope) definition(c *qt.C) string {
	c.Helper()
	var value string
	c.Assert(s.conn.QueryRowContext(c.Context(), "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 'v'").Scan(&value), qt.IsNil)
	return value
}

func (s clickHouseViewScope) relationCount(c *qt.C, names string) uint64 {
	c.Helper()
	var value uint64
	c.Assert(s.conn.QueryRowContext(c.Context(), fmt.Sprintf("SELECT count() FROM system.tables WHERE database = currentDatabase() AND name IN (%s)", names)).Scan(&value), qt.IsNil)
	return value
}

func (s clickHouseViewScope) policyCount(c *qt.C) uint64 {
	c.Helper()
	var value uint64
	c.Assert(s.conn.QueryRowContext(c.Context(), "SELECT count() FROM system.row_policies WHERE database = currentDatabase() AND short_name = 'view_rows'").Scan(&value), qt.IsNil)
	return value
}

// The desired database owns its views, but the diff runs on the current
// database. The local sources follow that connection; the foreign source and
// the alias named like the desired database keep their meanings.
func TestClickHouseViewScope_DiffAppliesToCurrentDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	foreign, current, desired := newClickHouseViewScope(c), newClickHouseViewScope(c), newClickHouseViewScope(c)
	foreign.source(c, "(900)")
	current.source(c, "(11)")
	desired.source(c, "(22)")
	desired.views(c, foreign.name)
	rebuildExec(c, desired.conn, "INSERT INTO src VALUES (23)")
	definition := desired.definition(c)

	diff := runPtahIn(c, "schema", "diff", "--from", current.url, "--to", desired.url)

	c.Assert(diff.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", diff.Stdout, diff.Stderr))
	c.Assert(diff.Stdout, qt.Contains, "CREATE VIEW")
	current.applySQL(c, diff.Stdout)
	rebuildExec(c, current.conn, "INSERT INTO src VALUES (33)")
	c.Assert(current.sum(c, "v"), qt.Equals, uint64(44))
	c.Assert(current.sum(c, "mv"), qt.Equals, uint64(33))
	c.Assert(current.sum(c, "foreign_v"), qt.Equals, uint64(900))
	c.Assert(desired.definition(c), qt.Equals, definition)
	c.Assert(desired.sum(c, "v"), qt.Equals, uint64(45))
	c.Assert(desired.sum(c, "mv"), qt.Equals, uint64(23))
	c.Assert(desired.policyCount(c), qt.Equals, uint64(1))
	c.Assert(foreign.sum(c, "src"), qt.Equals, uint64(900))

	again := runPtahIn(c, "schema", "diff", "--from", current.url, "--to", desired.url)

	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
	c.Assert(again.Stdout, qt.Not(qt.Contains), "CREATE ")
	c.Assert(again.Stdout, qt.Not(qt.Contains), "DROP ")
	c.Assert(again.Stdout, qt.Not(qt.Contains), "ALTER ")
}

// A migration generated from one database must be usable on another with the
// same schema. Its up file drops local views and adds a view; its down file
// restores the original views, materialized view and policy in the new place.
func TestClickHouseViewScope_GeneratedMigrationsArePortableE2E(t *testing.T) {
	c := qt.New(t)
	foreign, source, destination := newClickHouseViewScope(c), newClickHouseViewScope(c), newClickHouseViewScope(c)
	foreign.source(c, "(900)")
	source.source(c, "(42)")
	source.views(c, foreign.name)
	definition := source.definition(c)
	inspected := runPtahIn(c, "schema", "inspect", "--db-url", source.url, "--format", "sql")
	c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))
	destination.applySQL(c, inspected.Stdout)
	// Policies on views are kept as rollback context, outside a schema export.
	// The destination starts with the same policy as the source.
	rebuildExec(c, destination.conn, "CREATE ROW POLICY view_rows ON v USING id > 0 TO ALL", "INSERT INTO src VALUES (77)")
	c.Assert(destination.sum(c, "v"), qt.Equals, uint64(77))
	c.Assert(destination.sum(c, "mv"), qt.Equals, uint64(77))
	c.Assert(destination.sum(c, "foreign_v"), qt.Equals, uint64(900))

	dir := c.TempDir()
	migrations := filepath.Join(dir, "migrations")
	schema := filepath.Join(dir, "desired.yaml")
	c.Assert(os.MkdirAll(migrations, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(schema, []byte("tables:\n  src:\n"+handoverColumns+"views:\n  added:\n    body: SELECT id FROM src\n"), 0o600), qt.IsNil)
	runPtahNative(c, "migrations", "generate", "--db-url", source.url, "--migrations-dir", migrations, "--schema-file", schema, "--name", "views")
	runPtahNative(c, "migrations", "up", "--db-url", destination.url, "--migrations-dir", migrations, "--allow-destructive")
	c.Assert(destination.relationCount(c, "'v', 'mv', 'foreign_v'"), qt.Equals, uint64(0))
	c.Assert(destination.sum(c, "added"), qt.Equals, uint64(77))
	c.Assert(destination.policyCount(c), qt.Equals, uint64(0))
	c.Assert(source.definition(c), qt.Equals, definition)

	runPtahNative(c, "migrations", "down", "--db-url", destination.url, "--migrations-dir", migrations, "--target", "0", "--confirm")

	c.Assert(destination.relationCount(c, "'added'"), qt.Equals, uint64(0))
	c.Assert(destination.sum(c, "v"), qt.Equals, uint64(77))
	c.Assert(destination.sum(c, "foreign_v"), qt.Equals, uint64(900))
	c.Assert(destination.policyCount(c), qt.Equals, uint64(1))
	rebuildExec(c, destination.conn, "INSERT INTO src VALUES (88)")
	c.Assert(destination.sum(c, "v"), qt.Equals, uint64(165))
	c.Assert(destination.sum(c, "mv"), qt.Equals, uint64(88))
	c.Assert(source.definition(c), qt.Equals, definition)
	c.Assert(source.sum(c, "v"), qt.Equals, uint64(42))
	c.Assert(source.policyCount(c), qt.Equals, uint64(1))
	c.Assert(foreign.sum(c, "src"), qt.Equals, uint64(900))
}

// A dotted expression can name a tuple column rather than a database, and
// ARRAY JOIN reads an array expression rather than another table. Both keep
// their meaning when the database qualifier of their source is removed.
func TestClickHouseViewScope_KeepsColumnQualifiersE2E(t *testing.T) {
	c := qt.New(t)
	source, destination := newClickHouseViewScope(c), newClickHouseViewScope(c)
	rebuildExec(c, source.conn,
		"CREATE TABLE src (id UInt64, "+source.name+" Tuple(id UInt64), src Tuple(id UInt64), items Array(UInt64), ts DateTime, label String) ENGINE = MergeTree ORDER BY id",
		"INSERT INTO src VALUES (1, (7), (10), [3, 4], '2020-02-03 00:00:00', 'xvalue')",
		"CREATE VIEW tuple_v AS SELECT "+source.name+".id AS id FROM "+source.name+".src",
		"CREATE VIEW prewhere_v AS SELECT "+source.name+".id AS id FROM "+source.name+".src PREWHERE [1, "+source.name+".id] >= [1, 7]",
		"CREATE VIEW alias_tuple_v AS SELECT a.src.id AS id FROM "+source.name+".src AS a",
		"CREATE VIEW array_v AS SELECT "+source.name+".id + item AS id FROM "+source.name+".src AS "+source.name+" ARRAY JOIN "+source.name+".items AS item",
		"CREATE VIEW extract_v AS SELECT toUInt64(EXTRACT(DAY FROM "+source.name+".ts)) AS id FROM "+source.name+".src AS "+source.name,
		"CREATE VIEW trim_v AS SELECT length(TRIM(LEADING 'x' FROM "+source.name+".label)) AS id FROM "+source.name+".src AS "+source.name)
	inspected := runPtahIn(c, "schema", "inspect", "--db-url", source.url, "--format", "sql")
	c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))

	destination.applySQL(c, inspected.Stdout)
	rebuildExec(c, destination.conn, "INSERT INTO src VALUES (2, (8), (20), [5, 6], '2020-02-04 00:00:00', 'xother')")

	c.Assert(destination.sum(c, "tuple_v"), qt.Equals, uint64(8))
	c.Assert(destination.sum(c, "prewhere_v"), qt.Equals, uint64(8))
	c.Assert(destination.sum(c, "array_v"), qt.Equals, uint64(27))
	c.Assert(destination.sum(c, "alias_tuple_v"), qt.Equals, uint64(20))
	c.Assert(destination.sum(c, "extract_v"), qt.Equals, uint64(4))
	c.Assert(destination.sum(c, "trim_v"), qt.Equals, uint64(5))
	c.Assert(source.sum(c, "tuple_v"), qt.Equals, uint64(7))
	c.Assert(source.sum(c, "prewhere_v"), qt.Equals, uint64(7))
	c.Assert(source.sum(c, "array_v"), qt.Equals, uint64(21))
	c.Assert(source.sum(c, "alias_tuple_v"), qt.Equals, uint64(10))
	c.Assert(source.sum(c, "extract_v"), qt.Equals, uint64(3))
	c.Assert(source.sum(c, "trim_v"), qt.Equals, uint64(5))
}

func TestClickHouseViewScope_RefusesAmbiguousColumnsE2E(t *testing.T) {
	for _, test := range []struct {
		name, create, body string
		want               uint64
	}{
		{name: "view", create: "CREATE VIEW unsafe AS ", body: "SELECT %[1]s.src.id AS id FROM %[1]s.src", want: 1},
		{name: "update column", create: "CREATE VIEW unsafe AS ", body: "SELECT update, %[1]s.src.id AS id FROM %[1]s.src", want: 1},
		{name: "prewhere array", create: "CREATE VIEW unsafe AS ", body: "SELECT id FROM %[1]s.src PREWHERE [1, %[1]s.src.id] = [1, 1]", want: 1},
		{name: "materialized view", create: "CREATE MATERIALIZED VIEW unsafe ENGINE = MergeTree ORDER BY tuple() AS ", body: "SELECT %[1]s.src.id AS id FROM %[1]s.src", want: 1},
		{name: "scalar alias", create: "CREATE VIEW unsafe AS ", body: "WITH CAST(tuple(tuple(7)), 'Tuple(src Tuple(id UInt8))') AS %[1]s SELECT %[1]s.src.id AS id FROM %[1]s.src", want: 7},
		{name: "same-name table alias", create: "CREATE VIEW unsafe AS ", body: "SELECT %[1]s.src.id AS id FROM %[1]s.src AS %[1]s", want: 10},
		{name: "nested alias", create: "CREATE VIEW unsafe AS ", body: "SELECT %[1]s.src.id AS id FROM %[1]s.src WHERE EXISTS (SELECT 1 FROM %[1]s.src AS %[1]s)", want: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := newClickHouseViewScope(c)
			const table = "CREATE TABLE src (id UInt64, src Tuple(id UInt64), update UInt64) ENGINE = MergeTree ORDER BY id"
			rebuildExec(c, source.conn, table, test.create+fmt.Sprintf(test.body, source.name), "INSERT INTO src VALUES (1, (10), 0)")
			c.Assert(source.sum(c, "unsafe"), qt.Equals, test.want)
			dir := c.TempDir()
			migrations := filepath.Join(dir, "migrations")
			schema := filepath.Join(dir, "desired.sql")
			c.Assert(os.MkdirAll(migrations, 0o755), qt.IsNil)
			c.Assert(os.WriteFile(schema, []byte(table+";\n"), 0o600), qt.IsNil)

			inspected := runPtahIn(c, "schema", "inspect", "--db-url", source.url, "--format", "sql")
			generated := runPtahIn(c, "migrations", "generate", "--db-url", source.url, "--migrations-dir", migrations, "--schema-file", schema, "--name", "ambiguous")

			c.Assert(inspected.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(inspected.Stderr, qt.Contains, `view "unsafe"`)
			c.Assert(inspected.Stderr, qt.Contains, "use an explicit table alias different from the database name")
			c.Assert(inspected.Stdout, qt.Not(qt.Contains), "CREATE VIEW")
			c.Assert(generated.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(generated.Stderr, qt.Contains, `view "unsafe"`)
			c.Assert(generated.Stderr, qt.Contains, "use an explicit table alias different from the database name")
			files, err := os.ReadDir(migrations)
			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, 0)
			c.Assert(source.sum(c, "unsafe"), qt.Equals, test.want)
		})
	}
}

// A quoted keyword is a column, not the literal the unquoted keyword names.
// The comparison must plan the change even when the source becomes local.
func TestClickHouseViewScope_KeepsQuotedKeywordMeaningE2E(t *testing.T) {
	c := qt.New(t)
	source := newClickHouseViewScope(c)
	const table = "CREATE TABLE src (id UInt64, `NULL` UInt64) ENGINE = MergeTree ORDER BY id"
	rebuildExec(c, source.conn, table, "INSERT INTO src VALUES (1, 7)",
		"CREATE VIEW v AS SELECT `NULL` AS id FROM "+source.name+".src")
	c.Assert(source.sum(c, "v"), qt.Equals, uint64(7))
	schema := filepath.Join(c.TempDir(), "desired.sql")
	c.Assert(os.WriteFile(schema, []byte(table+";\nCREATE VIEW v AS SELECT NULL AS id FROM "+source.name+".src;\n"), 0o600), qt.IsNil)

	plan := runPtahIn(c, "schema", "apply", "--db-url", source.url, "--schema-file", schema, "--dry-run")

	c.Assert(plan.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", plan.Stderr))
	c.Assert(plan.Stdout, qt.Contains, "CREATE OR REPLACE VIEW")
	runPtahNative(c, "schema", "apply", "--db-url", source.url, "--schema-file", schema, "--auto-approve")
	var nullRows uint64
	c.Assert(source.conn.QueryRowContext(t.Context(), "SELECT countIf(isNull(id)) FROM v").Scan(&nullRows), qt.IsNil)
	c.Assert(nullRows, qt.Equals, uint64(1))
	c.Assert(source.sum(c, "src"), qt.Equals, uint64(1))
}
