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

// clickHouseComputedColumns is a table whose columns take their values from
// MATERIALIZED, ALIAS and EPHEMERAL clauses, with and without an expression,
// and from a DEFAULT that reads two EPHEMERAL columns.
const clickHouseComputedColumns = "CREATE TABLE cc (\n" +
	"  id UInt64,\n" +
	"  a Int64,\n" +
	"  b Int64,\n" +
	"  m Int64 MATERIALIZED a + b * 2,\n" +
	"  l Int64 ALIAS a * 3,\n" +
	"  e Int64 EPHEMERAL,\n" +
	"  f Int64 EPHEMERAL 5,\n" +
	"  g Int64 DEFAULT e + f,\n" +
	"  p UInt8 MATERIALIZED a = 1 OR b = 3 COMMENT 'flag',\n" +
	"  q Int64 MATERIALIZED a * 4,\n" +
	"  r Int64 ALIAS a * 5,\n" +
	"  s Int64 EPHEMERAL 9,\n" +
	"  t Int64 MATERIALIZED a - 1\n" +
	") ENGINE = MergeTree ORDER BY id;\n"

// clickHouseComputedColumnsChanged changes each expression and the comment
// on p, moves q from MATERIALIZED to ALIAS and s from EPHEMERAL to
// MATERIALIZED, and makes r and t ordinary.
const clickHouseComputedColumnsChanged = "CREATE TABLE cc (\n" +
	"  id UInt64,\n" +
	"  a Int64,\n" +
	"  b Int64,\n" +
	"  m Int64 MATERIALIZED a + b * 3,\n" +
	"  l Int64 ALIAS a * 4,\n" +
	"  e Int64 EPHEMERAL,\n" +
	"  f Int64 EPHEMERAL 6,\n" +
	"  g Int64 DEFAULT e + f,\n" +
	"  p UInt8 MATERIALIZED a = 2 OR b = 3 COMMENT 'it''s a flag',\n" +
	"  q Int64 ALIAS a * 4,\n" +
	"  r Int64,\n" +
	"  s Int64 MATERIALIZED a,\n" +
	"  t Int64\n" +
	") ENGINE = MergeTree ORDER BY id;\n"

// clickHouseComputedColumnsReshaped makes the EPHEMERAL column e ordinary,
// the ordinary column t EPHEMERAL without an expression, and removes the
// comment on p.
const clickHouseComputedColumnsReshaped = "CREATE TABLE cc (\n" +
	"  id UInt64,\n" +
	"  a Int64,\n" +
	"  b Int64,\n" +
	"  m Int64 MATERIALIZED a + b * 3,\n" +
	"  l Int64 ALIAS a * 4,\n" +
	"  e Int64,\n" +
	"  f Int64 EPHEMERAL 6,\n" +
	"  g Int64 DEFAULT f * 2,\n" +
	"  p UInt8 MATERIALIZED a = 2 OR b = 3,\n" +
	"  q Int64 ALIAS a * 4,\n" +
	"  r Int64,\n" +
	"  s Int64 MATERIALIZED a,\n" +
	"  t Int64 EPHEMERAL\n" +
	") ENGINE = MergeTree ORDER BY id;\n"

// computedColumnsRead lists the value clause and the comment of each column
// of cc that has a value clause, as system.columns reports them.
const computedColumnsRead = "SELECT arrayStringConcat(groupArray(concat(name, ' ', default_kind, ' ', " +
	"default_expression, if(comment = '', '', concat(' -- ', comment)))), '; ') FROM " +
	"(SELECT name, default_kind, default_expression, comment FROM system.columns " +
	"WHERE database = currentDatabase() AND table = 'cc' AND default_kind != '' ORDER BY position)"

// computedRowsRead lists the values every row of cc reads for the columns
// that are not EPHEMERAL in either schema.
const computedRowsRead = "SELECT arrayStringConcat(groupArray(concat(toString(id), ':', toString(m), ',', " +
	"toString(l), ',', toString(g), ',', toString(p), ',', toString(q), ',', toString(r), ',', " +
	"toString(t))), '; ') FROM (SELECT id, m, l, g, p, q, r, t FROM cc ORDER BY id)"

// clickHouseServers are the release lines CI starts: the current one through
// the registry, and 24.10, the oldest Ptah certifies, through its own variable.
func clickHouseServers(c *qt.C) []struct{ name, url string } {
	c.Helper()
	return []struct{ name, url string }{
		{name: "current", url: dbtarget.URL(c, dbtarget.ClickHouse)},
		{name: "24.10", url: legacyClickHouseURL(c)},
	}
}

// legacyClickHouseURL is the address of the 24.10 server CI starts, read from
// its own variable.
func legacyClickHouseURL(c *qt.C) string {
	c.Helper()
	legacy, configured := os.LookupEnv("PTAH_CLICKHOUSE_LEGACY_REALM_TEST_URL")
	c.Assert(configured, qt.IsTrue, qt.Commentf("PTAH_CLICKHOUSE_LEGACY_REALM_TEST_URL names the 24.10 server"))
	return legacy
}

// TestApplyClickHouseComputedColumnsE2E declares MATERIALIZED, ALIAS and
// EPHEMERAL columns in a SQL schema file and changes them, and adds, changes
// and removes a column comment. Each apply is followed by one that plans
// nothing, and the clauses and comments the catalog reports are the declared
// ones. The row inserted first reads what ClickHouse keeps: a
// MATERIALIZED column keeps the value its old expression stored, also when it
// becomes ordinary, an ALIAS column computes the new expression, an ALIAS
// column made ordinary reads 0, and an EPHEMERAL column made MATERIALIZED
// computes its expression. An EPHEMERAL column made ordinary reads 0.
func TestApplyClickHouseComputedColumnsE2E(t *testing.T) {
	c := qt.New(t)
	for _, server := range clickHouseServers(c) {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			database := createdDevDialectDatabase(c, server.url, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
			directory := c.TempDir()
			apply := func(schema string) [2]string {
				c.Helper()
				file := filepath.Join(directory, "schema.sql")
				c.Assert(os.WriteFile(file, []byte(schema), 0o600), qt.IsNil)
				args := []string{"schema", "apply", "--db-url", database, "--schema-file", file, "--auto-approve"}
				applied := runPtahIn(c, args...)
				c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
				again := runPtahIn(c, args...)
				c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
				return [2]string{applied.Stdout, again.Stdout}
			}

			created := apply(clickHouseComputedColumns)
			applyEngineFile(c, database, platform.ClickHouse, "INSERT INTO cc (id, a, b, e) VALUES (1, 1, 2, 10);\n")
			createdColumns := catalogReadBack(c, database, []string{computedColumnsRead, computedRowsRead})
			changed := apply(clickHouseComputedColumnsChanged)
			changedColumns := catalogReadBack(c, database, []string{computedColumnsRead, computedRowsRead, "SELECT toString(s) FROM cc"})
			reshaped := apply(clickHouseComputedColumnsReshaped)
			reshapedColumns := catalogReadBack(c, database, []string{computedColumnsRead, "SELECT toString(e) FROM cc"})

			synced := "Schema is synced, no changes to be made.\n"
			c.Assert(created[1], qt.Equals, synced)
			c.Assert(changed[1], qt.Equals, synced)
			c.Assert(reshaped[1], qt.Equals, synced)
			c.Assert(createdColumns, qt.DeepEquals, [][]string{
				{"m MATERIALIZED a + (b * 2); l ALIAS a * 3; e EPHEMERAL defaultValueOfTypeName('Int64'); " +
					"f EPHEMERAL 5; g DEFAULT e + f; p MATERIALIZED (a = 1) OR (b = 3) -- flag; q MATERIALIZED a * 4; " +
					"r ALIAS a * 5; s EPHEMERAL 9; t MATERIALIZED a - 1"},
				{"1:5,3,15,1,4,5,0"},
			})
			c.Assert(changedColumns, qt.DeepEquals, [][]string{
				{"m MATERIALIZED a + (b * 3); l ALIAS a * 4; e EPHEMERAL defaultValueOfTypeName('Int64'); " +
					"f EPHEMERAL 6; g DEFAULT e + f; p MATERIALIZED (a = 2) OR (b = 3) -- it's a flag; q ALIAS a * 4; s MATERIALIZED a"},
				{"1:5,4,15,1,4,0,0"},
				{"1"},
			})
			c.Assert(reshapedColumns, qt.DeepEquals, [][]string{
				{"m MATERIALIZED a + (b * 3); l ALIAS a * 4; f EPHEMERAL 6; g DEFAULT f * 2; " +
					"p MATERIALIZED (a = 2) OR (b = 3); q ALIAS a * 4; s MATERIALIZED a; " +
					"t EPHEMERAL defaultValueOfTypeName('Int64')"},
				{"0"},
			})
		})
	}
}

// clickHouseComputedEntity declares the computed kinds through Go
// annotations, with the portable VIRTUAL among them, and an EPHEMERAL column
// declared without an expression.
const clickHouseComputedEntity = "package entities\n\n" +
	"//ptah:schema:table name=\"gcc\" platform.clickhouse.engine=\"MergeTree\" platform.clickhouse.order_by=\"id\"\n" +
	"type Gcc struct {\n" +
	"\t//ptah:schema:field name=\"id\" type=\"UInt64\" primary=\"true\"\n\tID uint64\n" +
	"\t//ptah:schema:field name=\"a\" type=\"Int64\" not_null=\"true\"\n\tA int64\n" +
	"\t//ptah:schema:field name=\"m\" type=\"Int64\" not_null=\"true\" generated=\"a * 2\" generated_kind=\"MATERIALIZED\" comment=\"twice a\"\n\tM int64\n" +
	"\t//ptah:schema:field name=\"l\" type=\"Int64\" not_null=\"true\" generated=\"a + 1 * 3\" generated_kind=\"ALIAS\"\n\tL int64\n" +
	"\t//ptah:schema:field name=\"e\" type=\"Int64\" not_null=\"true\" generated=\"7\" generated_kind=\"EPHEMERAL\"\n\tE int64\n" +
	"\t//ptah:schema:field name=\"v\" type=\"Int64\" not_null=\"true\" generated=\"a + 1\" generated_kind=\"VIRTUAL\"\n\tV int64\n" +
	"\t//ptah:schema:field name=\"b\" type=\"Int64\" not_null=\"true\" generated_kind=\"EPHEMERAL\"\n\tB int64\n" +
	"}\n"

// clickHouseComputedDocument declares the computed kinds in a YAML schema,
// with the portable `stored: true` among them, and an EPHEMERAL column
// declared without an expression.
const clickHouseComputedDocument = "tables:\n" +
	"  gcc:\n" +
	"    platform:\n" +
	"      clickhouse:\n" +
	"        engine: MergeTree\n" +
	"        order_by: id\n" +
	"    columns:\n" +
	"      id: {type: UInt64, primary: true}\n" +
	"      a: {type: Int64, not_null: true}\n" +
	"      m: {type: Int64, not_null: true, generated: a * 2, generated_kind: MATERIALIZED, comment: twice a}\n" +
	"      l: {type: Int64, not_null: true, generated: a + 1 * 3, generated_kind: ALIAS}\n" +
	"      e: {type: Int64, not_null: true, generated: \"7\", generated_kind: EPHEMERAL}\n" +
	"      v: {type: Int64, not_null: true, generated: a + 1, stored: true}\n" +
	"      b: {type: Int64, not_null: true, generated_kind: EPHEMERAL}\n"

// TestApplyClickHouseComputedColumnsFromGoAndYAMLE2E declares the computed
// kinds through Go annotations and a YAML schema. Each source creates the
// columns it names, STORED as MATERIALIZED and VIRTUAL as ALIAS, and EPHEMERAL
// without an expression with the one ClickHouse reports for it, and a second
// apply plans nothing. With the bare kind dropped, column b is an ordinary
// one.
func TestApplyClickHouseComputedColumnsFromGoAndYAMLE2E(t *testing.T) {
	c := qt.New(t)
	for _, server := range clickHouseServers(c) {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			entities := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(entities, "gcc.go"), []byte(clickHouseComputedEntity), 0o600), qt.IsNil)
			document := filepath.Join(c.TempDir(), "schema.yaml")
			c.Assert(os.WriteFile(document, []byte(clickHouseComputedDocument), 0o600), qt.IsNil)
			read := "SELECT arrayStringConcat(groupArray(concat(name, ' ', default_kind, ' ', default_expression, " +
				"if(comment = '', '', concat(' -- ', comment)))), '; ') " +
				"FROM (SELECT name, default_kind, default_expression, comment FROM system.columns " +
				"WHERE database = currentDatabase() AND table = 'gcc' AND default_kind != '' ORDER BY position)"
			sources := []struct {
				name   string
				source []string
				want   string
			}{
				{
					name: "Go", source: []string{"--root-dir", entities},
					want: "m MATERIALIZED a * 2 -- twice a; l ALIAS a + (1 * 3); e EPHEMERAL 7; v ALIAS a + 1; b EPHEMERAL defaultValueOfTypeName('Int64')",
				},
				{
					name: "YAML", source: []string{"--schema-file", document},
					want: "m MATERIALIZED a * 2 -- twice a; l ALIAS a + (1 * 3); e EPHEMERAL 7; v MATERIALIZED a + 1; b EPHEMERAL defaultValueOfTypeName('Int64')",
				},
			}
			for _, source := range sources {
				t.Run(source.name, func(t *testing.T) {
					c := qt.New(t)
					database := createdDevDialectDatabase(c, server.url, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
					args := append([]string{"schema", "apply", "--db-url", database, "--auto-approve"}, source.source...)

					applied := runPtahIn(c, args...)
					again := runPtahIn(c, args...)

					c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
					c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
					c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
					c.Assert(catalogReadBack(c, database, []string{read}), qt.DeepEquals, [][]string{{source.want}})
				})
			}
		})
	}
}
