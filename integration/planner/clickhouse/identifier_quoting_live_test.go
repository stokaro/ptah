//go:build integration

package clickhouse_test

import (
	"context"
	"regexp"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/tableref"
)

// specialNames are the names a ClickHouse statement reads as something else
// when they are written bare: `--` starts a comment, a backslash inside
// backticks starts an escape, a backtick ends the quotes, and a keyword is
// syntax. Every row runs the same life cycle against the server.
//
// The quoted spellings are written out rather than produced by the writer
// under test, so a declaration names the table and the column its row means
// whatever the writer does.
var specialNames = []struct {
	name, table, quotedTable, column, quotedColumn string
}{
	{name: "a column holding a comment marker", table: "tq", quotedTable: "tq", column: "c -- d", quotedColumn: "`c -- d`"},
	{name: "a column holding a backslash", table: "tq", quotedTable: "tq", column: `a\tb`, quotedColumn: "`a\\\\tb`"},
	{name: "a column holding a backtick", table: "tq", quotedTable: "tq", column: "a`b", quotedColumn: "`a``b`"},
	{name: "a column named by a keyword", table: "tq", quotedTable: "tq", column: "select", quotedColumn: "`select`"},
	{name: "a column ending in a backslash", table: "tq", quotedTable: "tq", column: `ends\`, quotedColumn: "`ends\\\\`"},
	{name: "a column holding a single quote", table: "tq", quotedTable: "tq", column: "x'y", quotedColumn: "`x'y`"},
	{name: "a column holding a newline", table: "tq", quotedTable: "tq", column: "new\nline", quotedColumn: "`new\\nline`"},
	{name: "a column holding a double quote", table: "tq", quotedTable: "tq", column: `q"q`, quotedColumn: "`q\"q`"},
	{name: "a table holding a backtick", table: "t`q", quotedTable: "`t``q`", column: "n", quotedColumn: "n"},
}

// declaredTable is the SQL declaration of table with id and, when typ is not
// empty, column of type typ, each name in the spelling given.
func declaredTable(table, column, typ string) string {
	columns := "id Int32"
	if typ != "" {
		columns += ", " + column + " " + typ
	}
	return "CREATE TABLE " + table + " (" + columns + ") ENGINE = MergeTree ORDER BY id;\n"
}

// applyDeclaration plans the database toward declaration and applies the plan.
func applyDeclaration(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, declaration string) {
	c.Helper()
	plan, err := planFromFile(c, ctx, conn, "schema.sql", declaration)
	c.Assert(err, qt.IsNil)
	applyPlan(c, ctx, conn, must.Must(plan.Statements()))
}

// plannedStatements plans the database toward declaration without applying it.
func plannedStatements(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, declaration string) []string {
	c.Helper()
	plan, err := planFromFile(c, ctx, conn, "schema.sql", declaration)
	c.Assert(err, qt.IsNil)
	return must.Must(plan.Statements())
}

// columnsOf is every column of the database as the server reports it, as
// "table.column type", in table and column order.
func columnsOf(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, "SELECT concat(table, '.', name, ' ', type) FROM system.columns WHERE database = currentDatabase() ORDER BY table, position")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var column string
		c.Assert(rows.Scan(&column), qt.IsNil)
		columns = append(columns, column)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return columns
}

// readColumns is every column of the database as Ptah's reader reports it.
func readColumns(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	live, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	var columns []string
	for _, table := range live.Tables {
		for _, column := range table.Columns {
			columns = append(columns, table.Name+"."+column.Name+" "+column.DataType)
		}
	}
	return columns
}

// renamedColumn renders the rename of column on table. The plan has no column
// rename to infer from a declaration, so the statement is the renderer's.
func renamedColumn(c *qt.C, table, from, to string) string {
	c.Helper()
	statement, err := builtin.RenderSQL(platform.ClickHouse, &ast.AlterTableNode{
		Name:       tableref.Canonical("", table),
		Operations: []ast.AlterOperation{&ast.RenameColumnOperation{OldName: from, NewName: to}},
	})
	c.Assert(err, qt.IsNil)
	return statement
}

// Each name goes through its whole life on the server: the plan creates the
// table, the reader reads it back, a type change is planned and applied, a
// second plan finds nothing to do, the column is renamed, and a declaration
// without it drops it. Every step is read back from system.columns.
func TestSpecialNamesLifeCycle_HappyPath(t *testing.T) {
	for _, test := range specialNames {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			conn := setNotNullDatabase(c, ctx)
			prefix := test.table + "."

			applyDeclaration(c, ctx, conn, declaredTable(test.quotedTable, test.quotedColumn, "Nullable(Int32)"))
			c.Assert(columnsOf(c, ctx, conn), qt.DeepEquals, []string{prefix + "id Int32", prefix + test.column + " Nullable(Int32)"})
			c.Assert(readColumns(c, ctx, conn), qt.DeepEquals, []string{prefix + "id Int32", prefix + test.column + " Nullable(Int32)"})

			applyDeclaration(c, ctx, conn, declaredTable(test.quotedTable, test.quotedColumn, "Nullable(Int64)"))
			c.Assert(columnsOf(c, ctx, conn), qt.DeepEquals, []string{prefix + "id Int32", prefix + test.column + " Nullable(Int64)"})
			c.Assert(plannedStatements(c, ctx, conn, declaredTable(test.quotedTable, test.quotedColumn, "Nullable(Int64)")), qt.HasLen, 0)

			applyPlan(c, ctx, conn, []string{renamedColumn(c, test.table, test.column, test.column+"_r")})
			c.Assert(columnsOf(c, ctx, conn), qt.DeepEquals, []string{prefix + "id Int32", prefix + test.column + "_r Nullable(Int64)"})

			applyDeclaration(c, ctx, conn, declaredTable(test.quotedTable, "", ""))
			c.Assert(columnsOf(c, ctx, conn), qt.DeepEquals, []string{prefix + "id Int32"})
		})
	}
}

// Dropping the column `c -- d` leaves the column c and its rows. Written bare,
// the statement is `ALTER TABLE tq DROP COLUMN c -- d;`: the server reads the
// rest of the line as a comment and drops c, with its data, and `c -- d` stays.
func TestDropColumnBesideItsPrefix_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	conn := setNotNullDatabase(c, ctx)
	applyDeclaration(c, ctx, conn, "CREATE TABLE tq (id Int32, c Int32, `c -- d` Int32) ENGINE = MergeTree ORDER BY id;\n")
	applyPlan(c, ctx, conn, []string{"INSERT INTO tq VALUES (1, 10, 20)"})

	applyDeclaration(c, ctx, conn, "CREATE TABLE tq (id Int32, c Int32) ENGINE = MergeTree ORDER BY id;\n")

	c.Assert(columnsOf(c, ctx, conn), qt.DeepEquals, []string{"tq.id Int32", "tq.c Int32"})
	var kept int32
	c.Assert(conn.QueryRowContext(ctx, "SELECT c FROM tq WHERE id = 1").Scan(&kept), qt.IsNil)
	c.Assert(kept, qt.Equals, int32(10))
}

// A primary-key column with a special name takes the sorting key the table
// is created with, quoted, and a second plan finds the key the server
// reports equal to the declared one.
//
// The server prints a plain word such as id back bare, so the row id holds
// only while the comparison reads `id` and id as one column. The keyword row
// is `select`, which the server prints back quoted. A key holding `not` is
// left out: the server prints it back bare, and its own SHOW CREATE TABLE then
// fails to parse the table it reports.
func TestSpecialNameInTheSortingKey_HappyPath(t *testing.T) {
	for _, column := range []string{"id", "select", "c -- d", "a`b", `a\b`} {
		t.Run(column, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			conn := setNotNullDatabase(c, ctx)
			declaration := "tables:\n  tq:\n    columns:\n      " + yamlKey(column) + ": {type: Int32, nullable: false, primary: true}\n      v: {type: Int32, nullable: false}\n"
			plan, err := planFromFile(c, ctx, conn, "schema.yaml", declaration)
			c.Assert(err, qt.IsNil)
			applyPlan(c, ctx, conn, must.Must(plan.Statements()))

			c.Assert(columnsOf(c, ctx, conn), qt.DeepEquals, []string{"tq." + column + " Int32", "tq.v Int32"})
			again, err := planFromFile(c, ctx, conn, "schema.yaml", declaration)
			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(again.Statements()), qt.HasLen, 0)
		})
	}
}

// yamlKey writes name as a double-quoted YAML key.
func yamlKey(name string) string {
	quoted := []byte{'"'}
	for i := range len(name) {
		if name[i] == '"' || name[i] == '\\' {
			quoted = append(quoted, '\\')
		}
		quoted = append(quoted, name[i])
	}
	return string(append(quoted, '"'))
}

// skippingIndexKeys is every skipping index of the database as the server
// reports it, as "name expression", in name order.
func skippingIndexKeys(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, "SELECT concat(name, ' ', expr) FROM system.data_skipping_indices WHERE database = currentDatabase() ORDER BY name")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		c.Assert(rows.Scan(&key), qt.IsNil)
		keys = append(keys, key)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return keys
}

// A skipping index whose key names a column in backticks is the index the
// server reports, although the server prints each name back in its own
// spelling: `v` and lower(`s`) read back as v and lower(s). A second plan
// finds every key equal to the declared one. The key (`c -- d`) is an
// expression and keeps its quotes; the pattern leaves its parentheses open,
// since 26.10 prints them and 24.10 does not. Written without the quotes, the
// server reads the key as c and a comment.
func TestQuotedNameInASkippingIndexKey_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	conn := setNotNullDatabase(c, ctx)
	declaration := "CREATE TABLE tq (id Int32, v Int32, s String, `c -- d` Int32, " +
		"INDEX i_v `v` TYPE minmax GRANULARITY 1, INDEX i_s lower(`s`) TYPE minmax GRANULARITY 1, " +
		"INDEX i_cd (`c -- d`) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id;\n"

	applyDeclaration(c, ctx, conn, declaration)

	keys := skippingIndexKeys(c, ctx, conn)
	c.Assert(keys, qt.HasLen, 3)
	c.Assert(keys[0], qt.Matches, "i_cd \\(?`c -- d`\\)?")
	c.Assert(keys[1:], qt.DeepEquals, []string{"i_s lower(s)", "i_v v"})
	c.Assert(plannedStatements(c, ctx, conn, declaration), qt.HasLen, 0)
}

// A skipping index declared on a column by name takes the name quoted, and
// the server stores the key on that column: written bare, x-y is the
// expression x - y over two other columns, and a`b and a\b are refused. A
// second plan reads the server's quoted key as the declared column and plans
// nothing.
func TestSpecialNameInASkippingIndexColumnList_HappyPath(t *testing.T) {
	for _, test := range []struct{ column, stored string }{
		{column: "x-y", stored: "`x-y`"},
		{column: "a`b", stored: "`a\\`b`"},
		{column: `a\b`, stored: "`a\\\\b`"},
		{column: "c -- d", stored: "`c -- d`"},
	} {
		t.Run(test.column, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			conn := setNotNullDatabase(c, ctx)
			declaration := "tables:\n  tq:\n    columns:\n      id: {type: Int32, nullable: false, primary: true}\n" +
				"      x: {type: Int32, nullable: false}\n      y: {type: Int32, nullable: false}\n" +
				"      " + yamlKey(test.column) + ": {type: Int32, nullable: false}\n" +
				"    indexes:\n      i: {fields: [" + yamlKey(test.column) + "]}\n"
			plan, err := planFromFile(c, ctx, conn, "schema.yaml", declaration)
			c.Assert(err, qt.IsNil)
			applyPlan(c, ctx, conn, must.Must(plan.Statements()))

			c.Assert(skippingIndexKeys(c, ctx, conn), qt.DeepEquals, []string{"i " + test.stored})
			again, err := planFromFile(c, ctx, conn, "schema.yaml", declaration)
			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(again.Statements()), qt.HasLen, 0)
		})
	}
}

// An explicit sorting key whose names the author quoted is the key the server
// reports, which it prints back as (id, `a b`). A second plan reads the two
// spellings as one key and plans nothing: compared token by token, `id` and
// id differ, and the plan asks the server for a MODIFY ORDER BY it refuses
// with "can only drop the last expressions".
func TestQuotedExplicitSortingKey_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	conn := setNotNullDatabase(c, ctx)
	declaration := "CREATE TABLE tq (id Int32, `a b` Int32, v Int32) ENGINE = MergeTree ORDER BY (`id`, `a b`);\n"

	applyDeclaration(c, ctx, conn, declaration)

	var key string
	c.Assert(conn.QueryRowContext(ctx, "SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'tq'").Scan(&key), qt.IsNil)
	c.Assert(key, qt.Equals, "id, `a b`")
	c.Assert(plannedStatements(c, ctx, conn, declaration), qt.HasLen, 0)
}

// notRefusal is the refusal of a plan whose clause names a column called not,
// as written.
func notRefusal(clause, written string) string {
	return "(?s).*unsupported feature: ClickHouse: the " + regexp.QuoteMeta(clause) + " names " + regexp.QuoteMeta(written) +
		"; releases before 26.11 store that name as the bare word not, the NOT operator.*"
}

// A column named not, in any case, that a stored expression names is refused
// before anything is applied: releases before 26.11 store the name as the
// bare word not, and then SHOW CREATE and every read of the schema fail, so
// the plan would apply and leave a database nothing reads. The rows cover the
// sorting key derived from the primary key, an ORDER BY tuple, a call in
// PARTITION BY, a TTL, the arguments of the engine, a skipping index, a
// MATERIALIZED column, the body of a view and of a materialized view, and the
// filter of a row policy, which 24.10 and 26.10 refuse after the table is
// created.
func TestColumnNamedNotInANewTable_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, file, declaration, want string }{
		{
			name: "the derived sorting key", file: "schema.yaml",
			declaration: "tables:\n  tq:\n    columns:\n      id: {type: Int32, nullable: false, primary: true}\n      NOT: {type: Int32, nullable: false, primary: true}\n",
			want:        notRefusal(`ORDER BY of table "tq"`, "`NOT`"),
		},
		{
			name: "an ORDER BY tuple", file: "schema.sql",
			declaration: "CREATE TABLE tq (id Int32, `not` Int32) ENGINE = MergeTree ORDER BY (id, `not`);\n",
			want:        notRefusal(`ORDER BY of table "tq"`, "`not`"),
		},
		{
			name: "a PARTITION BY call", file: "schema.sql",
			declaration: "CREATE TABLE tq (id Int32, `Not` Int32) ENGINE = MergeTree PARTITION BY abs(`Not`) ORDER BY id;\n",
			want:        notRefusal(`PARTITION BY of table "tq"`, "`Not`"),
		},
		{
			name: "a TTL", file: "schema.sql",
			declaration: "CREATE TABLE tq (id Int32, `not` Int32, ts DateTime) ENGINE = MergeTree ORDER BY id TTL ts + toIntervalDay(`not`);\n",
			want:        notRefusal(`TTL of table "tq"`, "`not`"),
		},
		{
			name: "engine arguments", file: "schema.yaml",
			declaration: engineDeclaration("SummingMergeTree((`not`))"),
			want:        notRefusal(`ENGINE of table "tq"`, "`not`"),
		},
		{
			name: "a double-quoted engine argument", file: "schema.yaml",
			declaration: engineDeclaration(`ReplacingMergeTree("not")`),
			want:        notRefusal(`ENGINE of table "tq"`, `"not"`),
		},
		{
			name: "a skipping index", file: "schema.sql",
			declaration: "CREATE TABLE tq (id Int32, `not` Int32, INDEX i `not` TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id;\n",
			want:        notRefusal(`key of index "i" on table "tq"`, "`not`"),
		},
		{
			name: "a MATERIALIZED column", file: "schema.sql",
			declaration: "CREATE TABLE tq (id Int32, `not` Int32, v Int32 MATERIALIZED `not` * 2) ENGINE = MergeTree ORDER BY id;\n",
			want:        notRefusal(`MATERIALIZED expression of column "v"`, "`not`"),
		},
		{
			name: "a view", file: "schema.sql",
			declaration: "CREATE TABLE tq (id Int32, `not` Int32) ENGINE = MergeTree ORDER BY id;\nCREATE VIEW vq AS SELECT `not` FROM tq;\n",
			want:        notRefusal(`body of view "vq"`, "`not`"),
		},
		{
			name: "a materialized view", file: "schema.sql",
			declaration: "CREATE TABLE tq (id Int32, `not` Int32) ENGINE = MergeTree ORDER BY id;\n" +
				"CREATE MATERIALIZED VIEW mq ENGINE = MergeTree ORDER BY tuple() AS SELECT `NOT` FROM tq;\n",
			want: notRefusal(`body of materialized view "mq"`, "`NOT`"),
		},
		{
			name: "a row policy filter", file: "schema.sql",
			declaration: "CREATE TABLE tq (id Int32, `not` Int32) ENGINE = MergeTree ORDER BY id;\n" +
				"CREATE ROW POLICY rq ON tq FOR SELECT USING `not` > 0 AS PERMISSIVE TO ALL;\n",
			want: notRefusal(`filter of row policy "rq" on table "tq"`, "`not`"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			conn := setNotNullDatabase(c, ctx)

			_, err := planFromFile(c, ctx, conn, test.file, test.declaration)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(columnsOf(c, ctx, conn), qt.HasLen, 0)
		})
	}
}

// A change to a live table that would make a stored expression name its
// column not is refused the same way, and the table stays as it was: a new
// sorting key over an added column, a skipping index on an existing one, a
// CHECK, and a DEFAULT, which the server would store as NOT 1.
func TestColumnNamedNotInAnAlteredTable_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, before, declaration, want string }{
		{
			name:        "a new sorting key",
			before:      "CREATE TABLE tq (id Int32) ENGINE = MergeTree PRIMARY KEY id ORDER BY id;\n",
			declaration: "CREATE TABLE tq (id Int32, `not` Int32) ENGINE = MergeTree PRIMARY KEY id ORDER BY (id, `not`);\n",
			want:        notRefusal(`ORDER BY of table "tq"`, "`not`"),
		},
		{
			name:        "a skipping index",
			before:      "CREATE TABLE tq (id Int32, `not` Int32) ENGINE = MergeTree ORDER BY id;\n",
			declaration: "CREATE TABLE tq (id Int32, `not` Int32, INDEX i `not` TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id;\n",
			want:        notRefusal(`key of index "i" on table "tq"`, "`not`"),
		},
		{
			name:        "a CHECK",
			before:      "CREATE TABLE tq (id Int32, `not` Int32) ENGINE = MergeTree ORDER BY id;\n",
			declaration: "CREATE TABLE tq (id Int32, `not` Int32, CONSTRAINT c CHECK (`not` > 0)) ENGINE = MergeTree ORDER BY id;\n",
			want:        notRefusal(`CHECK "c" of table "tq"`, "`not`"),
		},
		{
			name:        "a changed DEFAULT",
			before:      "CREATE TABLE tq (id Int32, `not` Int32, v Int32 DEFAULT 1) ENGINE = MergeTree ORDER BY id;\n",
			declaration: "CREATE TABLE tq (id Int32, `not` Int32, v Int32 DEFAULT `not` + 1) ENGINE = MergeTree ORDER BY id;\n",
			want:        notRefusal(`DEFAULT expression of column "v"`, "`not`"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			conn := setNotNullDatabase(c, ctx)
			applyDeclaration(c, ctx, conn, test.before)
			columns := columnsOf(c, ctx, conn)

			_, err := planFromFile(c, ctx, conn, "schema.sql", test.declaration)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(columnsOf(c, ctx, conn), qt.DeepEquals, columns)
			c.Assert(skippingIndexKeys(c, ctx, conn), qt.HasLen, 0)
		})
	}
}

// engineDeclaration is the YAML declaration of tq, with id, a column not and
// ver, stored by engine. A SQL schema file cannot declare an engine with
// arguments yet (stokaro/ptah#6525).
func engineDeclaration(engine string) string {
	return "tables:\n  tq:\n    columns:\n      id: {type: Int64, nullable: false, primary: true}\n" +
		"      not: {type: UInt64, nullable: false}\n      ver: {type: UInt64, nullable: false}\n" +
		"    overrides:\n      clickhouse:\n        engine: '" + engine + "'\n"
}

// mergeTreeBesideNot is the live table the rebuild rows start from.
const mergeTreeBesideNot = "CREATE TABLE tq (id Int64, `not` UInt64, ver UInt64) ENGINE = MergeTree ORDER BY id"

// An engine change whose arguments name a column not is refused the same way
// when the plan may create the table again, and the table stays as it was: a
// rebuild creates the new table from the declaration, under the copy's name
// ptah_rebuild_tq, so the refusal of a new table holds for it and names the
// table that statement creates.
func TestColumnNamedNotInARebuiltEngine_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, engine, written string }{
		{name: "a version column", engine: "ReplacingMergeTree(`not`)", written: "`not`"},
		{name: "summed columns", engine: `SummingMergeTree(("not"))`, written: `"not"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			conn := setNotNullDatabase(c, ctx)
			createTable(c, ctx, conn, mergeTreeBesideNot)

			_, err := planFromFileWithPolicy(c, ctx, conn, "schema.yaml", engineDeclaration(test.engine), atlasschema.DiffPolicy{AllowTableRebuild: true})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, notRefusal(`ENGINE of table "ptah_rebuild_tq"`, test.written))
			c.Assert(relationsOf(c, ctx, conn), qt.DeepEquals, []string{"tq MergeTree"})
		})
	}
}

// An engine whose arguments name another column of a table holding a column
// not applies, on a new table and through a rebuild, and a second plan finds
// nothing to do: the refusal reads the names the engine writes, not the
// columns the table holds.
func TestEngineBesideAColumnNamedNot_HappyPath(t *testing.T) {
	declaration := engineDeclaration("ReplacingMergeTree(ver)")
	for _, test := range []struct {
		name   string
		before []string
	}{
		{name: "a new table"},
		{name: "a rebuilt engine", before: []string{mergeTreeBesideNot}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			conn := setNotNullDatabase(c, ctx)
			createTable(c, ctx, conn, test.before...)

			plan, err := planFromFileWithPolicy(c, ctx, conn, "schema.yaml", declaration, atlasschema.DiffPolicy{AllowTableRebuild: true})
			c.Assert(err, qt.IsNil)
			applyPlan(c, ctx, conn, must.Must(plan.Statements()))

			c.Assert(relationsOf(c, ctx, conn), qt.DeepEquals, []string{"tq ReplacingMergeTree"})
			again, err := planFromFile(c, ctx, conn, "schema.yaml", declaration)
			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(again.Statements()), qt.HasLen, 0)
		})
	}
}

// A view and a materialized view over a table holding a column named not, whose
// bodies do not name it, apply, and a second plan finds nothing to do: the
// refusal reads the names a body writes, not the columns its tables hold.
func TestViewOverAColumnNamedNot_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	conn := setNotNullDatabase(c, ctx)
	declaration := "CREATE TABLE tq (id Int32, `not` Int32) ENGINE = MergeTree ORDER BY id;\n" +
		"CREATE VIEW vq AS SELECT id, 'not' AS s FROM tq WHERE NOT (id < 0);\n" +
		"CREATE MATERIALIZED VIEW mq ENGINE = MergeTree ORDER BY tuple() AS SELECT id FROM tq;\n"

	applyDeclaration(c, ctx, conn, declaration)

	c.Assert(relationsOf(c, ctx, conn), qt.DeepEquals, []string{"mq MaterializedView", "tq MergeTree", "vq View"})
	c.Assert(plannedStatements(c, ctx, conn, declaration), qt.HasLen, 0)
}

// relationsOf is every table and view of the database as "name engine", in
// name order, leaving out the inner table that stores a materialized view's
// rows.
func relationsOf(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, "SELECT concat(name, ' ', engine) FROM system.tables WHERE database = currentDatabase() AND NOT startsWith(name, '.inner') ORDER BY name")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var relations []string
	for rows.Next() {
		var relation string
		c.Assert(rows.Scan(&relation), qt.IsNil)
		relations = append(relations, relation)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return relations
}

// A column named not, in any case, made NOT NULL is refused even with a
// default, and the column and its NULL row stay as they were. Its NULL rows
// would take the default through ALTER TABLE ... UPDATE, which 25.8 to 26.10
// refuse with code 62 for such a column however the name is quoted, after
// any statement before it in the plan had run.
func TestColumnNamedNotMadeNotNull_FailurePath(t *testing.T) {
	for _, name := range []string{"not", "NOT"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			conn := setNotNullDatabase(c, ctx)
			quoted := "`" + name + "`"
			applyDeclaration(c, ctx, conn, "CREATE TABLE tq (id Int32, "+quoted+" Nullable(Int32)) ENGINE = MergeTree ORDER BY id;\n")
			_, err := conn.ExecContext(ctx, "INSERT INTO tq VALUES (1, NULL), (2, 5)")
			c.Assert(err, qt.IsNil)

			_, err = planFromFile(c, ctx, conn, "schema.sql", "CREATE TABLE tq (id Int32, "+quoted+" Int32 DEFAULT 7) ENGINE = MergeTree ORDER BY id;\n")

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*column tq\.`+name+` cannot be made NOT NULL: its NULL rows take the default through `+
				`ALTER TABLE \.\.\. UPDATE, and ClickHouse 25\.8 to 26\.10 refuse that statement for a column named not, `+
				`however the name is quoted; rename the column first`)
			c.Assert(columnsOf(c, ctx, conn), qt.DeepEquals, []string{"tq.id Int32", "tq." + name + " Nullable(Int32)"})
			c.Assert(nullRows(c, ctx, conn, quoted), qt.DeepEquals, []int32{1})
		})
	}
}

// nullRows is the id of every row of tq whose column, written quoted, is NULL.
func nullRows(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, quoted string) []int32 {
	c.Helper()
	rows, err := conn.QueryContext(ctx, "SELECT id FROM tq WHERE "+quoted+" IS NULL ORDER BY id")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var ids []int32
	for rows.Next() {
		var id int32
		c.Assert(rows.Scan(&id), qt.IsNil)
		ids = append(ids, id)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return ids
}
