package clickhouse_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
)

// A table without ORDER BY takes its sorting key from its primary-key columns,
// written quoted, and a PRIMARY KEY the declaration states bare is read as the
// same columns: `id` and id name one column, so the key is a prefix of itself.
func TestCreateTable_ABarePrimaryKeyIsAPrefixOfTheQuotedDerivedKey(t *testing.T) {
	c := qt.New(t)
	tbl := ast.NewCreateTable("t").
		AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()).
		AddColumn(ast.NewColumn("v", "BIGINT").SetNotNull())
	tbl.SetOption("ENGINE", "MergeTree")
	tbl.SetOption("PRIMARY_KEY", "id")

	c.Assert(render(t, tbl), qt.Equals, "CREATE TABLE `t` (\n  `id` Int64,\n  `v` Int64\n) ENGINE = MergeTree ORDER BY (`id`) PRIMARY KEY (id);\n\n")
}

// A nullable column a sorting key names quoted is refused as a key column that
// must be NOT NULL, as the same column named bare is. The quoted name is read
// as one entry of the key even where it holds a comma or a parenthesis.
func TestCreateTable_AQuotedKeyColumnMustBeNotNull(t *testing.T) {
	for _, test := range []struct {
		name, column, quoted string
	}{
		{name: "a comment marker", column: "c -- d", quoted: "`c -- d`"},
		{name: "a comma", column: "a, b", quoted: "`a, b`"},
		{name: "a parenthesis", column: "a)b", quoted: "`a)b`"},
		{name: "a backtick", column: "a`b", quoted: "`a``b`"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			tbl := ast.NewCreateTable("t").
				AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()).
				AddColumn(ast.NewColumn(test.column, "BIGINT"))
			tbl.SetOption("ENGINE", "MergeTree")
			tbl.SetOption("ORDER_BY", "id, "+test.quoted)

			c.Assert(renderErr(tbl), qt.ErrorMatches, regexp.QuoteMeta(`clickhouse: rendering column "`+test.column+`" on table "t": clickhouse: column "`+test.column+
				`" is part of the sorting/primary key and must be NOT NULL (add not_null="true" or remove it from order_by/primary_key)`))
		})
	}
}

// A DROP INDEX with no table is written as a comment, and a line ending in
// the index name stays inside it: the writer escapes every text argument of a
// -- line, so the newline is written as \n. Written as it is,
// i<newline>DROP TABLE t would end the comment and leave a statement after it.
func TestDropIndex_ANameIsWrittenInsideTheSkippedComment(t *testing.T) {
	c := qt.New(t)
	drop := ast.NewDropIndex("i\nDROP TABLE t")

	c.Assert(render(t, drop), qt.Equals,
		"-- CLICKHOUSE: DROP INDEX i\\nDROP TABLE t skipped (no target table; ClickHouse requires ALTER TABLE ... DROP INDEX)\n")
}

// notRefusal is the refusal of a clause of table t that names the column
// written, which the server would store in a form it cannot read back.
func notRefusal(clause, written string) string {
	return regexp.QuoteMeta(`unsupported feature: ClickHouse: the `+clause+` of table "t" names `+written+
		`; releases before 26.11 store that name as the bare word not, the NOT operator`) + `.*`
}

// The sorting key derived from the primary-key columns quotes every name, so
// a primary-key column named not, in any case, is a column there, and the
// table is refused before anything is written.
func TestCreateTable_ADerivedKeyOverNotIsRefused(t *testing.T) {
	for _, column := range []string{"not", "NOT", "Not"} {
		t.Run(column, func(t *testing.T) {
			c := qt.New(t)
			tbl := ast.NewCreateTable("t").
				AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()).
				AddColumn(ast.NewColumn(column, "BIGINT").SetPrimary())
			tbl.SetOption("ENGINE", "MergeTree")

			err := renderErr(tbl)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, notRefusal("ORDER BY", "`"+column+"`"))
		})
	}
}

// A stated storage clause that names a column called not, alone or nested,
// is refused the same way, the arguments of the engine among them.
func TestCreateTable_AQuotedNotInAStorageClauseIsRefused(t *testing.T) {
	for _, test := range []struct {
		name, option, value, clause, written string
	}{
		{name: "engine arguments", option: "ENGINE", value: "SummingMergeTree((`not`))", clause: "ENGINE", written: "`not`"},
		{name: "a double-quoted engine argument", option: "ENGINE", value: `ReplacingMergeTree("not")`, clause: "ENGINE", written: `"not"`},
		{name: "an ORDER BY tuple", option: "ORDER_BY", value: "(id, `not`)", clause: "ORDER BY", written: "`not`"},
		{name: "a PRIMARY KEY", option: "PRIMARY_KEY", value: "`not`", clause: "PRIMARY KEY", written: "`not`"},
		{name: "a PARTITION BY call", option: "PARTITION_BY", value: "abs(`not`)", clause: "PARTITION BY", written: "`not`"},
		{name: "a SAMPLE BY", option: "SAMPLE_BY", value: "intHash32(`not`)", clause: "SAMPLE BY", written: "`not`"},
		{name: "a TTL", option: "TTL", value: "toDate('2100-01-01') + `not`", clause: "TTL", written: "`not`"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			tbl := ast.NewCreateTable("t").
				AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()).
				AddColumn(ast.NewColumn("not", "BIGINT").SetNotNull())
			tbl.SetOption("ENGINE", "MergeTree")
			tbl.SetOption(test.option, test.value)

			err := renderErr(tbl)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, notRefusal(test.clause, test.written))
		})
	}
}

// An engine whose arguments name another column of a table holding a column
// not renders: the refusal reads the names a clause writes, not the columns
// the table holds.
func TestCreateTable_AnEngineThatNamesAnotherColumnRenders(t *testing.T) {
	c := qt.New(t)
	tbl := ast.NewCreateTable("t").
		AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()).
		AddColumn(ast.NewColumn("not", "BIGINT").SetNotNull()).
		AddColumn(ast.NewColumn("ver", "BIGINT").SetNotNull())
	tbl.SetOption("ENGINE", "ReplacingMergeTree(ver)")

	c.Assert(render(t, tbl), qt.Equals, "CREATE TABLE `t` (\n  `id` Int64,\n  `not` Int64,\n  `ver` Int64\n) ENGINE = ReplacingMergeTree(ver) ORDER BY (`id`);\n\n")
}

// A skipping index, a CHECK and a MATERIALIZED column over a column not are
// refused the same way, and a column not that no expression names renders.
func TestCreateTable_AQuotedNotInAnExpressionIsRefused(t *testing.T) {
	c := qt.New(t)
	index := &ast.IndexNode{Name: "i", Table: "t", Columns: []string{"not"}}
	c.Assert(renderErr(index), qt.ErrorMatches, "unsupported feature: ClickHouse: the key of index \"i\" on table \"t\" names `not`; .*")

	checked := ast.NewCreateTable("t").AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()).AddColumn(ast.NewColumn("not", "BIGINT").SetNotNull())
	checked.SetOption("ENGINE", "MergeTree")
	checked.AddConstraint(&ast.ConstraintNode{Type: ast.CheckConstraint, Name: "c", Expression: "`not` > 0"})
	c.Assert(renderErr(checked), qt.ErrorMatches, "unsupported feature: ClickHouse: the CHECK \"c\" of table \"t\" names `not`; .*")

	materialized := ast.NewColumn("v", "BIGINT").SetNotNull()
	materialized.GeneratedExpression, materialized.GeneratedKind = "`not` * 2", "MATERIALIZED"
	computed := ast.NewCreateTable("t").AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()).
		AddColumn(ast.NewColumn("not", "BIGINT").SetNotNull()).AddColumn(materialized)
	computed.SetOption("ENGINE", "MergeTree")
	c.Assert(renderErr(computed), qt.ErrorMatches, "(?s).*unsupported feature: ClickHouse: the MATERIALIZED expression of column \"v\" names `not`; .*")

	plain := ast.NewCreateTable("t").AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()).AddColumn(ast.NewColumn("not", "BIGINT").SetNotNull())
	plain.SetOption("ENGINE", "MergeTree")
	c.Assert(render(t, plain), qt.Equals, "CREATE TABLE `t` (\n  `id` Int64,\n  `not` Int64\n) ENGINE = MergeTree ORDER BY (`id`);\n\n")
}

// A view or a materialized view whose body names not is refused before
// anything is written, as a table expression is. Measured on 24.10 and
// 26.10, a body that selects the quoted column not is stored as SELECT not
// FROM t, which the server cannot read back, and an alias spelled not that the
// body names again reads back as NOT.
func TestCreateView_AQuotedNotInTheBodyIsRefused(t *testing.T) {
	for _, test := range []struct {
		name string
		node ast.Node
		want string
	}{
		{
			name: "a view",
			node: ast.NewCreateView("v").SetBody("SELECT `not` FROM t"),
			want: "unsupported feature: ClickHouse: the body of view \"v\" names `not`; .*",
		},
		{
			name: "an alias a view names again",
			node: ast.NewCreateView("v").SetBody("SELECT id AS `NOT`, `NOT` + 1 AS y FROM t"),
			want: "unsupported feature: ClickHouse: the body of view \"v\" names `NOT`; .*",
		},
		{
			name: "a materialized view",
			node: ast.NewCreateMaterializedView("mv").SetBody(`SELECT "not" FROM t`),
			want: "unsupported feature: ClickHouse: the body of materialized view \"mv\" names \"not\"; .*",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(renderErr(test.node), qt.ErrorMatches, test.want)
		})
	}
}

// A body that names other columns, the NOT operator or a string 'not' renders
// as written.
func TestCreateView_ABodyWithoutAQuotedNotRenders(t *testing.T) {
	c := qt.New(t)
	body := "SELECT id, 'not' AS s FROM t WHERE NOT (id > 0)"

	c.Assert(render(t, ast.NewCreateView("v").SetBody(body)), qt.Equals, "CREATE VIEW `v` AS\n"+body+"\n;\n")
	c.Assert(render(t, ast.NewCreateMaterializedView("mv").SetBody(body)), qt.Equals,
		"CREATE MATERIALIZED VIEW `mv` ENGINE = MergeTree ORDER BY tuple() AS\n"+body+"\n;\n")
}
