package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin/internal/dialects/clickhouse"
)

// modifyColumn is the ALTER TABLE a plan writes for one column change of table
// asn, with the nullability the column had before it.
func modifyColumn(column *ast.ColumnNode, previousNullable bool) *ast.AlterTableNode {
	return &ast.AlterTableNode{Name: "asn", Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{
		Column:              column,
		PreviousNullable:    previousNullable,
		HasPreviousNullable: true,
	}}}
}

// A MODIFY COLUMN states the column's default as well as its type, as CREATE
// TABLE and ADD COLUMN do.
//
// Without it, a nullable column made NOT NULL is refused on 26.3 and 26.9 with
// `Please specify DEFAULT expression in ALTER MODIFY COLUMN statement`, even on
// an empty table, and a default the declaration changed is never set: a MODIFY
// COLUMN naming only a type keeps the default the column had, measured on 24.10
// and 26.9 (stokaro/ptah#4020).
//
// A nullable column made NOT NULL has its NULL rows filled from the default
// first, on every line: 24.10 and 25.8 accept the MODIFY COLUMN and then fail
// on a NULL row, which leaves the table unreadable (stokaro/ptah#4025).
func TestRenderModifyColumn_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		node *ast.AlterTableNode
		want string
	}{
		{
			name: "NOT NULL with a literal default, on 24.11 and above",
			caps: capability.ClickHouse2411(),
			node: modifyColumn(ast.NewColumn("n", "INTEGER").SetNotNull().SetDefault("7"), true),
			want: "ALTER TABLE asn UPDATE n = '7' WHERE n IS NULL SETTINGS mutations_sync = 2;\n" +
				"ALTER TABLE asn MODIFY COLUMN n Int32 DEFAULT '7';\n",
		},
		{
			name: "NOT NULL with an expression default, on 24.11 and above",
			caps: capability.ClickHouse2411(),
			node: modifyColumn(ast.NewColumn("n", "INTEGER").SetNotNull().SetDefaultExpression("toInt32(40 + 2)"), true),
			want: "ALTER TABLE asn UPDATE n = toInt32(40 + 2) WHERE n IS NULL SETTINGS mutations_sync = 2;\n" +
				"ALTER TABLE asn MODIFY COLUMN n Int32 DEFAULT toInt32(40 + 2);\n",
		},
		{
			name: "NOT NULL with a literal default, on 24.10",
			caps: capability.ClickHouse24(),
			node: modifyColumn(ast.NewColumn("n", "INTEGER").SetNotNull().SetDefault("7"), true),
			want: "ALTER TABLE asn UPDATE n = '7' WHERE n IS NULL SETTINGS mutations_sync = 2;\n" +
				"ALTER TABLE asn MODIFY COLUMN n Int32 DEFAULT '7';\n",
		},
		{
			name: "a default set on a column that is already NOT NULL",
			caps: capability.ClickHouse2411(),
			node: modifyColumn(ast.NewColumn("n", "INTEGER").SetNotNull().SetDefault("7"), false),
			want: "ALTER TABLE asn MODIFY COLUMN n Int32 DEFAULT '7';\n",
		},
		{
			name: "a type change of a NOT NULL column without a default",
			caps: capability.ClickHouse2411(),
			node: modifyColumn(ast.NewColumn("n", "BIGINT").SetNotNull(), false),
			want: "ALTER TABLE asn MODIFY COLUMN n Int64;\n",
		},
		{
			name: "NOT NULL dropped from a column without a default",
			caps: capability.ClickHouse2411(),
			node: modifyColumn(ast.NewColumn("n", "INTEGER"), false),
			want: "ALTER TABLE asn MODIFY COLUMN n Nullable(Int32);\n",
		},
		{
			// MATERIALIZED stands where DEFAULT would, and ClickHouse refuses a
			// column that carries both.
			name: "a computed column takes no DEFAULT",
			caps: capability.ClickHouse2411(),
			node: modifyColumn(ast.NewColumn("n", "INTEGER").SetNotNull().SetDefault("7").SetGenerated("a + 1", "MATERIALIZED"), false),
			want: "ALTER TABLE asn MODIFY COLUMN n Int32;\n",
		},
		{
			// MODIFY COLUMN fails its mutation for one on 26.3 and later and
			// leaves the column unreadable, so the column is added back as
			// declared (stokaro/ptah#4025).
			name: "a MATERIALIZED column made NOT NULL",
			caps: capability.ClickHouse2411(),
			node: modifyColumn(ast.NewColumn("n", "INTEGER").SetNotNull().SetGenerated("a + 1", "MATERIALIZED"), true),
			want: "ALTER TABLE asn DROP COLUMN n;\nALTER TABLE asn ADD COLUMN n Int32 MATERIALIZED a + 1;\n",
		},
		{
			// An ALIAS column stores nothing, and MODIFY COLUMN changes its
			// type on every line measured.
			name: "an ALIAS column made NOT NULL",
			caps: capability.ClickHouse2411(),
			node: modifyColumn(ast.NewColumn("n", "INTEGER").SetNotNull().SetGenerated("a + 1", "ALIAS"), true),
			want: "ALTER TABLE asn MODIFY COLUMN n Int32;\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := clickhouse.NewWithCapabilities(test.caps).Render(test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A nullable column made NOT NULL with no default to fill its NULL rows from
// is refused on every line, before any statement is written. 26.3 and later
// refuse MODIFY COLUMN from Nullable(Int32) to Int32 without a DEFAULT, and
// 24.10 and 25.8 accept it and then fail on a row holding NULL, which leaves
// the table unreadable (stokaro/ptah#4020, stokaro/ptah#4025). A value the type
// suggests, such as 0, is data nobody wrote.
func TestRenderModifyColumn_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
	}{
		{name: "24.11 and above", caps: capability.ClickHouse2411()},
		{name: "24.10", caps: capability.ClickHouse24()},
		{name: "a set that claims the key", caps: capability.ClickHouse2411().With(capability.AlterColumnSetNotNull, true)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := clickhouse.NewWithCapabilities(test.caps).Render(
				modifyColumn(ast.NewColumn("n", "INTEGER").SetNotNull(), true),
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, "column asn.n cannot be made NOT NULL without a default: ClickHouse 26.3"+
				" and later refuse MODIFY COLUMN to Int32 unless it names a DEFAULT, and 24.10 and 25.8 accept it"+
				" and then fail on a row holding NULL, which leaves the table unreadable; give the column a"+
				" default for its NULL rows to take")
			c.Assert(got, qt.Equals, "")
		})
	}
}
