package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer/internal/dialects/clickhouse"
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
// and 26.9 (stokaro/ptah#4020). With it, 26.9 accepts the change and fills the
// NULL rows with the default.
func TestRenderModifyColumn_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		node *ast.AlterTableNode
		want string
	}{
		{
			name: "NOT NULL with a literal default, where the key does not hold",
			caps: capability.ClickHouse2411(),
			node: modifyColumn(ast.NewColumn("n", "INTEGER").SetNotNull().SetDefault("7"), true),
			want: "ALTER TABLE asn MODIFY COLUMN n Int32 DEFAULT '7';\n",
		},
		{
			name: "NOT NULL with an expression default, where the key does not hold",
			caps: capability.ClickHouse2411(),
			node: modifyColumn(ast.NewColumn("n", "INTEGER").SetNotNull().SetDefaultExpression("toInt32(40 + 2)"), true),
			want: "ALTER TABLE asn MODIFY COLUMN n Int32 DEFAULT toInt32(40 + 2);\n",
		},
		{
			// 24.10 accepts the statement without a DEFAULT.
			name: "NOT NULL without a default, where the key holds",
			caps: capability.ClickHouse24(),
			node: modifyColumn(ast.NewColumn("n", "INTEGER").SetNotNull(), true),
			want: "ALTER TABLE asn MODIFY COLUMN n Int32;\n",
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
			// The server does not refuse this statement: MATERIALIZED is a
			// default kind to it. What the conversion does to the rows is
			// stokaro/ptah#4025.
			name: "a computed column made NOT NULL, where the key does not hold",
			caps: capability.ClickHouse2411(),
			node: modifyColumn(ast.NewColumn("n", "INTEGER").SetNotNull().SetGenerated("a + 1", "MATERIALIZED"), true),
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
// is refused where the target has no statement for it, before any statement is
// written. 26.3 and 26.9 refuse MODIFY COLUMN from Nullable(Int32) to Int32
// without a DEFAULT, and a value the type suggests, such as 0, is data nobody
// wrote (stokaro/ptah#4020).
func TestRenderModifyColumn_FailurePath(t *testing.T) {
	c := qt.New(t)

	got, err := clickhouse.NewWithCapabilities(capability.ClickHouse2411()).Render(
		modifyColumn(ast.NewColumn("n", "INTEGER").SetNotNull(), true),
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, "column asn.n cannot be made NOT NULL here: this target refuses MODIFY COLUMN to Int32"+
		" unless the statement names a DEFAULT for the NULL rows to take, and the column"+
		" has no default to name; give the column a default")
	c.Assert(got, qt.Equals, "")
}

// The refusal is the capability's, not the dialect's: the same column renders
// on a set that carries the key, and is refused on one that does not.
func TestRenderModifyColumn_RefusalFollowsTheKey(t *testing.T) {
	c := qt.New(t)
	node := modifyColumn(ast.NewColumn("n", "INTEGER").SetNotNull(), true)

	with, withErr := clickhouse.NewWithCapabilities(
		capability.ClickHouse2411().With(capability.AlterColumnSetNotNull, true),
	).Render(node)
	_, withoutErr := clickhouse.NewWithCapabilities(
		capability.ClickHouse24().With(capability.AlterColumnSetNotNull, false),
	).Render(node)

	c.Assert(withErr, qt.IsNil)
	c.Assert(with, qt.Equals, "ALTER TABLE asn MODIFY COLUMN n Int32;\n")
	c.Assert(withoutErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
}
