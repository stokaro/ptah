package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin/internal/dialects/clickhouse"
)

// changeKind is the ALTER TABLE a plan writes for column m of table asn,
// which holds the generated kind previous.
func changeKind(column *ast.ColumnNode, previous string) *ast.AlterTableNode {
	return &ast.AlterTableNode{Name: "asn", Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{
		Column:                column,
		PreviousGeneratedKind: previous,
	}}}
}

// TestRenderModifyColumn_GeneratedKind_HappyPath writes the statements that
// move a column between MATERIALIZED, ALIAS, EPHEMERAL and none, as ClickHouse
// 24.10 and 26.10 take them. MODIFY COLUMN that names a kind changes the kind
// and the expression in one statement. MODIFY COLUMN that names only a type
// keeps the kind, so a column made ordinary loses its kind first: REMOVE for
// MATERIALIZED and ALIAS, and for EPHEMERAL, which has no REMOVE, a DEFAULT
// that is then removed.
func TestRenderModifyColumn_GeneratedKind_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		node *ast.AlterTableNode
		want string
	}{
		{
			name: "another MATERIALIZED expression",
			node: changeKind(ast.NewColumn("m", "BIGINT").SetNotNull().SetGenerated("a + 2", "MATERIALIZED"), "MATERIALIZED"),
			want: "ALTER TABLE asn MODIFY COLUMN m Int64 MATERIALIZED a + 2;\n",
		},
		{
			name: "MATERIALIZED to ALIAS",
			node: changeKind(ast.NewColumn("m", "BIGINT").SetNotNull().SetGenerated("a + 2", "ALIAS"), "MATERIALIZED"),
			want: "ALTER TABLE asn MODIFY COLUMN m Int64 ALIAS a + 2;\n",
		},
		{
			name: "EPHEMERAL to MATERIALIZED",
			node: changeKind(ast.NewColumn("m", "BIGINT").SetNotNull().SetGenerated("a", "MATERIALIZED"), "EPHEMERAL"),
			want: "ALTER TABLE asn MODIFY COLUMN m Int64 MATERIALIZED a;\n",
		},
		{
			name: "MATERIALIZED to ordinary",
			node: changeKind(ast.NewColumn("m", "BIGINT").SetNotNull(), "MATERIALIZED"),
			want: "ALTER TABLE asn MODIFY COLUMN m REMOVE MATERIALIZED;\nALTER TABLE asn MODIFY COLUMN m Int64;\n",
		},
		{
			name: "ALIAS to ordinary",
			node: changeKind(ast.NewColumn("m", "BIGINT").SetNotNull(), "ALIAS"),
			want: "ALTER TABLE asn MODIFY COLUMN m REMOVE ALIAS;\nALTER TABLE asn MODIFY COLUMN m Int64;\n",
		},
		{
			name: "EPHEMERAL to ordinary",
			node: changeKind(ast.NewColumn("m", "BIGINT").SetNotNull(), "EPHEMERAL"),
			want: "ALTER TABLE asn MODIFY COLUMN m Int64 DEFAULT defaultValueOfTypeName('Int64');\n" +
				"ALTER TABLE asn MODIFY COLUMN m REMOVE DEFAULT;\n" +
				"ALTER TABLE asn MODIFY COLUMN m Int64;\n",
		},
		{
			name: "ordinary to EPHEMERAL without an expression",
			node: changeKind(ast.NewColumn("m", "BIGINT").SetNotNull().SetGenerated("", "EPHEMERAL"), ""),
			want: "ALTER TABLE asn MODIFY COLUMN m Int64 EPHEMERAL defaultValueOfTypeName('Int64');\n",
		},
		{
			name: "a type change keeps the kind",
			node: changeKind(ast.NewColumn("m", "INTEGER").SetNotNull().SetGenerated("a", "ALIAS"), "ALIAS"),
			want: "ALTER TABLE asn MODIFY COLUMN m Int32 ALIAS a;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := clickhouse.New().Render(test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestRender_GeneratedKind_FailurePath refuses a generated kind ClickHouse has
// no clause for, where a CREATE TABLE and a MODIFY COLUMN would write the
// expression as a DEFAULT or drop it.
func TestRender_GeneratedKind_FailurePath(t *testing.T) {
	persisted := ast.NewColumn("m", "BIGINT").SetNotNull().SetGenerated("a + 1", "PERSISTED")
	tests := []struct {
		name string
		node ast.Node
	}{
		{name: "CREATE TABLE", node: &ast.CreateTableNode{Name: "asn", Columns: []*ast.ColumnNode{ast.NewColumn("a", "BIGINT").SetPrimary(), persisted}}},
		{name: "MODIFY COLUMN", node: changeKind(persisted, "MATERIALIZED")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := clickhouse.New().Render(test.node)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*column m is generated as PERSISTED: ClickHouse takes MATERIALIZED, ALIAS or EPHEMERAL, `+
				`and reads STORED as MATERIALIZED and VIRTUAL as ALIAS`)
			c.Assert(got, qt.Equals, "")
		})
	}
}
