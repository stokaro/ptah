package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin/internal/dialects/clickhouse"
)

// A modification that takes a column's default away writes REMOVE DEFAULT
// before the MODIFY COLUMN that restates the type, because a MODIFY COLUMN that
// names only a type keeps the default the column had. Measured on 24.10 and 26.9: `MODIFY COLUMN n
// Int64` on `n Int32 DEFAULT 5` leaves `DEFAULT 5`, a type the old default
// cannot take is refused until the default is gone, and both actions in one
// ALTER keep the default on 24.10 (stokaro/ptah#4030).
func TestRenderModifyColumn_RemovesADefault(t *testing.T) {
	tests := []struct {
		name string
		op   ast.ModifyColumnOperation
		want string
	}{
		{
			name: "the default is the only change",
			op: ast.ModifyColumnOperation{
				Column:          ast.NewColumn("n", "INTEGER").SetNotNull(),
				PreviousDefault: "'0'", HasPreviousDefault: true,
				Changed: ast.ColumnProperties{Default: true}, HasChanged: true,
			},
			want: "ALTER TABLE asn MODIFY COLUMN n REMOVE DEFAULT;\nALTER TABLE asn MODIFY COLUMN n Int32;\n",
		},
		{
			name: "the default goes as the type changes",
			op: ast.ModifyColumnOperation{
				Column:          ast.NewColumn("n", "BIGINT").SetNotNull(),
				PreviousDefault: "5", HasPreviousDefault: true,
				Changed: ast.ColumnProperties{Type: true, Default: true}, HasChanged: true,
			},
			want: "ALTER TABLE asn MODIFY COLUMN n REMOVE DEFAULT;\nALTER TABLE asn MODIFY COLUMN n Int64;\n",
		},
		{
			name: "the default goes as NOT NULL is dropped",
			op: ast.ModifyColumnOperation{
				Column:          ast.NewColumn("n", "INTEGER"),
				PreviousDefault: "5", HasPreviousDefault: true,
				PreviousNullable: false, HasPreviousNullable: true,
				Changed: ast.ColumnProperties{Nullability: true, Default: true}, HasChanged: true,
			},
			want: "ALTER TABLE asn MODIFY COLUMN n REMOVE DEFAULT;\nALTER TABLE asn MODIFY COLUMN n Nullable(Int32);\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			op := test.op

			got, err := clickhouse.NewWithCapabilities(capability.ClickHouse2411()).Render(
				&ast.AlterTableNode{Name: "asn", Operations: []ast.AlterOperation{&op}},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// REMOVE DEFAULT is written only where a default goes away. A replaced default
// is set by the MODIFY COLUMN's own DEFAULT clause. A column that had none
// would answer `doesn't have DEFAULT, cannot remove it`. A computed column's
// expression is MATERIALIZED or ALIAS to the server, which refuses REMOVE
// DEFAULT for one.
func TestRenderModifyColumn_LeavesAStatedDefaultAlone(t *testing.T) {
	tests := []struct {
		name string
		op   ast.ModifyColumnOperation
		want string
	}{
		{
			name: "a default replaced",
			op: ast.ModifyColumnOperation{
				Column:          ast.NewColumn("n", "INTEGER").SetNotNull().SetDefault("7"),
				PreviousDefault: "5", HasPreviousDefault: true,
				Changed: ast.ColumnProperties{Default: true}, HasChanged: true,
			},
			want: "ALTER TABLE asn MODIFY COLUMN n Int32 DEFAULT '7';\n",
		},
		{
			name: "a column that had no default",
			op: ast.ModifyColumnOperation{
				Column:             ast.NewColumn("n", "BIGINT").SetNotNull(),
				HasPreviousDefault: true,
				Changed:            ast.ColumnProperties{Type: true}, HasChanged: true,
			},
			want: "ALTER TABLE asn MODIFY COLUMN n Int64;\n",
		},
		{
			name: "a computed column",
			op: ast.ModifyColumnOperation{
				Column:          ast.NewColumn("n", "INTEGER").SetNotNull().SetGenerated("a + 1", "MATERIALIZED"),
				PreviousDefault: "5", HasPreviousDefault: true,
				Changed: ast.ColumnProperties{Default: true}, HasChanged: true,
			},
			want: "ALTER TABLE asn MODIFY COLUMN n Int32;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			op := test.op

			got, err := clickhouse.NewWithCapabilities(capability.ClickHouse2411()).Render(
				&ast.AlterTableNode{Name: "asn", Operations: []ast.AlterOperation{&op}},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A change the target refuses writes nothing, REMOVE DEFAULT included: a
// nullable column made NOT NULL whose default goes away has no DEFAULT for
// 26.3 and 26.9 to take (stokaro/ptah#4020), and a REMOVE DEFAULT written ahead
// of the refusal would run on its own.
func TestRenderModifyColumn_RefusalWritesNoRemoveDefault_FailurePath(t *testing.T) {
	c := qt.New(t)
	op := ast.ModifyColumnOperation{
		Column:          ast.NewColumn("n", "INTEGER").SetNotNull(),
		PreviousDefault: "5", HasPreviousDefault: true,
		PreviousNullable: true, HasPreviousNullable: true,
		Changed: ast.ColumnProperties{Nullability: true, Default: true}, HasChanged: true,
	}

	got, err := clickhouse.NewWithCapabilities(capability.ClickHouse2411()).Render(
		&ast.AlterTableNode{Name: "asn", Operations: []ast.AlterOperation{&op}},
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(got, qt.Equals, "")
}
