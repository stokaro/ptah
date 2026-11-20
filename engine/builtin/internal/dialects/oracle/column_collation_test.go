package oracle_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
)

// collatedColumn is a character column that declares collation.
func collatedColumn(typ, collation string) *ast.ColumnNode {
	return &ast.ColumnNode{Name: "c", Type: typ, Nullable: true, Collate: collation}
}

// collationChanged is a MODIFY of column that changes its collation.
func collationChanged(column *ast.ColumnNode) *ast.AlterTableNode {
	return &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{
		Column:     column,
		Changed:    ast.ColumnProperties{Collation: true},
		HasChanged: true,
	}}}
}

// TestCreateTable_WritesAColumnCollation_HappyPath renders a declared collation
// right after the column's type. Measured on Oracle Free 23.26 with
// MAX_STRING_SIZE=EXTENDED, each statement here is accepted and
// USER_TAB_COLUMNS.COLLATION reads the name back upper-cased. A COLLATE after
// DEFAULT is read as the COLLATE operator of the default expression and the
// column keeps USING_NLS_COMP without an error, so the position is part of what
// the test pins.
func TestCreateTable_WritesAColumnCollation_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		column *ast.ColumnNode
		want   string
	}{
		{
			name:   "VARCHAR2",
			column: collatedColumn("VARCHAR(20)", "BINARY_CI"),
			want:   "CREATE TABLE t (\n  c VARCHAR2(20) COLLATE BINARY_CI\n);\n",
		},
		{
			name:   "NVARCHAR2",
			column: collatedColumn("NVARCHAR2(20)", "BINARY_AI"),
			want:   "CREATE TABLE t (\n  c NVARCHAR2(20) COLLATE BINARY_AI\n);\n",
		},
		{
			name:   "a name in lower case is written as declared",
			column: collatedColumn("VARCHAR(20)", "binary_ci"),
			want:   "CREATE TABLE t (\n  c VARCHAR2(20) COLLATE binary_ci\n);\n",
		},
		{
			name:   "an NLS sort collation",
			column: collatedColumn("CHAR(3)", "GERMAN_CI"),
			want:   "CREATE TABLE t (\n  c CHAR(3) COLLATE GERMAN_CI\n);\n",
		},
		{
			name: "before the default and NOT NULL",
			column: &ast.ColumnNode{
				Name: "c", Type: "VARCHAR(20)", Collate: "BINARY_CI",
				Default: &ast.DefaultValue{Value: "x"},
			},
			want: "CREATE TABLE t (\n  c VARCHAR2(20) COLLATE BINARY_CI DEFAULT 'x' NOT NULL\n);\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			sql := render(c, capability.ForDialect(platform.Oracle), &ast.CreateTableNode{
				Name: "t", Columns: []*ast.ColumnNode{tc.column},
			})
			c.Assert(sql, qt.Equals, tc.want)
		})
	}
}

// TestAddColumn_WritesAColumnCollation_HappyPath is the same clause in ALTER
// TABLE ... ADD.
func TestAddColumn_WritesAColumnCollation_HappyPath(t *testing.T) {
	c := qt.New(t)
	sql := render(c, capability.ForDialect(platform.Oracle), &ast.AlterTableNode{
		Name:       "t",
		Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: collatedColumn("VARCHAR(20)", "BINARY_CI")}},
	})
	c.Assert(sql, qt.Equals, "ALTER TABLE t ADD (c VARCHAR2(20) COLLATE BINARY_CI);\n")
}

// TestModifyColumn_WritesAChangedCollation_HappyPath renders a MODIFY that
// changes the collation. The column keeps its collation through any other
// MODIFY, so the clause is written only when the collation is what changes. A
// column that no longer declares one goes back to USING_NLS_COMP, which is
// what a character column that names none reads.
func TestModifyColumn_WritesAChangedCollation_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		node *ast.AlterTableNode
		want string
	}{
		{
			name: "to a named collation",
			node: collationChanged(collatedColumn("VARCHAR(20)", "BINARY_CI")),
			want: "ALTER TABLE t MODIFY (c VARCHAR2(20) COLLATE BINARY_CI);\n",
		},
		{
			name: "back to the collation of a column that names none",
			node: collationChanged(collatedColumn("VARCHAR(20)", "")),
			want: "ALTER TABLE t MODIFY (c VARCHAR2(20) COLLATE USING_NLS_COMP);\n",
		},
		{
			name: "a change of another property leaves the collation out",
			node: &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{
				Column:     collatedColumn("VARCHAR(40)", "BINARY_CI"),
				Changed:    ast.ColumnProperties{Type: true},
				HasChanged: true,
			}}},
			want: "ALTER TABLE t MODIFY (c VARCHAR2(40));\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(render(c, capability.ForDialect(platform.Oracle), tc.node), qt.Equals, tc.want)
		})
	}
}

// TestColumnCollation_FailurePath refuses a declaration before any statement
// can reach the server.
//
// A STANDARD database answers ORA-43929 to every COLLATE, and Oracle commits
// each DDL statement, so the refusal names the setting where the server's
// would arrive after the plan's earlier statements had applied. The other
// rows are ORA-43911, ORA-43912 and ORA-12746 turned into refusals Ptah can
// make from the declaration alone.
func TestColumnCollation_FailurePath(t *testing.T) {
	standard := capability.ForDialect(platform.Oracle).With(capability.StandardStringSize, true)
	extended := capability.ForDialect(platform.Oracle)
	create := func(column *ast.ColumnNode) ast.Node {
		return &ast.CreateTableNode{Name: "t", Columns: []*ast.ColumnNode{column}}
	}
	add := func(column *ast.ColumnNode) ast.Node {
		return &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: column}}}
	}
	tests := []struct {
		name string
		caps capability.Capabilities
		node ast.Node
		want string
	}{
		{
			name: "CREATE TABLE on a STANDARD database",
			caps: standard,
			node: create(collatedColumn("VARCHAR(20)", "BINARY_CI")),
			want: `.*column "c" declares collation BINARY_CI, and this database runs with MAX_STRING_SIZE=STANDARD.*ORA-43929.*EXTENDED.*`,
		},
		{
			name: "ADD COLUMN on a STANDARD database",
			caps: standard,
			node: add(collatedColumn("VARCHAR(20)", "BINARY_CI")),
			want: `.*MAX_STRING_SIZE=STANDARD.*ORA-43929.*`,
		},
		{
			name: "MODIFY on a STANDARD database",
			caps: standard,
			node: collationChanged(collatedColumn("VARCHAR(20)", "BINARY_CI")),
			want: `.*MAX_STRING_SIZE=STANDARD.*ORA-43929.*`,
		},
		{
			name: "a number column",
			caps: extended,
			node: create(collatedColumn("INT", "BINARY_CI")),
			want: `.*column "c" is NUMBER\(10\), and Oracle takes a collation on VARCHAR2, NVARCHAR2, CHAR and NCHAR columns only.*ORA-43911.*`,
		},
		{
			name: "a CLOB column",
			caps: extended,
			node: create(collatedColumn("CLOB", "BINARY_CI")),
			want: `.*column "c" is CLOB.*ORA-43912.*`,
		},
		{
			name: "a name that is not an identifier",
			caps: extended,
			node: create(collatedColumn("VARCHAR(20)", "BINARY_CI) DROP TABLE t; --")),
			want: `.*column "c" declares collation "BINARY_CI\) DROP TABLE t; --", which is not an Oracle collation name.*`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			err := renderErr(c, tc.caps, tc.node)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, tc.want)
		})
	}
}
