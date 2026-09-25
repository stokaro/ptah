package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/renderer/internal/dialects/postgres"
)

// renderPostgres renders nodes with a fresh PostgreSQL renderer.
func renderPostgres(c *qt.C, nodes ...ast.Node) string {
	r := postgres.New()
	r.Reset()
	for _, node := range nodes {
		c.Assert(node.Accept(r), qt.IsNil)
	}
	return r.Output()
}

// modifyColumn is an ALTER TABLE "t" carrying one column modification.
func modifyColumn(column *ast.ColumnNode, changed ast.ColumnProperties, hasChanged bool) *ast.AlterTableNode {
	return &ast.AlterTableNode{
		Name: "t",
		Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{
			Column:     column,
			Changed:    changed,
			HasChanged: hasChanged,
		}},
	}
}

// A modification that names its changed properties is rendered as those
// clauses and nothing else.
//
// Restating the column instead writes, for every row, a TYPE naming the type
// the column already has, a NULL backfill and SET NOT NULL for a column that
// is NOT NULL, and the default. Atlas CE v1.3.0 writes the one clause each
// change needs, and the rows below are what it writes for the same change,
// apart from the backfill a column becoming NOT NULL keeps
// (stokaro/ptah#3645).
func TestPostgres_ModifyColumn_RendersOnlyTheChangedProperties(t *testing.T) {
	tests := []struct {
		name    string
		column  *ast.ColumnNode
		changed ast.ColumnProperties
		want    string
	}{
		{
			name:    "a default set",
			column:  ast.NewColumn("c", "INTEGER").SetNotNull().SetDefault("7"),
			changed: ast.ColumnProperties{Default: true},
			want:    "-- ALTER statements: --\nALTER TABLE \"t\" ALTER COLUMN \"c\" SET DEFAULT 7;\n\n",
		},
		{
			name:    "a default expression set",
			column:  ast.NewColumn("c", "TIMESTAMPTZ").SetNotNull().SetDefaultExpression("now()"),
			changed: ast.ColumnProperties{Default: true},
			want:    "-- ALTER statements: --\nALTER TABLE \"t\" ALTER COLUMN \"c\" SET DEFAULT now();\n\n",
		},
		{
			name:    "a default dropped",
			column:  ast.NewColumn("c", "INTEGER").SetNotNull(),
			changed: ast.ColumnProperties{Default: true},
			want:    "-- ALTER statements: --\nALTER TABLE \"t\" ALTER COLUMN \"c\" DROP DEFAULT;\n\n",
		},
		{
			name:    "NOT NULL dropped",
			column:  ast.NewColumn("c", "INTEGER"),
			changed: ast.ColumnProperties{Nullability: true},
			want:    "-- ALTER statements: --\nALTER TABLE \"t\" ALTER COLUMN \"c\" DROP NOT NULL;\n\n",
		},
		{
			name:    "NOT NULL set, backfilled with the column's default",
			column:  ast.NewColumn("c", "INTEGER").SetNotNull().SetDefault("0"),
			changed: ast.ColumnProperties{Nullability: true},
			want: "-- ALTER statements: --\n" +
				"DO $$\nBEGIN\n" +
				"    IF EXISTS (SELECT 1 FROM \"t\" WHERE \"c\" IS NULL LIMIT 1) THEN\n" +
				"        UPDATE \"t\" SET \"c\" = '0' WHERE \"c\" IS NULL;\n" +
				"    END IF;\nEND\n$$;\n" +
				"ALTER TABLE \"t\" ALTER COLUMN \"c\" SET NOT NULL;\n\n",
		},
		{
			name:    "NOT NULL set with nothing to backfill with",
			column:  ast.NewColumn("c", "JSONB").SetNotNull(),
			changed: ast.ColumnProperties{Nullability: true},
			want:    "-- ALTER statements: --\nALTER TABLE \"t\" ALTER COLUMN \"c\" SET NOT NULL;\n\n",
		},
		{
			name:    "a type changed",
			column:  ast.NewColumn("c", "BIGINT").SetNotNull(),
			changed: ast.ColumnProperties{Type: true},
			want:    "-- ALTER statements: --\nALTER TABLE \"t\" ALTER COLUMN \"c\" TYPE BIGINT;\n\n",
		},
		{
			name:    "a type, NOT NULL and a default changed together",
			column:  ast.NewColumn("c", "BIGINT").SetNotNull().SetDefault("1"),
			changed: ast.ColumnProperties{Type: true, Nullability: true, Default: true},
			want: "-- ALTER statements: --\n" +
				"ALTER TABLE \"t\" ALTER COLUMN \"c\" TYPE BIGINT;\n" +
				"DO $$\nBEGIN\n" +
				"    IF EXISTS (SELECT 1 FROM \"t\" WHERE \"c\" IS NULL LIMIT 1) THEN\n" +
				"        UPDATE \"t\" SET \"c\" = '1' WHERE \"c\" IS NULL;\n" +
				"    END IF;\nEND\n$$;\n" +
				"ALTER TABLE \"t\" ALTER COLUMN \"c\" SET NOT NULL;\n" +
				"ALTER TABLE \"t\" ALTER COLUMN \"c\" SET DEFAULT 1;\n\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := renderPostgres(c, modifyColumn(test.column, test.changed, true))

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A modification that does not say what changed restates the column, which
// is what a parsed `ALTER TABLE ... MODIFY` carries: the whole new definition
// and no record of the old one.
func TestPostgres_ModifyColumn_UnstatedChangesRestateTheColumn(t *testing.T) {
	c := qt.New(t)
	column := ast.NewColumn("c", "INTEGER").SetNotNull().SetDefault("7")

	got := renderPostgres(c, modifyColumn(column, ast.ColumnProperties{}, false))

	c.Assert(got, qt.Equals, "-- ALTER statements: --\n"+
		"ALTER TABLE \"t\" ALTER COLUMN \"c\" TYPE INTEGER;\n"+
		"DO $$\nBEGIN\n"+
		"    IF EXISTS (SELECT 1 FROM \"t\" WHERE \"c\" IS NULL LIMIT 1) THEN\n"+
		"        UPDATE \"t\" SET \"c\" = '7' WHERE \"c\" IS NULL;\n"+
		"    END IF;\nEND\n$$;\n"+
		"ALTER TABLE \"t\" ALTER COLUMN \"c\" SET NOT NULL;\n"+
		"ALTER TABLE \"t\" ALTER COLUMN \"c\" SET DEFAULT 7;\n\n")
}
