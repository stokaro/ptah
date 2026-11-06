package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/engine/builtin"
)

// addedColumn is a nullable column an ALTER TABLE adds.
func addedColumn() ast.AlterOperation {
	return &ast.AddColumnOperation{Column: ast.NewColumn("x", "INTEGER")}
}

// checkOverIt is a check over the column addedColumn adds. It holds only inside one statement
// when it stands ahead of that column.
func checkOverIt() ast.AlterOperation {
	return &ast.AddConstraintOperation{Constraint: &ast.ConstraintNode{Type: ast.CheckConstraint, Name: "ck", Expression: "x > 0"}}
}

// backfilled is a column change that fills the column's NULLs ahead of its NOT NULL, so it
// cannot be a clause of one statement.
func backfilled() ast.AlterOperation {
	column := ast.NewColumn("s", "TEXT").SetNotNull().SetDefault("new")
	return &ast.ModifyColumnOperation{Column: column, HasChanged: true, Changed: ast.ColumnProperties{Nullability: true}}
}

// TestRenderSQL_OneStatement_HappyPath writes an ALTER TABLE whose changes all fit one
// statement in the statement's own order, which Operations need not share. It keeps what the
// writer held ahead of it.
func TestRenderSQL_OneStatement_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    string
	}{
		{name: "PostgreSQL", dialect: platform.Postgres, want: "DROP TABLE \"old\";\nALTER TABLE \"t\" ADD CONSTRAINT \"ck\" CHECK (x > 0), ADD COLUMN \"x\" INTEGER;\n\n"},
		{name: "MySQL", dialect: platform.MySQL, want: "DROP TABLE `old`;\nALTER TABLE `t` ADD CONSTRAINT `ck` CHECK (x > 0), ADD COLUMN `x` INTEGER;\n\n"},
		{name: "MariaDB", dialect: platform.MariaDB, want: "DROP TABLE `old`;\nALTER TABLE `t` ADD CONSTRAINT `ck` CHECK (x > 0), ADD COLUMN `x` INTEGER;\n\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			node := &ast.AlterTableNode{
				Name:         "t",
				Operations:   []ast.AlterOperation{addedColumn(), checkOverIt()},
				OneStatement: []ast.AlterOperation{checkOverIt(), addedColumn()},
			}

			c.Assert(must.Must(builtin.RenderSQL(test.dialect, ast.NewDropTable("old"), node)), qt.Equals, test.want)
		})
	}
}

// TestRenderSQL_OneStatement_FailurePath is a change that needs another statement beside it.
// The statement's own order holds only inside one statement, so the renderer writes
// Operations, each on its own, and keeps what it held ahead of them.
func TestRenderSQL_OneStatement_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		split ast.AlterOperation
		want  string
	}{
		{
			name: "a backfill", split: backfilled(),
			want: "DROP TABLE \"old\";\nALTER TABLE \"t\" ADD COLUMN \"x\" INTEGER;\nUPDATE \"t\" SET \"s\" = 'new' WHERE \"s\" IS NULL;\n" +
				"ALTER TABLE \"t\" ALTER COLUMN \"s\" SET NOT NULL;\nALTER TABLE \"t\" ADD CONSTRAINT \"ck\" CHECK (x > 0);\n\n",
		},
		{
			name: "a comment", split: &ast.SetCommentOperation{Column: "s", Comment: "why"},
			want: "DROP TABLE \"old\";\nALTER TABLE \"t\" ADD COLUMN \"x\" INTEGER;\nCOMMENT ON COLUMN \"t\".\"s\" IS 'why';\n" +
				"ALTER TABLE \"t\" ADD CONSTRAINT \"ck\" CHECK (x > 0);\n\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			node := &ast.AlterTableNode{
				Name:         "t",
				Operations:   []ast.AlterOperation{addedColumn(), test.split, checkOverIt()},
				OneStatement: []ast.AlterOperation{checkOverIt(), test.split, addedColumn()},
			}

			c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, ast.NewDropTable("old"), node)), qt.Equals, test.want)
		})
	}
}
