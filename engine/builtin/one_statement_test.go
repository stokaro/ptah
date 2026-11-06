package builtin_test

import (
	"slices"
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
			column, check := addedColumn(), checkOverIt()
			node := &ast.AlterTableNode{
				Name:         "t",
				Operations:   []ast.AlterOperation{column, check},
				OneStatement: []ast.AlterOperation{check, column},
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
			column, check := addedColumn(), checkOverIt()
			node := &ast.AlterTableNode{
				Name:         "t",
				Operations:   []ast.AlterOperation{column, test.split, check},
				OneStatement: []ast.AlterOperation{check, test.split, column},
			}

			c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, ast.NewDropTable("old"), node)), qt.Equals, test.want)
		})
	}
}

// foreignKeyWithActions is a key with actions as a schema file spells them.
func foreignKeyWithActions(onUpdate, onDelete string) *ast.ConstraintNode {
	return ast.NewForeignKeyConstraint("c_p", []string{"p_id"}, &ast.ForeignKeyRef{Table: "p", Column: "id", OnUpdate: onUpdate, OnDelete: onDelete})
}

// TestRenderSQL_OneStatementIsPreparedLikeOperations spells a foreign key's actions the way
// each server takes them in both forms of a node: alone, where it fits one statement, and with
// a statement beside it, where it does not.
func TestRenderSQL_OneStatementIsPreparedLikeOperations(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		split   []ast.AlterOperation
		want    string
	}{
		{
			name: "one statement on PostgreSQL", dialect: platform.Postgres,
			want: "ON DELETE SET NULL ON UPDATE NO ACTION",
		},
		{
			name: "one statement on MySQL", dialect: platform.MySQL,
			want: "ON DELETE SET NULL ON UPDATE NO ACTION",
		},
		{
			name: "one statement on MariaDB", dialect: platform.MariaDB,
			want: "ON DELETE SET NULL ON UPDATE NO ACTION",
		},
		{
			name: "beside a backfill on PostgreSQL", dialect: platform.Postgres, split: []ast.AlterOperation{backfilled()},
			want: "ON DELETE SET NULL ON UPDATE NO ACTION",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			key := &ast.AddConstraintOperation{Constraint: foreignKeyWithActions("NO_ACTION", "SET_NULL")}
			operations := append(slices.Clone(test.split), key)
			node := &ast.AlterTableNode{Name: "c", Operations: operations, OneStatement: slices.Clone(operations)}

			got := must.Must(builtin.RenderSQL(test.dialect, node))

			c.Assert(got, qt.Contains, test.want)
			c.Assert(got, qt.Not(qt.Contains), "NO_ACTION")
		})
	}
}

// TestRenderSQL_OneStatementANodeOutOfStep_FailurePath is a node whose two lists no longer say
// the same changes, as a pass that rewrites only one leaves it. The renderer writes Operations,
// which is an order that runs, and nothing of the stale list.
func TestRenderSQL_OneStatementANodeOutOfStep_FailurePath(t *testing.T) {
	column, check := addedColumn(), checkOverIt()
	tests := []struct {
		name       string
		operations []ast.AlterOperation
		statement  []ast.AlterOperation
		want       string
	}{
		{
			name:       "a change only the statement holds",
			operations: []ast.AlterOperation{column},
			statement:  []ast.AlterOperation{check, column},
			want:       "ALTER TABLE \"t\" ADD COLUMN \"x\" INTEGER;\n\n",
		},
		{
			name:       "a change only the operations hold",
			operations: []ast.AlterOperation{column, check},
			statement:  []ast.AlterOperation{column},
			want:       "ALTER TABLE \"t\" ADD COLUMN \"x\" INTEGER;\nALTER TABLE \"t\" ADD CONSTRAINT \"ck\" CHECK (x > 0);\n\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			node := &ast.AlterTableNode{Name: "t", Operations: test.operations, OneStatement: test.statement}

			c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, node)), qt.Equals, test.want)
		})
	}
}

// TestRenderSQL_OneStatementLeavesOutAChangeALaterOneReplaces is a column changed twice. One
// statement names it once, and the first change is not missing from it.
func TestRenderSQL_OneStatementLeavesOutAChangeALaterOneReplaces(t *testing.T) {
	c := qt.New(t)
	first := &ast.ModifyColumnOperation{Column: ast.NewColumn("x", "INT")}
	last := &ast.ModifyColumnOperation{Column: ast.NewColumn("x", "BIGINT")}
	node := &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{first, last}, OneStatement: []ast.AlterOperation{last}}

	c.Assert(must.Must(builtin.RenderSQL(platform.MySQL, node)), qt.Equals, "ALTER TABLE `t` MODIFY COLUMN `x` BIGINT;\n\n")
}

// TestRenderSQL_AKeyInsideItsTable writes a plain index in the column list of a table on MySQL
// and MariaDB, which is the only way to declare one over an AUTO_INCREMENT column.
func TestRenderSQL_AKeyInsideItsTable(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			table := ast.NewCreateTable("t").AddColumn(ast.NewColumn("id", "INT").SetNotNull().SetAutoIncrement())
			table.Indexes = []*ast.IndexNode{ast.NewIndex("t_id", "t", "id")}

			got := must.Must(builtin.RenderSQL(dialect, table))

			c.Assert(got, qt.Contains, "  `id` INT NOT NULL AUTO_INCREMENT,\n  INDEX `t_id` (`id`)\n)")
		})
	}
}

// TestRenderSQL_AKeyAddedInOneStatementWithItsColumn writes an added plain index as a clause of
// the statement that adds its AUTO_INCREMENT column.
func TestRenderSQL_AKeyAddedInOneStatementWithItsColumn(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			column := &ast.AddColumnOperation{Column: ast.NewColumn("id", "INT").SetNotNull().SetAutoIncrement()}
			key := &ast.AddIndexOperation{Index: ast.NewIndex("t_id", "t", "id")}
			operations := []ast.AlterOperation{column, key}
			node := &ast.AlterTableNode{Name: "t", Operations: operations, OneStatement: operations}

			c.Assert(must.Must(builtin.RenderSQL(dialect, node)), qt.Equals,
				"ALTER TABLE `t` ADD COLUMN `id` INT NOT NULL AUTO_INCREMENT, ADD INDEX `t_id` (`id`);\n\n")
		})
	}
}
