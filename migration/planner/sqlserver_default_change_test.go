package planner_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// sqlServerColumnDiff is a diff that changes column status of dbo.users.
func sqlServerColumnDiff(desired schemamodel.Field, changes map[string]string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "dbo.users",
		ColumnsModified: []difftypes.ColumnDiff{{
			ColumnName: desired.Name,
			Desired:    desired,
			Changes:    changes,
		}},
	}}}
}

// alterSteps names the ALTER TABLE operations of a plan in order: ALTER
// COLUMN for a restatement of the column, and the action of an ALTER COLUMN
// operation.
func alterSteps(nodes []ast.Node) []string {
	steps := make([]string, 0, len(nodes))
	for _, node := range nodes {
		alter, isAlter := node.(*ast.AlterTableNode)
		if !isAlter {
			continue
		}
		for _, operation := range alter.Operations {
			switch op := operation.(type) {
			case *ast.ModifyColumnOperation:
				steps = append(steps, "ALTER COLUMN")
			case *ast.AlterColumnOperation:
				steps = append(steps, string(op.Action))
			default:
				steps = append(steps, fmt.Sprintf("%T", op))
			}
		}
	}
	return steps
}

// A default is a constraint in SQL Server, so a default change is an ALTER
// COLUMN action of its own beside the ALTER COLUMN that restates the type and
// nullability, which cannot carry one. A type change drops the default first
// and sets it again after, because ALTER COLUMN refuses a column a default is
// bound to; a nullability change does not need to (stokaro/ptah#3650).
func TestGenerateSchemaDiffASTWithOptions_SQLServerDefaultChange(t *testing.T) {
	withDefault := schemamodel.Field{Name: "status", Type: "NVARCHAR(20)", Nullable: true, Default: "active", StructName: "User"}
	withoutDefault := schemamodel.Field{Name: "status", Type: "NVARCHAR(20)", Nullable: true, StructName: "User"}
	tests := []struct {
		name    string
		desired schemamodel.Field
		changes map[string]string
		want    []string
	}{
		{
			name:    "a default changed",
			desired: withDefault,
			changes: map[string]string{"default": "('inactive') -> active"},
			want:    []string{"SET DEFAULT"},
		},
		{
			name:    "a default added",
			desired: withDefault,
			changes: map[string]string{"default": " -> active"},
			want:    []string{"SET DEFAULT"},
		},
		{
			name:    "an expression default added",
			desired: schemamodel.Field{Name: "status", Type: "NVARCHAR(20)", Nullable: true, DefaultExpr: "suser_sname()", StructName: "User"},
			changes: map[string]string{"default_expr": " -> suser_sname()"},
			want:    []string{"SET DEFAULT"},
		},
		{
			name:    "a default removed",
			desired: withoutDefault,
			changes: map[string]string{"default": "('active') -> "},
			want:    []string{"DROP DEFAULT"},
		},
		{
			name:    "an expression default removed",
			desired: withoutDefault,
			changes: map[string]string{"default_expr": "(getdate()) -> "},
			want:    []string{"DROP DEFAULT"},
		},
		{
			name:    "the type changed under a default",
			desired: withDefault,
			changes: map[string]string{"type": "nvarchar(10) -> NVARCHAR(20)"},
			want:    []string{"DROP DEFAULT", "ALTER COLUMN", "SET DEFAULT"},
		},
		{
			name:    "the type changed and the default removed",
			desired: withoutDefault,
			changes: map[string]string{"type": "nvarchar(10) -> NVARCHAR(20)", "default": "('active') -> "},
			want:    []string{"DROP DEFAULT", "ALTER COLUMN"},
		},
		{
			name:    "the type changed and a default added",
			desired: withDefault,
			changes: map[string]string{"type": "nvarchar(10) -> NVARCHAR(20)", "default": " -> active"},
			want:    []string{"ALTER COLUMN", "SET DEFAULT"},
		},
		{
			name:    "the type changed with no default",
			desired: withoutDefault,
			changes: map[string]string{"type": "nvarchar(10) -> NVARCHAR(20)"},
			want:    []string{"ALTER COLUMN"},
		},
		{
			name:    "nullability changed under a default",
			desired: withDefault,
			changes: map[string]string{"nullable": "false -> true"},
			want:    []string{"ALTER COLUMN"},
		},
		{
			name:    "nullability and the default changed",
			desired: withDefault,
			changes: map[string]string{"nullable": "false -> true", "default": "('inactive') -> active"},
			want:    []string{"ALTER COLUMN", "SET DEFAULT"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := planner.GenerateSchemaDiffASTWithOptions(sqlServerColumnDiff(test.desired, test.changes), platform.SQLServer, planner.Options{})

			c.Assert(err, qt.IsNil)
			c.Assert(alterSteps(nodes), qt.DeepEquals, test.want)
		})
	}
}

// The default an ALTER COLUMN sets is the declared one, on the column and the
// table the change names.
func TestGenerateSchemaDiffASTWithOptions_SQLServerSetsTheDeclaredDefault(t *testing.T) {
	c := qt.New(t)
	desired := schemamodel.Field{Name: "status", Type: "NVARCHAR(20)", Nullable: true, Default: "active", StructName: "User"}

	nodes, err := planner.GenerateSchemaDiffASTWithOptions(
		sqlServerColumnDiff(desired, map[string]string{"default": "('inactive') -> active"}), platform.SQLServer, planner.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 3)
	c.Assert(nodes[2], qt.DeepEquals, ast.Node(&ast.AlterTableNode{Name: "dbo.users", Operations: []ast.AlterOperation{&ast.AlterColumnOperation{
		ColumnName: "status",
		Action:     ast.AlterColumnSetDefault,
		Default:    &ast.DefaultValue{Value: "active", ValueSet: true},
	}}}))
}

// Every other engine of the family keeps one restatement per change, a
// default change included: MySQL's MODIFY carries the default.
func TestGenerateSchemaDiffASTWithOptions_MySQLDefaultChangeStaysOneModify(t *testing.T) {
	c := qt.New(t)
	desired := schemamodel.Field{Name: "status", Type: "VARCHAR(20)", Nullable: true, Default: "active", StructName: "User"}

	nodes, err := planner.GenerateSchemaDiffASTWithOptions(
		sqlServerColumnDiff(desired, map[string]string{"type": "varchar(10) -> VARCHAR(20)", "default": "inactive -> active"}), platform.MySQL, planner.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(alterSteps(nodes), qt.DeepEquals, []string{"ALTER COLUMN"})
}
