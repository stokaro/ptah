package planner_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
)

// Every dialect planner states which properties a column modification
// changes, the MySQL family's too, although its MODIFY restates the whole
// column. The statement repeats NOT NULL on a type change as on a nullability
// change, and the safety report tells the two apart only by what is stated
// here (stokaro/ptah#3669).
func TestGenerateSchemaDiffASTWithOptions_ModificationStatesItsChanges(t *testing.T) {
	for _, dialect := range []string{platform.Postgres, platform.MySQL, platform.MariaDB, platform.SQLServer, platform.Oracle} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			diff := oneModifiedColumn(
				schemamodel.Field{Name: "c", Type: "BIGINT", StructName: "Flag"},
				map[string]string{"type": "int -> bigint"},
			)

			nodes, err := planner.GenerateSchemaDiffASTWithOptions(diff, dialect, planner.Options{})

			c.Assert(err, qt.IsNil)
			alters := slices.DeleteFunc(slices.Clone(nodes), func(node ast.Node) bool {
				_, isAlter := node.(*ast.AlterTableNode)
				return !isAlter
			})
			c.Assert(alters, qt.HasLen, 1)
			operations := alters[0].(*ast.AlterTableNode).Operations
			c.Assert(operations, qt.HasLen, 1)
			modify, isModify := operations[0].(*ast.ModifyColumnOperation)
			c.Assert(isModify, qt.IsTrue)
			c.Assert(modify.HasChanged, qt.IsTrue)
			c.Assert(modify.Changed, qt.DeepEquals, ast.ColumnProperties{Type: true})
		})
	}
}
