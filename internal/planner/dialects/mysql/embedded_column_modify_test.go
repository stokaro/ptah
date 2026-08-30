package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/mysql"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestPlanner_ModifiesAColumnDeclaredInsideAnEmbeddedStruct is the repair the
// column operand brought with it.
//
// The resolution this planner used scanned `desired.Fields` for a field whose
// StructName is the table's struct. A column declared inside an EMBEDDED struct
// carries the embedded struct's name, so the scan never matched it: the plan
// emitted `ERROR: Could not find field definition` as a COMMENT and left the
// column alone, and the migration applied cleanly. The comparison folds embedded
// structs before it compares, so the operand it carries is the folded column and
// there is nothing left to match.
//
// The diff comes from a real comparison rather than a literal, because the fold
// is the thing under test and a hand-written operand would supply the answer.
func TestPlanner_ModifiesAColumnDeclaredInsideAnEmbeddedStruct(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "wf2315_orders"}},
		Fields: []schemamodel.Field{
			{StructName: "Order", Name: "id", Type: "INT", Primary: true},
			{StructName: "Audit", Name: "revision", Type: "BIGINT"},
		},
		EmbeddedFields: []schemamodel.EmbeddedField{{
			StructName:       "Order",
			Mode:             "inline",
			EmbeddedTypeName: "Audit",
		}},
	}
	database := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "wf2315_orders", Type: "BASE TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "revision", DataType: "int", IsNullable: "NO"},
			},
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, database, platform.MySQL)
	nodes, err := mysql.New().GenerateMigrationAST(withDeclaredObjects(diff, desired))
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL(platform.MySQL, nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(sql, qt.Contains, "MODIFY COLUMN `revision` BIGINT",
		qt.Commentf("plan:\n%s", sql))
	c.Assert(sql, qt.Not(qt.Contains), "no column definition",
		qt.Commentf("plan:\n%s", sql))
}
