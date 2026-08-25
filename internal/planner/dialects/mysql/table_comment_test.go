package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// A table whose only difference is its comment produces a statement --
// stokaro/ptah#2168.
//
// The column half needs nothing here: these engines restate the whole column to
// change anything about it, and the MODIFY COLUMN the planner already emits
// carries the comment. The table half has no such carrier, so without this the
// comparator's finding reached the planner and stopped.
func TestPlanner_MySQLFamilyPlansATableCommentChange(t *testing.T) {
	for _, test := range mysqlFamilyPlannerCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := test.planner.GenerateMigrationASTChecked(
				tableCommentDiff("customers of record"),
				commentedTableSchema(),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(setCommentOperations(nodes), qt.DeepEquals,
				[]*ast.SetCommentOperation{{Comment: "customers of record"}})
		})
	}
}

// A comment removed from the declaration reaches the planner as an empty
// desired side, and must still produce a statement: leaving the table alone is
// what let a deleted comment survive in the database.
func TestPlanner_MySQLFamilyPlansATableCommentRemoval(t *testing.T) {
	for _, test := range mysqlFamilyPlannerCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := test.planner.GenerateMigrationASTChecked(
				tableCommentDiff(""),
				commentedTableSchema(),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(setCommentOperations(nodes), qt.DeepEquals,
				[]*ast.SetCommentOperation{{Comment: ""}})
		})
	}
}

// The control: a table with no comment transition produces no comment
// statement, so an ordinary plan does not grow a line that says nothing.
func TestPlanner_MySQLFamilyPlansNoCommentWithoutAChange(t *testing.T) {
	for _, test := range mysqlFamilyPlannerCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := test.planner.GenerateMigrationASTChecked(
				&types.SchemaDiff{TablesModified: []types.TableDiff{{TableName: "users"}}},
				commentedTableSchema(),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(setCommentOperations(nodes), qt.HasLen, 0)
		})
	}
}

func tableCommentDiff(desired string) *types.SchemaDiff {
	return &types.SchemaDiff{
		TablesModified: []types.TableDiff{{
			TableName:     "users",
			CommentChange: &types.CommentChange{Current: "people who buy", Desired: desired},
		}},
	}
}

func commentedTableSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{Name: "users", StructName: "User"}},
		Fields: []goschema.Field{{Name: "id", StructName: "User", Type: "INT", Primary: true}},
	}
}

func setCommentOperations(nodes []ast.Node) []*ast.SetCommentOperation {
	found := make([]*ast.SetCommentOperation, 0)
	for _, node := range nodes {
		found = appendSetComments(found, node)
	}
	return found
}

func appendSetComments(found []*ast.SetCommentOperation, node ast.Node) []*ast.SetCommentOperation {
	alter, isAlter := node.(*ast.AlterTableNode)
	if !isAlter {
		return found
	}
	for _, operation := range alter.Operations {
		if setComment, isSetComment := operation.(*ast.SetCommentOperation); isSetComment {
			found = append(found, setComment)
		}
	}
	return found
}
