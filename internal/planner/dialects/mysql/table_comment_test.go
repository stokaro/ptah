package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
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

			nodes, err := test.planner.GenerateMigrationAST(
				tableCommentDiff("customers of record"),
				commentedTableSchema(),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(setCommentOperations(nodes), qt.DeepEquals,
				[]*ast.SetCommentOperation{{Comment: "customers of record", HasCurrent: true}})
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

			nodes, err := test.planner.GenerateMigrationAST(
				tableCommentDiff(""),
				commentedTableSchema(),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(setCommentOperations(nodes), qt.DeepEquals,
				[]*ast.SetCommentOperation{{Comment: "", HasCurrent: true}})
		})
	}
}

// The control: a table with no comment transition produces no comment
// statement, so an ordinary plan does not grow a line that says nothing.
func TestPlanner_MySQLFamilyPlansNoCommentWithoutAChange(t *testing.T) {
	for _, test := range mysqlFamilyPlannerCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := test.planner.GenerateMigrationAST(
				&difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{TableName: "users"}}},
				commentedTableSchema(),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(setCommentOperations(nodes), qt.HasLen, 0)
		})
	}
}

func tableCommentDiff(desired string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:     "users",
			CommentChange: &difftypes.CommentChange{Current: "people who buy", Desired: desired},
		}},
	}
}

func commentedTableSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "users", StructName: "User"}},
		Fields: []schemamodel.Field{
			{Name: "id", StructName: "User", Type: "INT", Primary: true},
			{Name: "email", StructName: "User", Type: "VARCHAR(255)", Comment: "primary contact"},
		},
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

// A column's comment does NOT get a statement of its own here: MODIFY COLUMN
// restates the whole definition and carries it. This is the control for the
// dialect predicate -- without it, making every dialect emit a separate
// statement would pass, and MySQL would receive an operation its renderer
// refuses (stokaro/ptah#2168).
func TestPlanner_MySQLFamilyCarriesAColumnCommentInTheModify(t *testing.T) {
	for _, test := range mysqlFamilyPlannerCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := test.planner.GenerateMigrationAST(
				columnCommentOnlyDiff(),
				commentedTableSchema(),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(setCommentOperations(nodes), qt.HasLen, 0)
			c.Assert(modifiedColumnComments(nodes), qt.DeepEquals, []string{"primary contact"})
		})
	}
}

func columnCommentOnlyDiff() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName: "users",
			ColumnsModified: []difftypes.ColumnDiff{{
				ColumnName:    "email",
				Changes:       make(map[string]string),
				CommentChange: &difftypes.CommentChange{Current: "login address", Desired: "primary contact"},
			}},
		}},
	}
}

func modifiedColumnComments(nodes []ast.Node) []string {
	found := make([]string, 0)
	for _, node := range nodes {
		found = appendModifiedComments(found, node)
	}
	return found
}

func appendModifiedComments(found []string, node ast.Node) []string {
	alter, isAlter := node.(*ast.AlterTableNode)
	if !isAlter {
		return found
	}
	for _, operation := range alter.Operations {
		if modify, isModify := operation.(*ast.ModifyColumnOperation); isModify {
			found = append(found, modify.Column.Comment)
		}
	}
	return found
}
