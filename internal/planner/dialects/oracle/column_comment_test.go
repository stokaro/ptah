package oracle_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	oracleplanner "go.5x5.cz/ptah/internal/planner/dialects/oracle"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// Oracle emits a column's comment as a statement of its own, and emits no
// MODIFY when the comment is the only difference -- stokaro/ptah#2168.
//
// The Oracle planner is the MySQL-family algorithm, and there a column comment
// rides along inside MODIFY COLUMN. Oracle has no inline comment clause, so
// without a statement of its own the comment simply never reached the server.
// And a MODIFY that changes nothing is worse here than elsewhere: this
// planner's own note records that Oracle's nullability clause is not
// idempotent, so an empty restatement is a statement waiting to fail.
func TestPlanner_OraclePlansAColumnCommentOnItsOwn(t *testing.T) {
	c := qt.New(t)

	nodes, err := oracleplanner.New().GenerateMigrationASTChecked(
		columnCommentDiff("primary contact"),
		commentedColumnSchema(),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(setCommentOperations(nodes), qt.DeepEquals,
		[]*ast.SetCommentOperation{{Column: "EMAIL", Comment: "primary contact", HasCurrent: true}})
	c.Assert(modifyColumnOperations(nodes), qt.HasLen, 0)
}

// A comment removed from the declaration is still a statement: leaving the
// column alone is what let a deleted comment survive in the database.
func TestPlanner_OraclePlansAColumnCommentRemoval(t *testing.T) {
	c := qt.New(t)

	nodes, err := oracleplanner.New().GenerateMigrationASTChecked(
		columnCommentDiff(""),
		commentedColumnSchema(),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(setCommentOperations(nodes), qt.DeepEquals,
		[]*ast.SetCommentOperation{{Column: "EMAIL", Comment: "", HasCurrent: true}})
}

// The control: a column change that is not a comment still gets its MODIFY, so
// the skip above is scoped to the comment-only case.
func TestPlanner_OracleStillModifiesAColumnThatChanged(t *testing.T) {
	c := qt.New(t)
	diff := columnCommentDiff("primary contact")
	diff.TablesModified[0].ColumnsModified[0].Changes = map[string]string{
		"nullable": "true -> false",
	}

	nodes, err := oracleplanner.New().GenerateMigrationASTChecked(diff, commentedColumnSchema())

	c.Assert(err, qt.IsNil)
	c.Assert(setCommentOperations(nodes), qt.HasLen, 1)
	c.Assert(modifyColumnOperations(nodes), qt.HasLen, 1)
}

func columnCommentDiff(desired string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName: "USERS",
			ColumnsModified: []difftypes.ColumnDiff{{
				ColumnName:    "EMAIL",
				Changes:       make(map[string]string),
				CommentChange: &difftypes.CommentChange{Current: "login address", Desired: desired},
			}},
		}},
	}
}

func commentedColumnSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{Name: "USERS", StructName: "User"}},
		Fields: []goschema.Field{
			{Name: "ID", StructName: "User", Type: "NUMBER", Primary: true},
			{Name: "EMAIL", StructName: "User", Type: "VARCHAR2(255)"},
		},
	}
}

func setCommentOperations(nodes []ast.Node) []*ast.SetCommentOperation {
	found := make([]*ast.SetCommentOperation, 0)
	for _, node := range nodes {
		found = appendOperations(found, node)
	}
	return found
}

func appendOperations(found []*ast.SetCommentOperation, node ast.Node) []*ast.SetCommentOperation {
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

func modifyColumnOperations(nodes []ast.Node) []*ast.ModifyColumnOperation {
	found := make([]*ast.ModifyColumnOperation, 0)
	for _, node := range nodes {
		found = appendModifies(found, node)
	}
	return found
}

func appendModifies(found []*ast.ModifyColumnOperation, node ast.Node) []*ast.ModifyColumnOperation {
	alter, isAlter := node.(*ast.AlterTableNode)
	if !isAlter {
		return found
	}
	for _, operation := range alter.Operations {
		if modify, isModify := operation.(*ast.ModifyColumnOperation); isModify {
			found = append(found, modify)
		}
	}
	return found
}
