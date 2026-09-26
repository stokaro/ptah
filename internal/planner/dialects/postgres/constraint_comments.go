package postgres

import (
	"ptah.run/core/ast"
	"ptah.run/migration/schemadiff/difftypes"
)

// changeConstraintComments sets the comment of each constraint whose comment
// the comparison found changed, as COMMENT ON CONSTRAINT on its table
// (stokaro/ptah#3678). The comparison reports a change only for a constraint
// it neither adds nor recreates, so nothing else in the plan writes it.
func changeConstraintComments(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, change := range diff.ConstraintCommentsChanged {
		result = append(result, &ast.AlterTableNode{
			Name: change.TableName,
			Operations: []ast.AlterOperation{&ast.SetConstraintCommentOperation{
				Constraint: change.Name,
				Comment:    change.Desired,
			}},
		})
	}
	return result
}
