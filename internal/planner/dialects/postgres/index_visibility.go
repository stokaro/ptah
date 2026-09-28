package postgres

import (
	"ptah.run/core/ast"
	"ptah.run/migration/schemadiff/difftypes"
)

// changeIndexVisibility plans each index visibility change the comparison
// found. The PostgreSQL family has no invisible index, and the renderer
// refuses the operation by [capability.InvisibleIndexes]: a declaration that
// hides an index is refused loudly rather than planned as nothing.
func changeIndexVisibility(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, change := range diff.IndexVisibilityChanged {
		result = append(result, &ast.AlterTableNode{
			Name: change.TableName,
			Operations: []ast.AlterOperation{&ast.AlterIndexVisibilityOperation{
				IndexName: change.Name, Invisible: change.Invisible,
			}},
		})
	}
	return result
}
