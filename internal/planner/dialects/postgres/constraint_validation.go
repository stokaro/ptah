package postgres

import (
	"ptah.run/core/ast"
	"ptah.run/migration/schemadiff/difftypes"
)

// validateConstraints validates each constraint the database holds NOT VALID
// and the declaration holds validated, as VALIDATE CONSTRAINT on its table.
// The comparison reports one only for a constraint it neither adds nor
// recreates, so nothing else in the plan validates it.
func validateConstraints(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, validation := range diff.ConstraintsValidated {
		result = append(result, &ast.AlterTableNode{
			Name:       validation.TableName,
			Operations: []ast.AlterOperation{&ast.ValidateConstraintOperation{ConstraintName: validation.Name}},
		})
	}
	return result
}
