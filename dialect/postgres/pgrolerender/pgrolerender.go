// Package pgrolerender renders owner-validated PostgreSQL role settings: the
// ALTER ROLE statements of a planned change, and the ones a CREATE ROLE needs
// after it for the settings the new role declares. It reads no database.
package pgrolerender

import (
	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgrole"
)

// Handlers returns a fresh handler for the owner's statement payload.
func Handlers() []renderer.ExtensionHandler {
	return []renderer.ExtensionHandler{renderer.TypedHandler(&pgrole.AlterSettings{}, ast.StatementExtension, validate, render)}
}

func validate(ctx renderer.ExtensionContext, operation *pgrole.AlterSettings) error {
	if !pgrole.SupportsSettings(ctx.Target) {
		return renderer.UnsupportedExtension(ctx.Target, pgrole.OperationKind, ast.StatementExtension)
	}
	return operation.Validate()
}

func render(_ renderer.ExtensionContext, operation *pgrole.AlterSettings) ([]string, error) {
	return operation.Statements()
}

// LowerRoleFacets takes the settings a CREATE ROLE cannot carry and returns
// the statement that follows it, with the facets it left for the statement
// itself. A declaration holding only resets needs no statement on a new role.
func LowerRoleFacets(role string, facets schemaext.Facets) ([]ast.ExtensionPayload, schemaext.Facets, error) {
	operation, err := pgrole.CreationOperation(role, facets)
	if err != nil {
		return nil, schemaext.Facets{}, err
	}
	rest := facets.Without(pgrole.SettingsKind)
	if operation == nil {
		return nil, rest, nil
	}
	return []ast.ExtensionPayload{operation}, rest, nil
}
