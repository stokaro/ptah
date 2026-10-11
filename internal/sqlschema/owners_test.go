package sqlschema_test

import (
	"context"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
)

// readWithOwners is [sqlschema.Read] with the owners the built-in runtime
// registers, for a document that carries an owner's syntax.
func readWithOwners(data []byte, dialect string) (schemamodel.Database, *ast.StatementList, error) {
	return sqlschema.ReadWith(context.Background(), data, dialect, nil,
		sqlschema.Options{Owners: builtintest.Runtime().SQL()})
}
