package sqlschema_test

import (
	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
)

// readYQL reads a YQL document through the owners the built-in runtime
// registers, whose YDB grammar claims the YDB coverage a YQL document makes.
// sqlschema.Read selects no owner, so it converts the statements and claims
// nothing for YDB.
func readYQL(c *qt.C, source string) (schemamodel.Database, error) {
	database, _, err := sqlschema.ReadWith(c.Context(), []byte(source), "ydb", nil,
		sqlschema.Options{Owners: builtintest.Runtime().SQL()})
	return database, err
}
