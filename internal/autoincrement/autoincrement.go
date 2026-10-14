// Package autoincrement says whether a declared column is built
// auto-incrementing. The conversion of a declaration to a catalog and the
// comparison of a declaration with a catalog both ask it, so the two cannot
// disagree about which columns a declaration makes auto-increment.
package autoincrement

import (
	"strings"

	"ptah.run/core/schemamodel"
)

// Declared reports whether field declares an auto-increment column: AutoInc,
// or an identity generation, which the column lowers to the same column
// (ast.ColumnNode.SetIdentity sets AutoInc), so the MySQL family writes
// AUTO_INCREMENT, SQLite AUTOINCREMENT and SQL Server IDENTITY for either.
func Declared(field schemamodel.Field) bool {
	return field.AutoInc || field.IdentityGeneration != ""
}

// SerialType reports whether a declared type is one of PostgreSQL's serial
// types, SERIAL, BIGSERIAL or SMALLSERIAL, in any case.
func SerialType(declared string) bool {
	switch strings.ToUpper(strings.TrimSpace(declared)) {
	case "SERIAL", "BIGSERIAL", "SMALLSERIAL":
		return true
	default:
		return false
	}
}
