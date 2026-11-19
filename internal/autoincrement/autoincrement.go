// Package autoincrement says whether a declared column is built
// auto-incrementing. The rendering of a declaration, its conversion to a
// catalog and its comparison with a catalog all ask it, so the three cannot
// disagree about which columns a declaration makes auto-increment.
package autoincrement

import (
	"strings"

	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
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

// DeclaredOn reports whether field builds an auto-increment column on
// dialect: what [Declared] reports, or a serial type the target writes as its
// own auto-increment integer ([schemaprep.SerialWritesAutoIncrement]).
func DeclaredOn(field schemamodel.Field, dialect string) bool {
	return Declared(field) || schemaprep.SerialWritesAutoIncrement(field, dialect)
}

// NotNull reports whether the server builds field NOT NULL whatever its
// definition says, as columns states it for the target: an identity column
// where IdentityNotNull holds, any column [Declared] auto-increment where
// AutoIncrementNotNull holds, and a serial type where SerialNotNull holds.
// The comparison and the conversion of a declaration to a catalog both ask
// it: when the two answer differently, a declaration compared with the
// catalog converted from it plans a NOT NULL change.
func NotNull(field schemamodel.Field, columns descriptor.Columns) bool {
	return columns.IdentityNotNull && field.IdentityGeneration != "" ||
		columns.AutoIncrementNotNull && Declared(field) ||
		columns.SerialNotNull && SerialType(field.Type)
}
