package schemaprep

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlident"
)

// InlineEnumDialect reports whether a target models an enum column inline --
// as a MySQL `enum(...)` type, or as a string column with a CHECK -- rather
// than as a named type of its own.
func InlineEnumDialect(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB, platform.SQLite, platform.SQLServer, platform.Oracle:
		return true
	default:
		return false
	}
}

// ResolveInlineEnums returns a copy of db whose enum columns carry the type and
// the CHECK the target actually has, with the named enums removed.
//
// It is the one rule for what an enum column looks like on a target that has no
// enum type, and it has to be one rule because both sides of a comparison need
// the same answer. The comparison normalizes its desired side with it; the
// conversion into a description applies it too, so a document converted for the
// current side reports the column the way a reader of that target would. Two
// copies of the rule agree until one of them learns something: a description
// carrying the declared `ENUM` against a desired side carrying
// `enum('draft','live')` is a column type change on every run, and on MySQL that
// is a table rewrite (stokaro/ptah#3214).
//
// A target that has a real enum type gets the document back unchanged, and so
// does a nil one. The input is never modified.
func ResolveInlineEnums(db *schemamodel.Database, dialect string) *schemamodel.Database {
	if db == nil || !InlineEnumDialect(dialect) {
		return db
	}

	resolved := *db
	resolved.Enums = nil
	resolved.Fields = append([]schemamodel.Field(nil), db.Fields...)
	for index := range resolved.Fields {
		applyInlineEnum(&resolved.Fields[index], db.Enums, dialect)
	}
	return &resolved
}

// applyInlineEnum rewrites one field into the target's inline spelling.
func applyInlineEnum(field *schemamodel.Field, enums []schemamodel.Enum, dialect string) {
	resolveDeclaredEnumValues(field, enums)
	if len(field.Enum) == 0 {
		return
	}
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
		field.Type = MySQLInlineEnumType(field.Enum)
	case platform.SQLite:
		field.Type = "TEXT"
		field.Check = enumCheckExpression(*field, field.Name)
	case platform.SQLServer:
		field.Type = "NVARCHAR(255)"
		field.Check = enumCheckExpression(*field, "["+strings.ReplaceAll(field.Name, "]", "]]")+"]")
	case platform.Oracle:
		field.Type = "VARCHAR2(255)"
		field.Check = enumCheckExpression(*field, sqlident.Ident(platform.Oracle, field.Name))
	}
}

// resolveDeclaredEnumValues fills in the values for the other spelling of an
// enum column.
//
// A column can name its values two ways: inline on the field, or by naming an
// enum declared elsewhere. The renderer reads the second -- handleEnumTypes
// finds the enum by the column's type -- and a normalization reading only the
// first leaves a schema written with `//ptah:schema:enum` plus
// `type="status_kind"` rendered as the target's inline model and compared as the
// enum's own name. Nothing converges: measured on SQLite, the plan rebuilt the
// table into one whose only difference from the original was none, on every
// apply.
//
// Filling the values here rather than teaching every arm about the second
// spelling keeps the arms about what a dialect writes, which is what they are
// for.
func resolveDeclaredEnumValues(field *schemamodel.Field, enums []schemamodel.Enum) {
	if len(field.Enum) > 0 {
		return
	}
	for _, enum := range enums {
		if enum.Name == field.Type {
			field.Enum = enum.Values
			return
		}
	}
}

// enumCheckExpression is the membership test, with the column spelled the way
// the target's renderer spells it beside the declaration.
//
// Oracle refuses a CHECK whose spelling disagrees with the column it constrains,
// so the column reference is a parameter rather than a rule this function
// guesses: sqlident.Ident is what the renderer's escapeIdentifier calls, and it
// is what internal/modelast already uses for the same expression on the
// rendering side.
func enumCheckExpression(field schemamodel.Field, column string) string {
	quoted := make([]string, 0, len(field.Enum))
	for _, value := range field.Enum {
		quoted = append(quoted, "'"+strings.ReplaceAll(value, "'", "''")+"'")
	}
	membership := column + " IN (" + strings.Join(quoted, ", ") + ")"
	if field.Check != "" {
		return "(" + field.Check + ") AND " + membership
	}
	return membership
}

// MySQLInlineEnumType is the MySQL and MariaDB spelling of an enum column type.
func MySQLInlineEnumType(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, "'"+strings.ReplaceAll(value, "'", "''")+"'")
	}
	return "enum(" + strings.Join(quoted, ",") + ")"
}
