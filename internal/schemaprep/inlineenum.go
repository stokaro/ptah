package schemaprep

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlident"
)

// InlineEnumDialect reports whether dialect builds an enum column inline,
// with no standalone enum type: MySQL and MariaDB as an ENUM column type,
// SQLite, SQL Server and Oracle as a text column with a CHECK.
//
// YDB has no enum, inline or standalone, and the renderer and the planner
// refuse one; it is classed with the standalone dialects to agree with
// [EmitsStandaloneEnumDefinitions], which keeps the enum a declaration of its
// own for that refusal to meet.
func InlineEnumDialect(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB, platform.SQLite, platform.SQLServer, platform.Oracle:
		return true
	default:
		return false
	}
}

// EnumColumnValues returns the values of field as an enum column, and
// whether it names one of enums as its type. Values are nil when field is not
// an enum column.
//
// A column is an enum column two ways: typed ENUM with its values on the
// field, or typed with the name of one of enums. Both spellings have to be
// read: a lowering that reads only the values on the field compares a schema
// written with `//ptah:schema:enum` and `type="status_kind"` as the enum's own
// name, and on SQLite the plan rebuilds the table on every apply. Values
// beside any other type make no enum column, since [schemamodel.Field.Enum]
// holds the values of an ENUM type, and neither does a column
// [LowerInlineEnums] built: enum('a','b'), or a text column with its CHECK.
// Lowering a lowered column therefore changes nothing.
//
// An enum column that names no declared enum gives a target without inline
// enums no type to create, so the renderer refuses it there. The returned
// slice is field's or an enum's own and is not to be changed.
func EnumColumnValues(field schemamodel.Field, enums []schemamodel.Enum) (values []string, declared bool) {
	for _, enum := range enums {
		if enum.Name != field.Type {
			continue
		}
		if len(field.Enum) > 0 {
			return field.Enum, true
		}
		return enum.Values, true
	}
	if strings.EqualFold(field.Type, "ENUM") && len(field.Enum) > 0 {
		return field.Enum, false
	}
	return nil, false
}

// InlineEnumValues returns the values dialect builds field with as an inline
// enum column, and nil where dialect is not an [InlineEnumDialect] or field is
// not an enum column; see [EnumColumnValues].
//
// The renderer asks this to build the column, and [LowerInlineEnums] to
// describe it, so a render builds the column a comparison expects a read of
// the server to report.
func InlineEnumValues(field schemamodel.Field, enums []schemamodel.Enum, dialect string) []string {
	if !InlineEnumDialect(dialect) {
		return nil
	}
	values, _ := EnumColumnValues(field, enums)
	return values
}

// LowerInlineEnums returns a copy of fields with every enum column described
// as dialect builds it, and fields itself where dialect is not an
// [InlineEnumDialect].
//
// Two callers need this one answer. The comparison lowers the declared side so
// it compares with what a read of such a server reports, and goschematodb
// lowers a declared schema it describes as such a read. Lowered by the
// comparison alone, a schema file compared with itself planned a MODIFY to the
// same enum on MySQL, a varchar and a CHECK on SQL Server and Oracle, and a
// table rebuild on SQLite.
//
// Which columns are enums, and with which values, is [InlineEnumValues].
func LowerInlineEnums(fields []schemamodel.Field, enums []schemamodel.Enum, dialect string) []schemamodel.Field {
	if !InlineEnumDialect(dialect) {
		return fields
	}
	lowered := append([]schemamodel.Field(nil), fields...)
	for i := range lowered {
		field := &lowered[i]
		values := InlineEnumValues(*field, enums, dialect)
		if len(values) == 0 {
			continue
		}
		field.Enum = values
		switch platform.NormalizeDialect(dialect) {
		case platform.MySQL, platform.MariaDB:
			field.Type = mysqlInlineEnumType(field.Enum)
		case platform.SQLite:
			field.Type = "TEXT"
			field.Check = inlineEnumCheck(*field, field.Name)
		case platform.SQLServer:
			field.Type = "NVARCHAR(255)"
			field.Check = inlineEnumCheck(*field, "["+strings.ReplaceAll(field.Name, "]", "]]")+"]")
		case platform.Oracle:
			// Oracle refuses a CHECK whose spelling disagrees with the column
			// it constrains, so the column is spelled by sqlident.Ident, which
			// is what the renderer's escapeIdentifier and
			// modelast.applyInlineEnumModel call for the same expression.
			field.Type = "VARCHAR2(255)"
			field.Check = inlineEnumCheck(*field, sqlident.Ident(platform.Oracle, field.Name))
		}
	}
	return lowered
}

// inlineEnumCheck is the CHECK an inline enum column carries, over the
// column spelled as column, joined to the column's own CHECK where it has one.
func inlineEnumCheck(field schemamodel.Field, column string) string {
	enumCheck := column + " IN (" + strings.Join(quotedEnumValues(field.Enum), ", ") + ")"
	if field.Check != "" {
		return "(" + field.Check + ") AND " + enumCheck
	}
	return enumCheck
}

func mysqlInlineEnumType(values []string) string {
	return "enum(" + strings.Join(quotedEnumValues(values), ",") + ")"
}

func quotedEnumValues(values []string) []string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, "'"+strings.ReplaceAll(value, "'", "''")+"'")
	}
	return quoted
}
