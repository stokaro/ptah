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
// A column names its values inline, or by naming one of enums as its type; see
// [resolveDeclaredEnumValues].
func LowerInlineEnums(fields []schemamodel.Field, enums []schemamodel.Enum, dialect string) []schemamodel.Field {
	if !InlineEnumDialect(dialect) {
		return fields
	}
	lowered := append([]schemamodel.Field(nil), fields...)
	for i := range lowered {
		field := &lowered[i]
		resolveDeclaredEnumValues(field, enums)
		if len(field.Enum) == 0 {
			continue
		}
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

// resolveDeclaredEnumValues fills in the values for the other spelling of an
// enum column.
//
// A column can name its values two ways: inline on the field, or by naming an
// enum declared elsewhere. The renderer reads the second -- handleEnumTypes
// finds the enum by the column's type -- and a lowering that read only the
// first rendered a schema written with `//ptah:schema:enum` plus
// `type="status_kind"` as the target's inline model and compared it as the
// enum's own name. Nothing converged: measured on SQLite, the plan rebuilt the
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
