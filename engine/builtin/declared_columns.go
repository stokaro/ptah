package builtin

import (
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

// validateDeclaredColumns refuses a column declaration whose meaning no target
// receives.
//
// It was found by the field census (stokaro/ptah#2606) and filed as
// stokaro/ptah#2611. It is not a target's limitation: it is a fact an author
// writes that this pipeline replaces with a DIFFERENT constraint, which is
// worse than dropping it.
func validateDeclaredColumns(dialect string, database *schemamodel.Database) error {
	for _, field := range database.Fields {
		if err := validateUniqueExpression(dialect, field); err != nil {
			return err
		}
		if err := validateUndeclaredEnum(dialect, database, field); err != nil {
			return err
		}
	}
	return nil
}

// validateUndeclaredEnum refuses a column typed ENUM that carries its values
// and names no declared enum, on a target that cannot build it inline.
//
// The inline-enum dialects build such a column from its values, the way the
// comparison lowers it. The PostgreSQL family has no inline form: its column
// type has to name an enum type, and rendered as it stands the declaration is
// the bare type ENUM, which the server refuses. A type created here for the
// column would be an object no declaration owns, so the column is refused
// with what to declare instead. YDB refuses an ENUM type of any kind when it
// maps the column.
func validateUndeclaredEnum(dialect string, database *schemamodel.Database, field schemamodel.Field) error {
	values, declared := schemaprep.EnumColumnValues(field, database.Enums)
	target := platform.NormalizeDialect(dialect)
	if declared || len(values) == 0 || schemaprep.InlineEnumDialect(target) || target == platform.YDB {
		return nil
	}
	remedy := "type the column with a type " + target + " has"
	if platform.IsPostgresFamily(target) {
		remedy = "declare a named enum with these values (//ptah:schema:enum in Go, an enums entry in YAML, " +
			"an enum block in HCL) and type the column with its name"
	}
	tableName := field.StructName
	if table := tableByStructName(database.Tables, field.StructName); table != nil {
		tableName = table.QualifiedName()
	}
	return &ptaherr.RenderError{
		Dialect: dialect,
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf(
			"column %q of table %q is typed ENUM with the values %s and names no declared enum, "+
				"which %s cannot create: %s",
			field.Name, tableName, quotedValueList(values), target, remedy,
		),
	}
}

// quotedValueList writes values as a parenthesized list of quoted strings.
func quotedValueList(values []string) string {
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = strconv.Quote(value)
	}
	return "(" + strings.Join(quoted, ", ") + ")"
}

// validateUniqueExpression refuses a column declaring uniqueness over an
// expression.
//
// `unique_expr` is an attribute `internal/annotationmeta` documents, and it
// reached no renderer. Measured on PostgreSQL: a column carrying only
// `unique_expr` rendered with no uniqueness at all, and one carrying `unique`
// beside it rendered `UNIQUE` on the raw column -- uniqueness over `s` where
// the author asked for uniqueness over `lower(s)`. A different constraint,
// silently, is worse than a missing one, because the schema looks applied.
//
// Refusing is the answer rather than rendering, because rendering it means an
// expression index the comparison then has to recognize as the column's
// uniqueness, on ten targets that spell expression keys differently. That is a
// feature, and it is not one this refusal should quietly half-implement.
func validateUniqueExpression(dialect string, field schemamodel.Field) error {
	if strings.TrimSpace(field.UniqueExpr) == "" {
		return nil
	}
	return &ptaherr.RenderError{
		Dialect: dialect,
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf(
			"column %q declares unique_expr %q, and no target renders it: "+
				"uniqueness over an expression is not implemented, and rendering the "+
				"column's own UNIQUE instead would enforce a different constraint",
			field.Name,
			strings.TrimSpace(field.UniqueExpr),
		),
	}
}
