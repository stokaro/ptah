package builtin

import (
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/defaultlit"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/tableref"
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
		if err := validateRelativeTimeDefault(dialect, database, field); err != nil {
			return err
		}
	}
	return nil
}

// validateRelativeTimeDefault refuses a default on a date or time column that
// the target reads as a moment relative to when it reads it, such as
// PostgreSQL's 'now' ([defaultlit.RelativeTimeLiteral]). The parser refuses
// one a CREATE TABLE writes on a built-in type; this refuses one a Go or YAML
// model declares, one an ALTER COLUMN ... SET DEFAULT in a schema file sets,
// which reach the model without the column's type beside them, and one on a
// column typed by a domain ([dateTimeColumnType]). Measured on PostgreSQL 18,
// the server reads the default once, when it creates the table, so every row
// gets that moment and every plan changes the column again.
func validateRelativeTimeDefault(dialect string, database *schemamodel.Database, field schemamodel.Field) error {
	described, ok := platform.Lookup(dialect)
	if !ok {
		return nil
	}
	value := field.DefaultExpr
	if field.Default != "" || field.DefaultSet {
		value = field.Default
		if !defaultlit.IsSQLLiteral(strings.TrimSpace(value)) {
			value = "'" + strings.ReplaceAll(value, "'", "''") + "'"
		}
	}
	literals := described.Grammar.Columns.RelativeTimeLiterals
	if len(literals) == 0 || !defaultlit.RelativeTimeLiteral(value, literals) ||
		!dateTimeColumnType(field.Type, schemaext.DefinedTypes(database.FeatureObjects)) {
		return nil
	}
	tableName := field.StructName
	if table := tableByStructName(database.Tables, field.StructName); table != nil {
		tableName = table.QualifiedName()
	}
	return &ptaherr.RenderError{
		Dialect: dialect,
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf(
			"column %q of table %q: DEFAULT %s on a %s column is read once, when the table is created, so every row "+
				"gets that moment and every plan changes the column again; write a function the server calls for each "+
				"row, such as now() or CURRENT_DATE",
			field.Name, tableName, value, field.Type,
		),
	}
}

// dateTimeColumnType reports whether columnType is a built-in date or time
// type ([defaultlit.DateTimeType]), or a domain the model declares over one,
// directly or through other domains.
//
// Any other name holds no moment as far as the model shows: an enum, a domain
// over text, a type the model does not declare, and a name two domains share.
// A refusal there would turn away a default the server reads as the string it
// is, such as 'today' on an enum that has the value.
func dateTimeColumnType(columnType string, defined []schemaext.DefinedType) bool {
	spelling := columnType
	// Each step names a declared domain, so a walk over a chain ends within
	// as many steps as there are definitions, and a cycle ends with them.
	for range len(defined) + 1 {
		if defaultlit.DateTimeType(spelling) {
			return true
		}
		base, ok := declaredDomainBase(spelling, defined)
		if !ok {
			return false
		}
		spelling = base
	}
	return false
}

// declaredDomainBase returns the base type of the one domain defined declares
// under the name spelling writes, matched without case, and by schema where
// spelling names one. A name no domain or two domains answer to has none.
func declaredDomainBase(spelling string, defined []schemaext.DefinedType) (string, bool) {
	ref, ok := tableref.Parse(spelling)
	if !ok {
		return "", false
	}
	base, matches := "", 0
	for _, definition := range defined {
		if definition.ID.Kind != objectidentity.KindDomain || len(definition.References) == 0 {
			continue
		}
		name, named := tableref.Parse(definition.ID.Name.Source)
		schema, _ := tableref.Parse(definition.ID.Schema.Source)
		if !named || !strings.EqualFold(name.Name, ref.Name) ||
			ref.Qualified && !strings.EqualFold(schema.Name, ref.Schema) {
			continue
		}
		base, matches = definition.References[0], matches+1
	}
	return base, matches == 1
}

// validateUndeclaredEnum refuses a column typed ENUM that carries its values
// and names no declared enum, on a target that cannot build it inline.
//
// The inline-enum dialects build such a column from its values, the way the
// comparison lowers it. The PostgreSQL family has no inline form: its column
// type has to name an enum type, and rendered as it stands the declaration is
// the bare type ENUM, which the server refuses. A type created here for the
// column would be an object no declaration owns, so the column is refused
// with what to declare instead.
func validateUndeclaredEnum(dialect string, database *schemamodel.Database, field schemamodel.Field) error {
	values, declared := schemaprep.EnumColumnValues(field, database.Enums)
	target := platform.NormalizeDialect(dialect)
	if declared || len(values) == 0 || schemaprep.InlineEnumDialect(target) {
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
