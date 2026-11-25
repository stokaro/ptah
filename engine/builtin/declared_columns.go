package builtin

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/mysqlcollation"
	"ptah.run/internal/mysqlenum"
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
		if err := validateRelativeTimeDefault(dialect, database, field); err != nil {
			return err
		}
		if err := validateBareGeneratedKind(dialect, field); err != nil {
			return err
		}
	}
	return validateRelativeTimeDomainDefaults(dialect, database)
}

// validateBareGeneratedKind refuses a generated kind declared without the
// expression that computes the column, where the target's grammar does not let
// the kind stand alone ([descriptor.ComputedKind.ExpressionOptional]), which
// the DDL parser reads too. ClickHouse's EPHEMERAL does, and the renderer
// writes the expression ClickHouse reports for it. Every renderer writes a
// generated clause only for an expression, so any other kind without one
// would render a plain column.
func validateBareGeneratedKind(dialect string, field schemamodel.Field) error {
	kind := strings.ToUpper(strings.TrimSpace(field.GeneratedKind))
	if kind == "" || strings.TrimSpace(field.GeneratedExpression) != "" {
		return nil
	}
	described, _ := platform.Lookup(dialect)
	if slices.ContainsFunc(described.Grammar.Columns.ComputedKinds, func(computed descriptor.ComputedKind) bool {
		return computed.Keyword == kind && computed.ExpressionOptional
	}) {
		return nil
	}
	return &ptaherr.RenderError{
		Dialect: dialect,
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf(
			"column %q declares generated kind %s without a generated expression, and %s takes that kind only with one",
			field.Name, kind, dialect,
		),
	}
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

// validateEnumTrailingSpaces refuses an enum value or a SET member that ends
// in a space, on a target whose server removes the space when it creates the
// column ([descriptor.InlineEnum.TrailingSpacesRemoved]).
//
// The column built from 'ok ' holds 'ok', so every plan finds the declared
// value missing from the column and proposes the same MODIFY COLUMN again. A
// value of spaces alone is stored as the empty value, which a read of the
// column leaves out, so it is proposed again too. Refused before a plan
// exists, the author declares the value the server keeps. A named enum's
// values reach the column the same way, so they are read too, and so do the
// members of a SET type, which no reader states as values.
//
// A column of the binary character set keeps the space, so it is not refused
// ([mysqlcollation.Binary]). The check is a rule about a declaration, so the
// validation of a schema runs it and the render does not: a read verb renders
// what a server holds, and a binary column holding 'ok ' is one of those.
func validateEnumTrailingSpaces(dialect string, database schemamodel.Database) error {
	described, _ := platform.Lookup(dialect)
	if !described.InlineEnum.TrailingSpacesRemoved {
		return nil
	}
	for _, field := range database.Fields {
		var table schemamodel.Table
		if owner := tableByStructName(database.Tables, field.StructName); owner != nil {
			table = *owner
		}
		binary, err := mysqlcollation.Binary(dialect, table, field)
		if err != nil {
			return err
		}
		if binary {
			continue
		}
		// An HCL document states the values in the column's type text, as
		// enum("happy", "ok "), and the Go and YAML readers state them as
		// values.
		values := append(schemaprep.InlineEnumValues(field, database.Enums, dialect), mysqlenum.Values(field.Type)...)
		if err := refuseTrailingSpace(dialect, &database, field, "enum value", "an ENUM value", values); err != nil {
			return err
		}
		if err := refuseTrailingSpace(dialect, &database, field, "SET member", "a SET member", mysqlenum.SetValues(field.Type)); err != nil {
			return err
		}
	}
	return nil
}

// refuseTrailingSpace refuses the first of values, each of them a what of the
// column field, that ends in a space, which the server removes from each one
// of kind.
func refuseTrailingSpace(dialect string, database *schemamodel.Database, field schemamodel.Field, what, kind string,
	values []string,
) error {
	for _, value := range values {
		if !strings.HasSuffix(value, " ") {
			continue
		}
		remedy := "declare a value that does not end in a space"
		if kept := strings.TrimRight(value, " "); kept != "" {
			remedy = "declare " + strconv.Quote(kept)
		}
		tableName := field.StructName
		if table := tableByStructName(database.Tables, field.StructName); table != nil {
			tableName = table.QualifiedName()
		}
		target := platform.NormalizeDialect(dialect)
		return &ptaherr.RenderError{
			Dialect: dialect,
			Err:     ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf(
				"column %q of table %q declares the %s %q, and %s removes the trailing spaces of %s, "+
					"so the column would never match its declaration: %s",
				field.Name, tableName, what, value, target, kind, remedy,
			),
		}
	}
	return nil
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
