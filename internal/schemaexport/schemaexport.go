// Package schemaexport holds the schema-shaping logic shared by the API-schema
// exporters (OpenAPI and GraphQL): which tables to emit, how to resolve a
// table's columns, primary key, foreign keys and enum values, and the identifier
// helpers both formats need. Keeping this in one place means the two exporters
// agree on the schema they describe and differ only in how they render it.
package schemaexport

import (
	"regexp"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
)

// Severity classifies an export diagnostic.
type Severity string

// SeverityWarning reports a lossy or unresolved export detail that did not stop
// the export (for example an enum whose values could not be resolved, emitted as
// a plain string instead).
const SeverityWarning Severity = "warning"

// Diagnostic describes a lossy or unresolved export detail.
type Diagnostic struct {
	Severity Severity
	Path     string
	Message  string
}

// Options controls which tables an exporter emits. Include and Exclude are
// matched against a table's database name. When Include is non-empty only those
// tables are emitted; Exclude is always applied afterward.
type Options struct {
	IncludeTables []string
	ExcludeTables []string
}

// SelectTables returns the tables to export, in schema-definition order, after
// applying the include/exclude filters. A table named in both is excluded.
func SelectTables(db *goschema.Database, opts Options) []goschema.Table {
	include := toSet(opts.IncludeTables)
	exclude := toSet(opts.ExcludeTables)
	var out []goschema.Table
	for _, table := range db.Tables {
		if len(include) > 0 {
			if _, ok := include[table.Name]; !ok {
				continue
			}
		}
		if _, ok := exclude[table.Name]; ok {
			continue
		}
		out = append(out, table)
	}
	return out
}

// FieldsFor returns a table's columns, mirroring the resolution the shipping
// renderers use: fields are grouped by the struct they were parsed from and
// selected by the table's struct name. Embedded columns are already folded into
// this set during parsing, so no extra expansion is needed here.
func FieldsFor(db *goschema.Database, table goschema.Table) []goschema.Field {
	var out []goschema.Field
	for _, field := range db.Fields {
		if field.StructName == table.StructName {
			out = append(out, field)
		}
	}
	return out
}

// EffectivePrimaryKey returns the primary-key column names for a table, taking
// the table-level composite key if present and otherwise the union of fields
// marked primary.
func EffectivePrimaryKey(table goschema.Table, fields []goschema.Field) []string {
	if len(table.PrimaryKey) > 0 {
		return append([]string(nil), table.PrimaryKey...)
	}
	var pk []string
	for _, field := range fields {
		if field.Primary {
			pk = append(pk, field.Name)
		}
	}
	return pk
}

// EnumIndex maps enum type names to their allowed values, for resolving fields
// that reference a named enum type instead of carrying inline values.
func EnumIndex(db *goschema.Database) map[string][]string {
	index := make(map[string][]string, len(db.Enums))
	for _, enum := range db.Enums {
		index[enum.Name] = enum.Values
	}
	return index
}

// ResolveEnumValues returns the allowed values for an enum-typed field. A field
// may carry them inline (field.Enum) or reference a named enum type resolved
// through enums. The second result is false when the field is not an enum or its
// values cannot be resolved, in which case the caller falls back to a plain
// string.
func ResolveEnumValues(field goschema.Field, enums map[string][]string) ([]string, bool) {
	if len(field.Enum) > 0 {
		return field.Enum, true
	}
	if values, ok := enums[strings.TrimSpace(field.Type)]; ok && len(values) > 0 {
		return values, true
	}
	return nil, false
}

// ForeignRef is a parsed foreign-key reference, e.g. "users(id)".
type ForeignRef struct {
	Table  string
	Column string
}

// ParseForeignRef parses a "table(column)" reference. The second result is false
// when the field has no foreign key or the reference is malformed.
func ParseForeignRef(ref string) (ForeignRef, bool) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ForeignRef{}, false
	}
	open := strings.LastIndex(ref, "(")
	closeParen := strings.LastIndex(ref, ")")
	if open < 0 || closeParen < open {
		return ForeignRef{}, false
	}
	table := strings.TrimSpace(ref[:open])
	column := strings.TrimSpace(ref[open+1 : closeParen])
	if table == "" {
		return ForeignRef{}, false
	}
	// A composite reference lists several columns; the relation still points at
	// one table, so keep the first column for single-column relation naming.
	if comma := strings.Index(column, ","); comma >= 0 {
		column = strings.TrimSpace(column[:comma])
	}
	return ForeignRef{Table: table, Column: column}, true
}

// NormalizeType splits a raw column type into an uppercased base name and its
// parenthesized arguments, dropping MySQL column modifiers. Both API-schema type
// maps use it so "VARCHAR(255)", "int unsigned" and "DOUBLE PRECISION" normalize
// the same way regardless of dialect spelling.
func NormalizeType(raw string) (base string, args []string) {
	raw = strings.TrimSpace(raw)
	if open := strings.Index(raw, "("); open >= 0 {
		if closeParen := strings.LastIndex(raw, ")"); closeParen > open {
			for arg := range strings.SplitSeq(raw[open+1:closeParen], ",") {
				if arg = strings.TrimSpace(arg); arg != "" {
					args = append(args, arg)
				}
			}
			raw = raw[:open] + " " + raw[closeParen+1:]
		}
	}
	base = strings.ToUpper(strings.Join(strings.Fields(raw), " "))
	for _, modifier := range []string{" AUTO_INCREMENT", " UNSIGNED", " ZEROFILL"} {
		base = strings.ReplaceAll(base, modifier, "")
	}
	return strings.TrimSpace(base), args
}

var graphQLNamePattern = regexp.MustCompile(`^[_A-Za-z][_0-9A-Za-z]*$`)

// IsValidGraphQLName reports whether s is a legal GraphQL name (type, field or
// enum value). GraphQL forbids names starting with a digit or containing
// punctuation, so enum values like "in-progress" or "2fa" must be rejected and
// handled by the caller (typically by falling back to a scalar).
func IsValidGraphQLName(s string) bool {
	return graphQLNamePattern.MatchString(s)
}

// SanitizeGraphQLName maps an arbitrary identifier to a legal GraphQL name:
// characters outside [_0-9A-Za-z] become "_", a leading digit is prefixed with
// "_", and an empty result becomes "_". Column and table names come from
// annotation strings, so field/type/argument names must pass through this before
// they are emitted or the SDL fails to build.
func SanitizeGraphQLName(s string) string {
	var b strings.Builder
	for i, r := range s {
		switch {
		case r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'):
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			if i == 0 {
				b.WriteByte('_')
			}
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "_"
	}
	return b.String()
}

// ElementType strips one trailing "[]" from a column type, reporting whether the
// type was an array. Ptah renders Postgres array columns (e.g. "TEXT[]"), which
// the API exporters map to array/list schemas rather than scalars.
func ElementType(raw string) (element string, isArray bool) {
	t := strings.TrimSpace(raw)
	if strings.HasSuffix(t, "[]") {
		return strings.TrimSpace(t[:len(t)-2]), true
	}
	return t, false
}

// PascalCase converts a snake_case or kebab-case identifier to PascalCase.
func PascalCase(s string) string {
	parts := strings.FieldsFunc(s, func(r rune) bool { return r == '_' || r == '-' || r == ' ' })
	var b strings.Builder
	for _, part := range parts {
		if part == "" {
			continue
		}
		runes := []rune(part)
		b.WriteString(strings.ToUpper(string(runes[0])))
		if len(runes) > 1 {
			b.WriteString(string(runes[1:]))
		}
	}
	return b.String()
}

// Singularize applies a small set of English pluralization rules good enough for
// table-name-to-type-name derivation. It is intentionally conservative: unknown
// shapes are returned unchanged rather than mangled.
func Singularize(s string) string {
	lower := strings.ToLower(s)
	switch {
	case strings.HasSuffix(lower, "ies") && len(s) > 3:
		return s[:len(s)-3] + "y"
	case strings.HasSuffix(lower, "ses"), strings.HasSuffix(lower, "xes"),
		strings.HasSuffix(lower, "zes"), strings.HasSuffix(lower, "ches"),
		strings.HasSuffix(lower, "shes"):
		return s[:len(s)-2]
	case strings.HasSuffix(lower, "ss"):
		return s // "address" stays "address"
	case strings.HasSuffix(lower, "s") && len(s) > 1:
		return s[:len(s)-1]
	default:
		return s
	}
}

// TypeName derives a GraphQL/OpenAPI type name from a table name: singularized
// and PascalCased, e.g. "simplified_users" -> "SimplifiedUser".
func TypeName(tableName string) string {
	name := PascalCase(Singularize(tableName))
	if name == "" {
		name = PascalCase(tableName)
	}
	return name
}

// RelationFieldName derives a relation field name from a foreign-key column,
// stripping a trailing "_id"/"id" so "author_id" becomes "author". It returns
// false when no sensible name can be derived.
func RelationFieldName(column string) (string, bool) {
	switch {
	case strings.HasSuffix(column, "_id") && len(column) > 3:
		return column[:len(column)-3], true
	case strings.HasSuffix(column, "Id") && len(column) > 2:
		return column[:len(column)-2], true
	default:
		return "", false
	}
}

func toSet(values []string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			set[value] = struct{}{}
		}
	}
	return set
}
