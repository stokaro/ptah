// Package schemaexport holds the schema-shaping logic shared by the API-schema
// exporters (OpenAPI and GraphQL): which tables to emit, how to resolve a
// table's columns, primary key, foreign keys and enum values, and the identifier
// helpers both formats need. Keeping this in one place means the two exporters
// agree on the schema they describe and differ only in how they render it.
package schemaexport

import (
	"fmt"
	"regexp"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
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

// Target names an export format. It is what makes a per-target name resolvable,
// and it doubles as the word every message about that target uses, so the
// refusals cannot drift from the thing being refused.
type Target string

// The three export targets that publish names.
const (
	TargetOpenAPI  Target = "OpenAPI"
	TargetGraphQL  Target = "GraphQL"
	TargetProtobuf Target = "Protobuf"
)

// nameIn returns the declaration that applies to this target, or "" when none
// does. An unknown Target resolves to nothing rather than guessing, so a target
// added without a name field falls back to the shared one instead of silently
// reading another target's.
func (t Target) nameIn(names goschema.TargetNames) string {
	switch t {
	case TargetOpenAPI:
		return names.OpenAPI
	case TargetGraphQL:
		return names.GraphQL
	case TargetProtobuf:
		return names.Protobuf
	default:
		return ""
	}
}

// attribute names this target's own name attribute, or "" for a target that has
// none.
func (t Target) attribute() string {
	switch t {
	case TargetOpenAPI:
		return "openapi_name"
	case TargetGraphQL:
		return "graphql_name"
	case TargetProtobuf:
		return "proto_name"
	default:
		return ""
	}
}

// collisionAdvice names the attributes that would resolve a collision, shared
// one first.
//
// Both are named because either can be the fix and the message cannot tell
// which: two api_name declarations that collide in every export want the shared
// attribute changed, while a collision only this target has wants the scoped
// one. Naming only the scoped attribute would send a reader who declared
// neither to a line that does not exist in their schema.
func collisionAdvice(target Target) string {
	if scoped := target.attribute(); scoped != "" {
		return "api_name, or a distinct " + scoped + " for this export only"
	}
	return "api_name"
}

// FieldAPIName returns the name a field carries in the named export.
//
// Three declarations answer in order -- the target's own name, the shared API
// name, the column name -- so the ordinary schema declares nothing and exports
// byte-identically, a shared alias answers every target, and a per-target name
// exists only where a format's naming rules make the shared one unusable
// (stokaro/ptah#905).
func FieldAPIName(field goschema.Field, target Target) string {
	if name := target.nameIn(field.APINames); name != "" {
		return name
	}
	if field.APIName != "" {
		return field.APIName
	}
	return field.Name
}

// ValidateFieldAPINames refuses a table whose fields do not resolve to distinct
// API names, naming both columns that claimed the same one.
//
// The refusal is the point of allowing the two identities to differ at all: an
// alias that silently shadows another field would drop a column from the
// exported schema, and the reader of that schema has nothing left to notice it
// with. Both sources are named because the alias is as likely to be the
// mistake as the column it collided with.
func ValidateFieldAPINames(table goschema.Table, fields []goschema.Field, target Target) error {
	claimed := make(map[string]string, len(fields))
	for _, field := range fields {
		api := FieldAPIName(field, target)
		first, taken := claimed[api]
		if taken {
			return fmt.Errorf(
				"table %q exports two columns as %q in %s: %q and %q; give one of them a distinct %s",
				table.Name, api, target, first, field.Name, collisionAdvice(target),
			)
		}
		claimed[api] = field.Name
	}
	return nil
}

// TableAPIName returns the name a table is exported under.
//
// A table that declares no API name keeps its table name, so an unannotated
// schema exports byte-identically. This is the table-level half of what
// [FieldAPIName] does for a column (stokaro/ptah#905).
func TableAPIName(table goschema.Table, target Target) string {
	if name := target.nameIn(table.APINames); name != "" {
		return name
	}
	if table.APIName != "" {
		return table.APIName
	}
	return table.Name
}

// ValidateTableAPINames refuses a schema whose tables do not resolve to
// distinct API names, naming both tables that claimed the same one.
//
// Two tables published under one name means one of them is absent from the
// exported schema, exactly as with a field, and the reader of that schema has
// nothing left to notice it with.
func ValidateTableAPINames(tables []goschema.Table, target Target) error {
	claimed := make(map[string]string, len(tables))
	for _, table := range tables {
		api := TableAPIName(table, target)
		first, taken := claimed[api]
		if taken {
			return fmt.Errorf(
				"two tables export as %q in %s: %q and %q; give one of them a distinct %s",
				api, target, first, table.Name, collisionAdvice(target),
			)
		}
		claimed[api] = table.Name
	}
	return nil
}

// FieldAPIType returns the type an exporter should project a field as.
//
// A field that declares no override keeps its column type, so an unannotated
// schema exports exactly as before (stokaro/ptah#905).
func FieldAPIType(field goschema.Field) string {
	if field.APIType != "" {
		return field.APIType
	}
	return field.Type
}

// UnknownAPITypeError reports an explicit type override an exporter's mapping
// does not recognize.
//
// It is an error rather than the warning an unrecognized COLUMN type gets, and
// the difference is the whole reason it exists. An unmapped column type is a
// fact about the schema, and defaulting it to a string while saying so is the
// most a projection can do. An unmapped override is something the author typed
// on purpose and the exporter cannot honor -- projecting it as a string would
// answer a request nobody made and hide that the declaration did nothing.
func UnknownAPITypeError(table goschema.Table, field goschema.Field, target Target) error {
	return fmt.Errorf(
		"column %q on table %q declares api_type %q, which the %s projection does not recognize; "+
			"name a type Ptah maps, or drop the override to keep the column's own type %q",
		field.Name, table.Name, field.APIType, target, field.Type,
	)
}

// ProjectedField returns field as an exporter should read it, with the API type
// substituted for the column type.
//
// Substituting once at the top is what keeps the answer single. The type is
// read again further down for array detection, enum resolution and
// diagnostics, and an override honored by the mapping but not by those would
// project a DECIMAL as text while still resolving it as a numeric.
//
// An override also drops the inline enum values, and it has to: those describe
// the stored column, and enum resolution consults them before the type. Left in
// place they answer first, and a column declaring api_type="TEXT" exports as an
// enum anyway -- the declaration doing nothing, silently, which is the one
// outcome this whole annotation exists to rule out. An override that names a
// declared enum still resolves, through the enum index, on the type.
func ProjectedField(field goschema.Field) goschema.Field {
	if field.APIType != "" {
		field.Enum = nil
	}
	field.Type = FieldAPIType(field)
	return field
}

// RefuseUnknownAPIType reports an explicit override the exporter can neither map
// nor resolve as an enum, and reports nothing for a field that declares none.
// The mapping is supplied by the caller because each target has its own, and is
// asked about the projected type.
//
// The enum arm is why this is one function rather than a comparison repeated in
// each exporter: a target's type mapping only knows scalars, so asking it alone
// would refuse api_type="invoice_state" -- a projection all three targets can in
// fact produce, and the natural way to publish a column stored as text on a
// dialect with no native enum.
func RefuseUnknownAPIType(
	table goschema.Table,
	field goschema.Field,
	target Target,
	enums map[string][]string,
	maps func(projectedType string) bool,
) error {
	if field.APIType == "" {
		return nil
	}
	projected := ProjectedField(field)
	if maps(projected.Type) {
		return nil
	}
	if _, ok := ResolveEnumValues(projected, enums); ok {
		return nil
	}
	return UnknownAPITypeError(table, field, target)
}
