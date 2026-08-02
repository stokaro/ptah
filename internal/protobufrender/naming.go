package protobufrender

import (
	"strings"
	"unicode"

	"go.5x5.cz/ptah/internal/schemaexport"
)

// sanitizeIdent maps an arbitrary source identifier to a legal Protobuf
// identifier, using the same rule as schemaexport.SanitizeGraphQLName so the
// GraphQL and Protobuf targets agree on what a column named "2fa-enabled"
// becomes. It reports whether the input had to be changed, which the caller
// turns into an export diagnostic: a sanitized identifier is legal Protobuf but
// will not satisfy buf lint's naming rules.
func sanitizeIdent(s string) (ident string, changed bool) {
	ident = schemaexport.SanitizeGraphQLName(s)
	return ident, ident != s
}

// isProtoDelimiter mirrors buf's xstrings.isDelimiter.
func isProtoDelimiter(r rune) bool {
	return r == '.' || r == '-' || r == '_' || r == ' ' || r == '\t' || r == '\n' || r == '\r'
}

// isSnakeCaseNewWord mirrors buf's xstrings.isSnakeCaseNewWord with
// newWordOnDigits unset, which is the variant buf lint uses.
func isSnakeCaseNewWord(r rune) bool {
	return unicode.IsUpper(r)
}

// isSnakeCaseNewWordOnDigits mirrors the same helper with newWordOnDigits set.
// buf passes true unconditionally when looking at the *next* rune, whatever the
// option says for the current one.
func isSnakeCaseNewWordOnDigits(r rune) bool {
	return unicode.IsUpper(r) || unicode.IsDigit(r)
}

// bufSnakeCase is a faithful port of buf's xstrings.toSnakeCase with the
// newWordOnDigits option unset, which is how buf lint derives the names it
// expects. Reproducing it exactly matters because ENUM_VALUE_PREFIX builds the
// required prefix by running this over the *generated* enum type name, and
// ENUM_VALUE_UPPER_SNAKE_CASE requires every enum value name to be a fixed
// point of it. snake -> PascalCase -> snake is not the identity across digits:
// "enum_user_2fa_status" becomes "EnumUser2faStatus", which buf maps back to
// "ENUM_USER2FA_STATUS", not "ENUM_USER_2FA_STATUS".
//
// The port indexes s by byte exactly as buf does, so callers must pass an
// already-sanitized ASCII identifier.
func bufSnakeCase(s string) string {
	var output strings.Builder
	s = strings.TrimFunc(s, isProtoDelimiter)
	for i, c := range s {
		if isProtoDelimiter(c) {
			c = '_'
		}
		last := byte(0)
		if output.Len() > 0 {
			last = output.String()[output.Len()-1]
		}
		switch {
		case i == 0:
			output.WriteRune(c)
		case isSnakeCaseNewWord(c) && last != '_' &&
			((i < len(s)-1 && !isSnakeCaseNewWordOnDigits(rune(s[i+1])) && !isProtoDelimiter(rune(s[i+1]))) ||
				unicode.IsLower(rune(s[i-1]))):
			output.WriteByte('_')
			output.WriteRune(c)
		case !isProtoDelimiter(c) || last != '_':
			output.WriteRune(c)
		}
	}
	return output.String()
}

// bufUpperSnakeCase is buf's xstrings.ToUpperSnakeCase.
func bufUpperSnakeCase(s string) string {
	return strings.ToUpper(bufSnakeCase(s))
}

// messageName derives a Protobuf message name from a table name. It reuses
// schemaexport.TypeName so the base derivation matches the GraphQL exporter;
// the two targets deliberately diverge past that point, since GraphQL also
// applies shadow avoidance and numeric-suffix collision resolution that this
// exporter must not (a numeric suffix would depend on table order).
func messageName(table string) (string, bool) {
	return sanitizeIdent(schemaexport.TypeName(table))
}

// qualifiedMessageName prefixes a message name with its schema, used only to
// break a collision between two tables that produce the same bare name.
// PascalCase("") is "", so a table without an explicit schema keeps its bare
// name and adding a schema-qualified twin never silently renames it.
func qualifiedMessageName(schema, table string) (string, bool) {
	base, changed := messageName(table)
	if schema == "" {
		return base, changed
	}
	prefix, prefixChanged := sanitizeIdent(schemaexport.PascalCase(schema))
	return prefix + base, changed || prefixChanged
}

// fieldName derives a lower_snake_case Protobuf field name from a column name.
// The two report flags are deliberately separate: "changed" means characters had
// to be replaced, "lintDirty" means buf lint will report the result. They are not
// the same set - a column "UserID" is sanitized to nothing but lowercased, while
// "_2fa" is sanitized and still not a legal lower_snake_case name.
func fieldName(column string) (name string, changed, lintDirty bool) {
	ident, changed := sanitizeIdent(column)
	name = strings.ToLower(ident)
	return name, changed, strings.ToLower(bufSnakeCase(name)) != name
}

// enumValueName builds a value name for enumType from a source label. The
// mandatory ENUM_NAME_ prefix is derived from the generated enum type name via
// buf's own rule, so the result satisfies ENUM_VALUE_PREFIX. lintDirty reports
// whether the whole identifier is a fixed point of buf's rule, which is what
// ENUM_VALUE_UPPER_SNAKE_CASE actually checks.
func enumValueName(enumType, label string) (name string, changed, lintDirty bool) {
	sanitized, changed := sanitizeIdent(label)
	name = enumValuePrefix(enumType) + bufUpperSnakeCase(sanitized)
	return name, changed, bufUpperSnakeCase(name) != name
}

// enumValuePrefix is the prefix buf lint's ENUM_VALUE_PREFIX requires for every
// value of an enum named enumType.
func enumValuePrefix(enumType string) string {
	return bufUpperSnakeCase(enumType) + "_"
}

// unspecifiedValueName is the mandatory zero value. Editions default to
// features.enum_type = OPEN and protoc rejects an open enum whose first value
// is not zero, so this is a language requirement and not only a style rule.
func unspecifiedValueName(enumType string) string {
	return enumValuePrefix(enumType) + "UNSPECIFIED"
}
