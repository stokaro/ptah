// Package defaultlit decides whether a column default already carries SQL
// literal syntax, so that a renderer quotes it exactly once.
//
// `ast.DefaultValue.Value` has two producers that disagree about the form they
// store, and both are legitimate:
//
//   - struct tags keep the tag verbatim, so `default="active"` arrives bare, as
//     `active`, and has to be quoted before it can be rendered;
//   - the SQL parser keeps the literal as it was written, so
//     `DEFAULT 'active'` arrives as `'active'`, already quoted.
//
// Quoting the second form again is not a cosmetic fault. Measured against
// PostgreSQL 18, a varchar default written in a schema file rendered as
//
//	DEFAULT '''x'''
//
// which applies without an error and stores the three-character value `'x'`
// where `x` was meant. The same treatment of a boolean default is refused
// outright, with `invalid input syntax for type boolean: "'t'"`.
package defaultlit

import "strings"

// IsSQLLiteral reports whether s is already written as a single-quoted SQL
// string literal, optionally followed by a cast.
//
// It walks the literal instead of testing its two ends. Testing the ends is
// enough for a plain `'active'`, but it misclassifies both a literal that
// carries a cast, `'{}'::jsonb`, which does not end in a quote, and a bare
// value that merely happens to start with one.
func IsSQLLiteral(s string) bool {
	if len(s) < 2 || s[0] != '\'' {
		return false
	}

	i := 1
	for i < len(s) {
		if s[i] != '\'' {
			i++
			continue
		}
		// A doubled quote is an escaped quote within the literal, so the
		// literal continues past it.
		if i+1 < len(s) && s[i+1] == '\'' {
			i += 2
			continue
		}
		break
	}
	if i >= len(s) {
		// The opening quote was never closed, so this is not a literal that a
		// renderer can pass through untouched.
		return false
	}

	// Everything after the closing quote has to be a cast. The type name is not
	// checked any further than that: it spans spellings as unlike each other as
	// `jsonb`, `character varying`, `numeric(10,2)` and `text[]`, and the only
	// producer of this shape is the SQL parser, which builds it from a literal
	// it has already recognized.
	rest := s[i+1:]
	return rest == "" || (strings.HasPrefix(rest, "::") && len(rest) > 2)
}

// Render returns value ready to follow `DEFAULT`, quoting it with quote only
// when it is not already a literal.
func Render(value string, quote func(string) string) string {
	trimmed := strings.TrimSpace(value)
	if IsSQLLiteral(trimmed) {
		return trimmed
	}
	return quote(value)
}

// Typed returns value ready to follow `DEFAULT` in the form the column's own
// type takes, which for a number or a boolean is no quotes at all, and quotes
// any other value with quote unless it is already a literal.
//
// PostgreSQL forgives the quoted form -- `DEFAULT '7'` on a bigint is cast on
// the way in and reads back as 7 -- and that forgiveness is why every literal
// was quoted for as long as it was. Spanner does not forgive it: measured on
// the PGAdapter emulator v0.55.2, `DEFAULT '7'` on a bigint column is `Error
// parsing the default value ...: Expected type INT64; found STRING`, and
// `DEFAULT 'true'` on a boolean is the same refusal for BOOL. A quoted
// literal is also simply wrong about what it says, on every engine of the
// PostgreSQL family. The decision is made from the column's declared type
// rather than from the dialect.
func Typed(columnType, value string, quote func(string) string) string {
	trimmed := strings.TrimSpace(value)
	if IsSQLLiteral(trimmed) {
		return trimmed
	}
	if bare, unquoted := bareLiteral(columnType, trimmed); unquoted {
		return bare
	}
	return quote(value)
}

// bareLiteral answers with the unquoted spelling of a default whose column
// type takes one, and reports whether the type does.
//
// Anything it does not recognize keeps the quoted form, which is what every
// default got before: a type this does not classify is not a type whose bare
// literal syntax is known, and guessing there would produce DDL the server
// refuses rather than DDL it merely tolerates.
func bareLiteral(columnType, value string) (string, bool) {
	switch base := baseTypeName(columnType); {
	case booleanTypeNames[base]:
		lowered := strings.ToLower(value)
		return lowered, lowered == "true" || lowered == "false"
	case numericTypeNames[base]:
		return value, IsPlainNumber(value)
	default:
		return "", false
	}
}

// baseTypeName strips a size, a precision and any array suffix, so that
// `numeric(18,2)` classifies the same way `numeric` does.
//
// An array type is deliberately NOT reduced to its element: `integer[]` takes a
// default like `{1,2}` or `ARRAY[1,2]`, neither of which is a bare number.
func baseTypeName(columnType string) string {
	base := strings.TrimSpace(strings.ToLower(columnType))
	if open := strings.IndexByte(base, '('); open >= 0 {
		base = strings.TrimSpace(base[:open])
	}
	return base
}

// booleanTypeNames and numericTypeNames are the types whose literals need no
// quotes, in every spelling the PostgreSQL family's engines accept.
var (
	booleanTypeNames = map[string]bool{"bool": true, "boolean": true}
	numericTypeNames = map[string]bool{
		"smallint": true, "integer": true, "int": true, "bigint": true,
		"int2": true, "int4": true, "int8": true,
		"decimal": true, "numeric": true,
		"real": true, "double precision": true, "float": true,
		"float4": true, "float8": true,
	}
)

// IsPlainNumber reports whether value is a decimal number written the way SQL
// writes one.
//
// It is a hand-rolled check rather than strconv.ParseFloat because ParseFloat
// accepts `NaN`, `Inf` and `0x1p-2`, and PostgreSQL takes none of those as a
// bare default -- `NaN` has to be `'NaN'::numeric`. A value this refuses simply
// keeps its quotes.
func IsPlainNumber(value string) bool {
	digits := strings.TrimLeft(value, "+-")
	if digits == "" || digits != value && len(value)-len(digits) > 1 {
		return false
	}
	mantissa, exponent, hasExponent := cutExponent(digits)
	if hasExponent && !isDigits(strings.TrimLeft(exponent, "+-")) {
		return false
	}
	whole, fraction, hasPoint := strings.Cut(mantissa, ".")
	if !hasPoint {
		return isDigits(whole)
	}
	if whole == "" {
		return isDigits(fraction)
	}
	return isDigits(whole) && (fraction == "" || isDigits(fraction))
}

// cutExponent splits a mantissa from an `e`-notation exponent.
func cutExponent(value string) (mantissa, exponent string, found bool) {
	for i := range len(value) {
		if value[i] == 'e' || value[i] == 'E' {
			return value[:i], value[i+1:], true
		}
	}
	return value, "", false
}

// isDigits reports whether every byte is a decimal digit, and that there is at
// least one.
func isDigits(value string) bool {
	if value == "" {
		return false
	}
	for i := range len(value) {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}
