// Package normalize canonicalizes type names, default values, and expressions
// read from live databases so schema comparison is not tripped by cosmetic
// dialect spellings such as PostgreSQL typecasts.
package normalize

import (
	"strings"
)

// Type normalizes database type names for cross-platform comparison.
//
// This function converts database-specific type names to standardized forms that can be
// compared across different database systems. It handles the variations in type naming
// conventions between PostgreSQL, MySQL, MariaDB, and other databases.
//
// # Type Normalization Rules
//
//   - VARCHAR variations (VARCHAR, VARCHAR2, etc.) → "varchar"
//   - Integer variations (INT, INTEGER, BIGINT, etc.) → "integer"
//   - SERIAL types (SERIAL, BIGSERIAL) → "integer" (for comparison purposes)
//   - Boolean variations (BOOL, BOOLEAN, TINYINT(1)) → "boolean"
//   - Timestamp variations → "timestamp", and with a time zone → "timestamptz"
//   - Decimal variations (DECIMAL, NUMERIC) → "decimal"
//
// Every rule reads the type's whole NAME. A parameter list is not part of the name,
// so a token inside one names nothing here: VECTOR(512, INT8) is a vector. Nor does
// a word inside a name: INTERVAL is no integer.
//
// # Database-Specific Handling
//
//   - **MySQL/MariaDB**: TINYINT and TINYINT(1) are treated as BOOLEAN
//   - **PostgreSQL**: SERIAL types are normalized to INTEGER for comparison
//   - **Cross-platform**: Case-insensitive comparison with lowercase normalization
//
// # Example Usage
//
//	// These all normalize to "varchar"
//	Type("VARCHAR(255)")  // → "varchar"
//	Type("varchar(100)")  // → "varchar"
//	Type("VARCHAR2")      // → "varchar"
//
//	// These all normalize to "boolean"
//	Type("BOOLEAN")       // → "boolean"
//	Type("TINYINT(1)")    // → "boolean"
//	Type("BOOL")          // → "boolean"
//
//	// A parameter that spells a type is still a parameter
//	Type("INT8")               // → "integer"
//	Type("VECTOR(512, INT8)")  // → "vector(512, int8)"
//
// # Parameters
//
//   - typeName: The database-specific type name to normalize
//
// # Return Value
//
// Returns a normalized type name suitable for cross-database comparison.
func Type(typeName string) string {
	// Convert to lowercase for case-insensitive comparison
	typeName = strings.ToLower(typeName)

	// An array is not its element type, whatever the element folds to: a column
	// that goes from `int` to `int[]` is a change. PostgreSQL keeps no dimensions
	// or bounds in the catalog, so `int[]`, `int[][]`, `int[3]` and `int ARRAY` are
	// one type there and read back as `integer[]`. The element folds as a column of
	// that type does, which makes `int4[]` and `integer[]` one name as well.
	if element, isArray := ArrayElement(typeName); isArray {
		return Type(element) + "[]"
	}

	switch {
	// PostgreSQL's own names for types the renderer writes differently, folded
	// onto one spelling so a round trip through Ptah's own output converges.
	//
	// The catalog answers `bpchar`, `time`, `timetz` and `varbit`; a document
	// carries `char(8)`, `time without time zone`, `time with time zone` and
	// `bit varying(8)`. Without this, `schema inspect > out.hcl` followed by
	// `apply --to file://out.hcl` planned `ALTER COLUMN ... TYPE` for four
	// columns nobody had touched, on every run (stokaro/ptah#2035).
	//
	// The SIZE is kept rather than stripped, unlike the varchar arm below. A
	// `bit varying` width change is invisible to typechange -- it has no bit
	// kind -- so folding the width away here would be the only place that
	// question is asked, and it would answer "no change" to a real narrowing.
	case isAliasedType(typeName):
		return foldAliasedType(typeName)

	// PostgreSQL's own names for the two floating-point types, folded to the
	// spelling the renderer writes and a declaration uses.
	//
	// The catalog answers `float8` and `float4`; the renderer writes `double
	// precision` and `real` into the document it produces. Without this,
	// `schema inspect > out.hcl` followed by `apply --to file://out.hcl`
	// planned `ALTER COLUMN "x" TYPE double precision` for an unchanged column
	// -- on PostgreSQL a full table rewrite under an ACCESS EXCLUSIVE lock --
	// and planned it again on the next run, forever (stokaro/ptah#2027).
	//
	// The two are folded SEPARATELY, because they are different types: an
	// 8-byte column narrowed to 4 bytes loses precision, and a rule that folded
	// both to one token would report that as no change at all.
	//
	// Only these two spellings are folded, and never a bare `float`. `FLOAT`
	// means double precision on PostgreSQL and 4 bytes on MySQL, and `REAL`
	// means DOUBLE on MySQL unless REAL_AS_FLOAT is set -- so a cross-dialect
	// rule for either would be wrong on one engine. No engine Ptah targets
	// spells a type `float8`, which is what makes this fold narrow.
	case isFloatAlias(typeName, "float8"):
		return "double precision"
	case isFloatAlias(typeName, "float4"):
		return "real"
	default:
		return typeFamily(typeName)
	}
}

// typeFamilies names the family each type name belongs to. A width is asked about
// separately, by typechange, so `int` and `bigint` are one family here. The width
// of a string or a decimal is left out the same way.
//
// The whole name has to match. Recognizing one by a substring is how `interval`,
// `point` and `int4range` read as integers and `paint_kind`, an enum, did too, so
// a column changing to one of them planned nothing. Measured on PostgreSQL 18 and
// the pinned community binary, which writes the ALTER for each.
var typeFamilies = map[string]string{
	"varchar": "varchar", "varchar2": "varchar", "nvarchar": "varchar", "nvarchar2": "varchar",
	"character varying": "varchar", "char varying": "varchar",
	"text":   "text",
	"serial": "integer", "bigserial": "integer", "smallserial": "integer",
	"serial2": "integer", "serial4": "integer", "serial8": "integer",
	// MySQL and MariaDB store BOOLEAN as TINYINT.
	"tinyint": "boolean", "bool": "boolean", "boolean": "boolean",
	"int": "integer", "integer": "integer", "bigint": "integer", "smallint": "integer", "mediumint": "integer",
	"int2": "integer", "int4": "integer", "int8": "integer", "int64": "integer",
	"timestamp": "timestamp", "timestamp without time zone": "timestamp",
	"timestamp with local time zone": "timestamp",
	// A timestamp with a zone holds an instant and one without holds a wall clock.
	// Changing one to the other changes what every stored value means.
	"timestamptz": "timestamptz", "timestamp with time zone": "timestamptz",
	"decimal": "decimal", "numeric": "decimal",
	"interval": "interval",
}

// typeFamily folds a lowercase type name onto its family, or returns it as it is.
// An argument list is not part of the name, wherever it sits: Oracle writes
// `timestamp(6) with time zone`. `unsigned` is part of the type, since it changes
// what the column holds; `zerofill` implies it.
func typeFamily(typeName string) string {
	name, unsigned := nameWithoutArguments(typeName)
	family, known := typeFamilies[name]
	switch {
	case !known:
		return typeName
	case unsigned:
		return family + " unsigned"
	default:
		return family
	}
}

// nameWithoutArguments is typeName with its argument list and its unsigned and
// zerofill words taken out, and whether it had either word.
func nameWithoutArguments(typeName string) (name string, unsigned bool) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(typeName), "pg_catalog.")
	if open := strings.Index(trimmed, "("); open >= 0 {
		if end := strings.LastIndex(trimmed, ")"); end > open {
			trimmed = trimmed[:open] + " " + trimmed[end+1:]
		}
	}
	words := strings.Fields(trimmed)
	kept := words[:0]
	for _, word := range words {
		if word == "unsigned" || word == "zerofill" {
			unsigned = true
			continue
		}
		kept = append(kept, word)
	}
	return strings.Join(kept, " "), unsigned
}

// ArrayElement is the element type of an array type name and true, or typeName and
// false. It reads the spellings PostgreSQL takes: any number of dimensions, with or
// without a bound, and the SQL standard's ARRAY word.
func ArrayElement(typeName string) (element string, isArray bool) {
	element = strings.TrimSpace(typeName)
	for strings.HasSuffix(element, "]") {
		open := strings.LastIndex(element, "[")
		if open < 0 || strings.Trim(element[open+1:len(element)-1], " 0123456789") != "" {
			break
		}
		element, isArray = strings.TrimSpace(element[:open]), true
	}
	if at := len(element) - len(" array"); at > 0 && strings.EqualFold(element[at:], " array") {
		element, isArray = strings.TrimSpace(element[:at]), true
	}
	if !isArray {
		return typeName, false
	}
	return element, true
}

// DefaultValue normalizes default values for cross-database comparison.
//
// This function handles the variations in how different database systems represent
// default values, ensuring that semantically equivalent defaults are recognized
// as identical during schema comparison.
//
// # Normalization Rules
//
//   - Empty/NULL values: Converted to empty string for consistent comparison
//   - Quoted values: Quotes are removed for comparison (both single and double)
//   - PostgreSQL type casting: Removes a ::type cast that cannot change the
//     value (e.g., 'user'::text → 'user'), and keeps one that can, such as
//     (1.5)::integer
//   - Boolean values: MySQL/MariaDB '1'/'0' normalized to 'true'/'false'
//   - NULL literals: Database-specific NULL representations normalized to empty string
//
// # Database-Specific Handling
//
//   - **MySQL/MariaDB**: Returns 'NULL' string for columns without explicit defaults
//   - **PostgreSQL**: Returns actual NULL for columns without defaults, includes type casting
//   - **Boolean types**: Handles '1'/'0' vs 'true'/'false' variations
//   - **PostgreSQL type casting**: Strips a value-preserving ::type cast from
//     default values
//
// # Example Usage
//
//	// Boolean normalization
//	DefaultValue("1", "boolean")     // → "true"
//	DefaultValue("0", "boolean")     // → "false"
//	DefaultValue("true", "boolean")  // → "true"
//
//	// Quote removal
//	DefaultValue("'hello'", "varchar")  // → "hello"
//	DefaultValue("\"world\"", "text")   // → "world"
//
//	// PostgreSQL type casting removal
//	DefaultValue("'user'::text", "text")     // → "user"
//	DefaultValue("'0'::bigint", "integer")   // → "0"
//	DefaultValue("'active'::text", "text")   // → "active"
//
//	// A cast that changes the value stays
//	DefaultValue("(1.5)::integer", "decimal") // → "1.5::integer"
//
//	// NULL handling
//	DefaultValue("NULL", "varchar")     // → ""
//	DefaultValue("", "integer")        // → ""
//
// # Parameters
//
//   - defaultValue: The raw default value from database introspection
//   - typeName: The normalized type name (used for type-specific handling)
//
// # Return Value
//
// Returns a normalized default value suitable for cross-database comparison.
func DefaultValue(defaultValue, typeName string) string {
	if defaultValue == "" {
		return ""
	}

	if normalizedExpression := normalizeTemporalDefaultExpression(defaultValue, typeName); normalizedExpression != "" {
		return normalizedExpression
	}

	if normalizedSequence := normalizeSequenceDefaultExpression(defaultValue); normalizedSequence != "" {
		return normalizedSequence
	}

	// A PostgreSQL catalog types the literal it stores, as 'user'::text or
	// '0'::bigint, and a declaration rarely writes that cast. It is folded
	// before the quotes come off. A cast that can change the value is part of
	// it, so the value is compared as the expression it is.
	cleanValue, kept := withoutValuePreservingCast(defaultValue, typeName)
	if kept {
		return strings.TrimSpace(cleanValue)
	}

	// SQL NULL, including a PostgreSQL cast, is no default. Check before
	// removing quotes so the text 'NULL' remains a different value.
	if strings.EqualFold(strings.TrimSpace(cleanValue), "NULL") {
		return ""
	}

	// Remove surrounding quotes for comparison (both single and double quotes)
	cleanValue = strings.Trim(cleanValue, "'\"")

	// For boolean types, normalize database-specific representations
	if typeName == "boolean" {
		switch strings.ToLower(cleanValue) {
		case "1", "true":
			return "true"
		case "0", "false":
			return "false"
		}
		// If it's not a recognized boolean value, return as-is
		return cleanValue
	}
	if typeName == "decimal" {
		return normalizeDecimalDefaultValue(cleanValue)
	}

	// Return cleaned value for all other types
	return cleanValue
}

func normalizeTemporalDefaultExpression(defaultValue, typeName string) string {
	normalizedType := strings.ToLower(strings.TrimSpace(typeName))
	if normalizedType != "" && normalizedType != "timestamp" && normalizedType != "timestamptz" {
		return ""
	}
	normalizedValue := strings.ToUpper(strings.TrimSpace(defaultValue))
	normalizedValue = strings.TrimSuffix(normalizedValue, "()")
	switch normalizedValue {
	case "CURRENT_TIMESTAMP", "NOW", "LOCALTIME", "LOCALTIMESTAMP":
		return normalizedValue
	default:
		return ""
	}
}

// normalizeSequenceDefaultExpression canonicalizes a nextval(...) column
// default so a declared nextval('seq') matches the nextval('seq'::regclass)
// form PostgreSQL stores and reads back. It returns "" when the value is not a
// nextval call, so the general default handling applies.
//
// The general ::type stripping in DefaultValue cannot handle this case: the
// ::regclass cast is nested inside the call's parentheses, so truncating at the
// last "::" would drop the closing paren and never match the declared form.
func normalizeSequenceDefaultExpression(defaultValue string) string {
	trimmed := strings.TrimSpace(defaultValue)
	if !strings.HasPrefix(strings.ToLower(trimmed), "nextval(") {
		return ""
	}
	// Remove the regclass cast PostgreSQL adds around the sequence name. The
	// bare, quoted, and pg_catalog-qualified spellings are all handled. The
	// sequence name itself is left untouched: PostgreSQL reads the default back
	// with the sequence spelled exactly as declared for the default schema, so a
	// genuine cross-schema or dotted name must not be rewritten.
	for _, cast := range []string{
		`::pg_catalog."regclass"`,
		"::pg_catalog.regclass",
		`::"regclass"`,
		"::regclass",
	} {
		trimmed = strings.ReplaceAll(trimmed, cast, "")
	}
	return trimmed
}

// withoutValuePreservingCast returns value without its last `::type` cast
// where that cast cannot change the value. Otherwise kept reports a cast that
// can change it, and folded is the default in one spelling of that cast: the
// operand without the parentheses PostgreSQL prints around a simple one, and
// the type lowercase, under its family's name, with its length or precision.
// CockroachDB stores `(1.5::integer)` as `1.5::INT8` and PostgreSQL as
// `(1.5)::integer`, and the two read as one default.
//
// A cast cannot change the value where one of these holds:
//
//   - the operand is NULL;
//   - the type is text or character varying without a length, and the column
//     stores characters, so it converts the text as it converts the value
//     before the cast;
//   - the operand is a quoted literal and the type is the column's own,
//     without a length or precision, which is how a catalog types the literal
//     it stores;
//   - the column's type is known and the cast names it, without a length or
//     precision: the server converts a default to the column's type as it
//     stores each row, the same conversion the cast makes, so
//     `ARRAY[]::text[]` on a `text[]` column is `ARRAY[]`, which is how
//     CockroachDB stores it;
//   - the operand is a number and the type a numeric one that holds it
//     exactly, as `(0)::bigint` and `1.5::numeric`.
//
// Every other cast is part of the value: `(1.5)::integer` rounds,
// `'abcd'::character varying(3)` truncates, `(now())::date` drops the time,
// and `'2020-01-01 10:30'::date` does too on a timestamp column. A cast to
// text is one on a column that stores no characters: on a regclass column,
// `'users'::text` looks the table up when a row is inserted, and `'users'`
// when the default is set.
func withoutValuePreservingCast(value, columnType string) (folded string, kept bool) {
	at := strings.LastIndex(value, "::")
	if at < 0 {
		return value, false
	}
	operand := strings.TrimSpace(value[:at])
	castType, isCast := readCastType(value[at+2:])
	if !isCast {
		return value, false
	}
	simple := withoutGroupingParentheses(operand)
	switch {
	case strings.EqualFold(simple, "NULL"):
		return value[:at], false
	case castType.modifier != "":
		return simple + "::" + castType.canonical(), true
	case castType.isText() && holdsCharacters(columnType):
		return simple, false
	case isQuotedDefaultLiteral(operand) && castType.isColumnType(columnType):
		return value[:at], false
	case strings.TrimSpace(columnType) != "" && castType.isColumnType(columnType):
		return simple, false
	case isIntegerLiteral(simple) && exactNumericTypes[castType.name],
		isDecimalLiteral(simple) && (castType.name == "numeric" || castType.name == "decimal"):
		return simple, false
	default:
		return simple + "::" + castType.canonical(), true
	}
}

// exactNumericTypes are the types every integer literal converts to without
// rounding.
var exactNumericTypes = map[string]bool{
	"smallint": true, "integer": true, "int": true, "int2": true, "int4": true,
	"int8": true, "bigint": true, "numeric": true, "decimal": true,
}

// castType is the type that ends a default, after its `::`.
type castType struct {
	// name is the type's name, lowercase and without quotes or a pg_catalog
	// qualifier.
	name string
	// modifier is the length or precision written with it, without spaces,
	// and any words after it, as in `(3)` or `(3) with time zone`.
	modifier string
	// array is the array brackets written after it.
	array string
}

// readCastType reads the text after a default's last `::` as a type. It
// reports false for text that is no type name, as a `::` inside a string or
// in the middle of an expression is followed by.
func readCastType(text string) (castType, bool) {
	text = strings.ToLower(strings.TrimSpace(text))
	var cast castType
	for strings.HasSuffix(text, "]") {
		open := strings.LastIndex(text, "[")
		if open < 0 || strings.Trim(text[open+1:len(text)-1], "0123456789") != "" {
			return castType{}, false
		}
		cast.array = text[open:] + cast.array
		text = strings.TrimSpace(text[:open])
	}
	name, rest, modified := strings.Cut(text, "(")
	if modified {
		arguments, after, closed := strings.Cut(rest, ")")
		if !closed || strings.Trim(arguments, "0123456789, ") != "" || !spellsTypeName(after) {
			return castType{}, false
		}
		cast.modifier = "(" + strings.ReplaceAll(arguments, " ", "") + ")"
		if words := strings.Fields(after); len(words) > 0 {
			cast.modifier += " " + strings.Join(words, " ")
		}
	}
	name = strings.TrimPrefix(strings.ReplaceAll(strings.TrimSpace(name), `"`, ""), "pg_catalog.")
	if name == "" || !spellsTypeName(name) {
		return castType{}, false
	}
	cast.name = strings.Join(strings.Fields(name), " ")
	return cast, true
}

// spellsTypeName reports that text holds only what a type name, with the
// words and dots of a qualified or multi-word name, is spelled with.
func spellsTypeName(text string) bool {
	for _, r := range text {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_', r == ' ', r == '.', r == '"':
		default:
			return false
		}
	}
	return true
}

// canonical spells the cast's type with its family's name, so the spellings
// two catalogs give one type compare equal.
func (cast castType) canonical() string {
	return typeFamily(cast.name) + cast.modifier + cast.array
}

// isText reports that the cast is to text, or to character varying without a
// length.
func (cast castType) isText() bool {
	return cast.array == "" && (cast.name == "text" || cast.name == "character varying" || cast.name == "varchar")
}

// holdsCharacters reports that a column of columnType stores characters. An
// unknown column type is taken to.
func holdsCharacters(columnType string) bool {
	switch unqualifiedTypeFamily(columnType) {
	case "", "text", "varchar", "char", "name", "citext", "string":
		return true
	default:
		return false
	}
}

// isColumnType reports that the cast names the column's own type, compared
// by family and without a schema. An unknown column type is the column's own.
func (cast castType) isColumnType(columnType string) bool {
	if strings.TrimSpace(columnType) == "" {
		return true
	}
	return unqualifiedTypeFamily(cast.name+cast.array) == unqualifiedTypeFamily(columnType)
}

// unqualifiedTypeFamily is typeName's family without a schema, quotes, a
// length or a precision.
func unqualifiedTypeFamily(typeName string) string {
	name := strings.ToLower(strings.ReplaceAll(typeName, `"`, ""))
	element, isArray := ArrayElement(name)
	element, _ = nameWithoutArguments(element)
	if dot := strings.LastIndex(element, "."); dot >= 0 {
		element = element[dot+1:]
	}
	family := Type(element)
	if isArray {
		return family + "[]"
	}
	return family
}

// withoutGroupingParentheses returns operand without the parentheses around
// it where they hold one number, quoted literal, name or function call, as
// PostgreSQL prints `(1.5::integer)` as `(1.5)::integer`. Parentheses around
// anything else group it, and stay.
func withoutGroupingParentheses(operand string) string {
	for len(operand) >= 2 && operand[0] == '(' && closingParenthesis(operand, 0) == len(operand)-1 {
		inner := strings.TrimSpace(operand[1 : len(operand)-1])
		if !isSimpleOperand(inner) {
			break
		}
		operand = inner
	}
	return operand
}

// closingParenthesis is the index of the parenthesis that closes the one at
// open, outside quotes, or -1.
func closingParenthesis(text string, open int) int {
	depth := 0
	var quote byte
	for i := open; i < len(text); i++ {
		switch c := text[i]; {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// isSimpleOperand reports that operand is one term: a signed number, a quoted
// literal, a name, or a function call, with no operator outside parentheses.
func isSimpleOperand(operand string) bool {
	if operand == "" {
		return false
	}
	if isIntegerLiteral(operand) || isDecimalLiteral(operand) || isQuotedDefaultLiteral(operand) {
		return true
	}
	end := strings.IndexByte(operand, '(')
	if end < 0 {
		end = len(operand)
	} else if closingParenthesis(operand, end) != len(operand)-1 {
		return false
	}
	name := operand[:end]
	for _, r := range name {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' && r != '.' {
			return false
		}
	}
	return name != ""
}

// isQuotedDefaultLiteral reports that operand is one quoted literal: a string,
// with an optional E, B or X prefix, or a double-quoted value.
func isQuotedDefaultLiteral(operand string) bool {
	if len(operand) >= 3 && strings.ContainsRune("eEbBxX", rune(operand[0])) && operand[1] == '\'' {
		operand = operand[1:]
	}
	if len(operand) < 2 {
		return false
	}
	quote := operand[0]
	if quote != '\'' && quote != '"' || operand[len(operand)-1] != quote {
		return false
	}
	return !strings.ContainsRune(strings.ReplaceAll(operand[1:len(operand)-1], string([]byte{quote, quote}), ""), rune(quote))
}

// isIntegerLiteral reports that operand is a whole number, with an optional
// sign.
func isIntegerLiteral(operand string) bool {
	operand = strings.TrimLeft(operand, "+-")
	return operand != "" && isDecimalDigits(operand)
}

// isDecimalLiteral reports that operand is a number with a fractional part,
// with an optional sign.
func isDecimalLiteral(operand string) bool {
	whole, fraction, found := strings.Cut(strings.TrimLeft(operand, "+-"), ".")
	return found && (whole != "" || fraction != "") && (whole == "" || isDecimalDigits(whole)) &&
		(fraction == "" || isDecimalDigits(fraction))
}

func normalizeDecimalDefaultValue(value string) string {
	value = strings.TrimSpace(value)
	sign := ""
	if after, ok := strings.CutPrefix(value, "-"); ok {
		sign = "-"
		value = after
	} else if after, ok := strings.CutPrefix(value, "+"); ok {
		value = after
	}
	if value == "" || strings.Count(value, ".") > 1 {
		return sign + value
	}
	parts := strings.Split(value, ".")
	for _, part := range parts {
		if !isDecimalDigits(part) {
			return sign + value
		}
	}
	intPart := strings.TrimLeft(parts[0], "0")
	if intPart == "" {
		intPart = "0"
	}
	if len(parts) == 1 {
		if intPart == "0" {
			return intPart
		}
		return sign + intPart
	}
	fracPart := strings.TrimRight(parts[1], "0")
	if fracPart == "" {
		if intPart == "0" {
			return intPart
		}
		return sign + intPart
	}
	return sign + intPart + "." + fracPart
}

func isDecimalDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func IsDefaultExpr(value string) bool {
	cleanValue := strings.Trim(value, " \t\n\r")

	if strings.HasPrefix(cleanValue, `"`) && strings.HasSuffix(cleanValue, `"`) {
		return false
	}
	if strings.HasPrefix(cleanValue, "'") && strings.HasSuffix(cleanValue, "'") {
		return false
	}
	return true
}

// Expression normalizes SQL expressions for schema comparison.
func Expression(value string) string {
	cleanValue := strings.TrimSpace(value)
	for hasRedundantOuterParentheses(cleanValue) {
		cleanValue = strings.TrimSpace(cleanValue[1 : len(cleanValue)-1])
	}
	return cleanValue
}

func hasRedundantOuterParentheses(value string) bool {
	if len(value) < 2 || value[0] != '(' || value[len(value)-1] != ')' {
		return false
	}

	depth := 0
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '\'', '"':
			next, ok := skipQuotedSQL(value, i, value[i])
			if !ok {
				return false
			}
			i = next
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return false
			}
			if depth == 0 && i != len(value)-1 {
				return false
			}
		}
	}

	return depth == 0
}

func skipQuotedSQL(value string, start int, quote byte) (int, bool) {
	for i := start + 1; i < len(value); i++ {
		if value[i] != quote {
			continue
		}
		if i+1 < len(value) && value[i+1] == quote {
			i++
			continue
		}
		return i, true
	}
	return 0, false
}

// typeAliases folds a base type name -- the name with any size stripped -- onto
// the spelling both sides of a comparison will carry.
//
// Every pair is one type under two names. `char` and `bpchar` are what
// PostgreSQL calls `character`; `varbit` is what it calls `bit varying`. The
// two `time` rows are deliberately separate: a time WITH a zone and one without
// are different types, and one token for both would report a change between
// them as no change at all.
//
// No engine Ptah targets spells a different type with any of these names, which
// is what keeps the fold narrow.
var typeAliases = map[string]string{
	"bpchar":                 "char",
	"character":              "char",
	"char":                   "char",
	"time":                   "time",
	"time without time zone": "time",
	"timetz":                 "timetz",
	"time with time zone":    "timetz",
	"varbit":                 "bit varying",
	"bit varying":            "bit varying",
	"bit":                    "bit",
}

// isAliasedType reports whether a name is one of the aliased spellings.
func isAliasedType(typeName string) bool {
	_, aliased := typeAliases[baseTypeName(typeName)]
	return aliased
}

// foldAliasedType returns the folded spelling with the original size, so a
// width change is still a difference.
func foldAliasedType(typeName string) string {
	base, size := splitTypeSize(typeName)
	return typeAliases[base] + size
}

// baseTypeName is a type name with its size stripped and its schema
// qualification removed.
func baseTypeName(typeName string) string {
	base, _ := splitTypeSize(typeName)
	return base
}

// splitTypeSize separates `char(8)` into `char` and `(8)`.
//
// The size is returned rather than parsed, because it is put back verbatim: a
// caller comparing two normalized names needs them to differ when the widths
// do, and reformatting one side would be a second place for the two to come
// apart.
func splitTypeSize(typeName string) (base, size string) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(typeName), "pg_catalog.")
	open := strings.Index(trimmed, "(")
	if open < 0 {
		return strings.TrimSpace(trimmed), ""
	}
	return strings.TrimSpace(trimmed[:open]), strings.TrimSpace(trimmed[open:])
}

// isFloatAlias reports whether a type name IS one of PostgreSQL's internal
// float names, with the catalog's optional schema qualification stripped.
//
// The whole name has to match: `float8range` is a real PostgreSQL type, and a range
// is not the number it ranges over.
func isFloatAlias(typeName, alias string) bool {
	return strings.TrimPrefix(strings.TrimSpace(typeName), "pg_catalog.") == alias
}

// SQLiteAffinity is the type affinity SQLite derives from a declared type,
// which is the whole of what a declared type MEANS to that engine.
//
// SQLite stores the declaration text verbatim and never resolves it to a type
// of its own, so two spellings are the same type exactly when they yield the
// same affinity. Measured on SQLite 3, storing one value into columns declared
// four ways:
//
//	CREATE TABLE t (a VARCHAR(80), b TEXT, c BOOLEAN, d INTEGER);
//	INSERT INTO t VALUES ('1','1','1','1');
//	SELECT typeof(a), typeof(b), typeof(c), typeof(d);  ->  text|text|integer|integer
//
// The five rules are the ones SQLite documents, in the order it applies them.
// The order is the rule: `INT` is tested before `CHAR`, so a column declared
// `INTCHAR` is INTEGER and not TEXT, and a rule set that tested them the other
// way would disagree with the engine on exactly the names nobody expects.
//
// A name SQLite has never heard of falls through to NUMERIC, which is what
// SQLite does with it. That is the one place this answer is weaker than string
// equality: two unrelated custom type names compare equal, as they do to the
// engine (stokaro/ptah#2040).
func SQLiteAffinity(declaredType string) string {
	upper := strings.ToUpper(strings.TrimSpace(declaredType))
	switch {
	case strings.Contains(upper, "INT"):
		return "INTEGER"
	case strings.Contains(upper, "CHAR"),
		strings.Contains(upper, "CLOB"),
		strings.Contains(upper, "TEXT"):
		return "TEXT"
	case upper == "", strings.Contains(upper, "BLOB"):
		return "BLOB"
	case strings.Contains(upper, "REAL"),
		strings.Contains(upper, "FLOA"),
		strings.Contains(upper, "DOUB"):
		return "REAL"
	default:
		return "NUMERIC"
	}
}

// SQLiteColumnType is the type a DECLARED column produces once SQLite's
// renderer has written it.
//
// It is asked of a declaration and never of a catalog row. SQLite stores the
// declared type text verbatim and applies affinity to it, so the question a
// comparison has to answer is not "are these the same word" but "would
// rendering this declaration produce the type the catalog holds"
// (stokaro/ptah#1662 moved it here so the canonical path asks it too).
func SQLiteColumnType(rawType string) string {
	upper := strings.ToUpper(strings.TrimSpace(rawType))
	base := upper
	if idx := strings.Index(base, "("); idx >= 0 {
		base = strings.TrimSpace(base[:idx])
	}
	switch base {
	case "":
		return "blob"
	case "BOOLEAN", "BOOL":
		return "integer"
	case "SERIAL", "BIGSERIAL", "SMALLSERIAL", "AUTO_INCREMENT":
		return "integer"
	case "VARCHAR", "CHARACTER VARYING", "CHAR", "CHARACTER", "TEXT", "CITEXT", "ENUM":
		return "text"
	case "BYTEA", "BLOB":
		return "blob"
	case "DOUBLE PRECISION":
		return "real"
	default:
		return rawType
	}
}
