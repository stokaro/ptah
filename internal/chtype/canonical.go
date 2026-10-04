package chtype

import (
	"slices"
	"strconv"
	"strings"
)

// Canonical spells a ClickHouse type expression the way the server reports it
// in system.columns, so that two spellings of one type compare equal and two
// types never do.
//
// Every rewrite below was measured on 24.10.4 and 26.9.8, which agree on all of
// them: the server stores INT as Int32, Decimal32(2) as Decimal(9, 2), Boolean
// as Bool, a bare DateTime64 as DateTime64(3), Enum('a', 'b') as
// Enum8('a' = 1, 'b' = 2), and `Map(String,UInt64)` with a space after the
// comma. A width is never folded: Int32 and Int64, Enum8 and Enum16, and
// Decimal(9, 2) and Decimal(18, 2) stay apart. Whitespace is dropped except
// between two words, and quoted text is compared by its value, so 'it”s' and
// 'it\'s' are one label.
//
// The result is a comparison key, not a type to write: a tuple element named
// date reads as Date on both sides, which keeps the two sides equal and is not
// what the server would print.
func Canonical(t string) string {
	tokens := resolveAliases(scan(t))
	var out []token
	for i := 0; i < len(tokens); i++ {
		rewritten, next := rewrite(tokens, i)
		out = append(out, rewritten...)
		i = next
	}
	return join(out)
}

// AdmitsNull reports whether a column of type t can hold NULL: the type is
// Nullable(T), or LowCardinality(Nullable(T)), the only shape ClickHouse
// accepts for a nullable low-cardinality column.
//
// A Nullable inside an array, a map or a tuple makes the elements nullable and
// not the column, and ClickHouse refuses a Nullable around any of those three.
// Measured on 26.9.8: `MODIFY COLUMN b Nullable(Array(Nullable(Int32)))` fails
// with "Nested type Array(Nullable(Int32)) cannot be inside Nullable type", and
// the same for LowCardinality(Nullable(String)).
//
// The parser, the reader and the renderer each ask this, and they have to
// agree. When the parser called LowCardinality(Nullable(String)) nullable and
// the reader did not, a declaration identical to the live table planned a
// MODIFY COLUMN that wrapped the type in a second Nullable, which the server
// refuses (stokaro/ptah#4105).
func AdmitsNull(t string) bool {
	tokens := scan(t)
	if isWrapper(tokens, "LowCardinality") {
		tokens = tokens[2 : len(tokens)-1]
	}
	return isWrapper(tokens, "Nullable")
}

// StripNullable returns the type inside an outer Nullable(...), and t itself
// when it has none. A column's nullability is compared on its own, so the type
// comparison asks about what is inside.
func StripNullable(t string) string {
	t = strings.TrimSpace(t)
	if !isWrapper(scan(t), "Nullable") {
		return t
	}
	inner := strings.TrimSpace(t[strings.Index(t, "(")+1:])
	return strings.TrimSpace(inner[:len(inner)-1])
}

// isWrapper reports whether tokens are name(...) as a whole: the parenthesis
// after name closes at the last token.
func isWrapper(tokens []token, name string) bool {
	if len(tokens) < 3 || tokens[0].kind != wordToken || !strings.EqualFold(tokens[0].text, name) ||
		!tokens[1].is("(") {
		return false
	}
	return closingParen(tokens, 1) == len(tokens)-1
}

type tokenKind int

const (
	wordToken tokenKind = iota
	numberToken
	stringToken // single-quoted; text holds the value with the quotes removed
	identToken  // backquoted or double-quoted; text holds it as written
	punctToken
)

type token struct {
	kind tokenKind
	text string
}

func (t token) is(punct string) bool {
	return t.kind == punctToken && t.text == punct
}

// scan splits a type expression into words, numbers, quoted text and
// punctuation, dropping whitespace.
func scan(t string) []token {
	var tokens []token
	for i := 0; i < len(t); {
		ch := t[i]
		switch {
		case ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r':
			i++
		case ch == '\'':
			value, next := scanQuoted(t, i)
			tokens = append(tokens, token{kind: stringToken, text: value})
			i = next
		case ch == '`' || ch == '"':
			end := strings.IndexByte(t[i+1:], ch)
			if end < 0 {
				end = len(t) - i - 1
			}
			tokens = append(tokens, token{kind: identToken, text: t[i : i+end+2]})
			i += end + 2
		case isWordStart(ch):
			start := i
			for i < len(t) && (isWordStart(t[i]) || isDigit(t[i])) {
				i++
			}
			tokens = append(tokens, token{kind: wordToken, text: t[start:i]})
		case isDigit(ch):
			start := i
			for i < len(t) && (isDigit(t[i]) || t[i] == '.') {
				i++
			}
			tokens = append(tokens, token{kind: numberToken, text: t[start:i]})
		default:
			tokens = append(tokens, token{kind: punctToken, text: string(ch)})
			i++
		}
	}
	return tokens
}

// scanQuoted reads the single-quoted string starting at t[start], taking both
// escapes ClickHouse accepts for a quote inside one: a backslash and a doubled
// quote.
func scanQuoted(t string, start int) (value string, next int) {
	var b strings.Builder
	for i := start + 1; i < len(t); i++ {
		switch {
		case t[i] == '\\' && i+1 < len(t):
			b.WriteByte(t[i+1])
			i++
		case t[i] == '\'' && i+1 < len(t) && t[i+1] == '\'':
			b.WriteByte('\'')
			i++
		case t[i] == '\'':
			return b.String(), i + 1
		default:
			b.WriteByte(t[i])
		}
	}
	return b.String(), len(t)
}

func isWordStart(ch byte) bool {
	return ch == '_' || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

// aliases are ClickHouse's own type aliases, as system.data_type_families
// lists them on 24.10.4 and 26.9.8 (alias_to). Every one is case-insensitive.
// TIME and GEOMETRY are left out: 24.10 reads them as aliases of Int64 and
// String, and 26.9 as types of their own.
var aliases = map[string]string{
	"BOOL": "Bool", "BOOLEAN": "Bool",
	"TIMESTAMP": "DateTime",
	"DEC":       "Decimal", "FIXED": "Decimal", "NUMERIC": "Decimal",
	"ENUM":   "Enum",
	"BINARY": "FixedString",
	"FLOAT":  "Float32", "REAL": "Float32", "SINGLE": "Float32",
	"DOUBLE": "Float64", "DOUBLE PRECISION": "Float64",
	"INET4": "IPv4", "INET6": "IPv6",
	"SMALLINT": "Int16", "SMALLINT SIGNED": "Int16",
	"INT": "Int32", "INT SIGNED": "Int32", "INTEGER": "Int32", "INTEGER SIGNED": "Int32",
	"MEDIUMINT": "Int32", "MEDIUMINT SIGNED": "Int32",
	"BIGINT": "Int64", "BIGINT SIGNED": "Int64", "SIGNED": "Int64",
	"BYTE": "Int8", "INT1": "Int8", "INT1 SIGNED": "Int8", "TINYINT": "Int8", "TINYINT SIGNED": "Int8",
	"BINARY LARGE OBJECT": "String", "BINARY VARYING": "String", "BLOB": "String", "BYTEA": "String",
	"CHAR": "String", "CHAR LARGE OBJECT": "String", "CHAR VARYING": "String", "CHARACTER": "String",
	"CHARACTER LARGE OBJECT": "String", "CHARACTER VARYING": "String", "CLOB": "String",
	"LONGBLOB": "String", "LONGTEXT": "String", "MEDIUMBLOB": "String", "MEDIUMTEXT": "String",
	"NATIONAL CHAR": "String", "NATIONAL CHAR VARYING": "String", "NATIONAL CHARACTER": "String",
	"NATIONAL CHARACTER LARGE OBJECT": "String", "NATIONAL CHARACTER VARYING": "String",
	"NCHAR": "String", "NCHAR LARGE OBJECT": "String", "NCHAR VARYING": "String", "NVARCHAR": "String",
	"TEXT": "String", "TINYBLOB": "String", "TINYTEXT": "String", "VARBINARY": "String",
	"VARCHAR": "String", "VARCHAR2": "String",
	"SMALLINT UNSIGNED": "UInt16", "YEAR": "UInt16",
	"INT UNSIGNED": "UInt32", "INTEGER UNSIGNED": "UInt32", "MEDIUMINT UNSIGNED": "UInt32",
	"BIGINT UNSIGNED": "UInt64", "BIT": "UInt64", "SET": "UInt64", "UNSIGNED": "UInt64",
	"INT1 UNSIGNED": "UInt8", "TINYINT UNSIGNED": "UInt8",
}

// caseInsensitiveFamilies are the type families ClickHouse reads in any case
// (system.data_type_families.case_insensitive), keyed by their upper-case
// spelling. Time and Time64 are left out for the reason aliases gives.
var caseInsensitiveFamilies = map[string]string{
	"BOOL": "Bool", "DATE": "Date", "DATE32": "Date32", "DATETIME": "DateTime",
	"DATETIME32": "DateTime32", "DATETIME64": "DateTime64", "DECIMAL": "Decimal",
	"DECIMAL32": "Decimal32", "DECIMAL64": "Decimal64", "DECIMAL128": "Decimal128",
	"DECIMAL256": "Decimal256", "ENUM": "Enum", "JSON": "JSON",
}

// longestAlias is the most words an alias spans.
const longestAlias = 4

// resolveAliases replaces every alias with the family it names, taking the
// longest run of words that is one, so SMALLINT UNSIGNED is UInt16 and not
// Int16 followed by a word. An alias of String drops its length, as the server
// does: VARCHAR(10) is stored as String.
func resolveAliases(tokens []token) []token {
	out := make([]token, 0, len(tokens))
	for i := 0; i < len(tokens); i++ {
		if tokens[i].kind != wordToken {
			out = append(out, tokens[i])
			continue
		}
		resolved, span, alias := resolveWord(tokens, i)
		out = append(out, token{kind: wordToken, text: resolved})
		i += span - 1
		if alias && resolved == "String" && i+1 < len(tokens) && tokens[i+1].is("(") {
			if end := closingParen(tokens, i+1); end > 0 {
				i = end
			}
		}
	}
	return out
}

func resolveWord(tokens []token, start int) (resolved string, span int, alias bool) {
	for n := min(longestAlias, len(tokens)-start); n > 1; n-- {
		words := make([]string, 0, n)
		for _, tok := range tokens[start : start+n] {
			if tok.kind != wordToken {
				break
			}
			words = append(words, strings.ToUpper(tok.text))
		}
		if len(words) < n {
			continue
		}
		if family, ok := aliases[strings.Join(words, " ")]; ok {
			return family, n, true
		}
	}
	upper := strings.ToUpper(tokens[start].text)
	if family, ok := aliases[upper]; ok {
		return family, 1, true
	}
	if family, ok := caseInsensitiveFamilies[upper]; ok {
		return family, 1, false
	}
	return tokens[start].text, 1, false
}

// decimalPrecision is the precision ClickHouse stores a DecimalN(S) with.
var decimalPrecision = map[string]string{
	"Decimal32": "9", "Decimal64": "18", "Decimal128": "38", "Decimal256": "76",
}

// rewrite returns the canonical tokens for the type family at tokens[i] and
// the index of the last token it consumed.
func rewrite(tokens []token, i int) (out []token, last int) {
	tok := tokens[i]
	if tok.kind != wordToken {
		return []token{tok}, i
	}
	hasArgs := i+1 < len(tokens) && tokens[i+1].is("(")
	switch {
	case tok.text == "DateTime32":
		return []token{word("DateTime")}, i
	case tok.text == "DateTime64" && !hasArgs:
		return parenthesized("DateTime64", number("3")), i
	case tok.text == "Decimal" && !hasArgs:
		return parenthesized("Decimal", number("10"), punct(","), number("0")), i
	case tok.text == "Decimal" && hasSingleNumber(tokens, i+1):
		return parenthesized("Decimal", tokens[i+2], punct(","), number("0")), i + 3
	case decimalPrecision[tok.text] != "" && hasSingleNumber(tokens, i+1):
		return parenthesized("Decimal", number(decimalPrecision[tok.text]), punct(","), tokens[i+2]), i + 3
	case (tok.text == "Enum" || tok.text == "Enum8" || tok.text == "Enum16") && hasArgs:
		if enum, end, ok := canonicalEnum(tokens, i); ok {
			return enum, end
		}
	}
	return []token{tok}, i
}

// hasSingleNumber reports whether tokens[open:] starts with `( number )`.
func hasSingleNumber(tokens []token, open int) bool {
	return open+2 < len(tokens) && tokens[open].is("(") && tokens[open+1].kind == numberToken &&
		tokens[open+2].is(")")
}

type enumValue struct {
	label string
	value int
}

// canonicalEnum writes Enum, Enum8 and Enum16 the way the server stores them:
// a value left out is one more than the previous one, starting at 1, the
// values are listed in ascending order, and a bare Enum is Enum8 when every
// value fits in a signed byte and Enum16 otherwise. An argument list it does
// not recognize is left as written.
func canonicalEnum(tokens []token, start int) (out []token, last int, ok bool) {
	end := closingParen(tokens, start+1)
	if end < 0 {
		return nil, 0, false
	}
	var values []enumValue
	next := 1
	for i := start + 2; i < end; {
		if tokens[i].kind != stringToken {
			return nil, 0, false
		}
		label := tokens[i].text
		i++
		value := next
		if i < end && tokens[i].is("=") {
			parsed, consumed, valid := enumNumber(tokens[i+1 : end])
			if !valid {
				return nil, 0, false
			}
			value = parsed
			i += 1 + consumed
		}
		values = append(values, enumValue{label: label, value: value})
		next = value + 1
		if i < end && !tokens[i].is(",") {
			return nil, 0, false
		}
		i++
	}
	if len(values) == 0 {
		return nil, 0, false
	}
	slices.SortStableFunc(values, func(a, b enumValue) int { return a.value - b.value })

	family := tokens[start].text
	if family == "Enum" {
		family = "Enum8"
		if values[0].value < -128 || values[len(values)-1].value > 127 {
			family = "Enum16"
		}
	}
	args := make([]token, 0, len(values)*4)
	for n, v := range values {
		if n > 0 {
			args = append(args, punct(","))
		}
		args = append(args, token{kind: stringToken, text: v.label}, punct("="), number(strconv.Itoa(v.value)))
	}
	return parenthesized(family, args...), end, true
}

// enumNumber reads an optionally negative integer and how many tokens it took.
func enumNumber(tokens []token) (value, consumed int, ok bool) {
	sign := ""
	if len(tokens) > 0 && tokens[0].is("-") {
		sign, tokens, consumed = "-", tokens[1:], 1
	}
	if len(tokens) == 0 || tokens[0].kind != numberToken {
		return 0, 0, false
	}
	parsed, err := strconv.Atoi(sign + tokens[0].text)
	if err != nil {
		return 0, 0, false
	}
	return parsed, consumed + 1, true
}

// closingParen returns the index of the parenthesis closing the one at
// tokens[open], or -1.
func closingParen(tokens []token, open int) int {
	depth := 0
	for i := open; i < len(tokens); i++ {
		switch {
		case tokens[i].is("("):
			depth++
		case tokens[i].is(")"):
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func word(text string) token   { return token{kind: wordToken, text: text} }
func number(text string) token { return token{kind: numberToken, text: text} }
func punct(text string) token  { return token{kind: punctToken, text: text} }

func parenthesized(family string, args ...token) []token {
	out := append([]token{word(family), punct("(")}, args...)
	return append(out, punct(")"))
}

// join writes tokens back with a space only where two words or numbers would
// otherwise run together.
func join(tokens []token) string {
	var b strings.Builder
	for i, tok := range tokens {
		if i > 0 && isWordLike(tokens[i-1]) && isWordLike(tok) {
			b.WriteByte(' ')
		}
		switch tok.kind {
		case stringToken:
			b.WriteByte('\'')
			b.WriteString(strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(tok.text))
			b.WriteByte('\'')
		default:
			b.WriteString(tok.text)
		}
	}
	return b.String()
}

func isWordLike(tok token) bool {
	return tok.kind == wordToken || tok.kind == numberToken || tok.kind == identToken
}
