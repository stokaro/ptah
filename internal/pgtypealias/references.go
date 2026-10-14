package pgtypealias

import (
	"strings"

	"ptah.run/core/platform/identifier"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlident"
)

// Reference compares PostgreSQL object references by their decoded components.
// Unquoted names fold to lowercase; quoted names preserve case and whitespace.
// The implicit pg_catalog namespace and its explicit spelling name the same
// built-in object. Other schema qualifiers remain part of the reference.
func Reference(spelling string) string {
	parts, ok := referenceParts(spelling)
	if !ok {
		return strings.TrimSpace(spelling)
	}
	if len(parts) == 2 && parts[0] == "pg_catalog" {
		parts = parts[1:]
	}
	for i, part := range parts {
		parts[i] = sqlident.Quote("postgres", part)
	}
	return strings.Join(parts, ".")
}

func referenceParts(spelling string) ([]string, bool) {
	scan := lexer.NewLexerWithOptions(spelling, dialectlexer.Options("postgres"))
	var parts []string
	name := true
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		if token.Type == lexer.TokenWhitespace || token.Type == lexer.TokenComment {
			continue
		}
		if !name {
			if !token.MatchOperatorValue(".") || len(parts) > 1 {
				return nil, false
			}
			name = true
			continue
		}
		switch {
		case token.Type == lexer.TokenIdentifier:
			parts = append(parts, identifier.ComparisonASCIIInsensitive.IdentityKey(token.Value))
		case token.Type == lexer.TokenString && strings.HasPrefix(token.Value, `"`) && strings.HasSuffix(token.Value, `"`):
			parts = append(parts, sqlident.UnquoteDoubleQuoted(token.Value))
		default:
			return nil, false
		}
		name = false
	}
	return parts, !name
}

// referenceType normalizes a qualified or quoted type without folding quoted
// components. It leaves syntax such as modifiers and array bounds to Canonical.
func referenceType(spelling string) (string, bool) {
	base, suffix := typeReferenceParts(spelling)
	parts, ok := referenceParts(base)
	if !ok {
		return "", false
	}
	if len(parts) == 2 && parts[0] == "pg_catalog" {
		parts = parts[1:]
	}
	if len(parts) == 1 {
		if canonical, found := catalogAliases[parts[0]]; found {
			return canonical + suffix, true
		}
		if grammarTypeName(parts[0]) {
			return sqlident.Quote("postgres", parts[0]) + suffix, true
		}
	}
	for i, part := range parts {
		// A mixed-case component needs its quotes to remain distinct from the
		// lowercase object. Always quote other special identifier characters too.
		if identifier.ComparisonASCIIInsensitive.IdentityKey(part) != part {
			parts[i] = sqlident.Quote("postgres", part)
		} else {
			parts[i] = sqlident.BareOrQuoted("postgres", part)
		}
	}
	return strings.Join(parts, ".") + suffix, true
}

// A quoted or qualified name resolves a catalog type directly. Grammar-only
// aliases such as serial and decimal do not identify catalog types.
var catalogAliases = map[string]string{
	"numeric": "numeric",
	"varchar": "character varying", "bpchar": "character", "char": `"char"`,
	"int2": "smallint", "int4": "integer", "int8": "bigint", "float4": "real", "float8": "double precision",
	"bool": "boolean", "timestamptz": "timestamp with time zone", "timestamp": "timestamp without time zone", "timetz": "time with time zone",
}

func typeReferenceParts(spelling string) (base, suffix string) {
	scan := lexer.NewLexerWithOptions(spelling, dialectlexer.Options("postgres"))
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		if token.Value == "(" || token.Value == "[" {
			return strings.TrimSpace(spelling[:token.Start]), strings.Join(strings.Fields(spelling[token.Start:]), "")
		}
	}
	return spelling, ""
}

func grammarTypeName(name string) bool {
	if _, found := aliases[name]; found {
		return true
	}
	for _, canonical := range aliases {
		if canonical == name {
			return true
		}
	}
	return false
}
