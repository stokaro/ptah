package compare

import (
	"strings"

	"ptah.run/internal/defaultlit"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// literalsAsRead returns a declared literal default and the default a catalog
// reports in forms that are equal when the column stores one value.
//
// The declared literal is quoted the way a catalog reports it
// ([defaultlit.AsRead]). Where a backslash in a string escapes the character
// after it, as on the MySQL family and ClickHouse, a text default is compared
// by the string it stores rather than by its spelling: the engines spell one
// value several ways. Measured on ClickHouse 26.10, a default declared with a
// doubled quote reports the quote behind a backslash, `'it\'s;x'`, where the
// MySQL family doubles the quote. Compared by spelling, a declaration plans
// the column on every apply.
func literalsAsRead(declared, reported, typeName, dialect string) (comparedDeclared, comparedReported string) {
	lexing := dialectlexer.Options(dialect)
	if !lexing.BackslashEscapes || !defaultlit.StoresText(typeName) {
		return defaultlit.AsRead(typeName, declared), reported
	}
	return storedText(defaultlit.AsReadEscaped(typeName, declared), lexing), storedText(reported, lexing)
}

// storedText writes a single-quoted string literal as the string it stores,
// in one spelling: in quotes, with each backslash and each quote doubled.
// Two literals that store one string under lexing come out equal. Anything
// that is not one such literal is returned as it is.
func storedText(value string, lexing lexer.Options) string {
	trimmed := strings.TrimSpace(value)
	if !strings.HasPrefix(trimmed, "'") {
		return value
	}
	text, ok := lexer.StringValue(trimmed, lexing)
	if !ok {
		return value
	}
	return "'" + strings.ReplaceAll(strings.ReplaceAll(text, `\`, `\\`), "'", "''") + "'"
}
