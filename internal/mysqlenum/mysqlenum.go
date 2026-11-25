// Package mysqlenum reads the values of a MySQL-family ENUM column type, such
// as enum('a','b'), and the members of a SET column type, such as
// set('a','b'). The MySQL catalog reader reads a column type the server
// reports with it, and the renderer reads a column type a schema declares,
// such as the one an Atlas HCL schema writes as enum("a", "b"), so both read a
// value the same way.
package mysqlenum

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// Values reads the values of an ENUM column type, such as enum('a','b'). The
// keyword is matched in any case, and spaces may stand before the opening
// parenthesis.
//
// Each value is a string literal, single or double quoted, and is read as the
// server reads the literal, so one holding a comma, a quote or a backslash is
// one value: measured on MySQL 26.10 and MariaDB 12.3, the catalog writes the
// value e\f as 'e\\f', and doubles a quote inside a value. The spaces inside
// a literal are part of the value. An empty value is left out, and a type that
// is not an ENUM, or a list that is not literals separated by commas, reads as
// no values.
func Values(columnType string) []string {
	return literals(columnType, "enum")
}

// SetValues reads the members of a SET column type, such as set('a','b'), as
// [Values] reads the values of an ENUM type. A type that is not a SET reads as
// no members.
func SetValues(columnType string) []string {
	return literals(columnType, "set")
}

// literals reads the list of string literals of the column type keyword(...).
func literals(columnType, keyword string) []string {
	if len(columnType) < len(keyword) || !strings.EqualFold(columnType[:len(keyword)], keyword) {
		return nil
	}
	body, ok := strings.CutPrefix(strings.TrimLeft(columnType[len(keyword):], " "), "(")
	if !ok || !strings.HasSuffix(body, ")") {
		return nil
	}
	body = body[:len(body)-1]
	lexing := dialectlexer.Options(platform.MySQL)
	var values []string
	for i := 0; i < len(body); i++ {
		switch quote := body[i]; quote {
		case ' ', ',':
			continue
		case '\'', '"':
			end := closingQuote(body, i, quote)
			if end < 0 {
				return nil
			}
			value, read := lexer.StringValue(body[i:end+1], lexing)
			if !read {
				return nil
			}
			if value != "" {
				values = append(values, value)
			}
			i = end
		default:
			return nil
		}
	}
	return values
}

// closingQuote returns the position of the quote that closes the literal
// opening at start in text, reading a doubled quote and a backslash escape as
// part of the literal, or -1 when nothing closes it.
func closingQuote(text string, start int, quote byte) int {
	for i := start + 1; i < len(text); i++ {
		switch {
		case text[i] == '\\':
			i++
		case text[i] == quote && i+1 < len(text) && text[i+1] == quote:
			i++
		case text[i] == quote:
			return i
		}
	}
	return -1
}
