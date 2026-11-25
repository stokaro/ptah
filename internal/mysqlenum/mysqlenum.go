// Package mysqlenum reads the values of a MySQL-family ENUM column type, such
// as enum('a','b'), the members of a SET column type, such as set('a','b'),
// and the character set and collation such a type names after its list. The
// MySQL catalog reader reads a column type the server reports with it, and the
// renderer reads a column type a schema declares, such as the one an Atlas HCL
// schema writes as enum("a", "b"), so both read a value the same way.
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
// is not an ENUM, or a list that is not literals separated by commas and closed
// by a parenthesis, reads as no values. The text after the closing
// parenthesis, such as CHARACTER SET binary, is not read as values; [Charset]
// reads it.
func Values(columnType string) []string {
	values, _ := literals(columnType, "enum")
	return values
}

// SetValues reads the members of a SET column type, such as set('a','b'), as
// [Values] reads the values of an ENUM type. A type that is not a SET reads as
// no members.
func SetValues(columnType string) []string {
	values, _ := literals(columnType, "set")
	return values
}

// Charset reads the character set and the collation an ENUM or SET column type
// names after its list, as in enum('a') CHARACTER SET binary COLLATE binary.
// CHARSET is read as CHARACTER SET, the keywords are matched in any case, a
// name is returned without the quotes around it, and a word that names
// neither, such as NOT NULL, is passed over. Both are empty for a type that
// names neither, and for a type whose values [Values] and [SetValues] do not
// read.
func Charset(columnType string) (charset, collation string) {
	values, attributes := literals(columnType, "enum")
	if values == nil {
		values, attributes = literals(columnType, "set")
	}
	if values == nil {
		return "", ""
	}
	words := strings.Fields(attributes)
	for i := 0; i < len(words); i++ {
		switch word := strings.ToUpper(words[i]); {
		case word == "CHARACTER" && i+2 < len(words) && strings.EqualFold(words[i+1], "SET"):
			charset = unquoted(words[i+2])
			i += 2
		case word == "CHARSET" && i+1 < len(words):
			charset = unquoted(words[i+1])
			i++
		case word == "COLLATE" && i+1 < len(words):
			collation = unquoted(words[i+1])
			i++
		}
	}
	return charset, collation
}

// unquoted is name without the quotes or backticks around it.
func unquoted(name string) string {
	if len(name) >= 2 && strings.ContainsRune("'\"`", rune(name[0])) && name[len(name)-1] == name[0] {
		return name[1 : len(name)-1]
	}
	return name
}

// literals reads the list of string literals of the column type keyword(...),
// up to the parenthesis that closes it, and returns the text after that
// parenthesis without the spaces around it.
func literals(columnType, keyword string) (values []string, attributes string) {
	if len(columnType) < len(keyword) || !strings.EqualFold(columnType[:len(keyword)], keyword) {
		return nil, ""
	}
	body, ok := strings.CutPrefix(strings.TrimLeft(columnType[len(keyword):], " "), "(")
	if !ok {
		return nil, ""
	}
	lexing := dialectlexer.Options(platform.MySQL)
	for i := 0; i < len(body); i++ {
		switch quote := body[i]; quote {
		case ' ', ',':
			continue
		case ')':
			return values, strings.TrimSpace(body[i+1:])
		case '\'', '"':
			end := closingQuote(body, i, quote)
			if end < 0 {
				return nil, ""
			}
			value, read := lexer.StringValue(body[i:end+1], lexing)
			if !read {
				return nil, ""
			}
			if value != "" {
				values = append(values, value)
			}
			i = end
		default:
			return nil, ""
		}
	}
	return nil, ""
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
