package lexer

import (
	"strings"
	"unicode/utf8"
)

// StringValue answers the text a string token stands for: the characters
// between its quotes, with the quoting the dialect's lexer applied undone. It
// reports false for a token that is not one of the string forms below, and
// for one whose closing quote is missing, so a caller never reads the quoting
// as part of the text.
//
// The forms are the ones [Lexer] emits as [TokenString] under the same
// options:
//
//   - `'...'`, where a doubled quote stands for one, and where a backslash
//     escapes the next character only when opts.BackslashEscapes is set, as
//     MySQL and MariaDB read it;
//   - `E'...'` when opts.PostgreSQLEscapeStrings is set, with PostgreSQL's
//     backslash escapes;
//   - `$tag$...$tag$`, whose text is taken exactly as written;
//   - `"..."`, read like `'...'`. Whether a double-quoted token is a string at
//     all is the dialect's question, which the caller has answered by asking.
//
// A routine body written as a string is the reason this exists. The literal
// that carries the body `SELECT 'x'` doubles each quote inside it, and a body
// read without that undone is one the server refuses once it is written
// between dollar quotes (stokaro/ptah#3691).
func StringValue(token string, opts Options) (string, bool) {
	switch {
	case strings.HasPrefix(token, "$"):
		return dollarQuotedValue(token)
	case opts.PostgreSQLEscapeStrings && len(token) > 1 && (token[0] == 'E' || token[0] == 'e') && token[1] == '\'':
		return quotedValue(token[1:], postgresEscape)
	case opts.BackslashEscapes:
		return quotedValue(token, mysqlEscape)
	default:
		return quotedValue(token, nil)
	}
}

// dollarQuotedValue answers the text of `$tag$...$tag$`.
func dollarQuotedValue(token string) (string, bool) {
	end := strings.IndexByte(token[1:], '$')
	if end < 0 {
		return "", false
	}
	tag := token[:end+2]
	if len(token) < 2*len(tag) || !strings.HasSuffix(token, tag) {
		return "", false
	}
	return token[len(tag) : len(token)-len(tag)], true
}

// escapeReader decodes one backslash escape. text starts after the
// backslash; the answer is what the escape stands for and how many bytes of
// text it took.
type escapeReader func(text string) (string, int)

// quotedValue answers the text of a literal quoted with its first byte, a
// doubled quote standing for one and, where escape is not nil, a backslash
// starting an escape.
func quotedValue(token string, escape escapeReader) (string, bool) {
	if len(token) < 2 || (token[0] != '\'' && token[0] != '"') || token[len(token)-1] != token[0] {
		return "", false
	}
	quote := token[0]
	inner := token[1 : len(token)-1]
	var text strings.Builder
	text.Grow(len(inner))
	for i := 0; i < len(inner); i++ {
		switch {
		case inner[i] == quote:
			// Inside the literal a quote only ever arrives doubled; a lone
			// one would have closed it.
			if i+1 >= len(inner) || inner[i+1] != quote {
				return "", false
			}
			text.WriteByte(quote)
			i++
		case inner[i] == '\\' && escape != nil && i+1 < len(inner):
			decoded, width := escape(inner[i+1:])
			text.WriteString(decoded)
			i += width
		default:
			text.WriteByte(inner[i])
		}
	}
	return text.String(), true
}

// postgresEscape decodes a backslash escape of a PostgreSQL escape string:
// \b \f \n \r \t, one to three octal digits, \x with one or two hex digits,
// \u with four and \U with eight, and any other character standing for
// itself.
func postgresEscape(text string) (string, int) {
	switch text[0] {
	case 'b':
		return "\b", 1
	case 'f':
		return "\f", 1
	case 'n':
		return "\n", 1
	case 'r':
		return "\r", 1
	case 't':
		return "\t", 1
	case 'x':
		if digits := leadingDigits(text[1:], 2, isHexDigit); digits > 0 {
			return string([]byte{byteValue(text[1:1+digits], 16)}), 1 + digits
		}
	case 'u', 'U':
		width := 4
		if text[0] == 'U' {
			width = 8
		}
		if leadingDigits(text[1:], width, isHexDigit) == width {
			var value rune
			for _, digit := range []byte(text[1 : 1+width]) {
				value = value*16 + rune(digitValue(digit))
			}
			return string(value), 1 + width
		}
	}
	if digits := leadingDigits(text, 3, isOctalDigit); digits > 0 {
		return string([]byte{byteValue(text[:digits], 8)}), digits
	}
	_, size := utf8.DecodeRuneInString(text)
	return text[:size], size
}

// mysqlEscape decodes a backslash escape the way MySQL reads one in a string:
// \0 \b \n \r \t \Z, and any other character standing for itself, except \%
// and \_, which keep their backslash for LIKE.
func mysqlEscape(text string) (string, int) {
	switch text[0] {
	case '0':
		return "\x00", 1
	case 'b':
		return "\b", 1
	case 'n':
		return "\n", 1
	case 'r':
		return "\r", 1
	case 't':
		return "\t", 1
	case 'Z':
		return "\x1a", 1
	case '%', '_':
		return "\\" + text[:1], 1
	}
	_, size := utf8.DecodeRuneInString(text)
	return text[:size], size
}

// byteValue reads digits in base as one byte. A value above 255 keeps its low
// eight bits, which is what the server keeps of an octal escape such as \777.
func byteValue(digits string, base byte) byte {
	var value byte
	for _, digit := range []byte(digits) {
		value = value*base + digitValue(digit)
	}
	return value
}

// digitValue is the value of one hexadecimal, and so also octal, digit.
func digitValue(digit byte) byte {
	switch {
	case digit >= 'a':
		return digit - 'a' + 10
	case digit >= 'A':
		return digit - 'A' + 10
	default:
		return digit - '0'
	}
}

// leadingDigits counts the digits text starts with, up to limit.
func leadingDigits(text string, limit int, digit func(byte) bool) int {
	count := 0
	for count < limit && count < len(text) && digit(text[count]) {
		count++
	}
	return count
}

func isHexDigit(character byte) bool {
	return character >= '0' && character <= '9' || character >= 'a' && character <= 'f' || character >= 'A' && character <= 'F'
}

func isOctalDigit(character byte) bool {
	return character >= '0' && character <= '7'
}
