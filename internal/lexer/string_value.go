package lexer

import (
	"strings"
	"unicode/utf8"
)

// StringValue answers the text a string token stands for: the characters
// between its quotes, with the quoting the dialect's lexer applied undone. It
// reports false for a token that is not one of the string forms below, for one
// whose closing quote is missing, and for one whose escapes the server
// refuses, so a caller never reads the quoting as part of the text.
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
// With opts.PostgreSQLStringConstants set, a `'...'` or `E'...'` token may
// hold continued segments, whose texts are joined, and `U&'...'` is read with
// its Unicode escapes and any UESCAPE clause. An escape string's segments are
// read one at a time, as the server scans them, and a Unicode escape string's
// escapes are read once the segments are joined, as the server reads them.
//
// A routine body written as a string is the reason this exists. The literal
// that carries the body `SELECT 'x'` doubles each quote inside it, and a body
// read without that undone is one the server refuses once it is written
// between dollar quotes (stokaro/ptah#3691).
func StringValue(token string, opts Options) (string, bool) {
	read := quotedValue
	if opts.PostgreSQLStringConstants {
		read = continuedValue
	}
	switch {
	case strings.HasPrefix(token, "$"):
		return dollarQuotedValue(token)
	case opts.PostgreSQLStringConstants && hasUnicodeEscapePrefix(token):
		return unicodeEscapeStringValue(token[2:], opts)
	case opts.PostgreSQLEscapeStrings && len(token) > 1 && (token[0] == 'E' || token[0] == 'e') && token[1] == '\'':
		text, ok := read(token[1:], postgresEscape)
		if !ok || !isServerText(text) {
			return "", false
		}
		return text, true
	case opts.BackslashEscapes:
		return quotedValue(token, mysqlEscape)
	default:
		return read(token, nil)
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

// quotedValue answers the text of a literal quoted with its first byte.
func quotedValue(token string, escape escapeReader) (string, bool) {
	text, end, ok := quotedSegment(token, escape)
	if !ok || end != len(token) {
		return "", false
	}
	return text, true
}

// continuedValue answers the text of a literal quoted with its first byte
// and, for a single-quoted one, of the segments that continue it, each read
// like the first and their texts joined.
func continuedValue(token string, escape escapeReader) (string, bool) {
	var text strings.Builder
	rest := token
	for {
		segment, end, ok := quotedSegment(rest, escape)
		if !ok {
			return "", false
		}
		text.WriteString(segment)
		if end == len(rest) {
			return text.String(), true
		}
		next, found := continuationAt(rest, end)
		if token[0] != '\'' || !found {
			return "", false
		}
		rest = rest[next:]
	}
}

// quotedSegment reads the quoted segment text starts with and answers its
// text and the position after its closing quote. A doubled quote stands for
// one, and where escape is not nil a backslash starts an escape.
func quotedSegment(token string, escape escapeReader) (string, int, bool) {
	if len(token) < 2 || (token[0] != '\'' && token[0] != '"') {
		return "", 0, false
	}
	quote := token[0]
	var text strings.Builder
	for i := 1; i < len(token); i++ {
		switch {
		case token[i] == quote:
			if i+1 < len(token) && token[i+1] == quote {
				text.WriteByte(quote)
				i++
				continue
			}
			return text.String(), i + 1, true
		case token[i] == '\\' && escape != nil && i+1 < len(token):
			decoded, width, ok := escape(token[i+1:])
			if !ok {
				return "", 0, false
			}
			text.WriteString(decoded)
			i += width
		default:
			text.WriteByte(token[i])
		}
	}
	return "", 0, false
}

// escapeReader decodes one backslash escape. text starts after the
// backslash; the answer is what the escape stands for, how many bytes of text
// it took, and false for an escape the dialect refuses.
type escapeReader func(text string) (string, int, bool)

// postgresEscape decodes a backslash escape of a PostgreSQL escape string:
// \b \f \n \r \t, one to three octal digits, \x with one or two hex digits,
// \u with four and \U with eight, and any other character standing for
// itself. A \u or \U escape short of its digits is refused, as is one naming
// no character: zero, a value past U+10FFFF, or half of a surrogate pair
// without the other half written as the next escape.
func postgresEscape(text string) (string, int, bool) {
	switch text[0] {
	case 'b':
		return "\b", 1, true
	case 'f':
		return "\f", 1, true
	case 'n':
		return "\n", 1, true
	case 'r':
		return "\r", 1, true
	case 't':
		return "\t", 1, true
	case 'x':
		if digits := leadingDigits(text[1:], 2, isHexDigit); digits > 0 {
			return string([]byte{byteValue(text[1:1+digits], 16)}), 1 + digits, true
		}
	case 'u', 'U':
		return escapedCodePoint(text, '\\', escapeStringCodePoint)
	}
	if digits := leadingDigits(text, 3, isOctalDigit); digits > 0 {
		return string([]byte{byteValue(text[:digits], 8)}), digits, true
	}
	_, size := utf8.DecodeRuneInString(text)
	return text[:size], size, true
}

// escapeStringCodePoint reads the code point of a \u or \U escape of an
// escape string. text starts at the u or U.
func escapeStringCodePoint(text string) (rune, int, bool) {
	var width int
	switch {
	case strings.HasPrefix(text, "u"):
		width = 4
	case strings.HasPrefix(text, "U"):
		width = 8
	default:
		return 0, 0, false
	}
	if leadingDigits(text[1:], width, isHexDigit) != width {
		return 0, 0, false
	}
	return hexValue(text[1 : 1+width]), 1 + width, true
}

// escapedCodePoint decodes the escape at the start of text with read. A high
// surrogate is read together with the low surrogate that must follow it as
// the next escape, after the escape character.
func escapedCodePoint(text string, escape byte, read func(string) (rune, int, bool)) (string, int, bool) {
	value, width, ok := read(text)
	if !ok {
		return "", 0, false
	}
	if isHighSurrogate(value) {
		if width >= len(text) || text[width] != escape {
			return "", 0, false
		}
		low, lowWidth, ok := read(text[width+1:])
		if !ok || !isLowSurrogate(low) {
			return "", 0, false
		}
		value = (value-0xD800)<<10 + (low - 0xDC00) + 0x10000
		width += 1 + lowWidth
	}
	if value <= 0 || value > utf8.MaxRune || isHighSurrogate(value) || isLowSurrogate(value) {
		return "", 0, false
	}
	return string(value), width, true
}

func isHighSurrogate(value rune) bool {
	return value >= 0xD800 && value <= 0xDBFF
}

func isLowSurrogate(value rune) bool {
	return value >= 0xDC00 && value <= 0xDFFF
}

// isServerText reports text a UTF-8 server stores. An escape string's octal
// and hex escapes write bytes, and the server refuses the string when those
// bytes are a zero or are not UTF-8.
func isServerText(text string) bool {
	return utf8.ValidString(text) && !strings.ContainsRune(text, 0)
}

// unicodeEscapeStringValue answers the text of a Unicode escape string. token
// starts at the opening quote after U&, and holds the segments that continue
// it and the UESCAPE clause after them where the lexer found either.
//
// The escapes are \XXXX with four hex digits and \+XXXXXX with six, where the
// clause may name another escape character than the backslash, and a doubled
// escape character stands for itself. Any other use of the escape character
// is refused, as the server refuses it.
func unicodeEscapeStringValue(token string, opts Options) (string, bool) {
	var raw strings.Builder
	rest := token
	for {
		segment, end, ok := quotedSegment(rest, nil)
		if !ok || rest[0] != '\'' {
			return "", false
		}
		raw.WriteString(segment)
		rest = rest[end:]
		next, found := continuationAt(rest, 0)
		if !found {
			break
		}
		rest = rest[next:]
	}
	escape := byte('\\')
	if rest != "" {
		constant, found := unicodeEscapeClauseAt(rest, 0)
		if !found {
			return "", false
		}
		character, ok := unicodeEscapeCharacter(rest[constant:], opts)
		if !ok {
			return "", false
		}
		escape = character
	}
	return unicodeEscapesValue(raw.String(), escape)
}

// unicodeEscapeCharacter reads the string constant a UESCAPE clause names
// and answers the one character it holds. The server takes any string
// constant here but another Unicode escape string, and refuses a character
// that would read as part of an escape or as the end of the string: a hex
// digit, a plus sign, a quote of either kind, or whitespace.
func unicodeEscapeCharacter(constant string, opts Options) (byte, bool) {
	if hasUnicodeEscapePrefix(constant) {
		return 0, false
	}
	text, ok := StringValue(constant, opts)
	if !ok || len(text) != 1 {
		return 0, false
	}
	character := text[0]
	if isHexDigit(character) || character == '+' || character == '\'' || character == '"' || isGapSpace(character) {
		return 0, false
	}
	return character, true
}

// unicodeEscapesValue reads the escapes of a Unicode escape string's joined
// text.
func unicodeEscapesValue(raw string, escape byte) (string, bool) {
	var text strings.Builder
	text.Grow(len(raw))
	for i := 0; i < len(raw); i++ {
		if raw[i] != escape {
			text.WriteByte(raw[i])
			continue
		}
		if i+1 < len(raw) && raw[i+1] == escape {
			text.WriteByte(escape)
			i++
			continue
		}
		decoded, width, ok := escapedCodePoint(raw[i+1:], escape, unicodeEscapeCodePoint)
		if !ok {
			return "", false
		}
		text.WriteString(decoded)
		i += width
	}
	return text.String(), true
}

// unicodeEscapeCodePoint reads the code point of a Unicode escape string's
// escape: four hex digits, or a plus sign and six. text starts after the
// escape character.
func unicodeEscapeCodePoint(text string) (rune, int, bool) {
	start, width := 0, 4
	if strings.HasPrefix(text, "+") {
		start, width = 1, 6
	}
	if leadingDigits(text[start:], width, isHexDigit) != width {
		return 0, 0, false
	}
	return hexValue(text[start : start+width]), start + width, true
}

// mysqlEscape decodes a backslash escape the way MySQL reads one in a string:
// \0 \b \n \r \t \Z, and any other character standing for itself, except \%
// and \_, which keep their backslash for LIKE.
func mysqlEscape(text string) (string, int, bool) {
	switch text[0] {
	case '0':
		return "\x00", 1, true
	case 'b':
		return "\b", 1, true
	case 'n':
		return "\n", 1, true
	case 'r':
		return "\r", 1, true
	case 't':
		return "\t", 1, true
	case 'Z':
		return "\x1a", 1, true
	case '%', '_':
		return "\\" + text[:1], 1, true
	}
	_, size := utf8.DecodeRuneInString(text)
	return text[:size], size, true
}

// hexValue reads hex digits as a code point.
func hexValue(digits string) rune {
	var value rune
	for _, digit := range []byte(digits) {
		value = value*16 + rune(digitValue(digit))
	}
	return value
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
