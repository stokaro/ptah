package yql

import (
	"strings"
	"unicode/utf8"
)

// StringValue answers the text of a YQL string token: a quoted or multiline
// literal, with its type suffix set aside. It reports false for a token that
// is not one of those forms, for one whose closing quote is missing, and for
// one whose escape YDB refuses.
func StringValue(token string) (string, bool) {
	if strings.HasPrefix(token, "@@") {
		return multilineValue(token)
	}
	if len(token) < 2 || (token[0] != '\'' && token[0] != '"') {
		return "", false
	}
	quote := token[0]
	var text strings.Builder
	for i := 1; i < len(token); i++ {
		switch token[i] {
		case quote:
			if !isTypeSuffix(token[i+1:]) {
				return "", false
			}
			return text.String(), true
		case '\\':
			if i+1 >= len(token) {
				return "", false
			}
			decoded, width, ok := escape(token[i+1:])
			if !ok {
				return "", false
			}
			text.WriteString(decoded)
			i += width
		default:
			text.WriteByte(token[i])
		}
	}
	return "", false
}

// multilineValue answers the text of an @@...@@ literal: the segments joined
// by the @@ each later one stands for, and the trailing @ the literal may end
// with.
func multilineValue(token string) (string, bool) {
	var text strings.Builder
	pos := 0
	for {
		closing := strings.Index(token[pos+2:], "@@")
		if closing < 0 {
			return "", false
		}
		if pos > 0 {
			text.WriteString("@@")
		}
		text.WriteString(token[pos+2 : pos+2+closing])
		pos += 2 + closing + 2
		if !multilineSegmentAt(token, pos) {
			break
		}
	}
	if strings.HasPrefix(token[pos:], "@") {
		text.WriteByte('@')
		pos++
	}
	if !isTypeSuffix(token[pos:]) {
		return "", false
	}
	return text.String(), true
}

// isTypeSuffix reports whether rest is empty or one whole type suffix.
func isTypeSuffix(rest string) bool {
	switch strings.ToLower(rest) {
	case "", "s", "u", "y", "j", "p", "pt", "pb", "pv":
		return true
	default:
		return false
	}
}

// escape decodes a backslash escape the way YDB 26.2.1.14 reads one,
// measured: \a \b \f \n \r \t \v; \x with exactly two hex digits; three octal
// digits starting with 0 to 3; \u with four hex digits and \U with eight,
// naming a code point that is not a surrogate. Any other character stands for
// itself, so \q is q and \4 is 4. An escape short of its digits is refused,
// as the server refuses it.
func escape(text string) (string, int, bool) {
	switch text[0] {
	case 'a':
		return "\a", 1, true
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
	case 'v':
		return "\v", 1, true
	case 'x':
		if leadingDigits(text[1:], 2, isHexDigit) != 2 {
			return "", 0, false
		}
		return string([]byte{byteValue(text[1:3], 16)}), 3, true
	case '0', '1', '2', '3':
		if leadingDigits(text, 3, isOctalDigit) != 3 {
			return "", 0, false
		}
		return string([]byte{byteValue(text[:3], 8)}), 3, true
	case 'u', 'U':
		value, width, ok := codePoint(text)
		if !ok || value > utf8.MaxRune || isHighSurrogate(value) || isLowSurrogate(value) {
			return "", 0, false
		}
		return string(value), width, true
	}
	_, size := utf8.DecodeRuneInString(text)
	return text[:size], size, true
}

// codePoint reads the code point of a \u escape, four hex digits, or of a \U
// escape, eight. text starts at the u or U.
func codePoint(text string) (rune, int, bool) {
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

func isHighSurrogate(value rune) bool {
	return value >= 0xD800 && value <= 0xDBFF
}

func isLowSurrogate(value rune) bool {
	return value >= 0xDC00 && value <= 0xDFFF
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
// eight bits.
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

// IdentifierValue answers the name a YQL identifier token stands for. A
// backticked name has its backticks removed, a doubled backtick read as one
// and its backslash escapes decoded the way a string's are, as the grammar's
// ID_QUOTED rule reads them; any other identifier is its own name. It reports
// false for a backticked name whose closing backtick is missing or whose
// escape the server refuses.
//
// YQL compares names case-sensitively whether or not they are quoted, so the
// name keeps the case it was written in.
func IdentifierValue(token string) (string, bool) {
	if !strings.HasPrefix(token, "`") {
		return token, token != ""
	}
	var name strings.Builder
	for i := 1; i < len(token); i++ {
		switch token[i] {
		case '`':
			if i+1 < len(token) && token[i+1] == '`' {
				name.WriteByte('`')
				i++
				continue
			}
			if i != len(token)-1 {
				return "", false
			}
			return name.String(), true
		case '\\':
			if i+1 >= len(token) {
				return "", false
			}
			decoded, width, ok := escape(token[i+1:])
			if !ok {
				return "", false
			}
			name.WriteString(decoded)
			i += width
		default:
			name.WriteByte(token[i])
		}
	}
	return "", false
}
