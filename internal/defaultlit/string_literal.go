package defaultlit

import (
	"strconv"
	"strings"
	"unicode"
)

// plainString reads text as one PostgreSQL string literal and returns its
// contents: a quoted string, where a doubled quote stands for one quote; an
// escape string, E'...', with its backslash escapes decoded; a Unicode escape
// string, U&'...', with its \XXXX and \+XXXXXX escapes decoded; or a
// dollar-quoted string, $$...$$ or $tag$...$tag$. Any other text is not one,
// a Unicode escape string with a UESCAPE clause or a malformed escape
// included.
func plainString(text string) (string, bool) {
	if literalEnd(text, 0) != len(text) {
		return "", false
	}
	switch text[0] {
	case '\'':
		return strings.ReplaceAll(text[1:len(text)-1], "''", "'"), true
	case 'E', 'e':
		return decodeEscapeString(text[2 : len(text)-1]), true
	case 'U', 'u':
		return decodeUnicodeString(text[3 : len(text)-1])
	default:
		tag := dollarTag(text)
		return text[len(tag) : len(text)-len(tag)], true
	}
}

// literalEnd returns where the string literal that begins at text[start]
// ends, just after its closing delimiter, or -1 where none begins there or it
// does not end. A prefix letter right after another letter or digit begins no
// literal: the E in name'x' belongs to the word before it.
func literalEnd(text string, start int) int {
	if start >= len(text) {
		return -1
	}
	afterWord := start > 0 && identifierByte(text[start-1])
	switch rest := text[start:]; {
	case rest[0] == '\'':
		return quotedEnd(text, start+1, false)
	case afterWord:
		return -1
	case (rest[0] == 'E' || rest[0] == 'e') && strings.HasPrefix(rest[1:], "'"):
		return quotedEnd(text, start+2, true)
	case (rest[0] == 'U' || rest[0] == 'u') && strings.HasPrefix(rest[1:], "&'"):
		return quotedEnd(text, start+3, false)
	case rest[0] == '$':
		tag := dollarTag(rest)
		if tag == "" {
			return -1
		}
		end := strings.Index(rest[len(tag):], tag)
		if end < 0 {
			return -1
		}
		return start + len(tag) + end + len(tag)
	}
	return -1
}

// quotedEnd returns where a quoted string whose body begins at text[from]
// ends, just after its closing quote, or -1. A doubled quote continues the
// string, and so, in an escape string, does a backslash before any byte.
func quotedEnd(text string, from int, escapes bool) int {
	for i := from; i < len(text); i++ {
		switch {
		case escapes && text[i] == '\\':
			i++
		case text[i] != '\'':
		case i+1 < len(text) && text[i+1] == '\'':
			i++
		default:
			return i + 1
		}
	}
	return -1
}

// dollarTag returns the delimiter that opens a dollar-quoted string at the
// start of text, $$ or $tag$, or "" where none does. A tag starts with a
// letter or an underscore, so $1 opens nothing.
func dollarTag(text string) string {
	if len(text) < 2 || text[0] != '$' {
		return ""
	}
	for i := 1; i < len(text); i++ {
		ch := text[i]
		switch {
		case ch == '$':
			return text[:i+1]
		case ch == '_' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || i > 1 && ch >= '0' && ch <= '9':
		default:
			return ""
		}
	}
	return ""
}

// decodeEscapeString decodes the body of an escape string: \b, \f, \n, \r
// and \t, an octal byte of up to three digits, \x and a hex byte of up to two
// digits, \u and four hex digits, \U and eight, and a backslash before any
// other byte, which stands for that byte. A doubled quote is one quote.
func decodeEscapeString(body string) string {
	var out strings.Builder
	for i := 0; i < len(body); i++ {
		ch := body[i]
		switch {
		case ch == '\'' && i+1 < len(body) && body[i+1] == '\'':
			out.WriteByte('\'')
			i++
			continue
		case ch != '\\' || i+1 == len(body):
			out.WriteByte(ch)
			continue
		}
		i++
		switch next := body[i]; next {
		case 'b':
			out.WriteByte('\b')
		case 'f':
			out.WriteByte('\f')
		case 'n':
			out.WriteByte('\n')
		case 'r':
			out.WriteByte('\r')
		case 't':
			out.WriteByte('\t')
		case 'x':
			used := leadingDigits(body[i+1:], 2, 16)
			if !writeByteCode(&out, body[i+1:i+1+used], 16) {
				out.WriteByte(next)
				continue
			}
			i += used
		case 'u', 'U':
			width := map[byte]int{'u': 4, 'U': 8}[next]
			if leadingDigits(body[i+1:], width, 16) < width || !writeRuneCode(&out, body[i+1:i+1+width]) {
				out.WriteByte(next)
				continue
			}
			i += width
		default:
			used := leadingDigits(body[i:], 3, 8)
			if !writeByteCode(&out, body[i:i+used], 8) {
				out.WriteByte(next)
				continue
			}
			i += used - 1
		}
	}
	return out.String()
}

// decodeUnicodeString decodes the body of a Unicode escape string: a
// backslash and four hex digits, a backslash, a plus and six hex digits, and
// two backslashes for one. A doubled quote is one quote. Any other backslash
// is malformed.
func decodeUnicodeString(body string) (string, bool) {
	var out strings.Builder
	for i := 0; i < len(body); i++ {
		ch := body[i]
		switch {
		case ch == '\'' && i+1 < len(body) && body[i+1] == '\'':
			out.WriteByte('\'')
			i++
		case ch != '\\':
			out.WriteByte(ch)
		case strings.HasPrefix(body[i+1:], `\`):
			out.WriteByte('\\')
			i++
		case strings.HasPrefix(body[i+1:], "+"):
			if leadingDigits(body[i+2:], 6, 16) < 6 || !writeRuneCode(&out, body[i+2:i+8]) {
				return "", false
			}
			i += 7
		default:
			if leadingDigits(body[i+1:], 4, 16) < 4 || !writeRuneCode(&out, body[i+1:i+5]) {
				return "", false
			}
			i += 4
		}
	}
	return out.String(), true
}

// leadingDigits returns how many digits of base, up to width, text starts
// with.
func leadingDigits(text string, width, base int) int {
	digits := "0123456789abcdefABCDEF"[:base+max(0, base-10)]
	used := 0
	for used < width && used < len(text) && strings.IndexByte(digits, text[used]) >= 0 {
		used++
	}
	return used
}

// writeByteCode writes the byte that digits, in base, name, and reports
// whether they name one: at least one digit and a value below 256.
func writeByteCode(out *strings.Builder, digits string, base int) bool {
	value, err := strconv.ParseUint(digits, base, 8)
	if err != nil {
		return false
	}
	out.WriteByte(byte(value))
	return true
}

// writeRuneCode writes the UTF-8 encoding of the code point the hex digits
// name, and reports whether they name a valid one.
func writeRuneCode(out *strings.Builder, digits string) bool {
	value, err := strconv.ParseInt(digits, 16, 32)
	if err != nil || value > unicode.MaxRune || value < 0 {
		return false
	}
	out.WriteRune(rune(value))
	return true
}
