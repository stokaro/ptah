package pgsyntax

import (
	"strings"
	"unicode/utf8"
)

// Value reads what the string constant or quoted identifier that starts at
// tokens[i] stands for. tokens are the tokens [Scan] returned for source under
// options.
//
// A string constant is the String token at i, the String tokens that continue
// it on later lines, and for a U&'...' string the UESCAPE clause after them. A
// quoted identifier is the QuotedIdentifier token at i and, for U&"...", its
// UESCAPE clause. A dollar-quoted string is its one token, its text taken as
// written. Value returns the text and the index of the first token after the
// constant, so a reader that keeps one token per constant spans
// tokens[i].Start to tokens[next-1].End.
//
// A segment continues a constant when only white space and `--` comments
// stand between the two quotes and the white space holds a line ending, as
// section 4.1.2.2 of the PostgreSQL manual states. A block comment between
// them ends the constant. An escape string's segments are each read with its
// escapes; a Unicode escape string's segments are joined before its escapes
// are read.
//
// ok is false, and next is i+1, where the token stands for no text: a word, a
// number or any other kind, a bit string `B'...'` or `X'...'`, a zero-length
// quoted identifier, a constant whose escapes or UESCAPE clause the server
// refuses, a Unicode escape string or identifier under
// [Options.BackslashEscapes], which the server refuses, and a string whose
// escapes write a zero byte or bytes that are not UTF-8, which a UTF-8 server
// refuses.
func Value(source string, tokens []Token, i int, options Options) (value string, next int, ok bool) {
	if i < 0 || i >= len(tokens) {
		return "", i + 1, false
	}
	var read func(string, []Token, int, Options) (string, int, bool)
	switch tokens[i].Kind {
	case DollarString:
		read = dollarValue
	case QuotedIdentifier:
		read = identifierValue
	case String:
		read = stringValue
	default:
		return "", i + 1, false
	}
	value, next, ok = read(source, tokens, i, options)
	if !ok {
		return "", i + 1, false
	}
	return value, next, true
}

// dollarValue reads the text of `$tag$...$tag$`, which is written as it is.
func dollarValue(source string, tokens []Token, i int, _ Options) (string, int, bool) {
	text := tokens[i].Text(source)
	tag := text[:strings.IndexByte(text[1:], '$')+2]
	return text[len(tag) : len(text)-len(tag)], i + 1, true
}

// identifierValue reads a quoted identifier, and the UESCAPE clause of a
// Unicode escape identifier.
func identifierValue(source string, tokens []Token, i int, options Options) (string, int, bool) {
	text := tokens[i].Text(source)
	if text[0] == '"' {
		name := undoubled(text, '"')
		return name, i + 1, name != ""
	}
	if options.BackslashEscapes {
		return "", 0, false
	}
	raw := undoubled(text[2:], '"')
	escape, next, ok := unicodeEscapeClause(source, tokens, i+1, options)
	if !ok || raw == "" {
		return "", 0, false
	}
	name, ok := unicodeEscapes(raw, escape)
	return name, next, ok
}

// stringValue reads a string constant: its segments, and the UESCAPE clause
// of a Unicode escape string.
func stringValue(source string, tokens []Token, i int, options Options) (string, int, bool) {
	text := tokens[i].Text(source)
	quote := strings.IndexByte(text, '\'')
	prefix := strings.ToUpper(text[:quote])
	segments := continuedSegments(source, tokens, i)
	next := segments[len(segments)-1] + 1
	switch prefix {
	case "U&":
		if options.BackslashEscapes {
			return "", 0, false
		}
		var raw strings.Builder
		for _, segment := range segments {
			raw.WriteString(undoubled(segmentText(source, tokens[segment]), '\''))
		}
		escape, after, ok := unicodeEscapeClause(source, tokens, next, options)
		if !ok {
			return "", 0, false
		}
		value, ok := unicodeEscapes(raw.String(), escape)
		return value, after, ok
	case "E":
		return escapedSegments(source, tokens, segments, next)
	case "", "N":
		if options.BackslashEscapes {
			return escapedSegments(source, tokens, segments, next)
		}
		var value strings.Builder
		for _, segment := range segments {
			value.WriteString(undoubled(segmentText(source, tokens[segment]), '\''))
		}
		return value.String(), next, true
	default:
		return "", 0, false
	}
}

// escapedSegments reads each segment with backslash escapes and joins them.
func escapedSegments(source string, tokens []Token, segments []int, next int) (string, int, bool) {
	var value strings.Builder
	for _, segment := range segments {
		text, ok := backslashEscaped(segmentText(source, tokens[segment]))
		if !ok {
			return "", 0, false
		}
		value.WriteString(text)
	}
	text := value.String()
	if !utf8.ValidString(text) || strings.IndexByte(text, 0) >= 0 {
		return "", 0, false
	}
	return text, next, true
}

// continuedSegments returns the index of the String token at i and of each
// String token that continues it.
func continuedSegments(source string, tokens []Token, i int) []int {
	segments := []int{i}
	for {
		next, found := continuation(source, tokens, segments[len(segments)-1])
		if !found {
			return segments
		}
		segments = append(segments, next)
	}
}

// continuation returns the index of the String token that continues the
// segment at i, if one does.
func continuation(source string, tokens []Token, i int) (int, bool) {
	newline := false
	for j := i + 1; j < len(tokens); j++ {
		token := tokens[j]
		switch token.Kind {
		case Whitespace:
			newline = newline || strings.ContainsAny(token.Text(source), "\n\r")
		case LineComment:
		case String:
			return j, newline && source[token.Start] == '\''
		default:
			return 0, false
		}
	}
	return 0, false
}

// unicodeEscapeClause reads the UESCAPE clause that may start at tokens[i],
// after white space and comments, and returns the escape character it names,
// or a backslash where there is none, and the index after the clause. The
// clause names any string constant but a Unicode escape string, holding one
// character that cannot be read as part of an escape or as the end of the
// text: not a hex digit, a plus sign, a quote or white space.
func unicodeEscapeClause(source string, tokens []Token, i int, options Options) (byte, int, bool) {
	keyword := skipGap(tokens, i)
	if keyword >= len(tokens) || tokens[keyword].Kind != Word || !strings.EqualFold(tokens[keyword].Text(source), "UESCAPE") {
		return '\\', i, true
	}
	constant := skipGap(tokens, keyword+1)
	if constant >= len(tokens) || !isConstant(tokens[constant]) || strings.HasPrefix(strings.ToUpper(tokens[constant].Text(source)), "U&") {
		return 0, 0, false
	}
	text, next, ok := Value(source, tokens, constant, options)
	if !ok || len(text) != 1 || isHexDigit(text[0]) || strings.IndexByte("+'\"", text[0]) >= 0 || isSpace(text[0]) {
		return 0, 0, false
	}
	return text[0], next, true
}

// isConstant reports a token the server reads as a string constant.
func isConstant(token Token) bool { return token.Kind == String || token.Kind == DollarString }

// skipGap returns the index of the first token from i that is neither white
// space nor a comment.
func skipGap(tokens []Token, i int) int {
	for i < len(tokens) && (tokens[i].Kind == Whitespace || tokens[i].Kind == LineComment || tokens[i].Kind == BlockComment) {
		i++
	}
	return i
}

// segmentText returns a quoted segment from its opening quote, its prefix
// left out.
func segmentText(source string, token Token) string {
	text := token.Text(source)
	return text[strings.IndexByte(text, '\''):]
}

// undoubled returns the text between the quotes of quoted, a doubled quote
// read as one.
func undoubled(quoted string, quote byte) string {
	body := quoted[1 : len(quoted)-1]
	q := string(quote)
	return strings.ReplaceAll(body, q+q, q)
}

// backslashEscaped returns the text between the quotes of quoted with
// PostgreSQL's backslash escapes read: \b \f \n \r \t, one to three octal
// digits, \x with one or two hex digits, \u with four and \U with eight, and
// any other character standing for itself.
func backslashEscaped(quoted string) (string, bool) {
	body := quoted[1 : len(quoted)-1]
	var text strings.Builder
	for i := 0; i < len(body); i++ {
		switch {
		case body[i] == '\\' && i+1 < len(body):
			decoded, width, ok := backslashEscape(body[i+1:])
			if !ok {
				return "", false
			}
			text.WriteString(decoded)
			i += width
		case body[i] == '\'':
			text.WriteByte('\'')
			i++
		default:
			text.WriteByte(body[i])
		}
	}
	return text.String(), true
}

// simpleEscapes are the escapes that stand for one control character.
var simpleEscapes = map[byte]string{'b': "\b", 'f': "\f", 'n': "\n", 'r': "\r", 't': "\t"}

// backslashEscape reads one escape; text starts after the backslash.
func backslashEscape(text string) (string, int, bool) {
	if simple, found := simpleEscapes[text[0]]; found {
		return simple, 1, true
	}
	switch text[0] {
	case 'x':
		if digits := leadingDigits(text[1:], 2, isHexDigit); digits > 0 {
			return string([]byte{byteNumber(text[1:1+digits], 16)}), 1 + digits, true
		}
	case 'u', 'U':
		return codePoint(text, '\\', escapeStringCodePoint)
	}
	if digits := leadingDigits(text, 3, isOctalDigit); digits > 0 {
		return string([]byte{byteNumber(text[:digits], 8)}), digits, true
	}
	_, size := utf8.DecodeRuneInString(text)
	return text[:size], size, true
}

// escapeStringCodePoint reads \u with four hex digits or \U with eight; text
// starts at the u or U.
func escapeStringCodePoint(text string) (rune, int, bool) {
	width := 4
	if text[0] == 'U' {
		width = 8
	}
	if leadingDigits(text[1:], width, isHexDigit) != width {
		return 0, 0, false
	}
	return hexNumber(text[1 : 1+width]), 1 + width, true
}

// unicodeEscapes reads the escapes of a Unicode escape string or identifier:
// the escape character and four hex digits, or a plus sign and six, a doubled
// escape character standing for one. Any other use of the escape character is
// refused, as the server refuses it.
func unicodeEscapes(raw string, escape byte) (string, bool) {
	var text strings.Builder
	for i := 0; i < len(raw); i++ {
		switch {
		case raw[i] != escape:
			text.WriteByte(raw[i])
		case i+1 < len(raw) && raw[i+1] == escape:
			text.WriteByte(escape)
			i++
		default:
			decoded, width, ok := codePoint(raw[i+1:], escape, unicodeEscapeCodePoint)
			if !ok {
				return "", false
			}
			text.WriteString(decoded)
			i += width
		}
	}
	return text.String(), true
}

// unicodeEscapeCodePoint reads four hex digits, or a plus sign and six; text
// starts after the escape character.
func unicodeEscapeCodePoint(text string) (rune, int, bool) {
	start, width := 0, 4
	if strings.HasPrefix(text, "+") {
		start, width = 1, 6
	}
	if leadingDigits(text[start:], width, isHexDigit) != width {
		return 0, 0, false
	}
	return hexNumber(text[start : start+width]), start + width, true
}

// codePoint reads the code point at the start of text with read. A high
// surrogate is read with the low surrogate that must follow it as the next
// escape. Zero, a value past U+10FFFF and a surrogate left alone are refused.
func codePoint(text string, escape byte, read func(string) (rune, int, bool)) (string, int, bool) {
	if text == "" {
		return "", 0, false
	}
	value, width, ok := read(text)
	if !ok {
		return "", 0, false
	}
	if value >= 0xD800 && value <= 0xDBFF {
		if width >= len(text) || text[width] != escape || width+1 >= len(text) {
			return "", 0, false
		}
		low, lowWidth, ok := read(text[width+1:])
		if !ok || low < 0xDC00 || low > 0xDFFF {
			return "", 0, false
		}
		value = (value-0xD800)<<10 + (low - 0xDC00) + 0x10000
		width += 1 + lowWidth
	}
	if value <= 0 || value > utf8.MaxRune || value >= 0xD800 && value <= 0xDFFF {
		return "", 0, false
	}
	return string(value), width, true
}

// leadingDigits counts the digits text starts with, at most limit.
func leadingDigits(text string, limit int, digit func(byte) bool) int {
	n := 0
	for n < len(text) && n < limit && digit(text[n]) {
		n++
	}
	return n
}

// hexNumber reads at most eight hex digits as a code point. Eight digits fill
// the rune's 32 bits, so a value past U+10FFFF reads as negative or as past
// utf8.MaxRune, and the caller refuses both.
func hexNumber(digits string) rune {
	var value rune
	for i := 0; i < len(digits); i++ {
		value = value<<4 + rune(digitValue(digits[i]))
	}
	return value
}

// byteNumber reads the digits of an octal or hex escape as one byte, keeping
// its low eight bits as the server does, so `\777` is 0xFF.
func byteNumber(digits string, base byte) byte {
	var value byte
	for i := 0; i < len(digits); i++ {
		value = value*base + digitValue(digits[i])
	}
	return value
}

// digitValue is the value of one octal, decimal or hex digit.
func digitValue(c byte) byte {
	switch {
	case c >= 'a':
		return c - 'a' + 10
	case c >= 'A':
		return c - 'A' + 10
	default:
		return c - '0'
	}
}

func isOctalDigit(c byte) bool { return c >= '0' && c <= '7' }
