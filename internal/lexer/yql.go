package lexer

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// nextYQLToken is NextToken under [Options.YQL].
//
// It is a scanner of its own rather than a set of branches inside the shared
// one, so that no YQL rule can change how another dialect's text is read: the
// shared scanner never sees a YQL option, and this one never consults the
// options the shared scanner reads.
func (l *Lexer) nextYQLToken() Token {
	token := l.scanYQLToken()
	if token.Type != TokenWhitespace && token.Type != TokenEOF && !l.isTranslationSetting(token) {
		l.pastHead = true
	}
	return token
}

func (l *Lexer) scanYQLToken() Token {
	ch := l.peek()
	switch {
	case ch == 0:
		return l.emit(TokenEOF)
	case ch == ';':
		l.advance()
		return l.emit(TokenSemicolon)
	case ch == '`':
		return l.scanBacktickedIdentifier()
	case unicode.IsSpace(ch):
		return l.scanWhitespace()
	case ch == '\'' || ch == '"':
		l.consumeYQLQuoted()
		l.consumeYQLTypeSuffix()
		return l.emit(TokenString)
	case ch == '@' && l.peekNext() == '@':
		l.consumeYQLMultiline()
		l.consumeYQLTypeSuffix()
		return l.emit(TokenString)
	case ch == '-' && l.peekNext() == '-':
		comment := l.scanLineComment()
		if !l.pastHead && strings.HasPrefix(comment.Value, "--!") {
			comment.Type = TokenUnknown
		}
		return comment
	case ch == '/' && l.peekNext() == '*':
		return l.scanBlockComment()
	case ch == '$':
		if isIdentifierPart(l.peekNext()) {
			return l.scanIdentifier()
		}
		return l.scanOperator()
	case isIdentifierStart(ch):
		return l.scanIdentifier()
	case ch >= '0' && ch <= '9':
		l.pos = yqlNumberEnd(l.input, l.pos)
		return l.emit(TokenIdentifier)
	default:
		return l.scanOperator()
	}
}

// yqlNumberEnd returns where the YQL number starting at start ends, by the
// grammar's DIGITS, INTEGER_VALUE and REAL rules (SQLv1Antlr4.g.in), taking
// the longest match as the grammar's lexer does:
//
//   - 0x, 0o and 0b introduce hexadecimal, octal (digits 0 to 8, as the
//     grammar writes it) and binary digits, and need at least one;
//   - an integer may carry p or u and then one of l, s, t, i, b, n;
//   - a real has a fraction, an exponent or both, and may carry f, or p
//     followed by f4, f8 or n.
//
// A number is one token with its suffix, so the word after it starts where
// the server starts it: measured on YDB 26.2.1.14, SELECT 1uFROM `dir/t`
// reads `1u FROM`, 0x1fAS is 0x1fa with the suffix s, and 1.foo is 1.f
// followed by oo.
func yqlNumberEnd(input string, start int) int {
	if end, ok := yqlPrefixedNumberEnd(input, start); ok {
		return end
	}
	digits := asciiRunEnd(input, start, isDecimalDigit)
	integerEnd := yqlIntegerSuffixEnd(input, digits)
	realEnd := -1
	switch {
	case digits < len(input) && input[digits] == '.':
		fraction := asciiRunEnd(input, digits+1, isDecimalDigit)
		realEnd = yqlRealSuffixEnd(input, yqlExponentEnd(input, fraction))
	case yqlExponentEnd(input, digits) > digits:
		realEnd = yqlRealSuffixEnd(input, yqlExponentEnd(input, digits))
	}
	return max(integerEnd, realEnd)
}

// yqlPrefixedNumberEnd reads 0x, 0o or 0b and the digits after it, and
// reports false when no digit of the base follows, which leaves a decimal 0.
func yqlPrefixedNumberEnd(input string, start int) (int, bool) {
	if input[start] != '0' || start+1 >= len(input) {
		return 0, false
	}
	var digit func(byte) bool
	switch input[start+1] {
	case 'x', 'X':
		digit = isHexDigit
	case 'o', 'O':
		digit = func(c byte) bool { return c >= '0' && c <= '8' }
	case 'b', 'B':
		digit = func(c byte) bool { return c == '0' || c == '1' }
	default:
		return 0, false
	}
	end := asciiRunEnd(input, start+2, digit)
	if end == start+2 {
		return 0, false
	}
	return yqlIntegerSuffixEnd(input, end), true
}

// yqlIntegerSuffixEnd reads the integer type suffix (p|u)?(l|s|t|i|b|n)?.
func yqlIntegerSuffixEnd(input string, pos int) int {
	if pos < len(input) && strings.ContainsRune("pPuU", rune(input[pos])) {
		pos++
	}
	if pos < len(input) && strings.ContainsRune("lLsStTiIbBnN", rune(input[pos])) {
		pos++
	}
	return pos
}

// yqlExponentEnd reads e, an optional sign and at least one digit, and
// returns pos unchanged when they are not all there.
func yqlExponentEnd(input string, pos int) int {
	if pos >= len(input) || (input[pos] != 'e' && input[pos] != 'E') {
		return pos
	}
	digits := pos + 1
	if digits < len(input) && (input[digits] == '+' || input[digits] == '-') {
		digits++
	}
	end := asciiRunEnd(input, digits, isDecimalDigit)
	if end == digits {
		return pos
	}
	return end
}

// yqlRealSuffixEnd reads the real type suffix: f, or p followed by an
// optional f4, f8 or n.
func yqlRealSuffixEnd(input string, pos int) int {
	switch {
	case pos >= len(input):
		return pos
	case input[pos] == 'f' || input[pos] == 'F':
		return pos + 1
	case input[pos] != 'p' && input[pos] != 'P':
		return pos
	}
	pos++
	switch {
	case pos+1 < len(input) && (input[pos] == 'f' || input[pos] == 'F') && (input[pos+1] == '4' || input[pos+1] == '8'):
		return pos + 2
	case pos < len(input) && (input[pos] == 'n' || input[pos] == 'N'):
		return pos + 1
	default:
		return pos
	}
}

func asciiRunEnd(input string, pos int, accept func(byte) bool) int {
	for pos < len(input) && accept(input[pos]) {
		pos++
	}
	return pos
}

func isDecimalDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

// isTranslationSetting reports whether token is a --! comment the head of a
// YQL text carries, which [Lexer.scanYQLToken] emits as TokenUnknown.
func (l *Lexer) isTranslationSetting(token Token) bool {
	return token.Type == TokenUnknown && strings.HasPrefix(token.Value, "--!")
}

// consumeYQLQuoted advances over a '...' or "..." literal. A backslash
// escapes the next character; the first unescaped quote of the kind that
// opened the literal closes it.
func (l *Lexer) consumeYQLQuoted() {
	quote := l.advance()
	for {
		switch l.peek() {
		case 0:
			return
		case quote:
			l.advance()
			return
		case '\\':
			l.advance()
			if l.peek() != 0 {
				l.advance()
			}
		default:
			l.advance()
		}
	}
}

// consumeYQLMultiline advances over an @@...@@ literal, which the grammar
// writes as (@@ .*? @@)+ @?: one segment closes at the first @@ after it
// opens, another segment written directly against it continues the literal,
// and that is what makes @@@@ stand for @@. A single @ after the last segment
// belongs to the literal too, which is how its text can end with @.
//
// An unterminated literal runs to the end of the input, as an unterminated
// quoted literal does.
func (l *Lexer) consumeYQLMultiline() {
	for {
		closing := strings.Index(l.input[l.pos+2:], "@@")
		if closing < 0 {
			l.pos = len(l.input)
			return
		}
		l.pos += 2 + closing + 2
		if !yqlMultilineSegmentAt(l.input, l.pos) {
			break
		}
	}
	if l.peek() == '@' {
		l.advance()
	}
}

// yqlMultilineSegmentAt reports whether a complete @@...@@ segment starts at
// pos.
func yqlMultilineSegmentAt(input string, pos int) bool {
	return strings.HasPrefix(input[pos:], "@@") && strings.Contains(input[pos+2:], "@@")
}

// consumeYQLTypeSuffix advances over the type suffix written against a string
// literal: s, u, y, j, or p followed by an optional t, b or v. The grammar
// matches the longest of these, so "x"pt carries pt and "x"ux carries u,
// leaving x to start the next token.
func (l *Lexer) consumeYQLTypeSuffix() {
	switch unicode.ToLower(l.peek()) {
	case 's', 'u', 'y', 'j':
		l.advance()
	case 'p':
		l.advance()
		switch unicode.ToLower(l.peek()) {
		case 't', 'b', 'v':
			l.advance()
		}
	}
}

// yqlStringValue is [StringValue] under [Options.YQL]: the text of a quoted
// or multiline literal, with its type suffix set aside.
func yqlStringValue(token string) (string, bool) {
	if strings.HasPrefix(token, "@@") {
		return yqlMultilineValue(token)
	}
	if len(token) < 2 || (token[0] != '\'' && token[0] != '"') {
		return "", false
	}
	quote := token[0]
	var text strings.Builder
	for i := 1; i < len(token); i++ {
		switch token[i] {
		case quote:
			if !isYQLTypeSuffix(token[i+1:]) {
				return "", false
			}
			return text.String(), true
		case '\\':
			if i+1 >= len(token) {
				return "", false
			}
			decoded, width, ok := yqlEscape(token[i+1:])
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

// yqlMultilineValue answers the text of an @@...@@ literal: the segments
// joined by the @@ each later one stands for, and the trailing @ the literal
// may end with.
func yqlMultilineValue(token string) (string, bool) {
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
		if !yqlMultilineSegmentAt(token, pos) {
			break
		}
	}
	if strings.HasPrefix(token[pos:], "@") {
		text.WriteByte('@')
		pos++
	}
	if !isYQLTypeSuffix(token[pos:]) {
		return "", false
	}
	return text.String(), true
}

// isYQLTypeSuffix reports whether rest is empty or one whole type suffix.
func isYQLTypeSuffix(rest string) bool {
	switch strings.ToLower(rest) {
	case "", "s", "u", "y", "j", "p", "pt", "pb", "pv":
		return true
	default:
		return false
	}
}

// yqlEscape decodes a backslash escape the way YDB 26.2.1.14 reads one,
// measured: \a \b \f \n \r \t \v; \x with exactly two hex digits; three octal
// digits starting with 0 to 3; \u with four hex digits and \U with eight,
// naming a code point that is not a surrogate. Any other character stands for
// itself, so \q is q and \4 is 4. An escape short of its digits is refused,
// as the server refuses it.
func yqlEscape(text string) (string, int, bool) {
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
		value, width, ok := escapeStringCodePoint(text)
		if !ok || value > utf8.MaxRune || isHighSurrogate(value) || isLowSurrogate(value) {
			return "", 0, false
		}
		return string(value), width, true
	}
	_, size := utf8.DecodeRuneInString(text)
	return text[:size], size, true
}
