// Package yql reads YDB's query language, YQL, in its default lexer mode, the
// one the YDB documentation calls C++ mode. It serves the lexer a dialect's
// descriptor names as descriptor.LexerYQL: internal/dialectlexer gives such a
// dialect this package's [Scanner], so every reader of the dialect's text
// through internal/lexer gets YQL's tokens.
//
// The rules, from the YQL grammar (SQLv1Antlr4.g.in: STRING_VALUE,
// ID_QUOTED, COMMENT) and measured on YDB 26.2.1.14:
//
//   - '...' and "..." are both string literals. A backslash escapes the next
//     character. A doubled quote is not an escape: the first quote closes the
//     literal and the second opens another, so a literal ends at the first
//     quote no backslash escapes.
//   - A type suffix written against a literal belongs to it: s, u, y, j, p,
//     pt, pb or pv, in either case, so "x"u is one Utf8 literal.
//   - @@...@@ is a multiline string literal. Its text is taken as written,
//     and @@@@ inside it stands for @@.
//   - `...` is an identifier, where a backslash escapes the next character
//     and a doubled backtick stands for one.
//   - -- starts a line comment and /* a block comment, which does not nest.
//     # is not a comment.
//   - $name is a named expression or a parameter, never a dollar-quoted
//     string.
//   - A -- comment starting with ! at the head of the text, before any other
//     token, is a translation setting such as --!ansi_lexer or --!syntax_v1.
//     The server reads it before the query, so it is emitted as
//     lexer.TokenUnknown, as an executable comment is, and a caller that
//     removes comments keeps it.
//
// The ANSI lexer mode that --!ansi_lexer selects is not modeled: under it
// "..." is an identifier, a doubled quote is an escape, a backslash is an
// ordinary character, and block comments nest. A caller that has to agree
// with the server about the text refuses the setting instead.
//
// The reader is whole in itself. Whitespace, names, operators, comments and
// the digits of an escape are read by this package's own copies of those
// rules, on purpose: they are YQL's rules, so a change to a rule of the
// shared lexer does not change how YQL is read.
package yql

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/lexer"
	"ptah.run/internal/lexerext"
)

// Services returns the YQL lexer's services.
func Services() lexerext.Services {
	return lexerext.Services{Lexers: []string{descriptor.LexerYQL}, Scanner: Scanner{}}
}

// Scanner reads YQL. Its zero value is ready to use, and a lexer.Options
// holding it stays comparable.
type Scanner struct{}

// Scan returns the reader of text from start, at the head of the text when
// head is set.
func (Scanner) Scan(text string, start int, head bool) lexer.TokenReader {
	return &reader{input: text, pos: start, start: start, pastHead: !head}
}

// StringValue answers the text of a quoted or multiline literal, with its
// type suffix set aside; see [StringValue].
func (Scanner) StringValue(token string) (string, bool) {
	return StringValue(token)
}

// IdentifierValue answers the name an identifier stands for; see
// [IdentifierValue].
func (Scanner) IdentifierValue(token string) (string, bool) {
	return IdentifierValue(token)
}

// reader is the token stream of one YQL text.
type reader struct {
	input string
	pos   int
	start int
	// pastHead records that a token other than whitespace or a translation
	// setting has been emitted, which ends the head of the text where the
	// server reads its translation settings.
	pastHead bool
}

// NextToken returns the next token of the text.
func (r *reader) NextToken() lexer.Token {
	token := r.scanToken()
	if token.Type != lexer.TokenWhitespace && token.Type != lexer.TokenEOF && !isTranslationSetting(token) {
		r.pastHead = true
	}
	return token
}

func (r *reader) scanToken() lexer.Token {
	ch := r.peek()
	switch {
	case ch == 0:
		return r.emit(lexer.TokenEOF)
	case ch == ';':
		r.advance()
		return r.emit(lexer.TokenSemicolon)
	case ch == '`':
		return r.scanBacktickedIdentifier()
	case unicode.IsSpace(ch):
		return r.scanWhitespace()
	case ch == '\'' || ch == '"':
		r.consumeQuoted()
		r.consumeTypeSuffix()
		return r.emit(lexer.TokenString)
	case ch == '@' && r.peekNext() == '@':
		r.consumeMultiline()
		r.consumeTypeSuffix()
		return r.emit(lexer.TokenString)
	case ch == '-' && r.peekNext() == '-':
		comment := r.scanLineComment()
		if !r.pastHead && strings.HasPrefix(comment.Value, "--!") {
			comment.Type = lexer.TokenUnknown
		}
		return comment
	case ch == '/' && r.peekNext() == '*':
		return r.scanBlockComment()
	case ch == '$':
		if isIdentifierPart(r.peekNext()) {
			return r.scanIdentifier()
		}
		return r.scanOperator()
	case isIdentifierStart(ch):
		return r.scanIdentifier()
	case ch >= '0' && ch <= '9':
		r.pos = numberEnd(r.input, r.pos)
		return r.emit(lexer.TokenIdentifier)
	default:
		return r.scanOperator()
	}
}

// peek returns the character at the current position without advancing. A
// NUL character reads as the end of the text.
func (r *reader) peek() rune {
	if r.pos >= len(r.input) {
		return 0
	}
	ch, _ := utf8.DecodeRuneInString(r.input[r.pos:])
	return ch
}

// peekNext returns the character after the current one without advancing.
func (r *reader) peekNext() rune {
	if r.pos >= len(r.input) {
		return 0
	}
	_, size := utf8.DecodeRuneInString(r.input[r.pos:])
	next := r.pos + size
	if next >= len(r.input) {
		return 0
	}
	ch, _ := utf8.DecodeRuneInString(r.input[next:])
	return ch
}

// advance moves past the current character and returns it.
func (r *reader) advance() rune {
	if r.pos >= len(r.input) {
		return 0
	}
	ch, size := utf8.DecodeRuneInString(r.input[r.pos:])
	r.pos += size
	return ch
}

// emit returns the token of the text read since the last one.
func (r *reader) emit(tokenType lexer.TokenType) lexer.Token {
	token := lexer.Token{
		Type:  tokenType,
		Value: r.input[r.start:r.pos],
		Start: r.start,
		End:   r.pos,
	}
	r.start = r.pos
	return token
}

func (r *reader) scanWhitespace() lexer.Token {
	for unicode.IsSpace(r.peek()) {
		r.advance()
	}
	return r.emit(lexer.TokenWhitespace)
}

// scanLineComment reads a -- comment to the end of its line.
func (r *reader) scanLineComment() lexer.Token {
	r.advance()
	r.advance()
	for {
		ch := r.peek()
		if ch == 0 || ch == '\n' || ch == '\r' {
			break
		}
		r.advance()
	}
	return r.emit(lexer.TokenComment)
}

// scanBlockComment reads a /* comment through the first */, so a comment
// does not nest. An unterminated one runs to the end of the text.
func (r *reader) scanBlockComment() lexer.Token {
	r.advance()
	r.advance()
	for {
		ch := r.peek()
		if ch == 0 {
			break
		}
		if ch == '*' && r.peekNext() == '/' {
			r.advance()
			r.advance()
			break
		}
		r.advance()
	}
	return r.emit(lexer.TokenComment)
}

func (r *reader) scanIdentifier() lexer.Token {
	for isIdentifierPart(r.peek()) {
		r.advance()
	}
	return r.emit(lexer.TokenIdentifier)
}

func (r *reader) scanOperator() lexer.Token {
	r.advance()
	return r.emit(lexer.TokenOperator)
}

// scanBacktickedIdentifier reads a `...` name, where a doubled backtick and a
// backslash with the character after it stay inside. An unterminated one runs
// to the end of the text.
func (r *reader) scanBacktickedIdentifier() lexer.Token {
	r.advance()
	for {
		ch := r.peek()
		if ch == 0 {
			break
		}
		if ch == '`' {
			if r.peekNext() == '`' {
				r.advance()
				r.advance()
				continue
			}
			r.advance()
			break
		}
		if ch == '\\' {
			r.advance()
			if r.peek() != 0 {
				r.advance()
			}
		} else {
			r.advance()
		}
	}
	return r.emit(lexer.TokenIdentifier)
}

func isIdentifierStart(ch rune) bool {
	return unicode.IsLetter(ch) || ch == '_' || ch == '$'
}

func isIdentifierPart(ch rune) bool {
	return unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_' || ch == '$'
}

// numberEnd returns where the YQL number starting at start ends, by the
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
func numberEnd(input string, start int) int {
	if end, ok := prefixedNumberEnd(input, start); ok {
		return end
	}
	digits := asciiRunEnd(input, start, isDecimalDigit)
	integerEnd := integerSuffixEnd(input, digits)
	realEnd := -1
	switch {
	case digits < len(input) && input[digits] == '.':
		fraction := asciiRunEnd(input, digits+1, isDecimalDigit)
		realEnd = realSuffixEnd(input, exponentEnd(input, fraction))
	case exponentEnd(input, digits) > digits:
		realEnd = realSuffixEnd(input, exponentEnd(input, digits))
	}
	return max(integerEnd, realEnd)
}

// prefixedNumberEnd reads 0x, 0o or 0b and the digits after it, and reports
// false when no digit of the base follows, which leaves a decimal 0.
func prefixedNumberEnd(input string, start int) (int, bool) {
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
	return integerSuffixEnd(input, end), true
}

// integerSuffixEnd reads the integer type suffix (p|u)?(l|s|t|i|b|n)?.
func integerSuffixEnd(input string, pos int) int {
	if pos < len(input) && strings.ContainsRune("pPuU", rune(input[pos])) {
		pos++
	}
	if pos < len(input) && strings.ContainsRune("lLsStTiIbBnN", rune(input[pos])) {
		pos++
	}
	return pos
}

// exponentEnd reads e, an optional sign and at least one digit, and returns
// pos unchanged when they are not all there.
func exponentEnd(input string, pos int) int {
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

// realSuffixEnd reads the real type suffix: f, or p followed by an optional
// f4, f8 or n.
func realSuffixEnd(input string, pos int) int {
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

func isHexDigit(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

func isOctalDigit(c byte) bool {
	return c >= '0' && c <= '7'
}

// isTranslationSetting reports whether token is a --! comment the head of a
// text carries, which the reader emits as lexer.TokenUnknown.
func isTranslationSetting(token lexer.Token) bool {
	return token.Type == lexer.TokenUnknown && strings.HasPrefix(token.Value, "--!")
}

// consumeQuoted advances over a '...' or "..." literal. A backslash escapes
// the next character; the first unescaped quote of the kind that opened the
// literal closes it.
func (r *reader) consumeQuoted() {
	quote := r.advance()
	for {
		switch r.peek() {
		case 0:
			return
		case quote:
			r.advance()
			return
		case '\\':
			r.advance()
			if r.peek() != 0 {
				r.advance()
			}
		default:
			r.advance()
		}
	}
}

// consumeMultiline advances over an @@...@@ literal, which the grammar writes
// as (@@ .*? @@)+ @?: one segment closes at the first @@ after it opens,
// another segment written directly against it continues the literal, and that
// is what makes @@@@ stand for @@. A single @ after the last segment belongs
// to the literal too, which is how its text can end with @.
//
// An unterminated literal runs to the end of the input, as an unterminated
// quoted literal does.
func (r *reader) consumeMultiline() {
	for {
		closing := strings.Index(r.input[r.pos+2:], "@@")
		if closing < 0 {
			r.pos = len(r.input)
			return
		}
		r.pos += 2 + closing + 2
		if !multilineSegmentAt(r.input, r.pos) {
			break
		}
	}
	if r.peek() == '@' {
		r.advance()
	}
}

// multilineSegmentAt reports whether a complete @@...@@ segment starts at
// pos.
func multilineSegmentAt(input string, pos int) bool {
	return strings.HasPrefix(input[pos:], "@@") && strings.Contains(input[pos+2:], "@@")
}

// consumeTypeSuffix advances over the type suffix written against a string
// literal: s, u, y, j, or p followed by an optional t, b or v. The grammar
// matches the longest of these, so "x"pt carries pt and "x"ux carries u,
// leaving x to start the next token.
func (r *reader) consumeTypeSuffix() {
	switch unicode.ToLower(r.peek()) {
	case 's', 'u', 'y', 'j':
		r.advance()
	case 'p':
		r.advance()
		switch unicode.ToLower(r.peek()) {
		case 't', 'b', 'v':
			r.advance()
		}
	}
}
