package lexer

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// TokenType represents the type of SQL token
type TokenType int

const (
	TokenUnknown TokenType = iota
	TokenString
	TokenComment
	TokenSemicolon
	TokenWhitespace
	TokenIdentifier
	TokenOperator
	TokenEOF
)

func (tt TokenType) String() string {
	switch tt {
	case TokenUnknown:
		return "Unknown"
	case TokenString:
		return "String"
	case TokenComment:
		return "Comment"
	case TokenSemicolon:
		return "Semicolon"
	case TokenWhitespace:
		return "Whitespace"
	case TokenIdentifier:
		return "Identifier"
	case TokenOperator:
		return "Operator"
	case TokenEOF:
		return "EOF"
	default:
		return "Unknown"
	}
}

// Token represents a single SQL token
type Token struct {
	Type  TokenType
	Value string
	Start int
	End   int
}

func (tt *Token) MatchOperatorValue(value string) bool {
	if tt.Type != TokenOperator {
		return false
	}
	return strings.EqualFold(tt.Value, value)
}

func (tt *Token) MatchIdentifierValue(value string) bool {
	if tt.Type != TokenIdentifier {
		return false
	}
	return strings.EqualFold(tt.Value, value)
}

// Lexer tokenizes SQL input
type Lexer struct {
	input string
	pos   int
	start int
}

// NewLexer creates a new SQL lexer
func NewLexer(input string) *Lexer {
	return &Lexer{
		input: input,
		pos:   0,
		start: 0,
	}
}

// peek returns the character at the current position without advancing
func (l *Lexer) peek() rune {
	if l.pos >= len(l.input) {
		return 0
	}
	ch, _ := utf8.DecodeRuneInString(l.input[l.pos:])
	return ch
}

// peekNext returns the character at the next position without advancing
func (l *Lexer) peekNext() rune {
	if l.pos >= len(l.input) {
		return 0
	}
	_, size := utf8.DecodeRuneInString(l.input[l.pos:])
	next := l.pos + size
	if next >= len(l.input) {
		return 0
	}
	ch, _ := utf8.DecodeRuneInString(l.input[next:])
	return ch
}

// peekAfterNext returns the rune after peekNext without advancing.
func (l *Lexer) peekAfterNext() rune {
	if l.pos >= len(l.input) {
		return 0
	}
	_, size := utf8.DecodeRuneInString(l.input[l.pos:])
	next := l.pos + size
	if next >= len(l.input) {
		return 0
	}
	_, nextSize := utf8.DecodeRuneInString(l.input[next:])
	afterNext := next + nextSize
	if afterNext >= len(l.input) {
		return 0
	}
	ch, _ := utf8.DecodeRuneInString(l.input[afterNext:])
	return ch
}

// advance moves to the next character and returns it
func (l *Lexer) advance() rune {
	if l.pos >= len(l.input) {
		return 0
	}
	ch, size := utf8.DecodeRuneInString(l.input[l.pos:])
	l.pos += size
	return ch
}

// emit creates a token with the current accumulated text
func (l *Lexer) emit(tokenType TokenType) Token {
	token := Token{
		Type:  tokenType,
		Value: l.input[l.start:l.pos],
		Start: l.start,
		End:   l.pos,
	}
	l.start = l.pos
	return token
}

// ignore skips the current accumulated text
func (l *Lexer) ignore() { //nolint:unused // TODO: not used yet
	l.start = l.pos
}

// NextToken returns the next token from the input
func (l *Lexer) NextToken() Token {
	for {
		ch := l.peek()

		if ch == 0 {
			return l.emit(TokenEOF)
		}

		switch {
		case unicode.IsSpace(ch):
			return l.scanWhitespace()
		case ch == ';':
			l.advance()
			return l.emit(TokenSemicolon)
		case ch == '\'' || ch == '"':
			return l.scanString()
		case ch == '`':
			return l.scanBacktickedIdentifier()
		case ch == '$':
			// Check for PostgreSQL dollar-quoted strings
			if l.isDollarQuotedString() {
				return l.scanDollarQuotedString()
			}
			if isIdentifierPart(l.peekNext()) {
				return l.scanIdentifier()
			}
			return l.scanOperator()
		case ch == '-' && l.peekNext() == '-':
			return l.scanLineComment()
		case ch == '#' && !isHashOperatorContinuation(l.peekNext()):
			return l.scanHashLineComment()
		case ch == '/' && l.peekNext() == '*':
			return l.scanBlockComment()
		case isIdentifierStart(ch):
			return l.scanIdentifier()
		case unicode.IsDigit(ch):
			return l.scanNumber()
		default:
			return l.scanOperator()
		}
	}
}

// scanWhitespace scans whitespace characters
func (l *Lexer) scanWhitespace() Token {
	for unicode.IsSpace(l.peek()) {
		l.advance()
	}
	return l.emit(TokenWhitespace)
}

// scanString scans a quoted string literal
func (l *Lexer) scanString() Token {
	quote := l.advance() // consume opening quote

	for {
		ch := l.peek()
		if ch == 0 {
			// Unterminated string - return what we have
			break
		}

		if ch == quote {
			l.advance() // consume closing quote
			break
		}

		if ch == '\\' {
			if quote == '\'' && l.peekNext() == quote && isStringTerminator(l.peekAfterNext()) {
				l.advance()
				continue
			}
			l.advance() // consume backslash
			if l.peek() != 0 {
				l.advance() // consume escaped character
			}
		} else {
			l.advance()
		}
	}

	return l.emit(TokenString)
}

func isStringTerminator(ch rune) bool {
	return ch == 0 || ch == ';' || ch == ',' || ch == ')'
}

// scanLineComment scans a line comment (-- comment)
func (l *Lexer) scanLineComment() Token {
	l.advance() // consume first -
	l.advance() // consume second -

	for {
		ch := l.peek()
		if ch == 0 || ch == '\n' || ch == '\r' {
			break
		}
		l.advance()
	}

	return l.emit(TokenComment)
}

// scanHashLineComment scans a MySQL-style line comment (# comment).
func (l *Lexer) scanHashLineComment() Token {
	l.advance() // consume #

	for {
		ch := l.peek()
		if ch == 0 || ch == '\n' || ch == '\r' {
			break
		}
		l.advance()
	}

	return l.emit(TokenComment)
}

func isHashOperatorContinuation(ch rune) bool {
	switch ch {
	case '!', '#', '%', '&', '*', '+', '-', '/', '<', '=', '>', '?', '@', '^', '|', '~':
		return true
	default:
		return false
	}
}

// scanBlockComment scans a block comment (/* comment */)
func (l *Lexer) scanBlockComment() Token {
	l.advance() // consume /
	l.advance() // consume *

	for {
		ch := l.peek()
		if ch == 0 {
			// Unterminated comment - return what we have
			break
		}

		if ch == '*' && l.peekNext() == '/' {
			l.advance() // consume *
			l.advance() // consume /
			break
		}

		l.advance()
	}

	return l.emit(TokenComment)
}

// scanIdentifier scans an identifier or keyword
func (l *Lexer) scanIdentifier() Token {
	for {
		ch := l.peek()
		if !isIdentifierPart(ch) {
			break
		}
		l.advance()
	}
	return l.emit(TokenIdentifier)
}

func isIdentifierStart(ch rune) bool {
	return unicode.IsLetter(ch) || ch == '_' || ch == '$'
}

func isIdentifierPart(ch rune) bool {
	return unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_' || ch == '$'
}

// scanBacktickedIdentifier scans a backtick-quoted identifier (MySQL style)
func (l *Lexer) scanBacktickedIdentifier() Token {
	l.advance() // consume opening backtick

	for {
		ch := l.peek()
		if ch == 0 {
			// Unterminated identifier - return what we have
			break
		}

		if ch == '`' {
			l.advance() // consume closing backtick
			break
		}

		if ch == '\\' {
			l.advance() // consume backslash
			if l.peek() != 0 {
				l.advance() // consume escaped character
			}
		} else {
			l.advance()
		}
	}

	return l.emit(TokenIdentifier)
}

// scanNumber scans a numeric literal
func (l *Lexer) scanNumber() Token {
	for {
		ch := l.peek()
		if !unicode.IsDigit(ch) && ch != '.' {
			break
		}
		l.advance()
	}
	return l.emit(TokenIdentifier) // Treat numbers as identifiers for simplicity
}

// scanOperator scans operators and other symbols
func (l *Lexer) scanOperator() Token {
	l.advance()
	return l.emit(TokenOperator)
}

// isDollarQuotedString checks if the current position starts a PostgreSQL dollar-quoted string
func (l *Lexer) isDollarQuotedString() bool {
	if l.peek() != '$' {
		return false
	}

	// Look ahead to find the closing $ of the opening tag
	pos := l.pos + 1
	for pos < len(l.input) {
		ch, size := utf8.DecodeRuneInString(l.input[pos:])
		if ch == '$' {
			// Found potential closing $ of opening tag
			return true
		}
		if !unicode.IsLetter(ch) && !unicode.IsDigit(ch) && ch != '_' {
			// Invalid character in tag name
			return false
		}
		pos += size
	}
	return false
}

// scanDollarQuotedString scans a PostgreSQL dollar-quoted string literal
func (l *Lexer) scanDollarQuotedString() Token {
	// Extract the opening tag (e.g., "$$" or "$tag$")
	tagStart := l.pos
	l.advance() // consume first $

	// Scan tag name (if any)
	for {
		ch := l.peek()
		if ch == '$' {
			l.advance() // consume closing $
			break
		}
		if !unicode.IsLetter(ch) && !unicode.IsDigit(ch) && ch != '_' {
			// Invalid tag character - treat as regular operator
			l.pos = tagStart + 1
			return l.emit(TokenOperator)
		}
		l.advance()
	}

	// Extract the complete opening tag
	openingTag := l.input[tagStart:l.pos]

	// Scan until we find the matching closing tag
	for {
		ch := l.peek()
		if ch == 0 {
			// Unterminated dollar-quoted string - return what we have
			break
		}

		if ch == '$' {
			// Check if this might be the start of the closing tag
			remainingInput := l.input[l.pos:]
			if strings.HasPrefix(remainingInput, openingTag) {
				// Found matching closing tag
				tagEnd := l.pos + len(openingTag)
				for l.pos < tagEnd {
					l.advance()
				}
				break
			}
		}

		l.advance()
	}

	return l.emit(TokenString)
}
