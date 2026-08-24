// Package dbmlparse reads DBML into Ptah's schema model.
//
// It is a format adapter: it produces [goschema.Database] and decides no schema
// semantics of its own. There is no SQL conversion, no JavaScript runtime and no
// subprocess -- the grammar is read here (stokaro/ptah#2065).
//
// Every diagnostic carries a file, a line and a column. A schema file is
// written by a person, and "unexpected token" without a position is a message
// that makes them search rather than look.
package dbmlparse

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// tokenKind classifies one lexical token.
type tokenKind int

const (
	tokenEOF tokenKind = iota
	// tokenWord is a bare identifier or keyword, in the case it was written.
	tokenWord
	// tokenQuoted is a double-quoted identifier, with the quotes removed.
	tokenQuoted
	// tokenString is a single-quoted or triple-quoted literal, unescaped.
	tokenString
	// tokenExpr is a backtick-quoted expression, with the backticks removed.
	tokenExpr
	// tokenNumber is a numeric literal, in the spelling it was written.
	tokenNumber
	// tokenPunct is one of { } [ ] ( ) , : . < > - and the two-character
	// relationship operators.
	tokenPunct
)

// token is one lexical token and where it was written.
type token struct {
	kind tokenKind
	text string
	line int
	col  int
}

// position renders a token's place for a diagnostic.
func (t token) position(file string) string {
	if file == "" {
		return fmt.Sprintf("%d:%d", t.line, t.col)
	}
	return fmt.Sprintf("%s:%d:%d", file, t.line, t.col)
}

// describe names a token the way a message should refer to it.
func (t token) describe() string {
	if t.kind == tokenEOF {
		return "end of input"
	}
	return fmt.Sprintf("%q", t.text)
}

// lexer walks the document one rune at a time, tracking line and column so
// every token can say where it came from.
type lexer struct {
	src  string
	pos  int
	line int
	col  int
}

func newLexer(src string) *lexer {
	return &lexer{src: src, line: 1, col: 1}
}

// next reads the next token, skipping whitespace and comments.
func (l *lexer) next() (token, error) {
	l.skipTrivia()
	if l.pos >= len(l.src) {
		return token{kind: tokenEOF, line: l.line, col: l.col}, nil
	}
	line, col := l.line, l.col
	r, width := utf8.DecodeRuneInString(l.src[l.pos:])

	switch {
	case r == '"':
		return l.quoted('"', tokenQuoted, line, col)
	case r == '`':
		return l.quoted('`', tokenExpr, line, col)
	case r == '\'':
		return l.stringLiteral(line, col)
	case unicode.IsDigit(r):
		return l.number(line, col), nil
	case isWordRune(r):
		return l.word(line, col), nil
	default:
		l.advance(width)
		return token{kind: tokenPunct, text: string(r), line: line, col: col}, nil
	}
}

// skipTrivia consumes whitespace, // line comments and /* block comments.
func (l *lexer) skipTrivia() {
	for l.pos < len(l.src) {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		switch {
		case unicode.IsSpace(r):
			l.advance(width)
		case strings.HasPrefix(l.src[l.pos:], "//"):
			for l.pos < len(l.src) && l.src[l.pos] != '\n' {
				l.advance(1)
			}
		case strings.HasPrefix(l.src[l.pos:], "/*"):
			l.advance(2)
			for l.pos < len(l.src) && !strings.HasPrefix(l.src[l.pos:], "*/") {
				_, w := utf8.DecodeRuneInString(l.src[l.pos:])
				l.advance(w)
			}
			// An unterminated block comment consumes the rest of the file
			// rather than failing: the parser then reports the construct that
			// is missing, which is the thing a reader has to fix.
			if l.pos < len(l.src) {
				l.advance(2)
			}
		default:
			return
		}
	}
}

// word reads a bare identifier or keyword.
func (l *lexer) word(line, col int) token {
	start := l.pos
	for l.pos < len(l.src) {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		if !isWordRune(r) && !unicode.IsDigit(r) {
			break
		}
		l.advance(width)
	}
	return token{kind: tokenWord, text: l.src[start:l.pos], line: line, col: col}
}

// number reads a numeric literal, decimal point included.
func (l *lexer) number(line, col int) token {
	start := l.pos
	for l.pos < len(l.src) {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		if !unicode.IsDigit(r) && r != '.' {
			break
		}
		l.advance(width)
	}
	return token{kind: tokenNumber, text: l.src[start:l.pos], line: line, col: col}
}

// quoted reads a run delimited by one character, which is how DBML writes both
// identifiers and expressions.
func (l *lexer) quoted(delim rune, kind tokenKind, line, col int) (token, error) {
	l.advance(1)
	var out strings.Builder
	for l.pos < len(l.src) {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		if r == delim {
			l.advance(width)
			return token{kind: kind, text: out.String(), line: line, col: col}, nil
		}
		if r == '\n' {
			break
		}
		out.WriteRune(r)
		l.advance(width)
	}
	return token{}, fmt.Errorf("%d:%d: unterminated %s", line, col, delimiterName(delim))
}

// stringLiteral reads a single-quoted or triple-quoted literal, resolving the
// backslash escapes DBML defines.
func (l *lexer) stringLiteral(line, col int) (token, error) {
	if strings.HasPrefix(l.src[l.pos:], "'''") {
		return l.tripleQuoted(line, col)
	}
	l.advance(1)
	var out strings.Builder
	for l.pos < len(l.src) {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		switch {
		case r == '\\' && l.pos+1 < len(l.src):
			l.advance(1)
			escaped, w := utf8.DecodeRuneInString(l.src[l.pos:])
			out.WriteRune(escaped)
			l.advance(w)
		case r == '\'':
			l.advance(width)
			return token{kind: tokenString, text: out.String(), line: line, col: col}, nil
		case r == '\n':
			return token{}, fmt.Errorf("%d:%d: unterminated string", line, col)
		default:
			out.WriteRune(r)
			l.advance(width)
		}
	}
	return token{}, fmt.Errorf("%d:%d: unterminated string", line, col)
}

// tripleQuoted reads the multi-line literal form, which is the only one that
// can hold a newline.
func (l *lexer) tripleQuoted(line, col int) (token, error) {
	l.advance(3)
	var out strings.Builder
	for l.pos < len(l.src) {
		if strings.HasPrefix(l.src[l.pos:], "'''") {
			l.advance(3)
			return token{kind: tokenString, text: out.String(), line: line, col: col}, nil
		}
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		if r == '\\' && l.pos+1 < len(l.src) {
			l.advance(1)
			escaped, w := utf8.DecodeRuneInString(l.src[l.pos:])
			out.WriteRune(escaped)
			l.advance(w)
			continue
		}
		out.WriteRune(r)
		l.advance(width)
	}
	return token{}, fmt.Errorf("%d:%d: unterminated multi-line string", line, col)
}

// advance moves past n bytes, keeping line and column in step.
func (l *lexer) advance(n int) {
	for range n {
		if l.pos >= len(l.src) {
			return
		}
		if l.src[l.pos] == '\n' {
			l.line++
			l.col = 1
		} else {
			l.col++
		}
		l.pos++
	}
}

// isWordRune reports the characters a bare identifier may start with or hold.
func isWordRune(r rune) bool {
	return r == '_' || unicode.IsLetter(r)
}

// delimiterName names a delimiter for a diagnostic.
func delimiterName(delim rune) string {
	if delim == '`' {
		return "expression"
	}
	return "quoted identifier"
}
