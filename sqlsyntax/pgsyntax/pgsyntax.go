// Package pgsyntax reads the lexical syntax of PostgreSQL-family SQL text: it
// splits text into tokens and statements, and quotes identifiers and string
// literals. PostgreSQL, YugabyteDB, CockroachDB and the PostgreSQL interface of
// Spanner read text this way, as does DuckDB for the forms it takes.
//
// It knows no engine's catalog, types or DDL, and no keyword: a keyword is a
// [Word] like any other name.
package pgsyntax

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// Kind is what a [Token] is.
type Kind int

// The kinds of token. Every byte of the text belongs to exactly one token.
const (
	// Whitespace is a run of spaces, tabs, line endings and form feeds.
	Whitespace Kind = iota + 1
	// LineComment is `--` to the end of its line, the line ending excluded.
	LineComment
	// BlockComment is `/* ... */`, nested comments included.
	BlockComment
	// Word is an unquoted name or keyword.
	Word
	// QuotedIdentifier is `"..."` or `U&"..."`.
	QuotedIdentifier
	// String is `'...'`, `E'...'`, `U&'...'`, `B'...'` or `X'...'`. A string
	// continued on the next line is two tokens, as the server reads two
	// constants it then joins.
	String
	// DollarString is `$tag$...$tag$`, with an empty tag or a name.
	DollarString
	// Number is a numeric constant.
	Number
	// Parameter is a positional parameter such as `$1`.
	Parameter
	// Operator is a run of operator characters.
	Operator
	// Punctuation is one of `( ) [ ] , ; : .`, or `..`.
	Punctuation
)

// String names the kind, as its constant is named.
func (k Kind) String() string {
	names := [...]string{Whitespace: "Whitespace", LineComment: "LineComment", BlockComment: "BlockComment", Word: "Word",
		QuotedIdentifier: "QuotedIdentifier", String: "String", DollarString: "DollarString", Number: "Number",
		Parameter: "Parameter", Operator: "Operator", Punctuation: "Punctuation"}
	if k > 0 && int(k) < len(names) {
		return names[k]
	}
	return fmt.Sprintf("Kind(%d)", int(k))
}

// Token is one token of a text: its kind and its byte offsets, Start
// inclusive and End exclusive.
type Token struct {
	Kind       Kind
	Start, End int
}

// Text returns the token's text in source, the text it was scanned from.
func (t Token) Text(source string) string { return source[t.Start:t.End] }

// Options selects the lexical rules a server reads text with. The zero value
// is PostgreSQL's default, with standard_conforming_strings on.
type Options struct {
	// BackslashEscapes reads a backslash in an ordinary '...' string as the
	// start of an escape, as a server with standard_conforming_strings off
	// does. An E'...' string reads escapes either way.
	BackslashEscapes bool
}

// SyntaxError is text the rules cannot read, such as a string that never
// closes. Offset is the byte offset of the construct that is not closed.
type SyntaxError struct {
	Offset int
	Reason string
}

func (e *SyntaxError) Error() string {
	return fmt.Sprintf("at byte %d: %s", e.Offset, e.Reason)
}

// Scan splits text into tokens, in order. A construct that does not close
// returns a [*SyntaxError] and no tokens.
func Scan(text string, options Options) ([]Token, error) {
	s := scanner{text: text, options: options}
	var tokens []Token
	for s.pos < len(text) {
		start := s.pos
		kind, err := s.next()
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, Token{Kind: kind, Start: start, End: s.pos})
	}
	return tokens, nil
}

// Next reads the first token of text: a reader that keeps its own scanner, such
// as Ptah's lexer, reads one construct by these rules and goes on with its own.
// Text that holds no token returns a zero Token, and a construct that does not
// close returns a [*SyntaxError].
func Next(text string, options Options) (Token, error) {
	if text == "" {
		return Token{}, nil
	}
	s := scanner{text: text, options: options}
	kind, err := s.next()
	if err != nil {
		return Token{}, err
	}
	return Token{Kind: kind, Start: 0, End: s.pos}, nil
}

// Statement is one statement of a text: its byte offsets, the terminating
// semicolon excluded, and its text without the white space and comments
// around it. A statement holds at least one token that is neither white space
// nor a comment.
type Statement struct {
	Start, End int
	Text       string
}

// Statements splits text into statements at each semicolon outside a string,
// a quoted identifier, a dollar-quoted string and a comment. A text between
// two semicolons that holds only white space and comments is no statement.
// The split is lexical: the body of a function written with BEGIN ATOMIC, whose
// semicolons the server reads as part of the function, is split there too.
func Statements(text string, options Options) ([]Statement, error) {
	tokens, err := Scan(text, options)
	if err != nil {
		return nil, err
	}
	var statements []Statement
	first, last := -1, -1
	flush := func() {
		if first >= 0 {
			start, end := tokens[first].Start, tokens[last].End
			statements = append(statements, Statement{Start: start, End: end, Text: text[start:end]})
		}
		first, last = -1, -1
	}
	for i, token := range tokens {
		switch {
		case token.Kind == Punctuation && text[token.Start] == ';':
			flush()
		case token.Kind == Whitespace || token.Kind == LineComment || token.Kind == BlockComment:
		default:
			if first < 0 {
				first = i
			}
			last = i
		}
	}
	flush()
	return statements, nil
}

// QuoteIdentifier quotes name as a PostgreSQL-family identifier: in double
// quotes, an inner double quote doubled. The result names exactly name,
// whatever case it is written in.
func QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// QuoteString renders value as a string constant a server reads back as
// value, with standard_conforming_strings on or off: in single quotes, an inner
// single quote doubled, and as E'...' with backslashes doubled when value holds
// a backslash.
func QuoteString(value string) string {
	quoted := strings.ReplaceAll(value, `'`, `''`)
	if strings.Contains(value, `\`) {
		return `E'` + strings.ReplaceAll(quoted, `\`, `\\`) + `'`
	}
	return `'` + quoted + `'`
}

type scanner struct {
	text    string
	options Options
	pos     int
}

func (s *scanner) peek(offset int) byte {
	if s.pos+offset < len(s.text) {
		return s.text[s.pos+offset]
	}
	return 0
}

func (s *scanner) next() (Kind, error) {
	c := s.peek(0)
	if kind, found, err := s.comment(c); found {
		return kind, err
	}
	if kind, found, err := s.quotedText(c); found {
		return kind, err
	}
	switch {
	case isSpace(c):
		for s.pos < len(s.text) && isSpace(s.text[s.pos]) {
			s.pos++
		}
		return Whitespace, nil
	case c == '$':
		return s.dollar()
	case c == '.' && s.peek(1) == '.':
		// PostgreSQL reads `..` as one token, as in `1..10`, so the second dot
		// does not start a fraction.
		s.pos += 2
		return Punctuation, nil
	case isDigit(c) || c == '.' && isDigit(s.peek(1)):
		s.number()
		return Number, nil
	case isWordStart(c):
		s.word()
		return Word, nil
	case strings.IndexByte("()[],;:.", c) >= 0:
		s.pos++
		return Punctuation, nil
	case isOperator(c):
		s.operator()
		return Operator, nil
	default:
		_, size := utf8.DecodeRuneInString(s.text[s.pos:])
		s.pos += size
		return Operator, nil
	}
}

// comment reads a comment that starts at pos, and reports whether one starts
// there.
func (s *scanner) comment(c byte) (Kind, bool, error) {
	switch {
	case c == '-' && s.peek(1) == '-':
		for s.pos < len(s.text) && s.text[s.pos] != '\n' && s.text[s.pos] != '\r' {
			s.pos++
		}
		return LineComment, true, nil
	case c == '/' && s.peek(1) == '*':
		return BlockComment, true, s.blockComment()
	}
	return 0, false, nil
}

// quotedText reads a string or a quoted identifier that starts at pos, its
// prefix included, and reports whether one starts there.
func (s *scanner) quotedText(c byte) (Kind, bool, error) {
	switch {
	case c == '\'':
		return String, true, s.quoted('\'', s.options.BackslashEscapes)
	case c == '"':
		return QuotedIdentifier, true, s.quoted('"', false)
	case (c == 'E' || c == 'e') && s.peek(1) == '\'':
		s.pos++
		return String, true, s.quoted('\'', true)
	case strings.IndexByte("BbXxNn", c) >= 0 && s.peek(1) == '\'':
		s.pos++
		return String, true, s.quoted('\'', s.options.BackslashEscapes)
	case (c == 'U' || c == 'u') && s.peek(1) == '&' && s.peek(2) == '"':
		s.pos += 2
		return QuotedIdentifier, true, s.quoted('"', false)
	case (c == 'U' || c == 'u') && s.peek(1) == '&' && s.peek(2) == '\'':
		s.pos += 2
		return String, true, s.quoted('\'', false)
	}
	return 0, false, nil
}

// blockComment reads a comment that may hold nested comments.
func (s *scanner) blockComment() error {
	start := s.pos
	depth := 0
	for s.pos < len(s.text) {
		switch {
		case s.peek(0) == '/' && s.peek(1) == '*':
			depth++
			s.pos += 2
		case s.peek(0) == '*' && s.peek(1) == '/':
			depth--
			s.pos += 2
			if depth == 0 {
				return nil
			}
		default:
			s.pos++
		}
	}
	return &SyntaxError{Offset: start, Reason: "a block comment does not close"}
}

// quoted reads a string or identifier from its opening quote, which is at
// pos. A doubled quote stands for one, and with escapes a backslash takes the
// next byte with it.
func (s *scanner) quoted(quote byte, escapes bool) error {
	start := s.pos
	s.pos++
	for s.pos < len(s.text) {
		c := s.text[s.pos]
		switch {
		case escapes && c == '\\':
			s.pos += 2
		case c == quote && s.peek(1) == quote:
			s.pos += 2
		case c == quote:
			s.pos++
			return nil
		default:
			s.pos++
		}
	}
	what := "a string"
	if quote == '"' {
		what = "a quoted identifier"
	}
	return &SyntaxError{Offset: start, Reason: what + " does not close"}
}

// dollar reads a dollar-quoted string, a positional parameter, or a lone
// dollar sign, which is an operator character.
func (s *scanner) dollar() (Kind, error) {
	start := s.pos
	if isDigit(s.peek(1)) {
		s.pos++
		for s.pos < len(s.text) && isDigit(s.text[s.pos]) {
			s.pos++
		}
		return Parameter, nil
	}
	end := s.pos + 1
	if end < len(s.text) && isWordStart(s.text[end]) {
		for end < len(s.text) && isTagPart(s.text[end]) {
			end++
		}
	}
	if end >= len(s.text) || s.text[end] != '$' {
		s.pos++
		return Operator, nil
	}
	tag := s.text[start : end+1]
	closing := strings.Index(s.text[end+1:], tag)
	if closing < 0 {
		return 0, &SyntaxError{Offset: start, Reason: "a dollar-quoted string " + tag + " does not close"}
	}
	s.pos = end + 1 + closing + len(tag)
	return DollarString, nil
}

func (s *scanner) number() {
	if s.peek(0) == '0' && strings.IndexByte("xXoObB", s.peek(1)) >= 0 && isHexDigit(s.peek(2)) {
		s.pos += 2
		s.skip(func(c byte) bool { return isHexDigit(c) || c == '_' })
		return
	}
	digits := func(c byte) bool { return isDigit(c) || c == '_' }
	s.skip(digits)
	if s.peek(0) == '.' && s.peek(1) != '.' {
		s.pos++
		s.skip(digits)
	}
	if s.exponent() {
		s.skip(isDigit)
	}
}

// exponent moves past the `e`, and the sign after it, of an exponent that a
// digit follows, and reports whether there is one.
func (s *scanner) exponent() bool {
	if s.peek(0) != 'e' && s.peek(0) != 'E' {
		return false
	}
	switch {
	case isDigit(s.peek(1)):
		s.pos++
	case (s.peek(1) == '+' || s.peek(1) == '-') && isDigit(s.peek(2)):
		s.pos += 2
	default:
		return false
	}
	return true
}

// skip moves past every byte that accept takes.
func (s *scanner) skip(accept func(byte) bool) {
	for s.pos < len(s.text) && accept(s.text[s.pos]) {
		s.pos++
	}
}

func (s *scanner) word() {
	for s.pos < len(s.text) && isWordPart(s.text[s.pos]) {
		s.pos++
	}
}

// operator reads a run of operator characters, stopping before a comment that
// starts inside it, as the server does.
func (s *scanner) operator() {
	start := s.pos
	for s.pos < len(s.text) && isOperator(s.text[s.pos]) {
		if s.pos > start && (s.peek(0) == '-' && s.peek(1) == '-' || s.peek(0) == '/' && s.peek(1) == '*') {
			return
		}
		s.pos++
	}
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' }

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isHexDigit(c byte) bool {
	return isDigit(c) || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

func isWordStart(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' || c >= 0x80
}

func isWordPart(c byte) bool { return isWordStart(c) || isDigit(c) || c == '$' }

func isTagPart(c byte) bool { return isWordStart(c) || isDigit(c) }

func isOperator(c byte) bool { return strings.IndexByte("+-*/<>=~!@#%^&|`?", c) >= 0 }
