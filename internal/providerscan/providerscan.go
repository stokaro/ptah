// Package providerscan reads the SQL text of an engine an executable provider
// serves with the provider's own scanner (ScanSQL, ADR 0069). It turns the
// provider's tokens into internal/lexer tokens, so internal/providerhost can
// register the engine's scanner with internal/lexerext and every reader of the
// engine's text through the shared lexer reads the engine's rules.
package providerscan

import (
	"context"
	"maps"
	"sync"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/internal/lexer"
)

// Scan asks the provider for the tokens of one text of the engine's SQL.
// head reports that the text begins where the server reads settings ahead of
// a query. The provider's reply is validated by the SDK before it returns.
type Scan func(ctx context.Context, text string, head bool) (*protocol.SQLScan, error)

// maxCachedTexts bounds the scans a Scanner keeps. A reader of a large
// document asks for many short texts, and most of them repeat; past the bound
// the cache starts again rather than growing with the document.
const maxCachedTexts = 4096

// Scanner is a lexer.Scanner over a provider's scan. Its methods are safe for
// concurrent use, and a pointer to it is comparable, as lexer.Options
// requires.
//
// A scan that fails reads the rest of the text as one lexer.TokenUnknown
// token. No reader takes that token for a string, a name or a separator, and
// the scanner does not read the text by the shared rules instead, which answer
// for another language.
type Scanner struct {
	scan Scan

	mu     sync.Mutex
	texts  map[textKey][]token
	values map[valueKey]value
}

// New returns a Scanner that asks scan. It calls scan with
// context.Background, since a lexer reads no context: the provider client
// bounds each call itself.
func New(scan Scan) *Scanner {
	return &Scanner{scan: scan, texts: make(map[textKey][]token), values: make(map[valueKey]value)}
}

type textKey struct {
	text string
	head bool
}

type valueKey struct {
	kind  lexer.TokenType
	token string
}

// value is what a string or an identifier stands for, and whether the
// scanner read one.
type value struct {
	text string
	read bool
}

// token is a scanned token, with its end relative to the scanned text.
type token struct {
	kind lexer.TokenType
	end  int
}

// kinds maps the contract's token kinds to the lexer's token types.
var kinds = map[protocol.SQLToken_Kind]lexer.TokenType{
	protocol.SQLToken_UNKNOWN:    lexer.TokenUnknown,
	protocol.SQLToken_STRING:     lexer.TokenString,
	protocol.SQLToken_COMMENT:    lexer.TokenComment,
	protocol.SQLToken_SEMICOLON:  lexer.TokenSemicolon,
	protocol.SQLToken_WHITESPACE: lexer.TokenWhitespace,
	protocol.SQLToken_IDENTIFIER: lexer.TokenIdentifier,
	protocol.SQLToken_OPERATOR:   lexer.TokenOperator,
}

// Scan returns the reader of text from start: the provider's tokens of
// text[start:], scanned at the head of the text when head is set, with
// offsets in the whole text.
func (s *Scanner) Scan(text string, start int, head bool) lexer.TokenReader {
	return &reader{text: text, start: start, tokens: s.tokens(text[start:], head)}
}

// StringValue answers the text a string token stands for, as the provider
// read it.
func (s *Scanner) StringValue(written string) (string, bool) {
	return s.valueOf(lexer.TokenString, written)
}

// IdentifierValue answers the name an identifier token stands for, with its
// quoting undone, as the provider read it.
func (s *Scanner) IdentifierValue(written string) (string, bool) {
	return s.valueOf(lexer.TokenIdentifier, written)
}

// valueOf answers from the values of earlier scans, and otherwise scans the
// token alone: it stands for a value when it is one token of the kind asked
// for that carries one.
func (s *Scanner) valueOf(kind lexer.TokenType, written string) (string, bool) {
	key := valueKey{kind: kind, token: written}
	s.mu.Lock()
	known, found := s.values[key]
	s.mu.Unlock()
	if found {
		return known.text, known.read
	}
	tokens := s.tokens(written, false)
	s.mu.Lock()
	defer s.mu.Unlock()
	known, found = s.values[key]
	if !found || len(tokens) != 1 || tokens[0].kind != kind {
		return "", false
	}
	return known.text, known.read
}

// tokens returns the tokens of text, from the cache or from the provider.
func (s *Scanner) tokens(text string, head bool) []token {
	if text == "" {
		return nil
	}
	key := textKey{text: text, head: head}
	s.mu.Lock()
	cached, found := s.texts[key]
	s.mu.Unlock()
	if found {
		return cached
	}
	scanned, values, ok := s.ask(text, head)
	if !ok {
		return scanned
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.texts) >= maxCachedTexts {
		clear(s.texts)
		clear(s.values)
	}
	s.texts[key] = scanned
	maps.Copy(s.values, values)
	return scanned
}

// ask scans text with the provider, and reports whether the scan succeeded.
// A failed scan is one unknown token over the whole text and reads no value,
// and it is not cached, so a later reader of the text asks again.
func (s *Scanner) ask(text string, head bool) ([]token, map[valueKey]value, bool) {
	unknown := []token{{kind: lexer.TokenUnknown, end: len(text)}}
	result, err := s.scan(context.Background(), text, head)
	if err != nil {
		return unknown, nil, false
	}
	tokens := make([]token, 0, len(result.GetTokens()))
	values := make(map[valueKey]value)
	begin := 0
	for _, scanned := range result.GetTokens() {
		kind, known := kinds[scanned.GetKind()]
		end := int(scanned.GetEnd())
		if !known || end <= begin || end > len(text) {
			return unknown, nil, false
		}
		if kind == lexer.TokenString || kind == lexer.TokenIdentifier {
			values[valueKey{kind: kind, token: text[begin:end]}] = value{text: scanned.GetValue(), read: scanned.Value != nil}
		}
		tokens = append(tokens, token{kind: kind, end: end})
		begin = end
	}
	if begin != len(text) {
		return unknown, nil, false
	}
	return tokens, values, true
}

// reader returns the tokens of one scan in order, then lexer.TokenEOF.
type reader struct {
	text   string
	start  int
	tokens []token
	next   int
}

// NextToken returns the next token, with its offsets in the whole text.
func (r *reader) NextToken() lexer.Token {
	if r.next >= len(r.tokens) {
		return lexer.Token{Type: lexer.TokenEOF, Start: len(r.text), End: len(r.text)}
	}
	begin := r.start
	if r.next > 0 {
		begin = r.start + r.tokens[r.next-1].end
	}
	current := r.tokens[r.next]
	r.next++
	end := r.start + current.end
	return lexer.Token{Type: current.kind, Value: r.text[begin:end], Start: begin, End: end}
}
