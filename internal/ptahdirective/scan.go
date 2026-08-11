// Package ptahdirective locates line-anchored Ptah migration directives in
// SQL without confusing directive-looking bytes inside other SQL tokens.
package ptahdirective

import (
	"iter"
	"strings"

	"go.5x5.cz/ptah/internal/lexer"
)

const prefix = "+ptah"

// Bodies yields the text after every line-anchored -- +ptah marker. The lexer
// options select the owning SQL dialect's string and comment rules. Bare and
// malformed marker bodies are yielded too, so policy callers can refuse them
// even when a semantic directive parser would ignore their contents.
func Bodies(sql string, options lexer.Options) iter.Seq[string] {
	return func(yield func(string) bool) {
		lexr := lexer.NewLexerWithOptions(sql, options)
		for {
			tok := lexr.NextToken()
			if tok.Type == lexer.TokenEOF {
				return
			}
			if tok.Type != lexer.TokenComment {
				continue
			}
			body, ok := strings.CutPrefix(tok.Value, "--")
			if !ok {
				continue // block comment: not a directive carrier
			}
			if !commentStartsLine(sql, tok.Start) {
				continue // trailing comment: not a directive
			}
			body, ok = strings.CutPrefix(strings.TrimSpace(body), prefix)
			if !ok || (body != "" && body[0] != ' ' && body[0] != '\t') {
				continue
			}
			if !yield(body) {
				return
			}
		}
	}
}

// HasMarker reports whether SQL contains any Ptah directive marker recognized
// by Bodies.
func HasMarker(sql string, options lexer.Options) bool {
	for range Bodies(sql, options) {
		return true
	}
	return false
}

// commentStartsLine reports whether only whitespace precedes the byte at pos
// on its physical line.
func commentStartsLine(sql string, pos int) bool {
	for i := pos - 1; i >= 0; i-- {
		switch sql[i] {
		case '\n':
			return true
		case ' ', '\t', '\r':
			continue
		default:
			return false
		}
	}
	return true
}
