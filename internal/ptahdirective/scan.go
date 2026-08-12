// Package ptahdirective locates line-anchored Ptah migration directives in
// SQL without confusing directive-looking bytes inside other SQL tokens.
package ptahdirective

import (
	"iter"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/dialectlexer"
	"go.5x5.cz/ptah/internal/lexer"
)

const prefix = "+ptah"

type marker struct {
	start int
	body  string
}

// Bodies yields the text after every line-anchored -- +ptah marker. The lexer
// options select the owning SQL dialect's string and comment rules. Bare and
// malformed marker bodies are yielded too, so policy callers can refuse them
// even when a semantic directive parser would ignore their contents.
func Bodies(sql string, options lexer.Options) iter.Seq[string] {
	return func(yield func(string) bool) {
		for _, found := range scan(sql, options) {
			if !yield(found.body) {
				return
			}
		}
	}
}

// ConservativeBodies yields only markers that every supported dialect sees at
// the same byte offset with the same body. It is the safe fallback for source
// operations such as migration import that have no target dialect: an
// ambiguous marker-looking line inside a dialect-specific string remains SQL,
// while an actual line-anchored directive is common to every scan.
func ConservativeBodies(sql string) iter.Seq[string] {
	return func(yield func(string) bool) {
		dialects := []string{
			platform.Postgres,
			platform.MySQL,
			platform.MariaDB,
			platform.SQLite,
			platform.ClickHouse,
			platform.CockroachDB,
			platform.YugabyteDB,
			platform.SQLServer,
			platform.Spanner,
		}
		common := scan(sql, dialectlexer.Options(dialects[0]))
		for _, dialect := range dialects[1:] {
			present := make(map[marker]struct{})
			for _, found := range scan(sql, dialectlexer.Options(dialect)) {
				present[found] = struct{}{}
			}
			common = keepMarkers(common, present)
			if len(common) == 0 {
				return
			}
		}
		for _, found := range common {
			if !yield(found.body) {
				return
			}
		}
	}
}

func keepMarkers(candidates []marker, present map[marker]struct{}) []marker {
	kept := candidates[:0]
	for _, candidate := range candidates {
		if _, ok := present[candidate]; ok {
			kept = append(kept, candidate)
		}
	}
	return kept
}

func scan(sql string, options lexer.Options) []marker {
	var markers []marker
	lexr := lexer.NewLexerWithOptions(sql, options)
	for {
		tok := lexr.NextToken()
		if tok.Type == lexer.TokenEOF {
			return markers
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
		markers = append(markers, marker{start: tok.Start, body: body})
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
