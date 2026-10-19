// Package ptahdirective locates line-anchored Ptah migration directives in
// SQL without confusing directive-looking bytes inside other SQL tokens.
package ptahdirective

import (
	"iter"
	"strings"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/descriptortable"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

const prefix = "+ptah"

type marker struct {
	start int
	body  string
}

// Marker is one line-anchored `-- +ptah` directive together with where it was
// found. The offset is what lets a caller decide whether the directive lies in
// the region where directives are significant, which no scan of the bodies
// alone can answer.
type Marker struct {
	// Start is the byte offset of the comment's first `-` in the scanned SQL.
	Start int
	// Body is the text after the `+ptah` marker, exactly as [Bodies] yields it.
	Body string
}

// LineComment is one `--` comment that begins its physical line, which is the
// only shape either directive family accepts as a directive carrier. A trailing
// comment after a statement is deliberately not one.
type LineComment struct {
	// Start is the byte offset of the comment's first `-` in the scanned SQL.
	Start int
	// Text is the comment token, beginning with `--`.
	Text string
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

// Markers is [Bodies] carrying each marker's byte offset. A caller that must
// know WHERE a directive sits -- to honor it in one region and report it in
// another -- needs the offset that Bodies drops.
func Markers(sql string, options lexer.Options) iter.Seq[Marker] {
	return func(yield func(Marker) bool) {
		for _, found := range scan(sql, options) {
			if !yield(Marker{Start: found.start, Body: found.body}) {
				return
			}
		}
	}
}

// LineComments yields every `--` comment that begins its physical line, in
// source order. It is the shared reading of "a line that could carry a
// directive", so the `+ptah` and `atlas:` families answer the position question
// against one scan of the file rather than two hand-rolled line loops.
func LineComments(sql string, options lexer.Options) iter.Seq[LineComment] {
	return func(yield func(LineComment) bool) {
		lexr := lexer.NewLexerWithOptions(sql, options)
		for {
			tok := lexr.NextToken()
			if tok.Type == lexer.TokenEOF {
				return
			}
			if tok.Type != lexer.TokenComment {
				continue
			}
			if !strings.HasPrefix(tok.Value, "--") {
				continue // block comment: not a directive carrier
			}
			if !commentStartsLine(sql, tok.Start) {
				continue // trailing comment: not a directive
			}
			if !yield(LineComment{Start: tok.Start, Text: tok.Value}) {
				return
			}
		}
	}
}

// ConservativeBodies yields only markers that every engine deciding the
// directives of a file without a dialect sees at the same byte offset with the
// same body; see [ConservativeMarkers]. It is the safe fallback for source
// operations such as migration import that have no target dialect: an
// ambiguous marker-looking line inside a dialect-specific string remains SQL,
// while an actual line-anchored directive is common to every scan.
func ConservativeBodies(sql string) iter.Seq[string] {
	return func(yield func(string) bool) {
		for found := range ConservativeMarkers(sql) {
			if !yield(found.Body) {
				return
			}
		}
	}
}

// ConservativeMarkers is [ConservativeBodies] carrying each marker's byte
// offset, for the same reason [Markers] exists: a caller deciding whether a
// directive lies inside the region where directives are significant needs to
// know where it is, and it must ask that question of the same marker set the
// directive parser used or the two answers can disagree.
//
// The readers are the lexers of the engines whose descriptor states
// Migration.DirectivesWithoutDialect, in registry order. A marker is kept only
// where every one of them sees it at the same offset with the same body. An
// engine left out is read by its own rules wherever its dialect is known,
// through [Markers] with its lexer options.
func ConservativeMarkers(sql string) iter.Seq[Marker] {
	return func(yield func(Marker) bool) {
		readers := dialectlessReaders.Get()
		if len(readers) == 0 {
			return
		}
		common := scan(sql, readers[0])
		for _, options := range readers[1:] {
			present := make(map[marker]struct{})
			for _, found := range scan(sql, options) {
				present[found] = struct{}{}
			}
			common = keepMarkers(common, present)
			if len(common) == 0 {
				return
			}
		}
		for _, found := range common {
			if !yield(Marker{Start: found.start, Body: found.body}) {
				return
			}
		}
	}
}

// dialectlessReaders holds the lexer options of every engine that decides the
// directives of a file whose dialect is unknown, in registry order. A
// migration import asks for each file it reads, so the options are derived
// once per registry rather than per file. A registry with no such engine
// keeps no marker; the built-in engines always include some.
var dialectlessReaders = descriptortable.NewIndex(func(dialects []descriptor.Dialect) []lexer.Options {
	var readers []lexer.Options
	for _, described := range dialects {
		if described.Migration.DirectivesWithoutDialect {
			readers = append(readers, dialectlexer.FromDescriptor(described))
		}
	}
	return readers
})

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
	for comment := range LineComments(sql, options) {
		body := strings.TrimPrefix(comment.Text, "--")
		body, ok := strings.CutPrefix(strings.TrimSpace(body), prefix)
		if !ok || (body != "" && body[0] != ' ' && body[0] != '\t') {
			continue
		}
		markers = append(markers, marker{start: comment.Start, body: body})
	}
	return markers
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
