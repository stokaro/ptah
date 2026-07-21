// Package annotationparse scans Ptah Go annotation comments with positions.
package annotationparse

import (
	"regexp"
	"strings"

	"github.com/stokaro/ptah/internal/annotationmeta"
)

var attrRe = regexp.MustCompile(`(\w+(?:\.\w+)*)=("(?:\\.|[^"\\])*"|[^\s]+)`)

// Range is a zero-based byte-offset text range.
type Range struct {
	Start Position
	End   Position
}

// Position is a zero-based line and byte-offset position.
type Position struct {
	Line      int
	Character int
}

// Attribute is a parsed annotation attribute with source range.
type Attribute struct {
	Name       string
	Value      string
	Range      Range
	ValueRange Range
}

// Annotation is a parsed //migrator annotation comment.
type Annotation struct {
	Line           int
	Directive      string
	DirectiveRange Range
	CommentRange   Range
	Attributes     []Attribute
	Known          bool
}

// Scan returns every Ptah annotation comment found in text.
func Scan(text string) []Annotation {
	lines := strings.Split(text, "\n")
	out := make([]Annotation, 0)
	for lineNo, line := range lines {
		annotation, ok := scanLine(lineNo, line)
		if ok {
			out = append(out, annotation)
		}
	}
	return out
}

func scanLine(lineNo int, line string) (Annotation, bool) {
	commentStart := strings.Index(line, "//")
	if commentStart < 0 {
		return Annotation{}, false
	}
	if strings.TrimSpace(line[:commentStart]) != "" {
		return Annotation{}, false
	}
	bodyStart := commentStart + len("//")
	for bodyStart < len(line) && (line[bodyStart] == ' ' || line[bodyStart] == '\t') {
		bodyStart++
	}
	if !strings.HasPrefix(line[bodyStart:], "migrator:") {
		return Annotation{}, false
	}

	directive, end := readDirective(line[bodyStart:])
	annotation := Annotation{
		Line:      lineNo,
		Directive: directive,
		DirectiveRange: Range{
			Start: Position{Line: lineNo, Character: bodyStart},
			End:   Position{Line: lineNo, Character: bodyStart + end},
		},
		CommentRange: Range{
			Start: Position{Line: lineNo, Character: commentStart},
			End:   Position{Line: lineNo, Character: len(line)},
		},
		Attributes: scanAttributes(lineNo, line),
	}
	_, annotation.Known = annotationmeta.Lookup(directive)
	return annotation, true
}

func readDirective(body string) (string, int) {
	best := ""
	for _, directive := range annotationmeta.Directives() {
		name := directive.Name
		if !strings.HasPrefix(body, name) {
			continue
		}
		rest := body[len(name):]
		if rest != "" && rest[0] != ' ' && rest[0] != '\t' {
			continue
		}
		if len(name) > len(best) {
			best = name
		}
	}
	if best != "" {
		return best, len(best)
	}
	end := len(body)
	if i := strings.IndexAny(body, " \t"); i >= 0 {
		end = i
	}
	return body[:end], end
}

func scanAttributes(lineNo int, line string) []Attribute {
	matches := attrRe.FindAllStringSubmatchIndex(line, -1)
	attrs := make([]Attribute, 0, len(matches))
	for _, match := range matches {
		nameStart := match[2]
		nameEnd := match[3]
		valueStart := match[4]
		valueEnd := match[5]
		attrs = append(attrs, Attribute{
			Name:  line[nameStart:nameEnd],
			Value: strings.Trim(line[valueStart:valueEnd], `"`),
			Range: Range{
				Start: Position{Line: lineNo, Character: nameStart},
				End:   Position{Line: lineNo, Character: nameEnd},
			},
			ValueRange: Range{
				Start: Position{Line: lineNo, Character: valueStart},
				End:   Position{Line: lineNo, Character: valueEnd},
			},
		})
	}
	return attrs
}
