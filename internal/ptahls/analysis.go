// Package ptahls implements the Ptah annotation language server.
package ptahls

import (
	"fmt"
	"slices"
	"strings"

	"github.com/stokaro/ptah/internal/annotationmeta"
	"github.com/stokaro/ptah/internal/annotationparse"
)

// DiagnosticSeverity follows the LSP DiagnosticSeverity values.
type DiagnosticSeverity int

const (
	DiagnosticSeverityError DiagnosticSeverity = 1
)

// Diagnostic describes an editor diagnostic.
type Diagnostic struct {
	Range    annotationparse.Range
	Severity DiagnosticSeverity
	Code     string
	Message  string
	Source   string
}

// Completion describes one annotation attribute completion.
type Completion struct {
	Label         string
	Detail        string
	Documentation string
}

// Analyze returns diagnostics for Ptah annotations in text.
func Analyze(text string) []Diagnostic {
	var diagnostics []Diagnostic
	for _, annotation := range annotationparse.Scan(text) {
		if !annotation.Known {
			diagnostics = append(diagnostics, Diagnostic{
				Range:    annotation.DirectiveRange,
				Severity: DiagnosticSeverityError,
				Code:     "PTAH001",
				Message:  fmt.Sprintf("unknown Ptah annotation directive %q", annotation.Directive),
				Source:   "ptah",
			})
			continue
		}
		spec, _ := annotationmeta.Lookup(annotation.Directive)
		seen := make(map[string]bool, len(annotation.Attributes))
		for _, attr := range annotation.Attributes {
			seen[attr.Name] = true
			if annotationmeta.AllowsAttribute(annotation.Directive, attr.Name) {
				continue
			}
			diagnostics = append(diagnostics, Diagnostic{
				Range:    attr.Range,
				Severity: DiagnosticSeverityError,
				Code:     "PTAH002",
				Message:  fmt.Sprintf("unknown attribute %q on //%s", attr.Name, annotation.Directive),
				Source:   "ptah",
			})
		}
		for _, attr := range spec.Attributes {
			if !attr.Required || seen[attr.Name] {
				continue
			}
			diagnostics = append(diagnostics, Diagnostic{
				Range:    annotation.DirectiveRange,
				Severity: DiagnosticSeverityError,
				Code:     "PTAH003",
				Message:  fmt.Sprintf("missing required attribute %q on //%s", attr.Name, annotation.Directive),
				Source:   "ptah",
			})
		}
	}
	return diagnostics
}

// Hover returns Markdown documentation for the annotation at pos.
func Hover(text string, pos annotationparse.Position) (string, bool) {
	for _, annotation := range annotationparse.Scan(text) {
		if annotation.Line != pos.Line || !annotation.Known {
			continue
		}
		if !contains(annotation.CommentRange, pos) {
			continue
		}
		spec, _ := annotationmeta.Lookup(annotation.Directive)
		return annotationmeta.Markdown(spec), true
	}
	return "", false
}

// Complete returns attribute completions for the annotation at pos.
func Complete(text string, pos annotationparse.Position) []Completion {
	for _, annotation := range annotationparse.Scan(text) {
		if annotation.Line != pos.Line || !annotation.Known {
			continue
		}
		if !contains(annotation.CommentRange, pos) {
			continue
		}
		for _, attr := range annotation.Attributes {
			if contains(attr.ValueRange, pos) {
				return nil
			}
		}
		spec, _ := annotationmeta.Lookup(annotation.Directive)
		used := make(map[string]bool, len(annotation.Attributes))
		for _, attr := range annotation.Attributes {
			used[attr.Name] = true
		}
		out := make([]Completion, 0, len(spec.Attributes))
		for _, attr := range spec.Attributes {
			if used[attr.Name] {
				continue
			}
			detail := attr.Value
			if attr.Required {
				detail += ", required"
			}
			if attr.AliasFor != "" {
				detail += ", alias for " + attr.AliasFor
			}
			out = append(out, Completion{
				Label:         attr.Name,
				Detail:        strings.TrimPrefix(detail, ", "),
				Documentation: attr.Description,
			})
		}
		slices.SortFunc(out, func(a, b Completion) int {
			return strings.Compare(a.Label, b.Label)
		})
		return out
	}
	return nil
}

func contains(r annotationparse.Range, pos annotationparse.Position) bool {
	if pos.Line < r.Start.Line || pos.Line > r.End.Line {
		return false
	}
	if pos.Line == r.Start.Line && pos.Character < r.Start.Character {
		return false
	}
	if pos.Line == r.End.Line && pos.Character > r.End.Character {
		return false
	}
	return true
}
