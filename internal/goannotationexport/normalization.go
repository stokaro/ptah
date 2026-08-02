package goannotationexport

import (
	"bytes"
	"go/parser"
	"go/token"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/text/unicode/norm"

	"go.5x5.cz/ptah/internal/annotationparse"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

// normalizationDiagnostics reports annotation attribute values that are not in
// Unicode NFC.
//
// HCL rendering routes every string through cty.StringVal, which NFC-composes
// it, while Go annotation parsing preserves the source code points. A
// decomposed value therefore reaches the HCL with different bytes than it had
// in the Go source. On its own that is only a fidelity loss, but destructive
// cleanup then deletes the Go source, and with it the only copy of the original
// bytes. Reporting the value as lossy lets the existing ErrLossyCleanup gate
// refuse the cleanup before anything is written.
//
// Diagnostics identify the file, line, directive and attribute, and never carry
// the value itself: an attribute may hold a credential.
func normalizationDiagnostics(fsys fs.FS, rendered []byte) ([]atlashclrender.Diagnostic, error) {
	var diagnostics []atlashclrender.Diagnostic

	err := fs.WalkDir(fsys, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || path.Ext(name) != ".go" {
			return nil
		}
		data, err := fs.ReadFile(fsys, name)
		if err != nil {
			return err
		}
		diagnostics = append(diagnostics, fileNormalizationDiagnostics(name, string(data), rendered)...)
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(diagnostics, func(i, j int) bool {
		if diagnostics[i].Path != diagnostics[j].Path {
			return diagnostics[i].Path < diagnostics[j].Path
		}
		return diagnostics[i].Message < diagnostics[j].Message
	})
	return diagnostics, nil
}

func fileNormalizationDiagnostics(name, source string, rendered []byte) []atlashclrender.Diagnostic {
	var diagnostics []atlashclrender.Diagnostic
	lines := strings.Split(source, "\n")

	// Only real Go comments can become schema intent. Scanning raw text would
	// also match a "//ptah:" line inside a string literal, which goschema never
	// parses and cleanup never removes - reporting one would refuse a cleanup
	// that loses nothing. Mirrors annotationLineNumbers in goannotationcleanup.
	commentLines, ok := annotationCommentLines(name, source)
	if !ok {
		// Unparseable Go: goschema could not have built a schema from it, so
		// there is nothing this check could be protecting.
		return nil
	}

	for _, annotation := range annotationparse.Scan(source) {
		if !commentLines[annotation.Line+1] {
			continue
		}
		if !annotation.Known {
			// Unknown directives are never rendered, so they cannot lose bytes
			// through the HCL.
			continue
		}
		// annotationparse.Scan numbers lines from zero, so the slice index is
		// the line number itself. Reading lines[Line-1] would decode the
		// PREVIOUS line, whose ranges never match, silently falling back to the
		// quote-trimmed value and missing escaped code points entirely.
		if annotation.Line < 0 || annotation.Line >= len(lines) {
			continue
		}
		line := lines[annotation.Line]

		for _, attribute := range annotation.Attributes {
			value, ok := decodeAttributeValue(line, attribute)
			if !ok || norm.NFC.IsNormalString(value) {
				continue
			}
			// Being non-NFC is only a candidate signal. If the exact bytes
			// survive into the output, nothing was lost and refusing cleanup
			// would be a false positive: not every attribute is routed through
			// cty as a string.
			if bytes.Contains(rendered, []byte(value)) {
				continue
			}
			diagnostics = append(diagnostics, atlashclrender.Diagnostic{
				Severity: atlashclrender.SeverityWarning,
				Path:     name,
				Message: "line " + strconv.Itoa(annotation.Line+1) + ": " + annotation.Directive +
					" attribute " + attribute.Name +
					" is not Unicode NFC; HCL rendering composes it, so exporting changes its bytes",
			})
		}
	}
	return diagnostics
}

// annotationCommentLines returns the 1-based lines of source that are genuine
// Go comments. The second result is false when the file does not parse.
func annotationCommentLines(name, source string) (map[int]bool, bool) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, name, source, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		return nil, false
	}
	lines := make(map[int]bool)
	for _, group := range file.Comments {
		for _, comment := range group.List {
			lines[fileSet.PositionFor(comment.Pos(), false).Line] = true
		}
	}
	return lines, true
}

// decodeAttributeValue returns the attribute's value as the renderer will see
// it. annotationparse keeps the raw span, quotes included, and its Value field
// only trims quotes, so an escaped code point such as ́ would still look
// like plain ASCII. Unquoting first is what core/goschema does when it builds
// the schema, so this matches what actually reaches the renderer.
func decodeAttributeValue(line string, attribute annotationparse.Attribute) (string, bool) {
	start := attribute.ValueRange.Start.Character
	end := attribute.ValueRange.End.Character
	if start < 0 || end > len(line) || start >= end {
		return attribute.Value, attribute.Value != ""
	}
	raw := line[start:end]
	if !strings.HasPrefix(raw, `"`) {
		return raw, raw != ""
	}
	unquoted, err := strconv.Unquote(raw)
	if err != nil {
		return strings.Trim(raw, `"`), true
	}
	return unquoted, true
}
