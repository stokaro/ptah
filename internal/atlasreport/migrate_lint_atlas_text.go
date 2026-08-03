package atlasreport

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/internal/tableref"
	migrationlint "go.5x5.cz/ptah/migration/lint"
)

// Atlas's lint analyzers pre-wrap their rendered text at analyzer-specific
// boundaries before the report adds indentation. These widths were bracketed
// against Atlas CE v1.3.0: DS102 keeps 79 message columns and 87 fix columns,
// while DS103 keeps 80 and 88 respectively. MF103 uses the general 88-column
// report width.
const (
	atlasDS102MessageWidth = 79
	atlasDS102FixWidth     = 87
	atlasDS103MessageWidth = 80
)

type atlasDiagnosticCopy struct {
	message      string
	fix          string
	messageWidth int
	fixWidth     int
}

// atlasAnalyzerDocsURL is where Atlas points a reader for an analyzer code. The
// code travels in the fragment rather than inline as `[CODE]`, which is why the
// compat renderer does not print it twice.
const atlasAnalyzerDocsURL = "https://atlasgo.io/lint/analyzers#"

// atlasDiagnosticText renders one finding the way Atlas words it, plus the
// suggested fix Atlas prints under it.
//
// Ptah's own message is deliberately different and usually says more -- compare
// "Dropping table \"pets\"" with "DROP TABLE permanently deletes the table and
// every row in it; take a verified backup first and consider a rename-and-retire
// window instead". Losing that would be a real cost, so it is not lost: this is
// the compat surface only, and native `ptah migrations lint` keeps the fuller
// prose. That is the surface split, applied where it belongs.
//
// It is reconstructed from the finding's STRUCTURED subjects rather than by
// rewriting the analyzers, so neither surface has to know about the other. When
// a code has no Atlas wording here, or the subjects it needs are absent, the
// native message is used unchanged -- being wordier than Atlas is a smaller
// divergence than inventing a sentence Atlas never prints.
func atlasDiagnosticText(code string, finding migrationlint.Finding) (atlasDiagnosticCopy, bool) {
	subject := atlasPrimarySubject(finding)
	switch code {
	case "DS102":
		if subject == nil || subject.Name == "" {
			return atlasDiagnosticCopy{}, false
		}
		name, ok := atlasIdentifierName(subject.Name)
		if !ok {
			return atlasDiagnosticCopy{}, false
		}
		return atlasDiagnosticCopy{
			message:      fmt.Sprintf("Dropping table %q", name),
			fix:          fmt.Sprintf("Add a pre-migration check to ensure table %q is empty before dropping it", name),
			messageWidth: atlasDS102MessageWidth,
			fixWidth:     atlasDS102FixWidth,
		}, true
	case "DS103":
		if subject == nil || subject.Name == "" {
			return atlasDiagnosticCopy{}, false
		}
		name, ok := atlasIdentifierName(subject.Name)
		if !ok {
			return atlasDiagnosticCopy{}, false
		}
		return atlasDiagnosticCopy{
			message:      fmt.Sprintf("Dropping non-virtual column %q", name),
			fix:          fmt.Sprintf("Add a pre-migration check to ensure column %q is NULL before dropping it", name),
			messageWidth: atlasDS103MessageWidth,
			fixWidth:     lintWrapWidth,
		}, true
	case "MF103":
		if subject == nil || subject.Name == "" || subject.Parent == "" || subject.DataType == "" {
			return atlasDiagnosticCopy{}, false
		}
		dataType, ok := atlasDataType(subject.DataType)
		if !ok {
			return atlasDiagnosticCopy{}, false
		}
		name, ok := atlasIdentifierName(subject.Name)
		if !ok {
			return atlasDiagnosticCopy{}, false
		}
		parent, ok := atlasIdentifierName(subject.Parent)
		if !ok {
			return atlasDiagnosticCopy{}, false
		}
		return atlasDiagnosticCopy{
			message: fmt.Sprintf(
				"Adding a non-nullable %q column %q will fail in case table %q is not empty",
				dataType,
				name,
				parent,
			),
			messageWidth: lintWrapWidth,
			fixWidth:     lintWrapWidth,
		}, true
	default:
		return atlasDiagnosticCopy{}, false
	}
}

// atlasIdentifierName converts the linter's source-preserving table reference
// into the logical object name Atlas prints. Qualification and SQL delimiters
// are syntax, not part of that name.
func atlasIdentifierName(value string) (string, bool) {
	ref, ok := tableref.Parse(value)
	if !ok {
		return "", false
	}
	return ref.Name, true
}

// atlasDataType normalizes only type forms whose Atlas diagnostic spelling has
// been measured. Unrecognized forms fail closed so the caller keeps Ptah's
// visibly labeled message instead of fabricating Atlas copy.
func atlasDataType(value string) (string, bool) {
	normalized := strings.ToLower(strings.Join(strings.Fields(value), " "))
	switch normalized {
	case "bigint", "int", "integer", "text", "varchar":
		return normalized, true
	}
	if strings.ReplaceAll(normalized, " ", "") == "varchar(255)" {
		return "varchar", true
	}
	return "", false
}

// atlasPrimarySubject returns the schema object a diagnostic is about.
func atlasPrimarySubject(finding migrationlint.Finding) *migrationlint.Subject {
	if finding.Context == nil || len(finding.Context.Subjects) == 0 {
		return nil
	}
	return &finding.Context.Subjects[0]
}
