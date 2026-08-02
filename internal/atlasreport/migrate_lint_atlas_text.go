package atlasreport

import (
	"fmt"

	migrationlint "go.5x5.cz/ptah/migration/lint"
)

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
func atlasDiagnosticText(code string, finding migrationlint.Finding) (message, fix string) {
	subject := atlasPrimarySubject(finding)
	switch code {
	case "DS102":
		if subject == nil || subject.Name == "" {
			return finding.Message, ""
		}
		return fmt.Sprintf("Dropping table %q", subject.Name),
			fmt.Sprintf("Add a pre-migration check to ensure table %q is empty before dropping it",
				subject.Name)
	case "MF103":
		if subject == nil || subject.Name == "" || subject.Parent == "" || subject.DataType == "" {
			return finding.Message, ""
		}
		return fmt.Sprintf(
			"Adding a non-nullable %q column %q will fail in case table %q is not empty",
			subject.DataType, subject.Name, subject.Parent), ""
	default:
		return finding.Message, ""
	}
}

// atlasPrimarySubject returns the schema object a diagnostic is about.
func atlasPrimarySubject(finding migrationlint.Finding) *migrationlint.Subject {
	if finding.Context == nil || len(finding.Context.Subjects) == 0 {
		return nil
	}
	return &finding.Context.Subjects[0]
}
