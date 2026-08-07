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
	subject := atlasSoleSubject(finding)
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
		// One ALTER TABLE that drops several columns is ONE diagnostic naming
		// all of them, not one per column -- the opposite of DS102, and
		// measured that way rather than assumed. The nouns and the trailing
		// pronoun move with the count, so both forms are spelled out here
		// instead of being derived from the single-column string.
		names, ok := atlasIdentifierNames(finding)
		if !ok {
			return atlasDiagnosticCopy{}, false
		}
		if len(names) == 1 {
			return atlasDiagnosticCopy{
				message:      fmt.Sprintf("Dropping non-virtual column %q", names[0]),
				fix:          fmt.Sprintf("Add a pre-migration check to ensure column %q is NULL before dropping it", names[0]),
				messageWidth: atlasDS103MessageWidth,
				fixWidth:     lintWrapWidth,
			}, true
		}
		list := atlasQuotedList(names)
		return atlasDiagnosticCopy{
			message:      fmt.Sprintf("Dropping non-virtual columns %s", list),
			fix:          fmt.Sprintf("Add pre-migration checks to ensure columns %s are NULL before dropping them", list),
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
	if catalogSpelledDataType(normalized) {
		return normalized, true
	}
	return "", false
}

// catalogDataTypes are the PostgreSQL catalog type names the compatibility
// surface prints unchanged, each measured against the pinned community binary
// v1.3.0 on PostgreSQL 16 by renaming a NOT NULL column of that type and reading
// the MF103 diagnostic it produces (stokaro/ptah#1074).
//
// The set is deliberately short. `timestamp with time zone` is absent because
// the measured diagnostic says `timestamptz`, `timestamp without time zone`
// because it says `timestamp`, and array types because the diagnostic says
// `integer[]` where the catalog says `ARRAY` -- none of those is the catalog's
// own spelling, so passing the catalog through would print a type the tool being
// matched never prints. Those fall through to Ptah's labeled prose instead.
var catalogDataTypes = map[string]bool{
	"bigint":            true,
	"boolean":           true,
	"bytea":             true,
	"character":         true,
	"character varying": true,
	"date":              true,
	"double precision":  true,
	"integer":           true,
	"json":              true,
	"jsonb":             true,
	"numeric":           true,
	"real":              true,
	"smallint":          true,
	"text":              true,
	"uuid":              true,
}

// catalogSpelledDataType reports whether a lower-cased type name is one of the
// measured catalog spellings, with or without the length or precision the
// diagnostic carries in parentheses.
//
// Measured forms: `character varying(20)`, `character(5)`, `numeric(10,2)` and
// `numeric(10)` -- PostgreSQL reports scale 0 for `numeric(10,0)` and the
// diagnostic prints `numeric(10)` for both spellings.
func catalogSpelledDataType(normalized string) bool {
	base, arguments, found := strings.Cut(normalized, "(")
	if !found {
		return catalogDataTypes[normalized]
	}
	arguments, closed := strings.CutSuffix(arguments, ")")
	if !closed || arguments == "" || !catalogDataTypes[base] {
		return false
	}
	for argument := range strings.SplitSeq(arguments, ",") {
		if argument == "" || strings.TrimLeft(argument, "0123456789") != "" {
			return false
		}
	}
	return true
}

// atlasSoleSubject returns the schema object a one-object diagnostic is about.
//
// DS102 and MF103 are one diagnostic per object, so the analyzers emit one
// finding per object and a finding carrying several of them is a shape this
// wording cannot render. It returns nil in that case, which drops the caller to
// Ptah's own labeled prose. The alternative -- rendering the first subject --
// is what hid the other dropped tables: silently narrowing a destructive
// finding to one of its objects reads as complete output and is not.
func atlasSoleSubject(finding migrationlint.Finding) *migrationlint.Subject {
	if finding.Context == nil || len(finding.Context.Subjects) != 1 {
		return nil
	}
	return &finding.Context.Subjects[0]
}

// atlasIdentifierNames returns every subject's logical name. It fails as a unit
// so a diagnostic naming several objects can never render a partial list.
func atlasIdentifierNames(finding migrationlint.Finding) ([]string, bool) {
	if finding.Context == nil || len(finding.Context.Subjects) == 0 {
		return nil, false
	}
	names := make([]string, 0, len(finding.Context.Subjects))
	for _, subject := range finding.Context.Subjects {
		if subject.Name == "" {
			return nil, false
		}
		name, ok := atlasIdentifierName(subject.Name)
		if !ok {
			return nil, false
		}
		names = append(names, name)
	}
	return names, true
}

// atlasQuotedList renders two or more names as the measured prose list:
// comma-separated, with "and" before the last. Callers handle the single-name
// case, whose surrounding sentence differs by more than the separator.
func atlasQuotedList(names []string) string {
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, fmt.Sprintf("%q", name))
	}
	last := len(quoted) - 1
	return strings.Join(quoted[:last], ", ") + " and " + quoted[last]
}
