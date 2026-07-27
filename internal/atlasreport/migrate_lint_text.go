package atlasreport

import (
	"cmp"
	"fmt"
	"io"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/stokaro/ptah/internal/atlaslint"
	migrationlint "github.com/stokaro/ptah/migration/lint"
)

// atlasLintWrapWidth is the number of columns a diagnostic or suggested-fix
// line is wrapped to, excluding the fixed nine-column reporting prefix. It is
// derived from the Atlas migrate lint text fixtures: the longest diagnostic
// that fits on one line is 88 content columns and the shortest word that wraps
// to a continuation line pushes past 90, so 90 is the exact Atlas budget.
const atlasLintWrapWidth = 90

// atlasLintAnalyzerBaseURL is the documentation anchor prefix Atlas appends to
// every diagnostic message.
const atlasLintAnalyzerBaseURL = "https://atlasgo.io/lint/analyzers#"

// WriteMigrateLintText renders the default Atlas-compatible migration-analysis
// text report. It reproduces Atlas's `migrate lint` stdout for the no-custom-
// format path: a per-version analysis block followed by a summary. The report
// is written even when findings fail; the caller is responsible for the exit
// code.
func WriteMigrateLintText(w io.Writer, opts MigrateLintOptions) error {
	return writeMigrateLintText(w, opts, time.Now)
}

// writeMigrateLintText is the clock-injectable core of [WriteMigrateLintText].
// The now function supplies the wall clock used for the per-version and total
// elapsed durations, which the exported entry point sets to time.Now and tests
// replace with a fixed clock for deterministic output.
func writeMigrateLintText(w io.Writer, opts MigrateLintOptions, now func() time.Time) error {
	if opts.Analysis == nil {
		return fmt.Errorf("migration lint analysis is required")
	}
	model := buildMigrateLintText(*opts.Analysis)
	if model.analyzedCount == 0 {
		// No analyzed versions (empty changeset or a wholly ignored directory):
		// Atlas emits nothing.
		return nil
	}

	start := now()
	elapsed := func() string { return now().Sub(start).String() }

	if _, err := fmt.Fprintln(w, model.header); err != nil {
		return err
	}
	for i := range model.versions {
		if err := writeMigrateLintTextVersion(w, model.versions[i], elapsed()); err != nil {
			return err
		}
	}
	return writeMigrateLintTextSummary(w, model, elapsed())
}

func writeMigrateLintTextVersion(w io.Writer, version migrateLintTextVersion, elapsed string) error {
	if _, err := fmt.Fprintf(w, "\n  -- analyzing version %d\n", version.version); err != nil {
		return err
	}
	if len(version.groups) == 0 {
		if _, err := fmt.Fprintln(w, "    -- no diagnostics found"); err != nil {
			return err
		}
	}
	for _, group := range version.groups {
		if err := writeMigrateLintTextGroup(w, group); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(w, "  -- ok (%s)\n", elapsed)
	return err
}

func writeMigrateLintTextGroup(w io.Writer, group migrateLintTextGroup) error {
	if _, err := fmt.Fprintf(w, "    -- %s detected:\n", group.label); err != nil {
		return err
	}
	var fixes []string
	for _, diag := range group.diags {
		content := fmt.Sprintf("L%d: %s %s", diag.line, diag.message, diag.url)
		if err := writeWrapped(w, "      -- ", "         ", content); err != nil {
			return err
		}
		if diag.fix != "" {
			fixes = append(fixes, diag.fix)
		}
	}
	if len(fixes) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w, "    -- suggested fix:"); err != nil {
		return err
	}
	for _, fix := range fixes {
		if err := writeWrapped(w, "      -> ", "         ", fix); err != nil {
			return err
		}
	}
	return nil
}

func writeMigrateLintTextSummary(w io.Writer, model migrateLintText, elapsed string) error {
	if _, err := fmt.Fprintf(w, "\n  -------------------------\n  -- %s\n", elapsed); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  -- %s\n", versionSummary(model.okCount, model.warnCount, model.errCount)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  -- %s\n", pluralize(model.changes, "schema change", "schema changes")); err != nil {
		return err
	}
	if model.diagnostics == 0 {
		return nil
	}
	_, err := fmt.Fprintf(w, "  -- %s\n", pluralize(model.diagnostics, "diagnostic", "diagnostics"))
	return err
}

// writeWrapped renders text word-wrapped to [atlasLintWrapWidth] content
// columns, prefixing the first line with firstPrefix and every continuation
// line with contPrefix.
func writeWrapped(w io.Writer, firstPrefix, contPrefix, text string) error {
	lines := wrapContent(text, atlasLintWrapWidth)
	for i, line := range lines {
		prefix := contPrefix
		if i == 0 {
			prefix = firstPrefix
		}
		if _, err := fmt.Fprintf(w, "%s%s\n", prefix, line); err != nil {
			return err
		}
	}
	return nil
}

// wrapContent greedily wraps text on spaces so that no line exceeds width
// content columns, except when a single word is itself longer than width.
func wrapContent(text string, width int) []string {
	words := strings.Fields(text)
	if len(words) == 0 {
		return []string{""}
	}
	lines := make([]string, 0, 1)
	current := words[0]
	for _, word := range words[1:] {
		if len(current)+1+len(word) <= width {
			current += " " + word
			continue
		}
		lines = append(lines, current)
		current = word
	}
	return append(lines, current)
}

type migrateLintText struct {
	header        string
	versions      []migrateLintTextVersion
	changes       int
	diagnostics   int
	okCount       int
	warnCount     int
	errCount      int
	analyzedCount int
}

type migrateLintTextVersion struct {
	version int64
	groups  []migrateLintTextGroup
}

type migrateLintTextGroup struct {
	label string
	diags []migrateLintTextDiag
}

type migrateLintTextDiag struct {
	line     int
	message  string
	url      string
	fix      string
	group    string
	severity migrationlint.Severity
}

// buildMigrateLintText derives the intermediate text-report model from the lint
// analysis. It reuses the analysis's Selected flag and [File.Changes] count; it
// never re-parses SQL.
func buildMigrateLintText(analysis migrationlint.Analysis) migrateLintText {
	upFiles := sortedUpFiles(analysis.Files())
	diagsByFile := diagnosticsByFile(analysis.Findings())

	firstSelected := -1
	var model migrateLintText
	for i := range upFiles {
		file := upFiles[i]
		if !file.Selected {
			continue
		}
		if firstSelected < 0 {
			firstSelected = i
		}
		model.analyzedCount++
		model.changes += len(file.Changes)

		version := migrateLintTextVersion{version: file.Version}
		version.groups = groupDiagnostics(diagsByFile[path.Clean(file.Path)])
		hasError, hasWarning, count := versionSeverity(version.groups)
		model.diagnostics += count
		switch {
		case hasError:
			model.errCount++
		case hasWarning:
			model.warnCount++
		default:
			model.okCount++
		}
		model.versions = append(model.versions, version)
	}
	if model.analyzedCount == 0 {
		return model
	}

	last := model.versions[len(model.versions)-1].version
	migrations := pluralize(model.analyzedCount, "migration", "migrations")
	if firstSelected > 0 {
		base := upFiles[firstSelected-1].Version
		model.header = fmt.Sprintf("Analyzing changes from version %d to %d (%s in total):", base, last, migrations)
	} else {
		model.header = fmt.Sprintf("Analyzing changes until version %d (%s in total):", last, migrations)
	}
	return model
}

func sortedUpFiles(files []migrationlint.File) []migrationlint.File {
	upFiles := make([]migrationlint.File, 0, len(files))
	for _, file := range files {
		if file.Repeatable || file.Direction != "up" || file.Ignored {
			continue
		}
		upFiles = append(upFiles, file)
	}
	slices.SortStableFunc(upFiles, func(a, b migrationlint.File) int {
		return cmp.Or(cmp.Compare(a.Version, b.Version), strings.Compare(a.Name, b.Name))
	})
	return upFiles
}

// diagnosticsByFile expands every finding into Atlas-shaped diagnostics keyed
// by the cleaned reporting path of the file that owns them.
func diagnosticsByFile(findings []migrationlint.Finding) map[string][]migrateLintTextDiag {
	byFile := map[string][]migrateLintTextDiag{}
	for _, finding := range findings {
		key := path.Clean(finding.File)
		byFile[key] = append(byFile[key], atlasDiagnostics(finding)...)
	}
	return byFile
}

// atlasDiagnostics synthesizes the Atlas diagnostic message(s) for one native
// finding. Codes whose Atlas wording is pinned by the conformance fixtures are
// reproduced exactly from the finding's subjects; any other code falls back to
// the native message so the renderer never drops a finding.
func atlasDiagnostics(finding migrationlint.Finding) []migrateLintTextDiag {
	rule := atlaslint.RuleForNativeCode(finding.Rule)
	url := atlasLintAnalyzerBaseURL + rule.Code
	group := analyzerGroupLabel(rule.Analyzer)

	subjects := findingSubjects(finding)
	switch finding.Rule {
	case "DS101": // table dropped -> Atlas DS102
		if diags := tableDropDiagnostics(finding.Line, url, group, subjects); len(diags) > 0 {
			return diags
		}
	case "DD101": // non-nullable column added -> Atlas MF103
		if diags := nonNullableAddDiagnostics(finding.Line, url, group, subjects); len(diags) > 0 {
			return diags
		}
	}
	return []migrateLintTextDiag{{
		line:     finding.Line,
		message:  finding.Message,
		url:      url,
		group:    group,
		severity: finding.Severity,
	}}
}

func tableDropDiagnostics(line int, url, group string, subjects []migrationlint.Subject) []migrateLintTextDiag {
	var diags []migrateLintTextDiag
	for _, subject := range subjects {
		if subject.Kind != migrationlint.SubjectTable {
			continue
		}
		name := atlasQuote(subject.Name)
		diags = append(diags, migrateLintTextDiag{
			line:     line,
			message:  fmt.Sprintf("Dropping table %s", name),
			url:      url,
			fix:      fmt.Sprintf("Add a pre-migration check to ensure table %s is empty before dropping it", name),
			group:    group,
			severity: migrationlint.SeverityError,
		})
	}
	return diags
}

func nonNullableAddDiagnostics(line int, url, group string, subjects []migrationlint.Subject) []migrateLintTextDiag {
	var diags []migrateLintTextDiag
	for _, subject := range subjects {
		if subject.Kind != migrationlint.SubjectColumn {
			continue
		}
		diags = append(diags, migrateLintTextDiag{
			line: line,
			message: fmt.Sprintf(
				"Adding a non-nullable %s column %s will fail in case table %s is not empty",
				atlasQuote(subject.DataType), atlasQuote(subject.Name), atlasQuote(subject.Parent),
			),
			url:      url,
			group:    group,
			severity: migrationlint.SeverityWarning,
		})
	}
	return diags
}

func findingSubjects(finding migrationlint.Finding) []migrationlint.Subject {
	if finding.Context == nil {
		return nil
	}
	return finding.Context.Subjects
}

// atlasQuote renders an identifier or type the way Atlas does in diagnostics:
// stripped of any source quoting, then double-quoted.
func atlasQuote(name string) string {
	return strconv.Quote(strings.Trim(name, "`\"[]"))
}

func analyzerGroupLabel(analyzer string) string {
	switch analyzer {
	case atlaslint.AnalyzerDestructive:
		return "destructive changes"
	case atlaslint.AnalyzerDataDependent:
		return "data dependent changes"
	case atlaslint.AnalyzerConcurrentIndex:
		return "concurrent index violations"
	case atlaslint.AnalyzerIncompatible:
		return "backward incompatible changes"
	case atlaslint.AnalyzerNestedTX:
		return "nested transaction blocks"
	default:
		return "diagnostics"
	}
}

// groupDiagnostics collects a file's diagnostics into analyzer groups, keeping
// the order in which each analyzer first appears.
func groupDiagnostics(diags []migrateLintTextDiag) []migrateLintTextGroup {
	var groups []migrateLintTextGroup
	index := map[string]int{}
	for _, diag := range diags {
		at, ok := index[diag.group]
		if !ok {
			index[diag.group] = len(groups)
			groups = append(groups, migrateLintTextGroup{label: diag.group})
			at = len(groups) - 1
		}
		groups[at].diags = append(groups[at].diags, diag)
	}
	return groups
}

func versionSeverity(groups []migrateLintTextGroup) (hasError, hasWarning bool, count int) {
	for _, group := range groups {
		for _, diag := range group.diags {
			count++
			if diag.severity == migrationlint.SeverityError {
				hasError = true
				continue
			}
			hasWarning = true
		}
	}
	return hasError, hasWarning, count
}

// versionSummary renders the "-- N version ok, M with warnings" summary line.
// The leading segment carries the pluralized "version(s)" noun; ok versions
// always lead, then warnings, then errors.
func versionSummary(ok, warn, errc int) string {
	type segment struct {
		count int
		label string
	}
	var segments []segment
	if ok > 0 {
		segments = append(segments, segment{ok, "ok"})
	}
	if warn > 0 {
		segments = append(segments, segment{warn, "with warnings"})
	}
	if errc > 0 {
		segments = append(segments, segment{errc, "with errors"})
	}
	if len(segments) == 0 {
		return "0 versions ok"
	}
	noun := "version"
	if segments[0].count != 1 {
		noun = "versions"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d %s %s", segments[0].count, noun, segments[0].label)
	for _, seg := range segments[1:] {
		fmt.Fprintf(&b, ", %d %s", seg.count, seg.label)
	}
	return b.String()
}

func pluralize(n int, singular, plural string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, singular)
	}
	return fmt.Sprintf("%d %s", n, plural)
}
