package atlasreport

import (
	"cmp"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"time"

	"go.5x5.cz/ptah/internal/atlaslint"
	migrationlint "go.5x5.cz/ptah/migration/lint"
)

// lintWrapWidth is the content width Atlas wraps lint diagnostics at, measured
// on the pinned CE binary rather than chosen.
//
// It is bracketed, not guessed. A message whose content is 88 columns does NOT
// wrap; one whose next word would reach 89 DOES. Two fixtures with table names
// sized to land the boundary on 89 and on 90 both wrap, which rules out those
// values -- without them, 88, 89 and 90 all reproduce the everyday cases
// identically and the constant would have been unpinned.
const lintWrapWidth = 88

// WriteMigrateLintText renders the measured Atlas-compatible migration-analysis
// text report. The native `ptah migrations lint` command uses its own report
// renderer and retains Ptah's more detailed diagnostic prose. The report is
// written even when findings fail; the caller is responsible for the exit code.
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
	model := buildMigrateLintText(*opts.Analysis, opts.RevisionVersions)
	if model.analyzedCount == 0 {
		// No analyzed versions means there is no report to render.
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
	if _, err := fmt.Fprintf(w, "\n  -- analyzing version %s\n", version.version); err != nil {
		return err
	}
	if len(version.groups) == 0 {
		if _, err := fmt.Fprintln(w, "    -- no diagnostics found"); err != nil {
			return err
		}
	}
	var fixes []migrateLintTextFix
	for _, group := range version.groups {
		groupFixes, err := writeMigrateLintTextGroup(w, group)
		if err != nil {
			return err
		}
		fixes = append(fixes, groupFixes...)
	}
	if err := writeMigrateLintTextFixes(w, fixes); err != nil {
		return err
	}
	_, err := fmt.Fprintf(w, "  -- ok (%s)\n", elapsed)
	return err
}

func writeMigrateLintTextGroup(w io.Writer, group migrateLintTextGroup) ([]migrateLintTextFix, error) {
	if _, err := fmt.Fprintf(w, "    -- %s detected:\n", group.label); err != nil {
		return nil, err
	}
	// Atlas prints every diagnostic in the group first, then collects the
	// suggested fixes under ONE header at the end -- pluralized when there is
	// more than one. Interleaving fix-after-diagnostic produces identical
	// output for a single diagnostic, which is why that layout has to be
	// checked against a group with two.
	var fixes []migrateLintTextFix
	for _, diag := range group.diags {
		if err := writeMigrateLintTextDiagnostic(w, diag); err != nil {
			return nil, err
		}
		if diag.fix.text != "" {
			fixes = append(fixes, diag.fix)
		}
	}
	return fixes, nil
}

func writeMigrateLintTextFixes(w io.Writer, fixes []migrateLintTextFix) error {
	if len(fixes) == 0 {
		return nil
	}
	header := "    -- suggested fix:"
	if len(fixes) > 1 {
		header = "    -- suggested fixes:"
	}
	if _, err := fmt.Fprintln(w, header); err != nil {
		return err
	}
	for _, fix := range fixes {
		if err := writeWrappedAt(w, "      -> ", "         ", fix.text, fix.width); err != nil {
			return err
		}
	}
	return nil
}

func writeMigrateLintTextDiagnostic(w io.Writer, diag migrateLintTextDiag) error {
	if !diag.atlas {
		return writeWrapped(
			w,
			"      -- ",
			"         ",
			fmt.Sprintf("L%d [%s]: %s", diag.line, diag.code, diag.message),
		)
	}
	content := fmt.Sprintf("L%d: %s", diag.line, diag.message)
	if diag.code != "" {
		content += " " + atlasAnalyzerDocsURL + diag.code
	}
	return writeWrappedAt(w, "      -- ", "         ", content, diag.messageWidth)
}

func writeMigrateLintTextSummary(w io.Writer, model migrateLintText, elapsed string) error {
	if _, err := fmt.Fprintf(w, "\n  -------------------------\n  -- %s\n", elapsed); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  -- %s\n", versionSummary(model.okCount, model.warnCount, model.errCount)); err != nil {
		return err
	}
	// A run that expresses no schema change prints no schema-change line at all
	// rather than "0 schema changes". Measured on two shapes: a version whose
	// only statement is an INSERT, and a version whose only changes are to a
	// schema the dev URL does not cover -- both end at the version-summary line.
	if model.changes > 0 {
		if _, err := fmt.Fprintf(w, "  -- %s\n", pluralize(model.changes, "schema change", "schema changes")); err != nil {
			return err
		}
	}
	if model.diagnostics == 0 {
		return nil
	}
	_, err := fmt.Fprintf(w, "  -- %s\n", pluralize(model.diagnostics, "diagnostic", "diagnostics"))
	return err
}

// writeWrapped renders text word-wrapped to [lintWrapWidth] content
// columns, prefixing the first line with firstPrefix and every continuation
// line with contPrefix.
func writeWrapped(w io.Writer, firstPrefix, contPrefix, text string) error {
	return writeWrappedAt(w, firstPrefix, contPrefix, text, lintWrapWidth)
}

func writeWrappedAt(w io.Writer, firstPrefix, contPrefix, text string, width int) error {
	return writePrefixedLines(w, firstPrefix, contPrefix, wrapContent(text, width))
}

func writePrefixedLines(w io.Writer, firstPrefix, contPrefix string, lines []string) error {
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
	version string
	groups  []migrateLintTextGroup
}

type migrateLintTextGroup struct {
	label string
	diags []migrateLintTextDiag
}

type migrateLintTextDiag struct {
	line         int
	code         string
	message      string
	messageWidth int
	atlas        bool
	// fix is the Atlas-shaped suggested fix, printed under its own header.
	// Empty for analyzers where Atlas prints none.
	fix      migrateLintTextFix
	group    string
	severity migrationlint.Severity
}

type migrateLintTextFix struct {
	text  string
	width int
}

// buildMigrateLintText derives the intermediate text-report model from the lint
// analysis. It reuses the analysis's Selected flag and [File.Changes] count; it
// never re-parses SQL.
func buildMigrateLintText(
	analysis migrationlint.Analysis,
	revisionVersions map[int64]string,
) migrateLintText {
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

		version := migrateLintTextVersion{version: migrateLintVersionKey(file, revisionVersions)}
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
	switch {
	case last == "":
		model.header = fmt.Sprintf("Analyzing changes (%s in total):", migrations)
	case firstSelected > 0:
		base := migrateLintVersionKey(upFiles[firstSelected-1], revisionVersions)
		model.header = fmt.Sprintf("Analyzing changes from version %s to %s (%s in total):", base, last, migrations)
	default:
		model.header = fmt.Sprintf("Analyzing changes until version %s (%s in total):", last, migrations)
	}
	return model
}

func sortedUpFiles(files []migrationlint.File) []migrationlint.File {
	upFiles := make([]migrationlint.File, 0, len(files))
	for _, file := range files {
		if file.Direction != "up" || file.Ignored {
			continue
		}
		upFiles = append(upFiles, file)
	}
	maxVersion := migrateLintMaxVersion(upFiles)
	slices.SortStableFunc(upFiles, func(a, b migrationlint.File) int {
		return migrateLintFileOrder(a, b, maxVersion)
	})
	return upFiles
}

// diagnosticsByFile maps findings into compatibility diagnostics keyed
// by the cleaned reporting path of the file that owns them.
func diagnosticsByFile(findings []migrationlint.Finding) map[string][]migrateLintTextDiag {
	byFile := make(map[string][]migrateLintTextDiag)
	for _, finding := range findings {
		key := path.Clean(finding.File)
		byFile[key] = append(byFile[key], compatibilityDiagnostic(finding))
	}
	return byFile
}

// compatibilityDiagnostic maps a structured native finding to the measured
// Atlas analyzer code, wording, and suggested-fix shape used by ptah-compat.
func compatibilityDiagnostic(finding migrationlint.Finding) migrateLintTextDiag {
	rule := atlaslint.RuleForNativeCode(finding.Rule)
	diagnostic, mapped := atlasDiagnosticText(rule.Code, finding)
	if !mapped {
		return migrateLintTextDiag{
			line:         finding.Line,
			code:         rule.Code,
			message:      finding.Message,
			messageWidth: lintWrapWidth,
			group:        analyzerGroupLabel(rule.Analyzer),
			severity:     finding.Severity,
		}
	}
	return migrateLintTextDiag{
		line:         finding.Line,
		code:         rule.Code,
		message:      diagnostic.message,
		messageWidth: diagnostic.messageWidth,
		atlas:        true,
		fix:          migrateLintTextFix{text: diagnostic.fix, width: diagnostic.fixWidth},
		group:        analyzerGroupLabel(rule.Analyzer),
		severity:     finding.Severity,
	}
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

// groupDiagnostics collects a file's diagnostics into analyzer groups.
//
// Groups are emitted in analyzer order, not in the order the first diagnostic of
// each happens to appear. One version reporting a drop on line 2 and a
// non-nullable add on line 1 still prints the destructive group first: measured
// against the pinned community binary v1.3.0, `ALTER TABLE users ADD COLUMN x
// int NOT NULL` on line 1 followed by `ALTER TABLE users DROP COLUMN nick` on
// line 2 prints "destructive changes detected" above "data dependent changes
// detected". Ordering by first appearance printed them the other way round.
//
// Only analyzers whose relative order has been measured are ranked. Ptah's own
// diagnostics have no counterpart to be measured against, so they keep
// first-appearance order among themselves and follow the ranked groups.
func groupDiagnostics(diags []migrateLintTextDiag) []migrateLintTextGroup {
	var groups []migrateLintTextGroup
	index := make(map[string]int)
	for _, diag := range diags {
		at, ok := index[diag.group]
		if !ok {
			index[diag.group] = len(groups)
			groups = append(groups, migrateLintTextGroup{label: diag.group})
			at = len(groups) - 1
		}
		groups[at].diags = append(groups[at].diags, diag)
	}
	slices.SortStableFunc(groups, func(a, b migrateLintTextGroup) int {
		return cmp.Compare(analyzerGroupRank(a.label), analyzerGroupRank(b.label))
	})
	return groups
}

// analyzerGroupRank orders the analyzer groups whose relative order has been
// measured. Unranked groups share the last rank, which a stable sort leaves in
// first-appearance order behind the measured ones.
func analyzerGroupRank(label string) int {
	switch label {
	case analyzerGroupLabel(atlaslint.AnalyzerDestructive):
		return 0
	case analyzerGroupLabel(atlaslint.AnalyzerDataDependent):
		return 1
	default:
		return 2
	}
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
