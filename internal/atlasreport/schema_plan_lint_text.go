package atlasreport

import (
	"fmt"
	"io"
	"time"

	migrationlint "go.5x5.cz/ptah/migration/lint"
)

// The prefixes of the plan-lint report. A plan is one block of SQL rather than
// a directory of versions, so the report has no per-version block and every
// line sits one level shallower than the same line in the migration-directory
// report. Continuation prefixes are the width of their first prefix, so a
// wrapped message lines up under the text it continues rather than under the
// marker.
const (
	planLintLinePrefix   = "  -- "
	planLintDiagPrefix   = "    -- "
	planLintDiagContinue = "       "
	planLintFixPrefix    = "    -> "
	planLintFixContinue  = "       "
)

// SchemaPlanLintOptions are the inputs of the plan-file analysis report.
type SchemaPlanLintOptions struct {
	// Analysis is the lint analysis of the plan's statements. Required.
	Analysis *migrationlint.Analysis
}

// WriteSchemaPlanLintText renders the analysis of a plan file's statements.
//
// The report is written whatever the analysis found; the caller owns the exit
// code. It reuses the diagnostic wording, grouping and suggested-fix layout of
// the migration-directory report, because the two are one linter reporting on
// the same statements and a reader should not have to learn a second shape to
// read the same finding.
func WriteSchemaPlanLintText(w io.Writer, opts SchemaPlanLintOptions) error {
	return writeSchemaPlanLintText(w, opts, time.Now)
}

// writeSchemaPlanLintText is the clock-injectable core of
// [WriteSchemaPlanLintText]; tests replace now with a fixed clock so the
// elapsed line is deterministic.
func writeSchemaPlanLintText(w io.Writer, opts SchemaPlanLintOptions, now func() time.Time) error {
	if opts.Analysis == nil {
		return fmt.Errorf("plan lint analysis is required")
	}
	model := buildSchemaPlanLintText(*opts.Analysis)

	start := now()
	// The noun is already in the header sentence, so the count carries none:
	// "planned statements (7 in total)" rather than "(7 statements in total)".
	if _, err := fmt.Fprintf(w, "Analyzing planned statements (%d in total):\n\n", model.statements); err != nil {
		return err
	}
	if len(model.groups) == 0 {
		if _, err := fmt.Fprintln(w, planLintLinePrefix+"no diagnostics found"); err != nil {
			return err
		}
	}
	var fixes []migrateLintTextFix
	for _, group := range model.groups {
		groupFixes, err := writeSchemaPlanLintGroup(w, group)
		if err != nil {
			return err
		}
		fixes = append(fixes, groupFixes...)
	}
	if err := writeSchemaPlanLintFixes(w, fixes); err != nil {
		return err
	}
	return writeSchemaPlanLintSummary(w, model, now().Sub(start).String())
}

func writeSchemaPlanLintGroup(w io.Writer, group migrateLintTextGroup) ([]migrateLintTextFix, error) {
	if _, err := fmt.Fprintf(w, "%s%s detected:\n", planLintLinePrefix, group.label); err != nil {
		return nil, err
	}
	var fixes []migrateLintTextFix
	for _, diag := range group.diags {
		if err := writeSchemaPlanLintDiagnostic(w, diag); err != nil {
			return nil, err
		}
		if diag.fix.text != "" {
			fixes = append(fixes, diag.fix)
		}
	}
	return fixes, nil
}

func writeSchemaPlanLintDiagnostic(w io.Writer, diag migrateLintTextDiag) error {
	if !diag.atlas {
		return writeWrapped(
			w,
			planLintDiagPrefix,
			planLintDiagContinue,
			fmt.Sprintf("L%d [%s]: %s", diag.line, diag.code, diag.message),
		)
	}
	content := fmt.Sprintf("L%d: %s", diag.line, diag.message)
	if diag.code != "" {
		content += " " + atlasAnalyzerDocsURL + diag.code
	}
	return writeWrappedAt(w, planLintDiagPrefix, planLintDiagContinue, content, diag.messageWidth)
}

// writeSchemaPlanLintFixes prints every suggested fix of the report under one
// header, pluralized with the number of fixes rather than the number of
// diagnostics: a group can hold a diagnostic that suggests nothing.
func writeSchemaPlanLintFixes(w io.Writer, fixes []migrateLintTextFix) error {
	if len(fixes) == 0 {
		return nil
	}
	header := planLintLinePrefix + "suggested fix:"
	if len(fixes) > 1 {
		header = planLintLinePrefix + "suggested fixes:"
	}
	if _, err := fmt.Fprintln(w, header); err != nil {
		return err
	}
	for _, fix := range fixes {
		if err := writeWrappedAt(w, planLintFixPrefix, planLintFixContinue, fix.text, fix.width); err != nil {
			return err
		}
	}
	return nil
}

// writeSchemaPlanLintSummary closes the report with the elapsed time and the
// counts. A plan expressing no structural change prints no schema-change line
// at all rather than "0 schema changes", and a plan with nothing to report
// prints no diagnostic line, so each count line means "there were some".
func writeSchemaPlanLintSummary(w io.Writer, model schemaPlanLintText, elapsed string) error {
	if _, err := fmt.Fprintf(w, "\n  -------------------------\n%s%s\n", planLintLinePrefix, elapsed); err != nil {
		return err
	}
	if model.changes > 0 {
		if _, err := fmt.Fprintf(w, "%s%s\n",
			planLintLinePrefix, pluralize(model.changes, "schema change", "schema changes")); err != nil {
			return err
		}
	}
	if model.diagnostics == 0 {
		return nil
	}
	_, err := fmt.Fprintf(w, "%s%s\n",
		planLintLinePrefix, pluralize(model.diagnostics, "diagnostic", "diagnostics"))
	return err
}

// schemaPlanLintText is the intermediate model of the plan-lint report.
type schemaPlanLintText struct {
	statements  int
	changes     int
	diagnostics int
	groups      []migrateLintTextGroup
}

// buildSchemaPlanLintText derives the report model from the analysis.
//
// Findings are grouped by analyzer for the whole plan rather than per file:
// the analysis of a plan holds one file, and a report that grouped per file
// would be describing a directory the plan does not have.
func buildSchemaPlanLintText(analysis migrationlint.Analysis) schemaPlanLintText {
	var model schemaPlanLintText
	for _, file := range analysis.Files() {
		model.statements += len(file.Statements)
		model.changes += len(file.Changes)
	}
	var diags []migrateLintTextDiag
	for _, finding := range analysis.Findings() {
		diags = append(diags, compatibilityDiagnostic(finding))
	}
	model.diagnostics = len(diags)
	model.groups = groupDiagnostics(diags)
	return model
}
