// Package planlint analyzes the SQL a saved schema plan file carries with
// Ptah's migration lint rules.
//
// A plan file is not a migration directory. It holds one block of SQL under a
// plan name, with no file-name convention and no rollback half, so the rules
// that describe migration-file form have nothing to say about it. This package
// presents the plan under a name those rules already treat as complete, which
// leaves the plan judged on its statements and on nothing else.
package planlint

import (
	"fmt"

	"go.5x5.cz/ptah/internal/fsnapshot"
	"go.5x5.cz/ptah/internal/lintdialect"
	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
)

// sourceName is the name the plan's SQL is analyzed under.
//
// It is an Atlas-layout migration name because that is the layout whose files
// carry no rollback half and no NNNNNNNNNN_description.(up|down).sql spelling:
// the linter treats such a file as paired and well named, so MF101 and MF103
// stay silent rather than reporting a plan for not being a migration
// directory. The version digits are inert -- nothing orders one plan against
// another -- and they are fixed so two analyses of one plan produce identical
// findings.
const sourceName = "20060102150405_plan.sql"

// Analyze runs the migration lint rules over the SQL of a plan file.
//
// dialect is the engine the plan will be applied to, in any spelling Ptah
// accepts; it selects the dialect-specific rule families and the scanner's
// lexing mode. An unsupported spelling is refused rather than analyzed under
// the dialect-independent families alone: a run that silently drops the
// PostgreSQL or MySQL rules reports fewer hazards than the caller asked for,
// and reporting fewer hazards is the one failure this analysis must not have.
//
// The compatibility profile is the Atlas-compatible one, so a plan is analyzed
// on the same terms as a migration file holding the same SQL is analyzed by
// `ptah-compat migrate lint`: the same rules, the same codes, and the same
// `atlas:nolint` directives.
func Analyze(sql, dialect string) (lint.Analysis, error) {
	canonical, ok := lintdialect.Canonical(dialect)
	if !ok {
		return lint.Analysis{}, fmt.Errorf(
			"unsupported lint dialect %q; expected one of %s", dialect, lintdialect.Expected)
	}
	snapshot, err := fsnapshot.FromFiles(map[string][]byte{sourceName: []byte(sql)})
	if err != nil {
		return lint.Analysis{}, fmt.Errorf("prepare plan for analysis: %w", err)
	}
	return lint.AnalyzeFS(snapshot, lint.Options{
		Compatibility: lint.CompatibilityProfileAtlas,
		Dialect:       canonical,
		DirFormat:     migrator.MigrationDirFormatAtlas,
	})
}

// HasErrorSeverity reports whether the analysis carries a finding at error
// severity. It is what an opt-in failure threshold asks; the report itself
// prints every finding regardless of severity.
func HasErrorSeverity(analysis lint.Analysis) bool {
	for _, finding := range analysis.Findings() {
		if finding.Severity == lint.SeverityError {
			return true
		}
	}
	return false
}
