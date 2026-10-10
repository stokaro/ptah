package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"io"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/featureselect"
	"ptah.run/internal/schemaselection"
)

// scopeGeneratedSide filters one generated-schema comparison side. Positive
// scopes (--schema/--include) run the full projection with cross-scope
// dependency validation; exclusion-only scopes keep the plain --exclude path
// and its established semantics.
//
// An empty include selection is returned as a valid (empty) projection plus an
// [atlasfilter.EmptySelectionError], never wrapped: one side of a comparison
// matching nothing is ordinary — that is how a CREATE or a DROP looks — so the
// decision belongs to the caller that can see both sides.
func scopeGeneratedSide(
	db *schemamodel.Database,
	scope atlasfilter.Scope,
	side string,
) (*schemamodel.Database, atlasfilter.ScopeReports, error) {
	if scope.Positive() {
		filtered, reports, err := atlasfilter.ScopeGeneratedSelectionReport(db, scope)
		if emptySelection(err) {
			return filtered, reports, err
		}
		if err != nil {
			return nil, atlasfilter.ScopeReports{}, fmt.Errorf("apply --schema/--include to %s: %w", side, err)
		}
		return filtered, reports, nil
	}
	filtered, report, err := atlasfilter.ExcludeGeneratedScopeReport(db, scope)
	if err != nil {
		return nil, atlasfilter.ScopeReports{}, fmt.Errorf("apply --exclude to %s: %w", side, err)
	}
	return filtered, atlasfilter.ScopeReports{Exclude: report}, nil
}

// scopeDatabaseSide filters one introspected comparison side with the same
// projection as scopeGeneratedSide, so both sides of a comparison always see
// one selection.
func scopeDatabaseSide(
	db *catalog.Database,
	scope atlasfilter.Scope,
	side string,
) (*catalog.Database, atlasfilter.ScopeReports, error) {
	if scope.Positive() {
		filtered, reports, err := atlasfilter.ScopeDatabaseSelectionReport(db, scope)
		if emptySelection(err) {
			return filtered, reports, err
		}
		if err != nil {
			return nil, atlasfilter.ScopeReports{}, fmt.Errorf("apply --schema/--include to %s: %w", side, err)
		}
		return filtered, reports, nil
	}
	filtered, report, err := atlasfilter.ExcludeDatabaseScopeReport(db, scope)
	if err != nil {
		return nil, atlasfilter.ScopeReports{}, fmt.Errorf("apply --exclude to %s: %w", side, err)
	}
	return filtered, atlasfilter.ScopeReports{Exclude: report}, nil
}

// applyExtensionSupportCoverage aggregates the two positive-selection outcomes
// before comparison. A non-extension match on either side makes extensions
// support objects: desired declarations can still add them, but desired
// silence cannot request removal of an unrelated current extension.
func applyExtensionSupportCoverage(
	desired *schemamodel.Database,
	reports ...atlasfilter.SelectionReport,
) {
	if desired == nil {
		return
	}
	for _, report := range reports {
		if report.NonExtensionMatched {
			desired.NotDescribed = desired.NotDescribed.With(coverage.Object{
				Kind:       coverage.Extension,
				Reason:     coverage.OutsideScope,
				Provenance: coverage.Configured,
			})
			return
		}
	}
}

// extensionSupportScope turns a positive comparison scope into its second-pass
// projection when either side selected a non-extension resource. Both original
// sides must be projected again with this scope: carrying extensions on only
// the side that observed the match can manufacture an extension addition or
// removal that the database-wide object never underwent.
func extensionSupportScope(
	scope atlasfilter.Scope,
	reports ...atlasfilter.SelectionReport,
) (atlasfilter.Scope, bool) {
	if scope.ExtensionSupport {
		return scope, false
	}
	for _, report := range reports {
		if report.NonExtensionMatched {
			scope.ExtensionSupport = true
			return scope, true
		}
	}
	return scope, false
}

// emptySelection reports whether err is the empty-include-selection signal.
func emptySelection(err error) bool {
	var empty *atlasfilter.EmptySelectionError
	return errors.As(err, &empty)
}

// reportEmptySelection writes the selection-matched-nothing notice to the
// command's diagnostics stream. Inspection keeps exit 0 because an empty read
// is a legitimate result, but still says which selection produced it.
func reportEmptySelection(diagnostics io.Writer, err error) {
	if diagnostics == nil || err == nil {
		return
	}
	fmt.Fprintf(diagnostics, "Warning: %s.\n", err.Error())
}

// reportUnmatchedExclude writes the exclude-matched-nothing notice for verbs
// that keep their exit status. Silence is the one answer this must never give:
// an --exclude that named nothing left the object in the output, and the
// output alone cannot say whether that was the schema or the selector.
func reportUnmatchedExclude(diagnostics io.Writer, selectors []string) {
	if diagnostics == nil || len(selectors) == 0 {
		return
	}
	fmt.Fprintf(diagnostics, "Warning: %s.\n", (&atlasfilter.UnmatchedExcludeError{Selectors: selectors}).Error())
}

// refuseUnmatchedExclude turns unmatched --exclude selectors into the error
// `schema apply` fails with.
//
// Whether the run refuses at all is the caller's decision, resolved from
// [atlasfilter.AllowUnmatchedExcludeEnvVar] before any state is read; a caller
// that opted back into the permissive behavior calls
// [reportUnmatchedExclude] instead.
//
// Apply is the verb that executes, so it is the one that refuses: a user
// writes --exclude to keep an object out of the plan, and a selector that
// named nothing means the plan is free to change it. Diff and inspect warn
// instead, which is the split #1113 recorded for the --include side.
func refuseUnmatchedExclude(selectors []string) error {
	if len(selectors) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%w; schema apply refuses a selection that protects nothing, set %s=1 to keep the permissive behavior",
		&atlasfilter.UnmatchedExcludeError{Selectors: selectors},
		atlasfilter.AllowUnmatchedExcludeEnvVar)
}

// dialectDefaultSchema is the schema that owns unqualified objects when no
// database-backed side pins one. See [schemaselection.DialectDefault], which a
// composite desired state's placement check reads too.
func dialectDefaultSchema(dialect string) string {
	return schemaselection.DialectDefault(dialect)
}

// scopeBindings captures, for a scope that selects anything, the tables the
// standalone feature objects on sides bind, so the selection keeps or leaves
// out each whole and refuses one it would split. A scope selecting nothing
// needs none.
func scopeBindings(ctx context.Context, runtime any, dialect string, scope atlasfilter.Scope, sides ...featureselect.Side) (featureselect.Bindings, error) {
	if !scope.Positive() && len(scope.Exclude) == 0 {
		return featureselect.Bindings{}, nil
	}
	relations, _ := runtime.(featureselect.RelationRuntime)
	bindings, err := featureselect.CaptureBindings(ctx, relations, dialect, identifier.ForDialect(dialect), sides...)
	if err != nil {
		return featureselect.Bindings{}, fmt.Errorf("capture the tables feature objects bind: %w", err)
	}
	return bindings, nil
}

// generatedSide and databaseSide are the feature states of a declaration and
// of a read, for capturing bindings. A nil database holds none.
func generatedSide(db *schemamodel.Database) featureselect.Side {
	if db == nil {
		return featureselect.Side{Representation: schemaext.Desired}
	}
	return featureselect.Side{Representation: schemaext.Desired, Objects: db.FeatureObjects, Coverage: db.FeatureCoverage}
}

func databaseSide(db *catalog.Database) featureselect.Side {
	if db == nil {
		return featureselect.Side{Representation: schemaext.Observed}
	}
	return featureselect.Side{Representation: schemaext.Observed, Objects: db.FeatureObjects, Coverage: db.FeatureCoverage}
}
