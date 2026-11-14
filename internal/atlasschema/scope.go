package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/featureselect"
	"ptah.run/internal/readscope"
	"ptah.run/internal/schemaselection"
)

// LiveSelection is the selection a verb takes that compares a desired state with
// a live database and changes neither: the selectors `schema diff` takes, with
// the same meaning and the same diagnostics. Its zero value selects nothing.
type LiveSelection struct {
	// Schemas names the schemas the run reads and keeps on both sides, as
	// --schemas does on `schema diff`. An object outside them leaves both sides,
	// and a selected object that depends on one is refused.
	Schemas []string
	// Include positively selects what both sides keep.
	Include []string
	// Exclude subtracts from what both sides keep.
	Exclude []string
	// Diagnostics receives the warning for an Exclude selector that matched
	// nothing on either side. Nil writes no warning.
	Diagnostics io.Writer
}

// Validate refuses a malformed selector, for a caller that has not contacted a
// database yet. A pattern's depth depends on the connection, so [LiveSelection.Apply]
// can still refuse one that passes here.
func (s LiveSelection) Validate(selections selectext.Set) error {
	if err := atlasfilter.ValidateExcludeSelectors(s.Exclude, selections); err != nil {
		return err
	}
	return atlasfilter.ValidateIncludeSelectors(s.Include, selections)
}

// ReadContext marks ctx with the object kinds a selector leaves out whole, so a
// schema read does not have to answer for them. An account that cannot see a
// MySQL trigger reads the rest of the database under --exclude '*[type=trigger]'.
func (s LiveSelection) ReadContext(ctx context.Context, selections selectext.Set) context.Context {
	return readscope.Without(ctx, atlasfilter.KindsLeftOut(selections, s.Exclude, s.Include)...)
}

// Apply projects a live read and a desired state through the selection, and
// returns both unchanged when no selector was given.
//
// An Exclude selector that matched nothing on either side is a warning, because
// the verbs that take this selection only report. Include selectors that matched
// nothing on either side return the [atlasfilter.EmptySelectionError], because a
// comparison of nothing would report a synced database. A selected object that
// depends on an unselected one returns the [atlasfilter.CrossScopeError].
func (s LiveSelection) Apply(
	ctx context.Context,
	runtime selectext.Runtime,
	info catalog.ServerInfo,
	current *catalog.Database,
	desired *schemamodel.Database,
) (*catalog.Database, *schemamodel.Database, error) {
	scope := atlasfilter.Scope{
		Schemas:               s.Schemas,
		Include:               s.Include,
		Exclude:               s.Exclude,
		DefaultSchema:         info.Schema,
		RealmRelativePatterns: ConnectionIsRealmScoped(info),
		Selections:            runtime.Selections(),
	}
	if !slices.ContainsFunc(s.Schemas, atlasfilter.NamesSelector) &&
		!slices.ContainsFunc(s.Include, atlasfilter.NamesSelector) &&
		!slices.ContainsFunc(s.Exclude, atlasfilter.NamesSelector) {
		return current, desired, nil
	}
	scoped, err := projectLiveStates(ctx, runtime, info.Dialect, current, desired, scope)
	if err != nil {
		return nil, nil, err
	}
	reportUnmatchedExclude(s.Diagnostics, atlasfilter.UnmatchedAcrossStates(scoped.currentReports.Exclude, scoped.desiredReports.Exclude))
	if emptySelection(scoped.currentErr) && emptySelection(scoped.desiredErr) {
		return nil, nil, scoped.currentErr
	}
	applyExtensionSupportCoverage(scoped.desired, scoped.currentReports.Selection, scoped.desiredReports.Selection)
	return scoped.current, scoped.desired, nil
}

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

// requirementScope turns a positive comparison scope into its second-pass
// projection when either side kept an object for a requirement alone, such as
// the resource pool a kept classifier routes to. Both sides are projected
// again with what the desired side kept that way (see
// [atlasfilter.DesiredRequirements]): kept on one side only, a pool the
// database holds next to a declared classifier is planned as a creation the
// server refuses, and a pool a classifier routed to before the plan moves it
// is planned as a removal no selector asked for.
func requirementScope(scope atlasfilter.Scope, desired, current atlasfilter.SelectionReport) (atlasfilter.Scope, bool) {
	if scope.Requirements.Decided() || len(desired.Required) == 0 && len(current.Required) == 0 {
		return scope, false
	}
	scope.Requirements = atlasfilter.DesiredRequirements(desired)
	return scope, true
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

// described returns the descriptor of the dialect a spelling selects, and the
// zero descriptor, which states none of the facts this package reads, for one
// the process registry does not know.
func described(dialect string) descriptor.Dialect {
	found, _ := platform.Lookup(dialect)
	return found
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
	bindings, err := featureselect.CaptureBindings(ctx, featureselect.RelationsOf(runtime), dialect, identifier.ForDialect(dialect), sides...)
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
