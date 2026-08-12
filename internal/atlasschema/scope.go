package atlasschema

import (
	"errors"
	"fmt"
	"io"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
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
	db *goschema.Database,
	scope atlasfilter.Scope,
	side string,
) (*goschema.Database, atlasfilter.ExcludeReport, error) {
	if scope.Positive() {
		filtered, report, err := atlasfilter.ScopeGeneratedReport(db, scope)
		if emptySelection(err) {
			return filtered, report, err
		}
		if err != nil {
			return nil, atlasfilter.ExcludeReport{}, fmt.Errorf("apply --schema/--include to %s: %w", side, err)
		}
		return filtered, report, nil
	}
	filtered, report, err := atlasfilter.ExcludeGeneratedReport(db, scope.Exclude, scope.DefaultSchema)
	if err != nil {
		return nil, atlasfilter.ExcludeReport{}, fmt.Errorf("apply --exclude to %s: %w", side, err)
	}
	return filtered, report, nil
}

// scopeDatabaseSide filters one introspected comparison side with the same
// projection as scopeGeneratedSide, so both sides of a comparison always see
// one selection.
func scopeDatabaseSide(
	db *types.DBSchema,
	scope atlasfilter.Scope,
	side string,
) (*types.DBSchema, atlasfilter.ExcludeReport, error) {
	if scope.Positive() {
		filtered, report, err := atlasfilter.ScopeDatabaseReport(db, scope)
		if emptySelection(err) {
			return filtered, report, err
		}
		if err != nil {
			return nil, atlasfilter.ExcludeReport{}, fmt.Errorf("apply --schema/--include to %s: %w", side, err)
		}
		return filtered, report, nil
	}
	filtered, report, err := atlasfilter.ExcludeDatabaseReport(db, scope.Exclude, scope.DefaultSchema)
	if err != nil {
		return nil, atlasfilter.ExcludeReport{}, fmt.Errorf("apply --exclude to %s: %w", side, err)
	}
	return filtered, report, nil
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

// ReportUndecidedAdditions names every object the comparison declined to plan a
// creation for because the CURRENT side's coverage record says it does not
// describe that kind (`// ptah:not-described <kind>` in a document) and the
// creation Ptah would emit carries no IF NOT EXISTS guard.
//
// Withholding one is defensible; withholding it in silence is not. Only a
// currentDescription and desiredDescription name the two command-specific
// inputs in prose, so schema diff can say `--from` and migrate diff can name
// the replayed migration directory without producing a misleading diagnostic.
func ReportUndecidedAdditions(
	diagnostics io.Writer,
	undecided []coverage.Object,
	currentDescription string,
	desiredDescription string,
) {
	if diagnostics == nil {
		return
	}
	for _, object := range undecided {
		fmt.Fprintf(diagnostics,
			"Warning: %s %q is declared by %s but no change was planned for it:"+
				" %s records `%s %s`, so this comparison cannot tell it apart from one that already exists,"+
				" and the creation Ptah renders for it has no IF NOT EXISTS guard.\n",
			object.Kind, object.Name, desiredDescription, currentDescription,
			coverage.DirectiveMarker, object.Kind)
	}
}

// dialectDefaultSchema is the schema that owns unqualified objects when no
// database-backed side pins one: "public" for PostgreSQL-family dialects and
// "main" for SQLite. MySQL-family schemas are databases, so only a
// database-backed side can name the default.
func dialectDefaultSchema(dialect string) string {
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB:
		return "public"
	case platform.SQLite:
		return "main"
	default:
		return ""
	}
}
