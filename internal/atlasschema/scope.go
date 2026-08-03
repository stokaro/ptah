package atlasschema

import (
	"errors"
	"fmt"
	"io"

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
func scopeGeneratedSide(db *goschema.Database, scope atlasfilter.Scope, side string) (*goschema.Database, error) {
	if scope.Positive() {
		filtered, err := atlasfilter.ScopeGenerated(db, scope)
		if emptySelection(err) {
			return filtered, err
		}
		if err != nil {
			return nil, fmt.Errorf("apply --schema/--include to %s: %w", side, err)
		}
		return filtered, nil
	}
	filtered, err := atlasfilter.ExcludeGenerated(db, scope.Exclude)
	if err != nil {
		return nil, fmt.Errorf("apply --exclude to %s: %w", side, err)
	}
	return filtered, nil
}

// scopeDatabaseSide filters one introspected comparison side with the same
// projection as scopeGeneratedSide, so both sides of a comparison always see
// one selection.
func scopeDatabaseSide(db *types.DBSchema, scope atlasfilter.Scope, side string) (*types.DBSchema, error) {
	if scope.Positive() {
		filtered, err := atlasfilter.ScopeDatabase(db, scope)
		if emptySelection(err) {
			return filtered, err
		}
		if err != nil {
			return nil, fmt.Errorf("apply --schema/--include to %s: %w", side, err)
		}
		return filtered, nil
	}
	filtered, err := atlasfilter.ExcludeDatabase(db, scope.Exclude)
	if err != nil {
		return nil, fmt.Errorf("apply --exclude to %s: %w", side, err)
	}
	return filtered, nil
}

// emptySelection reports whether err is the empty-include-selection signal.
func emptySelection(err error) bool {
	var empty *atlasfilter.EmptySelectionError
	return errors.As(err, &empty)
}

// reportEmptySelection writes the selection-matched-nothing notice to the
// command's diagnostics stream. Verbs that keep exit 0 for an empty selection
// still have to say so: stdout is what CI compares, and an empty render there
// is indistinguishable from an empty database.
func reportEmptySelection(diagnostics io.Writer, err error) {
	if diagnostics == nil || err == nil {
		return
	}
	fmt.Fprintf(diagnostics, "Warning: %s.\n", err.Error())
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
