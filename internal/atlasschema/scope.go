package atlasschema

import (
	"fmt"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
)

// scopeGeneratedSide filters one generated-schema comparison side. Positive
// scopes (--schema/--include) run the full projection with cross-scope
// dependency validation; exclusion-only scopes keep the plain --exclude path
// and its established semantics.
func scopeGeneratedSide(db *goschema.Database, scope atlasfilter.Scope, side string) (*goschema.Database, error) {
	if scope.Positive() {
		filtered, err := atlasfilter.ScopeGenerated(db, scope)
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
