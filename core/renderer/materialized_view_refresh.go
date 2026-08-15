package renderer

import (
	"fmt"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
)

// manualRefreshStrategy is the one refresh policy a target can carry, because
// it is the absence of a policy: a materialized view nobody refreshes is
// exactly what CREATE MATERIALIZED VIEW leaves behind on every engine Ptah
// renders for.
const manualRefreshStrategy = "manual"

// canonicalRefreshStrategy folds a declared refresh strategy through the type
// that owns the spelling rules.
//
// It goes through [goschema.MaterializedView.Canonicalize] rather than
// repeating "lowercase, trim, default to manual" here, so the fold the refusal
// judges by and the fold the rest of the pipeline stores cannot drift apart.
// A hand-written `strategy == "manual"` refuses `MANUAL`, ` manual ` and the
// empty strategy the annotation parser leaves on a matview that declared none,
// all three of which are the default policy spelled differently.
func canonicalRefreshStrategy(strategy string) string {
	view := goschema.MaterializedView{RefreshStrategy: strategy}
	view.Canonicalize()
	return view.RefreshStrategy
}

// materializedViewRefreshStrategyRepresentable answers whether a rendered
// schema can carry strategy at all.
//
// Today the answer is "only the manual policy", for every dialect, and the
// reason is not that the engines are poor: it is that a refresh policy is not
// part of a schema on any of them. PostgreSQL stores nothing about how a
// materialized view is refreshed -- REFRESH MATERIALIZED VIEW [CONCURRENTLY]
// is a statement an operator runs, not an attribute pg_class keeps -- and
// ClickHouse keeps its materialized views current from inserts and has no
// REFRESH statement at all. Ptah plans no REFRESH in a migration either
// (migration/planner emits none, and the PostgreSQL planner test
// TestPlanner_GenerateMigrationAST_MaterializedViewRefreshStrategyDoesNotAutoRefresh
// pins that), so a declared `concurrently` or `every 5 minutes` had nowhere to
// go: the renderer dropped it, introspection reported `manual` on the way back
// in, and the comparator therefore saw no drift. The command reported a synced
// schema and the operator never learned the policy they asked for was never
// applied (stokaro/ptah#1523).
//
// Refusing is what this repository already does with a declared attribute a
// target cannot represent, rather than rendering it away: a view's WITH CHECK
// OPTION on ClickHouse and on SQLite, DROP VIEW CASCADE on ClickHouse, and an
// extension installation schema on CockroachDB and Spanner
// ([unsupportedExtensionInstallationSchema], the neighbor of this rule in the
// same validation seam) all answer ErrUnsupportedFeature naming the dialect
// and the value. The SQLite and MySQL renderers say why in their own words:
// a comment makes `schema render` exit 0 on a model `schema apply` refuses, so
// the surface a user validates with disagrees with the surface that executes.
//
// The dialect is not a parameter because no target represents a non-manual
// policy today; it is named in the refusal so the operator knows which target
// refused. A dialect that later grows a refresh workflow -- a plan that emits
// REFRESH MATERIALIZED VIEW CONCURRENTLY, with the unique index PostgreSQL
// requires for it -- makes that strategy representable here first.
func materializedViewRefreshStrategyRepresentable(strategy string) bool {
	return canonicalRefreshStrategy(strategy) == manualRefreshStrategy
}

// unsupportedMaterializedViewRefreshStrategy names the target, the materialized
// view and the value the operator wrote.
//
// The declared spelling is echoed verbatim rather than folded, because it is
// the string to search their schema for.
func unsupportedMaterializedViewRefreshStrategy(dialect, name, strategy string) error {
	normalized := platform.NormalizeDialect(dialect)
	return &ptaherr.CapabilityError{
		Dialect: normalized,
		Feature: "materialized view refresh strategies",
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf(
			"%s cannot represent refresh_strategy %q declared on materialized view %q: "+
				"only %q is supported, because the target stores no refresh policy and ptah plans no REFRESH statement",
			normalized,
			strategy,
			name,
			manualRefreshStrategy,
		),
	}
}

// validateDeclaredMaterializedViewRefreshStrategies refuses a declaration a
// target cannot represent before anything is emitted, which is the phase both
// whole-schema rendering and migration planning pass through.
//
// A target whose materialized views are switched off entirely is left alone:
// its renderer already answers for the whole object -- MySQL, MariaDB, SQLite
// and SQL Server refuse it, and the PostgreSQL family writes a skip comment
// naming what was omitted for Spanner -- and no DDL is produced for the
// strategy to be silently dropped from.
func validateDeclaredMaterializedViewRefreshStrategies(
	dialect string,
	caps capability.Capabilities,
	views []goschema.MaterializedView,
) error {
	if !caps.Has(capability.MaterializedViews) {
		return nil
	}
	for _, view := range views {
		if materializedViewRefreshStrategyRepresentable(view.RefreshStrategy) {
			continue
		}
		return unsupportedMaterializedViewRefreshStrategy(dialect, view.Name, view.RefreshStrategy)
	}
	return nil
}

// prepareCreateMaterializedViewNode applies the same rule to an AST node.
//
// The declaration seam above cannot be the only one: an AST node reaches a
// renderer from the migration planner and from callers of the public
// [RenderSQL] surface without a goschema.Database in sight, and that is the
// path stokaro/ptah#1523 measured. Both seams call the one predicate.
func prepareCreateMaterializedViewNode(
	dialect string,
	caps capability.Capabilities,
	node *ast.CreateMaterializedViewNode,
) (*ast.CreateMaterializedViewNode, error) {
	if node == nil {
		return nil, &ptaherr.RenderError{
			Dialect: dialect,
			Err:     ptaherr.ErrInvalidSchemaDiff,
			Message: "materialized view node is nil",
		}
	}
	if !caps.Has(capability.MaterializedViews) {
		return node, nil
	}
	if !materializedViewRefreshStrategyRepresentable(node.RefreshStrategy) {
		return nil, unsupportedMaterializedViewRefreshStrategy(dialect, node.Name, node.RefreshStrategy)
	}
	return node, nil
}
