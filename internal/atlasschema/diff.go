package atlasschema

import (
	"context"
	"fmt"
	"time"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/schemafile"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

type DiffOptions struct {
	FromURLs []string
	ToURLs   []string
	DevURL   string
	Exclude  []string
	// Schemas restricts both comparison sides to the named schema scopes.
	Schemas []string
	// Include restricts both comparison sides to resources matched by
	// Atlas-style include selectors.
	Include []string
	Policy  DiffPolicy
	// ProjectEnv expands env:// desired-state references in FromURLs and
	// ToURLs.
	ProjectEnv atlassource.ProjectEnv
	// ConnectTimeout bounds opening database-backed sources (including the
	// dev database) and reading their initial connection metadata. A zero
	// value leaves the caller's context deadline unchanged.
	ConnectTimeout time.Duration
	// IgnoreUnknownHCLNames is the Atlas-compatible surface's unknown-name
	// policy; see [go.5x5.cz/ptah/internal/atlassource.ResolveOptions].
	IgnoreUnknownHCLNames bool
}

// Diff computes the Atlas schema diff between two desired-state sources.
// Either side accepts local schema files, one database URL, one migration
// directory (replayed on --dev-url), or one env:// reference. The SQL dialect
// is pinned by --dev-url first, then by --from and --to database sources;
// local files alone still require --dev-url.
func Diff(ctx context.Context, opts DiffOptions) (atlasreport.SchemaDiff, error) {
	fromSet, err := atlassource.ClassifySet("--from", opts.FromURLs, opts.ProjectEnv)
	if err != nil {
		return atlasreport.SchemaDiff{}, err
	}
	toSet, err := atlassource.ClassifySet("--to", opts.ToURLs, opts.ProjectEnv)
	if err != nil {
		return atlasreport.SchemaDiff{}, err
	}
	if err := fromSet.EnsureDevDatabase(opts.DevURL); err != nil {
		return atlasreport.SchemaDiff{}, err
	}
	if err := toSet.EnsureDevDatabase(opts.DevURL); err != nil {
		return atlasreport.SchemaDiff{}, err
	}
	dialect, dialectFlag, err := atlassource.PinDialect(opts.DevURL, fromSet, toSet)
	if err != nil {
		return atlasreport.SchemaDiff{}, err
	}
	if dialect == "" {
		return atlasreport.SchemaDiff{}, fmt.Errorf("--dev-url is required for local schema file diffing")
	}

	resolveOpts := atlassource.ResolveOptions{
		Dialect:               dialect,
		DialectFlag:           dialectFlag,
		DevURL:                opts.DevURL,
		ConnectTimeout:        opts.ConnectTimeout,
		IgnoreUnknownHCLNames: opts.IgnoreUnknownHCLNames,
	}
	fromState, err := fromSet.Resolve(ctx, resolveOpts)
	if err != nil {
		return atlasreport.SchemaDiff{}, fmt.Errorf("load --from schema: %w", err)
	}
	toState, err := toSet.Resolve(ctx, resolveOpts)
	if err != nil {
		return atlasreport.SchemaDiff{}, fmt.Errorf("load --to schema: %w", err)
	}

	scope := atlasfilter.Scope{
		Schemas:       opts.Schemas,
		Include:       opts.Include,
		Exclude:       opts.Exclude,
		DefaultSchema: diffDefaultSchema(dialect, fromState, toState),
	}
	from, err := scopeGeneratedSide(fromState.Schema, scope, "--from schema")
	if err != nil {
		return atlasreport.SchemaDiff{}, err
	}
	to, err := scopeGeneratedSide(toState.Schema, scope, "--to schema")
	if err != nil {
		return atlasreport.SchemaDiff{}, err
	}
	fromDB, err := diffFromDBState(fromState, from, scope)
	if err != nil {
		return atlasreport.SchemaDiff{}, err
	}

	diff := applyDiffPolicy(schemadiff.CompareWithDialect(to, fromDB, dialect), opts.Policy)
	var statements []string
	if diff.HasChanges() {
		statements, err = planner.GenerateSchemaDiffSQLStatementsWithOptions(diff, to, dialect, planner.Options{
			ConcurrentIndexes: opts.Policy.ConcurrentIndexCreate,
		})
		if err != nil {
			return atlasreport.SchemaDiff{}, fmt.Errorf("generate schema diff SQL: %w", err)
		}
	}
	return atlasreport.NewSchemaDiff(from, to, statements), nil
}

// diffFromDBState shapes the --from side for comparison. Database-backed
// sources keep their introspected state, filtered by the same scope as every
// other side; local files convert the already-filtered desired IR, exactly as
// before URL sources existed.
func diffFromDBState(
	state atlassource.State,
	filtered *goschema.Database,
	scope atlasfilter.Scope,
) (*types.DBSchema, error) {
	if state.DB == nil {
		return schemafile.ToDBSchema(filtered), nil
	}
	return scopeDatabaseSide(state.DB, scope, "--from schema")
}

// diffDefaultSchema resolves the schema that owns unqualified objects for one
// diff. Both sides must share one default, so a database-backed side pins it
// (--from first), and local-file-only diffs fall back to the dialect default.
func diffDefaultSchema(dialect string, fromState, toState atlassource.State) string {
	if fromState.DefaultSchema != "" {
		return fromState.DefaultSchema
	}
	if toState.DefaultSchema != "" {
		return toState.DefaultSchema
	}
	return dialectDefaultSchema(dialect)
}
