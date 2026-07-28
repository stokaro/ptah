package atlasschema

import (
	"context"
	"fmt"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/atlasfilter"
	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/internal/atlassource"
	"github.com/stokaro/ptah/internal/schemafile"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
)

type DiffOptions struct {
	FromURLs []string
	ToURLs   []string
	DevURL   string
	Exclude  []string
	Policy   DiffPolicy
	// ProjectEnv expands env:// desired-state references in FromURLs and
	// ToURLs.
	ProjectEnv atlassource.ProjectEnv
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
		Dialect:     dialect,
		DialectFlag: dialectFlag,
		DevURL:      opts.DevURL,
	}
	fromState, err := fromSet.Resolve(ctx, resolveOpts)
	if err != nil {
		return atlasreport.SchemaDiff{}, fmt.Errorf("load --from schema: %w", err)
	}
	toState, err := toSet.Resolve(ctx, resolveOpts)
	if err != nil {
		return atlasreport.SchemaDiff{}, fmt.Errorf("load --to schema: %w", err)
	}

	from, err := excludeDesiredSchema(fromState.Schema, opts.Exclude)
	if err != nil {
		return atlasreport.SchemaDiff{}, fmt.Errorf("apply --exclude to --from schema: %w", err)
	}
	to, err := excludeDesiredSchema(toState.Schema, opts.Exclude)
	if err != nil {
		return atlasreport.SchemaDiff{}, fmt.Errorf("apply --exclude to --to schema: %w", err)
	}
	fromDB, err := diffFromDBState(fromState, from, opts.Exclude)
	if err != nil {
		return atlasreport.SchemaDiff{}, fmt.Errorf("apply --exclude to --from schema: %w", err)
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
// sources keep their introspected state (exclude-filtered); local files
// convert the already-filtered desired IR, exactly as before URL sources
// existed.
func diffFromDBState(
	state atlassource.State,
	filtered *goschema.Database,
	exclude []string,
) (*types.DBSchema, error) {
	if state.DB == nil {
		return schemafile.ToDBSchema(filtered), nil
	}
	return atlasfilter.ExcludeDatabase(state.DB, exclude)
}

func excludeDesiredSchema(db *goschema.Database, patterns []string) (*goschema.Database, error) {
	return atlasfilter.ExcludeGenerated(db, patterns)
}
