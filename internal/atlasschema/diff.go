package atlasschema

import (
	"fmt"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/internal/atlasfilter"
	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/internal/atlasurl"
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
}

func DiffLocalFiles(opts DiffOptions) (atlasreport.SchemaDiff, error) {
	dialect, err := atlasurl.DialectFromURL(opts.DevURL)
	if err != nil {
		return atlasreport.SchemaDiff{}, err
	}
	if dialect == "" {
		return atlasreport.SchemaDiff{}, fmt.Errorf("--dev-url is required for local schema file diffing")
	}
	from, err := schemafile.LoadAll(opts.FromURLs, schemafile.Options{Dialect: dialect})
	if err != nil {
		return atlasreport.SchemaDiff{}, fmt.Errorf("load --from schema: %w", err)
	}
	from, err = excludeDesiredSchema(from, opts.Exclude)
	if err != nil {
		return atlasreport.SchemaDiff{}, fmt.Errorf("apply --exclude to --from schema: %w", err)
	}
	to, err := schemafile.LoadAll(opts.ToURLs, schemafile.Options{Dialect: dialect})
	if err != nil {
		return atlasreport.SchemaDiff{}, fmt.Errorf("load --to schema: %w", err)
	}
	to, err = excludeDesiredSchema(to, opts.Exclude)
	if err != nil {
		return atlasreport.SchemaDiff{}, fmt.Errorf("apply --exclude to --to schema: %w", err)
	}

	diff := applyDiffPolicy(schemadiff.CompareWithDialect(to, schemafile.ToDBSchema(from), dialect), opts.Policy)
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

func excludeDesiredSchema(db *goschema.Database, patterns []string) (*goschema.Database, error) {
	return atlasfilter.ExcludeGenerated(db, patterns)
}
