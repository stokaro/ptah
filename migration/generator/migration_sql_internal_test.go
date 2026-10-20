package generator

// White-box testing required: a generated migration's files are rendered by
// unexported functions, and the exported route that writes them,
// GenerateMigration, needs a live database. These helpers render the files
// through the same production functions from a diff and the two schemas.

import (
	"context"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasmigrate"
	"ptah.run/migration/planner"
)

// upMigrationSQL renders the up file of diff the way a generated migration
// renders it: the planned nodes, split into statements under the file's
// header and the generated timeout directives. caps defaults to the
// dialect's preset.
func upMigrationSQL(ctx context.Context, runtime Runtime, diff *difftypes.SchemaDiff, _ *schemamodel.Database, dialect string,
	caps ...capability.Capabilities,
) (string, error) {
	selected := capability.ForDialect(dialect)
	if len(caps) > 0 {
		selected = caps[0]
	}
	nodes, err := planner.GenerateSchemaDiffASTWithOptions(ctx, runtime, diff, dialect, planext.Options{Capabilities: selected})
	if err != nil {
		return "", err
	}
	return renderGeneratedMigrationSQL(ctx, runtime, nodes, dialect, selected, "UP",
		generatedDirectiveOptions{skipTimeouts: planner.RequiresNoTransaction(dialect, nodes)})
}

// downMigrationSQL renders the down file of diff the way a generated
// migration renders it: the reverse direction of the bidirectional plan.
// caps defaults to the dialect's preset.
func downMigrationSQL(ctx context.Context, runtime Runtime, diff *difftypes.SchemaDiff, desired *schemamodel.Database, current *catalog.Database,
	dialect string, caps ...capability.Capabilities,
) (string, error) {
	options := BidirectionalSchemaPlanOptions{Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: dialect}
	if len(caps) > 0 {
		options.Capabilities = caps[0]
	}
	if options.DesiredSchema == nil {
		options.DesiredSchema = &schemamodel.Database{}
	}
	if options.CurrentSchema == nil {
		options.CurrentSchema = &catalog.Database{}
	}
	plan, err := PlanBidirectionalSchemaDiff(ctx, options)
	if err != nil {
		return "", err
	}
	return renderGeneratedDownMigrationSQL(ctx, runtime, plan, atlasmigrate.Qualifier{})
}
