package atlasschema

import (
	"context"
	"strings"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/planext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasreport"
	"ptah.run/internal/planstyle"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// CreationPlanRuntime plans the statements that create an inspected schema;
// see [InspectOptions.SQLCreationPlan].
type CreationPlanRuntime interface {
	schemadiff.TargetRuntime
	planner.Runtime
}

// The pinned community binary v1.3.0 answers `schema inspect --format
// '{{ sql . }}'` with the plan that creates the inspected schema, the plan its
// `schema diff` writes from an empty database. Measured on PostgreSQL 18 and
// SQLite:
//
//	-- Create "users" table
//	CREATE TABLE "users" ("id" integer NOT NULL, "name" text NOT NULL, PRIMARY KEY ("id"));
//	-- Create index "users_name" to table: "users"
//	CREATE INDEX "users_name" ON "users" ("name");
//
// So ptah-compat plans it the same way, in the statement style and shape its
// `schema diff` plans in, and writes the bytes that `schema diff` writes from a
// new database to the inspected one. Where those differ from v1.3.0 they differ
// for both commands: v1.3.0 drops the extensions, standalone sequences and
// policies a database holds from its plan, and Ptah keeps them. The rendered
// schema native `ptah schema inspect` writes is a description, laid out for
// reading, and stays the native answer.

// inspectCreationPlan returns the plan that creates desired, the model of the
// inspected read, on a new database of the same server.
//
// A new database holds the schema a connection lands in, with its comment and
// its grants, and the extensions the server installs in every database, which
// [config.DefaultCompareOptions] names. So the plan creates none of them, as
// `schema diff` from a new database plans none of them. It holds every
// inspected schema when describeSchemas is false: the scope came from the
// connection URL, and v1.3.0 then writes no CREATE SCHEMA, as
// [atlasreport.SchemaInspectReport] says for the rendered SQL.
func inspectCreationPlan(
	runtime CreationPlanRuntime,
	desired *schemamodel.Database,
	read *catalog.Database,
	info catalog.ServerInfo,
	describeSchemas bool,
) func(context.Context) ([]atlasreport.SchemaChange, error) {
	return func(ctx context.Context) ([]atlasreport.SchemaChange, error) {
		// The new database describes what the read described and holds
		// nothing else of it, so a kind the read established is created and a
		// kind it left out stays undecided.
		empty := &catalog.Database{
			FeatureCoverage:         read.FeatureCoverage,
			PlanningContextCoverage: read.PlanningContextCoverage,
			NotDescribed:            read.NotDescribed,
		}
		held := func(name string) bool {
			return !describeSchemas || name == info.IdentifierSemantics.DefaultSchema
		}
		for _, schema := range read.Schemas {
			if held(schema.Name) {
				empty.Schemas = append(empty.Schemas, schema)
			}
		}
		for _, grant := range read.Grants {
			if strings.EqualFold(grant.ObjectType, "SCHEMA") && held(grant.ObjectName) {
				empty.Grants = append(empty.Grants, grant)
			}
		}
		compared, err := schemadiff.CompareWithDatabaseInfo(ctx, desired, empty, info, config.DefaultCompareOptions(), runtime)
		if err != nil {
			return nil, err
		}
		if !compared.HasChanges() {
			return nil, nil
		}
		changes, plan, err := planChanges(ctx, runtime, compared, info.Dialect, planext.Options{
			Capabilities: info.Capabilities,
			Statements:   planstyle.Statements,
			Shape:        planext.StatementShapeCatalog,
		})
		if err != nil {
			return nil, err
		}
		if err := nameDiffChanges(changes, plan, info.Dialect); err != nil {
			return nil, err
		}
		return changes, nil
	}
}
