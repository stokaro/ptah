package atlasschema

import (
	"context"
	"io"
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

// creationScope is what a creation plan takes from the scope of the read.
type creationScope struct {
	// describeSchemas is false when the connection URL chose the scope, and
	// the new database then holds every inspected schema.
	describeSchemas bool
	// partial is set when the read covers some schemas of the database rather
	// than the whole database: --schema named them, or the connection URL
	// named one.
	partial bool
}

// inspectCreationPlan returns the plan that creates desired, the model of the
// inspected read, on a new database of the same server.
//
// A new database holds the schema a connection lands in, with its comment and
// its grants, and the extensions the server installs in every database, which
// [config.DefaultCompareOptions] names. So the plan creates none of them, as
// `schema diff` from a new database plans none of them. It holds every
// inspected schema when the scope came from the connection URL: v1.3.0 then
// writes no CREATE SCHEMA, as [atlasreport.SchemaInspectReport] says for the
// rendered SQL.
//
// A read of the whole database plans every object it holds. A read of some of
// its schemas plans what those schemas hold, and the database it creates them
// in already holds what belongs to the database rather than to any of them: a
// grant on the database itself, and an object of the whole database installed
// in a schema outside the scope, such as a PostgreSQL extension
// ([schemaext.SchemaInstallation]). The read keeps both, because neither has
// a schema to scope it by. So the plan creates neither, a step that uses one
// finds it present, and one note on diagnostics names what the plan left out.
// Planned, a grant on the database names a database the scope does not
// describe, and YDB refuses to render one without the database's path.
//
// The plan is made against the path of the inspected database, which is how
// YDB names the target of a grant on it.
func inspectCreationPlan(
	runtime CreationPlanRuntime,
	desired *schemamodel.Database,
	read *catalog.Database,
	info catalog.ServerInfo,
	scope creationScope,
	diagnostics io.Writer,
) func(context.Context) ([]atlasreport.SchemaChange, error) {
	return func(ctx context.Context) ([]atlasreport.SchemaChange, error) {
		options := config.DefaultCompareOptions()
		// The new database describes what the read described and holds
		// nothing else of it, so a kind the read established is created and a
		// kind it left out stays undecided.
		empty := &catalog.Database{
			FeatureCoverage:         read.FeatureCoverage,
			PlanningContextCoverage: read.PlanningContextCoverage,
			NotDescribed:            read.NotDescribed,
			DatabasePath:            read.DatabasePath,
		}
		held := func(name string) bool {
			return !scope.describeSchemas || name == info.IdentifierSemantics.DefaultSchema
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
		if scope.partial {
			leftOut, err := databaseObjectsOutsideScope(read, options)
			if err != nil {
				return nil, err
			}
			empty.Grants = append(empty.Grants, leftOut.grants...)
			empty.FeatureObjects = leftOut.objects
			leftOut.report(diagnostics)
		}
		compared, err := schemadiff.CompareWithDatabaseInfo(ctx, desired, empty, info, options, runtime)
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
