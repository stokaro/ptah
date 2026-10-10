package schemacensus

import (
	"context"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/engine"
	"ptah.run/internal/capabilityprobe"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// MeasurePlan is [Measure] over the other surface: the same desired schema
// compared against an empty database, planned, and rendered.
//
// It answers stokaro/ptah#2606's acceptance scenario 11 at the field level.
// `internal/modelast.TestRenderAndPlanAgreeOnEveryPostgresFamilyTarget`
// already compares the two surfaces by AST node kind, which catches an object
// one of them loses; this catches a FIELD one of them loses while both still
// emit the object.
//
// The entry points are the ones the product uses, and that is not a detail.
// Measured while writing this: reading the plan through
// [planner.GenerateSchemaDiffAST] and rendering the nodes separately reported
// eight TimescaleDB fields as lost, because the rule that turns a declared
// extension into a capability lives in the SQL-producing entry point. The
// product was right and the probe was wrong. Compare through the erroring
// [schemadiff.CompareWithDatabaseInfo] for the same reason: the pure entry
// points skip the validation every native command performs.
func MeasurePlan(ctx context.Context, runtime engine.SchemaRuntime) ([]Observation, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return nil, err
	}
	result, err := measure(planSurface(ctx, runtime))
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// planSurface answers with the shipping plan path. Each cell gets its own copy
// of the schema, because comparison is not promised to leave its input alone.
func planSurface(ctx context.Context, runtime engine.SchemaRuntime) surface {
	return func(schema schemamodel.Database) (func(capabilityprobe.Cell) (string, error), func() error) {
		answer := func(cell capabilityprobe.Cell) (string, error) {
			return planOne(ctx, runtime, schema, cell)
		}
		// Each cell plans its own copy, so there is nothing shared to verify.
		return answer, func() error { return nil }
	}
}

// planOne is the shipping plan path for one cell: compare against nothing, plan,
// render.
func planOne(ctx context.Context, runtime engine.SchemaRuntime, schema schemamodel.Database, cell capabilityprobe.Cell) (string, error) {
	finalized := deepCopyDatabase(schema)
	schemamodel.Finalize(&finalized)
	current, err := emptyCatalogForCell(cell)
	if err != nil {
		return "", err
	}

	diff, err := schemadiff.CompareWithDatabaseInfo(
		ctx, &finalized,
		current,
		catalog.ServerInfo{Dialect: cell.Dialect, Capabilities: cell.Preset()},
		nil, runtime,
	)
	if err != nil {
		return measuredRefusal(err)
	}
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		ctx, runtime,
		diff,
		cell.Dialect,
		planner.Options{Capabilities: cell.Preset()},
	)
	if err != nil {
		return measuredRefusal(err)
	}
	return strings.Join(statements, "\n"), nil
}

func emptyCatalogForCell(cell capabilityprobe.Cell) (*catalog.Database, error) {
	current := &catalog.Database{}
	if platform.IsPostgresFamily(cell.Dialect) {
		// A PostgreSQL-family read records TimescaleDB knowledge whether or not
		// the extension is installed; an empty database holds neither model.
		known, err := tsschema.CompleteCoverage(schemaext.Observed)
		if err != nil {
			return nil, err
		}
		current.FeatureCoverage = known
	}
	if cell.Dialect == platform.SQLServer {
		// A SQL Server read describes security policies; an empty database
		// holds none.
		known, err := mssqlschema.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)
		if err != nil {
			return nil, err
		}
		current.FeatureCoverage = known
	}
	if cell.Dialect != platform.YDB {
		return current, nil
	}
	// This fixture explicitly represents an empty database. Enroll the
	// standalone namespaces it knows are empty; an ordinary empty object
	// collection would correctly leave that namespace uninspected.
	// Keep enrollment explicit rather than deriving authority from runtime
	// growth when another provider model is registered.
	complete := schemaext.Knowledge{State: schemaext.Complete}
	for _, enroll := range []func() (schemaext.Coverage, error){
		func() (schemaext.Coverage, error) { return ydbcoordination.Coverage(schemaext.Observed, complete, nil) },
		func() (schemaext.Coverage, error) { return ydbstreaming.Coverage(schemaext.Observed, complete, nil) },
		func() (schemaext.Coverage, error) { return ydbsecret.Coverage(schemaext.Observed, complete, nil) },
		func() (schemaext.Coverage, error) { return ydbtopic.Coverage(schemaext.Observed, complete, nil) },
		func() (schemaext.Coverage, error) {
			return ydbexternal.SourceCoverage(schemaext.Observed, complete, nil)
		},
		func() (schemaext.Coverage, error) {
			return ydbexternal.TableCoverage(schemaext.Observed, complete, nil)
		},
		func() (schemaext.Coverage, error) {
			return ydbreplication.ReplicationCoverage(schemaext.Observed, complete, nil)
		},
		func() (schemaext.Coverage, error) {
			return ydbreplication.TransferCoverage(schemaext.Observed, complete, nil)
		},
		func() (schemaext.Coverage, error) {
			return ydbworkload.Coverage(ydbworkload.PoolKind, schemaext.Observed, complete, nil)
		},
		func() (schemaext.Coverage, error) {
			return ydbworkload.Coverage(ydbworkload.ClassifierKind, schemaext.Observed, complete, nil)
		},
	} {
		known, err := enroll()
		if err != nil {
			return nil, err
		}
		current.FeatureCoverage, err = current.FeatureCoverage.Combine(known)
		if err != nil {
			return nil, err
		}
	}
	return current, nil
}

// SurfaceDifference is one field the two surfaces do not agree about, and the
// reason it is recorded rather than repaired.
//
// Reason is required. A difference with no reason is the state this measurement
// exists to remove: two surfaces answering differently about the same
// declaration, with nobody having decided which is right.
type SurfaceDifference struct {
	Field string
	// RenderOnly is true when the render surface reads the field and the plan
	// surface does not, and false for the other direction.
	RenderOnly bool
	Reason     string
}

// SurfaceDifferences is every field the two surfaces are known to disagree
// about, measured at the commit that added this file.
//
// It is a ratchet rather than a description: the gate requires the measured
// disagreement to be exactly this set, so a NEW divergence fails and a repaired
// one fails until its entry is removed.
func SurfaceDifferences() []SurfaceDifference {
	return []SurfaceDifference{
		{
			Field: "schemamodel.Schema.Charset", RenderOnly: true,
			Reason: "only the MySQL-family renderer writes DEFAULT CHARACTER SET, " +
				"and a plan creates a schema on those dialects only for a whole server: a schema " +
				"there IS a database, and a plan creates one only against a connection that " +
				"selected no database (stokaro/ptah#3789). This measurement plans without a " +
				"connection, so it never plans a server. The field reaches every " +
				"CREATE SCHEMA a plan does emit (stokaro/ptah#2618)",
		},
		{
			Field: "schemamodel.Schema.Collate", RenderOnly: true,
			Reason: "the collation half of the same decision, unreachable on this plan for the same reason",
		},
		{
			Field: "schemamodel.Grant.Comment", RenderOnly: true,
			Reason: "the same leading `--` line, above GRANT",
		},
		{
			Field: "schemamodel.DefaultPrivilege.Comment", RenderOnly: true,
			Reason: "the same leading `--` line, above ALTER DEFAULT PRIVILEGES",
		},
		{
			Field: "schemamodel.Index.Concurrently", RenderOnly: true,
			Reason: "the render writes what the source declared, because its output is a " +
				"script the reader runs and a concurrent build is a promise about the lock " +
				"it takes. A plan cannot answer from the declaration alone: the statement " +
				"cannot run inside a transaction block, so it lands in a migration file " +
				"whose transaction mode the caller owns, and PostgreSQL refuses the build " +
				"outright on a partitioned table. `internal/concurrentindex.DeclaredRefs` " +
				"reads the same declaration, applies the capability and that exclusion, and " +
				"hands the planner the refs that survive (stokaro/ptah#3042)",
		},
	}
}
