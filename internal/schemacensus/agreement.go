package schemacensus

import (
	"context"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
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
	"ptah.run/feature/oratype"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgextension"
	"ptah.run/feature/pgtype"
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
		var err error
		current.FeatureCoverage, err = emptyPostgresFeatureCoverage()
		if err != nil {
			return nil, err
		}
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
	if cell.Dialect == platform.Oracle {
		known, err := emptyOracleTypeCoverage(cell.Preset())
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
			Field:  "schemamodel.Database.FeatureCoverage",
			Reason: "creation renders explicit feature values; comparison requires source knowledge before it can grant, revoke, or restore default privileges, so removing that knowledge produces an incomplete-comparison refusal",
		},
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
			Field: "pgdefaultprivilege.Desired.Comment", RenderOnly: true,
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

// emptyOracleTypeCoverage is what an Oracle read of an empty schema knows of
// the type owner's models: every kind the release line holds, of which it
// found none. A line without domains records nothing about them.
func emptyOracleTypeCoverage(caps capability.Capabilities) (schemaext.Coverage, error) {
	var known schemaext.Coverage
	for _, kind := range []struct {
		model schemaext.Kind
		key   capability.Capability
	}{{oratype.DomainKind, capability.DomainTypes}, {oratype.ObjectTypeKind, capability.CompositeTypes}} {
		if !caps.Has(kind.key) {
			continue
		}
		claim, err := oratype.Coverage(kind.model, schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)
		if err != nil {
			return schemaext.Coverage{}, err
		}
		if known, err = known.Combine(claim); err != nil {
			return schemaext.Coverage{}, err
		}
	}
	return known, nil
}

func emptyPostgresFeatureCoverage() (schemaext.Coverage, error) {
	var coverage schemaext.Coverage
	// A PostgreSQL-family read records TimescaleDB knowledge whether or not
	// the extension is installed; an empty database holds neither model.
	// It describes the installed extensions too, and an empty database
	// has none.
	known, err := tsschema.CompleteCoverage(schemaext.Observed)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	extensions, err := pgextension.CompleteCoverage(schemaext.Observed)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	// It describes the user types too, and an empty database has none.
	types, err := pgtype.CompleteCoverage(schemaext.Observed)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	if coverage, err = known.Combine(extensions); err != nil {
		return schemaext.Coverage{}, err
	}
	if coverage, err = coverage.Combine(types); err != nil {
		return schemaext.Coverage{}, err
	}
	defaults, err := pgdefaultprivilege.CompleteCoverage(schemaext.Observed)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	if coverage, err = coverage.Combine(defaults); err != nil {
		return schemaext.Coverage{}, err
	}

	return coverage, nil
}
