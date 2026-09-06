package schemacensus

import (
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
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
func MeasurePlan() []Observation {
	return measure(planOne)
}

// planOne is the shipping plan path for one cell: compare against nothing, plan,
// render.
func planOne(schema schemamodel.Database, cell capabilityprobe.Cell) string {
	finalized := deepCopyDatabase(schema)
	schemamodel.Finalize(&finalized)

	diff, err := schemadiff.CompareWithDatabaseInfo(
		&finalized,
		&catalog.Database{},
		catalog.ServerInfo{Dialect: cell.Dialect, Capabilities: cell.Preset()},
		nil,
	)
	if err != nil {
		return "refused: " + err.Error()
	}
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		diff,
		cell.Dialect,
		planner.Options{Capabilities: cell.Preset()},
	)
	if err != nil {
		return "refused: " + err.Error()
	}
	return strings.Join(statements, "\n")
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
				"and a plan creates no schema on those dialects at all: a schema there IS " +
				"a database, so `internal/planner/dialects/mysql.planSchemaPreconditions` " +
				"runs on SQL Server alone and creating one is an administrative act " +
				"outside what a schema migration owns. The field reaches every " +
				"CREATE SCHEMA a plan does emit (stokaro/ptah#2618)",
		},
		{
			Field: "schemamodel.Schema.Collate", RenderOnly: true,
			Reason: "the collation half of the same decision, unreachable on a plan for the same reason",
		},
		{
			Field: "schemamodel.ExtendedProperty.Comment", RenderOnly: true,
			Reason: "a leading `--` line the render writes above the statement; the statement itself is identical on both surfaces",
		},
		{
			Field: "schemamodel.Grant.Comment", RenderOnly: true,
			Reason: "the same leading `--` line, above GRANT",
		},
		{
			Field: "schemamodel.RLSEnabledTable.Comment", RenderOnly: true,
			Reason: "the same leading `--` line, above ALTER TABLE ... ENABLE ROW LEVEL SECURITY",
		},
		{
			Field: "schemamodel.Field.IdentityStart", RenderOnly: true,
			Reason: "both surfaces agree when the column declares no IdentityOptions, and both drop START WITH when it does; the corpus declares all four, so this is one dialect's handling of the pair rather than a lost fact",
		},
		{
			Field: "schemamodel.Field.IdentityIncrement", RenderOnly: true,
			Reason: "the INCREMENT BY half of the same pair",
		},
	}
}
