package atlas

import (
	"context"
	"io/fs"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/migrateclean"
	"go.5x5.cz/ptah/migration/migrator"
)

// atlasMigrateApplyRefusalOperands carries everything the refusals that run
// between planning and execution reason about.
//
// It is a struct rather than a parameter list because the refusals share most
// of their inputs and the order of nine positional arguments is not something a
// reader should have to keep straight.
type atlasMigrateApplyRefusalOperands struct {
	conn      *dbschema.DatabaseConnection
	plan      atlasmigrate.ApplyPlan
	captured  fs.FS
	dirFormat atlasmigrateimport.Format
	linearity flywayLinearity
	execOrder migrator.ExecOrder
	// revisionsSchema is the run's --revisions-schema.
	revisionsSchema string
	// allowDirty and baselineVersion are the two opt-outs of the not-clean
	// gate below.
	allowDirty      bool
	baselineVersion int64
}

// runAtlasMigrateApplyRefusals runs every check that can stop a prepared apply
// before it executes, in the order their measurements require.
func runAtlasMigrateApplyRefusals(ctx context.Context, operands atlasMigrateApplyRefusalOperands) error {
	// The adoption gate comes first. The three Flyway refusals below all
	// reason about revision rows somebody else wrote, and this one fires only
	// when there are none, so the order between them is not observable — but
	// stating it keeps the more fundamental precondition at the top.
	if err := checkConnectedDatabaseClean(ctx, operands); err != nil {
		return err
	}

	// #982 changed the Atlas version a Flyway file converts to, which is the
	// key `atlas_schema_revisions` stores. A database migrated by an older Ptah
	// build therefore reads as entirely pending here. Refuse before executing
	// anything rather than re-running migrations that already ran.
	if err := checkLegacyFlywayRevisions(
		operands.captured, operands.dirFormat, operands.plan, operands.revisionsSchema,
	); err != nil {
		return err
	}

	// The same question one implementation over: Atlas CE records a converted
	// Flyway migration under its SOURCE version token, so a revision table it
	// wrote also matches no file here and reads as entirely pending
	// (stokaro/ptah#1100). The Ptah-encoding check above runs first because it
	// is the more specific claim about who wrote the row, and its repair is a
	// different one.
	if err := checkForeignFlywayRevisions(operands.captured, operands.dirFormat, operands.plan); err != nil {
		return err
	}

	// The exemption above only stops the linear guard from reading the
	// baseline's band position as "authored earlier". Whether a baseline may
	// run against a database that already has history is a separate question,
	// and one Atlas CE answers three incompatible ways (stokaro/ptah#1003).
	return checkFlywayBaselineHistory(operands.linearity.baseline, operands.execOrder, operands.plan)
}

// checkConnectedDatabaseClean refuses to adopt a database that already holds
// tables no migration in the directory created (stokaro/ptah#1231 case 1).
//
// It is an adoption gate, not a standing drift check. Measured against the
// pinned community binary v1.3.0 on PostgreSQL 17, MySQL 9.7 and SQLite, the
// refusal fires only while the revision table is empty: a managed database that
// later grows an unmanaged table applies its next migration at exit 0, so a
// single recorded revision turns the gate off for good. That is the whole point
// of it — it protects the first run against a database somebody else's tooling
// owns, and after that the revision table is the record of ownership.
//
// The exemption operand is the revision ROW COUNT rather than the plan's
// applied-migration list, which is intersected with the directory: rotating
// migration files out of the directory would otherwise make a long-managed
// database read as unadopted and refuse.
//
// Placement is measured too. This runs after atlasmigrate.PrepareApply so that
// an unresolvable --baseline still reports the baseline diagnostic (the binary
// does the same), and before the plan executes so that --dry-run refuses as
// well — the binary refuses a dry run against an unclean database, which it
// could not do if the check lived in the execution path.
func checkConnectedDatabaseClean(ctx context.Context, operands atlasMigrateApplyRefusalOperands) error {
	// The two documented opt-ins. --allow-dirty says "the schema is not empty,
	// proceed"; --baseline says "treat history as starting here". Measured,
	// either one makes the binary apply against the same unclean database at
	// exit 0, so honoring them is the half of the parity rule that forbids
	// being stricter, not a convenience.
	//
	// --baseline is checked explicitly rather than left to the revision count
	// it usually produces, because a dry-run baseline records no row at all.
	if operands.allowDirty || operands.baselineVersion > 0 {
		return nil
	}
	if !migrateclean.Governs(operands.conn.Info().Dialect) {
		return nil
	}
	revisions, err := operands.plan.RevisionCount(ctx)
	if err != nil {
		return err
	}
	if revisions > 0 {
		return nil
	}
	revisionsSchema, revisionTable := operands.plan.RevisionTable()
	scope, err := migrateclean.Inspect(ctx, operands.conn, revisionTable, revisionsSchema)
	if err != nil {
		return err
	}
	return scope.Refusal()
}
