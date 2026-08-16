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
	// allowDirty and baselineVersion are the two opt-outs of the not-clean
	// gate below.
	allowDirty      bool
	baselineVersion int64
	// cleanScope is the catalog as it stood before this run prepared anything.
	// See inspectThenPrepareApply for why it is read that early.
	cleanScope migratecleanSnapshot
}

// migratecleanSnapshot is a catalog read that has happened but has not been
// judged yet.
//
// The read and the judgement are separated in time on purpose. The read has to
// happen before the run creates its revision table, and the judgement has to
// happen where stokaro/ptah#1252 measured it: after the plan is prepared, so an
// unresolvable --baseline still reports the baseline diagnostic. Carrying the
// error rather than returning it keeps that split honest — a run that never
// reaches the gate, because it passed --allow-dirty or because the revision
// table already holds rows, must not start failing on a catalog probe whose
// answer it does not need.
type migratecleanSnapshot struct {
	scope migrateclean.Scope
	err   error
}

// inspectThenPrepareApply reads the not-clean gate's catalog and then prepares
// the plan, in that order.
//
// The order is the whole reason this function exists rather than two statements
// at the call site. atlasmigrate.PrepareApply creates the revision table in the
// connected schema, and at realm scope that table belongs to no schema the
// pinned binary exempts: reading the catalog afterwards made the gate refuse
// `found schema "public"` against a database whose `public` held nothing but
// the table this run had just created, where the binary applies at exit 0
// (stokaro/ptah#1257). Keeping both calls here means the two cannot be
// reordered without editing the sentence that says why they must not be.
func inspectThenPrepareApply(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	opts atlasmigrate.ApplyOptions,
) (migratecleanSnapshot, atlasmigrate.ApplyPlan, error) {
	scope, scopeErr := migrateclean.Inspect(ctx, conn)
	plan, err := atlasmigrate.PrepareApply(ctx, conn, opts)
	return migratecleanSnapshot{scope: scope, err: scopeErr}, plan, err
}

// runAtlasMigrateApplyRefusals runs every check that can stop a prepared apply
// before it executes, in the order their measurements require.
func runAtlasMigrateApplyRefusals(ctx context.Context, operands atlasMigrateApplyRefusalOperands) error {
	// The adoption gate comes first. The Flyway refusals below all
	// reason about revision rows somebody else wrote, and this one fires only
	// when there are none, so the order between them is not observable — but
	// stating it keeps the more fundamental precondition at the top.
	if err := checkConnectedDatabaseClean(ctx, operands); err != nil {
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
	if operands.cleanScope.err != nil {
		return operands.cleanScope.err
	}
	return operands.cleanScope.scope.ForRevisions(operands.plan.RevisionTable()).Refusal()
}
