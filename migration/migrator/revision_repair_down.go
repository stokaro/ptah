package migrator

import (
	"context"
	"fmt"

	"go.5x5.cz/ptah/core/sqlutil"
)

// repairRolledBackMigration repairs a revision a rollback left dirty.
//
//   - --resume-from runs the remaining down statements and then deletes the
//     revision. A rollback that reaches its last statement has un-applied the
//     migration, and Ptah records an un-applied migration by removing its row,
//     the same way a rollback that never failed does.
//   - Without it, a row whose down body already completed is finalized by
//     deleting the revision. This covers a previous repair that ran every
//     statement but stopped before deletion on a database-state safety check.
//   - Otherwise the revision is recorded applied, which is what Ptah has always
//     done and is right for the rollback that changed nothing: a transactional
//     down that rolled its body back, or a non-transactional one whose very
//     first statement failed, leaves the migration exactly as applied as it
//     was.
//   - Once the rollback has committed a statement the two outcomes are
//     opposites and the row cannot say which one the operator wants, so
//     neither is guessed. Recording applied there is the defect: it signs off
//     a migration as applied over a schema whose objects the rollback already
//     dropped (stokaro/ptah#995). --force still records it applied, for the
//     operator who restored the schema by hand -- that is a claim about the
//     metadata, which --force is documented to rewrite, and the PostgreSQL
//     unusable-index probe still stands between it and an applied row.
func (m *Migrator) repairRolledBackMigration(
	ctx context.Context,
	migration *Migration,
	revision *MigrationRevision,
	opts RepairMigrationOptions,
) error {
	if opts.ResumeFrom <= 0 {
		return m.repairRolledBackMigrationWithoutResume(ctx, migration, revision, opts)
	}
	return m.withNoTransactionSession(ctx, func(scoped *Migrator) error {
		if err := refuseUnknownStatementOutcomeResume(migration, revision); err != nil {
			return err
		}
		if err := scoped.verifyCommittedPrefix(*revision, migration, MigrationDirectionDown, "resume the rollback"); err != nil {
			return err
		}
		if err := scoped.resumeRollback(ctx, migration, opts.ResumeFrom); err != nil {
			return err
		}
		if err := scoped.refuseRollbackCompletionOverUnsafeIndex(ctx, migration); err != nil {
			return err
		}
		return scoped.deleteRolledBackRevision(ctx, migration)
	})
}

func (m *Migrator) repairRolledBackMigrationWithoutResume(
	ctx context.Context,
	migration *Migration,
	revision *MigrationRevision,
	opts RepairMigrationOptions,
) error {
	if revision.Applied == revision.Total {
		return m.finalizeCompletedRollback(ctx, migration, revision)
	}
	if !opts.Force && rollbackChangedSchema(revision) {
		return rollbackRepairNeedsDirectionError(migration, revision)
	}
	if err := m.refuseRepairOverUnsafeIndex(ctx, migration); err != nil {
		return err
	}
	return m.forceAppliedMigration(ctx, migration)
}

func (m *Migrator) finalizeCompletedRollback(
	ctx context.Context,
	migration *Migration,
	revision *MigrationRevision,
) error {
	if err := m.verifyCommittedPrefix(*revision, migration, MigrationDirectionDown, "finalize the rollback"); err != nil {
		return err
	}
	if !m.needsPostgresIndexPostcheck(migration, MigrationDirectionDown) {
		return m.deleteRolledBackRevision(ctx, migration)
	}
	return m.withNoTransactionSession(ctx, func(scoped *Migrator) error {
		if err := scoped.restoreNoTransactionSessionPrefix(
			ctx,
			migration,
			MigrationDirectionDown,
			revision.Total+1,
		); err != nil {
			return err
		}
		if err := scoped.refuseRollbackCompletionOverUnsafeIndex(ctx, migration); err != nil {
			return err
		}
		return scoped.deleteRolledBackRevision(ctx, migration)
	})
}

// rollbackChangedSchema reports whether the rollback can have changed the
// schema before it stopped: either a down statement committed, or one was in
// flight when the process died and whether it committed is unknown.
func rollbackChangedSchema(revision *MigrationRevision) bool {
	return revision.Applied > 0 || revision.Error == unknownStatementOutcomeError
}

// rollbackRepairNeedsDirectionError renders the refusal to guess how a
// half-finished rollback should end. It reports how far the rollback got and
// names every lever that ends it, with the numbers the operator needs to choose
// between them. --resume-from is offered only when the row can say which
// statement to resume from.
func rollbackRepairNeedsDirectionError(migration *Migration, revision *MigrationRevision) error {
	const endings = "--force to record it applied if the schema it reverted was restored, " +
		"or run \"ptah migrations set --version <previous>\" if the rollback was finished by hand"
	if revision.Error == unknownStatementOutcomeError {
		return fmt.Errorf(
			"migration %d stopped while rolling back and the outcome of %q is unknown after an interruption; "+
				"inspect the database, then rerun with "+endings,
			migration.Version,
			revision.ErrorStatement,
		)
	}
	return fmt.Errorf(
		"migration %d stopped while rolling back: %d of %d down statements committed; "+
			"rerun with --resume-from %d to run the remaining down statements and remove the revision, with "+
			endings,
		migration.Version,
		revision.Applied,
		revision.Total,
		revision.Applied+1,
	)
}

// resumeRollback runs the down statements from resumeFrom to the end, outside
// any transaction, recording progress durably around each one exactly the way
// the original non-transactional rollback did. A statement that fails leaves
// the revision pointing at the new failure, so the operator can fix it and
// resume again from a later statement.
func (m *Migrator) resumeRollback(ctx context.Context, migration *Migration, resumeFrom int) error {
	return m.resumeMigrationDirectionOnSession(ctx, migration, resumeFrom, MigrationDirectionDown)
}

// deleteRolledBackRevision removes the revision of a migration whose rollback
// has now run to its last statement, with the same SQL and the same
// cancellation-proof write context a rollback that never failed uses.
func (m *Migrator) deleteRolledBackRevision(ctx context.Context, migration *Migration) error {
	recordCtx, cancelRecord := durableRevisionWriteContext(ctx)
	defer cancelRecord()
	deleteSQL := sqlutil.Rebind(m.conn.Info().Dialect, m.deleteMigrationSQL())
	if err := executeSQLOutsideTransaction(recordCtx, m.conn, deleteSQL, m.migrationRevisionVersionArg(migration)); err != nil {
		return fmt.Errorf("failed to record migration reversion %d: %w", migration.Version, err)
	}
	m.logger.Info(
		"Completed rollback of migration",
		"version", migration.Version,
		"description", migration.Description,
	)
	return nil
}

// refuseUpResumeOverRecordedRollback refuses an up-direction --resume-from over
// a row a rollback almost certainly wrote.
//
// Rows written before Ptah recorded the direction read as up rows even when a
// rollback wrote them, so --resume-from would replay the up body over a
// partially reverted schema -- the defect directional recording closes for rows
// written from here on. The statement totals still discriminate whenever the
// two bodies differ in length: a dirty row whose total matches the down body
// and not the up body cannot have come from this up body. Refusing those is
// fail-closed; it never runs SQL on a guess, and it cannot fire on a row whose
// total matches the up body it would replay.
//
// --force overrides it, because which direction a row records is metadata, and
// --force is the documented way to overrule the metadata. Rows whose two bodies
// happen to have the same statement count stay ambiguous and are not caught.
func (m *Migrator) refuseUpResumeOverRecordedRollback(
	migration *Migration,
	revision *MigrationRevision,
	opts RepairMigrationOptions,
) error {
	if opts.Force || revision == nil || !revision.Dirty || revision.Total <= 0 {
		return nil
	}
	upCount := m.migrationStatementCount(migration.UpSQL)
	downCount := m.migrationStatementCount(migration.DownSQL)
	if revision.Total == upCount || revision.Total != downCount {
		return nil
	}
	return fmt.Errorf(
		"migration %d records %d statements, which matches its down body (%d) and not its up body (%d): "+
			"the row predates directional rollback recording, and --resume-from would replay the up body over a "+
			"schema a rollback already changed; finish the rollback manually, "+
			"or rerun with --force to replay the up body anyway",
		migration.Version,
		revision.Total,
		downCount,
		upCount,
	)
}
