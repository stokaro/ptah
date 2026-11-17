package migrator

import (
	"context"
	"errors"
	"fmt"

	"ptah.run/dbschema"
	"ptah.run/internal/sqliterebuild"
	"ptah.run/migration/migrationfile"
)

// repeatableState is the directory's repeatable migrations and what the
// database records for them, read once and used for status, selection and
// execution alike, so the three cannot disagree.
type repeatableState struct {
	repeatables []*repeatableMigration
	rows        map[string]repeatableRow
}

// readRepeatableState reads the repeatable migrations of the provider and
// their record. It returns nil when the provider holds none, so a directory
// without repeatable migrations sends no query about them.
func (m *Migrator) readRepeatableState(ctx context.Context) (*repeatableState, error) {
	repeatables := m.providerRepeatables()
	if len(repeatables) == 0 {
		return nil, nil
	}
	if err := m.refuseRepeatablesHere(); err != nil {
		return nil, err
	}
	rows, err := m.readRepeatableRows(ctx)
	if err != nil {
		return nil, err
	}
	return &repeatableState{repeatables: repeatables, rows: rows}, nil
}

// refuseRepeatablesHere refuses a migrator that cannot keep the record a
// repeatable migration needs.
//
// The Atlas-compatible revision format is refused for the reason the operation
// log is off under it: that contract defines the revision table and nothing
// beside it. An engine is refused when its descriptor states no record table,
// which is how an engine on which the path is not measured leaves it out.
func (m *Migrator) refuseRepeatablesHere() error {
	if m.revisionTableFormat.isAtlas() {
		return &RepeatableMigrationsUnsupportedError{
			Reason: "the Atlas-compatible revision format defines no table to record them in; " +
				"use the native revision format",
		}
	}
	dialect := m.connectionDialect()
	if m.revisionTable().CreateRepeatables == "" {
		return &RepeatableMigrationsUnsupportedError{
			Dialect: dialect,
			Reason:  fmt.Sprintf("Ptah runs repeatable migrations on PostgreSQL and SQLite, and this database is %s", dialect),
		}
	}
	return nil
}

// records is the status of every repeatable migration, in execution order.
func (s *repeatableState) records() []RepeatableRecord {
	if s == nil {
		return nil
	}
	records := make([]RepeatableRecord, 0, len(s.repeatables))
	for _, repeatable := range s.repeatables {
		records = append(records, s.record(repeatable))
	}
	return records
}

// record is the status of one repeatable migration.
func (s *repeatableState) record(repeatable *repeatableMigration) RepeatableRecord {
	record := RepeatableRecord{
		Identity: repeatable.identity,
		Path:     repeatable.path,
		Checksum: repeatable.checksum,
		State:    RepeatableStatePending,
	}
	row, ok := s.rows[repeatable.identity]
	if !ok {
		return record
	}
	record.AppliedChecksum = row.checksum
	record.AttemptChecksum = row.attemptChecksum
	record.AttemptOrder = row.attemptOrder
	record.NoTransaction = !row.transactional
	record.Error = row.errorText
	if !row.attemptedAt.IsZero() {
		attemptedAt := row.attemptedAt.UTC()
		record.AttemptedAt = &attemptedAt
	}
	if !row.appliedAt.IsZero() {
		appliedAt := row.appliedAt.UTC()
		record.AppliedAt = &appliedAt
	}
	switch {
	case row.state != RepeatableStateApplied:
		record.State = row.state
	case row.checksum == repeatable.checksum:
		record.State = RepeatableStateApplied
	default:
		record.State = RepeatableStateChanged
	}
	return record
}

// dirty returns the records whose last attempt has no successful outcome.
func (s *repeatableState) dirty() []RepeatableRecord {
	var dirty []RepeatableRecord
	for _, record := range s.records() {
		if record.dirty() {
			dirty = append(dirty, record)
		}
	}
	return dirty
}

// work is what an up run executes, in order: every new and changed repeatable
// migration, and, when the run asked to retry dirty work, every dirty one.
func (s *repeatableState) work(retryDirty bool) []RepeatableWork {
	var work []RepeatableWork
	for _, record := range s.records() {
		reason := ""
		switch {
		case record.State == RepeatableStatePending:
			reason = RepeatableReasonNew
		case record.State == RepeatableStateChanged:
			reason = RepeatableReasonChanged
		case record.dirty() && retryDirty:
			reason = RepeatableReasonRetry
		}
		if reason == "" {
			continue
		}
		work = append(work, RepeatableWork{
			Identity:        record.Identity,
			Path:            record.Path,
			Checksum:        record.Checksum,
			AppliedChecksum: record.AppliedChecksum,
			Reason:          reason,
		})
	}
	return work
}

// hasWork reports whether a status should count the repeatable migrations as
// pending changes: work an up run would do, or a dirty record that stops it.
func (s *repeatableState) hasWork() bool {
	return s != nil && (len(s.work(false)) > 0 || len(s.dirty()) > 0)
}

// repeatable returns the loaded repeatable migration with identity.
func (s *repeatableState) repeatable(identity string) *repeatableMigration {
	for _, repeatable := range s.repeatables {
		if repeatable.identity == identity {
			return repeatable
		}
	}
	return nil
}

// planRepeatables reads what the database records for the directory's
// repeatable migrations and selects the work an up run executes after the
// versioned migrations it selected. Before anything runs it refuses what the
// run cannot honor: a dirty record the run did not ask to retry, a bound that
// stops below the latest version, and a file the versioned path would refuse.
// A directory without repeatable migrations selects nothing and sends no query.
func (m *Migrator) planRepeatables(
	ctx context.Context,
	opts MigrateUpOptions,
	migrations []*Migration,
	appliedMigrations []int64,
	appliedIdentities migrationIdentitySet,
	selected []*Migration,
) (*repeatableState, []RepeatableWork, error) {
	state, err := m.readRepeatableState(ctx)
	if err != nil || state == nil {
		return nil, nil, err
	}
	if err := state.refuseDirtyRepeatables(opts); err != nil {
		return nil, nil, err
	}
	work := state.work(opts.AllowDirty)
	if err := m.refuseBoundedRepeatables(opts, migrations, appliedMigrations, appliedIdentities, selected, work); err != nil {
		return nil, nil, err
	}
	if err := m.refuseRepeatablesWhatFilesRefuse(state, work); err != nil {
		return nil, nil, err
	}
	return state, work, nil
}

// refuseDirtyRepeatables refuses a run over a dirty repeatable migration
// unless it asked to retry dirty work.
func (s *repeatableState) refuseDirtyRepeatables(opts MigrateUpOptions) error {
	if opts.AllowDirty {
		return nil
	}
	if dirty := s.dirty(); len(dirty) > 0 {
		return &DirtyRepeatableError{Records: dirty}
	}
	return nil
}

// refuseBoundedRepeatables refuses a bounded run that would stop below the
// latest version while repeatable work is pending. The comparison is with the
// selection the same history makes without a bound, so it answers for what
// this run would leave pending rather than for the bound's arithmetic.
func (m *Migrator) refuseBoundedRepeatables(
	opts MigrateUpOptions,
	migrations []*Migration,
	appliedMigrations []int64,
	appliedIdentities migrationIdentitySet,
	selected []*Migration,
	work []RepeatableWork,
) error {
	if len(work) == 0 || (opts.TargetVersion == 0 && opts.Amount == 0) {
		return nil
	}
	unbounded, err := m.migrationsToApply(migrations, appliedMigrations, appliedIdentities, 0)
	if err != nil {
		return err
	}
	if len(unbounded.apply) == 0 ||
		(len(selected) > 0 && maxMigrationVersion(selected) >= maxMigrationVersion(unbounded.apply)) {
		return nil
	}
	return &RepeatableBoundError{TargetVersion: opts.TargetVersion, Amount: opts.Amount, Work: work}
}

// refuseRepeatablesWhatFilesRefuse asks every selected repeatable migration,
// before anything runs, what the versioned path asks of a file before it
// marks anything: a check directive, which a repeatable migration does not
// take, a directive or statement the installed hooks refuse, and a statement
// that cannot run inside the transaction the file would run in.
func (m *Migrator) refuseRepeatablesWhatFilesRefuse(state *repeatableState, work []RepeatableWork) error {
	for _, item := range work {
		repeatable := state.repeatable(item.Identity)
		if repeatable.migration.ownTransactions {
			return fmt.Errorf(
				"repeatable migration %s runs in a transaction Ptah opens, and this directory is run by a runner that "+
					"opens its own", repeatable.path)
		}
		groups, err := m.migrationCheckGroups(repeatable.migration, MigrationDirectionUp)
		if err != nil {
			return fmt.Errorf("repeatable migration %s: %w", repeatable.path, err)
		}
		if len(groups) > 0 {
			return fmt.Errorf(
				"repeatable migration %s declares a -- +ptah check directive, which a repeatable migration does not take",
				repeatable.path)
		}
		if refuse := repeatable.migration.upRefuseByHooks; refuse != nil {
			if err := refuse(m.conn); err != nil {
				return err
			}
		}
		transactional, err := m.repeatableRunsInTransaction(repeatable)
		if err != nil {
			return err
		}
		if refuse := repeatable.migration.upRefuseInsideTransaction; transactional && refuse != nil {
			if err := refuse(m.conn); err != nil {
				return err
			}
		}
	}
	return nil
}

// repeatableRunsInTransaction reports whether a repeatable migration runs in a
// transaction of its own: its file's directive decides, and without one the
// migrator's mode does. Under --tx-mode all each repeatable migration still
// runs in a transaction of its own, after the one around the versioned batch
// has committed.
func (m *Migrator) repeatableRunsInTransaction(repeatable *repeatableMigration) (bool, error) {
	parsed := repeatable.migration.parsedUpTxModeForDialect(m.connectionDialect())
	if parsed.Err != nil {
		return false, fmt.Errorf("repeatable migration %s: %w", repeatable.path, parsed.Err)
	}
	switch parsed.Mode {
	case migrationfile.FileTxModeNone:
		return false, nil
	case migrationfile.FileTxModeFile:
		return true, nil
	default:
		return m.txMode != MigrationTxModeNone, nil
	}
}

// applyRepeatables executes the selected repeatable work in order, stopping
// at the first failure. A dry run executes nothing and records nothing.
func (m *Migrator) applyRepeatables(ctx context.Context, state *repeatableState, work []RepeatableWork) error {
	if len(work) == 0 {
		return nil
	}
	if m.conn.Writer().IsDryRun() {
		for _, item := range work {
			m.logger.Info("[DRY RUN] Would run repeatable migration",
				"identity", item.Identity, "checksum", item.Checksum, "reason", item.Reason)
		}
		return nil
	}
	if err := m.ensureRepeatableTable(ctx); err != nil {
		return err
	}
	return m.withRunSession(ctx, func(scoped *Migrator) error {
		for index, item := range work {
			_, recorded := state.rows[item.Identity]
			attempt := repeatableAttempt{repeatable: state.repeatable(item.Identity), order: index + 1, recorded: recorded}
			if err := scoped.applyRepeatable(ctx, attempt); err != nil {
				return err
			}
		}
		return nil
	})
}

// repeatableAttempt is one execution of a repeatable migration within a run.
type repeatableAttempt struct {
	repeatable *repeatableMigration
	// order is the attempt's 1-based position in the run's repeatable work.
	order int
	// transactional reports that the body runs in a transaction of its own.
	transactional bool
	// recorded reports that the record table already holds a row for the
	// identity, which the attempt then updates rather than inserts.
	recorded bool
}

// transactionalColumn is transactional as the record's INTEGER column holds it.
func (a repeatableAttempt) transactionalColumn() int {
	if a.transactional {
		return 1
	}
	return 0
}

// applyRepeatable runs one repeatable migration: it records the attempt, runs
// the body, and records the new digest only once the body has succeeded.
func (m *Migrator) applyRepeatable(ctx context.Context, attempt repeatableAttempt) (err error) {
	repeatable := attempt.repeatable
	observer := m.migrationObserver()
	ctx, span := observer.StartSpan(ctx, "ptah.migrate.repeatable",
		attr("db.system", m.connectionDialect()),
		attr("migration.repeatable", repeatable.identity),
		attr("migration.checksum", repeatable.checksum),
	)
	defer func() { span.End(err) }()

	attempt.transactional, err = m.repeatableRunsInTransaction(repeatable)
	if err != nil {
		return err
	}
	m.logger.Info("Applying repeatable migration", "identity", repeatable.identity, "checksum", repeatable.checksum)
	if err := m.markRepeatableAttempt(ctx, attempt); err != nil {
		return fmt.Errorf("failed to record the attempt of repeatable migration %s: %w", repeatable.identity, err)
	}
	if attempt.transactional {
		err = m.applyRepeatableInTransaction(ctx, repeatable)
	} else {
		err = m.applyRepeatableWithoutTransaction(ctx, repeatable)
	}
	if err != nil {
		return err
	}
	m.logger.Info("Applied repeatable migration", "identity", repeatable.identity, "checksum", repeatable.checksum)
	return nil
}

// applyRepeatableInTransaction runs the body and the record of its digest in
// one transaction. A body that fails before the commit took no effect, and the
// record says failed; a commit that answers with an error leaves the record
// running, because nothing here can say whether it took effect.
func (m *Migrator) applyRepeatableInTransaction(ctx context.Context, repeatable *repeatableMigration) error {
	migration := repeatable.migration
	body := m.bodyConnection()
	tx, err := sqliterebuild.BeginTransactionForSQL(ctx, body, migration.UpSQL)
	if err != nil {
		return m.repeatableFailed(ctx, repeatable, fmt.Errorf("failed to begin transaction: %w", err), false)
	}
	txConn := body.WithExecutor(tx)
	rollback := func(failure error) error {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			failure = errors.Join(failure, fmt.Errorf("rollback: %w", rollbackErr))
		}
		return m.repeatableFailed(ctx, repeatable, failure, false)
	}
	timeouts, err := m.effectiveUpTimeouts(migration)
	if err != nil {
		return rollback(err)
	}
	restoreTimeouts, err := m.applyTimeoutsWithRestore(ctx, txConn, timeouts, timeoutScopeTransaction)
	if err != nil {
		return rollback(err)
	}
	if err := migration.executeUp(withStatementBound(ctx, txConn, timeouts), txConn, migrationExecutionTransactional); err != nil {
		return rollback(m.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, err))
	}
	if err := m.restoreTimeouts(ctx, migration.Version, restoreTimeouts); err != nil {
		return rollback(err)
	}
	if err := m.completeRepeatableOn(ctx, txConn, repeatable); err != nil {
		return rollback(fmt.Errorf("failed to record the applied digest: %w", err))
	}
	if err := tx.Commit(); err != nil {
		return &RepeatableMigrationError{
			Identity:       repeatable.identity,
			Err:            errors.Join(fmt.Errorf("commit: %w", err), tx.Rollback()),
			OutcomeUnknown: true,
		}
	}
	return nil
}

// applyRepeatableWithoutTransaction runs the body on the run's session with no
// transaction around it, and records the digest once every statement has run.
func (m *Migrator) applyRepeatableWithoutTransaction(ctx context.Context, repeatable *repeatableMigration) error {
	migration := repeatable.migration
	err := m.withBodySession(ctx, func(conn *dbschema.DatabaseConnection) error {
		timeouts, err := m.effectiveUpTimeouts(migration)
		if err != nil {
			return err
		}
		restoreTimeouts, err := m.applyTimeoutsWithRestore(ctx, conn, timeouts, timeoutScopeSession)
		if err != nil {
			return err
		}
		if err := migration.executeUp(withStatementBound(ctx, conn, timeouts), conn, migrationExecutionNoTransaction); err != nil {
			return m.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, err)
		}
		return m.restoreTimeouts(ctx, migration.Version, restoreTimeouts)
	})
	if err != nil {
		return m.repeatableFailed(ctx, repeatable, err, true)
	}
	if err := m.completeRepeatableOn(ctx, m.conn, repeatable); err != nil {
		return &RepeatableMigrationError{
			Identity:       repeatable.identity,
			Err:            fmt.Errorf("the body ran and the applied digest could not be recorded: %w", err),
			OutcomeUnknown: true,
			NoTransaction:  true,
		}
	}
	return nil
}

// repeatableFailed records a failed body and returns its error. When the
// failure cannot be recorded, the row keeps the running state its attempt
// wrote, which blocks the next run as a failure would.
func (m *Migrator) repeatableFailed(
	ctx context.Context,
	repeatable *repeatableMigration,
	failure error,
	noTransaction bool,
) error {
	result := &RepeatableMigrationError{Identity: repeatable.identity, Err: failure, NoTransaction: noTransaction}
	if err := m.failRepeatable(ctx, repeatable, failure); err != nil {
		result.Err = errors.Join(failure, fmt.Errorf("additionally failed to record the failure: %w", err))
	}
	return result
}

// repeatableRollbackRefusal refuses a rollback over a database that records
// any repeatable migration of this directory, before anything changes.
func (m *Migrator) repeatableRollbackRefusal(ctx context.Context) error {
	state, err := m.readRepeatableState(ctx)
	if err != nil || state == nil {
		return err
	}
	var recorded []string
	for _, repeatable := range state.repeatables {
		if _, ok := state.rows[repeatable.identity]; ok {
			recorded = append(recorded, repeatable.identity)
		}
	}
	if len(recorded) == 0 {
		return nil
	}
	return &RepeatableRollbackError{Identities: recorded, Table: m.repeatableTable()}
}
