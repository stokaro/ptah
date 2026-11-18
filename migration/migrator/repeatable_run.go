package migrator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"ptah.run/core/platform/capability"
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
// every row of their record, including rows no file of the directory carries
// any more. It returns nil when the provider does not load repeatable
// migrations, and when it holds none and the target keeps no record of any,
// which is every engine that cannot run them.
func (m *Migrator) readRepeatableState(ctx context.Context) (*repeatableState, error) {
	source, ok := m.migrationProvider.(repeatableSource)
	if !ok || !source.loadsRepeatableMigrations() {
		return nil, nil
	}
	repeatables := source.repeatableMigrations()
	if refusal := m.refuseRepeatablesHere(); refusal != nil {
		if len(repeatables) > 0 {
			return nil, refusal
		}
		return nil, nil
	}
	rows, err := m.readRepeatableRows(ctx)
	if err != nil {
		return nil, err
	}
	if len(repeatables) == 0 && len(rows) == 0 {
		return nil, nil
	}
	return &repeatableState{repeatables: repeatables, rows: rows}, nil
}

// refuseRepeatablesHere refuses a migrator that cannot keep the record a
// repeatable migration needs. Where it refuses, no record can exist either, so
// a reader of the record takes the refusal as "nothing recorded".
//
// An engine is refused when its descriptor states no record table, which is
// how an engine on which the path is not measured leaves it out. A connection
// through a provider session is refused because the record is a table read and
// written with SQL, which such a session does not expose; a history store that
// fails to open is that kind of connection too. The revision format is not a
// reason: the record is a table of its own beside the revision table, named
// after it, so under the Atlas-compatible format it sits beside
// atlas_schema_revisions and the revision table keeps the shape that contract
// defines.
func (m *Migrator) refuseRepeatablesHere() error {
	if store, err := m.conn.HistoryStore(); err != nil || store != nil {
		return &RepeatableMigrationsUnsupportedError{
			Dialect: m.connectionDialect(),
			Reason: "this connection reaches the database through a provider rather than a database/sql driver, " +
				"and the record of repeatable migrations is a table Ptah reads and writes with SQL",
		}
	}
	if m.revisionTable().CreateRepeatables == "" {
		return &RepeatableMigrationsUnsupportedError{
			Dialect: m.connectionDialect(),
			Reason:  "Ptah keeps no record of repeatable migrations on this engine",
		}
	}
	return nil
}

// records is the status of every repeatable migration, in execution order,
// followed by the orphaned records in identity order.
func (s *repeatableState) records() []RepeatableRecord {
	if s == nil {
		return nil
	}
	records := make([]RepeatableRecord, 0, len(s.rows)+len(s.repeatables))
	for _, repeatable := range s.repeatables {
		records = append(records, s.record(repeatable))
	}
	for _, identity := range s.orphans() {
		records = append(records, orphanedRecord(s.rows[identity]))
	}
	return records
}

// orphans are the identities the record holds and no file of the directory
// carries, in identity order.
func (s *repeatableState) orphans() []string {
	var orphans []string
	for identity := range s.rows {
		if s.repeatable(identity) == nil {
			orphans = append(orphans, identity)
		}
	}
	slices.Sort(orphans)
	return orphans
}

// orphanedRecord is the status of a row no file carries. A finished attempt
// reads as orphaned; an unfinished one keeps its state, because it is the one
// that stops a run.
func orphanedRecord(row repeatableRow) RepeatableRecord {
	state := row.state
	if state == RepeatableStateApplied {
		state = RepeatableStateOrphaned
	}
	record := RepeatableRecord{Identity: row.identity, State: state, Orphaned: true}
	record.fill(row)
	return record
}

// fill copies what a row says of the last attempt into the record.
func (r *RepeatableRecord) fill(row repeatableRow) {
	r.AppliedChecksum = row.checksum
	r.AttemptChecksum = row.attemptChecksum
	r.AttemptOrder = row.attemptOrder
	r.NoTransaction = !row.transactional
	r.Error = row.errorText
	if !row.attemptedAt.IsZero() {
		attemptedAt := row.attemptedAt.UTC()
		r.AttemptedAt = &attemptedAt
	}
	if !row.appliedAt.IsZero() {
		appliedAt := row.appliedAt.UTC()
		r.AppliedAt = &appliedAt
	}
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
	record.fill(row)
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
// migration, and, when the run asked to retry dirty work, every dirty one that
// still has a file.
func (s *repeatableState) work(retryDirty bool) []RepeatableWork {
	var work []RepeatableWork
	for _, record := range s.records() {
		reason := ""
		switch {
		case record.Orphaned:
			// No file carries it, so there is nothing to run.
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
	if err := state.refuseDirtyRepeatables(opts, m.repeatableTable()); err != nil {
		return nil, nil, err
	}
	work := state.work(opts.AllowDirty)
	if err := m.refuseBoundedRepeatables(opts, migrations, appliedMigrations, appliedIdentities, selected, work); err != nil {
		return nil, nil, err
	}
	if err := m.refuseRepeatablesWhatFilesRefuse(state, work); err != nil {
		return nil, nil, err
	}
	if err := m.refuseUnaddressableRepeatableRecord(work); err != nil {
		return nil, nil, err
	}
	for _, identity := range state.orphans() {
		m.logger.Warn("A repeatable migration record has no file in the directory, and the run leaves it alone",
			"identity", identity, "table", m.repeatableTable())
	}
	return state, work, nil
}

// refuseUnaddressableRepeatableRecord refuses, before anything runs, a run
// with repeatable work whose record would be created under a name the target
// truncates. The table does not exist yet, so the check is on the name alone,
// and a dry run makes it as an apply does.
func (m *Migrator) refuseUnaddressableRepeatableRecord(work []RepeatableWork) error {
	if len(work) == 0 {
		return nil
	}
	return m.refuseTruncatedIdentifier(
		m.repeatableTableName(),
		"the repeatable migration record for migrations table "+strconv.Quote(m.migrationsTableName()),
		"shorten the migrations table name",
	)
}

// refuseDirtyRepeatables refuses a run over a dirty repeatable migration
// unless it asked to retry dirty work, and over a dirty record no file
// carries in any case, since there is nothing to retry.
func (s *repeatableState) refuseDirtyRepeatables(opts MigrateUpOptions, table string) error {
	var blocking []RepeatableRecord
	for _, record := range s.dirty() {
		if record.Orphaned || !opts.AllowDirty {
			blocking = append(blocking, record)
		}
	}
	if len(blocking) > 0 {
		return &DirtyRepeatableError{Records: blocking, Table: table}
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
//
// On an engine that does not roll schema changes back as a unit
// ([capability.TransactionalDDL] off: the MySQL family and CockroachDB from
// 25 commit the open transaction before each one, and YugabyteDB keeps them
// through a rollback), a repeatable migration runs without a transaction. The
// record keeps the last attempt's outcome and no progress within it, so a
// transaction there would make it say that a failed body took no effect while
// the server kept part of it. A file whose directive asks for a transaction is
// refused there instead, before anything runs.
func (m *Migrator) repeatableRunsInTransaction(repeatable *repeatableMigration) (bool, error) {
	parsed := repeatable.migration.parsedUpTxModeForDialect(m.connectionDialect())
	if parsed.Err != nil {
		return false, fmt.Errorf("repeatable migration %s: %w", repeatable.path, parsed.Err)
	}
	atomic := m.conn.Info().Capabilities.Has(capability.TransactionalDDL)
	switch parsed.Mode {
	case migrationfile.FileTxModeNone:
		return false, nil
	case migrationfile.FileTxModeFile:
		if !atomic {
			return false, fmt.Errorf("repeatable migration %s asks for a transaction of its own, and %s does not roll "+
				"schema changes back as a unit, so a failed attempt could leave part of it behind while its record "+
				"said it took no effect; remove the txmode directive to run it without a transaction",
				repeatable.path, m.connectionDialect())
		}
		return true, nil
	default:
		return atomic && m.txMode != MigrationTxModeNone, nil
	}
}

// createRepeatableRecord creates the record of a run with repeatable work
// before the run's first versioned statement, so a target that refuses the
// table, for a missing privilege or a foreign table created in the meantime,
// refuses it before anything else changed. A dry run creates nothing.
func (m *Migrator) createRepeatableRecord(ctx context.Context, work []RepeatableWork) error {
	if len(work) == 0 || m.conn.Writer().IsDryRun() {
		return nil
	}
	return m.ensureRepeatableTable(ctx)
}

// applyRepeatables executes the selected repeatable work in order, stopping
// at the first failure, into the record [Migrator.createRepeatableRecord]
// created. A dry run executes nothing and records nothing.
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
	m.logRepeatableEvent(ctx, repeatable, MigrationLogStarted, nil)
	if err := m.markRepeatableAttempt(ctx, attempt); err != nil {
		err = fmt.Errorf("failed to record the attempt of repeatable migration %s: %w", repeatable.identity, err)
		m.logRepeatableEvent(ctx, repeatable, MigrationLogFailed, err)
		return err
	}
	if attempt.transactional {
		err = m.applyRepeatableInTransaction(ctx, repeatable)
	} else {
		err = m.applyRepeatableWithoutTransaction(ctx, repeatable)
	}
	if err != nil {
		m.logRepeatableFailure(ctx, repeatable, err)
		return err
	}
	m.logRepeatableEvent(ctx, repeatable, MigrationLogApplied, nil)
	m.logger.Info("Applied repeatable migration", "identity", repeatable.identity, "checksum", repeatable.checksum)
	return nil
}

// logRepeatableFailure records the outcome of a failed attempt. An attempt
// whose outcome nobody knows gets no outcome entry, so the log reports it as
// undetermined, as the record keeps it running.
func (m *Migrator) logRepeatableFailure(ctx context.Context, repeatable *repeatableMigration, err error) {
	if failure, ok := errors.AsType[*RepeatableMigrationError](err); ok && failure.OutcomeUnknown {
		return
	}
	m.logRepeatableEvent(ctx, repeatable, MigrationLogFailed, err)
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

// repeatableRollbackRefusal refuses a rollback over a database whose record
// holds any row, before anything changes. It reads the record whatever the
// directory and the provider hold: a deleted or renamed file leaves its
// objects behind, and a rollback past them would leave a row that says
// applied over a schema that no longer has them.
func (m *Migrator) repeatableRollbackRefusal(ctx context.Context) error {
	if m.refuseRepeatablesHere() != nil {
		return nil
	}
	rows, err := m.readRepeatableRows(ctx)
	if err != nil || len(rows) == 0 {
		return err
	}
	recorded := make([]string, 0, len(rows))
	for identity := range rows {
		recorded = append(recorded, identity)
	}
	slices.Sort(recorded)
	return &RepeatableRollbackError{Identities: recorded, Table: m.repeatableTable()}
}
