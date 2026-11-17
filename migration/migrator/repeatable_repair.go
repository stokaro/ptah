package migrator

import (
	"context"
	"errors"
	"fmt"

	"ptah.run/core/sqlutil"
	"ptah.run/migration/migrationfile"
)

// ErrRepeatableNotRecorded reports an identity the repeatable migration record
// holds no row for.
var ErrRepeatableNotRecorded = errors.New("the repeatable migration record holds no row for this identity")

// ErrRepeatableStillInDirectory reports a row whose identity a file of the
// directory still carries, which [Migrator.RemoveRepeatableRecord] deletes
// only when asked to with [RemoveRepeatableRecordOptions.Force].
var ErrRepeatableStillInDirectory = errors.New("a file of the directory still carries this repeatable migration")

// RemoveRepeatableRecordOptions selects the row [Migrator.RemoveRepeatableRecord]
// deletes.
type RemoveRepeatableRecordOptions struct {
	// Identity is the repeatable migration whose row is deleted: the part of
	// its file name between R__ and .sql.
	Identity string
	// Force deletes the row even when a file of the directory still carries
	// the identity, and also when the migrator's provider does not load
	// repeatable migrations, which leaves it unable to tell whether a file
	// does. The next up run then executes such a file as new.
	Force bool
}

// RemoveRepeatableRecord deletes one row of the repeatable migration record and
// runs no migration SQL. It is the way out of a record no file carries any
// more: such a row blocks rollbacks, and a dirty one blocks up runs, because
// nothing can run it again. What the migration created stays; drop it with a
// versioned migration first when it should go.
//
// Without opts.Force, a row is deleted only when the migrator's provider loads
// repeatable migrations (see [WithRepeatableMigrations]) and none of them
// carries the identity; otherwise the error wraps
// [ErrRepeatableStillInDirectory]. An identity with no row returns an error
// wrapping [ErrRepeatableNotRecorded]. A migrator that cannot run repeatable
// migrations returns [*RepeatableMigrationsUnsupportedError] before it
// connects to the record. The read and the delete hold the migration lock.
//
// An empty identity, or one no repeatable file name could carry (see
// [migrationfile.ParseRepeatableFileName]), returns an error that wraps no
// sentinel, before anything is read.
//
// The migration lock is awaited under ctx and the migrator's lock timeout (see
// [Migrator.WithMigrationLockTimeout]). A ctx canceled during the wait
// abandons it and returns an error wrapping ctx's error; a wait that outlasts
// the timeout returns an error wrapping [*MigrationLockTimeoutError]. Either
// way nothing is deleted. A lock lost or not released after the delete is an
// error too, although the row is gone by then.
//
// On success it returns what the record said of the row before it was deleted,
// with [RepeatableRecord.Orphaned] set when no file carries the identity. The
// returned record is nil whenever the error is not nil.
func (m *Migrator) RemoveRepeatableRecord(
	ctx context.Context,
	opts RemoveRepeatableRecordOptions,
) (*RepeatableRecord, error) {
	if opts.Identity == "" {
		return nil, errors.New("a repeatable migration identity is required")
	}
	if _, err := migrationfile.ParseRepeatableFileName(migrationfile.RepeatableFileName(opts.Identity)); err != nil {
		return nil, fmt.Errorf("invalid repeatable migration identity %q: %w", opts.Identity, err)
	}
	if err := m.refuseRepeatablesHere(); err != nil {
		return nil, err
	}
	var removed *RepeatableRecord
	err := m.withMigrationLock(ctx, "remove a repeatable migration record", func(ctx context.Context) error {
		record, err := m.removeRepeatableRecordLocked(ctx, opts)
		removed = record
		return err
	})
	if err != nil {
		return nil, err
	}
	return removed, nil
}

func (m *Migrator) removeRepeatableRecordLocked(
	ctx context.Context,
	opts RemoveRepeatableRecordOptions,
) (*RepeatableRecord, error) {
	rows, err := m.readRepeatableRows(ctx)
	if err != nil {
		return nil, err
	}
	row, ok := rows[opts.Identity]
	if !ok {
		return nil, fmt.Errorf("%w: %s has no row for %q", ErrRepeatableNotRecorded, m.repeatableTable(), opts.Identity)
	}
	source, loads := m.migrationProvider.(repeatableSource)
	loads = loads && source.loadsRepeatableMigrations()
	state := &repeatableState{rows: rows}
	if loads {
		state.repeatables = source.repeatableMigrations()
	}
	repeatable := state.repeatable(opts.Identity)
	switch {
	case opts.Force:
	case repeatable != nil:
		return nil, fmt.Errorf("%w: %s carries %s, and the next up run would execute it as new without its row; "+
			"rerun with --force to remove the row anyway", ErrRepeatableStillInDirectory, repeatable.path, opts.Identity)
	case !loads:
		return nil, fmt.Errorf("%w: this migrator does not load repeatable migrations, so it cannot tell "+
			"whether a file carries %s; rerun with --force to remove the row anyway", ErrRepeatableStillInDirectory, opts.Identity)
	}
	record := orphanedRecord(row)
	if repeatable != nil {
		record = state.record(repeatable)
	}
	query := "DELETE FROM " + m.repeatableTable() + " WHERE identity = ?"
	if err := executeSQLOn(ctx, m.conn, sqlutil.Rebind(m.connectionDialect(), query), opts.Identity); err != nil {
		return nil, fmt.Errorf("failed to delete the row of repeatable migration %s: %w", opts.Identity, err)
	}
	m.logger.Info("Removed a repeatable migration record", "identity", opts.Identity, "table", m.repeatableTable(),
		"state", record.State)
	return &record, nil
}
