package migrator

import (
	"cmp"
	"context"
	"fmt"
	"slices"
)

// DerivedRollback is a rollback whose statements the caller derived itself,
// such as from the difference between the live schema and the schema of the
// target version, rather than from the down bodies of the migrations.
type DerivedRollback struct {
	// TargetVersion is the version the rollback takes the database back to.
	TargetVersion int64
	// Versions are the applied migrations above TargetVersion the statements
	// were derived for, in any order. The statements describe the database at
	// exactly these versions and at no other.
	Versions []int64
	// Apply runs the derived statements on the migrator's connection. Its
	// context ends when the migration lock is lost. A nil Apply runs nothing.
	Apply func(ctx context.Context) error
}

// StaleDerivedRollbackError reports that the migrations applied above the
// target of a [DerivedRollback] are not the ones it was derived for: another
// runner applied or rolled back a migration in between, so the statements
// describe a database that is no longer there. It is returned before any
// statement runs, so nothing was rolled back and no revision changed.
type StaleDerivedRollbackError struct {
	// TargetVersion is the version the rollback was to take the database
	// back to.
	TargetVersion int64
	// Derived are the versions the rollback was derived for, newest first.
	Derived []int64
	// Applied are the versions applied above TargetVersion when the migrator
	// read them again, newest first.
	Applied []int64
}

func (e *StaleDerivedRollbackError) Error() string {
	return fmt.Sprintf("the rollback to version %d was derived for the applied migrations %v, "+
		"and the migrations applied above version %d are now %v; nothing was rolled back",
		e.TargetVersion, e.Derived, e.TargetVersion, e.Applied)
}

// MigrateDownDerived runs a rollback the caller derived and moves the revision
// boundary to its target, holding the migration lock
// [Migrator.MigrateDownToWithPreflight] takes for a rollback through the down
// bodies. Runners that take the same lock wait for it, and it waits for them.
//
// Holding the lock, it refuses a dirty revision with a [*DirtyMigrationError]
// and reads the applied migrations again. When the ones above the target are
// not [DerivedRollback.Versions], it returns a [*StaleDerivedRollbackError].
// Otherwise it runs hook with the selected plan, opens a log attempt for each
// migration the rollback undoes as [Migrator.LogDerivedRollback] does, runs
// Apply, and moves the revision boundary as [Migrator.SetRevision] does. Every
// log attempt is settled with the outcome.
//
// A refusal, a hook error and every check come before Apply, so they change
// nothing. When Apply or the boundary move fails, the revision rows above the
// target stay as they were, describing a database the rollback may have
// changed in part. On a dialect that has no migration lock the applied
// migrations are still read again just before Apply, and nothing stops another
// runner from changing them while Apply runs. On a dry-run connection the
// checks and hook run, and Apply does not.
func (m *Migrator) MigrateDownDerived(ctx context.Context, rollback DerivedRollback, hook PreMigrationHook) (err error) {
	observer := m.migrationObserver()
	ctx, span := observer.StartSpan(ctx, "ptah.migrate.down", append(m.operationAttributes(MigrationDirectionDown),
		attr("migration.requested_target_version", rollback.TargetVersion))...)
	defer func() { span.End(err) }()
	ctx = contextWithRootSpan(ctx, span)
	return m.withMigrationLock(ctx, "migrate down", func(ctx context.Context) error {
		return m.migrateDownDerivedLocked(ctx, rollback, hook)
	})
}

func (m *Migrator) migrateDownDerivedLocked(ctx context.Context, rollback DerivedRollback, hook PreMigrationHook) error {
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migrations table: %w", err)
	}
	if err := m.failIfDirty(ctx); err != nil {
		return err
	}
	applied, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}
	above := newestFirst(slices.DeleteFunc(slices.Clone(applied), func(version int64) bool {
		return version <= rollback.TargetVersion
	}))
	derived := newestFirst(slices.Clone(rollback.Versions))
	if !slices.Equal(above, derived) {
		return &StaleDerivedRollbackError{TargetVersion: rollback.TargetVersion, Derived: derived, Applied: above}
	}
	if err := runPreMigrationHook(ctx, hook, MigrationPlan{
		Direction:      MigrationDirectionDown,
		CurrentVersion: maxAppliedVersion(applied),
		TargetVersion:  downTargetVersion(applied, rollback.TargetVersion),
		Versions:       above,
	}); err != nil {
		return err
	}
	if m.conn.Writer().IsDryRun() {
		return nil
	}
	settle := m.LogDerivedRollback(ctx, rollback.TargetVersion)
	err = applyDerivedRollback(ctx, rollback)
	if err == nil {
		if _, setErr := m.setRevisionLocked(ctx, rollback.TargetVersion); setErr != nil {
			err = fmt.Errorf("move the revision boundary to version %d: %w", rollback.TargetVersion, setErr)
		}
	}
	settle(err)
	return err
}

// applyDerivedRollback runs the statements of rollback, if it has any.
func applyDerivedRollback(ctx context.Context, rollback DerivedRollback) error {
	if rollback.Apply == nil {
		return nil
	}
	return rollback.Apply(ctx)
}

// newestFirst sorts versions in place, highest first, and returns them, so
// two selections compare equal whatever order each was read in.
func newestFirst(versions []int64) []int64 {
	slices.SortFunc(versions, func(a, b int64) int { return cmp.Compare(b, a) })
	return versions
}
