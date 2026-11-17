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
	// The revision boundary moves to the newest migration file at or below
	// it, or to 0 when there is none, so a version no file names is accepted.
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
// boundary, holding the migration lock [Migrator.MigrateDownToWithPreflight]
// takes for a rollback through the down bodies. Runners that take the same
// lock wait for it, and it waits for them.
//
// The boundary is the newest migration at or below the target, or zero when
// there is none: the statements were derived against the directory up to the
// target, so that is what they leave in place. It is the target when a
// migration file names it, and the migration below it when none does. With
// every migration at or below the target applied, it is also where a rollback
// through the down bodies to the same target lands.
//
// Holding the lock, it refuses a negative target, refuses a dirty revision
// with a [*DirtyMigrationError] and reads the applied migrations again. When
// the ones above the target are not [DerivedRollback.Versions], it returns a
// [*StaleDerivedRollbackError]. It then gives every refusal that moving the
// boundary as [Migrator.SetRevision] does would give over the revisions as
// they are. After these checks it runs hook with the selected plan, whose
// TargetVersion is the boundary, opens a log attempt for each migration the
// rollback undoes as [Migrator.LogDerivedRollback] does, runs Apply, and moves
// the boundary. Every log attempt is settled with the outcome.
//
// The checks and the hook come before Apply, so a refusal changes nothing.
// When Apply fails, or writing the boundary fails, the revision rows above the
// target stay as they were, describing a database the rollback may have
// changed in part. On a dialect that has no migration lock the checks still
// read the database right before Apply, and nothing stops another runner from
// changing it while Apply runs. On a dry-run connection the checks and the
// hook run, and Apply and the boundary move do not.
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
	if rollback.TargetVersion < 0 {
		return fmt.Errorf("migration version must not be negative")
	}
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
	if err := m.repeatableRollbackRefusal(ctx); err != nil {
		return err
	}
	boundary := m.derivedRollbackBoundary(rollback.TargetVersion)
	if err := m.checkRevisionSet(ctx, boundary); err != nil {
		return fmt.Errorf("move the revision boundary to version %d: %w", boundary, err)
	}
	if err := runPreMigrationHook(ctx, hook, MigrationPlan{
		Direction:      MigrationDirectionDown,
		CurrentVersion: maxAppliedVersion(applied),
		TargetVersion:  boundary,
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
		if _, setErr := m.setRevisionLocked(ctx, boundary); setErr != nil {
			err = fmt.Errorf("move the revision boundary to version %d: %w", boundary, setErr)
		}
	}
	settle(err)
	return err
}

// derivedRollbackBoundary returns the newest migration at or below target,
// or zero when there is none.
func (m *Migrator) derivedRollbackBoundary(target int64) int64 {
	var boundary int64
	for _, migration := range m.migrationProvider.Migrations() {
		if migration.Version <= target && migration.Version > boundary {
			boundary = migration.Version
		}
	}
	return boundary
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
