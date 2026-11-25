package shadowdb

import "ptah.run/migration/migrator"

// ReplayMigrator returns a copy of mig, built on the database lease claimed,
// that replays through the lease's replay guard ([GuardedMigrator]) and
// without the migration advisory lock.
//
// A claimed database is the verification's alone, and the revision history
// the replay writes there lives only until [Lease.Release] drops it, so the
// lock would serialize nothing. Taking it costs a hang: the caller may hold
// the target's lock while it verifies, as `migrations up --check-drift` does,
// and on an engine whose lock names are server-wide, MySQL and MariaDB, a
// shadow database on the target's server waits on that lock for as long as
// the caller holds it, which is until the replay returns. Every replay of
// package shadow into a claimed database goes through here.
//
// The checkpoint generator is not a verification of a target and holds no
// target lock around its replay; it keeps the lock and bounds the wait with
// its own --migration-lock-timeout, so it calls [GuardedMigrator] alone.
func ReplayMigrator(lease *Lease, mig *migrator.Migrator) *migrator.Migrator {
	return GuardedMigrator(lease, mig).WithoutMigrationLock()
}

// GuardedMigrator returns a copy of mig, built on the database lease claimed,
// that validates every statement a run of it executes with the lease's replay
// guard, before the run's first statement ([migrator.Migrator.WithStatementValidator]).
//
// A replay executes a migration history on a database the operator named for
// the purpose, which may sit on a server holding other databases and roles.
// The guard refuses what the reset after the replay cannot take back, such as
// a write to another database, a role or a routine body, the same way it
// refuses it for `migrations validate`, and the operator lifts a refusal the
// same ways: a server the run owns, or trust in the replayed SQL.
func GuardedMigrator(lease *Lease, mig *migrator.Migrator) *migrator.Migrator {
	return mig.WithStatementValidator(lease.guard)
}
