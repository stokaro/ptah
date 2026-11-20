package shadowdb

import "ptah.run/migration/migrator"

// ReplayMigrator returns a copy of mig, built on a claimed database, that
// replays without the migration advisory lock.
//
// A claimed database is the verification's alone, and the revision history
// the replay writes there lives only until [Lease.Release] drops it, so the
// lock would serialize nothing. Taking it costs a hang: the caller may hold
// the target's lock while it verifies, as `migrations up --check-drift` does,
// and on an engine whose lock names are server-wide, MySQL and MariaDB, a
// shadow database on the target's server waits on that lock for as long as
// the caller holds it, which is until the replay returns. Every replay of package shadow into a
// claimed database goes through here.
//
// The checkpoint generator is not a verification of a target and holds no
// target lock around its replay; it keeps the lock and bounds the wait with
// its own --migration-lock-timeout.
func ReplayMigrator(mig *migrator.Migrator) *migrator.Migrator {
	return mig.WithoutMigrationLock()
}
