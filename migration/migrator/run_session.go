package migrator

import (
	"context"

	"ptah.run/dbschema"
)

// withRunSession runs use with a migrator whose up run executes every
// migration body on one pinned session, which is discarded when use returns.
//
// A session setting, a temporary object or a prepared statement one migration
// leaves therefore holds for the migrations after it in the same run, and for
// none in the next run. golang-migrate runs a directory on one connection, and
// the pinned community binary carries a setting from one file to the next as
// well. Without the shared session each migration starts on a session of its
// own, and a table the file before it meant for another schema through `SET
// search_path` lands in the default one (stokaro/ptah#5034).
//
// Revision bookkeeping that runs on that session, as a transactional
// migration's does, is qualified with the schema the connection keeps the
// migrator's tables in, so a setting a migration leaves cannot move it.
//
// A dry run executes nothing. An engine whose services ask for the pool's
// sessions keeps them (see migratorext.Services.PoolSessions): SQLite, where a
// migration in a transaction runs on the pool's connection.
//
// On a provider's connection the run's session is the connection's own, where
// the provider holds the migration lock, because the provider guards the
// commits of that session alone (see withMigrationLockSession). That session
// belongs to the connection, so what a migration leaves on it outlives the
// run.
func (m *Migrator) withRunSession(ctx context.Context, use func(*Migrator) error) error {
	services := m.services()
	if m.runSession != nil || m.conn == nil || m.conn.Writer().IsDryRun() || services.PoolSessions {
		return use(m)
	}
	// The lock is the session's that took it, so a migrator on another
	// connection, such as a shadow replay run while the target's lock is held,
	// keeps to its own connection.
	if locked := migrationLockSession(ctx); locked != nil && locked == m.conn {
		return use(m.inRunSession(locked))
	}
	return m.conn.WithSessionOrCurrent(ctx, func(session *dbschema.DatabaseConnection) error {
		return use(m.inRunSession(session))
	})
}

// inRunSession returns a copy of the migrator whose up run executes every
// migration body on session.
func (m *Migrator) inRunSession(session *dbschema.DatabaseConnection) *Migrator {
	scoped := *m
	scoped.runSession = session
	if scoped.migrationsSchema == "" {
		scoped.migrationsSchema = scoped.connectionMetadataSchema()
	}
	return &scoped
}

// sessionSettingsResetter is an executor that can give its session back a
// setting a statement changed: the SQLite writer and transaction, for
// legacy_alter_table. A run calls it after each migration file whose session
// outlives the file, so one file cannot change how the next one's statements
// behave. An executor that is not one has nothing to give back.
type sessionSettingsResetter interface {
	ResetSessionSettings(ctx context.Context) error
}

// resetSessionSettings gives executor's session back the settings a statement
// changed, where executor keeps any; see [sessionSettingsResetter].
func resetSessionSettings(ctx context.Context, executor any) error {
	if resetter, ok := executor.(sessionSettingsResetter); ok {
		return resetter.ResetSessionSettings(ctx)
	}
	return nil
}

// withBodySession calls use with the session a migration body runs on: the
// run's, which outlives the call, or a session pinned for this call alone and
// discarded when it returns.
func (m *Migrator) withBodySession(ctx context.Context, use func(*dbschema.DatabaseConnection) error) error {
	if m.runSession != nil {
		return use(m.runSession)
	}
	return m.conn.WithSession(ctx, use)
}

// bodyConnection is the connection a migration body that runs in the
// migrator's own transaction opens that transaction on.
func (m *Migrator) bodyConnection() *dbschema.DatabaseConnection {
	if m.runSession != nil {
		return m.runSession
	}
	return m.conn
}
