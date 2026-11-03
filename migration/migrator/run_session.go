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
// migration's does, is qualified with the connection's schema, so a setting a
// migration leaves cannot move it.
//
// A dry run executes nothing. An engine that splits a body into query units
// keeps no session state between queries (see migratorext.Units), and one
// whose services ask for the pool's sessions keeps them (see
// migratorext.Services.PoolSessions): SQLite, where a migration in a
// transaction runs on the pool's connection.
//
// On a provider's connection the run's session is the connection's own, where
// the provider holds the migration lock, because the provider guards the
// commits of that session alone (see withMigrationLockSession). That session
// belongs to the connection, so what a migration leaves on it outlives the
// run.
func (m *Migrator) withRunSession(ctx context.Context, use func(*Migrator) error) error {
	services := m.services()
	if m.runSession != nil || m.conn == nil || m.conn.Writer().IsDryRun() ||
		services.Units != nil || services.PoolSessions {
		return use(m)
	}
	if locked := migrationLockSession(ctx); locked != nil {
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
		scoped.migrationsSchema = scoped.connectionSchemaName()
	}
	return &scoped
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
