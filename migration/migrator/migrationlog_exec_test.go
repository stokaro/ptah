package migrator_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// newLogMigrator builds a migrator over a fresh SQLite database with one
// migration that creates and drops a table.
func newLogMigrator(t *testing.T) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	c := qt.New(t)
	t.Helper()
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(t.TempDir(), "log.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })
	migration := migrator.CreateMigrationFromSQL(1, "create_notes",
		"CREATE TABLE notes (id INTEGER PRIMARY KEY);\n", "DROP TABLE notes;\n")
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithActor("release-bot")
	c.Assert(m.Initialize(ctx), qt.IsNil)
	return conn, m
}

// TestMigrationLog_KeepsTheRecordARollbackErases is the property the log
// exists for. The revision table deletes the row on a completed rollback, so
// after this sequence the database is back where it started and only the log
// can say it was ever anywhere else (stokaro/ptah#3406).
func TestMigrationLog_KeepsTheRecordARollbackErases(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	_, m := newLogMigrator(t)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	c.Assert(m.MigrateDown(ctx), qt.IsNil)

	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(0))

	attempts, err := m.MigrationLog(ctx, 0)
	c.Assert(err, qt.IsNil)
	c.Assert(attempts, qt.HasLen, 2)
	// Newest first: the rollback, then the apply it undid.
	c.Assert(attempts[0].Start.Operation, qt.Equals, "down")
	c.Assert(attempts[0].Outcome.State, qt.Equals, migrator.MigrationLogRolledBack)
	c.Assert(attempts[1].Start.Operation, qt.Equals, "up")
	c.Assert(attempts[1].Outcome.State, qt.Equals, migrator.MigrationLogApplied)
	c.Assert(attempts[1].Start.Version, qt.Equals, int64(1))
}

// The actor travels with the provenance that says what the name is worth. A
// name the caller supplied is unverified and is recorded as such.
func TestMigrationLog_RecordsTheActorWithItsProvenance(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	_, m := newLogMigrator(t)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	attempts, err := m.MigrationLog(ctx, 0)

	c.Assert(err, qt.IsNil)
	c.Assert(attempts, qt.HasLen, 1)
	c.Assert(attempts[0].Start.Actor, qt.Equals, "release-bot")
	c.Assert(attempts[0].Start.ActorSource, qt.Equals, migrator.ActorProvided)
}

// The control: a migrator nobody gave a name to records the user the process
// runs as, under a different source. Storing an unverified name and an
// observed one the same way is how a log becomes evidence it cannot support.
func TestMigrationLog_RecordsTheProcessUserWhenNobodyNamedAnActor(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(context.Background(),
		"sqlite://"+filepath.Join(t.TempDir(), "log.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "create_notes",
			"CREATE TABLE notes (id INTEGER PRIMARY KEY);\n", "DROP TABLE notes;\n")))
	c.Assert(m.Initialize(ctx), qt.IsNil)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	attempts, err := m.MigrationLog(ctx, 0)

	c.Assert(err, qt.IsNil)
	c.Assert(attempts, qt.HasLen, 1)
	c.Assert(attempts[0].Start.ActorSource, qt.Not(qt.Equals), migrator.ActorProvided)
}

// A failed migration leaves its record. Without one, a database that refused a
// change reads exactly like one nobody tried to change.
func TestMigrationLog_KeepsTheRecordOfAFailure(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(context.Background(),
		"sqlite://"+filepath.Join(t.TempDir(), "log.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "broken",
			"CREATE TABLE notes (id INTEGER PRIMARY KEY);\nDROP TABLE absent;\n", "SELECT 1;\n")))
	c.Assert(m.Initialize(ctx), qt.IsNil)
	c.Assert(m.MigrateUp(ctx), qt.IsNotNil)

	attempts, err := m.MigrationLog(ctx, 0)

	c.Assert(err, qt.IsNil)
	c.Assert(attempts, qt.HasLen, 1)
	c.Assert(attempts[0].Outcome.State, qt.Equals, migrator.MigrationLogFailed)
	c.Assert(attempts[0].Outcome.Error, qt.Not(qt.Equals), "")
	c.Assert(attempts[0].Undetermined(), qt.IsFalse)
}

// The log is off where the caller says so, and on where nobody said anything:
// a log nobody turned on closes nothing.
func TestMigrationLog_CanBeTurnedOff(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(context.Background(),
		"sqlite://"+filepath.Join(t.TempDir(), "log.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "create_notes",
			"CREATE TABLE notes (id INTEGER PRIMARY KEY);\n", "DROP TABLE notes;\n"))).
		WithMigrationLog(false)
	c.Assert(m.Initialize(ctx), qt.IsNil)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	var tables int
	c.Assert(conn.QueryRow(
		"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='schema_migrations_log'",
	).Scan(&tables), qt.IsNil)
	c.Assert(tables, qt.Equals, 0)
}

// The Atlas-compatible revision format ships the table that contract defines
// and nothing beside it. A second table Ptah added would appear in a database
// an Atlas user believes only Atlas writes, so the log is off there whatever
// the caller asked for.
func TestMigrationLog_IsAbsentUnderTheAtlasRevisionFormat(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(context.Background(),
		"sqlite://"+filepath.Join(t.TempDir(), "log.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "create_notes",
			"CREATE TABLE notes (id INTEGER PRIMARY KEY);\n", "DROP TABLE notes;\n"))).
		WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(m.Initialize(ctx), qt.IsNil)

	var tables int
	c.Assert(conn.QueryRow(
		"SELECT count(*) FROM sqlite_master WHERE type='table' AND name LIKE '%\\_log' ESCAPE '\\'",
	).Scan(&tables), qt.IsNil)
	c.Assert(tables, qt.Equals, 0)

	_, err = m.MigrationLog(ctx, 0)
	c.Assert(err, qt.ErrorMatches, `this migrator keeps no operation log.*`)
}

// The log table must be invisible to the schema comparison, or Ptah plans a
// migration that drops its own bookkeeping: the generator sees a table no
// declaration describes and writes the DROP for it.
//
// A reader-level property, so it is measured through a reader rather than
// through the migrator that created the table.
func TestMigrationLog_IsInvisibleToTheSchemaReader(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newLogMigrator(t)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	schema, err := dbschema.ReadSchemaWithSchemas(conn, nil)

	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		names = append(names, table.Name)
	}
	c.Assert(names, qt.Not(qt.Contains), "schema_migrations_log")
	// The control: the migration's own table IS reported, so the assertion
	// above measures an exclusion rather than a reader that found nothing.
	c.Assert(names, qt.Contains, "notes")
}
