package migrator_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// releasedMigrationLogDDL is the operation log as Ptah v0.26.0 creates it on
// SQLite: the layout before run_reference and actor_origin, which every
// database a released Ptah migrated already holds.
const releasedMigrationLogDDL = `CREATE TABLE IF NOT EXISTS schema_migrations_log (
    run_id VARCHAR(64) NOT NULL,
    seq BIGINT NOT NULL,
    operation VARCHAR(32) NOT NULL,
    version BIGINT NOT NULL,
    state VARCHAR(32) NOT NULL,
    actor VARCHAR(256) NULL,
    actor_source VARCHAR(32) NOT NULL,
    checksum VARCHAR(64) NULL,
    logged_at TIMESTAMP NOT NULL,
    error TEXT NULL,
    PRIMARY KEY (run_id, seq)
)`

// newReferenceLogDatabase opens a fresh SQLite database.
func newReferenceLogDatabase(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "reference.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	return conn
}

// newReferenceMigrator is a migrator over conn with one migration that
// creates a table.
func newReferenceMigrator(conn *dbschema.DatabaseConnection) *migrator.Migrator {
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "create_notes",
			"CREATE TABLE notes (id INTEGER PRIMARY KEY);\n", "DROP TABLE notes;\n")))
}

// logColumns reads the columns of the operation log, in table order.
func logColumns(c *qt.C, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(), "SELECT name FROM pragma_table_info('schema_migrations_log') ORDER BY cid")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var columns []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		columns = append(columns, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return columns
}

// The reference and the origin the caller gave are on both entries of the
// attempt, as the database holds them, and the report names the reference.
func TestMigrationLog_StoresTheRunReference_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	m := newReferenceMigrator(newReferenceLogDatabase(c)).
		WithActor("alice@example.com").
		WithActorOrigin("admission").
		WithRunReference(operationID)
	c.Assert(m.Initialize(ctx), qt.IsNil)

	err := m.MigrateUp(ctx)

	c.Assert(err, qt.IsNil)
	attempts, err := m.MigrationLog(ctx, 0)
	c.Assert(err, qt.IsNil)
	c.Assert(attempts, qt.HasLen, 1)
	c.Assert(attempts[0].Start.RunReference, qt.Equals, operationID)
	c.Assert(attempts[0].Outcome.RunReference, qt.Equals, operationID)
	c.Assert(attempts[0].Start.ActorOrigin, qt.Equals, "admission")
	c.Assert(attempts[0].Outcome.ActorOrigin, qt.Equals, "admission")
	c.Assert(attempts[0].Start.ActorSource, qt.Equals, migrator.ActorProvided)
	c.Assert(m.MigrationLogReport(), qt.DeepEquals, &migrator.MigrationLogReport{Written: 2, RunReference: operationID})
}

// The control: a run given neither writes neither, and its report names no
// reference rather than an empty one.
func TestMigrationLog_StoresNoReferenceNobodyGave_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	m := newReferenceMigrator(newReferenceLogDatabase(c)).WithActor("alice@example.com")
	c.Assert(m.Initialize(ctx), qt.IsNil)

	err := m.MigrateUp(ctx)

	c.Assert(err, qt.IsNil)
	attempts, err := m.MigrationLog(ctx, 0)
	c.Assert(err, qt.IsNil)
	c.Assert(attempts, qt.HasLen, 1)
	c.Assert(attempts[0].Start.RunReference, qt.Equals, "")
	c.Assert(attempts[0].Start.ActorOrigin, qt.Equals, "")
	c.Assert(m.MigrationLogReport(), qt.DeepEquals, &migrator.MigrationLogReport{Written: 2})
}

// An origin describes a name the caller supplied. Beside the user the process
// runs as it would claim the caller's word for Ptah's own reading, so it is
// not recorded there.
func TestMigrationLog_RecordsNoOriginBesideTheProcessUser_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	m := newReferenceMigrator(newReferenceLogDatabase(c)).
		WithActor("").
		WithActorOrigin("admission").
		WithRunReference(operationID)
	c.Assert(m.Initialize(ctx), qt.IsNil)

	err := m.MigrateUp(ctx)

	c.Assert(err, qt.IsNil)
	attempts, err := m.MigrationLog(ctx, 0)
	c.Assert(err, qt.IsNil)
	c.Assert(attempts, qt.HasLen, 1)
	c.Assert(attempts[0].Start.ActorSource, qt.Not(qt.Equals), migrator.ActorProvided)
	c.Assert(attempts[0].Start.ActorOrigin, qt.Equals, "")
	c.Assert(attempts[0].Start.RunReference, qt.Equals, operationID)
}

// A log table a released Ptah created gains the two columns on the first
// write of a run, keeps the rows it held, and reads them back with neither.
func TestMigrationLog_UpgradesALogTableAReleaseCreated_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := newReferenceLogDatabase(c)
	_, err := conn.ExecContext(ctx, releasedMigrationLogDDL)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx,
		`INSERT INTO schema_migrations_log (run_id, seq, operation, version, state, actor, actor_source, checksum, logged_at, error)
VALUES ('released-run', 1, 'up', 9, 'started', 'bob', 'provided', NULL, ?, NULL)`,
		time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC))
	c.Assert(err, qt.IsNil)
	m := newReferenceMigrator(conn).WithActor("alice@example.com").WithActorOrigin("admission").WithRunReference(operationID)
	c.Assert(m.Initialize(ctx), qt.IsNil)

	err = m.MigrateUp(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(logColumns(c, conn), qt.DeepEquals, []string{
		"run_id", "seq", "operation", "version", "state", "actor", "actor_source", "checksum", "logged_at", "error",
		"run_reference", "actor_origin",
	})
	attempts, err := m.MigrationLog(ctx, 0)
	c.Assert(err, qt.IsNil)
	c.Assert(attempts, qt.HasLen, 2)
	c.Assert(attempts[0].Start.RunReference, qt.Equals, operationID)
	c.Assert(attempts[0].Start.ActorOrigin, qt.Equals, "admission")
	c.Assert(attempts[1].Start.RunID, qt.Equals, "released-run")
	c.Assert(attempts[1].Start.Actor, qt.Equals, "bob")
	c.Assert(attempts[1].Start.RunReference, qt.Equals, "")
	c.Assert(attempts[1].Start.ActorOrigin, qt.Equals, "")
	c.Assert(m.MigrationLogReport(), qt.DeepEquals, &migrator.MigrationLogReport{Written: 2, RunReference: operationID})
}

// Reading changes nothing: a log table a released Ptah created is read as it
// stands, without the columns it lacks, and still lacks them afterwards.
func TestMigrationLog_ReadsALogTableAReleaseCreatedAsItStands_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := newReferenceLogDatabase(c)
	_, err := conn.ExecContext(ctx, releasedMigrationLogDDL)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx,
		`INSERT INTO schema_migrations_log (run_id, seq, operation, version, state, actor, actor_source, checksum, logged_at, error)
VALUES ('released-run', 1, 'up', 9, 'started', 'bob', 'provided', NULL, ?, NULL)`,
		time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC))
	c.Assert(err, qt.IsNil)
	m := newReferenceMigrator(conn)
	c.Assert(m.Initialize(ctx), qt.IsNil)

	attempts, err := m.MigrationLog(ctx, 0)

	c.Assert(err, qt.IsNil)
	c.Assert(attempts, qt.HasLen, 1)
	c.Assert(attempts[0].Start.RunID, qt.Equals, "released-run")
	c.Assert(attempts[0].Start.RunReference, qt.Equals, "")
	c.Assert(attempts[0].Start.ActorOrigin, qt.Equals, "")
	c.Assert(logColumns(c, conn), qt.DeepEquals, []string{
		"run_id", "seq", "operation", "version", "state", "actor", "actor_source", "checksum", "logged_at", "error",
	})
}
