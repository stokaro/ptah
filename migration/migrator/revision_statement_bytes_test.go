package migrator_test

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// statementWithInvalidUTF8 carries one byte that is not part of a valid UTF-8
// sequence, inside a string literal, which is where a hand-written migration
// puts a binary value. The table it names does not exist, so the server is what
// refuses the statement and the revision row has to record it.
const statementWithInvalidUTF8 = "INSERT INTO missing_payloads (payload) VALUES ('\xff');"

// recordedStatementWithInvalidUTF8 is that statement as the revision row
// records it: the executor hands on the statement without its terminating
// semicolon, and the byte that is not valid UTF-8 is rendered as an escape.
const recordedStatementWithInvalidUTF8 = `INSERT INTO missing_payloads (payload) VALUES ('\xFF')`

func invalidUTF8StatementConnection(c *qt.C, path string) *dbschema.DatabaseConnection {
	c.Helper()

	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+path)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// TestMigrateUp_FailurePathRecordsAStatementCarryingBytesThatAreNotUTF8 drives
// the row a failed body leaves behind. The failing statement is what repair and
// `ptah migrations status` name, so it is recorded rather than dropped, and it
// is recorded as text every UTF-8 column accepts.
func TestMigrateUp_FailurePathRecordsAStatementCarryingBytesThatAreNotUTF8(t *testing.T) {
	c := qt.New(t)
	conn := invalidUTF8StatementConnection(c, filepath.Join(t.TempDir(), "failed-statement.db"))

	mig := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(
			1,
			"binary payload",
			"CREATE TABLE payloads (id INTEGER PRIMARY KEY);\n"+statementWithInvalidUTF8,
			"DROP TABLE payloads;",
		),
	))

	c.Assert(mig.MigrateUp(c.Context()), qt.IsNotNil)

	revisions, err := mig.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 1)
	c.Assert(revisions[0].State, qt.Equals, "failed")
	c.Assert(revisions[0].ErrorStatement, qt.Equals, recordedStatementWithInvalidUTF8)
	// The recorded failure repeats the statement on its own SQL line, so the
	// error column carries the same bytes and takes the same rendering.
	c.Assert(revisions[0].Error, qt.Contains, recordedStatementWithInvalidUTF8)
}

// TestMigrateUp_InFlightRowRecordsAStatementCarryingBytesThatAreNotUTF8 drives
// the write that precedes a statement. It runs before the server has seen the
// statement at all, which is why a row it cannot store refuses a migration the
// server would have accepted. Cancelling from the observer that follows
// statement 1 leaves statement 2's marker in place instead of replacing it with
// an ordinary failure.
func TestMigrateUp_InFlightRowRecordsAStatementCarryingBytesThatAreNotUTF8(t *testing.T) {
	c := qt.New(t)
	conn := invalidUTF8StatementConnection(c, filepath.Join(t.TempDir(), "in-flight-statement.db"))
	ctx, cancel := context.WithCancel(c.Context())

	provider, err := migrator.NewFSMigrationProvider(
		fstest.MapFS{
			"000001_binary_payload.up.sql": {
				Data: []byte("-- +ptah no_transaction\nCREATE TABLE payloads (id INTEGER PRIMARY KEY);\n" +
					statementWithInvalidUTF8),
			},
			"000001_binary_payload.down.sql": {Data: []byte("DROP TABLE payloads;")},
		},
		migrator.WithStatementObserver(migrator.StatementObserverFunc(
			func(context.Context, migrator.StatementEvent) error {
				cancel()
				return nil
			},
		)),
	)
	c.Assert(err, qt.IsNil)
	mig := migrator.NewMigrator(conn, provider)

	c.Assert(mig.MigrateUp(ctx), qt.IsNotNil)

	revisions, err := mig.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 1)
	c.Assert(revisions[0].Error, qt.Equals, "statement execution outcome is unknown after process interruption")
	c.Assert(revisions[0].ErrorStatement, qt.Equals, recordedStatementWithInvalidUTF8)
}

// TestMigrateUp_AtlasFormatFailurePathRecordsAStatementCarryingBytesThatAreNotUTF8
// drives the same failure over the Atlas-shaped revision table, whose text
// columns carry a character set on the MySQL family exactly as the native one
// does. That table records the statement from the migration source, so the
// terminating semicolon is part of what it stores.
func TestMigrateUp_AtlasFormatFailurePathRecordsAStatementCarryingBytesThatAreNotUTF8(t *testing.T) {
	c := qt.New(t)
	conn := invalidUTF8StatementConnection(c, filepath.Join(t.TempDir(), "atlas-failed-statement.db"))

	mig := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(
			1,
			"binary payload",
			"CREATE TABLE payloads (id INTEGER PRIMARY KEY);\n"+statementWithInvalidUTF8,
			"DROP TABLE payloads;",
		),
	)).WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)

	c.Assert(mig.MigrateUp(c.Context()), qt.IsNotNil)

	revisions, err := mig.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 1)
	c.Assert(revisions[0].ErrorStatement, qt.Equals, recordedStatementWithInvalidUTF8+";")
	c.Assert(revisions[0].Error, qt.Not(qt.Equals), "")
}
