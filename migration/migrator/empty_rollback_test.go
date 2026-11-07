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

// emptyRollbackUp is the migration under every test here: an up file whose object
// a down file with no statements would leave behind.
const emptyRollbackUp = "CREATE TABLE subscriptions (id INTEGER PRIMARY KEY);\n"

// emptyRollbackMigrator applies a real first migration and then the second one,
// whose down file is down, on a fresh SQLite database.
func emptyRollbackMigrator(c *qt.C, down string) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "rollback.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	m, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"0000000001_first.up.sql":    {Data: []byte("CREATE TABLE first (id INTEGER PRIMARY KEY);\n")},
		"0000000001_first.down.sql":  {Data: []byte("DROP TABLE first;\n")},
		"0000000002_second.up.sql":   {Data: []byte(emptyRollbackUp)},
		"0000000002_second.down.sql": {Data: []byte(down)},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(m.Initialize(c.Context()), qt.IsNil)
	c.Assert(m.MigrateUp(c.Context()), qt.IsNil)
	return conn, m
}

// tableExists asks the database, so a test reads what the rollback left rather
// than what the migrator reported.
func tableExists(c *qt.C, conn *dbschema.DatabaseConnection, table string) bool {
	c.Helper()
	var found int
	err := conn.QueryRowContext(c.Context(), "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&found)
	c.Assert(err, qt.IsNil)
	return found == 1
}

// TestMigrateDown_RefusesADownFileWithNoStatements is the defect: the rollback ran
// nothing, moved the version, and the next apply failed on the table it left.
// Each row is a file a reader takes for empty, and nothing moves under any of them,
// including the rollback of the migration above it in the plan.
func TestMigrateDown_RefusesADownFileWithNoStatements(t *testing.T) {
	tests := []struct {
		name string
		down string
	}{
		{name: "no bytes", down: ""},
		{name: "blank lines", down: "\n  \n"},
		{name: "what the importer writes", down: "-- No rollback was provided by the source migration.\n"},
		{name: "a block comment", down: "/* nothing */\n"},
		{name: "a transaction directive", down: "-- +ptah no_transaction\n-- nothing\n"},
		{name: "a directive that says false", down: "-- +ptah empty_rollback=false\n"},
		{name: "a check and nothing to run after it", down: `-- +ptah check name="c" assert="SELECT 1"` + "\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, m := emptyRollbackMigrator(c, test.down)

			err := m.MigrateDown(c.Context())

			var refused *migrator.EmptyRollbackError
			c.Assert(err, qt.ErrorAs, &refused)
			c.Assert(refused.Version, qt.Equals, int64(2))
			c.Assert(refused.Path, qt.Equals, "0000000002_second.down.sql")
			c.Assert(err, qt.ErrorMatches, `.*migration 2 has a down file with no statements \(0000000002_second.down.sql\).*empty_rollback.*`)
			version, versionErr := m.GetCurrentVersion(c.Context())
			c.Assert(versionErr, qt.IsNil)
			c.Assert(version, qt.Equals, int64(2))
			c.Assert(tableExists(c, conn, "subscriptions"), qt.IsTrue)
			c.Assert(m.MigrateDownTo(c.Context(), 0), qt.ErrorAs, &refused)
			c.Assert(tableExists(c, conn, "first"), qt.IsTrue, qt.Commentf("the plan is refused whole, before its first file"))
		})
	}
}

// TestMigrateDown_RunsNothingForADirectiveAndMovesTheVersion is the statement a
// refusal needs beside it. The rollback runs nothing, so the table stays, and the
// version moves because the author said that is right.
func TestMigrateDown_RunsNothingForADirectiveAndMovesTheVersion(t *testing.T) {
	tests := []struct {
		name string
		down string
	}{
		{name: "the directive alone", down: "-- +ptah empty_rollback\n"},
		{name: "beside a comment", down: "-- The up file is safe to run again.\n-- +ptah empty_rollback\n"},
		{name: "beside a transaction directive", down: "-- +ptah no_transaction\n-- +ptah empty_rollback\n"},
		{name: "an explicit true", down: "-- +ptah empty_rollback=true\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, m := emptyRollbackMigrator(c, test.down)

			c.Assert(m.MigrateDown(c.Context()), qt.IsNil)

			version, err := m.GetCurrentVersion(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(version, qt.Equals, int64(1))
			c.Assert(tableExists(c, conn, "subscriptions"), qt.IsTrue)
		})
	}
}

// TestMigrateDown_RefusesADirectiveBesideStatements covers a file that contradicts
// itself. Running the statements ignores what the author stated, and skipping them
// ignores what the author wrote.
func TestMigrateDown_RefusesADirectiveBesideStatements(t *testing.T) {
	c := qt.New(t)
	conn, m := emptyRollbackMigrator(c, "-- +ptah empty_rollback\nDROP TABLE subscriptions;\n")

	err := m.MigrateDown(c.Context())

	var refused *migrator.EmptyRollbackConflictError
	c.Assert(err, qt.ErrorAs, &refused)
	c.Assert(refused.Statements, qt.Equals, 1)
	c.Assert(err, qt.ErrorMatches, `.*migration 2: 0000000002_second.down.sql states .* and holds 1 statement\(s\).*`)
	c.Assert(tableExists(c, conn, "subscriptions"), qt.IsTrue)
}

// TestMigrateDown_RefusesAMalformedDirective names the value rather than reading a
// typo as no statement at all.
func TestMigrateDown_RefusesAMalformedDirective(t *testing.T) {
	c := qt.New(t)
	_, m := emptyRollbackMigrator(c, "-- +ptah empty_rollback=maybe\n")

	err := m.MigrateDown(c.Context())

	c.Assert(err, qt.ErrorMatches, `.*invalid \+ptah empty_rollback value "maybe": expected true or false.*`)
}

// TestMigrateDown_ADownOfStatementsIsNotAffected is the control: a directive below
// a statement is not honored, so the statement runs.
func TestMigrateDown_ADownOfStatementsIsNotAffected(t *testing.T) {
	tests := []struct {
		name string
		down string
	}{
		{name: "a statement", down: "DROP TABLE subscriptions;\n"},
		{name: "a statement above the directive", down: "DROP TABLE subscriptions;\n-- +ptah empty_rollback\n"},
		{name: "a statement below a false directive", down: "-- +ptah empty_rollback=false\nDROP TABLE subscriptions;\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, m := emptyRollbackMigrator(c, test.down)

			c.Assert(m.MigrateDown(c.Context()), qt.IsNil)

			c.Assert(tableExists(c, conn, "subscriptions"), qt.IsFalse)
		})
	}
}

// TestMigrateDown_AGoFunctionAnswersForItself covers the down the refusal cannot
// read. A function has no SQL to count, and [migrator.NoopMigrationFunc] says what
// it does in its name.
func TestMigrateDown_AGoFunctionAnswersForItself(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(c.TempDir(), "rollback.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(&migrator.Migration{
		Version: 1, Description: "in code",
		Up:   migrator.NoopMigrationFunc,
		Down: migrator.NoopMigrationFunc,
	}))
	c.Assert(m.Initialize(ctx), qt.IsNil)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	c.Assert(m.MigrateDown(ctx), qt.IsNil)

	version, err := m.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(0))
}

// TestMigrateDown_ACreatedMigrationIsHeldToTheSameRule covers the SQL an embedder
// hands over as a string, which the shadow verification of a generated migration
// reads through too.
func TestMigrateDown_ACreatedMigrationIsHeldToTheSameRule(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(c.TempDir(), "rollback.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "in_code", emptyRollbackUp, "  "),
	))
	c.Assert(m.Initialize(ctx), qt.IsNil)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	err = m.MigrateDown(ctx)

	var refused *migrator.EmptyRollbackError
	c.Assert(err, qt.ErrorAs, &refused)
	c.Assert(err.Error(), qt.Contains, "in_code")
}
