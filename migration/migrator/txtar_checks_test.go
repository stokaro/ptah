package migrator_test

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

// txtarCheckedAddEmail mirrors the measured Atlas Pro fixture: a txtar
// migration whose checks.sql asserts users is empty before migration.sql adds
// the email column. Atlas v1.2.4 (licensed) aborts with exit 1 before any body
// statement when the assertion fails; Ptah maps the section onto the
// `-- +ptah check` machinery (#956).
const txtarCheckedAddEmail = `-- atlas:txtar

-- checks.sql --
-- The assertion below must evaluate to true for migration.sql to run.
SELECT NOT EXISTS (SELECT * FROM users);

-- migration.sql --
ALTER TABLE users ADD COLUMN email TEXT;
`

const txtarUncheckedAddEmail = `-- atlas:txtar

-- migration.sql --
ALTER TABLE users ADD COLUMN email TEXT;
`

const txtarTwoChecksAddEmail = `-- atlas:txtar

-- checks.sql --
SELECT 1;
SELECT NOT EXISTS (SELECT * FROM users);

-- migration.sql --
ALTER TABLE users ADD COLUMN email TEXT;
`

func newSQLiteTxtarMigrator(t *testing.T, seededRows int, migrationSQL string) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "txtar-checks.db")
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+path)
	qt.Assert(t, err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = conn.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	qt.Assert(t, err, qt.IsNil)
	for i := range seededRows {
		_, err = conn.Exec("INSERT INTO users (id, name) VALUES (?, 'alice')", i+1)
		qt.Assert(t, err, qt.IsNil)
	}

	m, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_add_users_email.sql": &fstest.MapFile{Data: []byte(migrationSQL)},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	qt.Assert(t, err, qt.IsNil)
	m = m.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	qt.Assert(t, m.Initialize(ctx), qt.IsNil)
	return conn, m
}

func usersHasEmailColumn(t *testing.T, conn *dbschema.DatabaseConnection) bool {
	t.Helper()
	var count int
	err := conn.QueryRow("SELECT count(*) FROM pragma_table_info('users') WHERE name = 'email'").Scan(&count)
	qt.Assert(t, err, qt.IsNil)
	return count == 1
}

func TestMigrateUp_TxtarFailingCheckAbortsBeforeBody(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteTxtarMigrator(t, 1, txtarCheckedAddEmail) // users not empty -> check fails

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.IsNotNil)
	var checkErr *migrator.CheckFailedError
	c.Assert(err, qt.ErrorAs, &checkErr, qt.Commentf("want CheckFailedError, got %v", err))
	c.Assert(checkErr.Version, qt.Equals, int64(1))
	c.Assert(checkErr.Name, qt.Equals, "checks.sql#1")
	c.Assert(checkErr.Assert, qt.Equals, "SELECT NOT EXISTS (SELECT * FROM users)")
	// The error names the migration and the failing check.
	c.Assert(err.Error(), qt.Contains, "pre-migration check checks.sql#1 for migration 1 was not satisfied")
	// Nothing from migration.sql was applied.
	c.Assert(usersHasEmailColumn(t, conn), qt.IsFalse)
}

func TestMigrateUp_TxtarPassingCheckApplies(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteTxtarMigrator(t, 0, txtarCheckedAddEmail) // users empty -> check passes

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(usersHasEmailColumn(t, conn), qt.IsTrue)
}

func TestMigrateUp_TxtarWithoutChecksAppliesUnchanged(t *testing.T) {
	c := qt.New(t)
	// No checks.sql section: rows in users must not block the migration.
	conn, m := newSQLiteTxtarMigrator(t, 1, txtarUncheckedAddEmail)

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(usersHasEmailColumn(t, conn), qt.IsTrue)
}

func TestMigrateUp_TxtarMultipleChecksAllMustPass(t *testing.T) {
	c := qt.New(t)
	// The first check passes; the second fails and is named by position.
	conn, m := newSQLiteTxtarMigrator(t, 1, txtarTwoChecksAddEmail)

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.IsNotNil)
	var checkErr *migrator.CheckFailedError
	c.Assert(err, qt.ErrorAs, &checkErr, qt.Commentf("want CheckFailedError, got %v", err))
	c.Assert(checkErr.Name, qt.Equals, "checks.sql#2")
	c.Assert(usersHasEmailColumn(t, conn), qt.IsFalse)
}

func TestMigrateUp_TxtarMultipleChecksPassApplies(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteTxtarMigrator(t, 0, txtarTwoChecksAddEmail)

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(usersHasEmailColumn(t, conn), qt.IsTrue)
}

func TestMigrateUp_TxtarSkipChecksBypassesFailingCheck(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteTxtarMigrator(t, 1, txtarCheckedAddEmail) // check would fail

	err := m.WithSkipChecks(true).MigrateUp(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(usersHasEmailColumn(t, conn), qt.IsTrue)
}

func TestMigrateUp_TxtarChecksRejectedUnderTxModeAll(t *testing.T) {
	c := qt.New(t)
	// Parity with `-- +ptah check`: tx-mode all refuses checked files even when
	// the assertion would pass, because a pool read cannot see the batch's
	// uncommitted state.
	conn, m := newSQLiteTxtarMigrator(t, 0, txtarCheckedAddEmail)

	err := m.WithTransactionMode(migrator.MigrationTxModeAll).MigrateUp(context.Background())

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "cannot run with tx-mode all")
	c.Assert(usersHasEmailColumn(t, conn), qt.IsFalse) // nothing applied
}

func TestMigrateUp_TxtarTxModeAllWithSkipChecksProceeds(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteTxtarMigrator(t, 1, txtarCheckedAddEmail) // check would fail; bypass lifts the restriction

	err := m.WithTransactionMode(migrator.MigrationTxModeAll).WithSkipChecks(true).MigrateUp(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(usersHasEmailColumn(t, conn), qt.IsTrue)
}

func TestNewFSMigrator_TxtarDuplicateChecksSectionFails(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(t.TempDir(), "dup-checks.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_add_users_email.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- checks.sql --
SELECT 1;

-- checks.sql --
SELECT 1;

-- migration.sql --
SELECT 1;
`)},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.ErrorMatches, `.*duplicate checks.sql section.*`)
}
