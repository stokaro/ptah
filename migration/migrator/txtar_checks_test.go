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

const txtarNamedOneOfAddEmail = `-- atlas:txtar

-- checks/users.sql --
SELECT 1;

-- checks/roles.sql --
-- atlas:assert oneof
SELECT 0;
SELECT 1;

-- migration.sql --
ALTER TABLE users ADD COLUMN email TEXT;
`

const txtarNamedOneOfFailure = `-- atlas:txtar

-- checks/roles.sql --
-- atlas:assert oneof
SELECT 0;
SELECT 0;

-- migration.sql --
ALTER TABLE users ADD COLUMN email TEXT;
`

const txtarEmptyOneOf = `-- atlas:txtar

-- checks/empty.sql --
-- atlas:assert oneof

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
	c.Assert(
		checkErr.Assert,
		qt.Equals,
		"-- The assertion below must evaluate to true for migration.sql to run.\nSELECT NOT EXISTS (SELECT * FROM users)",
	)
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

func TestMigrateUp_TxtarNamedCheckFilesRunInArchiveOrder(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteTxtarMigrator(t, 0, `-- atlas:txtar

-- checks/users.sql --
SELECT 0;

-- checks/roles.sql --
SELECT NULL;

-- migration.sql --
ALTER TABLE users ADD COLUMN email TEXT;
`)

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.IsNotNil)
	var checkErr *migrator.CheckFailedError
	c.Assert(err, qt.ErrorAs, &checkErr)
	c.Assert(checkErr.Name, qt.Equals, "checks/users.sql#1")
	c.Assert(usersHasEmailColumn(t, conn), qt.IsFalse)
}

func TestMigrateUp_TxtarNamedOneOfGroupPasses(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteTxtarMigrator(t, 0, txtarNamedOneOfAddEmail)

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(usersHasEmailColumn(t, conn), qt.IsTrue)
}

func TestMigrateUp_TxtarNamedOneOfGroupFailsWhenAllAssertionsFail(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteTxtarMigrator(t, 0, txtarNamedOneOfFailure)

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.IsNotNil)
	var checkErr *migrator.CheckGroupFailedError
	c.Assert(err, qt.ErrorAs, &checkErr)
	c.Assert(checkErr.Name, qt.Equals, "checks/roles.sql")
	c.Assert(checkErr.Version, qt.Equals, int64(1))
	c.Assert(checkErr.Assertions, qt.Equals, 2)
	c.Assert(err.Error(), qt.Equals, "pre-migration check failed for migration 1: pre-migration check group checks/roles.sql for migration 1 was not satisfied: none of 2 assertions passed")
	c.Assert(usersHasEmailColumn(t, conn), qt.IsFalse)
}

func TestMigrateUp_TxtarEmptyOneOfGroupFailsClosed(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteTxtarMigrator(t, 0, txtarEmptyOneOf)

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.IsNotNil)
	var checkErr *migrator.CheckGroupFailedError
	c.Assert(err, qt.ErrorAs, &checkErr)
	c.Assert(checkErr.Name, qt.Equals, "checks/empty.sql")
	c.Assert(checkErr.Assertions, qt.Equals, 0)
	c.Assert(usersHasEmailColumn(t, conn), qt.IsFalse)
}

func TestMigrateUp_TxtarEmptyOneOfGroupRejectedUnderTxModeAll(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteTxtarMigrator(t, 0, txtarEmptyOneOf)

	err := m.WithTransactionMode(migrator.MigrationTxModeAll).MigrateUp(context.Background())

	c.Assert(
		err,
		qt.ErrorMatches,
		`migration 1 declares pre-migration checks, which cannot run with tx-mode all; use the default per-file transaction mode`,
	)
	c.Assert(usersHasEmailColumn(t, conn), qt.IsFalse)
}

func TestMigrateUp_TxtarNamedOneOfGroupFailsClosedOnInvalidLaterAssertion(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteTxtarMigrator(t, 0, `-- atlas:txtar

-- checks/roles.sql --
-- atlas:assert oneof
SELECT 1;
SELECT 1 UNION ALL SELECT 1;

-- migration.sql --
ALTER TABLE users ADD COLUMN email TEXT;
`)

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.ErrorMatches, `.*check assertion must return exactly one row, got more than 1.*`)
	c.Assert(usersHasEmailColumn(t, conn), qt.IsFalse)
}

func TestMigrateUp_TxtarLintAssertionDirectiveKeepsAllOfSemantics(t *testing.T) {
	c := qt.New(t)
	conn, m := newSQLiteTxtarMigrator(t, 0, `-- atlas:txtar

-- checks/destructive.sql --
-- atlas:assert DS102
SELECT 0;
SELECT 1;

-- migration.sql --
ALTER TABLE users ADD COLUMN email TEXT;
`)

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.IsNotNil)
	var checkErr *migrator.CheckFailedError
	c.Assert(err, qt.ErrorAs, &checkErr)
	c.Assert(checkErr.Name, qt.Equals, "checks/destructive.sql#1")
	c.Assert(usersHasEmailColumn(t, conn), qt.IsFalse)
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

// newSQLiteTxtarWedgeMigrator builds the measured wedge scenario: migration 1
// applies and seeds a row, migration 2 is a txtar migration whose checks.sql
// asserts users is empty and therefore fails.
func newSQLiteTxtarWedgeMigrator(t *testing.T) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	t.Helper()
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(t.TempDir(), "wedge.db"))
	qt.Assert(t, err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })

	m, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_create_users.sql": &fstest.MapFile{Data: []byte(
				"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\nINSERT INTO users (id, name) VALUES (1, 'alice');\n")},
			"2_add_users_email.sql": &fstest.MapFile{Data: []byte(txtarCheckedAddEmail)},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	qt.Assert(t, err, qt.IsNil)
	m = m.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	qt.Assert(t, m.Initialize(ctx), qt.IsNil)
	return conn, m
}

func revisionVersions(t *testing.T, conn *dbschema.DatabaseConnection) []string {
	t.Helper()
	rows, err := conn.Query("SELECT version FROM atlas_schema_revisions ORDER BY version")
	qt.Assert(t, err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	var versions []string
	for rows.Next() {
		var version string
		qt.Assert(t, rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	qt.Assert(t, rows.Err(), qt.IsNil)
	return versions
}

// TestMigrateUp_TxtarFailingCheckWritesNoRevisionRow pins the recovery contract
// behind the check gate: because checks run before any bookkeeping write, a
// failed check leaves the revision table exactly as it was. Recording the
// failure instead would strand the Atlas-compatible surface, which has neither
// --skip-checks, and whose --allow-dirty cannot clear the row because it fails
// on the re-insert (#966), to recover (#956).
func TestMigrateUp_TxtarFailingCheckWritesNoRevisionRow(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newSQLiteTxtarWedgeMigrator(t)

	err := m.MigrateUp(ctx)

	c.Assert(err, qt.IsNotNil)
	var checkErr *migrator.CheckFailedError
	c.Assert(err, qt.ErrorAs, &checkErr, qt.Commentf("want CheckFailedError, got %v", err))
	// Only migration 1's row exists: migration 2 was never started.
	c.Assert(revisionVersions(t, conn), qt.DeepEquals, []string{"1"})

	// Status still reports the pre-failure version, with migration 2 merely
	// pending rather than dirty.
	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))
	c.Assert(status.PendingMigrations, qt.DeepEquals, []int64{2})
	c.Assert(status.DirtyRevision, qt.IsNil)
}

// TestMigrateUp_TxtarRetryAfterFixingDataSucceeds is the other direction: once
// the operator fixes the data the check guarded, the very next apply works with
// no flags and no manual revision repair.
func TestMigrateUp_TxtarRetryAfterFixingDataSucceeds(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newSQLiteTxtarWedgeMigrator(t)
	c.Assert(m.MigrateUp(ctx), qt.IsNotNil)

	_, err := conn.Exec("DELETE FROM users") // the precondition now holds
	c.Assert(err, qt.IsNil)

	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	c.Assert(usersHasEmailColumn(t, conn), qt.IsTrue)
	c.Assert(revisionVersions(t, conn), qt.DeepEquals, []string{"1", "2"})
	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(2))
	c.Assert(status.PendingMigrations, qt.HasLen, 0)
	c.Assert(status.DirtyRevision, qt.IsNil)
}

// TestMigrateUp_TxtarFailingCheckUnderGlobalTxModeNone pins the check gate on
// the GLOBAL `--tx-mode none` entry point. That path reaches the body through
// applyUpMigrationForcedNoTransactionAt, while a per-file
// `-- +ptah no_transaction` directive routes through applyUpMigrationObserved:
// two distinct gates, so a test for one stays green when the other is deleted.
func TestMigrateUp_TxtarFailingCheckUnderGlobalTxModeNone(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newSQLiteTxtarWedgeMigrator(t)
	m = m.WithTransactionMode(migrator.MigrationTxModeNone)

	// Apply migration 1 alone first, so there is a pre-failure revision state
	// to compare against.
	c.Assert(m.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{TargetVersion: 1}), qt.IsNil)
	before := atlasRevisionTuples(t, conn)
	c.Assert(before, qt.HasLen, 1)

	err := m.MigrateUp(ctx)

	c.Assert(err, qt.IsNotNil)
	var checkErr *migrator.CheckFailedError
	c.Assert(err, qt.ErrorAs, &checkErr, qt.Commentf("want CheckFailedError, got %v", err))
	c.Assert(checkErr.Name, qt.Equals, "checks.sql#1")
	c.Assert(checkErr.Version, qt.Equals, int64(2))
	// The body never ran and the revision table is untouched.
	c.Assert(usersHasEmailColumn(t, conn), qt.IsFalse)
	c.Assert(atlasRevisionTuples(t, conn), qt.DeepEquals, before)
}

// TestMigrateUp_FailingCheckOnNoTransactionPathWritesNoRevisionRow pins the same
// no-bookkeeping contract on the non-transactional apply path.
func TestMigrateUp_FailingCheckOnNoTransactionPathWritesNoRevisionRow(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(t.TempDir(), "wedge-notx.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })

	m, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_create_users.sql": &fstest.MapFile{Data: []byte(
				"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\nINSERT INTO users (id, name) VALUES (1, 'alice');\n")},
			"2_add_users_email.sql": &fstest.MapFile{Data: []byte(
				"-- atlas:txtar\n\n-- checks.sql --\nSELECT NOT EXISTS (SELECT * FROM users);\n\n" +
					"-- migration.sql --\n-- +ptah no_transaction\nALTER TABLE users ADD COLUMN email TEXT;\n")},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	m = m.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(m.Initialize(ctx), qt.IsNil)

	c.Assert(m.MigrateUp(ctx), qt.IsNotNil)

	c.Assert(revisionVersions(t, conn), qt.DeepEquals, []string{"1"})
	c.Assert(usersHasEmailColumn(t, conn), qt.IsFalse)
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

func TestNewFSMigrator_TxtarDuplicateNamedChecksSectionFails(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(t.TempDir(), "dup-named-checks.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_add_users_email.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- checks/users.sql --
SELECT 1;

-- checks/users.sql --
SELECT 2;

-- migration.sql --
SELECT 3;
`)},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.ErrorMatches, `.*duplicate checks/users.sql section.*`)
}
