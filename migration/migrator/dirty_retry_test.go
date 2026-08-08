package migrator_test

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// Recovery after a failed migration body (#966).
//
// A body that fails part-way records a dirty revision row. Every retry then had
// to insert that same version again and died on the revision table's UNIQUE
// constraint, including the retry the operator asked for with --allow-dirty
// after fixing the migration; `migrations repair` was the only way out. These
// tests fail a real migration's body, fix it, and require the retry to run.
//
// The fixture discriminates: the same fixed migration set applies cleanly to an
// empty database, so only the dirty-row retry path is under test.

const dirtyRetryFirstUp = "CREATE TABLE users (id INTEGER PRIMARY KEY);\n"

// dirtyRetryFailingUp fails on its second statement. Under a per-file
// transaction the whole body is rolled back; under no-transaction the INSERT is
// already committed when the retry starts, which is what makes the audit row
// count a resume detector rather than an idempotence accident.
const dirtyRetryFailingUp = `INSERT INTO users (id) VALUES (1);
THIS IS A FAILING STATEMENT;
`

const dirtyRetryFixedUp = `INSERT INTO users (id) VALUES (1);
CREATE TABLE pets (id INTEGER PRIMARY KEY);
`

// newDirtyRetryMigrator builds a migrator over a two-migration directory whose
// second file is secondUp, sharing dbPath so a later call models a second run of
// the tool against the database the first one left behind.
func newDirtyRetryMigrator(
	c *qt.C,
	dbPath string,
	secondUp string,
	txMode migrator.MigrationTxMode,
	revisionFormat migrator.RevisionTableFormat,
) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })

	m, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"0000000001_users.up.sql":   &fstest.MapFile{Data: []byte(dirtyRetryFirstUp)},
		"0000000001_users.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE users;\n")},
		"0000000002_seed.up.sql":    &fstest.MapFile{Data: []byte(secondUp)},
		"0000000002_seed.down.sql":  &fstest.MapFile{Data: []byte("DROP TABLE pets;\n")},
	})
	c.Assert(err, qt.IsNil)
	return conn, m.WithTransactionMode(txMode).WithRevisionTableFormat(revisionFormat)
}

func dirtyRetryUserCount(c *qt.C, conn *dbschema.DatabaseConnection) int {
	c.Helper()
	var count int
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT count(*) FROM users").Scan(&count), qt.IsNil)
	return count
}

func dirtyRetryTableExists(c *qt.C, conn *dbschema.DatabaseConnection, table string) bool {
	c.Helper()
	var count int
	c.Assert(
		conn.QueryRowContext(
			c.Context(),
			"SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
			table,
		).Scan(&count),
		qt.IsNil,
	)
	return count == 1
}

// TestMigrateUp_AllowDirtyRetryReusesTheDirtyRevisionRow is the core #966
// contract on the native surface under the default per-file transaction mode.
//
// Reverted (the up path inserts unconditionally again), the retry fails with
// "failed to record pending migration 2: ... UNIQUE constraint failed:
// schema_migrations.version" and version 2 stays dirty forever.
func TestMigrateUp_AllowDirtyRetryReusesTheDirtyRevisionRow(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "retry.db")

	conn, m := newDirtyRetryMigrator(c, dbPath, dirtyRetryFailingUp, migrator.MigrationTxModeFile, migrator.RevisionTableFormatPtah)
	c.Assert(m.MigrateUp(c.Context()), qt.IsNotNil)

	// The per-file transaction rolled the body back, so nothing was applied and
	// the counter says so.
	revisions, err := m.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 2)
	c.Assert(revisions[1].Version, qt.Equals, int64(2))
	c.Assert(revisions[1].Dirty, qt.IsTrue)
	c.Assert(revisions[1].Applied, qt.Equals, 0)
	c.Assert(dirtyRetryUserCount(c, conn), qt.Equals, 0)

	// The operator fixes the migration and reruns with the escape hatch.
	_, fixed := newDirtyRetryMigrator(c, dbPath, dirtyRetryFixedUp, migrator.MigrationTxModeFile, migrator.RevisionTableFormatPtah)
	c.Assert(fixed.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)

	// One row per version, and version 2 is applied rather than duplicated.
	after, err := fixed.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(after, qt.HasLen, 2)
	c.Assert(after[1].Version, qt.Equals, int64(2))
	c.Assert(after[1].Dirty, qt.IsFalse)
	c.Assert(after[1].Applied, qt.Equals, 2)
	c.Assert(after[1].Total, qt.Equals, 2)
	// The rolled-back body ran in full, exactly once.
	c.Assert(dirtyRetryUserCount(c, conn), qt.Equals, 1)
	c.Assert(dirtyRetryTableExists(c, conn, "pets"), qt.IsTrue)

	status, err := fixed.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(2))
}

// TestMigrateUp_AllowDirtyRetryResumesAfterCommittedStatements covers the
// no-transaction case, where the earlier attempt's first statement is already
// committed when the retry starts.
//
// Reverted to a plain row reuse with no resume, the retry re-runs the committed
// INSERT and users holds 2 rows instead of 1; reverted entirely, the retry fails
// with "UNIQUE constraint failed: schema_migrations.version" as above.
func TestMigrateUp_AllowDirtyRetryResumesAfterCommittedStatements(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "resume.db")

	conn, m := newDirtyRetryMigrator(c, dbPath, dirtyRetryFailingUp, migrator.MigrationTxModeNone, migrator.RevisionTableFormatPtah)
	c.Assert(m.MigrateUp(c.Context()), qt.IsNotNil)

	// Statement 1 committed outside any transaction, and the counter says so.
	revisions, err := m.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 2)
	c.Assert(revisions[1].Applied, qt.Equals, 1)
	c.Assert(revisions[1].Total, qt.Equals, 2)
	c.Assert(dirtyRetryUserCount(c, conn), qt.Equals, 1)

	_, fixed := newDirtyRetryMigrator(c, dbPath, dirtyRetryFixedUp, migrator.MigrationTxModeNone, migrator.RevisionTableFormatPtah)
	c.Assert(fixed.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)

	// The committed INSERT was skipped, not repeated: one user, not two. The
	// primary key would have rejected a repeat, so this also proves the retry
	// did not merely swallow an error.
	c.Assert(dirtyRetryUserCount(c, conn), qt.Equals, 1)
	c.Assert(dirtyRetryTableExists(c, conn, "pets"), qt.IsTrue)

	after, err := fixed.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(after, qt.HasLen, 2)
	c.Assert(after[1].Dirty, qt.IsFalse)
	c.Assert(after[1].Applied, qt.Equals, 2)
}

// TestMigrateUp_AtlasFormatAllowDirtyRetryReusesTheDirtyRevisionRow is the same
// contract on the Atlas-shaped revision table that `ptah-compat migrate apply`
// writes, which registers --allow-dirty and no --skip-checks.
//
// Reverted, the retry fails with "UNIQUE constraint failed:
// atlas_schema_revisions.version".
func TestMigrateUp_AtlasFormatAllowDirtyRetryReusesTheDirtyRevisionRow(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "atlas-retry.db")

	conn, m := newDirtyRetryMigrator(c, dbPath, dirtyRetryFailingUp, migrator.MigrationTxModeNone, migrator.RevisionTableFormatAtlas)
	c.Assert(m.MigrateUp(c.Context()), qt.IsNotNil)

	_, fixed := newDirtyRetryMigrator(c, dbPath, dirtyRetryFixedUp, migrator.MigrationTxModeNone, migrator.RevisionTableFormatAtlas)
	c.Assert(fixed.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)

	var rows int
	c.Assert(
		conn.QueryRowContext(c.Context(), "SELECT count(*) FROM atlas_schema_revisions WHERE version = '2'").Scan(&rows),
		qt.IsNil,
	)
	c.Assert(rows, qt.Equals, 1)

	after, err := fixed.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(after, qt.HasLen, 2)
	c.Assert(after[1].Dirty, qt.IsFalse)
	c.Assert(after[1].Applied, qt.Equals, 2)
	c.Assert(dirtyRetryUserCount(c, conn), qt.Equals, 1)
}

// TestMigrateUp_AllowDirtyRefusesToResumeIntoARestructuredFile pins the one
// place this deliberately does not match the community Atlas binary, which
// resumes at applied+1 by index whatever the file now contains. Once the
// statement count has changed, that index no longer names the same statement,
// so ptah refuses and names the repair command instead of applying the wrong
// SQL.
//
// Reverted, the retry resumes at statement 2 of the restructured file, creates
// pets, marks version 2 applied, and the skipped statement never runs: the
// assertion that pets does not exist is what goes red.
func TestMigrateUp_AllowDirtyRefusesToResumeIntoARestructuredFile(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "restructured.db")

	conn, m := newDirtyRetryMigrator(c, dbPath, dirtyRetryFailingUp, migrator.MigrationTxModeNone, migrator.RevisionTableFormatPtah)
	c.Assert(m.MigrateUp(c.Context()), qt.IsNotNil)

	// The operator restructures the file: three statements now, so the recorded
	// "1 of 2 applied" no longer indexes into it.
	restructured := `CREATE TABLE audit (id INTEGER PRIMARY KEY);
INSERT INTO users (id) VALUES (1);
CREATE TABLE pets (id INTEGER PRIMARY KEY);
`
	_, retried := newDirtyRetryMigrator(c, dbPath, restructured, migrator.MigrationTxModeNone, migrator.RevisionTableFormatPtah)
	err := retried.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "migration 2 cannot resume automatically")
	c.Assert(err.Error(), qt.Contains, "applied 1 of 2 statements but the file now has 3")
	c.Assert(err.Error(), qt.Contains, "ptah migrations repair --version 2")
	// Nothing from the restructured file ran.
	c.Assert(dirtyRetryTableExists(c, conn, "audit"), qt.IsFalse)
	c.Assert(dirtyRetryTableExists(c, conn, "pets"), qt.IsFalse)
}

// TestMigrateUp_AllowDirtyRefusesToResumeAnUnknownStatementOutcome covers the
// interrupted-process row, where the recorded error says the last statement's
// fate is unknown. RepairMigration already refuses --resume-from on such a row;
// the automatic resume has to refuse for the same reason.
//
// Reverted, the retry silently resumes at statement 2 and reports success, so
// the assertion on the error goes red with a nil error.
func TestMigrateUp_AllowDirtyRefusesToResumeAnUnknownStatementOutcome(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "unknown.db")

	conn, m := newDirtyRetryMigrator(c, dbPath, dirtyRetryFailingUp, migrator.MigrationTxModeNone, migrator.RevisionTableFormatPtah)
	c.Assert(m.MigrateUp(c.Context()), qt.IsNotNil)

	// Model a process killed mid-statement: the row keeps its progress but the
	// error records that the statement's outcome was never observed.
	_, err := conn.ExecContext(
		c.Context(),
		"UPDATE schema_migrations SET error = ? WHERE version = 2",
		"statement execution outcome is unknown after process interruption",
	)
	c.Assert(err, qt.IsNil)

	_, retried := newDirtyRetryMigrator(c, dbPath, dirtyRetryFixedUp, migrator.MigrationTxModeNone, migrator.RevisionTableFormatPtah)
	err = retried.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "migration 2 cannot resume automatically")
	c.Assert(err.Error(), qt.Contains, "the outcome of statement 2 is unknown")
	c.Assert(dirtyRetryTableExists(c, conn, "pets"), qt.IsFalse)
}

// TestMigrateUp_TxModeAllAllowDirtyRetryReusesTheDirtyRevisionRow covers the
// third transaction mode, whose bookkeeping goes through its own callers
// (recordRolledBackBatchFailure and recordAppliedMigrationOn) rather than the
// per-file path the tests above exercise.
//
// Reverted, the retry fails with "failed to record pending migration 2: ...
// UNIQUE constraint failed: schema_migrations.version" exactly as the per-file
// path did.
func TestMigrateUp_TxModeAllAllowDirtyRetryReusesTheDirtyRevisionRow(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "txall.db")

	conn, m := newDirtyRetryMigrator(c, dbPath, dirtyRetryFailingUp, migrator.MigrationTxModeAll, migrator.RevisionTableFormatPtah)
	c.Assert(m.MigrateUp(c.Context()), qt.IsNotNil)

	// The batch rolled back whole: version 1 has no row, version 2 has a dirty
	// one recording that nothing was applied.
	revisions, err := m.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 1)
	c.Assert(revisions[0].Version, qt.Equals, int64(2))
	c.Assert(revisions[0].Applied, qt.Equals, 0)
	c.Assert(dirtyRetryTableExists(c, conn, "users"), qt.IsFalse)

	_, fixed := newDirtyRetryMigrator(c, dbPath, dirtyRetryFixedUp, migrator.MigrationTxModeAll, migrator.RevisionTableFormatPtah)
	c.Assert(fixed.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)

	after, err := fixed.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(after, qt.HasLen, 2)
	c.Assert(after[1].Version, qt.Equals, int64(2))
	c.Assert(after[1].Dirty, qt.IsFalse)
	c.Assert(after[1].Applied, qt.Equals, 2)
	c.Assert(dirtyRetryUserCount(c, conn), qt.Equals, 1)
	c.Assert(dirtyRetryTableExists(c, conn, "pets"), qt.IsTrue)
}

// newRegisteredDirtyRetryMigrator builds a migrator over programmatically
// registered SQL, which executes through [executeSQLStatements] rather than the
// file provider's loop. The two loops are separate gates: a resume floor
// honored by only one of them looks fixed from every file-based test.
func newRegisteredDirtyRetryMigrator(
	c *qt.C,
	dbPath string,
	secondUp string,
) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })

	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "users", dirtyRetryFirstUp, "DROP TABLE users;\n"),
		migrator.CreateMigrationFromSQL(2, "seed", secondUp, "DROP TABLE pets;\n"),
	)).WithTransactionMode(migrator.MigrationTxModeNone)
	c.Assert(m.Initialize(c.Context()), qt.IsNil)
	return conn, m
}

// TestMigrateUp_RegisteredMigrationAllowDirtyRetryResumes is the same resume
// contract on the registered-migration path.
//
// Reverted only in [executeSQLStatements] — the mutation every file-based test
// above survives — the committed INSERT runs a second time and the primary key
// rejects it, so the retry fails with "UNIQUE constraint failed: users.id".
func TestMigrateUp_RegisteredMigrationAllowDirtyRetryResumes(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "registered.db")

	conn, m := newRegisteredDirtyRetryMigrator(c, dbPath, dirtyRetryFailingUp)
	c.Assert(m.MigrateUp(c.Context()), qt.IsNotNil)
	c.Assert(dirtyRetryUserCount(c, conn), qt.Equals, 1)

	_, fixed := newRegisteredDirtyRetryMigrator(c, dbPath, dirtyRetryFixedUp)
	c.Assert(fixed.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)

	c.Assert(dirtyRetryUserCount(c, conn), qt.Equals, 1)
	c.Assert(dirtyRetryTableExists(c, conn, "pets"), qt.IsTrue)
}

// TestMigrateUp_FirstAttemptRecordsOneRowPerVersion is the non-interference
// control for the reuse path, and it cannot be proved by reverting the change:
// removing a reuse branch never makes a first attempt stop inserting. It is
// aimed at the INVERSE mutant, "always rewrite instead of inserting", under
// which a first attempt updates zero rows, the revision table stays empty, and
// the assertions on the recorded revisions and the reported current version go
// red while the schema itself still looks right.
func TestMigrateUp_FirstAttemptRecordsOneRowPerVersion(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "first.db")

	conn, m := newDirtyRetryMigrator(c, dbPath, dirtyRetryFixedUp, migrator.MigrationTxModeFile, migrator.RevisionTableFormatPtah)
	c.Assert(m.MigrateUp(c.Context()), qt.IsNil)

	revisions, err := m.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 2)
	c.Assert(revisions[0].Version, qt.Equals, int64(1))
	c.Assert(revisions[1].Version, qt.Equals, int64(2))
	c.Assert(revisions[1].Applied, qt.Equals, 2)
	c.Assert(revisions[1].Dirty, qt.IsFalse)

	status, err := m.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(2))
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(dirtyRetryUserCount(c, conn), qt.Equals, 1)
	c.Assert(dirtyRetryTableExists(c, conn, "pets"), qt.IsTrue)
}

// TestMigrateUp_DirtyGuardStillRefusesWithoutAllowDirty keeps the escape hatch
// an escape hatch: reusing the row is what --allow-dirty now does, not what
// every run does.
//
// Reverted, this test passes unchanged — it is here so a later change that
// makes the retry automatic has to say so out loud.
func TestMigrateUp_DirtyGuardStillRefusesWithoutAllowDirty(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "guard.db")

	_, m := newDirtyRetryMigrator(c, dbPath, dirtyRetryFailingUp, migrator.MigrationTxModeFile, migrator.RevisionTableFormatPtah)
	c.Assert(m.MigrateUp(c.Context()), qt.IsNotNil)

	_, fixed := newDirtyRetryMigrator(c, dbPath, dirtyRetryFixedUp, migrator.MigrationTxModeFile, migrator.RevisionTableFormatPtah)
	err := fixed.MigrateUp(context.Background())

	c.Assert(err, qt.IsNotNil)
	c.Assert(migrator.IsDirtyMigration(err), qt.IsTrue)
}

func TestMigrateUp_AllowDirtyRefusesInterruptedRollback(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(t.TempDir(), "down-direction.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })

	migration := migrator.CreateMigrationFromSQL(
		1,
		"direction guard",
		"CREATE TABLE parent (id INTEGER PRIMARY KEY);\nCREATE TABLE child (id INTEGER PRIMARY KEY);\n",
		"-- +ptah no_transaction\nDROP TABLE child;\nDROP TABLE definitely_missing;\n",
	)
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration))
	c.Assert(m.MigrateUp(c.Context()), qt.IsNil)
	c.Assert(m.MigrateDownTo(c.Context(), 0), qt.IsNotNil)

	before, err := m.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(before.DirtyRevision, qt.IsNotNil)
	c.Assert(before.DirtyRevision.Direction, qt.Equals, migrator.MigrationDirectionDown)
	c.Assert(before.DirtyRevision.Applied, qt.Equals, 1)
	c.Assert(dirtyRetryTableExists(c, conn, "parent"), qt.IsTrue)
	c.Assert(dirtyRetryTableExists(c, conn, "child"), qt.IsFalse)

	err = m.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true})

	c.Assert(err, qt.ErrorMatches, "migration 1 is dirty from an interrupted rollback; repair the rollback before migrating up")
	after, err := m.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(after.DirtyRevision, qt.IsNotNil)
	c.Assert(after.DirtyRevision.Direction, qt.Equals, migrator.MigrationDirectionDown)
	c.Assert(after.DirtyRevision.Applied, qt.Equals, 1)
	c.Assert(dirtyRetryTableExists(c, conn, "parent"), qt.IsTrue)
	c.Assert(dirtyRetryTableExists(c, conn, "child"), qt.IsFalse)
}

func TestMigrateUp_AllowDirtyResumeSkipsObsoletePreMigrationChecks(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "resume-check.db")

	failing := `-- +ptah check name="users_empty" assert="SELECT count(*) = 0 FROM users"
-- +ptah no_transaction
INSERT INTO users (id) VALUES (1);
INSERT INTO definitely_missing (id) VALUES (1);
`
	conn, m := newDirtyRetryMigrator(c, dbPath, failing, migrator.MigrationTxModeFile, migrator.RevisionTableFormatPtah)
	c.Assert(m.MigrateUp(c.Context()), qt.IsNotNil)
	c.Assert(dirtyRetryUserCount(c, conn), qt.Equals, 1)

	fixed := `-- +ptah check name="users_empty" assert="SELECT count(*) = 0 FROM users"
-- +ptah no_transaction
INSERT INTO users (id) VALUES (1);
CREATE TABLE pets (id INTEGER PRIMARY KEY);
`
	_, retried := newDirtyRetryMigrator(c, dbPath, fixed, migrator.MigrationTxModeFile, migrator.RevisionTableFormatPtah)
	c.Assert(retried.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)

	c.Assert(dirtyRetryUserCount(c, conn), qt.Equals, 1)
	c.Assert(dirtyRetryTableExists(c, conn, "pets"), qt.IsTrue)
	status, err := retried.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
}

func TestRepairMigration_UpResumePersistsAbsoluteProgress(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(t.TempDir(), "manual-resume.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })

	initial := migrator.CreateMigrationFromSQL(
		1,
		"manual resume",
		"-- +ptah no_transaction\nCREATE TABLE first (id INTEGER PRIMARY KEY);\nINSERT INTO missing_one (id) VALUES (1);\nCREATE TABLE third (id INTEGER PRIMARY KEY);\n",
		"DROP TABLE third;\nDROP TABLE first;\n",
	)
	c.Assert(migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(initial)).MigrateUp(c.Context()), qt.IsNotNil)

	secondFailure := migrator.CreateMigrationFromSQL(
		1,
		"manual resume",
		"-- +ptah no_transaction\nCREATE TABLE first (id INTEGER PRIMARY KEY);\nCREATE TABLE second (id INTEGER PRIMARY KEY);\nINSERT INTO missing_two (id) VALUES (1);\n",
		"DROP TABLE second;\nDROP TABLE first;\n",
	)
	secondAttempt := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(secondFailure))
	err = secondAttempt.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 2})
	c.Assert(err, qt.ErrorMatches, `(?s)failed to resume migration 1: failed to execute SQL statement: .*missing_two.*`)

	revisions, err := secondAttempt.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 1)
	c.Assert(revisions[0].Direction, qt.Equals, migrator.MigrationDirectionUp)
	c.Assert(revisions[0].Applied, qt.Equals, 2)
	c.Assert(revisions[0].Total, qt.Equals, 3)
	c.Assert(revisions[0].ErrorStatement, qt.Contains, "missing_two")
	c.Assert(dirtyRetryTableExists(c, conn, "second"), qt.IsTrue)

	completed := migrator.CreateMigrationFromSQL(
		1,
		"manual resume",
		"-- +ptah no_transaction\nCREATE TABLE first (id INTEGER PRIMARY KEY);\nCREATE TABLE second (id INTEGER PRIMARY KEY);\nCREATE TABLE third (id INTEGER PRIMARY KEY);\n",
		"DROP TABLE third;\nDROP TABLE second;\nDROP TABLE first;\n",
	)
	finalAttempt := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(completed))
	c.Assert(finalAttempt.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 3}), qt.IsNil)

	status, err := finalAttempt.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))
	c.Assert(dirtyRetryTableExists(c, conn, "third"), qt.IsTrue)
}
