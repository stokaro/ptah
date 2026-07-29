package atlasmigrate

// White-box testing required: filesystem publication races and post-replay
// fault injection cannot be staged deterministically through the exported
// GenerateDiff API.

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/atlassource"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/internal/migrationreplay"
	"github.com/stokaro/ptah/internal/migrationsnapshot"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestWriteMigrationFilesAt_CollisionRejectsStalePlan(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	// A non-cooperating writer claimed version 2 after this run allocated 1..2.
	// Publishing the stale plan must fail instead of silently moving it above
	// a migration that was never replayed.
	c.Assert(os.WriteFile(filepath.Join(dir, "2_add_email_concurrent_indexes.sql"), []byte("taken"), 0o600), qt.IsNil)
	contents := []MigrationFileContent{
		{NameSuffix: "_transactional", SQL: "SELECT 1;"},
		{NameSuffix: "_concurrent_indexes", SQL: "SELECT 2;", NoTransaction: true},
	}

	paths, err := writeMigrationFilesAt(dir, "add_email", 1, contents)

	c.Assert(err, qt.ErrorMatches, `migration directory changed during publication: .*2_add_email_concurrent_indexes\.sql already exists`)
	c.Assert(paths, qt.HasLen, 0)
	// The colliding file kept its content and no version-1 leftover remains
	// from the aborted attempt.
	taken, readErr := os.ReadFile(filepath.Join(dir, "2_add_email_concurrent_indexes.sql"))
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(taken), qt.Equals, "taken")
	_, leftoverErr := os.Stat(filepath.Join(dir, "1_add_email_transactional.sql"))
	c.Assert(leftoverErr, qt.ErrorIs, os.ErrNotExist)
}

func TestWriteMigrationFiles_FailedWriteRollsBackEarlierFiles(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	// The second file's name exceeds the filesystem's name limit, so its
	// create fails with a non-collision error after the first file was
	// already written; the batch rolls back completely.
	oversizedSuffix := "_" + strings.Repeat("x", 512)

	paths, err := writeMigrationFiles(dir, "add_email", []MigrationFileContent{
		{NameSuffix: "_transactional", SQL: "SELECT 1;"},
		{NameSuffix: oversizedSuffix, SQL: "SELECT 2;", NoTransaction: true},
	})

	c.Assert(err, qt.ErrorMatches, `publish migration file: .*`)
	c.Assert(paths, qt.HasLen, 0)
	_, leftoverErr := os.Stat(filepath.Join(dir, "1_add_email_transactional.sql"))
	c.Assert(leftoverErr, qt.ErrorIs, os.ErrNotExist)
}

func TestWriteDiffArtifacts_SumPublishFailureRollsBackMigrations(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.Mkdir(filepath.Join(dir, "atlas.sum"), 0o700), qt.IsNil)
	baseSnapshot, err := migrationsnapshot.CaptureStable(os.DirFS(dir))
	c.Assert(err, qt.IsNil)

	result, err := writeDiffArtifacts(
		t.Context(),
		dir,
		"add_email",
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
		baseSnapshot,
	)

	c.Assert(err, qt.ErrorMatches, `write atlas\.sum: .*`)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	entries, readErr := os.ReadDir(dir)
	c.Assert(readErr, qt.IsNil)
	c.Assert(entries, qt.HasLen, 1)
	c.Assert(entries[0].Name(), qt.Equals, "atlas.sum")
	c.Assert(entries[0].IsDir(), qt.IsTrue)
}

func TestRecoverPendingPublication_RollsBackInterruptedBatch(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	initialPath := filepath.Join(dir, "1_initial.sql")
	c.Assert(os.WriteFile(initialPath, []byte("SELECT 1;"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	batch, err := stageMigrationBatchAt(
		dir,
		"interrupted",
		2,
		[]MigrationFileContent{{SQL: "SELECT 2;"}},
	)
	c.Assert(err, qt.IsNil)
	_, _ = beginTestPublication(
		c,
		dir,
		batch,
		[]MigrationFileContent{{SQL: "SELECT 2;"}},
	)
	published, err := publishMigrationBatch(dir, batch)
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 1)

	c.Assert(recoverPendingPublication(dir), qt.IsNil)

	_, err = os.Stat(batch.paths[0])
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	_, err = os.Stat(batch.stagedPaths[0])
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	journalPath, err := publicationJournalPath(dir)
	c.Assert(err, qt.IsNil)
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	result, err := migratesum.VerifyWithFormat(os.DirFS(dir), migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(result.OK(), qt.IsTrue)
}

func TestRecoverPendingPublication_FinalizesCommittedBatch(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	batch, err := stageMigrationBatchAt(
		dir,
		"committed",
		1,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	c.Assert(err, qt.IsNil)
	journal, sum := beginTestPublication(
		c,
		dir,
		batch,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	published, err := publishMigrationBatch(dir, batch)
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 1)
	_, err = writeDirSum(dir, sum)
	c.Assert(err, qt.IsNil)
	committed, err := publicationCommitted(dir, journal)
	c.Assert(err, qt.IsNil)
	c.Assert(committed, qt.IsTrue)

	c.Assert(recoverPendingPublication(dir), qt.IsNil)

	contents, err := os.ReadFile(batch.paths[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "SELECT 1;")
	_, err = os.Stat(batch.stagedPaths[0])
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	journalPath, err := publicationJournalPath(dir)
	c.Assert(err, qt.IsNil)
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	result, err := migratesum.VerifyWithFormat(os.DirFS(dir), migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(result.OK(), qt.IsTrue)
}

func TestRecoverPendingPublication_RejectsForeignCollision(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	batch, err := stageMigrationBatchAt(
		dir,
		"collision",
		1,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	c.Assert(err, qt.IsNil)
	_, _ = beginTestPublication(
		c,
		dir,
		batch,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	c.Assert(os.WriteFile(batch.paths[0], []byte("foreign"), 0o600), qt.IsNil)

	err = recoverPendingPublication(dir)

	c.Assert(err, qt.ErrorMatches, `cannot safely recover migration publication: .* is not the journaled staging file; preserved at .*`)
	_, err = os.Stat(batch.paths[0])
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	contents, err := os.ReadFile(filepath.Join(batch.stagedPaths[0]+".rollback", "published"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "foreign")
	_, err = os.Stat(batch.stagedPaths[0])
	c.Assert(err, qt.IsNil)
	journalPath, err := publicationJournalPath(dir)
	c.Assert(err, qt.IsNil)
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.IsNil)
}

func TestAbortPendingPublication_RejectsForeignReplacement(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	batch, err := stageMigrationBatchAt(
		dir,
		"collision",
		1,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	c.Assert(err, qt.IsNil)
	_, _ = beginTestPublication(
		c,
		dir,
		batch,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	published, err := publishMigrationBatch(dir, batch)
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 1)
	c.Assert(os.Remove(batch.paths[0]), qt.IsNil)
	c.Assert(os.WriteFile(batch.paths[0], []byte("foreign"), 0o600), qt.IsNil)

	err = abortPendingPublication(dir, batch, published)

	c.Assert(err, qt.ErrorMatches, `roll back published migration files: cannot safely recover migration publication: .* is not the journaled staging file; preserved at .*`)
	_, err = os.Stat(batch.paths[0])
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	contents, err := os.ReadFile(filepath.Join(batch.stagedPaths[0]+".rollback", "published"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "foreign")
	_, err = os.Stat(batch.stagedPaths[0])
	c.Assert(err, qt.IsNil)
	journalPath, err := publicationJournalPath(dir)
	c.Assert(err, qt.IsNil)
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.IsNil)
}

func TestWriteDiffArtifacts_CommitUncertainRetainsRecoverableBatch(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	baseSnapshot, err := migrationsnapshot.CaptureStable(os.DirFS(dir))
	c.Assert(err, qt.IsNil)

	result, err := writeDiffArtifactsWithSumWriter(
		t.Context(),
		dir,
		"uncertain",
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
		baseSnapshot,
		func(dir string, sum *migratesum.SumFile) (string, error) {
			path, writeErr := writeDirSum(dir, sum)
			c.Assert(writeErr, qt.IsNil)
			return path, &migratesum.CommitUncertainError{
				Err: errors.New("injected directory sync failure"),
			}
		},
	)

	c.Assert(err, qt.ErrorMatches, `write atlas\.sum; migration publication journal retained for recovery: injected directory sync failure`)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	sqlFiles, err := filepath.Glob(filepath.Join(dir, "*_uncertain.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(sqlFiles, qt.HasLen, 1)
	stagedFiles, err := filepath.Glob(filepath.Join(dir, stagedMigrationPattern))
	c.Assert(err, qt.IsNil)
	c.Assert(stagedFiles, qt.HasLen, 1)
	journalPath, err := publicationJournalPath(dir)
	c.Assert(err, qt.IsNil)
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.IsNil)

	c.Assert(recoverPendingPublication(dir), qt.IsNil)
	sqlFiles, err = filepath.Glob(filepath.Join(dir, "*_uncertain.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(sqlFiles, qt.HasLen, 1)
	stagedFiles, err = filepath.Glob(filepath.Join(dir, stagedMigrationPattern))
	c.Assert(err, qt.IsNil)
	c.Assert(stagedFiles, qt.HasLen, 0)
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	verifyResult, err := migratesum.VerifyWithFormat(
		os.DirFS(dir),
		migrator.MigrationDirFormatAtlas,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(verifyResult.OK(), qt.IsTrue)
}

func TestWriteDiffArtifacts_RejectsUnreplayedConcurrentMigration(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	baseSnapshot, err := migrationsnapshot.CaptureStable(os.DirFS(dir))
	c.Assert(err, qt.IsNil)
	foreignPath := filepath.Join(dir, "99_foreign.sql")
	c.Assert(os.WriteFile(foreignPath, []byte("SELECT 99;"), 0o600), qt.IsNil)

	result, err := writeDiffArtifacts(
		t.Context(),
		dir,
		"planned",
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
		baseSnapshot,
	)

	c.Assert(err, qt.ErrorMatches, `migration directory changed during migrate diff planning`)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	contents, err := os.ReadFile(foreignPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "SELECT 99;")
	plannedFiles, err := filepath.Glob(filepath.Join(dir, "*_planned.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(plannedFiles, qt.HasLen, 0)
	journalPath, err := publicationJournalPath(dir)
	c.Assert(err, qt.IsNil)
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func beginTestPublication(
	c *qt.C,
	dir string,
	batch migrationBatch,
	contents []MigrationFileContent,
) (publicationJournal, *migratesum.SumFile) {
	c.Helper()
	baseSnapshot, err := migrationsnapshot.CaptureStable(os.DirFS(dir))
	c.Assert(err, qt.IsNil)
	_, sum, err := preparePublicationSnapshot(baseSnapshot, batch, contents)
	c.Assert(err, qt.IsNil)
	journal, err := beginPublication(dir, batch, sum.Bytes())
	c.Assert(err, qt.IsNil)
	return journal, sum
}

func TestRecoverPendingPublication_RemovesOrphanTemps(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	stagedPath, err := stageMigrationFile(dir, "SELECT 1;")
	c.Assert(err, qt.IsNil)
	journalPath, err := publicationJournalPath(dir)
	c.Assert(err, qt.IsNil)
	journalTemp, err := os.CreateTemp(
		filepath.Dir(journalPath),
		filepath.Base(journalPath)+".*.tmp",
	)
	c.Assert(err, qt.IsNil)
	journalTempPath := journalTemp.Name()
	c.Assert(journalTemp.Close(), qt.IsNil)

	c.Assert(recoverPendingPublication(dir), qt.IsNil)

	_, err = os.Stat(stagedPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	_, err = os.Stat(journalTempPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func TestWriteMigrationFiles_EmptySQLIsRejected(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()

	paths, err := writeMigrationFiles(dir, "noop", []MigrationFileContent{{SQL: "   \n"}})

	c.Assert(err, qt.ErrorMatches, `migration SQL is empty`)
	c.Assert(paths, qt.HasLen, 0)
}

func TestGenerateDiff_PostReplayReadFailureCleansAndReleasesLock(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	readErr := errors.New("injected schema read failure")

	result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
		readDevSchema: func(
			*dbschema.DatabaseConnection,
			[]string,
			string,
		) (*dbschematypes.DBSchema, error) {
			return nil, readErr
		},
		withReplayedSnapshot: migrationreplay.WithReplayedSnapshotLocked,
	})

	c.Assert(err, qt.ErrorIs, readErr)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	assertSQLiteCleanupObjectCount(c, conn, 0)
	assertDiffDirectoryReleased(c, opts.Dir)
}

func TestGenerateDiff_FinalCleanupFailureIsNotRetried(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	cleanupErr := errors.New("injected final cleanup failure")
	cleanupCalls := 0

	result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
		readDevSchema: readScopedDevSchema,
		withReplayedSnapshot: func(
			ctx context.Context,
			conn *dbschema.DatabaseConnection,
			snapshot fs.FS,
			format migrator.MigrationDirFormat,
			consume func(*dbschema.DatabaseConnection) error,
		) error {
			cleanupCalls++
			return errors.Join(
				migrationreplay.WithReplayedSnapshotLocked(ctx, conn, snapshot, format, consume),
				cleanupErr,
			)
		},
	})

	c.Assert(err, qt.ErrorIs, cleanupErr)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(cleanupCalls, qt.Equals, 1)
	assertSQLiteCleanupObjectCount(c, conn, 0)
	assertDiffDirectoryReleased(c, opts.Dir)
}

func TestGenerateDiff_JoinsPostReplayFailureAndCleanupFailure(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	readErr := errors.New("injected schema read failure")
	cleanupErr := errors.New("injected cleanup failure")
	cleanupCalls := 0

	result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
		readDevSchema: func(
			*dbschema.DatabaseConnection,
			[]string,
			string,
		) (*dbschematypes.DBSchema, error) {
			return nil, readErr
		},
		withReplayedSnapshot: func(
			ctx context.Context,
			conn *dbschema.DatabaseConnection,
			snapshot fs.FS,
			format migrator.MigrationDirFormat,
			consume func(*dbschema.DatabaseConnection) error,
		) error {
			cleanupCalls++
			return errors.Join(
				migrationreplay.WithReplayedSnapshotLocked(ctx, conn, snapshot, format, consume),
				cleanupErr,
			)
		},
	})

	c.Assert(err, qt.ErrorIs, readErr)
	c.Assert(err, qt.ErrorIs, cleanupErr)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(cleanupCalls, qt.Equals, 1)
	assertSQLiteCleanupObjectCount(c, conn, 0)
	assertDiffDirectoryReleased(c, opts.Dir)
}

func TestGenerateDiff_CancellationDuringCleanupPreventsArtifacts(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	ctx, cancel := context.WithCancel(t.Context())
	cleanupCalls := 0

	result, err := generateDiff(ctx, conn, opts, diffRuntime{
		readDevSchema: readScopedDevSchema,
		withReplayedSnapshot: func(
			replayCtx context.Context,
			replayConn *dbschema.DatabaseConnection,
			snapshot fs.FS,
			format migrator.MigrationDirFormat,
			consume func(*dbschema.DatabaseConnection) error,
		) error {
			cleanupCalls++
			replayErr := migrationreplay.WithReplayedSnapshotLocked(
				replayCtx,
				replayConn,
				snapshot,
				format,
				consume,
			)
			cancel()
			return replayErr
		},
	})

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(cleanupCalls, qt.Equals, 1)
	assertSQLiteCleanupObjectCount(c, conn, 0)
	assertDiffDirectoryReleased(c, opts.Dir)
}

func TestCanonicalMigrationDirResolvesSymlinkAlias(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	realDir := filepath.Join(root, "real", "migrations")
	c.Assert(os.MkdirAll(realDir, 0o755), qt.IsNil)
	aliasDir := filepath.Join(root, "alias")
	c.Assert(os.Symlink(realDir, aliasDir), qt.IsNil)

	canonicalReal, err := canonicalMigrationDir(realDir)
	c.Assert(err, qt.IsNil)
	canonicalAlias, err := canonicalMigrationDir(aliasDir)
	c.Assert(err, qt.IsNil)

	c.Assert(canonicalAlias, qt.Equals, canonicalReal)
	c.Assert(migrationDirLockPath(canonicalAlias), qt.Equals, migrationDirLockPath(canonicalReal))
}

func TestTryAcquireDirLock_ReclaimsStaleFile(t *testing.T) {
	c := qt.New(t)
	lockPath := filepath.Join(c.TempDir(), ".migrations"+lockFileName)
	c.Assert(os.WriteFile(lockPath, []byte("stale"), 0o600), qt.IsNil)

	lock, err := tryAcquireDirLock(lockPath)
	c.Assert(err, qt.IsNil)
	c.Assert(lock.release(), qt.IsNil)

	_, err = os.Stat(lockPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func TestGenerateDiff_SerializesSQLiteDevDatabaseAcrossDirectories(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	opts.Dir = filepath.Join(c.TempDir(), "other-migrations")
	opts.LockTimeout = time.Millisecond
	devLock, err := acquireDevDatabaseLock(t.Context(), conn, 0)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(devLock.release(), qt.IsNil)
	})

	result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
		readDevSchema:        readScopedDevSchema,
		withReplayedSnapshot: migrationreplay.WithReplayedSnapshotLocked,
	})

	c.Assert(err, qt.ErrorMatches, `acquire migrate diff dev database lock: acquire sqlite dev database realm lock: lock timeout after 1ms`)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	_, err = os.Stat(opts.Dir)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func prepareGenerateDiffFaultTest(c *qt.C) (*dbschema.DatabaseConnection, DiffOptions) {
	c.Helper()
	dir := c.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "1_init.sql"),
		[]byte(`
CREATE TABLE replayed_users (id INTEGER PRIMARY KEY);
CREATE VIEW replayed_user_ids AS SELECT id FROM replayed_users;
`),
		0o600,
	), qt.IsNil)
	_, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(
		schemaPath,
		[]byte("CREATE TABLE desired_users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	desired, err := atlassource.ClassifySet(
		"--to",
		[]string{"file://" + schemaPath},
		atlassource.ProjectEnv{},
	)
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(dir, "dev.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
	})
	return conn, DiffOptions{
		Dir:     migrationsDir,
		Desired: desired,
		Name:    "fault_injection",
	}
}

func assertSQLiteCleanupObjectCount(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	want int,
) {
	c.Helper()
	var count int
	err := conn.QueryRowContext(c.Context(), `
		SELECT COUNT(*)
		FROM pragma_table_list
		WHERE schema = ?
		  AND type IN ('table', 'view', 'virtual')
		  AND name NOT LIKE 'sqlite_%'
		  AND name <> 'schema_migrations'
	`, "main").Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, want)
}

func assertDiffDirectoryReleased(c *qt.C, dir string) {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 2)
	_, err = os.Stat(migrationDirLockPath(dir))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}
