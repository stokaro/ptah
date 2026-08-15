//go:build integration

package gonative_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

const (
	concurrentWriterTable   = "ptah_issue997_orders"
	concurrentWriterIndex   = "idx_ptah_issue997_orders_email"
	concurrentWriterTracker = "schema_migrations_issue_997_writer"
	concurrentWriterEmail   = "rival@example.com"
	concurrentWriterVersion = int64(1786561790)
	concurrentWriterSeedID  = 900001
	concurrentWriterRivalID = 900002
)

// TestPostgreSQLConcurrentWriterLeavesInvalidUniqueIndexIntegration is the
// concurrent-writer case acceptance line 6 of stokaro/ptah#997 names, and it
// reaches the invalid-index state of lines 1 and 2 the way production reaches
// it: through another session, not through data seeded before the run.
//
// The distinction is the whole point of having this test alongside the
// pre-seeded ones. A build that fails on data already present never had a
// chance of succeeding; the case operators actually hit is a build that was
// viable when it started and is invalidated by a write that lands while it
// runs. PostgreSQL makes that deterministic: CREATE INDEX CONCURRENTLY waits
// for every transaction holding a conflicting lock on the table before it
// scans, so a writer that inserts and stays open pins the build in
// `waiting for writers before build` for as long as the test needs. The build
// is genuinely waiting on this writer and not merely running beside it, and
// pg_stat_progress_create_index.current_locker_pid naming the writer's own
// backend is what says so -- a read no test in this repository made before.
//
// Everything after that is catalog, not text: the leftover is condemned by
// pg_index.indisvalid and indisready, and the unique constraint the migration
// was supposed to install is proved absent and then present by attempting the
// duplicate write itself.
func TestPostgreSQLConcurrentWriterLeavesInvalidUniqueIndexIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openConcurrentWriterDB(c.TB, dsn)
	seedConcurrentWriterTable(c.TB, db)

	conn, err := dbschema.ConnectToDatabase(ctx, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := concurrentWriterMigrator(conn)

	// The rival holds its INSERT open, so the build cannot get past its wait.
	writer := beginConcurrentWriter(c.TB, db)
	writerPID := concurrentWriterBackendPID(c.TB, ctx, writer)

	// The channel is closed after the send so the cleanup below can wait for
	// the build on the path where an assertion fails before it is read, and
	// still return at once on the path where it was.
	migrated := make(chan error, 1)
	go func() {
		migrated <- mig.MigrateUp(ctx)
		close(migrated)
	}()
	c.Cleanup(func() {
		_, _ = writer.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		<-migrated
	})

	// The build is waiting, and it is waiting on this writer.
	progress := awaitConcurrentIndexLocker(c.TB, db)
	c.Assert(progress.currentLockerPID, qt.Equals, writerPID)
	c.Assert(progress.phase, qt.Contains, "waiting for writers")
	c.Assert(progress.lockersTotal, qt.Equals, int64(1))
	c.Assert(progress.lockersDone, qt.Equals, int64(0))

	// Releasing the rival is what breaks the build: its row is now visible to
	// the scan the build was waiting to start.
	_, err = writer.ExecContext(ctx, "COMMIT")
	c.Assert(err, qt.IsNil)
	c.Assert(<-migrated, qt.ErrorMatches, "(?s).*could not create unique index.*")

	valid, ready := concurrentWriterIndexFlags(c.TB, db)
	c.Assert(valid, qt.IsFalse)
	c.Assert(ready, qt.IsFalse)
	c.Assert(concurrentWriterDuplicateInsert(ctx, db), qt.IsNil)
	c.Assert(concurrentWriterDeleteDuplicates(ctx, db), qt.IsNil)

	// Repair refuses over the residue, names the catalog flags that condemn it,
	// and names the command that rebuilds it without holding writes.
	err = mig.RepairMigration(ctx, migrator.RepairMigrationOptions{
		Version:    concurrentWriterVersion,
		ResumeFrom: 1,
	})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `"public"."`+concurrentWriterIndex+`"`)
	c.Assert(err.Error(), qt.Contains, "indisvalid=false, indisready=false")
	c.Assert(err.Error(), qt.Contains, "REINDEX INDEX CONCURRENTLY")

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Version, qt.Equals, concurrentWriterVersion)
	c.Assert(status.AppliedMigrations, qt.HasLen, 0)

	// The escape hatch the refusal names is the one that works.
	_, err = db.ExecContext(ctx, `REINDEX INDEX CONCURRENTLY "public"."`+concurrentWriterIndex+`"`)
	c.Assert(err, qt.IsNil)
	valid, ready = concurrentWriterIndexFlags(c.TB, db)
	c.Assert(valid, qt.IsTrue)
	c.Assert(ready, qt.IsTrue)

	c.Assert(mig.RepairMigration(ctx, migrator.RepairMigrationOptions{
		Version:    concurrentWriterVersion,
		ResumeFrom: 1,
	}), qt.IsNil)
	status, err = mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{concurrentWriterVersion})
	c.Assert(concurrentWriterDuplicateInsert(ctx, db), qt.IsNotNil)
}

// concurrentIndexProgress is one row of pg_stat_progress_create_index.
type concurrentIndexProgress struct {
	phase            string
	currentLockerPID int64
	lockersTotal     int64
	lockersDone      int64
}

// awaitConcurrentIndexLocker polls until PostgreSQL reports the in-flight
// index build blocked behind a specific backend, and returns that observation.
//
// It returns the zero value rather than failing on its own, so a build that
// never waits is reported by the caller's assertions -- which name the values
// they wanted -- instead of by a bare timeout message.
func awaitConcurrentIndexLocker(tb testing.TB, db *sql.DB) concurrentIndexProgress {
	c := qt.New(tb)
	c.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		var progress concurrentIndexProgress
		err := db.QueryRowContext(c.Context(), `
			SELECT progress.phase,
			       progress.current_locker_pid,
			       progress.lockers_total,
			       progress.lockers_done
			FROM pg_stat_progress_create_index AS progress
			JOIN pg_class AS target ON target.oid = progress.relid
			JOIN pg_namespace AS namespace ON namespace.oid = target.relnamespace
			WHERE namespace.nspname = current_schema()
			  AND target.relname = $1`,
			concurrentWriterTable,
		).Scan(
			&progress.phase,
			&progress.currentLockerPID,
			&progress.lockersTotal,
			&progress.lockersDone,
		)
		if err == nil && progress.currentLockerPID != 0 {
			return progress
		}
		time.Sleep(20 * time.Millisecond)
	}
	return concurrentIndexProgress{}
}

// beginConcurrentWriter opens a rival session and leaves an INSERT uncommitted
// on it.
//
// It is a pinned connection rather than a pooled handle because the whole
// arrangement depends on one backend holding one transaction open: a pool is
// free to run the COMMIT on a different connection from the INSERT, which
// would release nothing and leave the build waiting forever.
func beginConcurrentWriter(tb testing.TB, db *sql.DB) *sql.Conn {
	c := qt.New(tb)
	c.Helper()
	conn, err := db.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	_, err = conn.ExecContext(c.Context(), "BEGIN")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(c.Context(), fmt.Sprintf(
		"INSERT INTO %s (id, email) VALUES (%d, '%s')",
		concurrentWriterTable, concurrentWriterRivalID, concurrentWriterEmail,
	))
	c.Assert(err, qt.IsNil)
	return conn
}

func concurrentWriterBackendPID(tb testing.TB, ctx context.Context, writer *sql.Conn) int64 {
	c := qt.New(tb)
	c.Helper()
	var pid int64
	c.Assert(writer.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid), qt.IsNil)
	return pid
}

func concurrentWriterMigrator(conn *dbschema.DatabaseConnection) *migrator.Migrator {
	up := fmt.Sprintf(
		"-- +ptah no_transaction\nCREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %q ON %q (%q);",
		concurrentWriterIndex, concurrentWriterTable, "email",
	)
	down := fmt.Sprintf("-- +ptah no_transaction\nDROP INDEX CONCURRENTLY IF EXISTS %q;", concurrentWriterIndex)
	migration := migrator.CreateMigrationFromSQL(concurrentWriterVersion, "add unique email", up, down)
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable("", concurrentWriterTracker)
}

func openConcurrentWriterDB(tb testing.TB, dsn string) *sql.DB {
	c := qt.New(tb)
	c.Helper()
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	return db
}

// seedConcurrentWriterTable builds a fixture on which the concurrent unique
// build would succeed if nothing else wrote to the table. Every seeded address
// is distinct, so the only duplicate in the run is the one the rival session
// contributes while the build waits.
func seedConcurrentWriterTable(tb testing.TB, db *sql.DB) {
	c := qt.New(tb)
	c.Helper()
	cleanup := func() {
		_, err := db.Exec("DROP TABLE IF EXISTS " + concurrentWriterTable + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP TABLE IF EXISTS " + concurrentWriterTracker + " CASCADE")
		c.Check(err, qt.IsNil)
	}
	cleanup()
	c.Cleanup(cleanup)

	_, err := db.Exec(fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY, email TEXT NOT NULL)", concurrentWriterTable,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		"INSERT INTO %s SELECT g, 'user-' || g || '@example.com' FROM generate_series(1, 5000) g",
		concurrentWriterTable,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		"INSERT INTO %s (id, email) VALUES (%d, '%s')",
		concurrentWriterTable, concurrentWriterSeedID, concurrentWriterEmail,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec("ANALYZE " + concurrentWriterTable)
	c.Assert(err, qt.IsNil)
}

func concurrentWriterIndexFlags(tb testing.TB, db *sql.DB) (valid, ready bool) {
	c := qt.New(tb)
	c.Helper()
	err := db.QueryRow(`
		SELECT ix.indisvalid, ix.indisready
		FROM pg_index ix
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE n.nspname = 'public' AND i.relname = $1`,
		concurrentWriterIndex,
	).Scan(&valid, &ready)
	c.Assert(err, qt.IsNil)
	return valid, ready
}

// concurrentWriterDuplicateInsert reports whether the database enforces the
// uniqueness the migration claims to install, by attempting to break it.
func concurrentWriterDuplicateInsert(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (id, email) VALUES (%d, '%s')",
		concurrentWriterTable, concurrentWriterRivalID+1, concurrentWriterEmail,
	))
	return err
}

// concurrentWriterDeleteDuplicates is the operator's part: the data that broke
// the build has to be fixed before any rebuild can succeed.
func concurrentWriterDeleteDuplicates(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(
		"DELETE FROM %s WHERE email = '%s' AND id <> %d",
		concurrentWriterTable, concurrentWriterEmail, concurrentWriterSeedID,
	))
	return err
}
