//go:build integration

package gonative_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// A failed CREATE INDEX CONCURRENTLY leaves an invalid index occupying the
// name, and `migrations up --allow-dirty` re-runs the body over the dirty row
// the failure left. The generated statement carries IF NOT EXISTS, so the
// leftover is skipped rather than rebuilt, nothing errors, and the run used to
// clear the dirty state over an object PostgreSQL will not use. These tests pin
// what the up path does with that residue, on both halves of the issue -- a
// unique index whose constraint is then unenforced, and a plain index that is
// simply never built -- on the same residue reached without a dirty row at all,
// and on the control where the index is usable and the run has to finish the
// migration as it always did.
const (
	retryInvalidIndexTable   = "ptah_issue1101_retry_members"
	retryPartitionedTable    = "ptah_issue1101_retry_events"
	retryInvalidIndexMarker  = "ptah_issue1101_retry_marker"
	retryInvalidUniqueIndex  = "idx_ptah_issue1101_retry_email"
	retryInvalidPlainIndex   = "idx_ptah_issue1101_retry_ratio"
	retryInvalidIndexTracker = "schema_migrations_issue_1101_retry"
	retryInvalidIndexVersion = int64(1785756329)
)

// TestPostgreSQLAllowDirtyRetryRefusesOverInvalidUniqueIndexIntegration is the
// reproduction from the issue, driven through the retry path rather than
// through repair. The measurement that matters is the last one: a write that
// duplicates an existing email is accepted while the index is invalid and
// rejected once it is rebuilt, so the fixture is testing whether the index
// enforces anything, not whether it exists.
func TestPostgreSQLAllowDirtyRetryRefusesOverInvalidUniqueIndexIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openRetryInvalidIndexDB(c, dsn)
	seedRetryInvalidIndexTable(c, db, "'shared@example.com'")

	conn, err := dbschema.ConnectToDatabase(ctx, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := retryInvalidUniqueIndexMigrator(conn)

	// The concurrent build fails on the duplicates and leaves the index behind.
	c.Assert(mig.MigrateUp(ctx), qt.ErrorMatches, "(?s).*could not create unique index.*")
	valid, ready := retryInvalidIndexFlags(c, db, retryInvalidUniqueIndex)
	c.Assert(valid, qt.IsFalse)
	c.Assert(ready, qt.IsFalse)
	failed := retryInvalidIndexDirtyRevision(c, ctx, mig)

	// The operator fixes the data that broke the build and reaches for the
	// documented recovery flag instead of repair.
	_, err = db.ExecContext(ctx, "DELETE FROM "+retryInvalidIndexTable+" WHERE id > 1")
	c.Assert(err, qt.IsNil)

	err = mig.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{AllowDirty: true})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `"public"."`+retryInvalidUniqueIndex+`"`)
	c.Assert(err.Error(), qt.Contains, "indisvalid=false, indisready=false")
	c.Assert(err.Error(), qt.Contains, "REINDEX INDEX CONCURRENTLY")
	c.Assert(err.Error(), qt.Contains, "cannot be applied")

	// The refusal reads the catalog and writes nothing, so the row still carries
	// the failure that produced it and the progress a later retry resumes from.
	assertRetryInvalidIndexRevisionUnchanged(c, retryInvalidIndexDirtyRevision(c, ctx, mig), failed)

	// Why refusing is the conservative direction: nothing is enforced yet.
	c.Assert(retryInvalidIndexDuplicateInsert(ctx, db), qt.IsNil)
	_, err = db.ExecContext(ctx, "DELETE FROM "+retryInvalidIndexTable+" WHERE id > 1")
	c.Assert(err, qt.IsNil)

	// Rebuilding the index is the escape hatch the refusal names, and it works.
	_, err = db.ExecContext(ctx, `REINDEX INDEX CONCURRENTLY "public"."`+retryInvalidUniqueIndex+`"`)
	c.Assert(err, qt.IsNil)
	valid, ready = retryInvalidIndexFlags(c, db, retryInvalidUniqueIndex)
	c.Assert(valid, qt.IsTrue)
	c.Assert(ready, qt.IsTrue)

	c.Assert(mig.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)
	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{retryInvalidIndexVersion})
	c.Assert(retryInvalidIndexDuplicateInsert(ctx, db), qt.IsNotNil)
}

// TestPostgreSQLAllowDirtyRetryRefusesOverInvalidPlainIndexIntegration is the
// other half. A plain index carries no constraint, so nothing about a write can
// reveal that it was never built -- the migration claims an access path the
// planner can never use, and the only witness is the catalog. The build is made
// to fail on an expression that divides by zero for one row, which is the
// deterministic way to leave a non-unique index invalid.
func TestPostgreSQLAllowDirtyRetryRefusesOverInvalidPlainIndexIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openRetryInvalidIndexDB(c, dsn)
	seedRetryInvalidIndexTable(c, db, "'user' || g || '@example.com'")

	conn, err := dbschema.ConnectToDatabase(ctx, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := retryInvalidPlainIndexMigrator(conn)

	c.Assert(mig.MigrateUp(ctx), qt.ErrorMatches, "(?s).*division by zero.*")
	valid, ready := retryInvalidIndexFlags(c, db, retryInvalidPlainIndex)
	c.Assert(valid, qt.IsFalse)
	c.Assert(ready, qt.IsFalse)
	unique := retryInvalidIndexUnique(c, db, retryInvalidPlainIndex)
	c.Assert(unique, qt.IsFalse)
	failed := retryInvalidIndexDirtyRevision(c, ctx, mig)

	_, err = db.ExecContext(ctx, "DELETE FROM "+retryInvalidIndexTable+" WHERE id = 3")
	c.Assert(err, qt.IsNil)

	err = mig.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{AllowDirty: true})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `"public"."`+retryInvalidPlainIndex+`"`)
	c.Assert(err.Error(), qt.Contains, "indisvalid=false, indisready=false")
	assertRetryInvalidIndexRevisionUnchanged(c, retryInvalidIndexDirtyRevision(c, ctx, mig), failed)

	// Dropping the leftover is the refusal's other named remedy: with the name
	// free, IF NOT EXISTS no longer skips and the retry actually builds it.
	_, err = db.ExecContext(ctx, `DROP INDEX "public"."`+retryInvalidPlainIndex+`"`)
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)
	valid, ready = retryInvalidIndexFlags(c, db, retryInvalidPlainIndex)
	c.Assert(valid, qt.IsTrue)
	c.Assert(ready, qt.IsTrue)

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{retryInvalidIndexVersion})
}

// TestPostgreSQLAllowDirtyRetryOverValidIndexStillAppliesIntegration is the
// discriminating control. It is a retry over a dirty row -- the same path both
// refusals travel -- where the index the migration creates was built and is
// usable, and the failure was in a later statement. The retry has to resume,
// finish, and record the migration applied. A probe that refused whenever a
// migration mentions an index, or whenever a revision row is dirty, fails here.
func TestPostgreSQLAllowDirtyRetryOverValidIndexStillAppliesIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openRetryInvalidIndexDB(c, dsn)
	seedRetryInvalidIndexTable(c, db, "'user' || g || '@example.com'")

	conn, err := dbschema.ConnectToDatabase(ctx, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := retryValidIndexThenFailureMigrator(conn)

	// Statement 1 builds the index and commits; statement 2 writes to a table
	// that does not exist yet and fails.
	c.Assert(mig.MigrateUp(ctx), qt.ErrorMatches, "(?s).*"+retryInvalidIndexMarker+".*")
	valid, ready := retryInvalidIndexFlags(c, db, retryInvalidUniqueIndex)
	c.Assert(valid, qt.IsTrue)
	c.Assert(ready, qt.IsTrue)
	failed := retryInvalidIndexDirtyRevision(c, ctx, mig)
	c.Assert(failed.Applied, qt.Equals, 1)
	c.Assert(failed.Total, qt.Equals, 2)

	// The operator creates the missing table and retries.
	_, err = db.ExecContext(ctx, "CREATE TABLE "+retryInvalidIndexMarker+" (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)

	c.Assert(mig.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)
	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{retryInvalidIndexVersion})
	c.Assert(retryInvalidIndexDuplicateInsert(ctx, db), qt.IsNotNil)
}

// TestPostgreSQLFirstAttemptRefusesOverPreexistingInvalidIndexIntegration is
// the same defect reached with no dirty row anywhere.
//
// The residue here was not left by a run this migrator is finishing: the index
// was built by hand and failed, which is also what a restored dump or a revision
// row cleaned up out of band leaves behind. The migration has never been
// attempted, so nothing about the revision table hints that anything is wrong --
// and IF NOT EXISTS skips the leftover exactly as it does on a retry. Scope the
// refusal to a dirty retry and this test is what goes red: measured on that
// shape, the run exits 0, records the migration applied, and accepts a duplicate
// write.
func TestPostgreSQLFirstAttemptRefusesOverPreexistingInvalidIndexIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openRetryInvalidIndexDB(c, dsn)
	seedRetryInvalidIndexTable(c, db, "'shared@example.com'")

	conn, err := dbschema.ConnectToDatabase(ctx, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := retryInvalidUniqueIndexMigrator(conn)

	// Nothing here goes through the migrator: the build and its failure happen
	// out of band, so no revision row exists for the version at all.
	_, err = db.ExecContext(ctx, fmt.Sprintf(
		"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %q ON %q (%q)",
		retryInvalidUniqueIndex, retryInvalidIndexTable, "email",
	))
	c.Assert(err, qt.IsNotNil)
	_, err = db.ExecContext(ctx, "DELETE FROM "+retryInvalidIndexTable+" WHERE id > 1")
	c.Assert(err, qt.IsNil)
	valid, ready := retryInvalidIndexFlags(c, db, retryInvalidUniqueIndex)
	c.Assert(valid, qt.IsFalse)
	c.Assert(ready, qt.IsFalse)

	// A dry run is exempt. It records nothing, so it has nothing to be wrong
	// about, and no surface's dry-run exit code was measured for this refusal.
	dryConn, err := dbschema.ConnectToDatabase(ctx, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(dryConn.Close(), qt.IsNil) })
	dryConn.SchemaWriter().SetDryRun(true)
	c.Assert(retryInvalidUniqueIndexMigrator(dryConn).MigrateUp(ctx), qt.IsNil)

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `"public"."`+retryInvalidUniqueIndex+`"`)
	c.Assert(err.Error(), qt.Contains, "cannot be applied")

	// Refusing before any bookkeeping means the revision table is untouched:
	// this is a migration that was never started, not one that failed.
	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.HasLen, 0)

	// And the remedy still gets the operator all the way through.
	_, err = db.ExecContext(ctx, `REINDEX INDEX CONCURRENTLY "public"."`+retryInvalidUniqueIndex+`"`)
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(ctx), qt.IsNil)
	c.Assert(retryInvalidIndexDuplicateInsert(ctx, db), qt.IsNotNil)
}

// TestPostgreSQLFirstAttemptOverPartitionedParentIndexStillAppliesIntegration
// pins that an index which is deliberately invalid does not block the migration
// that creates it.
//
// CREATE INDEX ... ON ONLY a partitioned parent is the documented way to build a
// partitioned index without locking every partition at once, and PostgreSQL
// marks the parent invalid on purpose until an index is attached for every
// partition -- measured here as relkind='I', indisvalid=false, indisready=true.
// The probe runs before the body, so the index it asks about does not exist yet
// and the migration applies.
//
// The limit is honest, and this test does not pin it: a migration in this shape
// that failed on a later statement and was run again WOULD be refused, because
// by then the parent index exists and is invalid. The operator would have to
// finish or drop it first. Telling a partitioned parent apart from residue is
// stokaro/ptah#997's partitioned-parent awareness, which is measured there
// rather than guessed at here.
func TestPostgreSQLFirstAttemptOverPartitionedParentIndexStillAppliesIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openRetryInvalidIndexDB(c, dsn)
	seedRetryPartitionedTable(c, db)

	conn, err := dbschema.ConnectToDatabase(ctx, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := retryPartitionedParentIndexMigrator(conn)

	c.Assert(mig.MigrateUp(ctx), qt.IsNil)
	valid, ready := retryInvalidIndexFlags(c, db, retryInvalidPlainIndex)
	c.Assert(valid, qt.IsFalse)
	c.Assert(ready, qt.IsTrue)

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{retryInvalidIndexVersion})
}

func retryInvalidUniqueIndexMigrator(conn *dbschema.DatabaseConnection) *migrator.Migrator {
	up := fmt.Sprintf(
		"-- +ptah no_transaction\nCREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %q ON %q (%q);",
		retryInvalidUniqueIndex, retryInvalidIndexTable, "email",
	)
	down := fmt.Sprintf("-- +ptah no_transaction\nDROP INDEX IF EXISTS %q;", retryInvalidUniqueIndex)
	return retryInvalidIndexMigrator(conn, up, down)
}

func retryInvalidPlainIndexMigrator(conn *dbschema.DatabaseConnection) *migrator.Migrator {
	up := fmt.Sprintf(
		"-- +ptah no_transaction\nCREATE INDEX CONCURRENTLY IF NOT EXISTS %q ON %q ((10 / (id - 3)));",
		retryInvalidPlainIndex, retryInvalidIndexTable,
	)
	down := fmt.Sprintf("-- +ptah no_transaction\nDROP INDEX IF EXISTS %q;", retryInvalidPlainIndex)
	return retryInvalidIndexMigrator(conn, up, down)
}

// retryValidIndexThenFailureMigrator builds the control's two-statement body:
// an index build that succeeds, then a statement that fails for a reason the
// operator can fix without touching the index.
func retryValidIndexThenFailureMigrator(conn *dbschema.DatabaseConnection) *migrator.Migrator {
	up := fmt.Sprintf(
		"-- +ptah no_transaction\nCREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %q ON %q (%q);\nINSERT INTO %s (id) VALUES (1);",
		retryInvalidUniqueIndex, retryInvalidIndexTable, "email", retryInvalidIndexMarker,
	)
	down := fmt.Sprintf(
		"-- +ptah no_transaction\nDELETE FROM %s WHERE id = 1;\nDROP INDEX IF EXISTS %q;",
		retryInvalidIndexMarker, retryInvalidUniqueIndex,
	)
	return retryInvalidIndexMigrator(conn, up, down)
}

func retryPartitionedParentIndexMigrator(conn *dbschema.DatabaseConnection) *migrator.Migrator {
	up := fmt.Sprintf(
		"CREATE INDEX IF NOT EXISTS %q ON ONLY %q (%q);",
		retryInvalidPlainIndex, retryPartitionedTable, "email",
	)
	down := fmt.Sprintf("DROP INDEX IF EXISTS %q;", retryInvalidPlainIndex)
	return retryInvalidIndexMigrator(conn, up, down)
}

func retryInvalidIndexMigrator(conn *dbschema.DatabaseConnection, up, down string) *migrator.Migrator {
	migration := migrator.CreateMigrationFromSQL(retryInvalidIndexVersion, "add unique email", up, down)
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable("", retryInvalidIndexTracker)
}

func openRetryInvalidIndexDB(c *qt.C, dsn string) *sql.DB {
	c.Helper()
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	return db
}

// seedRetryInvalidIndexTable rebuilds the fixture from scratch so a previous run
// cannot decide this one's outcome. emailExpr is evaluated per generated row: a
// constant produces the duplicates that break a unique build, and an expression
// over g produces distinct values that let it succeed.
func seedRetryInvalidIndexTable(c *qt.C, db *sql.DB, emailExpr string) {
	c.Helper()
	cleanup := func() {
		for _, table := range []string{retryInvalidIndexTable, retryInvalidIndexMarker, retryInvalidIndexTracker} {
			_, err := db.Exec("DROP TABLE IF EXISTS " + table + " CASCADE")
			c.Check(err, qt.IsNil)
		}
	}
	cleanup()
	c.Cleanup(cleanup)

	_, err := db.Exec(fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY, email TEXT NOT NULL)", retryInvalidIndexTable,
	))
	c.Assert(err, qt.IsNil)
	// PostgreSQL only picks a concurrent-build-worthy plan on a table it knows
	// has rows, so the seed is populated and analyzed before the migration runs.
	_, err = db.Exec(fmt.Sprintf(
		"INSERT INTO %s SELECT g, %s FROM generate_series(1, 5000) g", retryInvalidIndexTable, emailExpr,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec("ANALYZE " + retryInvalidIndexTable)
	c.Assert(err, qt.IsNil)
}

// seedRetryPartitionedTable builds a partitioned table with one partition, which
// is what makes CREATE INDEX ... ON ONLY leave a deliberately invalid parent.
func seedRetryPartitionedTable(c *qt.C, db *sql.DB) {
	c.Helper()
	cleanup := func() {
		for _, table := range []string{retryPartitionedTable, retryInvalidIndexTracker} {
			_, err := db.Exec("DROP TABLE IF EXISTS " + table + " CASCADE")
			c.Check(err, qt.IsNil)
		}
	}
	cleanup()
	c.Cleanup(cleanup)

	_, err := db.Exec(fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER, email TEXT NOT NULL) PARTITION BY RANGE (id)", retryPartitionedTable,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		"CREATE TABLE %s_p1 PARTITION OF %s FOR VALUES FROM (1) TO (100)",
		retryPartitionedTable, retryPartitionedTable,
	))
	c.Assert(err, qt.IsNil)
}

func retryInvalidIndexFlags(c *qt.C, db *sql.DB, name string) (valid, ready bool) {
	c.Helper()
	err := db.QueryRow(`
		SELECT ix.indisvalid, ix.indisready
		FROM pg_index ix
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE n.nspname = 'public' AND i.relname = $1`,
		name,
	).Scan(&valid, &ready)
	c.Assert(err, qt.IsNil)
	return valid, ready
}

func retryInvalidIndexUnique(c *qt.C, db *sql.DB, name string) bool {
	c.Helper()
	var unique bool
	err := db.QueryRow(`
		SELECT ix.indisunique
		FROM pg_index ix
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE n.nspname = 'public' AND i.relname = $1`,
		name,
	).Scan(&unique)
	c.Assert(err, qt.IsNil)
	return unique
}

// retryInvalidIndexDirtyRevision returns the dirty row as the status command
// reports it, so a later call can assert the refusal left it alone. Timings are
// blanked because a revision that was not rewritten still reports the wall clock
// the failing attempt took, which no assertion should depend on.
func retryInvalidIndexDirtyRevision(c *qt.C, ctx context.Context, mig *migrator.Migrator) migrator.MigrationRevision {
	c.Helper()
	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	revision := *status.DirtyRevision
	revision.ExecutionTime = 0
	revision.AppliedAt = revision.AppliedAt.UTC().Truncate(0)
	return revision
}

func assertRetryInvalidIndexRevisionUnchanged(
	c *qt.C,
	got migrator.MigrationRevision,
	want migrator.MigrationRevision,
) {
	c.Helper()
	c.Assert(got.Version, qt.Equals, want.Version)
	c.Assert(got.Description, qt.Equals, want.Description)
	c.Assert(got.State, qt.Equals, want.State)
	c.Assert(got.Direction, qt.Equals, want.Direction)
	c.Assert(got.AtlasType, qt.Equals, want.AtlasType)
	c.Assert(got.Applied, qt.Equals, want.Applied)
	c.Assert(got.Total, qt.Equals, want.Total)
	c.Assert(got.Error, qt.Equals, want.Error)
	c.Assert(got.ErrorStatement, qt.Equals, want.ErrorStatement)
	c.Assert(got.ExecutionTime, qt.Equals, want.ExecutionTime)
	c.Assert(got.Checksum, qt.Equals, want.Checksum)
	c.Assert(got.AppliedAt, qt.Equals, want.AppliedAt)
	c.Assert(got.OperatorVersion, qt.Equals, want.OperatorVersion)
	c.Assert(got.Dirty, qt.Equals, want.Dirty)
	c.Assert(got.ChecksumCurrent, qt.Equals, want.ChecksumCurrent)
}

// retryInvalidIndexDuplicateInsert returns the error PostgreSQL raises for a
// second row carrying an email that already exists, or nil when the write is
// accepted. Nil is the shape of the defect: the index exists and the migration
// is recorded applied while enforcing nothing.
func retryInvalidIndexDuplicateInsert(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (id, email) SELECT 999999, email FROM %s ORDER BY id LIMIT 1",
		retryInvalidIndexTable, retryInvalidIndexTable,
	))
	return err
}
