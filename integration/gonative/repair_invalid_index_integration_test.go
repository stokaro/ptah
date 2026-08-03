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

// A failed CREATE UNIQUE INDEX CONCURRENTLY leaves an invalid index occupying
// the name, so re-issuing the generated IF NOT EXISTS statement is skipped
// rather than retried and nothing errors. These tests pin what repair does with
// that residue: it refuses while the index is unusable, and it does not
// interfere once the index is usable.
const (
	repairInvalidIndexTable   = "ptah_issue1101_members"
	repairInvalidIndexName    = "idx_ptah_issue1101_members_email"
	repairInvalidIndexTracker = "schema_migrations_issue_1101"
	repairInvalidIndexVersion = int64(1785756328)
)

func TestPostgreSQLRepairRefusesOverInvalidUniqueIndexIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openRepairInvalidIndexDB(c, dsn)
	seedRepairInvalidIndexTable(c, db, "'shared@example.com'")

	conn, err := dbschema.ConnectToDatabase(ctx, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := repairInvalidIndexMigrator(conn)

	// The concurrent build fails on the duplicates and leaves the index behind.
	c.Assert(mig.MigrateUp(ctx), qt.ErrorMatches, "(?s).*could not create unique index.*")
	valid, ready := repairInvalidIndexFlags(c, db)
	c.Assert(valid, qt.IsFalse)
	c.Assert(ready, qt.IsFalse)

	// The operator fixes the data that broke the build, which is the whole
	// prerequisite for repair. The leftover index is still unusable.
	_, err = db.ExecContext(ctx, "DELETE FROM "+repairInvalidIndexTable+" WHERE id > 1")
	c.Assert(err, qt.IsNil)

	err = mig.RepairMigration(ctx, migrator.RepairMigrationOptions{
		Version:    repairInvalidIndexVersion,
		ResumeFrom: 1,
	})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `"public"."`+repairInvalidIndexName+`"`)
	c.Assert(err.Error(), qt.Contains, "indisvalid=false, indisready=false")
	c.Assert(err.Error(), qt.Contains, "REINDEX INDEX CONCURRENTLY")

	// --force relaxes a precondition about the revision row, not a fact about
	// the database, so it does not buy past an unenforced constraint.
	err = mig.RepairMigration(ctx, migrator.RepairMigrationOptions{
		Version:    repairInvalidIndexVersion,
		ResumeFrom: 1,
		Force:      true,
	})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "REINDEX INDEX CONCURRENTLY")

	// The dirty state the operator can still see is the point of refusing.
	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Version, qt.Equals, repairInvalidIndexVersion)
	c.Assert(status.AppliedMigrations, qt.HasLen, 0)

	// Rebuilding the index is the escape hatch the refusal names, and it works.
	_, err = db.ExecContext(ctx, `REINDEX INDEX CONCURRENTLY "public"."`+repairInvalidIndexName+`"`)
	c.Assert(err, qt.IsNil)
	valid, ready = repairInvalidIndexFlags(c, db)
	c.Assert(valid, qt.IsTrue)
	c.Assert(ready, qt.IsTrue)

	err = mig.RepairMigration(ctx, migrator.RepairMigrationOptions{
		Version:    repairInvalidIndexVersion,
		ResumeFrom: 1,
	})
	c.Assert(err, qt.IsNil)
	status, err = mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{repairInvalidIndexVersion})
	c.Assert(repairInvalidIndexDuplicateInsert(ctx, db), qt.IsNotNil)
}

// TestPostgreSQLRepairLeavesUsableIndexAloneIntegration is the control. On data
// where the concurrent build succeeds, nothing about the probe may show: the
// migration applies, the index is usable, the constraint is enforced, and a
// later repair over that same migration still records it. A check that refused
// whenever a migration created an index would fail here.
func TestPostgreSQLRepairLeavesUsableIndexAloneIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openRepairInvalidIndexDB(c, dsn)
	seedRepairInvalidIndexTable(c, db, "'user' || g || '@example.com'")

	conn, err := dbschema.ConnectToDatabase(ctx, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := repairInvalidIndexMigrator(conn)

	c.Assert(mig.MigrateUp(ctx), qt.IsNil)
	valid, ready := repairInvalidIndexFlags(c, db)
	c.Assert(valid, qt.IsTrue)
	c.Assert(ready, qt.IsTrue)
	c.Assert(repairInvalidIndexDuplicateInsert(ctx, db), qt.IsNotNil)

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{repairInvalidIndexVersion})

	err = mig.RepairMigration(ctx, migrator.RepairMigrationOptions{
		Version: repairInvalidIndexVersion,
		Force:   true,
	})
	c.Assert(err, qt.IsNil)
}

func repairInvalidIndexMigrator(conn *dbschema.DatabaseConnection) *migrator.Migrator {
	up := fmt.Sprintf(
		"-- +ptah no_transaction\nCREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %q ON %q (%q);",
		repairInvalidIndexName, repairInvalidIndexTable, "email",
	)
	down := fmt.Sprintf("-- +ptah no_transaction\nDROP INDEX IF EXISTS %q;", repairInvalidIndexName)
	migration := migrator.CreateMigrationFromSQL(repairInvalidIndexVersion, "add unique email", up, down)
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable("", repairInvalidIndexTracker)
}

func openRepairInvalidIndexDB(c *qt.C, dsn string) *sql.DB {
	c.Helper()
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	return db
}

// seedRepairInvalidIndexTable rebuilds the fixture from scratch so a previous
// run cannot decide this one's outcome. emailExpr is evaluated per generated
// row: a constant produces the duplicates that break a unique build, and an
// expression over g produces distinct values that let it succeed.
func seedRepairInvalidIndexTable(c *qt.C, db *sql.DB, emailExpr string) {
	c.Helper()
	cleanup := func() {
		_, err := db.Exec("DROP TABLE IF EXISTS " + repairInvalidIndexTable + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP TABLE IF EXISTS " + repairInvalidIndexTracker + " CASCADE")
		c.Check(err, qt.IsNil)
	}
	cleanup()
	c.Cleanup(cleanup)

	_, err := db.Exec(fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY, email TEXT NOT NULL)", repairInvalidIndexTable,
	))
	c.Assert(err, qt.IsNil)
	// PostgreSQL only picks a concurrent-build-worthy plan on a table it knows
	// has rows, so the seed is populated and analyzed before the migration runs.
	_, err = db.Exec(fmt.Sprintf(
		"INSERT INTO %s SELECT g, %s FROM generate_series(1, 5000) g", repairInvalidIndexTable, emailExpr,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec("ANALYZE " + repairInvalidIndexTable)
	c.Assert(err, qt.IsNil)
}

func repairInvalidIndexFlags(c *qt.C, db *sql.DB) (valid, ready bool) {
	c.Helper()
	err := db.QueryRow(`
		SELECT ix.indisvalid, ix.indisready
		FROM pg_index ix
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE n.nspname = 'public' AND i.relname = $1`,
		repairInvalidIndexName,
	).Scan(&valid, &ready)
	c.Assert(err, qt.IsNil)
	return valid, ready
}

// repairInvalidIndexDuplicateInsert returns the error PostgreSQL raises for a
// second row carrying an email that already exists, or nil when the write is
// accepted. Nil is the shape of the defect: the index exists and is recorded
// applied while enforcing nothing.
func repairInvalidIndexDuplicateInsert(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (id, email) SELECT 999999, email FROM %s ORDER BY id LIMIT 1",
		repairInvalidIndexTable, repairInvalidIndexTable,
	))
	return err
}
