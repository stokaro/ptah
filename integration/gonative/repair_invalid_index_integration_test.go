//go:build integration

package gonative_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
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
	repairInvalidIndexTable         = "ptah_issue1101_members"
	repairInvalidIndexName          = "idx_ptah_issue1101_members_email"
	repairInvalidIndexTracker       = "schema_migrations_issue_1101"
	repairInvalidIndexSchema        = "ptah_issue1101_hidden"
	repairInvalidIndexSchemaTracker = "schema_migrations_issue_1101_hidden"
	repairInvalidIndexDropTracker   = "schema_migrations_issue_1101_drop"
	repairInvalidIndexTxTracker     = "schema_migrations_issue_1101_tx"
	repairInvalidIndexResumeTracker = "schema_migrations_issue_1101_resume"
	repairInvalidIndexShadowEarly   = "ptah_issue1101_early"
	repairInvalidIndexShadowLater   = "ptah_issue1101_later"
	repairInvalidIndexShadowTracker = "schema_migrations_issue_1101_shadow"
	repairInvalidIndexPostTracker   = "schema_migrations_issue_1101_postcheck"
	repairInvalidIndexOtherTracker  = "schema_migrations_issue_1101_unrelated"
	repairInvalidIndexVersion       = int64(1785756328)
)

func TestPostgreSQLRepairRefusesOverInvalidUniqueIndexIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openRepairInvalidIndexDB(c.TB, dsn)
	seedRepairInvalidIndexTable(c.TB, db, "'shared@example.com'")

	conn, err := dbschema.ConnectToDatabase(ctx, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := repairInvalidIndexMigrator(conn)

	// The concurrent build fails on the duplicates and leaves the index behind.
	c.Assert(mig.MigrateUp(ctx), qt.ErrorMatches, "(?s).*could not create unique index.*")
	valid, ready := repairInvalidIndexFlags(c.TB, db)
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
	valid, ready = repairInvalidIndexFlags(c.TB, db)
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

func TestPostgreSQLRollbackRepairRefusesOverInvalidUniqueIndexIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openRepairInvalidIndexDB(c.TB, dsn)
	seedRepairInvalidIndexTable(c.TB, db, "'shared@example.com'")

	conn, err := dbschema.ConnectToDatabase(ctx, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := repairInvalidIndexRollbackMigrator(conn)
	c.Assert(mig.MigrateUp(ctx), qt.IsNil)

	c.Assert(mig.MigrateDownTo(ctx, 0), qt.ErrorMatches, "(?s).*could not create unique index.*")
	valid, ready := repairInvalidIndexFlags(c.TB, db)
	c.Assert(valid, qt.IsFalse)
	c.Assert(ready, qt.IsFalse)

	_, err = db.ExecContext(ctx, "DELETE FROM "+repairInvalidIndexTable+" WHERE id > 1")
	c.Assert(err, qt.IsNil)
	err = mig.RepairMigration(ctx, migrator.RepairMigrationOptions{
		Version:    repairInvalidIndexVersion,
		ResumeFrom: 1,
	})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "completing the rollback would hide an unusable index")
	c.Assert(err.Error(), qt.Contains, "resume the rollback")

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Direction, qt.Equals, migrator.MigrationDirectionDown)
	c.Assert(status.DirtyRevision.Applied, qt.Equals, status.DirtyRevision.Total)

	_, err = db.ExecContext(ctx, `REINDEX INDEX CONCURRENTLY "public"."`+repairInvalidIndexName+`"`)
	c.Assert(err, qt.IsNil)
	err = mig.RepairMigration(ctx, migrator.RepairMigrationOptions{Version: repairInvalidIndexVersion})
	c.Assert(err, qt.IsNil)

	status, err = mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.HasLen, 0)
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

	db := openRepairInvalidIndexDB(c.TB, dsn)
	seedRepairInvalidIndexTable(c.TB, db, "'user' || g || '@example.com'")

	conn, err := dbschema.ConnectToDatabase(ctx, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := repairInvalidIndexMigrator(conn)

	c.Assert(mig.MigrateUp(ctx), qt.IsNil)
	valid, ready := repairInvalidIndexFlags(c.TB, db)
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

func TestPostgreSQLRetryRefusesInvalidIndexOutsideSearchPathIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openRepairInvalidIndexDB(c.TB, dsn)
	seedRepairInvalidIndexSchemaTable(c.TB, db)

	conn, err := dbschema.ConnectToDatabase(ctx, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := repairInvalidIndexSchemaMigrator(conn)

	c.Assert(mig.MigrateUp(ctx), qt.ErrorMatches, "(?s).*could not create unique index.*")
	valid, ready := repairInvalidIndexFlagsInSchema(c.TB, db, repairInvalidIndexSchema)
	c.Assert(valid, qt.IsFalse)
	c.Assert(ready, qt.IsFalse)

	_, err = db.ExecContext(ctx, fmt.Sprintf(
		`DELETE FROM %q.%q WHERE id > 1`,
		repairInvalidIndexSchema,
		repairInvalidIndexTable,
	))
	c.Assert(err, qt.IsNil)

	err = mig.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{AllowDirty: true})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `"`+repairInvalidIndexSchema+`"."`+repairInvalidIndexName+`"`)
	c.Assert(err.Error(), qt.Contains, "indisvalid=false, indisready=false")
	c.Assert(err.Error(), qt.Contains, "IF NOT EXISTS finds the name taken")

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Version, qt.Equals, repairInvalidIndexVersion)
	c.Assert(status.AppliedMigrations, qt.HasLen, 0)
}

func TestPostgreSQLFreshDropRecreateRepairsInvalidIndexIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openRepairInvalidIndexDB(c.TB, dsn)
	seedRepairInvalidIndexTable(c.TB, db, "'shared@example.com'")
	leaveRepairInvalidIndex(c.TB, db, `"`+repairInvalidIndexTable+`"`)
	_, err := db.ExecContext(ctx, "DELETE FROM "+repairInvalidIndexTable+" WHERE id > 1")
	c.Assert(err, qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(ctx, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := repairInvalidIndexDropRecreateMigrator(conn, repairInvalidIndexDropTracker)

	c.Assert(mig.MigrateUp(ctx), qt.IsNil)
	valid, ready := repairInvalidIndexFlags(c.TB, db)
	c.Assert(valid, qt.IsTrue)
	c.Assert(ready, qt.IsTrue)

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{repairInvalidIndexVersion})
	c.Assert(repairInvalidIndexDuplicateInsert(ctx, db), qt.IsNotNil)
}

func TestPostgreSQLTransactionalDropRecreateUsesTransactionCatalogIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openRepairInvalidIndexDB(c.TB, dsn)
	seedRepairInvalidIndexTable(c.TB, db, "'shared@example.com'")
	leaveRepairInvalidIndex(c.TB, db, `"`+repairInvalidIndexTable+`"`)
	_, err := db.ExecContext(ctx, "DELETE FROM "+repairInvalidIndexTable+" WHERE id > 1")
	c.Assert(err, qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(ctx, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := repairInvalidIndexTransactionalDropRecreateMigrator(conn)

	c.Assert(mig.MigrateUp(ctx), qt.IsNil)
	valid, ready := repairInvalidIndexFlags(c.TB, db)
	c.Assert(valid, qt.IsTrue)
	c.Assert(ready, qt.IsTrue)
	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{repairInvalidIndexVersion})
}

func TestPostgreSQLResumeCannotReuseSkippedIndexDropIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openRepairInvalidIndexDB(c.TB, dsn)
	seedRepairInvalidIndexTable(c.TB, db, "'shared@example.com'")
	leaveRepairInvalidIndex(c.TB, db, `"`+repairInvalidIndexTable+`"`)

	conn, err := dbschema.ConnectToDatabase(ctx, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := repairInvalidIndexDropRecreateMigrator(conn, repairInvalidIndexResumeTracker)

	c.Assert(mig.MigrateUp(ctx), qt.ErrorMatches, "(?s).*could not create unique index.*")
	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Applied, qt.Equals, 1)
	c.Assert(status.DirtyRevision.Total, qt.Equals, 2)

	_, err = db.ExecContext(ctx, "DELETE FROM "+repairInvalidIndexTable+" WHERE id > 1")
	c.Assert(err, qt.IsNil)
	err = mig.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{AllowDirty: true})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `"public"."`+repairInvalidIndexName+`"`)
	c.Assert(err.Error(), qt.Contains, "IF NOT EXISTS finds the name taken")

	status, err = mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Applied, qt.Equals, 1)
	c.Assert(status.DirtyRevision.Total, qt.Equals, 2)
}

func TestPostgreSQLRetryFindsInvalidIndexShadowedOnSearchPathIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openRepairInvalidIndexDB(c.TB, dsn)
	seedRepairInvalidIndexShadowSchemas(c.TB, db)
	shadowDSN := repairInvalidIndexSearchPathDSN(c.TB, dsn)

	conn, err := dbschema.ConnectToDatabase(ctx, shadowDSN)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := repairInvalidIndexShadowCreateMigrator(conn, repairInvalidIndexShadowTracker)

	c.Assert(mig.MigrateUp(ctx), qt.ErrorMatches, "(?s).*could not create unique index.*")
	_, err = db.ExecContext(ctx, fmt.Sprintf(
		`DELETE FROM %q.%q WHERE id > 1`,
		repairInvalidIndexShadowLater,
		repairInvalidIndexTable,
	))
	c.Assert(err, qt.IsNil)

	err = mig.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{AllowDirty: true})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `"`+repairInvalidIndexShadowLater+`"."`+repairInvalidIndexName+`"`)
	c.Assert(err.Error(), qt.Contains, "IF NOT EXISTS finds the name taken")

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.AppliedMigrations, qt.HasLen, 0)
}

func TestPostgreSQLPreflightRefusesMisdirectedIndexDropIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openRepairInvalidIndexDB(c.TB, dsn)
	seedRepairInvalidIndexShadowSchemas(c.TB, db)
	leaveRepairInvalidIndex(c.TB, db, fmt.Sprintf(
		`%q.%q`,
		repairInvalidIndexShadowLater,
		repairInvalidIndexTable,
	))
	_, err := db.ExecContext(ctx, fmt.Sprintf(
		`DELETE FROM %q.%q WHERE id > 1`,
		repairInvalidIndexShadowLater,
		repairInvalidIndexTable,
	))
	c.Assert(err, qt.IsNil)
	shadowDSN := repairInvalidIndexSearchPathDSN(c.TB, dsn)

	conn, err := dbschema.ConnectToDatabase(ctx, shadowDSN)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := repairInvalidIndexShadowDropRecreateMigrator(conn, repairInvalidIndexPostTracker)

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `"`+repairInvalidIndexShadowLater+`"."`+repairInvalidIndexName+`"`)
	c.Assert(err.Error(), qt.Contains, "IF NOT EXISTS finds the name taken")
	earlyValid, earlyReady := repairInvalidIndexFlagsInSchema(c.TB, db, repairInvalidIndexShadowEarly)
	c.Assert(earlyValid, qt.IsTrue)
	c.Assert(earlyReady, qt.IsTrue)
	laterValid, laterReady := repairInvalidIndexFlagsInSchema(c.TB, db, repairInvalidIndexShadowLater)
	c.Assert(laterValid, qt.IsFalse)
	c.Assert(laterReady, qt.IsFalse)

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.HasLen, 0)
}

func TestPostgreSQLUnrelatedVisibleInvalidIndexDoesNotBlockIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openRepairInvalidIndexDB(c.TB, dsn)
	seedRepairInvalidIndexUnrelatedSchemas(c.TB, db)
	leaveRepairInvalidIndex(c.TB, db, fmt.Sprintf(
		`%q.shadow_owner`,
		repairInvalidIndexShadowLater,
	))
	shadowDSN := repairInvalidIndexSearchPathDSN(c.TB, dsn)

	conn, err := dbschema.ConnectToDatabase(ctx, shadowDSN)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := repairInvalidIndexShadowCreateMigrator(conn, repairInvalidIndexOtherTracker)

	c.Assert(mig.MigrateUp(ctx), qt.IsNil)
	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{repairInvalidIndexVersion})
	valid, ready := repairInvalidIndexFlagsInSchema(c.TB, db, repairInvalidIndexShadowLater)
	c.Assert(valid, qt.IsFalse)
	c.Assert(ready, qt.IsFalse)
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

func repairInvalidIndexRollbackMigrator(conn *dbschema.DatabaseConnection) *migrator.Migrator {
	up := fmt.Sprintf("-- +ptah no_transaction\nDROP INDEX CONCURRENTLY IF EXISTS %q;", repairInvalidIndexName)
	down := fmt.Sprintf(
		"-- +ptah no_transaction\nCREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %q ON %q (%q);",
		repairInvalidIndexName, repairInvalidIndexTable, "email",
	)
	migration := migrator.CreateMigrationFromSQL(repairInvalidIndexVersion, "drop unique email", up, down)
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable("", repairInvalidIndexTracker)
}

func repairInvalidIndexSchemaMigrator(conn *dbschema.DatabaseConnection) *migrator.Migrator {
	up := fmt.Sprintf(
		"-- +ptah no_transaction\nCREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %q ON %q.%q (%q);",
		repairInvalidIndexName,
		repairInvalidIndexSchema,
		repairInvalidIndexTable,
		"email",
	)
	down := fmt.Sprintf(
		"-- +ptah no_transaction\nDROP INDEX IF EXISTS %q.%q;",
		repairInvalidIndexSchema,
		repairInvalidIndexName,
	)
	migration := migrator.CreateMigrationFromSQL(repairInvalidIndexVersion, "add unique email", up, down)
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable("", repairInvalidIndexSchemaTracker)
}

func repairInvalidIndexDropRecreateMigrator(
	conn *dbschema.DatabaseConnection,
	tracker string,
) *migrator.Migrator {
	up := fmt.Sprintf(
		"-- +ptah no_transaction\nDROP INDEX CONCURRENTLY IF EXISTS %q;\n"+
			"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %q ON %q (%q);",
		repairInvalidIndexName,
		repairInvalidIndexName,
		repairInvalidIndexTable,
		"email",
	)
	down := fmt.Sprintf("-- +ptah no_transaction\nDROP INDEX CONCURRENTLY IF EXISTS %q;", repairInvalidIndexName)
	migration := migrator.CreateMigrationFromSQL(repairInvalidIndexVersion, "rebuild unique email", up, down)
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable("", tracker)
}

func repairInvalidIndexTransactionalDropRecreateMigrator(conn *dbschema.DatabaseConnection) *migrator.Migrator {
	up := fmt.Sprintf(
		"DROP INDEX IF EXISTS %q;\nCREATE UNIQUE INDEX IF NOT EXISTS %q ON %q (%q);",
		repairInvalidIndexName,
		repairInvalidIndexName,
		repairInvalidIndexTable,
		"email",
	)
	down := fmt.Sprintf("DROP INDEX IF EXISTS %q;", repairInvalidIndexName)
	migration := migrator.CreateMigrationFromSQL(repairInvalidIndexVersion, "rebuild unique email", up, down)
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable("", repairInvalidIndexTxTracker)
}

func repairInvalidIndexShadowCreateMigrator(
	conn *dbschema.DatabaseConnection,
	tracker string,
) *migrator.Migrator {
	up := fmt.Sprintf(
		"-- +ptah no_transaction\nCREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %q ON %q (%q);",
		repairInvalidIndexName,
		repairInvalidIndexTable,
		"email",
	)
	return repairInvalidIndexShadowMigrator(conn, tracker, up)
}

func repairInvalidIndexShadowDropRecreateMigrator(
	conn *dbschema.DatabaseConnection,
	tracker string,
) *migrator.Migrator {
	up := fmt.Sprintf(
		"-- +ptah no_transaction\nDROP INDEX CONCURRENTLY IF EXISTS %q;\n"+
			"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %q ON %q (%q);",
		repairInvalidIndexName,
		repairInvalidIndexName,
		repairInvalidIndexTable,
		"email",
	)
	return repairInvalidIndexShadowMigrator(conn, tracker, up)
}

func repairInvalidIndexShadowMigrator(
	conn *dbschema.DatabaseConnection,
	tracker string,
	up string,
) *migrator.Migrator {
	down := fmt.Sprintf(
		"-- +ptah no_transaction\nDROP INDEX CONCURRENTLY IF EXISTS %q.%q;",
		repairInvalidIndexShadowLater,
		repairInvalidIndexName,
	)
	migration := migrator.CreateMigrationFromSQL(repairInvalidIndexVersion, "add unique email", up, down)
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable("", tracker)
}

func openRepairInvalidIndexDB(tb testing.TB, dsn string) *sql.DB {
	c := qt.New(tb)
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
func seedRepairInvalidIndexTable(tb testing.TB, db *sql.DB, emailExpr string) {
	c := qt.New(tb)
	c.Helper()
	cleanup := func() {
		_, err := db.Exec("DROP TABLE IF EXISTS " + repairInvalidIndexTable + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP TABLE IF EXISTS " + repairInvalidIndexTracker + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP TABLE IF EXISTS " + repairInvalidIndexDropTracker + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP TABLE IF EXISTS " + repairInvalidIndexTxTracker + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP TABLE IF EXISTS " + repairInvalidIndexResumeTracker + " CASCADE")
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

func seedRepairInvalidIndexSchemaTable(tb testing.TB, db *sql.DB) {
	c := qt.New(tb)
	c.Helper()
	cleanup := func() {
		_, err := db.Exec(`DROP SCHEMA IF EXISTS "` + repairInvalidIndexSchema + `" CASCADE`)
		c.Check(err, qt.IsNil)
		_, err = db.Exec(`DROP TABLE IF EXISTS "` + repairInvalidIndexSchemaTracker + `" CASCADE`)
		c.Check(err, qt.IsNil)
	}
	cleanup()
	c.Cleanup(cleanup)

	_, err := db.Exec(`CREATE SCHEMA "` + repairInvalidIndexSchema + `"`)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`CREATE TABLE %q.%q (id INTEGER PRIMARY KEY, email TEXT NOT NULL)`,
		repairInvalidIndexSchema,
		repairInvalidIndexTable,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`INSERT INTO %q.%q SELECT g, 'shared@example.com' FROM generate_series(1, 5000) g`,
		repairInvalidIndexSchema,
		repairInvalidIndexTable,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(`ANALYZE %q.%q`, repairInvalidIndexSchema, repairInvalidIndexTable))
	c.Assert(err, qt.IsNil)
}

func seedRepairInvalidIndexShadowSchemas(tb testing.TB, db *sql.DB) {
	c := qt.New(tb)
	c.Helper()
	cleanup := func() {
		_, err := db.Exec(`DROP SCHEMA IF EXISTS "` + repairInvalidIndexShadowEarly + `" CASCADE`)
		c.Check(err, qt.IsNil)
		_, err = db.Exec(`DROP SCHEMA IF EXISTS "` + repairInvalidIndexShadowLater + `" CASCADE`)
		c.Check(err, qt.IsNil)
	}
	cleanup()
	c.Cleanup(cleanup)

	_, err := db.Exec(`CREATE SCHEMA "` + repairInvalidIndexShadowEarly + `"`)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(`CREATE SCHEMA "` + repairInvalidIndexShadowLater + `"`)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`CREATE TABLE %q.shadow_owner (id INTEGER PRIMARY KEY, email TEXT NOT NULL)`,
		repairInvalidIndexShadowEarly,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`CREATE INDEX %q ON %q.shadow_owner (email)`,
		repairInvalidIndexName,
		repairInvalidIndexShadowEarly,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`CREATE TABLE %q.%q (id INTEGER PRIMARY KEY, email TEXT NOT NULL)`,
		repairInvalidIndexShadowLater,
		repairInvalidIndexTable,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`INSERT INTO %q.%q SELECT g, 'shared@example.com' FROM generate_series(1, 5000) g`,
		repairInvalidIndexShadowLater,
		repairInvalidIndexTable,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`ANALYZE %q.%q`,
		repairInvalidIndexShadowLater,
		repairInvalidIndexTable,
	))
	c.Assert(err, qt.IsNil)
}

func seedRepairInvalidIndexUnrelatedSchemas(tb testing.TB, db *sql.DB) {
	c := qt.New(tb)
	c.Helper()
	cleanup := func() {
		_, err := db.Exec(`DROP SCHEMA IF EXISTS "` + repairInvalidIndexShadowEarly + `" CASCADE`)
		c.Check(err, qt.IsNil)
		_, err = db.Exec(`DROP SCHEMA IF EXISTS "` + repairInvalidIndexShadowLater + `" CASCADE`)
		c.Check(err, qt.IsNil)
	}
	cleanup()
	c.Cleanup(cleanup)

	_, err := db.Exec(`CREATE SCHEMA "` + repairInvalidIndexShadowEarly + `"`)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(`CREATE SCHEMA "` + repairInvalidIndexShadowLater + `"`)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`CREATE TABLE %q.%q (id INTEGER PRIMARY KEY, email TEXT NOT NULL)`,
		repairInvalidIndexShadowEarly,
		repairInvalidIndexTable,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`INSERT INTO %q.%q SELECT g, 'user' || g || '@example.com' FROM generate_series(1, 5000) g`,
		repairInvalidIndexShadowEarly,
		repairInvalidIndexTable,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`CREATE TABLE %q.shadow_owner (id INTEGER PRIMARY KEY, email TEXT NOT NULL)`,
		repairInvalidIndexShadowLater,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`INSERT INTO %q.shadow_owner SELECT g, 'shared@example.com' FROM generate_series(1, 5000) g`,
		repairInvalidIndexShadowLater,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`ANALYZE %q.%q`,
		repairInvalidIndexShadowEarly,
		repairInvalidIndexTable,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`ANALYZE %q.shadow_owner`,
		repairInvalidIndexShadowLater,
	))
	c.Assert(err, qt.IsNil)
}

func repairInvalidIndexSearchPathDSN(tb testing.TB, dsn string) string {
	c := qt.New(tb)
	c.Helper()
	parsed, err := url.Parse(dsn)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("search_path", repairInvalidIndexShadowEarly+","+repairInvalidIndexShadowLater)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func leaveRepairInvalidIndex(tb testing.TB, db *sql.DB, tableRef string) {
	c := qt.New(tb)
	c.Helper()
	_, err := db.Exec(fmt.Sprintf(
		`CREATE UNIQUE INDEX CONCURRENTLY %q ON %s (email)`,
		repairInvalidIndexName,
		tableRef,
	))
	c.Assert(err, qt.IsNotNil)
}

func repairInvalidIndexFlags(tb testing.TB, db *sql.DB) (valid, ready bool) {
	c := qt.New(tb)
	c.Helper()
	return repairInvalidIndexFlagsInSchema(c.TB, db, "public")
}

func repairInvalidIndexFlagsInSchema(tb testing.TB, db *sql.DB, schema string) (valid, ready bool) {
	c := qt.New(tb)
	c.Helper()
	err := db.QueryRow(`
		SELECT ix.indisvalid, ix.indisready
		FROM pg_index ix
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE n.nspname = $1 AND i.relname = $2`,
		schema,
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
