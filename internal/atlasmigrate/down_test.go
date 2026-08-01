package atlasmigrate_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasmigrate"
	"github.com/stokaro/ptah/migration/migrator"
)

// prepareDownFixture applies two Atlas-format migrations (with ptah-style
// supplementary down files) so a down plan has real revision state to select
// from. The second migration's down body is caller-provided so failure paths
// can plant a broken statement.
func prepareDownFixture(c *qt.C, secondDownSQL string) (migrationsDir string, conn *dbschema.DatabaseConnection) {
	c.Helper()
	ctx := context.Background()
	dir := c.TB.TempDir()
	migrationsDir = filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c, migrationsDir, "1_init.sql", "CREATE TABLE down_users (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigrationFile(c, migrationsDir, "1_init.down.sql", "DROP TABLE down_users;")
	writeAtlasApplyMigrationFile(c, migrationsDir, "2_add_email.sql", "ALTER TABLE down_users ADD COLUMN email TEXT;")
	writeAtlasApplyMigrationFile(c, migrationsDir, "2_add_email.down.sql", secondDownSQL)
	conn = connectSQLite(c, filepath.Join(dir, "down.db"))

	plan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)
	_, err = plan.Execute(ctx)
	c.Assert(err, qt.IsNil)
	return migrationsDir, conn
}

func TestPrepareDownExecute_HappyPathRevertsToTarget(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	migrationsDir, conn := prepareDownFixture(c, "ALTER TABLE down_users DROP COLUMN email;")
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasmigrate.PrepareDown(ctx, conn, atlasmigrate.DownOptions{
		Dir:           migrationsDir,
		FS:            os.DirFS(migrationsDir),
		TargetVersion: 1,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.CurrentVersion, qt.Equals, int64(2))
	c.Assert(plan.PlannedVersions, qt.DeepEquals, []int64{2})
	c.Assert(plan.Noop(), qt.IsFalse)

	result, err := plan.Execute(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Reverted, qt.IsTrue)
	c.Assert(result.RevertedVersions, qt.DeepEquals, []int64{2})
	c.Assert(result.FinalStatus.CurrentVersion, qt.Equals, int64(1))
	c.Assert(sqliteAtlasRevisionVersions(c, conn), qt.DeepEquals, []string{"1"})
}

func TestPrepareDownExecute_RevertsAllInNewestFirstOrder(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	migrationsDir, conn := prepareDownFixture(c, "ALTER TABLE down_users DROP COLUMN email;")
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasmigrate.PrepareDown(ctx, conn, atlasmigrate.DownOptions{
		Dir:           migrationsDir,
		FS:            os.DirFS(migrationsDir),
		TargetVersion: 0,
	})
	c.Assert(err, qt.IsNil)
	// Revert order is newest first, matching the migrator's execution order.
	c.Assert(plan.PlannedVersions, qt.DeepEquals, []int64{2, 1})

	result, err := plan.Execute(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(result.RevertedVersions, qt.DeepEquals, []int64{2, 1})
	c.Assert(sqliteTableExists(c, conn, "down_users"), qt.IsFalse)
}

func TestPrepareDownExecute_DryRunLeavesRevisionsUntouched(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	migrationsDir, conn := prepareDownFixture(c, "ALTER TABLE down_users DROP COLUMN email;")
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasmigrate.PrepareDown(ctx, conn, atlasmigrate.DownOptions{
		Dir:           migrationsDir,
		FS:            os.DirFS(migrationsDir),
		TargetVersion: 0,
		DryRun:        true,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.CurrentVersion, qt.Equals, int64(2))
	c.Assert(plan.PlannedVersions, qt.DeepEquals, []int64{2, 1})

	result, err := plan.Execute(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Reverted, qt.IsFalse)
	c.Assert(result.RevertedVersions, qt.HasLen, 0)
	c.Assert(sqliteAtlasRevisionVersions(c, conn), qt.DeepEquals, []string{"1", "2"})
	c.Assert(sqliteTableExists(c, conn, "down_users"), qt.IsTrue)
}

func TestPrepareDownExecute_MissingDownPreservesRevisionAndSchemaState(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c, migrationsDir, "1_users.sql", "CREATE TABLE missing_down_users (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigrationFile(c, migrationsDir, "2_posts.sql", "CREATE TABLE missing_down_posts (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigrationFile(c, migrationsDir, "2_posts.down.sql", "DROP TABLE missing_down_posts;")
	conn := connectSQLite(c, filepath.Join(dir, "missing-down.db"))
	defer dbschema.CloseAndWarn(conn)

	applyPlan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)
	_, err = applyPlan.Execute(ctx)
	c.Assert(err, qt.IsNil)

	downPlan, err := atlasmigrate.PrepareDown(ctx, conn, atlasmigrate.DownOptions{
		Dir:           migrationsDir,
		FS:            os.DirFS(migrationsDir),
		TargetVersion: 0,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(downPlan.PlannedVersions, qt.DeepEquals, []int64{2, 1})

	result, err := downPlan.Execute(ctx)
	c.Assert(err, qt.ErrorMatches, `.*migration 1 has no Atlas down migration.*`)
	c.Assert(result.Reverted, qt.IsFalse)
	c.Assert(result.RevertedVersions, qt.HasLen, 0)
	c.Assert(sqliteAtlasRevisionVersions(c, conn), qt.DeepEquals, []string{"1", "2"})
	c.Assert(sqliteTableExists(c, conn, "missing_down_users"), qt.IsTrue)
	c.Assert(sqliteTableExists(c, conn, "missing_down_posts"), qt.IsTrue)

	status, err := atlasmigrate.Status(ctx, conn, atlasmigrate.StatusOptions{
		Dir: migrationsDir,
		FS:  os.DirFS(migrationsDir),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(status.Status.CurrentVersion, qt.Equals, int64(2))
	c.Assert(status.Status.AppliedMigrations, qt.DeepEquals, []int64{1, 2})
	c.Assert(status.Status.PendingMigrations, qt.HasLen, 0)
	c.Assert(status.Status.DirtyRevision, qt.IsNil)
}

func TestPrepareDownExecute_DryRunRejectsMissingDownWithoutMutation(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c, migrationsDir, "1_users.sql", "CREATE TABLE dry_missing_down_users (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c, filepath.Join(dir, "dry-missing-down.db"))
	defer dbschema.CloseAndWarn(conn)

	applyPlan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)
	_, err = applyPlan.Execute(ctx)
	c.Assert(err, qt.IsNil)

	downPlan, err := atlasmigrate.PrepareDown(ctx, conn, atlasmigrate.DownOptions{
		Dir:           migrationsDir,
		FS:            os.DirFS(migrationsDir),
		TargetVersion: 0,
		DryRun:        true,
	})
	c.Assert(err, qt.IsNil)

	result, err := downPlan.Execute(ctx)
	c.Assert(err, qt.ErrorMatches, `error rolling back migrations: migration 1 has no Atlas down migration.*`)
	c.Assert(result.Reverted, qt.IsFalse)
	c.Assert(result.RevertedVersions, qt.HasLen, 0)
	c.Assert(sqliteAtlasRevisionVersions(c, conn), qt.DeepEquals, []string{"1"})
	c.Assert(sqliteTableExists(c, conn, "dry_missing_down_users"), qt.IsTrue)
}

func TestPrepareDownExecute_DirtyPreflightIsNotReportedAsRollbackAttempt(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	migrationsDir, conn := prepareDownFixture(c, "ALTER TABLE down_users DROP COLUMN email;")
	defer dbschema.CloseAndWarn(conn)
	_, err := conn.ExecContext(ctx, `UPDATE atlas_schema_revisions
SET applied = 0, total = 1, error = 'broken'
WHERE version = '2'`)
	c.Assert(err, qt.IsNil)

	plan, err := atlasmigrate.PrepareDown(ctx, conn, atlasmigrate.DownOptions{
		Dir:           migrationsDir,
		FS:            os.DirFS(migrationsDir),
		TargetVersion: 0,
	})
	c.Assert(err, qt.IsNil)

	result, err := plan.Execute(ctx)
	c.Assert(err, qt.ErrorMatches, `error rolling back migrations: migration 2 is dirty:.*`)
	c.Assert(result.Reverted, qt.IsFalse)
	c.Assert(result.RevertedVersions, qt.HasLen, 0)
	var dirty *migrator.DirtyMigrationError
	c.Assert(result.DownError, qt.ErrorAs, &dirty)
	c.Assert(sqliteAtlasRevisionVersions(c, conn), qt.DeepEquals, []string{"1", "2"})
	c.Assert(sqliteTableExists(c, conn, "down_users"), qt.IsTrue)
}

func TestPrepareDownExecute_NoopWhenTargetAtOrAboveCurrent(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	migrationsDir, conn := prepareDownFixture(c, "ALTER TABLE down_users DROP COLUMN email;")
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasmigrate.PrepareDown(ctx, conn, atlasmigrate.DownOptions{
		Dir:           migrationsDir,
		FS:            os.DirFS(migrationsDir),
		TargetVersion: 2,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Noop(), qt.IsTrue)

	result, err := plan.Execute(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Reverted, qt.IsFalse)
	c.Assert(sqliteAtlasRevisionVersions(c, conn), qt.DeepEquals, []string{"1", "2"})
}

func TestPrepareDownExecute_FailurePathReportsRevertedPrefix(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	// Version 2 reverts cleanly; version 1's down file is broken, so the run
	// fails after reverting only the newest migration.
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c, migrationsDir, "1_init.sql", "CREATE TABLE partial_users (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigrationFile(c, migrationsDir, "1_init.down.sql", "DROP TABLE no_such_table;")
	writeAtlasApplyMigrationFile(c, migrationsDir, "2_add_email.sql", "ALTER TABLE partial_users ADD COLUMN email TEXT;")
	writeAtlasApplyMigrationFile(c, migrationsDir, "2_add_email.down.sql", "ALTER TABLE partial_users DROP COLUMN email;")
	conn := connectSQLite(c, filepath.Join(dir, "partial.db"))
	defer dbschema.CloseAndWarn(conn)
	applyPlan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)
	_, err = applyPlan.Execute(ctx)
	c.Assert(err, qt.IsNil)

	plan, err := atlasmigrate.PrepareDown(ctx, conn, atlasmigrate.DownOptions{
		Dir:           migrationsDir,
		FS:            os.DirFS(migrationsDir),
		TargetVersion: 0,
	})
	c.Assert(err, qt.IsNil)

	result, err := plan.Execute(ctx)

	c.Assert(err, qt.ErrorMatches, `(?s)error rolling back migrations: .*no_such_table.*`)
	c.Assert(result.Reverted, qt.IsTrue)
	c.Assert(result.RevertedVersions, qt.DeepEquals, []int64{2})
	c.Assert(result.DownError, qt.IsNotNil)
	c.Assert(result.ErrorText, qt.Contains, "failed to revert migration 1")
}

func TestPrepareDownExecute_FailurePathReportsFirstRollbackAttempt(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	migrationsDir, conn := prepareDownFixture(c, "DROP TABLE no_such_table;")
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasmigrate.PrepareDown(ctx, conn, atlasmigrate.DownOptions{
		Dir:           migrationsDir,
		FS:            os.DirFS(migrationsDir),
		TargetVersion: 1,
	})
	c.Assert(err, qt.IsNil)

	result, err := plan.Execute(ctx)

	c.Assert(err, qt.ErrorMatches, `(?s)error rolling back migrations: .*no_such_table.*`)
	c.Assert(result.Reverted, qt.IsTrue)
	c.Assert(result.RevertedVersions, qt.HasLen, 0)
	c.Assert(result.DownError, qt.IsNotNil)
	c.Assert(result.ErrorText, qt.Contains, "failed to revert migration 2")
}

func TestPrepareDown_FailurePathValidatesOptions(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := connectSQLite(c, filepath.Join(t.TempDir(), "validate.db"))
	defer dbschema.CloseAndWarn(conn)

	_, err := atlasmigrate.PrepareDown(ctx, nil, atlasmigrate.DownOptions{Dir: "migrations"})
	c.Assert(err, qt.ErrorMatches, `migrate down requires database connection`)

	_, err = atlasmigrate.PrepareDown(ctx, conn, atlasmigrate.DownOptions{})
	c.Assert(err, qt.ErrorMatches, `migrate down requires migration directory`)

	_, err = atlasmigrate.PrepareDown(ctx, conn, atlasmigrate.DownOptions{Dir: "migrations"})
	c.Assert(err, qt.ErrorMatches, `migrate down requires migration filesystem`)

	_, err = atlasmigrate.PrepareDown(ctx, conn, atlasmigrate.DownOptions{
		Dir:           "migrations",
		FS:            os.DirFS(t.TempDir()),
		TargetVersion: -1,
	})
	c.Assert(err, qt.ErrorMatches, `migrate down target version must be greater than or equal to zero`)
}
