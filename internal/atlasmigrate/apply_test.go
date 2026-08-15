package atlasmigrate_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestPrepareApplyExecute_HappyPathAppliesSelectedAmount(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "1_one.sql", "CREATE TABLE apply_amount_one (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "2_two.sql", "CREATE TABLE apply_amount_two (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "3_three.sql", "CREATE TABLE apply_amount_three (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c.TB, filepath.Join(dir, "apply.db"))
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
		Amount:    2,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.SelectedVersions, qt.DeepEquals, []int64{1, 2})

	result, err := plan.Execute(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Applied, qt.IsTrue)
	c.Assert(result.SelectedVersions, qt.DeepEquals, []int64{1, 2})
	c.Assert(result.FinalStatus.CurrentVersion, qt.Equals, int64(2))
	c.Assert(sqliteTableExists(c.TB, conn, "apply_amount_one"), qt.IsTrue)
	c.Assert(sqliteTableExists(c.TB, conn, "apply_amount_two"), qt.IsTrue)
	c.Assert(sqliteTableExists(c.TB, conn, "apply_amount_three"), qt.IsFalse)
}

func TestPrepareApplyExecute_AppliesConvertedExternalFormatFS(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "1_widgets.sql",
		"-- +goose Up\nCREATE TABLE goose_up (id INTEGER PRIMARY KEY);\n-- +goose Down\nCREATE TABLE goose_down (id INTEGER PRIMARY KEY);\n")
	migrationFS, err := resolveApplySource(
		os.DirFS(migrationsDir),
		migrationsDir,
		"goose",
		nil,
	)
	c.Assert(err, qt.IsNil)
	conn := connectSQLite(c.TB, filepath.Join(dir, "goose.db"))
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        migrationFS,
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.SelectedVersions, qt.DeepEquals, []int64{1})

	result, err := plan.Execute(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Applied, qt.IsTrue)
	// Only the converted up section ran; the down section was dropped during
	// conversion, so its table was never created.
	c.Assert(sqliteTableExists(c.TB, conn, "goose_up"), qt.IsTrue)
	c.Assert(sqliteTableExists(c.TB, conn, "goose_down"), qt.IsFalse)
}

func TestPrepareApplyExecute_BaselineRecordsAtlasRevisions(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "1_one.sql", "CREATE TABLE baseline_one (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "2_two.sql", "CREATE TABLE baseline_two (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "3_three.sql", "CREATE TABLE baseline_three (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c.TB, filepath.Join(dir, "baseline.db"))
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:             migrationsDir,
		FS:              os.DirFS(migrationsDir),
		ExecOrder:       migrator.ExecOrderLinear,
		TxMode:          migrator.MigrationTxModeFile,
		BaselineVersion: 2,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.CurrentVersion, qt.Equals, int64(2))
	c.Assert(plan.SelectedVersions, qt.DeepEquals, []int64{3})

	result, err := plan.Execute(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Applied, qt.IsTrue)
	c.Assert(result.FinalStatus.CurrentVersion, qt.Equals, int64(3))
	c.Assert(sqliteTableExists(c.TB, conn, "baseline_one"), qt.IsFalse)
	c.Assert(sqliteTableExists(c.TB, conn, "baseline_two"), qt.IsFalse)
	c.Assert(sqliteTableExists(c.TB, conn, "baseline_three"), qt.IsTrue)
	c.Assert(sqliteAtlasRevisionVersions(c.TB, conn), qt.DeepEquals, []string{"2", "3"})
}

func TestPrepareApplyExecute_SQLiteMainRevisionsSchema(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "1_one.sql", "CREATE TABLE main_revisions_schema_one (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c.TB, filepath.Join(dir, "main-revisions-schema.db"))
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:             migrationsDir,
		FS:              os.DirFS(migrationsDir),
		ExecOrder:       migrator.ExecOrderLinear,
		TxMode:          migrator.MigrationTxModeFile,
		RevisionsSchema: "main",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.SelectedVersions, qt.DeepEquals, []int64{1})
	c.Assert(plan.RevisionsTableIdentifier, qt.Equals, `"main"."atlas_schema_revisions"`)

	result, err := plan.Execute(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Applied, qt.IsTrue)
	c.Assert(result.FinalStatus.CurrentVersion, qt.Equals, int64(1))
	c.Assert(sqliteTableExists(c.TB, conn, "main_revisions_schema_one"), qt.IsTrue)
	c.Assert(sqliteAtlasRevisionVersions(c.TB, conn), qt.DeepEquals, []string{"1"})
}

func TestPrepareApplyExecute_DryRunBaselinePlansRemaining(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "1_one.sql", "CREATE TABLE dry_baseline_one (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "2_two.sql", "CREATE TABLE dry_baseline_two (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "3_three.sql", "CREATE TABLE dry_baseline_three (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c.TB, filepath.Join(dir, "dry-baseline.db"))
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:             migrationsDir,
		FS:              os.DirFS(migrationsDir),
		DryRun:          true,
		ExecOrder:       migrator.ExecOrderLinear,
		TxMode:          migrator.MigrationTxModeFile,
		BaselineVersion: 2,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.CurrentVersion, qt.Equals, int64(2))
	c.Assert(plan.SelectedVersions, qt.DeepEquals, []int64{3})

	result, err := plan.Execute(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Applied, qt.IsFalse)
	c.Assert(result.CurrentVersion, qt.Equals, int64(2))
	c.Assert(result.SelectedVersions, qt.DeepEquals, []int64{3})
	c.Assert(sqliteTableExists(c.TB, conn, "dry_baseline_one"), qt.IsFalse)
	c.Assert(sqliteTableExists(c.TB, conn, "dry_baseline_two"), qt.IsFalse)
	c.Assert(sqliteTableExists(c.TB, conn, "dry_baseline_three"), qt.IsFalse)
}

func TestPrepareApplyExecute_DryRunBaselineKeepsExactRevisionIdentity(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "10_half.sql", "CREATE TABLE dry_exact_half (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "20_two.sql", "CREATE TABLE dry_exact_two (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c.TB, filepath.Join(dir, "dry-exact-baseline.db"))
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasmigrate.PrepareApply(c.Context(), conn, atlasmigrate.ApplyOptions{
		Dir:              migrationsDir,
		FS:               os.DirFS(migrationsDir),
		DryRun:           true,
		ExecOrder:        migrator.ExecOrderLinear,
		TxMode:           migrator.MigrationTxModeFile,
		BaselineVersion:  10,
		RevisionVersions: map[int64]string{10: "1.5", 20: "2"},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.CurrentVersion, qt.Equals, int64(10))
	c.Assert(plan.CurrentKey, qt.Equals, "1.5")
	c.Assert(plan.CurrentKeySet, qt.IsTrue)
	c.Assert(plan.SelectedVersions, qt.DeepEquals, []int64{20})
	c.Assert(plan.SelectedKeys, qt.DeepEquals, []string{"2"})

	result, err := plan.Execute(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(result.CurrentKey, qt.Equals, "1.5")
	c.Assert(result.CurrentKeySet, qt.IsTrue)
	c.Assert(result.SelectedKeys, qt.DeepEquals, []string{"2"})
}

func TestPrepareApplyExecute_DryRunUsesStoredRevisionState(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "1_one.sql", "CREATE TABLE dry_state_one (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "2_two.sql", "CREATE TABLE dry_state_two (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "3_three.sql", "CREATE TABLE dry_state_three (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c.TB, filepath.Join(dir, "dry-state.db"))
	defer dbschema.CloseAndWarn(conn)

	applyPlan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
		Amount:    2,
	})
	c.Assert(err, qt.IsNil)
	_, err = applyPlan.Execute(ctx)
	c.Assert(err, qt.IsNil)

	dryRunPlan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		DryRun:    true,
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(dryRunPlan.CurrentVersion, qt.Equals, int64(2))
	c.Assert(dryRunPlan.Status.AppliedMigrations, qt.DeepEquals, []int64{1, 2})
	c.Assert(dryRunPlan.Status.PendingMigrations, qt.DeepEquals, []int64{3})
	c.Assert(dryRunPlan.SelectedVersions, qt.DeepEquals, []int64{3})

	result, err := dryRunPlan.Execute(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Applied, qt.IsFalse)
	c.Assert(sqliteAtlasRevisionVersions(c.TB, conn), qt.DeepEquals, []string{"1", "2"})
	c.Assert(sqliteTableExists(c.TB, conn, "dry_state_three"), qt.IsFalse)
}

func TestPrepareApplyExecute_DryRunRejectsDirtyRevision(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "1_users.sql", "CREATE TABLE dry_dirty_users (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c.TB, filepath.Join(dir, "dry-dirty.db"))
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
	_, err = conn.ExecContext(ctx, `UPDATE atlas_schema_revisions
SET applied = 0, total = 1, error = 'broken'
WHERE version = '1'`)
	c.Assert(err, qt.IsNil)

	dryRunPlan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		DryRun:    true,
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)

	result, err := dryRunPlan.Execute(ctx)
	c.Assert(err, qt.ErrorMatches, `error applying migrations: migration 1 is dirty:.*`)
	c.Assert(result.Applied, qt.IsFalse)
	c.Assert(result.ApplyError, qt.IsNotNil)
	c.Assert(sqliteAtlasRevisionVersions(c.TB, conn), qt.DeepEquals, []string{"1"})
	c.Assert(sqliteTableExists(c.TB, conn, "dry_dirty_users"), qt.IsTrue)
}

func TestPrepareApplyExecute_DirtyPreflightIsNotReportedAsApplyAttempt(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "1_users.sql", "CREATE TABLE dirty_apply_users (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c.TB, filepath.Join(dir, "dirty-apply.db"))
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
	_, err = conn.ExecContext(ctx, `UPDATE atlas_schema_revisions
SET applied = 0, total = 1, error = 'broken'
WHERE version = '1'`)
	c.Assert(err, qt.IsNil)

	retryPlan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)

	result, err := retryPlan.Execute(ctx)
	c.Assert(err, qt.ErrorMatches, `error applying migrations: migration 1 is dirty:.*`)
	c.Assert(result.Applied, qt.IsFalse)
	var dirty *migrator.DirtyMigrationError
	c.Assert(result.ApplyError, qt.ErrorAs, &dirty)
	c.Assert(sqliteAtlasRevisionVersions(c.TB, conn), qt.DeepEquals, []string{"1"})
	c.Assert(sqliteTableExists(c.TB, conn, "dirty_apply_users"), qt.IsTrue)
}

func TestPrepareApplyExecute_DryRunRejectsChecksumMismatch(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "1_users.sql", "CREATE TABLE dry_checksum_users (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c.TB, filepath.Join(dir, "dry-checksum.db"))
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
	_, err = conn.ExecContext(ctx, `UPDATE atlas_schema_revisions SET hash = 'deadbeef' WHERE version = '1'`)
	c.Assert(err, qt.IsNil)

	dryRunPlan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		DryRun:    true,
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)

	result, err := dryRunPlan.Execute(ctx)
	c.Assert(err, qt.ErrorMatches, `error applying migrations: migration 1 checksum mismatch:.*`)
	c.Assert(result.Applied, qt.IsFalse)
	c.Assert(result.ApplyError, qt.IsNotNil)
	c.Assert(sqliteAtlasRevisionVersions(c.TB, conn), qt.DeepEquals, []string{"1"})
	c.Assert(sqliteTableExists(c.TB, conn, "dry_checksum_users"), qt.IsTrue)
}

func TestPrepareApplyExecute_NoopReturnsResult(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "1_noop.sql", "CREATE TABLE apply_noop (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c.TB, filepath.Join(dir, "noop.db"))
	defer dbschema.CloseAndWarn(conn)
	firstPlan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)
	_, err = firstPlan.Execute(ctx)
	c.Assert(err, qt.IsNil)

	plan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Noop(), qt.IsTrue)

	result, err := plan.Execute(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Applied, qt.IsFalse)
	c.Assert(result.CurrentVersion, qt.Equals, int64(1))
	c.Assert(result.SelectedVersions, qt.HasLen, 0)
}

func TestPrepareApplyExecute_StalePlanUsesLockedState(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "1_stale.sql", "CREATE TABLE apply_stale (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c.TB, filepath.Join(dir, "stale.db"))
	defer dbschema.CloseAndWarn(conn)
	opts := atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	}

	stalePlan, err := atlasmigrate.PrepareApply(ctx, conn, opts)
	c.Assert(err, qt.IsNil)
	freshPlan, err := atlasmigrate.PrepareApply(ctx, conn, opts)
	c.Assert(err, qt.IsNil)
	_, err = freshPlan.Execute(ctx)
	c.Assert(err, qt.IsNil)

	result, err := stalePlan.ExecuteWithPreflight(
		ctx,
		func(context.Context, migrator.MigrationPlan) error {
			c.Fatal("preflight must not run for a fresh no-op plan")
			return nil
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Applied, qt.IsFalse)
	c.Assert(result.SelectedVersions, qt.HasLen, 0)
	c.Assert(result.CurrentVersion, qt.Equals, int64(1))
	c.Assert(result.FinalStatus, qt.IsNotNil)
	c.Assert(result.FinalStatus.CurrentVersion, qt.Equals, int64(1))
}

func TestPrepareApplyExecute_StaleDryRunUsesLockedState(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "1_stale.sql", "CREATE TABLE apply_stale_dry (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c.TB, filepath.Join(dir, "stale-dry.db"))
	defer dbschema.CloseAndWarn(conn)
	dryOpts := atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		DryRun:    true,
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	}

	staleDryPlan, err := atlasmigrate.PrepareApply(ctx, conn, dryOpts)
	c.Assert(err, qt.IsNil)
	applyOpts := dryOpts
	applyOpts.DryRun = false
	freshPlan, err := atlasmigrate.PrepareApply(ctx, conn, applyOpts)
	c.Assert(err, qt.IsNil)
	_, err = freshPlan.Execute(ctx)
	c.Assert(err, qt.IsNil)

	result, err := staleDryPlan.Execute(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Applied, qt.IsFalse)
	c.Assert(result.SelectedVersions, qt.HasLen, 0)
	c.Assert(result.CurrentVersion, qt.Equals, int64(1))
	c.Assert(result.FinalStatus, qt.IsNotNil)
	c.Assert(result.FinalStatus.CurrentVersion, qt.Equals, int64(1))
}

func TestPrepareApplyExecute_StaleNoopBecomesWork(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "1_reapply.sql", "CREATE TABLE apply_reappeared (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c.TB, filepath.Join(dir, "reappeared.db"))
	defer dbschema.CloseAndWarn(conn)
	opts := atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	}

	firstPlan, err := atlasmigrate.PrepareApply(ctx, conn, opts)
	c.Assert(err, qt.IsNil)
	_, err = firstPlan.Execute(ctx)
	c.Assert(err, qt.IsNil)
	staleNoopPlan, err := atlasmigrate.PrepareApply(ctx, conn, opts)
	c.Assert(err, qt.IsNil)
	c.Assert(staleNoopPlan.Noop(), qt.IsTrue)
	_, err = conn.ExecContext(ctx, "DROP TABLE apply_reappeared")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, "DELETE FROM atlas_schema_revisions WHERE version = '1'")
	c.Assert(err, qt.IsNil)
	var observed migrator.MigrationPlan

	result, err := staleNoopPlan.ExecuteWithPreflight(
		ctx,
		func(_ context.Context, plan migrator.MigrationPlan) error {
			observed = plan
			return nil
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(observed.CurrentVersion, qt.Equals, int64(0))
	c.Assert(observed.Versions, qt.DeepEquals, []int64{1})
	c.Assert(result.Applied, qt.IsTrue)
	c.Assert(result.CurrentVersion, qt.Equals, int64(0))
	c.Assert(result.SelectedVersions, qt.DeepEquals, []int64{1})
	c.Assert(result.FinalStatus.CurrentVersion, qt.Equals, int64(1))
	c.Assert(sqliteTableExists(c.TB, conn, "apply_reappeared"), qt.IsTrue)
}

func TestPrepareApplyExecute_StaleNoopDryRunBecomesWork(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "1_reapply.sql", "CREATE TABLE apply_reappeared_dry (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c.TB, filepath.Join(dir, "reappeared-dry.db"))
	defer dbschema.CloseAndWarn(conn)
	opts := atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	}

	firstPlan, err := atlasmigrate.PrepareApply(ctx, conn, opts)
	c.Assert(err, qt.IsNil)
	_, err = firstPlan.Execute(ctx)
	c.Assert(err, qt.IsNil)
	dryOpts := opts
	dryOpts.DryRun = true
	staleNoopPlan, err := atlasmigrate.PrepareApply(ctx, conn, dryOpts)
	c.Assert(err, qt.IsNil)
	c.Assert(staleNoopPlan.Noop(), qt.IsTrue)
	_, err = conn.ExecContext(ctx, "DROP TABLE apply_reappeared_dry")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, "DELETE FROM atlas_schema_revisions WHERE version = '1'")
	c.Assert(err, qt.IsNil)

	result, err := staleNoopPlan.Execute(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Applied, qt.IsFalse)
	c.Assert(result.CurrentVersion, qt.Equals, int64(0))
	c.Assert(result.SelectedVersions, qt.DeepEquals, []int64{1})
	c.Assert(result.FinalStatus, qt.IsNil)
	c.Assert(sqliteTableExists(c.TB, conn, "apply_reappeared_dry"), qt.IsFalse)
}

func TestPrepareApplyExecute_ValidationErrorUsesLockedPlan(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "1_valid.sql", "CREATE TABLE apply_valid (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "2_invalid.sql", "-- atlas:txmode bogus\n\nCREATE TABLE apply_invalid (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c.TB, filepath.Join(dir, "validation-plan.db"))
	defer dbschema.CloseAndWarn(conn)
	opts := atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
		Amount:    1,
	}

	stalePlan, err := atlasmigrate.PrepareApply(ctx, conn, opts)
	c.Assert(err, qt.IsNil)
	c.Assert(stalePlan.SelectedVersions, qt.DeepEquals, []int64{1})
	freshPlan, err := atlasmigrate.PrepareApply(ctx, conn, opts)
	c.Assert(err, qt.IsNil)
	_, err = freshPlan.Execute(ctx)
	c.Assert(err, qt.IsNil)

	result, err := stalePlan.ExecuteWithPreflight(
		ctx,
		func(context.Context, migrator.MigrationPlan) error {
			c.Fatal("preflight must not run after transaction-mode validation fails")
			return nil
		},
	)

	var txModeErr *migrator.AtlasTxModeDirectiveError
	c.Assert(err, qt.ErrorAs, &txModeErr)
	c.Assert(err, qt.ErrorMatches, `unknown txmode "bogus" found in file directive "2_invalid.sql"`)
	c.Assert(result.Applied, qt.IsFalse)
	c.Assert(result.CurrentVersion, qt.Equals, int64(1))
	c.Assert(result.SelectedVersions, qt.DeepEquals, []int64{2})
	c.Assert(result.ApplyError, qt.IsNotNil)
	c.Assert(sqliteTableExists(c.TB, conn, "apply_invalid"), qt.IsFalse)
}

func TestPrepareApplyExecute_ReturnsPlannedResultOnApplyError(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c.TB, migrationsDir, "1_error.sql", "CREATE TABLE apply_error_before (id INTEGER PRIMARY KEY); SELECT * FROM missing_table;")
	conn := connectSQLite(c.TB, filepath.Join(dir, "error.db"))
	defer dbschema.CloseAndWarn(conn)
	plan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)

	result, err := plan.Execute(ctx)

	c.Assert(err, qt.ErrorMatches, `(?s)error applying migrations: .*missing_table.*`)
	c.Assert(result.Applied, qt.IsTrue)
	c.Assert(result.ApplyError, qt.IsNotNil)
	c.Assert(result.ErrorText, qt.Contains, "missing_table")
	c.Assert(result.SelectedVersions, qt.DeepEquals, []int64{1})
	c.Assert(result.Status.CurrentVersion, qt.Equals, int64(0))
	c.Assert(result.EndedAt.IsZero(), qt.IsFalse)
	c.Assert(sqliteTableExists(c.TB, conn, "apply_error_before"), qt.IsFalse)
}

func TestPrepareApply_FailurePath(t *testing.T) {

	t.Run("nil database connection", func(t *testing.T) {
		c := qt.New(t)
		plan, err := atlasmigrate.PrepareApply(context.Background(), nil, atlasmigrate.ApplyOptions{
			Dir: c.TempDir(),
		})
		c.Assert(err, qt.ErrorMatches, "migrate apply requires database connection")
		c.Assert(plan.SelectedVersions, qt.HasLen, 0)
	})

	t.Run("missing migration directory", func(t *testing.T) {
		c := qt.New(t)
		conn := connectSQLite(c.TB, filepath.Join(c.TempDir(), "missing-dir.db"))
		defer dbschema.CloseAndWarn(conn)

		plan, err := atlasmigrate.PrepareApply(context.Background(), conn, atlasmigrate.ApplyOptions{})
		c.Assert(err, qt.ErrorMatches, "migrate apply requires migration directory")
		c.Assert(plan.SelectedVersions, qt.HasLen, 0)
	})

	t.Run("missing migration filesystem", func(t *testing.T) {
		c := qt.New(t)
		conn := connectSQLite(c.TB, filepath.Join(c.TempDir(), "missing-fs.db"))
		defer dbschema.CloseAndWarn(conn)

		plan, err := atlasmigrate.PrepareApply(context.Background(), conn, atlasmigrate.ApplyOptions{
			Dir: c.TempDir(),
		})
		c.Assert(err, qt.ErrorMatches, "migrate apply requires migration filesystem")
		c.Assert(plan.SelectedVersions, qt.HasLen, 0)
	})

	t.Run("negative baseline version", func(t *testing.T) {
		c := qt.New(t)
		conn := connectSQLite(c.TB, filepath.Join(c.TempDir(), "negative-baseline.db"))
		defer dbschema.CloseAndWarn(conn)

		plan, err := atlasmigrate.PrepareApply(context.Background(), conn, atlasmigrate.ApplyOptions{
			Dir:             c.TempDir(),
			FS:              os.DirFS(c.TempDir()),
			BaselineVersion: -1,
		})
		c.Assert(err, qt.ErrorMatches, "migrate apply baseline version must be greater than or equal to zero")
		c.Assert(plan.SelectedVersions, qt.HasLen, 0)
	})

	t.Run("dry-run baseline without matching migrations", func(t *testing.T) {
		c := qt.New(t)
		dir := c.TempDir()
		migrationsDir := filepath.Join(dir, "migrations")
		writeAtlasApplyMigrationFile(c.TB, migrationsDir, "3_three.sql", "CREATE TABLE missing_baseline_three (id INTEGER PRIMARY KEY);")
		conn := connectSQLite(c.TB, filepath.Join(dir, "missing-baseline.db"))
		defer dbschema.CloseAndWarn(conn)

		plan, err := atlasmigrate.PrepareApply(context.Background(), conn, atlasmigrate.ApplyOptions{
			Dir:             migrationsDir,
			FS:              os.DirFS(migrationsDir),
			DryRun:          true,
			BaselineVersion: 2,
		})
		c.Assert(err, qt.ErrorMatches, `baseline version "2" not found`)
		c.Assert(plan.SelectedVersions, qt.HasLen, 0)
	})
}

func TestParseApplyAmount_HappyPath(t *testing.T) {

	tests := []struct {
		name string
		args []string
		want uint64
	}{
		{
			name: "empty",
			args: nil,
			want: 0,
		},
		{
			name: "positive amount",
			args: []string{"2"},
			want: 2,
		},
		{
			name: "trimmed amount",
			args: []string{" 3 "},
			want: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := atlasmigrate.ParseApplyAmount(tt.args)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

func TestParseApplyAmount_FailurePath(t *testing.T) {

	t.Run("too many arguments", func(t *testing.T) {
		c := qt.New(t)
		got, err := atlasmigrate.ParseApplyAmount([]string{"1", "2"})
		c.Assert(err, qt.ErrorMatches, "accepts at most one amount argument")
		c.Assert(got, qt.Equals, uint64(0))
	})

	t.Run("invalid unsigned integer", func(t *testing.T) {
		c := qt.New(t)
		got, err := atlasmigrate.ParseApplyAmount([]string{"nope"})
		c.Assert(err, qt.ErrorMatches, `amount argument "nope" is not a valid unsigned integer: .*`)
		c.Assert(got, qt.Equals, uint64(0))
	})
}

func TestParseMigrationVersionFlag_HappyPath(t *testing.T) {

	tests := []struct {
		name  string
		value string
		want  int64
	}{
		{
			name:  "empty",
			value: "",
			want:  0,
		},
		{
			name:  "positive version",
			value: "42",
			want:  42,
		},
		{
			name:  "trimmed version",
			value: " 7 ",
			want:  7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := atlasmigrate.ParseMigrationVersionFlag("baseline", tt.value)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

func TestParseMigrationVersionFlag_FailurePath(t *testing.T) {

	t.Run("invalid integer", func(t *testing.T) {
		c := qt.New(t)
		got, err := atlasmigrate.ParseMigrationVersionFlag("baseline", "nope")
		c.Assert(err, qt.ErrorMatches, `--baseline "nope" is not a valid migration version: .*`)
		c.Assert(got, qt.Equals, int64(0))
	})

	t.Run("zero", func(t *testing.T) {
		c := qt.New(t)
		got, err := atlasmigrate.ParseMigrationVersionFlag("baseline", "0")
		c.Assert(err, qt.ErrorMatches, "--baseline must be greater than zero")
		c.Assert(got, qt.Equals, int64(0))
	})

	t.Run("negative", func(t *testing.T) {
		c := qt.New(t)
		got, err := atlasmigrate.ParseMigrationVersionFlag("baseline", "-1")
		c.Assert(err, qt.ErrorMatches, "--baseline must be greater than zero")
		c.Assert(got, qt.Equals, int64(0))
	})
}

func writeAtlasApplyMigrationFile(tb testing.TB, dir, name, sql string) {
	c := qt.New(tb)
	c.Helper()
	c.Assert(os.MkdirAll(dir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600), qt.IsNil)
}

func sqliteTableExists(tb testing.TB, conn *dbschema.DatabaseConnection, table string) bool {
	c := qt.New(tb)
	c.Helper()
	var count int
	err := conn.QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`,
		table,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count == 1
}

func sqliteAtlasRevisionVersions(tb testing.TB, conn *dbschema.DatabaseConnection) []string {
	c := qt.New(tb)
	c.Helper()
	rows, err := conn.QueryContext(context.Background(), `SELECT version FROM atlas_schema_revisions ORDER BY CAST(version AS INTEGER)`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	versions := make([]string, 0)
	for rows.Next() {
		var version string
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}
