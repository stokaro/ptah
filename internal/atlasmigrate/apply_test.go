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

func TestPrepareApplyExecute_HappyPathAppliesSelectedAmount(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c, migrationsDir, "1_one.sql", "CREATE TABLE apply_amount_one (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigrationFile(c, migrationsDir, "2_two.sql", "CREATE TABLE apply_amount_two (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigrationFile(c, migrationsDir, "3_three.sql", "CREATE TABLE apply_amount_three (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c, filepath.Join(dir, "apply.db"))
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
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
	c.Assert(sqliteTableExists(c, conn, "apply_amount_one"), qt.IsTrue)
	c.Assert(sqliteTableExists(c, conn, "apply_amount_two"), qt.IsTrue)
	c.Assert(sqliteTableExists(c, conn, "apply_amount_three"), qt.IsFalse)
}

func TestPrepareApplyExecute_BaselineRecordsAtlasRevisions(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c, migrationsDir, "1_one.sql", "CREATE TABLE baseline_one (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigrationFile(c, migrationsDir, "2_two.sql", "CREATE TABLE baseline_two (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigrationFile(c, migrationsDir, "3_three.sql", "CREATE TABLE baseline_three (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c, filepath.Join(dir, "baseline.db"))
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:             migrationsDir,
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
	c.Assert(sqliteTableExists(c, conn, "baseline_one"), qt.IsFalse)
	c.Assert(sqliteTableExists(c, conn, "baseline_two"), qt.IsFalse)
	c.Assert(sqliteTableExists(c, conn, "baseline_three"), qt.IsTrue)
	c.Assert(sqliteAtlasRevisionVersions(c, conn), qt.DeepEquals, []string{"1", "2", "3"})
}

func TestPrepareApplyExecute_DryRunBaselinePlansRemaining(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c, migrationsDir, "1_one.sql", "CREATE TABLE dry_baseline_one (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigrationFile(c, migrationsDir, "2_two.sql", "CREATE TABLE dry_baseline_two (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigrationFile(c, migrationsDir, "3_three.sql", "CREATE TABLE dry_baseline_three (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c, filepath.Join(dir, "dry-baseline.db"))
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:             migrationsDir,
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
	c.Assert(sqliteTableExists(c, conn, "dry_baseline_one"), qt.IsFalse)
	c.Assert(sqliteTableExists(c, conn, "dry_baseline_two"), qt.IsFalse)
	c.Assert(sqliteTableExists(c, conn, "dry_baseline_three"), qt.IsFalse)
}

func TestPrepareApplyExecute_NoopReturnsResult(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c, migrationsDir, "1_noop.sql", "CREATE TABLE apply_noop (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c, filepath.Join(dir, "noop.db"))
	defer dbschema.CloseAndWarn(conn)
	firstPlan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)
	_, err = firstPlan.Execute(ctx)
	c.Assert(err, qt.IsNil)

	plan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
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

func TestPrepareApplyExecute_ReturnsPlannedResultOnApplyError(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c, migrationsDir, "1_error.sql", "CREATE TABLE apply_error_before (id INTEGER PRIMARY KEY); SELECT * FROM missing_table;")
	conn := connectSQLite(c, filepath.Join(dir, "error.db"))
	defer dbschema.CloseAndWarn(conn)
	plan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
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
	c.Assert(sqliteTableExists(c, conn, "apply_error_before"), qt.IsFalse)
}

func TestPrepareApply_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("nil database connection", func(c *qt.C) {
		plan, err := atlasmigrate.PrepareApply(context.Background(), nil, atlasmigrate.ApplyOptions{
			Dir: c.TempDir(),
		})
		c.Assert(err, qt.ErrorMatches, "migrate apply requires database connection")
		c.Assert(plan.SelectedVersions, qt.HasLen, 0)
	})

	c.Run("missing migration directory", func(c *qt.C) {
		conn := connectSQLite(c, filepath.Join(c.TempDir(), "missing-dir.db"))
		defer dbschema.CloseAndWarn(conn)

		plan, err := atlasmigrate.PrepareApply(context.Background(), conn, atlasmigrate.ApplyOptions{})
		c.Assert(err, qt.ErrorMatches, "migrate apply requires migration directory")
		c.Assert(plan.SelectedVersions, qt.HasLen, 0)
	})

	c.Run("ambiguous target", func(c *qt.C) {
		conn := connectSQLite(c, filepath.Join(c.TempDir(), "ambiguous.db"))
		defer dbschema.CloseAndWarn(conn)

		plan, err := atlasmigrate.PrepareApply(context.Background(), conn, atlasmigrate.ApplyOptions{
			Dir:       c.TempDir(),
			Amount:    1,
			ToVersion: 1,
		})
		c.Assert(err, qt.ErrorMatches, "amount argument and --to-version cannot both be set")
		c.Assert(plan.SelectedVersions, qt.HasLen, 0)
	})

	c.Run("negative target version", func(c *qt.C) {
		conn := connectSQLite(c, filepath.Join(c.TempDir(), "negative-target.db"))
		defer dbschema.CloseAndWarn(conn)

		plan, err := atlasmigrate.PrepareApply(context.Background(), conn, atlasmigrate.ApplyOptions{
			Dir:       c.TempDir(),
			ToVersion: -1,
		})
		c.Assert(err, qt.ErrorMatches, "migrate apply target version must be greater than or equal to zero")
		c.Assert(plan.SelectedVersions, qt.HasLen, 0)
	})

	c.Run("negative baseline version", func(c *qt.C) {
		conn := connectSQLite(c, filepath.Join(c.TempDir(), "negative-baseline.db"))
		defer dbschema.CloseAndWarn(conn)

		plan, err := atlasmigrate.PrepareApply(context.Background(), conn, atlasmigrate.ApplyOptions{
			Dir:             c.TempDir(),
			BaselineVersion: -1,
		})
		c.Assert(err, qt.ErrorMatches, "migrate apply baseline version must be greater than or equal to zero")
		c.Assert(plan.SelectedVersions, qt.HasLen, 0)
	})

	c.Run("dry-run baseline without matching migrations", func(c *qt.C) {
		dir := c.TempDir()
		migrationsDir := filepath.Join(dir, "migrations")
		writeAtlasApplyMigrationFile(c, migrationsDir, "3_three.sql", "CREATE TABLE missing_baseline_three (id INTEGER PRIMARY KEY);")
		conn := connectSQLite(c, filepath.Join(dir, "missing-baseline.db"))
		defer dbschema.CloseAndWarn(conn)

		plan, err := atlasmigrate.PrepareApply(context.Background(), conn, atlasmigrate.ApplyOptions{
			Dir:             migrationsDir,
			DryRun:          true,
			BaselineVersion: 2,
		})
		c.Assert(err, qt.ErrorMatches, "no migrations found at or below baseline version 2")
		c.Assert(plan.SelectedVersions, qt.HasLen, 0)
	})
}

func writeAtlasApplyMigrationFile(c *qt.C, dir, name, sql string) {
	c.Helper()
	c.Assert(os.MkdirAll(dir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600), qt.IsNil)
}

func sqliteTableExists(c *qt.C, conn *dbschema.DatabaseConnection, table string) bool {
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

func sqliteAtlasRevisionVersions(c *qt.C, conn *dbschema.DatabaseConnection) []string {
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
