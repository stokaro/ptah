package atlasmigrate_test

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasmigrate"
	"ptah.run/migration/migrator"
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
	var operatorVersion string
	var applied, total int
	c.Assert(conn.QueryRowContext(
		ctx,
		"SELECT operator_version, applied, total FROM atlas_schema_revisions WHERE version = '2'",
	).Scan(&operatorVersion, &applied, &total), qt.IsNil)
	c.Assert(operatorVersion, qt.Equals, "Ptah/down")
	c.Assert(applied, qt.Equals, 0)
	c.Assert(total, qt.Equals, 1)
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

	_, err = atlasmigrate.PrepareDown(ctx, conn, atlasmigrate.DownOptions{
		Dir:           "migrations",
		FS:            os.DirFS(t.TempDir()),
		TargetVersion: 1,
		Limit:         1,
	})
	c.Assert(err, qt.ErrorMatches, `migrate down takes a target version or a limit, not both`)
}

// TestPrepareDownExecute_LimitCountsBackFromTheLastApplied reverts the last
// Limit applied migrations, and a limit above the history reverts what is
// applied. The plan names the version it stops at, read from the history.
func TestPrepareDownExecute_LimitCountsBackFromTheLastApplied(t *testing.T) {
	tests := []struct {
		name         string
		limit        uint64
		wantTarget   int64
		wantReverted []int64
		wantVersions []string
	}{
		{name: "the last one", limit: 1, wantTarget: 1, wantReverted: []int64{2}, wantVersions: []string{"1"}},
		{name: "the whole history", limit: 2, wantTarget: 0, wantReverted: []int64{2, 1}, wantVersions: make([]string, 0)},
		{name: "more than the history", limit: 5, wantTarget: 0, wantReverted: []int64{2, 1}, wantVersions: make([]string, 0)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			migrationsDir, conn := prepareDownFixture(c, "ALTER TABLE down_users DROP COLUMN email;")
			defer dbschema.CloseAndWarn(conn)

			plan, err := atlasmigrate.PrepareDown(ctx, conn, atlasmigrate.DownOptions{
				Dir:   migrationsDir,
				FS:    os.DirFS(migrationsDir),
				Limit: test.limit,
			})
			c.Assert(err, qt.IsNil)
			c.Assert(plan.TargetVersion, qt.Equals, test.wantTarget)

			result, err := plan.Execute(ctx)

			c.Assert(err, qt.IsNil)
			c.Assert(result.TargetVersion, qt.Equals, test.wantTarget)
			c.Assert(result.RevertedVersions, qt.DeepEquals, test.wantReverted)
			c.Assert(sqliteAtlasRevisionVersions(c, conn), qt.DeepEquals, test.wantVersions)
		})
	}
}

// TestParseDownAmount_HappyPath reads the amount argument as `migrate apply`
// reads its own, so the two verbs take the same spellings.
// TestPrepareDownExecute_FailurePathRefusesAHistoryThatChanged applies a third
// migration between PrepareDown and Execute, as a `migrate apply` running
// beside the rollback does. PrepareDown reads the history without the
// migration lock and plans to revert version 2 down to version 1. Under the
// lock versions 3 and 2 stand above version 1, and the rollback would revert
// both while it reports 2, so Execute refuses and reverts nothing. The two
// calls are made one after the other, which orders the race with no timing.
func TestPrepareDownExecute_FailurePathRefusesAHistoryThatChanged(t *testing.T) {
	tests := []struct {
		name          string
		limit         uint64
		targetVersion int64
	}{
		{name: "counting back", limit: 1},
		{name: "to a version", targetVersion: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			migrationsDir, conn := prepareDownFixture(c, "ALTER TABLE down_users DROP COLUMN email;")
			defer dbschema.CloseAndWarn(conn)
			writeAtlasApplyMigrationFile(c, migrationsDir, "3_add_name.sql", "ALTER TABLE down_users ADD COLUMN name TEXT;")
			writeAtlasApplyMigrationFile(c, migrationsDir, "3_add_name.down.sql", "ALTER TABLE down_users DROP COLUMN name;")
			plan, err := atlasmigrate.PrepareDown(ctx, conn, atlasmigrate.DownOptions{
				Dir:           migrationsDir,
				FS:            os.DirFS(migrationsDir),
				Limit:         test.limit,
				TargetVersion: test.targetVersion,
			})
			c.Assert(err, qt.IsNil)
			c.Assert(plan.PlannedVersions, qt.DeepEquals, []int64{2})
			apply, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
				Dir:       migrationsDir,
				FS:        os.DirFS(migrationsDir),
				ExecOrder: migrator.ExecOrderLinear,
				TxMode:    migrator.MigrationTxModeFile,
			})
			c.Assert(err, qt.IsNil)
			_, err = apply.Execute(ctx)
			c.Assert(err, qt.IsNil)

			result, err := plan.Execute(ctx)

			c.Assert(err, qt.ErrorIs, atlasmigrate.ErrDownHistoryChanged)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(
				"error rolling back migrations: the applied history changed after the rollback was planned: "+
					"it reverts [2], and the migrations applied above version 1 are now [3 2]; "+
					"nothing was rolled back, run migrate down again"))
			c.Assert(result.Reverted, qt.IsFalse)
			c.Assert(result.RevertedVersions, qt.HasLen, 0)
			c.Assert(sqliteAtlasRevisionVersions(c, conn), qt.DeepEquals, []string{"1", "2", "3"})
		})
	}
}

func TestParseDownAmount_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want uint64
	}{
		{name: "no argument", args: nil, want: 0},
		{name: "one", args: []string{"1"}, want: 1},
		{name: "two", args: []string{"2"}, want: 2},
		{name: "plus sign", args: []string{"+1"}, want: 1},
		{name: "leading zero", args: []string{"02"}, want: 2},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := atlasmigrate.ParseDownAmount(test.args)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestParseDownAmount_FailurePath refuses what `migrate apply` refuses, in the
// same words, with the verb revert in the refusal of an amount below one.
func TestParseDownAmount_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "too many arguments", args: []string{"1", "2"}, wantErr: "accepts at most 1 arg(s), received 2"},
		{
			name:    "not a number",
			args:    []string{"two"},
			wantErr: `invalid amount argument "two" (invalid syntax). Omit the argument or pass a valid integer instead`,
		},
		{
			name:    "fraction",
			args:    []string{"1.5"},
			wantErr: `invalid amount argument "1.5" (invalid syntax). Omit the argument or pass a valid integer instead`,
		},
		{name: "zero", args: []string{"0"}, wantErr: "cannot revert '0' migration files"},
		{name: "negative", args: []string{"-1"}, wantErr: "cannot revert '-1' migration files"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := atlasmigrate.ParseDownAmount(test.args)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(got, qt.Equals, uint64(0))
		})
	}
}

// TestDownStopLimit_HappyPath says how many migrations each accepted
// combination reverts by counting back: the amount, the documented default of
// one, or none when a version or a tag names the stop.
func TestDownStopLimit_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		stop atlasmigrate.DownStop
		want uint64
	}{
		{name: "nothing names the stop", stop: atlasmigrate.DownStop{}, want: 1},
		{name: "an amount", stop: atlasmigrate.DownStop{Amount: 2}, want: 2},
		{name: "a version", stop: atlasmigrate.DownStop{ToVersion: "3", ToVersionGiven: true}, want: 0},
		{name: "a tag", stop: atlasmigrate.DownStop{ToTag: "v1", ToTagGiven: true}, want: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := test.stop.Limit()
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestDownStopLimit_FailurePath refuses two stops together and a version or
// tag that names nothing.
func TestDownStopLimit_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		stop    atlasmigrate.DownStop
		wantErr string
	}{
		{
			name:    "an amount and a version",
			stop:    atlasmigrate.DownStop{Amount: 2, ToVersion: "3", ToVersionGiven: true},
			wantErr: "the amount argument and --to-version both name where to stop; pass one",
		},
		{
			name:    "an amount and a tag",
			stop:    atlasmigrate.DownStop{Amount: 2, ToTag: "v1", ToTagGiven: true},
			wantErr: "the amount argument and --to-tag both name where to stop; pass one",
		},
		{
			name:    "a version and a tag",
			stop:    atlasmigrate.DownStop{ToVersion: "3", ToVersionGiven: true, ToTag: "v1", ToTagGiven: true},
			wantErr: "--to-version and --to-tag both name where to stop; pass one",
		},
		{
			name:    "an empty version",
			stop:    atlasmigrate.DownStop{ToVersionGiven: true},
			wantErr: "--to-version is empty: give it a version, or leave it out to revert the last applied migration",
		},
		{
			name:    "a blank tag",
			stop:    atlasmigrate.DownStop{ToTag: " ", ToTagGiven: true},
			wantErr: "--to-tag is empty: give it a tag, or leave it out to revert the last applied migration",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := test.stop.Limit()
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(got, qt.Equals, uint64(0))
		})
	}
}
