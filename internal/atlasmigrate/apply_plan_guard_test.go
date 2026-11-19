package atlasmigrate_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasmigrate"
	"ptah.run/migration/migrator"
)

// TestPrepareApplyExecuteTimesTheMigrationsAfterThePlanGuard holds the run's
// clock to the migrations: a guard that takes its time, as a drift check's
// replay does, is over before StartedAt, which the report times every
// migration from.
func TestPrepareApplyExecuteTimesTheMigrationsAfterThePlanGuard(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c, migrationsDir, "1_one.sql", "CREATE TABLE guard_clock (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c, filepath.Join(dir, "apply.db"))
	defer dbschema.CloseAndWarn(conn)
	plan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)
	var guardDone time.Time
	plan = plan.WithPlanGuard(func(context.Context, migrator.MigrationPlan) error {
		time.Sleep(50 * time.Millisecond)
		guardDone = time.Now()
		return nil
	})

	result, err := plan.Execute(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Applied, qt.IsTrue)
	c.Assert(guardDone.IsZero(), qt.IsFalse)
	c.Assert(result.StartedAt.Before(guardDone), qt.IsFalse, qt.Commentf("started %s, guard done %s", result.StartedAt, guardDone))
	c.Assert(result.EndedAt.Before(result.StartedAt), qt.IsFalse)
}
