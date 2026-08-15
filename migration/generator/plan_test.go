package generator_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/migration/generator"
)

func TestPlanMigration_DoesNotWriteArtifacts(t *testing.T) {
	c := qt.New(t)
	plan, outputDir := newSQLiteMigrationPlan(c.TB)

	matches, err := filepath.Glob(filepath.Join(outputDir, "*.sql"))

	c.Assert(err, qt.IsNil)
	c.Assert(plan, qt.IsNotNil)
	c.Assert(matches, qt.HasLen, 0)
}

func TestPlanMigration_RecoversPendingPublicationBeforePlanning(t *testing.T) {
	c := qt.New(t)
	outputDir := c.TempDir()
	pendingPaths, err := generator.WritePendingPublicationForTest(outputDir)
	c.Assert(err, qt.IsNil)

	plan := newSQLiteMigrationPlanAt(c.TB, outputDir)

	c.Assert(plan, qt.IsNotNil)
	for _, path := range pendingPaths {
		_, err = os.Stat(path)
		c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	}
	files, err := plan.WriteFilesContext(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.IsNotNil)
	c.Assert(files.Files, qt.HasLen, 1)
	c.Assert(files.Files[0].Version < 9_999_999_999, qt.IsTrue)
}

func TestMigrationPlanWriteFiles_HappyPath(t *testing.T) {
	c := qt.New(t)
	plan, outputDir := newSQLiteMigrationPlan(c.TB)

	files, err := plan.WriteFiles()

	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.IsNotNil)
	c.Assert(files.Files, qt.HasLen, 1)
	matches, err := filepath.Glob(filepath.Join(outputDir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.HasLen, 2)
}

func TestMigrationPlanWriteFiles_FailurePath(t *testing.T) {
	c := qt.New(t)
	plan, _ := newSQLiteMigrationPlan(c.TB)
	files, err := plan.WriteFiles()
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.IsNotNil)

	files, err = plan.WriteFiles()

	c.Assert(err, qt.ErrorMatches, `migration plan has already been written`)
	c.Assert(files, qt.IsNil)
}

// TestMigrationPlanWriteFiles_FailedPublicationSpendsThePlan is the portable
// half of the handle-release measurement, and the reason the release is safe:
// once a publication attempt has returned, the plan no longer holds the
// migration directory, so a second attempt has nothing to publish through and
// says so instead of reaching for a released handle.
//
// Retrying is not what the caller wants anyway. Both the contents the plan
// verifies against and the version it chose describe the directory as it was
// before the attempt, so the honest retry is a fresh PlanMigration.
func TestMigrationPlanWriteFiles_FailedPublicationSpendsThePlan(t *testing.T) {
	c := qt.New(t)
	plan, outputDir := newSQLiteMigrationPlan(c.TB)
	concurrentMigration := filepath.Join(outputDir, "20000101000000_concurrent.up.sql")
	c.Assert(os.WriteFile(concurrentMigration, []byte("SELECT 1;\n"), 0o600), qt.IsNil)
	files, err := plan.WriteFiles()
	c.Assert(err, qt.ErrorIs, generator.ErrMigrationDirectoryChanged)
	c.Assert(files, qt.IsNil)

	files, err = plan.WriteFiles()

	c.Assert(err, qt.ErrorMatches, `migration plan was released by a failed publication`)
	c.Assert(files, qt.IsNil)
	matches, err := filepath.Glob(filepath.Join(outputDir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.DeepEquals, []string{concurrentMigration})
}

func TestMigrationPlanWriteFiles_RejectsChangedDirectory(t *testing.T) {
	c := qt.New(t)
	plan, outputDir := newSQLiteMigrationPlan(c.TB)
	concurrentMigration := filepath.Join(outputDir, "20000101000000_concurrent.up.sql")
	c.Assert(os.WriteFile(concurrentMigration, []byte("SELECT 1;\n"), 0o600), qt.IsNil)

	files, err := plan.WriteFiles()

	c.Assert(err, qt.ErrorIs, generator.ErrMigrationDirectoryChanged)
	c.Assert(files, qt.IsNil)
	matches, err := filepath.Glob(filepath.Join(outputDir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.DeepEquals, []string{concurrentMigration})
}

func TestMigrationPlanWriteFiles_RejectsHistoryChangedBeforePlanning(t *testing.T) {
	c := qt.New(t)
	outputDir := c.TempDir()
	migrationPath := filepath.Join(outputDir, "0000000001_seed.up.sql")
	c.Assert(os.WriteFile(migrationPath, []byte("SELECT 1;\n"), 0o600), qt.IsNil)
	authorized, err := migrationsnapshot.CaptureDirectory(outputDir)
	c.Assert(err, qt.IsNil)
	// This mutation lands before PlanMigration binds the directory, so the
	// ordinary plan-vs-publication contents check sees only the changed bytes.
	// The authorized snapshot is the additional boundary that must reject it.
	c.Assert(os.WriteFile(migrationPath, []byte("SELECT 2;\n"), 0o600), qt.IsNil)
	opts := newSQLiteMigrationOptions(c.TB, outputDir)
	opts.PriorMigrationsFS = authorized
	plan, err := generator.PlanMigration(t.Context(), opts)
	c.Assert(err, qt.IsNil)

	files, err := plan.WriteFilesContext(t.Context())

	c.Assert(err, qt.ErrorIs, generator.ErrMigrationDirectoryChanged)
	c.Assert(files, qt.IsNil)
	contents, err := os.ReadFile(migrationPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "SELECT 2;\n")
}

func TestMigrationPlanWriteFilesContext_RejectsConcurrentUse(t *testing.T) {
	c := qt.New(t)
	plan, outputDir := newSQLiteMigrationPlan(c.TB)
	lockHeld := make(chan struct{})
	releaseLock := make(chan struct{})
	lockDone := make(chan error, 1)
	go func() {
		lockDone <- atlasmigrate.WithMigrationDirectoryLock(
			t.Context(),
			outputDir,
			0,
			func(context.Context) error {
				close(lockHeld)
				<-releaseLock
				return nil
			},
		)
	}()
	<-lockHeld

	type writeResult struct {
		files *generator.MigrationFiles
		err   error
	}
	writeDone := make(chan writeResult, 1)
	go func() {
		files, err := plan.WriteFilesContext(t.Context())
		writeDone <- writeResult{files: files, err: err}
	}()
	time.Sleep(50 * time.Millisecond)

	files, err := plan.WriteFilesContext(t.Context())

	c.Assert(err, qt.ErrorIs, generator.ErrMigrationPlanInUse)
	c.Assert(files, qt.IsNil)
	close(releaseLock)
	firstResult := <-writeDone
	c.Assert(firstResult.err, qt.IsNil)
	c.Assert(firstResult.files, qt.IsNotNil)
	c.Assert(<-lockDone, qt.IsNil)
}

func TestMigrationPlanWriteFilesContext_RejectsCanceledContext(t *testing.T) {
	c := qt.New(t)
	plan, outputDir := newSQLiteMigrationPlan(c.TB)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	files, err := plan.WriteFilesContext(ctx)

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(files, qt.IsNil)
	matches, err := filepath.Glob(filepath.Join(outputDir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.HasLen, 0)
}

func TestMigrationPlanWriteFilesContext_CancelsWhileWaitingForLock(t *testing.T) {
	c := qt.New(t)
	plan, outputDir := newSQLiteMigrationPlan(c.TB)
	lockHeld := make(chan struct{})
	releaseLock := make(chan struct{})
	lockDone := make(chan error, 1)
	go func() {
		lockDone <- atlasmigrate.WithMigrationDirectoryLock(
			t.Context(),
			outputDir,
			0,
			func(context.Context) error {
				close(lockHeld)
				<-releaseLock
				return nil
			},
		)
	}()
	<-lockHeld

	ctx, cancel := context.WithCancel(t.Context())
	type writeResult struct {
		files *generator.MigrationFiles
		err   error
	}
	writeDone := make(chan writeResult, 1)
	go func() {
		files, err := plan.WriteFilesContext(ctx)
		writeDone <- writeResult{files: files, err: err}
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	result := <-writeDone
	close(releaseLock)

	c.Assert(result.err, qt.ErrorIs, context.Canceled)
	c.Assert(result.files, qt.IsNil)
	c.Assert(<-lockDone, qt.IsNil)
	matches, err := filepath.Glob(filepath.Join(outputDir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.HasLen, 0)
}

func TestGenerateMigration_CancelsWhileWaitingForRecoveryLock(t *testing.T) {
	c := qt.New(t)
	outputDir := c.TempDir()
	opts := newSQLiteMigrationOptions(c.TB, outputDir)
	lockHeld := make(chan struct{})
	releaseLock := make(chan struct{})
	lockDone := make(chan error, 1)
	go func() {
		lockDone <- atlasmigrate.WithMigrationDirectoryLock(
			t.Context(),
			outputDir,
			0,
			func(context.Context) error {
				close(lockHeld)
				<-releaseLock
				return nil
			},
		)
	}()
	<-lockHeld
	ctx, cancel := context.WithCancel(t.Context())
	type generateResult struct {
		files *generator.MigrationFiles
		err   error
	}
	generateDone := make(chan generateResult, 1)
	go func() {
		files, err := generator.GenerateMigration(ctx, opts)
		generateDone <- generateResult{files: files, err: err}
	}()
	time.Sleep(50 * time.Millisecond)

	cancel()
	result := <-generateDone
	close(releaseLock)

	c.Assert(result.err, qt.ErrorIs, context.Canceled)
	c.Assert(result.files, qt.IsNil)
	c.Assert(<-lockDone, qt.IsNil)
}

func newSQLiteMigrationPlan(tb testing.TB) (*generator.MigrationPlan, string) {
	c := qt.New(tb)
	c.Helper()
	outputDir := c.TempDir()
	return newSQLiteMigrationPlanAt(c.TB, outputDir), outputDir
}

func newSQLiteMigrationPlanAt(tb testing.TB, outputDir string) *generator.MigrationPlan {
	c := qt.New(tb)
	c.Helper()
	opts := newSQLiteMigrationOptions(c.TB, outputDir)
	plan, err := generator.PlanMigration(c.Context(), opts)
	c.Assert(err, qt.IsNil)
	return plan
}

func newSQLiteMigrationOptions(
	tb testing.TB,
	outputDir string,
) generator.GenerateMigrationOptions {
	c := qt.New(tb)
	c.Helper()
	devURL := atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "dev.db"))
	conn, err := dbschema.ConnectToDatabase(c.Context(), devURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})
	return generator.GenerateMigrationOptions{
		Generated: &goschema.Database{
			Tables: []goschema.Table{
				{StructName: "User", Name: "users"},
			},
			Fields: []goschema.Field{
				{
					StructName: "User",
					FieldName:  "ID",
					Name:       "id",
					Type:       "INTEGER",
					Primary:    true,
				},
			},
		},
		DBConn:        conn,
		MigrationName: "create_users",
		OutputDir:     outputDir,
	}
}
