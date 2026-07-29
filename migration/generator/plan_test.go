package generator_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasmigrate"
	"github.com/stokaro/ptah/internal/atlasurl"
	"github.com/stokaro/ptah/migration/generator"
)

func TestPlanMigration_DoesNotWriteArtifacts(t *testing.T) {
	c := qt.New(t)
	plan, outputDir := newSQLiteMigrationPlan(c)

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

	plan := newSQLiteMigrationPlanAt(c, outputDir)

	c.Assert(plan, qt.IsNotNil)
	for _, path := range pendingPaths {
		_, err = os.Stat(path)
		c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	}
	files, err := plan.WriteFilesContext(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.IsNotNil)
	c.Assert(files.Version < 9_999_999_999, qt.IsTrue)
}

func TestMigrationPlanWriteFiles_HappyPath(t *testing.T) {
	c := qt.New(t)
	plan, outputDir := newSQLiteMigrationPlan(c)

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
	plan, _ := newSQLiteMigrationPlan(c)
	files, err := plan.WriteFiles()
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.IsNotNil)

	files, err = plan.WriteFiles()

	c.Assert(err, qt.ErrorMatches, `migration plan has already been written`)
	c.Assert(files, qt.IsNil)
}

func TestMigrationPlanWriteFiles_RejectsChangedDirectory(t *testing.T) {
	c := qt.New(t)
	plan, outputDir := newSQLiteMigrationPlan(c)
	concurrentMigration := filepath.Join(outputDir, "20000101000000_concurrent.up.sql")
	c.Assert(os.WriteFile(concurrentMigration, []byte("SELECT 1;\n"), 0o600), qt.IsNil)

	files, err := plan.WriteFiles()

	c.Assert(err, qt.ErrorIs, generator.ErrMigrationDirectoryChanged)
	c.Assert(files, qt.IsNil)
	matches, err := filepath.Glob(filepath.Join(outputDir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.DeepEquals, []string{concurrentMigration})
}

func TestMigrationPlanWriteFilesContext_RejectsConcurrentUse(t *testing.T) {
	c := qt.New(t)
	plan, outputDir := newSQLiteMigrationPlan(c)
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
	plan, outputDir := newSQLiteMigrationPlan(c)
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
	plan, outputDir := newSQLiteMigrationPlan(c)
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
	opts := newSQLiteMigrationOptions(c, outputDir)
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

func newSQLiteMigrationPlan(c *qt.C) (*generator.MigrationPlan, string) {
	c.Helper()
	outputDir := c.TempDir()
	return newSQLiteMigrationPlanAt(c, outputDir), outputDir
}

func newSQLiteMigrationPlanAt(c *qt.C, outputDir string) *generator.MigrationPlan {
	c.Helper()
	opts := newSQLiteMigrationOptions(c, outputDir)
	plan, err := generator.PlanMigration(c.Context(), opts)
	c.Assert(err, qt.IsNil)
	return plan
}

func newSQLiteMigrationOptions(
	c *qt.C,
	outputDir string,
) generator.GenerateMigrationOptions {
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
