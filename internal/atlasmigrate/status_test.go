package atlasmigrate_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasmigrate"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestStatus_SQLiteMainRevisionsSchema(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigrationFile(c, migrationsDir, "1_one.sql", "CREATE TABLE status_main_revisions_schema_one (id INTEGER PRIMARY KEY);")
	conn := connectSQLite(c, filepath.Join(dir, "status-main-revisions-schema.db"))
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:             migrationsDir,
		ExecOrder:       migrator.ExecOrderLinear,
		TxMode:          migrator.MigrationTxModeFile,
		RevisionsSchema: "main",
	})
	c.Assert(err, qt.IsNil)
	_, err = plan.Execute(ctx)
	c.Assert(err, qt.IsNil)

	got, err := atlasmigrate.Status(ctx, conn, atlasmigrate.StatusOptions{
		Dir:             migrationsDir,
		RevisionsSchema: "main",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got.Status.CurrentVersion, qt.Equals, int64(1))
	c.Assert(got.Status.PendingMigrations, qt.HasLen, 0)
	c.Assert(got.AppliedRevisions, qt.HasLen, 1)
	c.Assert(got.AppliedRevisions[0].Version, qt.Equals, int64(1))
	c.Assert(got.AppliedRevisions[0].Description, qt.Equals, "One")
	c.Assert(got.AppliedRevisions[0].OperatorVersion, qt.Equals, "Ptah")
}
