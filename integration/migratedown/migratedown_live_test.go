//go:build integration

package migratedown_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/cli/migratedown"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/revisiontable"
	"ptah.run/migration/migrator"
)

// TestMigrateDownCommand_Integration migrates up and down through a revision
// table of its own. The database is shared with every other test on the
// runner, and a test that leaves a version 1 in the default revision table
// would otherwise make this migrator refuse its own migration 1 as a checksum
// mismatch.
func TestMigrateDownCommand_Integration(t *testing.T) {
	c := qt.New(t)
	dbURL := requiredMigrateDownDatabaseURL(t)
	tableName := fmt.Sprintf("ptah_migrate_down_%d", time.Now().UnixNano())
	revisionTable := tableName + "_revisions"
	tempDir := t.TempDir()

	c.Assert(os.WriteFile(
		filepath.Join(tempDir, "001_create_test_table.up.sql"),
		fmt.Appendf(nil, "CREATE TABLE %s (id INTEGER PRIMARY KEY);", tableName),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(tempDir, "001_create_test_table.down.sql"),
		fmt.Appendf(nil, "DROP TABLE %s;", tableName),
		0o600,
	), qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	defer dropMigrateDownTables(c, conn, tableName, revisionTable)
	mig, err := migrator.NewFSMigrator(conn, os.DirFS(tempDir))
	c.Assert(err, qt.IsNil)
	mig = mig.WithMigrationsTable("", revisionTable)
	c.Assert(mig.MigrateUp(t.Context()), qt.IsNil)
	status, err := mig.GetMigrationStatus(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))

	cmd := migratedown.NewMigrateDownCommand()
	cmd.SetArgs([]string{
		"--db-url", dbURL,
		"--migrations-dir", tempDir,
		"--migrations-table", revisionTable,
		"--target", "0",
		"--confirm",
	})
	c.Assert(cmd.Execute(), qt.IsNil)

	finalStatus, err := mig.GetMigrationStatus(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(finalStatus.CurrentVersion, qt.Equals, int64(0))
}

// dropMigrateDownTables drops the table the migration creates, in case the test
// stopped before migrating down, and every table the migrator kept beside
// revisionTable.
func dropMigrateDownTables(c *qt.C, conn *dbschema.DatabaseConnection, tableName, revisionTable string) {
	c.Helper()
	names := append([]string{tableName}, revisiontable.Configured("", revisionTable)...)
	for _, name := range names {
		statement, err := builtin.RenderSQL(platform.Postgres, ast.NewDropTable(name).SetIfExists().SetCascade())
		c.Assert(err, qt.IsNil)
		_, err = conn.ExecContext(context.Background(), statement)
		c.Check(err, qt.IsNil)
	}
}

func requiredMigrateDownDatabaseURL(t *testing.T) string {
	t.Helper()
	return dbtarget.URL(t, dbtarget.PostgreSQL)
}
