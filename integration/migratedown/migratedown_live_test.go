//go:build integration

package migratedown_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migratedown"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestMigrateDownCommand_Integration(t *testing.T) {
	c := qt.New(t)
	dbURL := requiredMigrateDownDatabaseURL(t)
	tableName := fmt.Sprintf("ptah_migrate_down_%d", time.Now().UnixNano())
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
	mig, err := migrator.NewFSMigrator(conn, os.DirFS(tempDir))
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(t.Context()), qt.IsNil)
	status, err := mig.GetMigrationStatus(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))

	cmd := migratedown.NewMigrateDownCommand()
	cmd.SetArgs([]string{
		"--db-url", dbURL,
		"--migrations-dir", tempDir,
		"--target", "0",
		"--confirm",
	})
	c.Assert(cmd.Execute(), qt.IsNil)

	finalStatus, err := mig.GetMigrationStatus(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(finalStatus.CurrentVersion, qt.Equals, int64(0))
}

func requiredMigrateDownDatabaseURL(t *testing.T) string {
	t.Helper()
	return dbtarget.URL(t, dbtarget.PostgreSQL)
}
