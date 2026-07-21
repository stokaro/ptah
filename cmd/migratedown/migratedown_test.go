package migratedown_test

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/pflag"

	"github.com/stokaro/ptah/cmd/migratedown"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestMigrateDownCommand_Creation(t *testing.T) {
	c := qt.New(t)

	cmd := migratedown.NewMigrateDownCommand()
	c.Assert(cmd, qt.IsNotNil)
	c.Assert(cmd.Use, qt.Equals, "down")
	c.Assert(cmd.Short, qt.Contains, "Roll back migrations")
	c.Assert(cmd.Flag("migration-lock-timeout"), qt.IsNotNil)
}

func TestMigrateDownCommandPreflightHookAbortPreventsRollback(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	tempDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(tempDir, "000001_create_guarded.up.sql"), []byte("CREATE TABLE guarded_down (id INTEGER PRIMARY KEY);"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(tempDir, "000001_create_guarded.down.sql"), []byte("DROP TABLE guarded_down;"), 0o600), qt.IsNil)

	dbURL := (&url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "ptah.db")}).String()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	mig, err := migrator.NewFSMigrator(conn, os.DirFS(tempDir))
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(ctx), qt.IsNil)

	cmd := migratedown.NewMigrateDownCommand()
	resetMigrateDownCommandForTest(c, cmd)
	cmd.SetArgs([]string{
		"--db-url", dbURL,
		"--migrations-dir", tempDir,
		"--target", "0",
		"--confirm",
		"--pre-down-hook", "echo rollback backup refused; exit 8",
	})

	err = cmd.Execute()
	c.Assert(err, qt.ErrorMatches, "(?s).*down pre-flight custom command hook failed: exit status 8\nrollback backup refused")
	resetMigrateDownCommandForTest(c, cmd)

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))

	var count int
	err = conn.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'guarded_down'").Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 1)
}

// TestMigrateDownCommand_Integration tests the actual migration logic
// This test requires a real database connection and is skipped if no test database is available
func TestMigrateDownCommand_Integration(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set, skipping integration test")
	}

	c := qt.New(t)

	// Create a temporary directory for test migrations
	tempDir := t.TempDir()

	// Create test migration files
	upSQL := `CREATE TABLE test_table (id INTEGER PRIMARY KEY);`
	downSQL := `DROP TABLE test_table;`

	err := os.WriteFile(tempDir+"/001_create_test_table.up.sql", []byte(upSQL), 0644) //nolint:gosec // 0644 is fine
	c.Assert(err, qt.IsNil)

	err = os.WriteFile(tempDir+"/001_create_test_table.down.sql", []byte(downSQL), 0644) //nolint:gosec // 0644 is fine
	c.Assert(err, qt.IsNil)

	// Connect to database
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	// Apply migration first
	migrationsFS := os.DirFS(tempDir)
	mig, err := migrator.NewFSMigrator(conn, migrationsFS)
	c.Assert(err, qt.IsNil)
	err = mig.MigrateUp(context.Background())
	c.Assert(err, qt.IsNil)

	// Verify migration was applied
	status, err := mig.GetMigrationStatus(context.Background())
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, 1)

	// Test the migrate down command
	cmd := migratedown.NewMigrateDownCommand()
	resetMigrateDownCommandForTest(c, cmd)
	cmd.SetArgs([]string{
		"--db-url", dbURL,
		"--migrations-dir", tempDir,
		"--target", "0",
		"--confirm", // Skip confirmation prompt
	})

	err = cmd.Execute()
	c.Assert(err, qt.IsNil)
	resetMigrateDownCommandForTest(c, cmd)

	// Verify migration was rolled back
	finalStatus, err := mig.GetMigrationStatus(context.Background())
	c.Assert(err, qt.IsNil)
	c.Assert(finalStatus.CurrentVersion, qt.Equals, 0)
}

func resetMigrateDownCommandForTest(c *qt.C, cmd interface{ Flag(string) *pflag.Flag }) {
	c.Helper()
	for name, value := range map[string]string{
		"db-url":                 "",
		"migrations-dir":         "",
		"target":                 "0",
		"dir-format":             "auto",
		"atlas-env":              "",
		"dry-run":                "false",
		"verbose":                "false",
		"confirm":                "false",
		"exec-order":             "linear",
		"migration-lock-timeout": "",
		"lock-timeout":           "",
		"statement-timeout":      "",
		"pre-down-hook":          "",
		"pg-dump-to":             "",
		"mysqldump-to":           "",
		"webhook":                "",
		"connect-timeout":        "10s",
		"config":                 "",
		"env":                    "",
		"migrations-schema":      "",
		"migrations-table":       "",
		"revision-format":        "ptah",
	} {
		flag := cmd.Flag(name)
		c.Assert(flag, qt.IsNotNil, qt.Commentf("flag %s", name))
		c.Assert(flag.Value.Set(value), qt.IsNil)
		flag.Changed = false
	}
}
