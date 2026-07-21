package migratedown_test

import (
	"context"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/pflag"

	"github.com/stokaro/ptah/cmd/internal/cliobs"
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
	c.Assert(cmd.Flag(cliobs.LogFormatFlagName), qt.IsNotNil)
	c.Assert(cmd.Flag(cliobs.LogLevelFlagName), qt.IsNotNil)
	c.Assert(cmd.Flag(cliobs.MetricsAddrFlagName), qt.IsNotNil)
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

func TestMigrateDownCommandDeclinedConfirmationPrintsCanceled(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	tempDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(tempDir, "000001_create_declined.up.sql"), []byte("CREATE TABLE declined_down (id INTEGER PRIMARY KEY);"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(tempDir, "000001_create_declined.down.sql"), []byte("DROP TABLE declined_down;"), 0o600), qt.IsNil)

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
	})

	out, err := captureStdIO(c, "NO\n", cmd.Execute)
	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "Migration rollback canceled.")
	resetMigrateDownCommandForTest(c, cmd)

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))
}

func TestMigrateDownCommandRejectsRelativeTraversalDirectory(t *testing.T) {
	c := qt.New(t)
	cmd := migratedown.NewMigrateDownCommand()
	resetMigrateDownCommandForTest(c, cmd)
	cmd.SetArgs([]string{
		"--db-url", "sqlite://ignored",
		"--migrations-dir", "../outside",
		"--target", "0",
		"--confirm",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `.*outside allowed root.*`)
	resetMigrateDownCommandForTest(c, cmd)
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

func captureStdIO(c *qt.C, input string, run func() error) (string, error) {
	c.Helper()

	oldStdin := os.Stdin
	oldStdout := os.Stdout
	defer func() {
		os.Stdin = oldStdin
		os.Stdout = oldStdout
	}()

	inR, inW, err := os.Pipe()
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(inR.Close(), qt.IsNil) }()

	_, err = inW.WriteString(input)
	c.Assert(err, qt.IsNil)
	c.Assert(inW.Close(), qt.IsNil)

	outR, outW, err := os.Pipe()
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(outR.Close(), qt.IsNil) }()

	os.Stdin = inR
	os.Stdout = outW

	runErr := run()
	c.Assert(outW.Close(), qt.IsNil)

	output, err := io.ReadAll(outR)
	c.Assert(err, qt.IsNil)
	return string(output), runErr
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
		"log-format":             "text",
		"log-level":              "info",
		"metrics-addr":           "",
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
