package migrate_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migrate"
)

// TestMigrateGenerateHonorsTheCommandContext pins that the run is governed by
// the context the command was given.
//
// It was governed by a context built from --connect-timeout instead, derived
// from context.Background(), so the command context reached nothing on this
// path: a caller cancelling it changed nothing, and the connect budget --
// 10 seconds by default, documented as "maximum time to wait when establishing
// the initial database connection" -- bounded planning, rendering and file
// publication as well. On a slow runner that expired mid-publication and was
// reported as `error creating migration files: context deadline exceeded`,
// naming the step that noticed rather than the flag that set it
// (stokaro/ptah#1749).
//
// Cancellation is what the assertion uses, rather than a short timeout,
// because a timing test here would be the thing it is meant to prevent: the
// window between a SQLite connect (~0.1ms) and a whole generate run (~35ms) is
// too narrow to survive a runner 1.4-1.9x slower, which is precisely the
// machine stokaro/ptah#1812 measured.
func TestMigrateGenerateHonorsTheCommandContext(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaFile := filepath.Join(dir, "schema.sql")
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.WriteFile(schemaFile, []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := migrate.NewMigrateGenerateCommand()
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{
		"--schema-file", schemaFile,
		"--db-url", "sqlite:///" + filepath.Join(dir, "ptah.db"),
		"--migrations-dir", migrationsDir,
		"--name", "init",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil,
		qt.Commentf("a cancelled command context must stop the run; it reached nothing on this path"))
	// And it stopped before writing anything, which is the half that matters:
	// a run that ignored the cancellation would leave a migration behind.
	_, statErr := os.Stat(migrationsDir)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue,
		qt.Commentf("a cancelled run must not publish a migration directory"))
}
