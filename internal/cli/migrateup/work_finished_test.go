package migrateup_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/migrateup"
)

// The reports below are what keeps a fully migrated database from being
// reported as an interrupted run. The root command replaces the status of an
// interrupted process with the signal's, and the verb is the only place that
// knows whether the signal stopped anything (stokaro/ptah#3314).

func TestMigrateUpReportsFinishedWork_HappyPath(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeUpMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "finished.db")

	finished, out, err := runUpUnderWorkReport(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(finished, qt.IsTrue)
}

func TestMigrateUpReportsFinishedWork_FailurePath(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeUpMigrations(t)
	broken := "CREATE TABLE payments (id INTEGER PRIMARY KEY);\nNOT SQL;\n"
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "0000000003_broken.up.sql"), []byte(broken), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "0000000003_broken.down.sql"),
		[]byte("DROP TABLE payments;\n"), 0o600), qt.IsNil)
	dbPath := filepath.Join(t.TempDir(), "unfinished.db")

	finished, _, err := runUpUnderWorkReport(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir)

	c.Assert(err, qt.IsNotNil)
	c.Assert(finished, qt.IsFalse)
}

// TestMigrateUpReportsFinishedWorkWithNothingToDo covers the run an operator
// repeats: everything is applied already, the verb does no work, and the work
// it was asked for is still finished.
func TestMigrateUpReportsFinishedWorkWithNothingToDo(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeUpMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "again.db")
	args := []string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", migrationsDir}

	_, out, err := runUpUnderWorkReport(c, args...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	finished, out, err := runUpUnderWorkReport(c, args...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(finished, qt.IsTrue)
}

// runUpUnderWorkReport runs the command under a context carrying a work report,
// which is what the root command installs, and answers whether the run reported
// its work finished.
func runUpUnderWorkReport(c *qt.C, args ...string) (finished bool, output string, err error) {
	c.Helper()

	ctx, report := cmdutil.WithWorkReport(context.Background())
	cmd := migrateup.NewMigrateUpCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	cmd.SetContext(ctx)
	err = cmd.Execute()
	return report(), out.String(), err
}
