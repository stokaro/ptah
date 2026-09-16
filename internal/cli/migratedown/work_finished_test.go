package migratedown_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/migratedown"
)

// A rollback that ran to the end reports its work finished, so an interrupt
// arriving between the last rollback and the process exit does not tell the
// operator a completed rollback was stopped (stokaro/ptah#3314).

func TestMigrateDownReportsFinishedWork_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := writeHashedWidgetsDir(c)
	dbPath := filepath.Join(c.TempDir(), "finished.db")
	applyWidgets(c, dir, dbPath)

	finished, out, err := runDownUnderWorkReport(c,
		"--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(finished, qt.IsTrue)
	c.Assert(tableCensus(c, dbPath), qt.Not(qt.Contains), "widgets")
}

func TestMigrateDownReportsFinishedWork_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := writeHashedWidgetsDir(c)
	dbPath := filepath.Join(c.TempDir(), "unfinished.db")
	applyWidgets(c, dir, dbPath)
	rewriteDownFile(c, dir)

	finished, _, err := runDownUnderWorkReport(c,
		"--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(err, qt.IsNotNil)
	c.Assert(finished, qt.IsFalse)
}

// runDownUnderWorkReport runs the command under a context carrying a work
// report, which is what the root command installs, and answers whether the run
// reported its work finished.
func runDownUnderWorkReport(c *qt.C, args ...string) (finished bool, output string, err error) {
	c.Helper()

	ctx, report := cmdutil.WithWorkReport(context.Background())
	cmd := migratedown.NewMigrateDownCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	cmd.SetContext(ctx)
	err = cmd.Execute()
	return report(), out.String(), err
}
