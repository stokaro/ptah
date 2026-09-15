package migratedown

// White-box testing required: executeRollback is the one call that hands the
// migrator its context for a file-based rollback, and a canceled command driven
// through Execute is refused when it connects, before this call is reached.
// Canceling a rollback mid-run through its public surface needs a signal, a
// process and a server that can hold a statement open, which
// integration/migrations_interrupt_unix_e2e_test.go provides.

import (
	"context"
	"io"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/cliobs"
	"ptah.run/migration/migrator"
)

func TestExecuteRollbackRollsBackUnderALiveCommandContext(t *testing.T) {
	c := qt.New(t)
	mig := newAppliedInterruptContextMigrator(c)

	err := executeRollback(interruptContextCommand(context.Background()), rollbackExecution{migrator: mig}, cliobs.NewEmitter(io.Discard, nil))

	c.Assert(err, qt.IsNil)
	status, err := mig.GetMigrationStatus(context.Background())
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(0))
}

// TestExecuteRollbackStopsWhenTheCommandIsCanceled is the destructive half of
// an interrupt: an operator who stops a rollback must find the migration still
// applied, not a down body that ran on a context nothing could cancel.
func TestExecuteRollbackStopsWhenTheCommandIsCanceled(t *testing.T) {
	c := qt.New(t)
	mig := newAppliedInterruptContextMigrator(c)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := executeRollback(interruptContextCommand(ctx), rollbackExecution{migrator: mig}, cliobs.NewEmitter(io.Discard, nil))

	c.Assert(err, qt.ErrorIs, context.Canceled)
	status, err := mig.GetMigrationStatus(context.Background())
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))
}

func interruptContextCommand(ctx context.Context) *cobra.Command {
	cmd := &cobra.Command{}
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetContext(ctx)
	return cmd
}

// newAppliedInterruptContextMigrator returns a migrator over a SQLite database
// where migration 1 is already applied, so a rollback to version 0 has work.
func newAppliedInterruptContextMigrator(c *qt.C) *migrator.Migrator {
	c.Helper()

	dbURL := atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "ptah.db"))
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"0000000001_create_widgets.up.sql":   {Data: []byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n")},
		"0000000001_create_widgets.down.sql": {Data: []byte("DROP TABLE widgets;\n")},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(context.Background()), qt.IsNil)
	return mig
}
