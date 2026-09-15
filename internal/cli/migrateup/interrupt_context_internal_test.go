package migrateup

// White-box testing required: applyPendingMigrations is the one call that hands
// the migrator its context, and a canceled command driven through Execute is
// refused when it connects, before this call is reached. Canceling a command
// mid-run through its public surface needs a signal, a process and a server
// that can hold a statement open, which integration/migrations_interrupt_unix_e2e_test.go
// provides.

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
	"ptah.run/migration/migrator"
)

func TestApplyPendingMigrationsAppliesUnderALiveCommandContext(t *testing.T) {
	c := qt.New(t)
	mig := newInterruptContextMigrator(c)

	outcome := applyPendingMigrations(interruptContextCommand(context.Background()), mig, &options{}, 0, nil)

	c.Assert(outcome.runErr, qt.IsNil)
	c.Assert(outcome.statusErr, qt.IsNil)
	c.Assert(outcome.status.CurrentVersion, qt.Equals, int64(1))
}

// TestApplyPendingMigrationsStopsWhenTheCommandIsCanceled pins both halves of
// what an interrupted run owes its caller. The migrator watches the command's
// context, so a canceled command applies nothing; and the status is still read
// afterwards, because the run that stopped is the one whose caller most needs
// to know what the database holds.
func TestApplyPendingMigrationsStopsWhenTheCommandIsCanceled(t *testing.T) {
	c := qt.New(t)
	mig := newInterruptContextMigrator(c)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	outcome := applyPendingMigrations(interruptContextCommand(ctx), mig, &options{}, 0, nil)

	c.Assert(outcome.runErr, qt.ErrorIs, context.Canceled)
	c.Assert(outcome.statusErr, qt.IsNil)
	c.Assert(outcome.status.CurrentVersion, qt.Equals, int64(0))
	c.Assert(outcome.status.PendingMigrations, qt.DeepEquals, []int64{1})
}

func interruptContextCommand(ctx context.Context) *cobra.Command {
	cmd := &cobra.Command{}
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetContext(ctx)
	return cmd
}

func newInterruptContextMigrator(c *qt.C) *migrator.Migrator {
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
	return mig
}
