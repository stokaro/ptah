//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// unchangedRestart is a revision layout whose failed row a test makes equal
// to what a retry's restart writes, apart from the time the restart stamps,
// which a trigger keeps.
type unchangedRestart struct {
	name   string
	format migrator.RevisionTableFormat
	// settle makes the failed row equal to the restart's values.
	settle string
	// keep is the trigger that keeps the row's time on every rewrite.
	keep string
	// recorded reads what the retry recorded.
	recorded string
	want     string
}

var unchangedRestarts = []unchangedRestart{
	{
		name:   "ptah layout",
		format: migrator.RevisionTableFormatPtah,
		settle: "UPDATE schema_migrations SET state = 'pending', error = NULL, error_stmt = NULL, execution_time_ms = 0",
		keep: "CREATE TRIGGER keep_restart_time BEFORE UPDATE ON schema_migrations FOR EACH ROW " +
			"SET NEW.applied_at = OLD.applied_at",
		recorded: "SELECT state FROM schema_migrations WHERE version = 1",
		want:     "applied",
	},
	{
		name:   "atlas layout",
		format: migrator.RevisionTableFormatAtlas,
		settle: "UPDATE atlas_schema_revisions SET error = '', error_stmt = '', execution_time = 0",
		keep: "CREATE TRIGGER keep_restart_time BEFORE UPDATE ON atlas_schema_revisions FOR EACH ROW " +
			"SET NEW.executed_at = OLD.executed_at",
		recorded: "SELECT applied FROM atlas_schema_revisions WHERE version = '1'",
		want:     "1",
	},
}

// MySQL and MariaDB count the rows an UPDATE changed, not the rows it
// matched, and Ptah's connector does not ask for found rows. Here the retry's
// guarded restart matches the dirty row as the retry read it and writes the
// values the row already has, so the server counts no row: the row was
// settled to the restart's values, and a trigger keeps the time, as a retry
// within the second the row was written would. The retry is the only run and
// must restart, run the body and record it. Read as a lost claim, it would
// stop with ErrRevisionClaimLost.
func TestUnchangedRestartLive_HappyPath(t *testing.T) {
	for _, engine := range repeatableMySQLFamilyEngines {
		for _, layout := range unchangedRestarts {
			t.Run(engine.name+" "+layout.name, func(t *testing.T) {
				c := qt.New(t)
				r := newRepeatableEngineTarget(c, t, engine)
				c.Assert(restartMigrator(c, r, layout.format).MigrateUp(context.Background()), qt.IsNotNil)
				execAll(c, r.conn, strings.NewReplacer(), []string{"CREATE TABLE repairs (id INTEGER)", layout.settle, layout.keep})

				err := restartMigrator(c, r, layout.format).
					MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{AllowDirty: true})

				c.Assert(err, qt.IsNil)
				c.Assert(countRows(c, r.conn, "repairs"), qt.Equals, 1)
				c.Assert(readOne(c, r.conn, layout.recorded), qt.Equals, layout.want)
			})
		}
	}
}

// restartMigrator runs atlasRepairs in format on a connection of its own,
// outside a transaction, so its restart of a dirty row records on the
// migrator's own connection.
func restartMigrator(c *qt.C, r *repeatableEngineTarget, format migrator.RevisionTableFormat) *migrator.Migrator {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), r.url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	mig, err := migrator.NewFSMigrator(conn, atlasRepairs())
	c.Assert(err, qt.IsNil)
	return mig.WithRevisionTableFormat(format).WithTransactionMode(migrator.MigrationTxModeNone)
}
