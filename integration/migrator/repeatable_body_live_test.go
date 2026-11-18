//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
	"ptah.run/migration/migrator"
)

// repeatableEnginesLongBody is a view body longer than the 65,535 bytes a
// MySQL BLOB or TEXT holds, padded with a comment of two-byte characters, so
// both the column's size and its byte fidelity decide whether it reads back.
var repeatableEnginesLongBody = "-- " + strings.Repeat("é", 40000) + "\n" + repeatableEnginesView

// body reads the bytes the target's record keeps for identity.
func (r *repeatableEngineTarget) body(c *qt.C, identity string) []byte {
	c.Helper()
	var body []byte
	query := sqlutil.Rebind(r.engine.dialect, "SELECT body FROM schema_migrations_repeatable WHERE identity = ?")
	c.Assert(r.conn.QueryRowContext(context.Background(), query, identity).Scan(&body), qt.IsNil)
	return body
}

// exec runs each statement on the target outside any migration.
func (r *repeatableEngineTarget) exec(c *qt.C, statements ...string) {
	c.Helper()
	for _, statement := range statements {
		_, err := r.conn.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil)
	}
}

// appliedRepeatables reads the history a drift check replays, against fsys.
func (r *repeatableEngineTarget) appliedRepeatables(c *qt.C, fsys map[string]string) ([]migrator.AppliedRepeatable, error) {
	c.Helper()
	history, err := r.migrator(c, repeatableEngineMigrations(fsys)).AppliedHistory(context.Background())
	return history.Repeatables, err
}

// The record keeps the body that applied, byte for byte, so a history read
// against a directory whose file changed still names the content the server
// ran.
func TestRepeatableBodyOnEnginesLive_HappyPath(t *testing.T) {
	for _, engine := range repeatableEngines {
		t.Run(engine.name+": the body that applied is kept and read back", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			c.Assert(r.up(c, repeatableEngineMigrations(map[string]string{"R__widget_ids.sql": repeatableEnginesLongBody}),
				migrator.MigrateUpOptions{}), qt.IsNil)

			applied, err := r.appliedRepeatables(c, map[string]string{"R__widget_ids.sql": repeatableEnginesWider})

			c.Assert(err, qt.IsNil)
			c.Assert(string(r.body(c, "widget_ids")), qt.Equals, repeatableEnginesLongBody)
			c.Assert(applied, qt.HasLen, 1)
			c.Assert(applied[0].Path, qt.Equals, "R__widget_ids.sql")
			c.Assert(applied[0].Checksum, qt.Equals, repeatableDigest(repeatableEnginesLongBody))
		})

		// The next run that executes repeatable work adds the column with the
		// engine's binary type and fills it.
		t.Run(engine.name+": a record created without the body column gets it", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			fsys := repeatableEngineMigrations(map[string]string{"R__widget_ids.sql": repeatableEnginesView})
			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.IsNil)
			r.exec(c, "ALTER TABLE schema_migrations_repeatable DROP COLUMN body")
			c.Assert(r.columns(c, "schema_migrations_repeatable"), qt.Equals, 10)
			writeRepeatable(fsys, "R__widget_ids.sql", repeatableEnginesLongBody)

			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.IsNil)

			c.Assert(r.columns(c, "schema_migrations_repeatable"), qt.Equals, 11)
			c.Assert(string(r.body(c, "widget_ids")), qt.Equals, repeatableEnginesLongBody)
		})
	}
}

func TestRepeatableBodyOnEnginesLive_FailurePath(t *testing.T) {
	for _, engine := range repeatableEngines {
		t.Run(engine.name+": a changed file whose record keeps no body cannot be replayed", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			c.Assert(r.up(c, repeatableEngineMigrations(map[string]string{"R__widget_ids.sql": repeatableEnginesView}),
				migrator.MigrateUpOptions{}), qt.IsNil)
			r.exec(c, "UPDATE schema_migrations_repeatable SET body = NULL")

			applied, err := r.appliedRepeatables(c, map[string]string{"R__widget_ids.sql": repeatableEnginesWider})

			var unreplayable *migrator.UnreplayableHistoryError
			c.Assert(err, qt.ErrorAs, &unreplayable)
			c.Assert(unreplayable.Reason, qt.Equals, migrator.UnreplayableChanged)
			c.Assert(err, qt.ErrorMatches, `.*R__widget_ids\.sql changed after it applied, and its record keeps no body for the content that applied`)
			c.Assert(applied, qt.HasLen, 0)
		})
	}
}
