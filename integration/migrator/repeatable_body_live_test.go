//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
	"ptah.run/internal/sqlident"
	"ptah.run/migration/migrator"
)

// repeatableBodyFixtures are file contents whose bytes the record has to keep
// as they are: one longer than the 65,535 bytes a MySQL BLOB holds, padded
// with two-byte characters; one with CRLF line ends and the bytes of a byte
// order mark, which a text column or a line-oriented reader would change; one
// that runs outside a transaction; and an empty one, which is a kept body and
// not a missing one. The byte order mark sits inside the comment because a
// file that starts with one fails to execute on these servers, versioned or
// repeatable alike.
func repeatableBodyFixtures(engine repeatableEngine) []repeatableBodyFixture {
	view := engine.widgetIDs()
	return []repeatableBodyFixture{
		{name: "longer than a BLOB", body: "-- " + strings.Repeat("é", 40000) + "\n" + view},
		{
			name: "CRLF and byte order mark bytes",
			body: "-- widget ids \xEF\xBB\xBF\r\n" + strings.ReplaceAll(view, "\n", "\r\n"),
		},
		{name: "no_transaction", body: "-- +ptah no_transaction\n" + view},
		{name: "empty", body: ""},
	}
}

// repeatableBodyFixture is one file content whose bytes the record keeps.
type repeatableBodyFixture struct {
	name, body string
}

// keptBody is what the target's record keeps for identity: the bytes, and
// whether the column holds a value at all, which an empty body must.
type keptBody struct {
	Body string
	Kept bool
}

func (r *repeatableEngineTarget) body(c *qt.C, identity string) keptBody {
	c.Helper()
	var body []byte
	var kept int
	query := sqlutil.Rebind(r.engine.dialect,
		"SELECT body, CASE WHEN body IS NULL THEN 0 ELSE 1 END FROM "+r.recordTable()+" WHERE "+
			sqlident.Ident(r.engine.dialect, "identity")+" = ?")
	c.Assert(r.conn.QueryRowContext(context.Background(), query, identity).Scan(&body, &kept), qt.IsNil)
	return keptBody{Body: string(body), Kept: kept == 1}
}

// recordTable is the record's name as the migrator spells it, which on Oracle
// is quoted and so is not the name a bare spelling folds to.
func (r *repeatableEngineTarget) recordTable() string {
	return sqlident.Qualified(r.engine.dialect, "", "schema_migrations_repeatable")
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
		for _, fixture := range repeatableBodyFixtures(engine) {
			t.Run(engine.name+": "+fixture.name, func(t *testing.T) {
				c := qt.New(t)
				r := newRepeatableEngineTarget(c, t, engine)
				c.Assert(r.up(c, repeatableEngineMigrations(map[string]string{"R__widget_ids.sql": fixture.body}),
					migrator.MigrateUpOptions{}), qt.IsNil)

				applied, err := r.appliedRepeatables(c, map[string]string{"R__widget_ids.sql": engine.widerIDs()})

				c.Assert(err, qt.IsNil)
				c.Assert(r.body(c, "widget_ids"), qt.DeepEquals, keptBody{Body: fixture.body, Kept: true})
				c.Assert(applied, qt.HasLen, 1)
				c.Assert(applied[0].Path, qt.Equals, "R__widget_ids.sql")
				c.Assert(applied[0].Checksum, qt.Equals, repeatableDigest(fixture.body))
			})
		}
	}
}

func TestRepeatableBodyOnEnginesLive_FailurePath(t *testing.T) {
	for _, engine := range repeatableEngines {
		// A failed attempt records its state and error and leaves the digest
		// and the body of the content that applied before it.
		t.Run(engine.name+": a failed attempt leaves the body and the digest that applied", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			fsys := repeatableEngineMigrations(map[string]string{"R__widget_ids.sql": engine.widgetIDs()})
			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.IsNil)
			writeRepeatable(fsys, "R__widget_ids.sql", repeatableEnginesFailing)

			err := r.up(c, fsys, migrator.MigrateUpOptions{})

			c.Assert(err, qt.ErrorAs, new(*migrator.RepeatableMigrationError))
			record := r.record(c, fsys, "widget_ids")
			c.Assert(record.State, qt.Equals, migrator.RepeatableStateFailed)
			c.Assert(record.AppliedChecksum, qt.Equals, repeatableDigest(engine.widgetIDs()))
			c.Assert(record.AttemptChecksum, qt.Equals, repeatableDigest(repeatableEnginesFailing))
			c.Assert(r.body(c, "widget_ids"), qt.DeepEquals, keptBody{Body: engine.widgetIDs(), Kept: true})
		})

		t.Run(engine.name+": a changed file whose record keeps no body cannot be replayed", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			c.Assert(r.up(c, repeatableEngineMigrations(map[string]string{"R__widget_ids.sql": engine.widgetIDs()}),
				migrator.MigrateUpOptions{}), qt.IsNil)
			r.exec(c, "UPDATE "+r.recordTable()+" SET body = NULL")

			applied, err := r.appliedRepeatables(c, map[string]string{"R__widget_ids.sql": engine.widerIDs()})

			var unreplayable *migrator.UnreplayableHistoryError
			c.Assert(err, qt.ErrorAs, &unreplayable)
			c.Assert(unreplayable.Reason, qt.Equals, migrator.UnreplayableChanged)
			c.Assert(err, qt.ErrorMatches, `.*R__widget_ids\.sql changed after it applied, and its record keeps no body for the content that applied`)
			c.Assert(applied, qt.HasLen, 0)
		})
	}
}
