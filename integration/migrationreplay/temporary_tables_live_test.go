//go:build integration

package migrationreplay_test

import (
	"context"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/devclean"
	"ptah.run/internal/migrationreplay"
	"ptah.run/migration/migrationfile"
)

func TestPostgresTemporaryTableReplay_HappyPath(t *testing.T) {
	c := qt.New(t)
	realmURL := newPostgresReplayRealmDatabase(c)
	realm, err := dbschema.ConnectToDatabase(c.Context(), realmURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(realm)
	neighbor, err := dbschema.ConnectToDatabase(c.Context(), realmURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(neighbor)
	snapshot := fstest.MapFS{
		"1_cleanup.sql": {Data: []byte(`
CREATE TABLE events (id integer PRIMARY KEY, value integer NOT NULL);
INSERT INTO events VALUES (1, 7), (2, 7), (3, 9);
CREATE TEMP TABLE first_ids AS SELECT id FROM events WHERE id = 2;
CREATE TEMP TABLE second_ids AS SELECT id FROM first_ids;
DELETE FROM events WHERE id IN (SELECT id FROM pg_temp.second_ids);
CREATE UNIQUE INDEX events_value_key ON events (value);
`)},
		"2_later.sql": {Data: []byte(`
INSERT INTO events SELECT id + 10, 20 FROM pg_temp.second_ids;
`)},
	}
	c.Assert(neighbor.WithSession(c.Context(), func(neighborSession *dbschema.DatabaseConnection) error {
		_, createErr := neighborSession.ExecContext(c.Context(), "CREATE TEMP TABLE neighbor_state (id integer); INSERT INTO neighbor_state VALUES (42)")
		c.Assert(createErr, qt.IsNil)
		for range 3 {
			var firstOID, secondOID uint32
			replayErr := migrationreplay.WithReplayedSnapshot(c.Context(), realm, snapshot, migrationfile.DirFormatAtlas, func(session *dbschema.DatabaseConnection, _ devclean.Baseline) error {
				var rows, distinctValues int
				c.Assert(session.QueryRowContext(c.Context(), "SELECT count(*), count(DISTINCT value) FROM events").Scan(&rows, &distinctValues), qt.IsNil)
				c.Assert(rows, qt.Equals, 3)
				c.Assert(distinctValues, qt.Equals, 3)
				c.Assert(session.QueryRowContext(c.Context(), "SELECT 'pg_temp.first_ids'::regclass::oid, 'pg_temp.second_ids'::regclass::oid").Scan(&firstOID, &secondOID), qt.IsNil)
				return nil
			})
			c.Assert(replayErr, qt.IsNil)
			waitForSessionEnd(c, realm, firstOID, secondOID)
			var permanentCount, neighborValue int
			c.Assert(realm.QueryRowContext(c.Context(), "SELECT count(*) FROM pg_tables WHERE schemaname = 'public'").Scan(&permanentCount), qt.IsNil)
			c.Assert(permanentCount, qt.Equals, 0)
			c.Assert(neighborSession.QueryRowContext(c.Context(), "SELECT id FROM pg_temp.neighbor_state").Scan(&neighborValue), qt.IsNil)
			c.Assert(neighborValue, qt.Equals, 42)
		}
		return nil
	}), qt.IsNil)
}

// waitForSessionEnd waits until the temporary tables a replay created are gone
// from the catalog. The replay discards its session, and PostgreSQL drops a
// session's temporary tables while its backend exits, which happens after the
// client has closed the connection: another session can still find them for a
// moment. A session returned to the pool would keep them for as long as it
// lived, so the wait reaching its bound is the failure this proves absent.
func waitForSessionEnd(c *qt.C, realm *dbschema.DatabaseConnection, first, second uint32) {
	c.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var left int
		c.Assert(realm.QueryRowContext(c.Context(), "SELECT count(*) FROM pg_class WHERE oid IN ($1, $2)", first, second).Scan(&left), qt.IsNil)
		if left == 0 {
			return
		}
		c.Assert(time.Now().Before(deadline), qt.IsTrue,
			qt.Commentf("%d temporary tables of the replayed session are still in pg_class", left))
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPostgresTemporaryTableReplay_StatementFailure(t *testing.T) {
	c := qt.New(t)
	realm, err := dbschema.ConnectToDatabase(c.Context(), newPostgresReplayRealmDatabase(c))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(realm)
	snapshot := fstest.MapFS{
		"1_failure.sql": {Data: []byte(`-- atlas:txmode none

CREATE TABLE durable_stage (id integer);
CREATE TEMP TABLE temporary_stage AS SELECT 1 AS id;
INSERT INTO durable_stage SELECT id FROM temporary_stage;
SELECT missing_column FROM temporary_stage;
`)},
	}
	err = migrationreplay.WithReplayedSnapshot(c.Context(), realm, snapshot, migrationfile.DirFormatAtlas, func(*dbschema.DatabaseConnection, devclean.Baseline) error { return nil })
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `column "missing_column" does not exist`)
	var count int
	c.Assert(realm.QueryRowContext(c.Context(), "SELECT count(*) FROM pg_class WHERE relname IN ('durable_stage', 'temporary_stage')").Scan(&count), qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}

func TestPostgresTemporaryTableReplay_Cancellation(t *testing.T) {
	c := qt.New(t)
	realm, err := dbschema.ConnectToDatabase(c.Context(), newPostgresReplayRealmDatabase(c))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(realm)
	ctx, cancel := context.WithCancel(c.Context())
	defer cancel()
	snapshot := fstest.MapFS{"1_cancel.sql": {Data: []byte("CREATE TABLE durable_stage (id integer); CREATE TEMP TABLE temporary_stage AS SELECT 1 AS id;")}}
	err = migrationreplay.WithReplayedSnapshot(ctx, realm, snapshot, migrationfile.DirFormatAtlas, func(session *dbschema.DatabaseConnection, _ devclean.Baseline) error {
		var value int
		c.Assert(session.QueryRowContext(ctx, "SELECT id FROM temporary_stage").Scan(&value), qt.IsNil)
		c.Assert(value, qt.Equals, 1)
		cancel()
		return ctx.Err()
	})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	var count int
	c.Assert(realm.QueryRowContext(c.Context(), "SELECT count(*) FROM pg_class WHERE relname IN ('durable_stage', 'temporary_stage')").Scan(&count), qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}
