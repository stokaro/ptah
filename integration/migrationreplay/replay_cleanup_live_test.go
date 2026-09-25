//go:build integration

package migrationreplay_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devclean"
	"ptah.run/internal/migrationreplay"
	"ptah.run/migration/migrationfile"
)

// TestWithReplayedSnapshot_PostgresCleanupOutlivesTheGraceLive pins that the
// cleanup after a successful replay runs as long as the work takes while the
// command is still waiting for it. A lock another session holds on a replayed
// table keeps the cleanup's DROP waiting for longer than
// [devclean.CleanupGrace], which stands in for a large schema over a slow link:
// both make the cleanup outlast any fixed budget of that size.
func TestWithReplayedSnapshot_PostgresCleanupOutlivesTheGraceLive(t *testing.T) {
	c := qt.New(t)
	realmURL := newPostgresReplayRealmDatabase(c)
	realm, err := dbschema.ConnectToDatabase(c.Context(), realmURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(realm)
	blocker, err := sql.Open("pgx", realmURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(blocker.Close(), qt.IsNil) }()
	holdFor := devclean.CleanupGrace + 3*time.Second
	snapshot := fstest.MapFS{
		"1_events.sql": {Data: []byte("CREATE TABLE events (id bigint PRIMARY KEY);\n")},
	}
	var cleanupStarted time.Time
	released := make(chan error, 1)

	err = migrationreplay.WithReplayedSnapshot(
		c.Context(),
		realm,
		snapshot,
		migrationfile.DirFormatAtlas,
		func(*dbschema.DatabaseConnection) error {
			tx, beginErr := blocker.BeginTx(c.Context(), nil)
			c.Assert(beginErr, qt.IsNil)
			_, lockErr := tx.ExecContext(c.Context(), "LOCK TABLE events IN ACCESS SHARE MODE")
			c.Assert(lockErr, qt.IsNil)
			time.AfterFunc(holdFor, func() { released <- tx.Rollback() })
			cleanupStarted = time.Now()
			return nil
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(time.Since(cleanupStarted) >= holdFor, qt.IsTrue)
	c.Assert(<-released, qt.IsNil)
	var tableCount int
	err = blocker.QueryRowContext(
		c.Context(),
		"SELECT count(*) FROM pg_tables WHERE schemaname = 'public'",
	).Scan(&tableCount)
	c.Assert(err, qt.IsNil)
	c.Assert(tableCount, qt.Equals, 0)
}

// newPostgresReplayRealmDatabase creates a database of its own for one test
// and returns its URL, so the realm cleanup a replay runs empties nothing a
// neighboring test put on the shared server.
func newPostgresReplayRealmDatabase(c *qt.C) string {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Scheme = "postgres"
	admin, err := sql.Open("pgx", parsed.String())
	c.Assert(err, qt.IsNil)
	name := fmt.Sprintf("ptah_replay_cleanup_%d", time.Now().UnixNano())
	nameIdent := pgx.Identifier{name}.Sanitize()
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+nameIdent)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(
			context.Background(),
			"DROP DATABASE IF EXISTS "+nameIdent+" WITH (FORCE)",
		)
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	parsed.Path = "/" + name
	parsed.RawPath = ""
	return parsed.String()
}
