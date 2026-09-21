//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// TestSetRevisionToZeroClearsEveryRevisionLive measures a set to version 0
// against a server: the delete it builds for a boundary no migration occupies
// is accepted, and the revision table is empty afterwards.
//
// The count comes from the table rather than from the result, which is computed
// in Go and would read the same whatever the server did. What the result cannot
// answer is the question a server owns -- whether the statement ran at all --
// and refusing version 0 again makes this test red where every offline
// assertion about the delete text still passes.
//
// What it does not measure is the shape of the boundary clause: on PostgreSQL
// an empty-string boundary and an absent clause delete the same rows. That
// distinction belongs to Oracle, where the empty string is NULL, and it is
// pinned offline in migration/migrator/set_revision_zero_internal_test.go.
//
// Both formats run. They build their delete separately, and the native one is
// where PostgreSQL decides: its version column is an integer, so a predicate
// written for the Atlas text column -- `version LIKE '.%'` -- is refused with
// `operator does not exist: bigint ~~ unknown` and the set fails at every
// version. SQLite accepts that comparison, which is why only a server can say
// the statement is well formed (stokaro/ptah#3455, stokaro/ptah#3461).
func TestSetRevisionToZeroClearsEveryRevisionLive(t *testing.T) {
	tests := []struct {
		name   string
		format migrator.RevisionTableFormat
	}{
		{name: "ptah revision table", format: migrator.RevisionTableFormatPtah},
		{name: "atlas revision table", format: migrator.RevisionTableFormatAtlas},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			conn, err := dbschema.ConnectToDatabase(ctx, postgresTestURL(t))
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			suffix := time.Now().UnixNano()
			revisionsTable := fmt.Sprintf("ptah_3455_revisions_%d", suffix)
			widgets := fmt.Sprintf("ptah_3455_widgets_%d", suffix)
			gadgets := fmt.Sprintf("ptah_3455_gadgets_%d", suffix)
			m := newSetZeroMigrator(c, conn, widgets, gadgets, test.format, revisionsTable)
			defer dropSetZeroTables(conn, revisionsTable, widgets, gadgets)

			c.Assert(m.MigrateUp(ctx), qt.IsNil)
			c.Assert(setZeroRevisionCount(c, conn, revisionsTable), qt.Equals, 2)

			result, err := m.SetRevision(ctx, 0)

			c.Assert(err, qt.IsNil)
			c.Assert(result.CurrentVersion, qt.Equals, int64(0))
			c.Assert(result.Removed, qt.HasLen, 2)
			c.Assert(setZeroRevisionCount(c, conn, revisionsTable), qt.Equals, 0)
			applied, err := m.GetAppliedMigrations(ctx)
			c.Assert(err, qt.IsNil)
			c.Assert(applied, qt.HasLen, 0)
			// A set moves bookkeeping only, so the tables the migrations
			// created are still there and a rerun is what puts the rows back.
			c.Assert(setZeroTableExists(c, conn, widgets), qt.IsTrue)
			c.Assert(setZeroTableExists(c, conn, gadgets), qt.IsTrue)
		})
	}
}

func newSetZeroMigrator(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	widgets, gadgets string,
	format migrator.RevisionTableFormat,
	revisionsTable string,
) *migrator.Migrator {
	c.Helper()
	m, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"0000000001_widgets.up.sql": {Data: fmt.Appendf(nil,
			"CREATE TABLE %s (id BIGINT PRIMARY KEY);", widgets)},
		"0000000001_widgets.down.sql": {Data: fmt.Appendf(nil, "DROP TABLE %s;", widgets)},
		"0000000002_gadgets.up.sql": {Data: fmt.Appendf(nil,
			"CREATE TABLE %s (id BIGINT PRIMARY KEY);", gadgets)},
		"0000000002_gadgets.down.sql": {Data: fmt.Appendf(nil, "DROP TABLE %s;", gadgets)},
	})
	c.Assert(err, qt.IsNil)
	return m.WithRevisionTableFormat(format).WithMigrationsTable("", revisionsTable)
}

// setZeroRevisionCount counts every row of the revision table without going
// through a Ptah reader, whichever format wrote it.
func setZeroRevisionCount(c *qt.C, conn *dbschema.DatabaseConnection, table string) int {
	c.Helper()
	var count int
	err := conn.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM "`+table+`"`).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func setZeroTableExists(c *qt.C, conn *dbschema.DatabaseConnection, table string) bool {
	c.Helper()
	var count int
	err := conn.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = $1", table).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count == 1
}

// dropSetZeroTables removes what the test created from the shared server. A
// table that is not there is the state this wants, so the error is not read.
func dropSetZeroTables(conn *dbschema.DatabaseConnection, tables ...string) {
	for _, table := range tables {
		_, _ = conn.ExecContext(context.Background(), `DROP TABLE IF EXISTS "`+table+`"`)
	}
}
