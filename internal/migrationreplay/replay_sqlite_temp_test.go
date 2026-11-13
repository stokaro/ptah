package migrationreplay_test

import (
	"context"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/devclean"
	"ptah.run/internal/migrationreplay"
	"ptah.run/migration/migrationfile"
)

// sqliteTempTableDir creates a TEMP table named scratch in version 2. A second
// replay of it on a connection that kept scratch fails with `table scratch
// already exists`.
func sqliteTempTableDir() fstest.MapFS {
	return fstest.MapFS{
		"1_users.sql": {Data: []byte("CREATE TABLE users (id int);\n")},
		"2_scratch.sql": {Data: []byte(
			"CREATE TEMP TABLE scratch (id int);\n" +
				"INSERT INTO scratch SELECT id FROM users;\n" +
				"INSERT INTO users SELECT id + 1 FROM scratch;\n",
		)},
	}
}

func countSQLiteTempObjects(c *qt.C, conn *dbschema.DatabaseConnection) int {
	c.Helper()
	var count int
	err := conn.QueryRowContext(c.Context(), `SELECT COUNT(*) FROM temp.sqlite_schema WHERE name = 'scratch'`).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func TestReplay_SQLiteTempTableDoesNotOutliveTheReplay_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		url  func(*testing.T) string
	}{
		{name: "in-memory database keeps its only connection", url: func(*testing.T) string { return "sqlite://:memory:" }},
		{name: "file database discards the session", url: func(t *testing.T) string { return "sqlite://" + t.TempDir() + "/dev.db" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(t.Context(), test.url(t))
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			// The second replay would meet "table scratch already exists" if
			// the first left its TEMP table on the connection.
			for range 2 {
				var mainObjects []string
				err = migrationreplay.WithReplayedSnapshot(
					t.Context(),
					conn,
					sqliteTempTableDir(),
					migrationfile.DirFormatAtlas,
					func(replayConn *dbschema.DatabaseConnection, _ devclean.Baseline) error {
						rows, err := replayConn.QueryContext(t.Context(),
							`SELECT name FROM main.sqlite_schema WHERE type = 'table' ORDER BY name`)
						c.Assert(err, qt.IsNil)
						defer rows.Close()
						for rows.Next() {
							var name string
							c.Assert(rows.Scan(&name), qt.IsNil)
							mainObjects = append(mainObjects, name)
						}
						return rows.Err()
					},
				)
				c.Assert(err, qt.IsNil)
				c.Assert(mainObjects, qt.DeepEquals, []string{"users"})
				c.Assert(countSQLiteTempObjects(c, conn), qt.Equals, 0)
				assertSQLiteRealmObjectCount(c, conn, 0)
			}
		})
	}
}

func TestReplay_SQLiteTempTableDoesNotOutliveAFailedReplay_FailurePath(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), "sqlite://:memory:")
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	snapshot := fstest.MapFS{
		"1_scratch.sql": {Data: []byte("CREATE TEMP TABLE scratch (id int);\n")},
		"2_broken.sql":  {Data: []byte("INSERT INTO missing VALUES (1);\n")},
	}

	err = migrationreplay.ReplaySnapshotOnConnection(t.Context(), conn, snapshot, migrationfile.DirFormatAtlas)

	c.Assert(err, qt.ErrorMatches, `(?s).*no such table: missing.*`)
	c.Assert(countSQLiteTempObjects(c, conn), qt.Equals, 0)
}

func TestReplay_SQLiteTempViewIsStillRefused_FailurePath(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), "sqlite://:memory:")
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	snapshot := fstest.MapFS{
		"1_view.sql": {Data: []byte("CREATE TEMP VIEW scratch AS SELECT 1;\n")},
	}

	err = migrationreplay.ReplaySnapshotOnConnection(t.Context(), conn, snapshot, migrationfile.DirFormatAtlas)

	c.Assert(err, qt.ErrorMatches, `(?s).*sqlite migration replay rejects TEMP object .*`)
}

func TestReplay_SQLiteTempTablesWithForeignKeysAreDropped_HappyPath(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), "sqlite://:memory:")
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	snapshot := fstest.MapFS{
		"1_tables.sql": {Data: []byte(
			"CREATE TEMP TABLE a (id integer primary key);\n" +
				"CREATE TEMP TABLE b (id int, a_id int references a(id));\n" +
				"INSERT INTO a VALUES (1);\n" +
				"INSERT INTO b VALUES (1, 1);\n" +
				"CREATE INDEX b_a ON b (a_id);\n" +
				"CREATE TRIGGER b_log AFTER INSERT ON b BEGIN SELECT 1; END;\n",
		)},
	}

	err = migrationreplay.ReplaySnapshotOnConnection(t.Context(), conn, snapshot, migrationfile.DirFormatAtlas)

	c.Assert(err, qt.IsNil)
	var temp int
	c.Assert(conn.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM temp.sqlite_schema`).Scan(&temp), qt.IsNil)
	c.Assert(temp, qt.Equals, 0)
}

func TestReplay_SQLiteTempTableOfTheCallerIsKept_FailurePath(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), "sqlite://:memory:")
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(t.Context(), "CREATE TEMP TABLE mine (x int)")
	c.Assert(err, qt.IsNil)

	err = migrationreplay.ReplaySnapshotOnConnection(t.Context(), conn, sqliteTempTableDir(), migrationfile.DirFormatAtlas)

	c.Assert(err, qt.ErrorMatches, `(?s).*refusing database-realm cleanup with TEMP objects: table:mine.*`)
	var mine int
	c.Assert(conn.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM temp.sqlite_schema WHERE name = 'mine'`).Scan(&mine), qt.IsNil)
	c.Assert(mine, qt.Equals, 1)
}

func TestReplay_SQLiteTempTableIsDroppedAfterCancellation_FailurePath(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), "sqlite://:memory:")
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	err = migrationreplay.WithReplayedSnapshot(
		ctx,
		conn,
		sqliteTempTableDir(),
		migrationfile.DirFormatAtlas,
		func(*dbschema.DatabaseConnection, devclean.Baseline) error {
			// Every migration, the TEMP one included, has run.
			cancel()
			return ctx.Err()
		},
	)

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(countSQLiteTempObjects(c, conn), qt.Equals, 0)
}
