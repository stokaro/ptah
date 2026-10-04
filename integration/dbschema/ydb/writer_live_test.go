//go:build integration

package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/dbschema"
)

// TestYDBWriter_DropAllTablesKeepsWhatItDoesNotDescribe pins what drop-all
// leaves: an object the reader records as not described, and the directory
// that holds it. A view and a topic, which the reader describes, go with the
// tables, and a directory whose tables, views and topics went, and whose
// subdirectory went, is removed. A table carrying a changefeed goes with its
// changefeed and the topic's consumers: DROP TABLE takes them, measured on
// 25.1.4.7 and 26.2.1.14, so neither needs a statement of its own.
func TestYDBWriter_DropAllTablesKeepsWhatItDoesNotDescribe(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			c.Cleanup(func() {
				c.Assert(conn.Writer().ExecuteSQL(context.Background(), "DROP TABLE IF EXISTS `ptah_ydb_dropall/keep/olap`"),
					qt.IsNil)
			})
			for _, statement := range []string{
				"CREATE TABLE `ptah_ydb_dropall/gone/t1` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))",
				"ALTER TABLE `ptah_ydb_dropall/gone/t1` ADD CHANGEFEED `feed` WITH (MODE = 'UPDATES', FORMAT = 'JSON')",
				"ALTER TOPIC `ptah_ydb_dropall/gone/t1/feed` ADD CONSUMER `reader`",
				"CREATE TABLE `ptah_ydb_dropall/gone/deeper/t2` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))",
				"CREATE VIEW `ptah_ydb_dropall/gone/v` WITH (security_invoker = TRUE) AS " +
					"SELECT `id` FROM `ptah_ydb_dropall/gone/t1`",
				"CREATE VIEW `ptah_ydb_dropall/viewonly/v` WITH (security_invoker = TRUE) AS SELECT 1 AS a",
				"CREATE TOPIC `ptah_ydb_dropall/gone/deeper/events`",
				"CREATE TABLE `ptah_ydb_dropall/keep/t3` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))",
				"CREATE TOPIC `ptah_ydb_dropall/keep/events`",
				"CREATE TABLE `ptah_ydb_dropall/keep/olap` (`id` Int64 NOT NULL, PRIMARY KEY (`id`)) " +
					"PARTITION BY HASH(`id`) WITH (STORE = COLUMN)",
			} {
				c.Assert(conn.Writer().ExecuteSQL(c.Context(), statement), qt.IsNil, qt.Commentf("execute: %s", statement))
			}

			c.Assert(conn.SchemaWriter().DropAllTables(c.Context()), qt.IsNil)

			live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, nil)
			c.Assert(err, qt.IsNil)
			c.Assert(live.Tables, qt.HasLen, 0)
			c.Assert(live.Views, qt.HasLen, 0)
			c.Assert(live.Topics, qt.HasLen, 0)
			c.Assert(live.NotDescribed.Describes(coverage.ColumnTable, "ptah_ydb_dropall/keep.olap"), qt.IsFalse)
			c.Assert(directoryNames(c, c.Context(), line, "ptah_ydb_dropall"), qt.DeepEquals, []string{"keep"})
			c.Assert(directoryNames(c, c.Context(), line, "ptah_ydb_dropall", "keep"), qt.DeepEquals, []string{"olap"})
		})
	}
}

// TestYDBWriter_DropDirectoryRemovesEverythingInIt drops a directory a caller
// made for itself -- row and column tables, one carrying a changefeed, a
// view, a topic and a nested directory -- and leaves the directory beside it
// alone. It is the teardown of the capability probe's namespace, which nothing
// else in YDB's SQL can remove.
func TestYDBWriter_DropDirectoryRemovesEverythingInIt(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropper, ok := conn.SchemaWriter().(interface {
				DropDirectory(ctx context.Context, dir string) error
			})
			c.Assert(ok, qt.IsTrue, qt.Commentf("the YDB schema writer %T removes no directory", conn.SchemaWriter()))
			c.Cleanup(func() { c.Check(dropper.DropDirectory(context.Background(), "ptah_ydb_dropdir"), qt.IsNil) })
			for _, statement := range []string{
				"CREATE TABLE `ptah_ydb_dropdir/probe/t1` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))",
				"ALTER TABLE `ptah_ydb_dropdir/probe/t1` ADD CHANGEFEED `feed` WITH (MODE = 'UPDATES', FORMAT = 'JSON')",
				"CREATE TABLE `ptah_ydb_dropdir/probe/deeper/t2` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))",
				"CREATE TABLE `ptah_ydb_dropdir/probe/olap` (`id` Int64 NOT NULL, PRIMARY KEY (`id`)) " +
					"PARTITION BY HASH(`id`) WITH (STORE = COLUMN)",
				"CREATE VIEW `ptah_ydb_dropdir/probe/v` WITH (security_invoker = TRUE) AS " +
					"SELECT `id` FROM `ptah_ydb_dropdir/probe/t1`",
				"CREATE TOPIC `ptah_ydb_dropdir/probe/deeper/events` (CONSUMER `reader`)",
				"CREATE TABLE `ptah_ydb_dropdir/keep/t3` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))",
			} {
				c.Assert(conn.Writer().ExecuteSQL(c.Context(), statement), qt.IsNil, qt.Commentf("execute: %s", statement))
			}

			c.Assert(dropper.DropDirectory(c.Context(), "ptah_ydb_dropdir/probe"), qt.IsNil)

			c.Assert(directoryNames(c, c.Context(), line, "ptah_ydb_dropdir"), qt.DeepEquals, []string{"keep"})
			c.Assert(directoryNames(c, c.Context(), line, "ptah_ydb_dropdir", "keep"), qt.DeepEquals, []string{"t3"})
		})
	}
}
