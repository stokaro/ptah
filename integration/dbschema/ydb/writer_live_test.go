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
// that holds it. A directory whose tables went, and whose subdirectory went,
// is removed.
func TestYDBWriter_DropAllTablesKeepsWhatItDoesNotDescribe(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c)
	c.Cleanup(func() {
		c.Assert(conn.Writer().ExecuteSQL(context.Background(), "DROP TOPIC IF EXISTS `ptah_ydb_dropall/keep/events`"),
			qt.IsNil)
	})
	for _, statement := range []string{
		"CREATE TABLE `ptah_ydb_dropall/gone/t1` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))",
		"CREATE TABLE `ptah_ydb_dropall/gone/deeper/t2` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))",
		"CREATE TABLE `ptah_ydb_dropall/keep/t3` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))",
		"CREATE TOPIC `ptah_ydb_dropall/keep/events`",
	} {
		c.Assert(conn.Writer().ExecuteSQL(c.Context(), statement), qt.IsNil, qt.Commentf("execute: %s", statement))
	}

	c.Assert(conn.SchemaWriter().DropAllTables(c.Context()), qt.IsNil)

	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(live.Tables, qt.HasLen, 0)
	c.Assert(live.NotDescribed.Describes(coverage.Topic, "ptah_ydb_dropall/keep.events"), qt.IsFalse)
	c.Assert(directoryNames(c, c.Context(), "ptah_ydb_dropall"), qt.DeepEquals, []string{"keep"})
}
