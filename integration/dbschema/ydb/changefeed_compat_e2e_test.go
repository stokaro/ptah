//go:build integration

package ydb_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// compatChangefeedDesired declares compatDir's table cf, whose column v is an
// Int32 or, widened, an Int64. HCL has no block for a changefeed, so the
// document says nothing about the one the table carries.
func compatChangefeedDesired(vType string) string {
	return `schema "` + compatDir + `" {
}

table "cf" {
  schema = schema.` + compatDir + `
  column "id" {
    type = Int64
  }
  column "v" {
    type = ` + vType + `
    null = true
  }
  primary_key {
    columns = [column.id]
  }
}
`
}

// An HCL document cannot spell a changefeed, so its silence about one is not a
// request to drop it: applying the document leaves the table's changefeed as it
// is, and a rebuild the document asks for drops the changefeed before the swap
// and adds it back after, with its consumer, rather than losing it with the old
// table.
func TestYDBCompatBinary_HCLKeepsTheChangefeedsItCannotSpell(t *testing.T) {
	c := qt.New(t)
	binary := buildCompatBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			dropDirectory(c, conn, compatDir, "cf")
			c.Cleanup(func() { dropDirectory(c, conn, compatDir, "cf") })
			for _, statement := range []string{
				"CREATE TABLE `" + compatDir + "/cf` (`id` Int64 NOT NULL, `v` Int32, PRIMARY KEY (`id`))",
				"ALTER TABLE `" + compatDir + "/cf` ADD CHANGEFEED `feed` WITH (MODE = 'UPDATES', FORMAT = 'JSON')",
				"ALTER TOPIC `" + compatDir + "/cf/feed` ADD CONSUMER `reader`",
				"UPSERT INTO `" + compatDir + "/cf` (id, v) VALUES (1l, 10), (2l, NULL)",
			} {
				c.Assert(conn.Writer().ExecuteSQL(ctx, statement), qt.IsNil, qt.Commentf("%s", statement))
			}
			held := tableNamed(c, readScoped(c, conn, []string{compatDir}), compatDir, "cf").Changefeeds
			c.Assert(held, qt.HasLen, 1)
			c.Assert(held[0].Consumers, qt.HasLen, 1)
			kept := "file://" + writeCompatFile(c, c.TempDir(), "kept.hcl", compatChangefeedDesired("Int32"))
			widened := "file://" + writeCompatFile(c, c.TempDir(), "widened.hcl", compatChangefeedDesired("Int64"))

			synced, syncedStderr, syncedErr := runCompatWithEnv(ctx, binary, nil,
				"schema", "apply", "--url", url, "--schema", compatDir, "--to", kept, "--dry-run")
			c.Assert(syncedErr, qt.IsNil, qt.Commentf("schema apply:\n%s\n%s", synced, syncedStderr))
			c.Assert(synced, qt.Equals, "Schema is synced, no changes to be made\n")

			rebuilt, rebuildStderr, rebuildErr := runCompatWithEnv(ctx, binary, []string{"PTAH_ALLOW_TABLE_REBUILD=1"},
				"schema", "apply", "--url", url, "--schema", compatDir, "--to", widened, "--auto-approve")
			c.Assert(rebuildErr, qt.IsNil, qt.Commentf("schema apply:\n%s\n%s", rebuilt, rebuildStderr))
			c.Assert(rebuilt, qt.Contains, "ALTER TABLE `"+compatDir+"/cf` DROP CHANGEFEED `feed`;\n")
			c.Assert(rebuilt, qt.Contains, "ALTER TABLE `"+compatDir+"/cf` ADD CHANGEFEED `feed` WITH "+
				"(MODE = 'UPDATES', FORMAT = 'JSON');\nALTER TOPIC `"+compatDir+"/cf/feed` ADD CONSUMER `reader`;\n")
			table := tableNamed(c, readScoped(c, conn, []string{compatDir}), compatDir, "cf")
			c.Assert(columnNamed(c, table, "v").DataType, qt.Equals, "Int64")
			c.Assert(table.Changefeeds, qt.DeepEquals, held)
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+compatDir+"/cf`"), qt.Equals, int64(2))
		})
	}
}
