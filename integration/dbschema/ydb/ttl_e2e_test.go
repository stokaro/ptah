//go:build integration

package ydb_test

import (
	"context"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/dbtarget"
)

// ttlInspectedPlatform is the block `schema inspect` writes for the events
// table's TTL: the YDB owner's platform properties, in the spelling YDB shows.
const ttlInspectedPlatform = `  platform "ydb" {
    override "row_deletion_column" {
      value = "created_at"
    }
    override "row_deletion_interval" {
      value = "P1D"
    }
  }
`

// `ptah-compat schema inspect` writes a table's TTL into HCL as the YDB
// owner's platform properties and reports no loss, and the document applied
// back to the database it came from plans nothing. The same document without
// the block plans nothing either: HCL that names no TTL leaves the table's
// TTL alone rather than losing it to a RESET (TTL).
func TestYDBCompatBinary_InspectWritesTheTTLAsPlatformProperties(t *testing.T) {
	c := qt.New(t)
	binary := buildCompatBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			dropTables(c, conn, ttlSchemas)
			c.Cleanup(func() { dropTables(c, conn, ttlSchemas) })
			apply(c, conn, planAgainst(c, conn,
				ttlEvents(&ydbschema.TTL{Column: "created_at", Interval: "P1D"}, "created_at"), ttlSchemas))

			inspected, notes, inspectErr := runCompat(ctx, binary, "schema", "inspect", "--url", url, "--schema", ttlSchema)
			c.Assert(inspectErr, qt.IsNil, qt.Commentf("schema inspect:\n%s", notes))
			c.Assert(inspected, qt.Contains, ttlInspectedPlatform)
			c.Assert(notes, qt.Not(qt.Contains), string(ydbschema.TTLKind))
			for _, document := range []string{inspected, strings.Replace(inspected, ttlInspectedPlatform, "", 1)} {
				reapplied, _, reapplyErr := runCompat(ctx, binary, "schema", "apply", "--url", url, "--schema", ttlSchema,
					"--to", "file://"+writeCompatFile(c, c.TempDir(), "inspected.hcl", document), "--dry-run")
				c.Assert(reapplyErr, qt.IsNil, qt.Commentf("schema apply of the document:\n%s\n%s", document, reapplied))
				c.Assert(reapplied, qt.Equals, "Schema is synced, no changes to be made\n", qt.Commentf("document:\n%s", document))
			}
			c.Assert(policyOf(c, conn, "events"), qt.DeepEquals, &ydbschema.TTL{Column: "created_at", Interval: "P1D"})
		})
	}
}

// ttlRebuildDesired is the events table with its n column widened, in HCL that
// names no TTL.
const ttlRebuildDesired = `schema "` + ttlSchema + `" {
}

table "events" {
  schema = schema.` + ttlSchema + `
  column "id" {
    type = Int64
  }
  column "n" {
    type = Int64
    null = true
  }
  column "ts" {
    type = Timestamp
    null = true
  }
  primary_key {
    columns = [column.id]
  }
}
`

// A table rebuilt from an HCL desired state that names no TTL keeps its TTL:
// the plan takes the database's TTL as declared and writes it on the new table
// rather than dropping it with the old one.
func TestYDBCompatBinary_RebuildKeepsATTLTheDocumentLeavesOut(t *testing.T) {
	c := qt.New(t)
	binary := buildCompatBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			dropTables(c, conn, ttlSchemas)
			c.Cleanup(func() { dropTables(c, conn, ttlSchemas) })
			c.Assert(conn.Writer().ExecuteSQL(ctx, "CREATE TABLE `"+ttlSchema+"/events` "+
				"(`id` Int64 NOT NULL, `n` Int32, `ts` Timestamp, PRIMARY KEY (`id`)) WITH (TTL = Interval('P1D') ON `ts`)"),
				qt.IsNil)
			desired := "file://" + writeCompatFile(c, c.TempDir(), "desired.hcl", ttlRebuildDesired)

			rebuilt, notes, rebuildErr := runCompatWithEnv(ctx, binary, []string{"PTAH_ALLOW_TABLE_REBUILD=1"},
				"schema", "apply", "--url", url, "--schema", ttlSchema, "--to", desired, "--auto-approve")

			c.Assert(rebuildErr, qt.IsNil, qt.Commentf("schema apply:\n%s\n%s", rebuilt, notes))
			c.Assert(rebuilt, qt.Contains, ") WITH (TTL = Interval(\"P1D\") ON `ts`, AUTO_PARTITIONING_BY_SIZE = ENABLED, ")
			c.Assert(policyOf(c, conn, "events"), qt.DeepEquals, &ydbschema.TTL{Column: "ts", Interval: "P1D"})
			synced, _, syncedErr := runCompat(ctx, binary,
				"schema", "apply", "--url", url, "--schema", ttlSchema, "--to", desired, "--dry-run")
			c.Assert(syncedErr, qt.IsNil, qt.Commentf("schema apply again:\n%s", synced))
			c.Assert(synced, qt.Equals, "Schema is synced, no changes to be made\n")
		})
	}
}
