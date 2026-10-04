//go:build integration

package ydb_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/dbtarget"
)

// HCL has no spelling for a TTL. `ptah-compat schema inspect` says so for each
// TTL it leaves out, and the document applied back to the database it came
// from plans nothing: the loader records that HCL cannot express a TTL, so the
// table keeps it rather than losing it to a RESET (TTL).
func TestYDBCompatBinary_KeepsATTLHCLCannotWrite(t *testing.T) {
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
				ttlEvents(&ast.RowDeletionPolicySpec{Column: "created_at", Interval: "P1D"}, "created_at"), ttlSchemas))

			inspected, notes, inspectErr := runCompat(ctx, binary, "schema", "inspect", "--url", url, "--schema", ttlSchema)
			c.Assert(inspectErr, qt.IsNil, qt.Commentf("schema inspect:\n%s", notes))
			c.Assert(notes, qt.Contains,
				"warning: table."+ttlSchema+".events: row deletion policy (TTL P1D on created_at) is not represented in HCL")
			reapplied, _, reapplyErr := runCompat(ctx, binary, "schema", "apply", "--url", url, "--schema", ttlSchema,
				"--to", "file://"+writeCompatFile(c, c.TempDir(), "inspected.hcl", inspected), "--dry-run")
			c.Assert(reapplyErr, qt.IsNil, qt.Commentf("schema apply of the inspected document:\n%s", reapplied))
			c.Assert(reapplied, qt.Equals, "Schema is synced, no changes to be made\n")
			c.Assert(policyOf(c, conn, "events"), qt.DeepEquals, &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "P1D"})
		})
	}
}

// ttlRebuildDesired is the events table with its n column widened, in HCL,
// which has no spelling for the table's TTL.
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

// A table rebuilt from an HCL desired state keeps its TTL: the document cannot
// spell one, so the plan takes the database's TTL as declared and writes it on
// the new table rather than dropping it with the old one.
func TestYDBCompatBinary_RebuildKeepsATTLHCLCannotWrite(t *testing.T) {
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
			c.Assert(rebuilt, qt.Contains, ") WITH (TTL = Interval(\"P1D\") ON `ts`);")
			c.Assert(policyOf(c, conn, "events"), qt.DeepEquals, &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P1D"})
			synced, _, syncedErr := runCompat(ctx, binary,
				"schema", "apply", "--url", url, "--schema", ttlSchema, "--to", desired, "--dry-run")
			c.Assert(syncedErr, qt.IsNil, qt.Commentf("schema apply again:\n%s", synced))
			c.Assert(synced, qt.Equals, "Schema is synced, no changes to be made\n")
		})
	}
}
