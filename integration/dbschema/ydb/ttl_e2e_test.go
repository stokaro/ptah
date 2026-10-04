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
