//go:build integration

package migrator_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// TestOracleSetRevisionMovesTheBoundaryLive drives `migrations set` against
// Oracle in both revision-table formats.
//
// The transaction the set opens asks for serializable isolation, and Oracle's
// driver implements one isolation level through database/sql: given any other
// through sql.TxOptions it refuses before the transaction opens, with "only
// support default value for isolation". The command then failed on Oracle at
// every version and in both formats, before it read a single revision row.
// Asking for the level in SQL is what Oracle accepts (stokaro/ptah#3462).
func TestOracleSetRevisionMovesTheBoundaryLive(t *testing.T) {
	tests := []struct {
		name   string
		format migrator.RevisionTableFormat
		table  string
	}{
		{name: "ptah revision table", format: migrator.RevisionTableFormatPtah, table: "ptah_3462_revisions"},
		{name: "atlas revision table", format: migrator.RevisionTableFormatAtlas, table: "ptah_3462_atlas_revisions"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			conn := connectOracle3298(c)
			dropOracle3298Tables(c, conn, "ptah_3298_widgets", "ptah_3298_gadgets", `"`+test.table+`"`)
			mig := newOracle3298Migrator(c, conn, oracle3298Migrations(), test.format, test.table)
			c.Assert(mig.MigrateUp(ctx), qt.IsNil)

			result, err := mig.SetRevision(ctx, 1)

			c.Assert(err, qt.IsNil)
			c.Assert(result.CurrentVersion, qt.Equals, int64(1))
			// Read back through the revision table rather than from the result,
			// which would restate what the call already returned.
			applied, err := mig.GetAppliedMigrations(ctx)
			c.Assert(err, qt.IsNil)
			c.Assert(applied, qt.DeepEquals, []int64{1})
		})
	}
}

// TestOracleSetRevisionToZeroClearsTheHistoryLive is the boundary the other
// test cannot reach: version 0 names the state where no migration is applied,
// so it removes every row rather than moving a boundary between rows.
func TestOracleSetRevisionToZeroClearsTheHistoryLive(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := connectOracle3298(c)
	const table = "ptah_3462_zero_revisions"
	dropOracle3298Tables(c, conn, "ptah_3298_widgets", "ptah_3298_gadgets", `"`+table+`"`)
	mig := newOracle3298Migrator(c, conn, oracle3298Migrations(), migrator.RevisionTableFormatPtah, table)
	c.Assert(mig.MigrateUp(ctx), qt.IsNil)

	result, err := mig.SetRevision(ctx, 0)

	c.Assert(err, qt.IsNil)
	c.Assert(result.CurrentVersion, qt.Equals, int64(0))
	applied, err := mig.GetAppliedMigrations(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(applied, qt.HasLen, 0)
}
