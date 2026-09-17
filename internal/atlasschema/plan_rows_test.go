package atlasschema_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	digest "github.com/opencontainers/go-digest"

	"ptah.run/internal/atlasschema"
)

// A plan that restores a declared value writes the same SQL whatever the row
// said before, so two plans computed against different row values were one plan,
// and applying it overwrote a row that changed after it was reviewed
// (stokaro/ptah#3378). The plan now records the rows it read, and applying it
// reads them again.
func TestVerifyPlanTargetRefusesAPlanWhoseDeclaredRowsMoved(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := managedDataConnection(c, "rows-moved.db")
	applyPlan(c, conn, regionsSchema(regionRow("NO", "Norway", 1)))
	_, err := conn.ExecContext(ctx, `UPDATE regions SET name = 'Edited once' WHERE code = 'NO'`)
	c.Assert(err, qt.IsNil)

	plan, err := atlasschema.PreparePlanFile(ctx, conn, atlasschema.PlanFileOptions{
		Desired: regionsSchema(regionRow("NO", "Norway", 1)),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.ManagedRows, qt.DeepEquals, []atlasschema.PlanRowSet{{
		Table: "regions", Keys: []string{"code"}, Columns: []string{"code", "name", "rank"},
	}})
	c.Assert(plan.RowsFingerprint, qt.Matches, `sha256:[0-9a-f]{64}`)
	c.Assert(atlasschema.VerifyPlanTarget(ctx, conn, plan), qt.IsNil)

	_, err = conn.ExecContext(ctx, `UPDATE regions SET name = 'Edited twice' WHERE code = 'NO'`)
	c.Assert(err, qt.IsNil)

	err = atlasschema.VerifyPlanTarget(ctx, conn, plan)
	c.Assert(err, qt.ErrorMatches,
		`pre-planned migration is stale: the declared rows it reads no longer hold what the plan was computed against .*`)
	var stale *atlasschema.StalePlanError
	c.Assert(err, qt.ErrorAs, &stale)
	c.Assert(stale.Rows, qt.IsTrue)
	c.Assert(stale.PlanFingerprint, qt.Equals, plan.RowsFingerprint)
	c.Assert(stale.DatabaseFingerprint, qt.Not(qt.Equals), plan.RowsFingerprint)

	// The same statements, computed against different rows, are a different
	// plan: a consumer that binds an approval to the plan sees a new one.
	replanned, err := atlasschema.PreparePlanFile(ctx, conn, atlasschema.PlanFileOptions{
		Desired: regionsSchema(regionRow("NO", "Norway", 1)),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(planSQL(replanned), qt.Equals, planSQL(plan))
	c.Assert(replanned.RowsFingerprint, qt.Not(qt.Equals), plan.RowsFingerprint)
	c.Assert(replanned.Name, qt.Not(qt.Equals), plan.Name)
}

// The rows a plan covers are the columns the declaration manages. A column it
// does not manage belongs to somebody else, and a change there leaves the plan
// exactly as current as it was.
func TestVerifyPlanTargetIgnoresAColumnTheDeclarationDoesNotManage(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := managedDataConnection(c, "rows-unmanaged.db")
	applyPlan(c, conn, regionsSchema(regionRow("NO", "Norway", 1)))
	_, err := conn.ExecContext(ctx, `UPDATE regions SET name = 'Edited once' WHERE code = 'NO'`)
	c.Assert(err, qt.IsNil)
	plan, err := atlasschema.PreparePlanFile(ctx, conn, atlasschema.PlanFileOptions{
		Desired: regionsSchema(regionRow("NO", "Norway", 1)),
	})
	c.Assert(err, qt.IsNil)

	_, err = conn.ExecContext(ctx, `UPDATE regions SET note = 'written by an application' WHERE code = 'NO'`)
	c.Assert(err, qt.IsNil)

	c.Assert(atlasschema.VerifyPlanTarget(ctx, conn, plan), qt.IsNil)
}

// A table the plan creates holds nothing to read, so nothing is recorded for it:
// the structural fingerprint already refuses the plan if the table appears.
func TestPreparePlanFileRecordsNoRowsForATableItCreates(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "rows-fresh.db")

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Desired: regionsSchema(regionRow("NO", "Norway", 1)),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.ManagedRows, qt.IsNil)
	c.Assert(plan.RowsFingerprint, qt.Equals, "")
}

// A plan that declares no rows reads as it always did, down to its name, so a
// plan saved before this change and one saved after it agree.
func TestPreparePlanFileWithoutDeclaredRowsKeepsItsName(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	conn := connectPlanSQLite(c, filepath.Join(dir, "no-rows.db"))
	desired := writePlanDesiredSchema(c, dir, "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		ToURLs: []string{desired},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.ManagedRows, qt.IsNil)
	c.Assert(plan.RowsFingerprint, qt.Equals, "")
	sum := digest.FromString(plan.FromFingerprint + "\n" + plan.ToFingerprint).Encoded()
	c.Assert(plan.Name, qt.Equals, "plan_"+sum[:12])
}
