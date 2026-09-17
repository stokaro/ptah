package atlasschema_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
)

// A fenced table is refused rather than rated. The severity a statement carries
// is a question put to a policy, and every mechanism that rates one can answer
// yes; a fence is the statement that no such yes exists for that table
// (stokaro/ptah#3362).

func TestPreparePlanFile_RefusesAChangeToAProtectedTable(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "protected-change.db")
	applyPlan(c, conn, regionsSchema(regionRow("NO", "Norway", 1)))

	_, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Desired:         regionsSchema(regionRow("NO", "Norge", 1)),
		ProtectedTables: []string{"regions"},
	})

	var fenced *atlasschema.ProtectedTableError
	c.Assert(err, qt.ErrorAs, &fenced)
	c.Assert(fenced.Tables, qt.DeepEquals, []string{"regions"})
	c.Assert(err, qt.ErrorMatches, `(?s)refusing to change protected table\(s\) regions.*no override.*`)
}

// TestPreparePlanFile_RefusesADeletionFromAProtectedTable is the change the
// fence exists for: a row the declaration stopped naming is the loss an
// approval policy is most likely to wave through, because the SQL analyzer
// reads a DELETE of a reference row as safe.
func TestPreparePlanFile_RefusesADeletionFromAProtectedTable(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "protected-delete.db")
	applyPlan(c, conn, regionsSchema(regionRow("NO", "Norway", 1), regionRow("CZ", "Czechia", 2)))

	_, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Desired:         regionsSchema(regionRow("NO", "Norway", 1)),
		ProtectedTables: []string{"regions"},
	})

	var fenced *atlasschema.ProtectedTableError
	c.Assert(err, qt.ErrorAs, &fenced)
}

// TestPreparePlanFile_AProtectedTableTheDeclarationAgreesWithPlansNothing is
// what lets a fence sit in a configuration permanently: it refuses a change,
// not a run.
func TestPreparePlanFile_AProtectedTableTheDeclarationAgreesWithPlansNothing(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "protected-converged.db")
	applyPlan(c, conn, regionsSchema(regionRow("NO", "Norway", 1)))

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Desired:         regionsSchema(regionRow("NO", "Norway", 1)),
		ProtectedTables: []string{"regions"},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.HasChanges(), qt.IsFalse)
}

// TestPreparePlanFile_AFenceOnAnotherTableLeavesThePlanAlone is the control
// that keeps the refusal from being "any entry refuses everything".
func TestPreparePlanFile_AFenceOnAnotherTableLeavesThePlanAlone(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "protected-elsewhere.db")
	applyPlan(c, conn, regionsSchema(regionRow("NO", "Norway", 1)))

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Desired:         regionsSchema(regionRow("NO", "Norge", 1)),
		ProtectedTables: []string{"countries", "currencies"},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(planSQL(plan), qt.Contains, `UPDATE "regions"`)
}

// TestPreparePlanFile_AProtectedEntryIsReadCaseInsensitively pins the grammar
// the migration body reads entries with, at the declarative end.
func TestPreparePlanFile_AProtectedEntryIsReadCaseInsensitively(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "protected-case.db")
	applyPlan(c, conn, regionsSchema(regionRow("NO", "Norway", 1)))

	_, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Desired:         regionsSchema(regionRow("NO", "Norge", 1)),
		ProtectedTables: []string{"REGIONS"},
	})

	var fenced *atlasschema.ProtectedTableError
	c.Assert(err, qt.ErrorAs, &fenced)
}
