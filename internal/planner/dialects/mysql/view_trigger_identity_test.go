package mysql_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestViewAndTriggerLookupsDoNotCrossDatabases pins mysql's findView and
// findTrigger at the thing that changed about them.
//
// Both were already routed through objectlookup on master, but through a
// STRUCTURAL rule that compared unqualified names with no refusal when both
// sides name a database and the two disagree. Crossing databases is not a
// missing statement here, it is a wrong one: `reporting.v`'s body rendered under
// the name `app.v`.
func TestViewAndTriggerLookupsDoNotCrossDatabases(t *testing.T) {
	tests := []struct {
		name        string
		desired     *schemamodel.Database
		diff        *difftypes.SchemaDiff
		unwantedSQL string
	}{
		{
			name: "a modified view declared in another database is left alone",
			desired: &schemamodel.Database{
				Views: []schemamodel.View{{
					Name: "reporting.active_orders",
					Body: "SELECT id FROM orders WHERE open",
				}},
			},
			diff: &difftypes.SchemaDiff{ViewsModified: []difftypes.ViewDiff{{
				ViewName:     "app.active_orders",
				PreviousBody: "SELECT id FROM orders",
				Changes:      map[string]string{"body": "changed"},
			}}},
			unwantedSQL: "VIEW",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatements(test.diff, test.desired, "mysql")
			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(plan, qt.Not(qt.Contains), test.unwantedSQL, qt.Commentf("plan:\n%s", plan))
		})
	}
}

// TestCompare_ATriggerDoesNotResolveTheDatabaseQualifier records what MySQL
// actually does with the case the view test above resolves, and it is not the
// same answer.
//
// A declaration writes the table as `orders` and MySQL's reader reports it as
// `app.orders`. For a view that is one modification; for a trigger it is one
// addition and one removal, so the plan drops the trigger and creates it again
// on every run. The two families resolve identity differently -- views through
// objectlookup's three tiers, triggers through a map key, and a key has no tier
// for "unqualified matches qualified when only one candidate does", because that
// tier needs the whole candidate set.
//
// This is stokaro/ptah#2436 and it is not what the carry changed: the comparison
// produced the same pair before it, and the planner rendered them from the same
// declaration. The assertions below are deliberately the MEASUREMENT rather than
// the wish, so that fixing #2436 turns this test red and it gets rewritten to
// the answer the view already gives.
func TestCompare_ATriggerDoesNotResolveTheDatabaseQualifier(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Triggers: []schemamodel.Trigger{{
			StructName: "Order",
			Name:       "touch",
			Table:      "orders",
			Timing:     "AFTER",
			Event:      "UPDATE",
			ForEach:    "ROW",
			Body:       "SET @x = 1;",
		}},
	}
	database := &catalog.Database{
		Triggers: []catalog.Trigger{{
			Schema: "app", Name: "touch", Table: "orders",
			Timing: "AFTER", Event: "UPDATE", ForEach: "ROW",
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, database, platform.MySQL)

	c.Assert(diff.TriggersModified, qt.HasLen, 0)
	c.Assert(diff.TriggersAdded, qt.HasLen, 1)
	c.Assert(diff.TriggersRemoved, qt.HasLen, 1)
	c.Assert(diff.TriggersAdded[0].Desired.Name, qt.Equals, "touch",
		qt.Commentf("the addition still carries the declaration it renders from"))
	c.Assert(diff.TriggersRemoved[0].Desired.Name, qt.Equals, "",
		qt.Commentf("a removal is written from its two names and carries no operand"))

	sql, err := planner.GenerateSchemaDiffSQL(diff, desired, platform.MySQL)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "TRIGGER")
}

// TestCompare_AModifiedViewResolvesTheDatabaseQualifier is the view half of the
// capability above, measured where it now happens.
//
// A declaration writes `active_orders` and MySQL reports every view under its
// database name. The planner used to reconcile the two by looking the name up;
// the comparison does it now, and the change carries the view it resolved to
// (stokaro/ptah#2315).
func TestCompare_AModifiedViewResolvesTheDatabaseQualifier(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Views: []schemamodel.View{{Name: "active_orders", Body: "SELECT id FROM orders WHERE open"}},
	}
	database := &catalog.Database{
		Views: []catalog.View{{Schema: "app", Name: "active_orders", Body: "SELECT id FROM orders"}},
	}

	diff := schemadiff.CompareWithDialect(desired, database, platform.MySQL)

	c.Assert(diff.ViewsModified, qt.HasLen, 1)
	c.Assert(diff.ViewsModified[0].Desired.Name, qt.Equals, "active_orders",
		qt.Commentf("the comparison resolved the qualified readback to the declaration"))

	sql, err := planner.GenerateSchemaDiffSQL(diff, desired, platform.MySQL)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "VIEW")
}
