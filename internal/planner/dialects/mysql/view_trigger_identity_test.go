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

// TestCompare_ATriggerResolvesTheDatabaseQualifier is the trigger half of the
// capability the view test below measures, and it now gives the same answer.
//
// A declaration writes the table as `orders` and MySQL's reader reports it as
// `app.orders`. Both families resolve that to one object: views always did,
// through objectlookup's three tiers, and triggers were paired by a map key
// instead -- one string, with no tier for "unqualified matches qualified when
// only one candidate does", because that tier needs the whole candidate set.
//
// Until stokaro/ptah#2436 this test recorded the measurement rather than the
// wish: one addition and one removal, so the plan dropped the trigger and
// created it again on every run. It succeeded each time, and each apply left a
// window in which the table had no trigger.
func TestCompare_ATriggerResolvesTheDatabaseQualifier(t *testing.T) {
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

	// One object. The body differs -- the reader reported none -- so it is a
	// modification rather than nothing, and it carries the declaration it
	// resolved to, the way the view change does.
	c.Assert(diff.TriggersAdded, qt.HasLen, 0)
	c.Assert(diff.TriggersRemoved, qt.HasLen, 0)
	c.Assert(diff.TriggersModified, qt.HasLen, 1)
	c.Assert(diff.TriggersModified[0].Desired.Name, qt.Equals, "touch",
		qt.Commentf("the change carries the declaration the comparison resolved to"))

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
