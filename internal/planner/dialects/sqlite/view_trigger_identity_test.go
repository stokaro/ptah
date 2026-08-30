package sqlite_test

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

// TestViewAndTriggerLookupsDoNotCrossSchemas pins sqlite's findView and
// findTrigger at the thing that changed about them: the structural rule master
// used resolved a name across two DIFFERENT schemas, so `attached.v`'s body was
// rendered under the name the diff carried.
func TestViewAndTriggerLookupsDoNotCrossSchemas(t *testing.T) {
	tests := []struct {
		name        string
		desired     *schemamodel.Database
		diff        *difftypes.SchemaDiff
		unwantedSQL string
	}{
		{
			name: "a modified view declared in another schema is left alone",
			desired: &schemamodel.Database{
				Views: []schemamodel.View{{
					Name: "attached.active_notes",
					Body: "SELECT id FROM notes WHERE body IS NOT NULL",
				}},
			},
			diff: &difftypes.SchemaDiff{ViewsModified: []difftypes.ViewDiff{{
				ViewName:     "main.active_notes",
				PreviousBody: "SELECT id FROM notes",
				Changes:      map[string]string{"body": "changed"},
			}}},
			unwantedSQL: "VIEW",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatements(withDeclaredTable(test.diff, test.desired), "sqlite")
			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(plan, qt.Not(qt.Contains), test.unwantedSQL, qt.Commentf("plan:\n%s", plan))
		})
	}
}

// TestCompare_ATriggerFoldsACaseDifference is the capability half of the
// refusal above, measured where it now happens.
//
// SQLite folds ASCII, so a declaration writing the table as `Notes` and a
// server reporting `notes` name one object, and only a folding rule joins them.
// The planner used to reconcile the two by looking the trigger up; the
// comparison does it now, and the entry carries the trigger it resolved to
// (stokaro/ptah#2315). Driving the comparison rather than hand-building the diff
// is the point: a hand-built one bypasses the resolution the planner no longer
// performs.
func TestCompare_ATriggerFoldsACaseDifference(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Triggers: []schemamodel.Trigger{{
			StructName: "Note",
			Name:       "touch",
			Table:      "Notes",
			Timing:     "AFTER",
			Event:      "UPDATE",
			ForEach:    "ROW",
			Body:       "SELECT 1;",
		}},
	}
	database := &catalog.Database{
		Triggers: []catalog.Trigger{{
			Name: "touch", Table: "notes",
			Timing: "BEFORE", Event: "UPDATE", ForEach: "ROW",
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, database, "sqlite")

	c.Assert(diff.TriggersAdded, qt.HasLen, 0,
		qt.Commentf("the two spellings are one trigger, not one to add and one to drop"))
	c.Assert(diff.TriggersRemoved, qt.HasLen, 0)
	c.Assert(diff.TriggersModified, qt.HasLen, 1)
	c.Assert(diff.TriggersModified[0].Desired.Table, qt.Equals, "Notes",
		qt.Commentf("the entry carries the declaration as written, not the folded spelling"))

	statements, err := planner.GenerateSchemaDiffSQLStatements(withDeclaredTable(diff, desired), "sqlite")

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "TRIGGER")
}

// TestCompare_AModifiedViewResolvesACaseDifference is the view half of the
// capability above, measured where it now happens.
//
// A declaration writes `Active_Notes` and a server reports `active_notes`. The
// planner used to reconcile the two by looking the name up; the comparison does
// it now, and the change carries the view it resolved to (stokaro/ptah#2315).
// Driving the comparison rather than hand-building the diff is the point: a
// hand-built one bypasses the resolution the planner no longer performs.
func TestCompare_AModifiedViewResolvesACaseDifference(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Views: []schemamodel.View{{Name: "Active_Notes", Body: "SELECT id FROM notes WHERE body IS NOT NULL"}},
	}
	database := &catalog.Database{
		Views: []catalog.View{{Name: "active_notes", Body: "SELECT id FROM notes"}},
	}

	diff := schemadiff.CompareWithDialect(desired, database, platform.SQLite)

	c.Assert(diff.ViewsModified, qt.HasLen, 1)
	c.Assert(diff.ViewsModified[0].Desired.Name, qt.Equals, "Active_Notes",
		qt.Commentf("the comparison folded the case and resolved to the declaration"))

	sql, err := planner.GenerateSchemaDiffSQL(withDeclaredTable(diff, desired), platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "VIEW")
}
