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
		{
			name: "a trigger whose table is declared in another schema is left alone",
			desired: &schemamodel.Database{
				Triggers: []schemamodel.Trigger{{
					StructName: "Note",
					Name:       "touch",
					Table:      "attached.notes",
					Timing:     "AFTER",
					Event:      "UPDATE",
					ForEach:    "ROW",
					Body:       "SELECT 1;",
				}},
			},
			diff: &difftypes.SchemaDiff{TriggersAdded: []difftypes.TriggerRef{{
				TriggerName: "touch",
				TableName:   "main.notes",
			}}},
			unwantedSQL: "TRIGGER",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatements(test.diff, test.desired, "sqlite")
			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(plan, qt.Not(qt.Contains), test.unwantedSQL, qt.Commentf("plan:\n%s", plan))
		})
	}
}

// TestViewAndTriggerLookupsFoldASCIICase is the capability half, and it is the
// half the structural rule could not answer at all: SQLite folds ASCII, so
// `Active_Notes` and `active_notes` are one object and only a folding rule joins
// them.
func TestViewAndTriggerLookupsFoldASCIICase(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		diff    *difftypes.SchemaDiff
		wantSQL string
	}{
		{
			name: "a trigger whose table is spelled in a different case",
			desired: &schemamodel.Database{
				Triggers: []schemamodel.Trigger{{
					StructName: "Note",
					Name:       "touch",
					Table:      "Notes",
					Timing:     "AFTER",
					Event:      "UPDATE",
					ForEach:    "ROW",
					Body:       "SELECT 1;",
				}},
			},
			diff: &difftypes.SchemaDiff{TriggersAdded: []difftypes.TriggerRef{{
				TriggerName: "touch",
				TableName:   "notes",
			}}},
			wantSQL: "TRIGGER",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatements(test.diff, test.desired, "sqlite")
			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(plan, qt.Contains, test.wantSQL, qt.Commentf("plan:\n%s", plan))
		})
	}
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

	sql, err := planner.GenerateSchemaDiffSQL(diff, desired, platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "VIEW")
}
