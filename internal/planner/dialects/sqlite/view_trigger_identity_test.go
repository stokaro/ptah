package sqlite_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestViewAndTriggerLookupsDoNotCrossSchemas pins sqlite's findView and
// findTrigger at the thing that changed about them: the structural rule master
// used resolved a name across two DIFFERENT schemas, so `attached.v`'s body was
// rendered under the name the diff carried.
func TestViewAndTriggerLookupsDoNotCrossSchemas(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name        string
		generated   *goschema.Database
		diff        *types.SchemaDiff
		unwantedSQL string
	}{
		{
			name: "a modified view declared in another schema is left alone",
			generated: &goschema.Database{
				Views: []goschema.View{{
					Name: "attached.active_notes",
					Body: "SELECT id FROM notes WHERE body IS NOT NULL",
				}},
			},
			diff: &types.SchemaDiff{ViewsModified: []types.ViewDiff{{
				ViewName:     "main.active_notes",
				PreviousBody: "SELECT id FROM notes",
				Changes:      map[string]string{"body": "changed"},
			}}},
			unwantedSQL: "VIEW",
		},
		{
			name: "a trigger whose table is declared in another schema is left alone",
			generated: &goschema.Database{
				Triggers: []goschema.Trigger{{
					StructName: "Note",
					Name:       "touch",
					Table:      "attached.notes",
					Timing:     "AFTER",
					Event:      "UPDATE",
					ForEach:    "ROW",
					Body:       "SELECT 1;",
				}},
			},
			diff: &types.SchemaDiff{TriggersAdded: []types.TriggerRef{{
				TriggerName: "touch",
				TableName:   "main.notes",
			}}},
			unwantedSQL: "TRIGGER",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			statements, err := planner.GenerateSchemaDiffSQLStatements(test.diff, test.generated, "sqlite")
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
	c := qt.New(t)

	tests := []struct {
		name      string
		generated *goschema.Database
		diff      *types.SchemaDiff
		wantSQL   string
	}{
		{
			name: "a modified view spelled in a different case",
			generated: &goschema.Database{
				Views: []goschema.View{{
					Name: "Active_Notes",
					Body: "SELECT id FROM notes WHERE body IS NOT NULL",
				}},
			},
			diff: &types.SchemaDiff{ViewsModified: []types.ViewDiff{{
				ViewName:     "active_notes",
				PreviousBody: "SELECT id FROM notes",
				Changes:      map[string]string{"body": "changed"},
			}}},
			wantSQL: "VIEW",
		},
		{
			name: "a trigger whose table is spelled in a different case",
			generated: &goschema.Database{
				Triggers: []goschema.Trigger{{
					StructName: "Note",
					Name:       "touch",
					Table:      "Notes",
					Timing:     "AFTER",
					Event:      "UPDATE",
					ForEach:    "ROW",
					Body:       "SELECT 1;",
				}},
			},
			diff: &types.SchemaDiff{TriggersAdded: []types.TriggerRef{{
				TriggerName: "touch",
				TableName:   "notes",
			}}},
			wantSQL: "TRIGGER",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			statements, err := planner.GenerateSchemaDiffSQLStatements(test.diff, test.generated, "sqlite")
			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(plan, qt.Contains, test.wantSQL, qt.Commentf("plan:\n%s", plan))
		})
	}
}
