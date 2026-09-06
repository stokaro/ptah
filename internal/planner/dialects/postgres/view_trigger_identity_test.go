package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestViewAndTriggerLookupsDoNotCrossSchemas pins the view, materialized-view
// and trigger call sites at the thing that changed about them.
//
// These were already routed through objectlookup on master, but through a
// STRUCTURAL rule: exact, then unqualified, with no folding, no default-schema
// resolution and -- the part that matters here -- no refusal when both sides
// name a schema and the two disagree. Every row below stays green under the
// current rule and turns red under that structural one, so the rows measure the
// call sites rather than the helper.
//
// Crossing schemas here is not a missing statement, it is a wrong one: the view
// body or trigger definition of `reporting.x` rendered under the name `app.x`.
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
					Name: "reporting.active_users",
					Body: "SELECT id FROM users WHERE active",
				}},
			},
			diff: &difftypes.SchemaDiff{ViewsModified: []difftypes.ViewDiff{{
				ViewName:     "app.active_users",
				PreviousBody: "SELECT id FROM users",
				Changes:      map[string]string{"body": "changed"},
			}}},
			unwantedSQL: "VIEW",
		},
		{
			name: "a materialized view declared in another schema is left alone",
			desired: &schemamodel.Database{
				MaterializedViews: []schemamodel.MaterializedView{{
					Name: "reporting.user_stats",
					Body: "SELECT count(*) FROM users",
				}},
			},
			diff: &difftypes.SchemaDiff{MaterializedViewsModified: []difftypes.MaterializedViewDiff{{
				ViewName: "app.user_stats",
				Changes:  map[string]string{"body": "changed"},
			}}},
			unwantedSQL: "MATERIALIZED VIEW",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatements(test.diff, "postgres")
			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(plan, qt.Not(qt.Contains), test.unwantedSQL, qt.Commentf("plan:\n%s", plan))
		})
	}
}

// TestCompare_ATriggerResolvesTheDiffSpelling is the capability half of the
// refusal above, measured where it now happens.
//
// A declaration writes the table as `users` and a server reports it as
// `public.users`. The planner used to reconcile the two by looking the trigger
// up; the comparison does it now, and the entry carries the trigger it resolved
// to (stokaro/ptah#2315). Driving the comparison rather than hand-building the
// diff is the point: a hand-built one bypasses the resolution, which is exactly
// what the planner no longer performs.
//
// The refusal half moved too, and its home is
// TestCompareWithDialect_DifferentSchemaDoesNotMatchSchemaObjects in
// migration/schemadiff: a trigger on `public.items` and one on
// `reporting.items` come back as one addition and one removal, never as a
// modification of a single object.
func TestCompare_ATriggerResolvesTheDiffSpelling(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Triggers: []schemamodel.Trigger{{
			StructName: "User",
			Name:       "touch",
			Table:      "users",
			Timing:     "AFTER",
			Event:      "UPDATE",
			ForEach:    "ROW",
			Body:       "BEGIN RETURN NEW; END;",
		}},
	}
	database := &catalog.Database{
		Triggers: []catalog.Trigger{{
			Schema: "public", Name: "touch", Table: "users",
			Timing: "AFTER", Event: "UPDATE", ForEach: "ROW",
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, database, "postgres")

	c.Assert(diff.TriggersAdded, qt.HasLen, 0,
		qt.Commentf("the two spellings are one trigger, not one to add and one to drop"))
	c.Assert(diff.TriggersRemoved, qt.HasLen, 0)
	c.Assert(diff.TriggersModified, qt.HasLen, 1)
	c.Assert(diff.TriggersModified[0].Desired.Name, qt.Equals, "touch",
		qt.Commentf("the comparison resolved the two spellings to one declaration"))

	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "TRIGGER")
}

// TestCompare_AModifiedViewResolvesTheDiffSpelling is the view half of the
// capability above, measured where it now happens.
//
// A declaration writes `active_users` and a server reports `public.active_users`.
// The planner used to reconcile the two by looking the name up; the comparison
// does it now, and the change carries the view it resolved to. Driving the
// comparison rather than hand-building the diff is the point: a hand-built one
// bypasses the resolution, which is exactly what the planner no longer performs.
func TestCompare_AModifiedViewResolvesTheDiffSpelling(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Views: []schemamodel.View{{Name: "active_users", Body: "SELECT id FROM users WHERE active"}},
	}
	database := &catalog.Database{
		Views: []catalog.View{{Schema: "public", Name: "active_users", Body: "SELECT id FROM users"}},
	}

	diff := schemadiff.CompareWithDialect(desired, database, "postgres")

	c.Assert(diff.ViewsModified, qt.HasLen, 1)
	c.Assert(diff.ViewsModified[0].Desired.Name, qt.Equals, "active_users",
		qt.Commentf("the comparison resolved the two spellings to one declaration"))

	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "VIEW")
}
