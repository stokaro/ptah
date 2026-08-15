package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/types"
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
		generated   *goschema.Database
		diff        *types.SchemaDiff
		unwantedSQL string
	}{
		{
			name: "a modified view declared in another schema is left alone",
			generated: &goschema.Database{
				Views: []goschema.View{{
					Name: "reporting.active_users",
					Body: "SELECT id FROM users WHERE active",
				}},
			},
			diff: &types.SchemaDiff{ViewsModified: []types.ViewDiff{{
				ViewName:     "app.active_users",
				PreviousBody: "SELECT id FROM users",
				Changes:      map[string]string{"body": "changed"},
			}}},
			unwantedSQL: "VIEW",
		},
		{
			name: "a materialized view declared in another schema is left alone",
			generated: &goschema.Database{
				MaterializedViews: []goschema.MaterializedView{{
					Name: "reporting.user_stats",
					Body: "SELECT count(*) FROM users",
				}},
			},
			diff: &types.SchemaDiff{MaterializedViewsModified: []types.MaterializedViewDiff{{
				ViewName: "app.user_stats",
				Changes:  map[string]string{"body": "changed"},
			}}},
			unwantedSQL: "MATERIALIZED VIEW",
		},
		{
			name: "a trigger whose table is declared in another schema is left alone",
			generated: &goschema.Database{
				Triggers: []goschema.Trigger{{
					StructName: "User",
					Name:       "touch",
					Table:      "reporting.users",
					Timing:     "AFTER",
					Event:      "UPDATE",
					ForEach:    "ROW",
					Body:       "BEGIN RETURN NEW; END;",
				}},
			},
			diff: &types.SchemaDiff{TriggersAdded: []types.TriggerRef{{
				TriggerName: "touch",
				TableName:   "app.users",
			}}},
			unwantedSQL: "TRIGGER",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatements(test.diff, test.generated, "postgres")
			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(plan, qt.Not(qt.Contains), test.unwantedSQL, qt.Commentf("plan:\n%s", plan))
		})
	}
}

// TestViewAndTriggerLookupsResolveAcrossSpellings is the capability half: the
// spellings that DO name one object still resolve, so the rows above are a
// refusal to guess rather than a refusal to work.
func TestViewAndTriggerLookupsResolveAcrossSpellings(t *testing.T) {
	tests := []struct {
		name      string
		generated *goschema.Database
		diff      *types.SchemaDiff
		wantSQL   string
	}{
		{
			name: "a modified view the diff qualifies with public",
			generated: &goschema.Database{
				Views: []goschema.View{{
					Name: "active_users",
					Body: "SELECT id FROM users WHERE active",
				}},
			},
			diff: &types.SchemaDiff{ViewsModified: []types.ViewDiff{{
				ViewName:     "public.active_users",
				PreviousBody: "SELECT id FROM users",
				Changes:      map[string]string{"body": "changed"},
			}}},
			wantSQL: "VIEW",
		},
		{
			name: "a trigger whose table the diff qualifies with public",
			generated: &goschema.Database{
				Triggers: []goschema.Trigger{{
					StructName: "User",
					Name:       "touch",
					Table:      "users",
					Timing:     "AFTER",
					Event:      "UPDATE",
					ForEach:    "ROW",
					Body:       "BEGIN RETURN NEW; END;",
				}},
			},
			diff: &types.SchemaDiff{TriggersAdded: []types.TriggerRef{{
				TriggerName: "touch",
				TableName:   "public.users",
			}}},
			wantSQL: "TRIGGER",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatements(test.diff, test.generated, "postgres")
			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(plan, qt.Contains, test.wantSQL, qt.Commentf("plan:\n%s", plan))
		})
	}
}
