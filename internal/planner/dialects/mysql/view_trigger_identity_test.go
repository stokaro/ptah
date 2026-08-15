package mysql_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/types"
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
		generated   *goschema.Database
		diff        *types.SchemaDiff
		unwantedSQL string
	}{
		{
			name: "a modified view declared in another database is left alone",
			generated: &goschema.Database{
				Views: []goschema.View{{
					Name: "reporting.active_orders",
					Body: "SELECT id FROM orders WHERE open",
				}},
			},
			diff: &types.SchemaDiff{ViewsModified: []types.ViewDiff{{
				ViewName:     "app.active_orders",
				PreviousBody: "SELECT id FROM orders",
				Changes:      map[string]string{"body": "changed"},
			}}},
			unwantedSQL: "VIEW",
		},
		{
			name: "a trigger whose table is declared in another database is left alone",
			generated: &goschema.Database{
				Triggers: []goschema.Trigger{{
					StructName: "Order",
					Name:       "touch",
					Table:      "reporting.orders",
					Timing:     "AFTER",
					Event:      "UPDATE",
					ForEach:    "ROW",
					Body:       "SET @x = 1;",
				}},
			},
			diff: &types.SchemaDiff{TriggersAdded: []types.TriggerRef{{
				TriggerName: "touch",
				TableName:   "app.orders",
			}}},
			unwantedSQL: "TRIGGER",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatements(test.diff, test.generated, "mysql")
			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(plan, qt.Not(qt.Contains), test.unwantedSQL, qt.Commentf("plan:\n%s", plan))
		})
	}
}

// TestViewAndTriggerLookupsResolveAcrossDatabaseSpellings is the capability
// half. MySQL's reader reports the database name for every view while a Go
// annotation leaves it bare, which is the case that made objectlookup exist
// (stokaro/ptah#1287) and the one no static default schema can answer.
func TestViewAndTriggerLookupsResolveAcrossDatabaseSpellings(t *testing.T) {
	tests := []struct {
		name      string
		generated *goschema.Database
		diff      *types.SchemaDiff
		wantSQL   string
	}{
		{
			name: "a modified view the diff qualifies with the database",
			generated: &goschema.Database{
				Views: []goschema.View{{
					Name: "active_orders",
					Body: "SELECT id FROM orders WHERE open",
				}},
			},
			diff: &types.SchemaDiff{ViewsModified: []types.ViewDiff{{
				ViewName:     "app.active_orders",
				PreviousBody: "SELECT id FROM orders",
				Changes:      map[string]string{"body": "changed"},
			}}},
			wantSQL: "VIEW",
		},
		{
			name: "a trigger whose table the diff qualifies with the database",
			generated: &goschema.Database{
				Triggers: []goschema.Trigger{{
					StructName: "Order",
					Name:       "touch",
					Table:      "orders",
					Timing:     "AFTER",
					Event:      "UPDATE",
					ForEach:    "ROW",
					Body:       "SET @x = 1;",
				}},
			},
			diff: &types.SchemaDiff{TriggersAdded: []types.TriggerRef{{
				TriggerName: "touch",
				TableName:   "app.orders",
			}}},
			wantSQL: "TRIGGER",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatements(test.diff, test.generated, "mysql")
			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(plan, qt.Contains, test.wantSQL, qt.Commentf("plan:\n%s", plan))
		})
	}
}
