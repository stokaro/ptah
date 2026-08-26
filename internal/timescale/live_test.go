package timescale_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/timescale"
)

// TestValidateLive_RefusesADeclarationThatWantsAnAggregatesName holds the one
// refusal, and the cases that must not fire.
//
// The refusal is about a name collision the server resolves badly: a
// continuous aggregate occupies its name as a view, so a declaration naming a
// view with that name reaches CREATE VIEW and the server answers
// `relation ... already exists` halfway through the script.
func TestValidateLive_RefusesADeclarationThatWantsAnAggregatesName(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		desired *schemamodel.Database
		current *catalog.Database
		wantErr string
		// wantProblems is how many refusals the row expects. It is here
		// because qt.Contains with an empty substring matches ANY string, so
		// the rows that expect no refusal would assert nothing without it.
		wantProblems int
	}{
		{
			name:    "a declared view with the aggregate's name",
			dialect: platform.Postgres,
			desired: &schemamodel.Database{
				Views: []schemamodel.View{{Name: "conditions_hourly", Body: "SELECT 1"}},
			},
			current:      liveWithAggregate(),
			wantErr:      `declared view "conditions_hourly" is a TimescaleDB continuous aggregate`,
			wantProblems: 1,
		},
		{
			name:    "a declared table with the aggregate's name",
			dialect: platform.Postgres,
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "C", Name: "conditions_hourly"}},
			},
			current:      liveWithAggregate(),
			wantErr:      `declared table "conditions_hourly" is a TimescaleDB continuous aggregate`,
			wantProblems: 1,
		},
		{
			name:    "a declared materialized view with the aggregate's name",
			dialect: platform.Postgres,
			desired: &schemamodel.Database{
				MaterializedViews: []schemamodel.MaterializedView{
					{Name: "conditions_hourly", Body: "SELECT 1"},
				},
			},
			current:      liveWithAggregate(),
			wantErr:      `declared materialized view "conditions_hourly" is a TimescaleDB continuous aggregate`,
			wantProblems: 1,
		},
		{
			// The reader blanks the connection's own schema, and a declaration
			// that names no schema means the same one. Keying on the raw
			// strings would let the collision through.
			name:    "the schema is folded on both sides",
			dialect: platform.Postgres,
			desired: &schemamodel.Database{
				Views: []schemamodel.View{{Name: "APP.Conditions_Hourly", Body: "SELECT 1"}},
			},
			current: &catalog.Database{ContinuousAggregates: []catalog.ContinuousAggregate{{
				Schema: "app", Name: "conditions_hourly",
				HypertableSchema: "app", HypertableName: "conditions",
			}}},
			wantErr:      `declared view "APP.Conditions_Hourly" is a TimescaleDB continuous aggregate`,
			wantProblems: 1,
		},
		{
			name:    "a declaration that wants a different name",
			dialect: platform.Postgres,
			desired: &schemamodel.Database{Views: []schemamodel.View{{Name: "other", Body: "SELECT 1"}}},
			current: liveWithAggregate(),
		},
		{
			name:    "a server with no continuous aggregates",
			dialect: platform.Postgres,
			desired: &schemamodel.Database{
				Views: []schemamodel.View{{Name: "conditions_hourly", Body: "SELECT 1"}},
			},
			current: &catalog.Database{},
		},
		{
			// The catalog is PostgreSQL's, and so is the collision. A MySQL
			// comparison must not be asked about it.
			name:    "another dialect entirely",
			dialect: platform.MySQL,
			desired: &schemamodel.Database{
				Views: []schemamodel.View{{Name: "conditions_hourly", Body: "SELECT 1"}},
			},
			current: liveWithAggregate(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := timescale.ValidateLive(test.dialect, test.desired, test.current)

			c.Assert(errText(err), qt.Contains, test.wantErr)
			c.Assert(problemCount(err), qt.Equals, test.wantProblems)
		})
	}
}

func liveWithAggregate() *catalog.Database {
	return &catalog.Database{ContinuousAggregates: []catalog.ContinuousAggregate{{
		Name:             "conditions_hourly",
		HypertableSchema: "public",
		HypertableName:   "conditions",
		Definition:       "SELECT 1",
	}}}
}

// errText renders an error for a table row without a branch in the test body.
func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// problemCount counts the refusals errors.Join carried, which is one per line.
//
// It exists because qt.Contains with an empty substring matches any string: a
// row expecting no refusal would pass against a refusal without this.
func problemCount(err error) int {
	if err == nil {
		return 0
	}
	return len(strings.Split(err.Error(), "\n"))
}
