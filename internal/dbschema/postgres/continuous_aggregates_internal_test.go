package postgres

// White-box testing required: whether a server has continuous aggregates is
// decided from a driver error code inside the reader, and the exported read
// returns the same empty list either way -- for a PostgreSQL server that has
// none, and for one whose extension catalog answered something else.

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// sqlStateError is what a PostgreSQL driver hands back: an error carrying the
// server's SQLSTATE.
type sqlStateError struct {
	state   string
	message string
}

func (e sqlStateError) Error() string    { return e.message }
func (e sqlStateError) SQLState() string { return e.state }

// TestIsUndefinedTable_ReadsTheCodeRatherThanTheMessage holds the distinction
// the degradation rests on.
//
// A server without the extension answers 42P01 to a query naming
// timescaledb_information.continuous_aggregates, and that means "this server
// has no continuous aggregates". Any other failure means something else, and
// reading it as an absence would describe a broken server as a clean one.
func TestIsUndefinedTable_ReadsTheCodeRatherThanTheMessage(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "the relation is not there",
			err:  sqlStateError{state: "42P01", message: `relation "x" does not exist`},
			want: true,
		},
		{
			name: "wrapped, because database/sql wraps",
			err: fmt.Errorf("read: %w",
				sqlStateError{state: "42P01", message: `relation "x" does not exist`}),
			want: true,
		},
		{
			name: "insufficient privilege is not an absence",
			err:  sqlStateError{state: "42501", message: "permission denied"},
			want: false,
		},
		{
			name: "an undefined COLUMN is not an undefined table",
			err:  sqlStateError{state: "42703", message: `column "x" does not exist`},
			want: false,
		},
		{
			name: "a message that says the words but carries no code",
			err:  errors.New(`relation "timescaledb_information.continuous_aggregates" does not exist`),
			want: false,
		},
		{name: "no error", err: nil, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(isUndefinedTable(test.err), qt.Equals, test.want)
		})
	}
}

// TestReadContinuousAggregates_CarriesTheWrittenDefinition pins what the read
// takes from the catalog.
func TestReadContinuousAggregates_CarriesTheWrittenDefinition(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringContinuousAggregates)
	reader := NewPostgreSQLReader(db.SQL, "public")

	aggregates, err := reader.readContinuousAggregates()

	c.Assert(err, qt.IsNil)
	c.Assert(aggregates, qt.DeepEquals, []types.DBContinuousAggregate{{
		Name:             "conditions_hourly",
		HypertableSchema: "public",
		HypertableName:   "conditions",
		MaterializedOnly: true,
		Definition:       "SELECT device_id, avg(temperature) FROM conditions GROUP BY 1",
	}})
}

// TestReadContinuousAggregates_AnAbsentCatalogIsAnEmptyAnswer is the ordinary
// PostgreSQL case, which is every PostgreSQL server that does not have the
// extension installed.
func TestReadContinuousAggregates_AnAbsentCatalogIsAnEmptyAnswer(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, refusingContinuousAggregates)
	reader := NewPostgreSQLReader(db.SQL, "public")

	aggregates, err := reader.readContinuousAggregates()

	c.Assert(err, qt.IsNil)
	c.Assert(aggregates, qt.HasLen, 0)
}

// TestReadContinuousAggregates_AnyOtherFailureIsSurfaced is the control the
// two above need: one code apart, and the reader has to fail rather than
// describe a server it could not read.
func TestReadContinuousAggregates_AnyOtherFailureIsSurfaced(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, faultingContinuousAggregates)
	reader := NewPostgreSQLReader(db.SQL, "public")

	_, err := reader.readContinuousAggregates()

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "permission denied")
}

// TestWithoutContinuousAggregates_RemovesOnlyTheAggregates pins the filter
// that keeps a continuous aggregate out of the view list.
func TestWithoutContinuousAggregates_RemovesOnlyTheAggregates(t *testing.T) {
	tests := []struct {
		name       string
		views      []types.DBView
		aggregates []types.DBContinuousAggregate
		want       []string
	}{
		{
			name:  "the aggregate goes and the view stays",
			views: []types.DBView{{Name: "conditions_hourly"}, {Name: "ordinary"}},
			aggregates: []types.DBContinuousAggregate{
				{Name: "conditions_hourly"},
			},
			want: []string{"ordinary"},
		},
		{
			name:  "a server with no aggregates keeps every view",
			views: []types.DBView{{Name: "conditions_hourly"}, {Name: "ordinary"}},
			want:  []string{"conditions_hourly", "ordinary"},
		},
		{
			// The two lists are read through different catalogs, and one may
			// qualify a name the other does not. Comparing raw strings would
			// leave the aggregate in the view list on a schema-scoped read.
			name:  "the schema is folded on both sides",
			views: []types.DBView{{Schema: "APP", Name: "Conditions_Hourly"}},
			aggregates: []types.DBContinuousAggregate{
				{Schema: "app", Name: "conditions_hourly"},
			},
			want: nil,
		},
		{
			name:  "an aggregate in another schema leaves this one's views alone",
			views: []types.DBView{{Schema: "app", Name: "conditions_hourly"}},
			aggregates: []types.DBContinuousAggregate{
				{Schema: "other", Name: "conditions_hourly"},
			},
			want: []string{"conditions_hourly"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(viewNames(withoutContinuousAggregates(test.views, test.aggregates)),
				qt.DeepEquals, test.want)
		})
	}
}

// TestContinuousAggregateQuery_AsksForTheWrittenDefinition holds the one
// column the whole read exists for, on the query itself.
//
// A scripted server answers whatever it is told, so a query that had gone back
// to pg_get_viewdef would return the same fixture rows and pass.
func TestContinuousAggregateQuery_AsksForTheWrittenDefinition(t *testing.T) {
	tests := []struct {
		name     string
		fragment string
	}{
		{name: "the extension's own catalog", fragment: "timescaledb_information.continuous_aggregates"},
		{name: "the written definition", fragment: "c.view_definition"},
		{name: "the hypertable it materializes", fragment: "c.hypertable_name"},
		{name: "scoped to one schema", fragment: "WHERE c.view_schema = $1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(continuousAggregateQuery, qt.Contains, test.fragment)
			c.Assert(continuousAggregateQuery, qt.Not(qt.Contains), "pg_get_viewdef")
		})
	}
}

func viewNames(views []types.DBView) []string {
	var names []string
	for _, view := range views {
		names = append(names, view.Name)
	}
	return names
}

func answeringContinuousAggregates(
	query string, _ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	return continuousAggregateAnswer(query, nil)
}

func refusingContinuousAggregates(
	query string, _ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	return continuousAggregateAnswer(query,
		sqlStateError{state: "42P01", message: `relation "timescaledb_information.continuous_aggregates" does not exist`})
}

func faultingContinuousAggregates(
	query string, _ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	return continuousAggregateAnswer(query,
		sqlStateError{state: "42501", message: "permission denied for schema timescaledb_information"})
}

// continuousAggregateAnswer answers the catalog with the six columns the read
// scans, so a query that stopped selecting one fails here rather than being
// handed the same rows.
func continuousAggregateAnswer(query string, refusal error) (dbtest.QueryResult, error) {
	if !strings.Contains(query, "continuous_aggregates") {
		return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
	}
	if refusal != nil {
		return dbtest.QueryResult{}, refusal
	}
	return dbtest.QueryResult{
		Columns: []string{
			"view_schema", "view_name", "hypertable_schema", "hypertable_name",
			"materialized_only", "view_definition",
		},
		Rows: [][]driver.Value{{
			"public", "conditions_hourly", "public", "conditions", true,
			"  SELECT device_id, avg(temperature) FROM conditions GROUP BY 1  ",
		}},
	}, nil
}
