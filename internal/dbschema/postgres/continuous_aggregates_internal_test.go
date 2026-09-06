package postgres

// White-box testing required: whether a server has continuous aggregates is
// decided from a driver error code inside the reader, and the exported read
// returns the same empty list either way -- for a PostgreSQL server that has
// none, and for one whose extension catalog answered something else.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/dbschema/dbtest"
)

// sqlStateError is what a PostgreSQL driver hands back: an error carrying the
// server's SQLSTATE.
type sqlStateError struct {
	state   string
	message string
}

func (e sqlStateError) Error() string    { return e.message }
func (e sqlStateError) SQLState() string { return e.state }

// TestHasTimescaleExtension_DecidesWhetherTheCatalogIsAskedAtAll holds the
// gate that keeps the aggregate query off a server that would refuse it.
//
// The gate is not an optimization. A failed statement ABORTS the enclosing
// PostgreSQL transaction, so a read that asked anyway and tolerated the
// failure left every later statement answering `current transaction is
// aborted, commands ignored until end of transaction block` (SQLSTATE 25P02).
// That is what an earlier draft did, and no unit test showed it: the failure
// is in the statements that come after.
func TestHasTimescaleExtension_DecidesWhetherTheCatalogIsAskedAtAll(t *testing.T) {
	tests := []struct {
		name       string
		extensions []catalog.Extension
		want       bool
	}{
		{
			name:       "installed",
			extensions: []catalog.Extension{{Name: "plpgsql"}, {Name: "timescaledb"}},
			want:       true,
		},
		{
			name:       "installed under another spelling",
			extensions: []catalog.Extension{{Name: "TimescaleDB"}},
			want:       true,
		},
		{
			name:       "an ordinary PostgreSQL server",
			extensions: []catalog.Extension{{Name: "plpgsql"}, {Name: "pg_trgm"}},
			want:       false,
		},
		{
			// A read whose preset left the extension list out reports none,
			// and that is the right answer for the targets it happens to:
			// neither Spanner nor a catalog without pg_extension has
			// TimescaleDB.
			name:       "a read that did not look",
			extensions: nil,
			want:       false,
		},
		{
			name:       "a name that merely contains it",
			extensions: []catalog.Extension{{Name: "timescaledb_toolkit"}},
			want:       false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(hasTimescaleExtension(test.extensions), qt.Equals, test.want)
		})
	}
}

// TestReadContinuousAggregates_CarriesTheWrittenDefinition pins what the read
// takes from the catalog.
func TestReadContinuousAggregates_CarriesTheWrittenDefinition(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringContinuousAggregates)
	reader := NewPostgreSQLReader(db.SQL, "public")

	aggregates, err := reader.readContinuousAggregates(t.Context(), timescaleInstalled())

	c.Assert(err, qt.IsNil)
	c.Assert(aggregates, qt.DeepEquals, []catalog.ContinuousAggregate{{
		Name:             "conditions_hourly",
		HypertableSchema: "public",
		HypertableName:   "conditions",
		MaterializedOnly: true,
		Definition:       "SELECT device_id, avg(temperature) FROM conditions GROUP BY 1",
	}})
}

// TestReadContinuousAggregates_AsksNothingWithoutTheExtension is the ordinary
// PostgreSQL case, and the assertion is that no statement is sent at all.
//
// Counting queries rather than checking the result is the whole point: an
// empty answer is what BOTH a skipped read and a tolerated failure produce,
// and only one of them leaves the transaction usable.
func TestReadContinuousAggregates_AsksNothingWithoutTheExtension(t *testing.T) {
	c := qt.New(t)
	var asked []string
	db := dbtest.Open(t, recordingQueries(&asked))
	reader := NewPostgreSQLReader(db.SQL, "public")

	aggregates, err := reader.readContinuousAggregates(t.Context(), []catalog.Extension{{Name: "plpgsql"}})

	c.Assert(err, qt.IsNil)
	c.Assert(aggregates, qt.HasLen, 0)
	c.Assert(asked, qt.HasLen, 0)
}

// TestReadContinuousAggregates_AFailureWithTheExtensionIsSurfaced is the
// control the gate needs.
//
// Once the extension IS installed the catalog is there, so a failure means
// something else, and an empty answer would claim the server has no continuous
// aggregates -- hiding exactly the objects a plan must not treat as views.
func TestReadContinuousAggregates_AFailureWithTheExtensionIsSurfaced(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, faultingContinuousAggregates)
	reader := NewPostgreSQLReader(db.SQL, "public")

	_, err := reader.readContinuousAggregates(t.Context(), timescaleInstalled())

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "permission denied")
}

// TestWithoutContinuousAggregates_RemovesOnlyTheAggregates pins the filter
// that keeps a continuous aggregate out of the view list.
func TestWithoutContinuousAggregates_RemovesOnlyTheAggregates(t *testing.T) {
	tests := []struct {
		name       string
		views      []catalog.View
		aggregates []catalog.ContinuousAggregate
		want       []string
	}{
		{
			name:  "the aggregate goes and the view stays",
			views: []catalog.View{{Name: "conditions_hourly"}, {Name: "ordinary"}},
			aggregates: []catalog.ContinuousAggregate{
				{Name: "conditions_hourly"},
			},
			want: []string{"ordinary"},
		},
		{
			name:  "a server with no aggregates keeps every view",
			views: []catalog.View{{Name: "conditions_hourly"}, {Name: "ordinary"}},
			want:  []string{"conditions_hourly", "ordinary"},
		},
		{
			// The two lists are read through different catalogs, and one may
			// qualify a name the other does not. Comparing raw strings would
			// leave the aggregate in the view list on a schema-scoped read.
			name:  "the schema is folded on both sides",
			views: []catalog.View{{Schema: "APP", Name: "Conditions_Hourly"}},
			aggregates: []catalog.ContinuousAggregate{
				{Schema: "app", Name: "conditions_hourly"},
			},
			want: nil,
		},
		{
			name:  "an aggregate in another schema leaves this one's views alone",
			views: []catalog.View{{Schema: "app", Name: "conditions_hourly"}},
			aggregates: []catalog.ContinuousAggregate{
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

func viewNames(views []catalog.View) []string {
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

// timescaleInstalled is the extension list of a server that has it.
func timescaleInstalled() []catalog.Extension {
	return []catalog.Extension{{Name: "plpgsql"}, {Name: "timescaledb"}}
}

// recordingQueries answers nothing and remembers what it was asked.
func recordingQueries(asked *[]string) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		*asked = append(*asked, query)
		return dbtest.QueryResult{}, nil
	}
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
