package postgres

// White-box testing required: whether the hypertable catalog is asked at all is
// decided inside the reader from the extension list, and the exported read
// returns the same empty list either way -- for a PostgreSQL server that has no
// TimescaleDB, and for one whose catalog answered nothing.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// TestReadHypertables_CarriesThePrimaryDimension pins what the read takes from
// the catalog.
//
// The primary dimension is the whole reason this read exists rather than a
// bare list of names: the note it feeds has to say WHAT was lost, and "the
// table is partitioned on time" is that. Measured on TimescaleDB 2.29.2 /
// PostgreSQL 17.11, timescaledb_information.hypertables answers exactly these
// columns for `create_hypertable('conditions', by_range('time'))`.
func TestReadHypertables_CarriesThePrimaryDimension(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringHypertables)
	reader := NewPostgreSQLReader(db.SQL, "public")

	hypertables, err := reader.readHypertables(timescaleInstalled())

	c.Assert(err, qt.IsNil)
	c.Assert(hypertables, qt.DeepEquals, []types.DBHypertable{{
		Name:                 "conditions",
		PrimaryDimension:     "time",
		PrimaryDimensionType: "timestamp with time zone",
		Dimensions:           1,
	}})
}

// TestReadHypertables_AsksNothingWithoutTheExtension is the ordinary
// PostgreSQL case, and the assertion is that no statement is sent at all.
//
// Counting queries rather than checking the result is the whole point: an
// empty answer is what BOTH a skipped read and a tolerated failure produce, and
// only one of them leaves the transaction usable. A failed statement aborts the
// enclosing PostgreSQL transaction, so every later read answers SQLSTATE 25P02
// rather than the question it was asked.
func TestReadHypertables_AsksNothingWithoutTheExtension(t *testing.T) {
	c := qt.New(t)
	var asked []string
	db := dbtest.Open(t, recordingQueries(&asked))
	reader := NewPostgreSQLReader(db.SQL, "public")

	hypertables, err := reader.readHypertables([]types.DBExtension{{Name: "plpgsql"}})

	c.Assert(err, qt.IsNil)
	c.Assert(hypertables, qt.HasLen, 0)
	c.Assert(asked, qt.HasLen, 0)
}

// TestReadHypertables_AFailureWithTheExtensionIsSurfaced is the control the
// gate needs.
//
// Once the extension IS installed the catalog is there, so a failure means
// something else, and an empty answer would claim that no table on this server
// is partitioned. The consequence of that claim is not a wrong statement but a
// MISSING note, which is the whole thing this read exists to produce.
func TestReadHypertables_AFailureWithTheExtensionIsSurfaced(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, faultingHypertables)
	reader := NewPostgreSQLReader(db.SQL, "public")

	_, err := reader.readHypertables(timescaleInstalled())

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "permission denied")
}

// TestReadHypertables_TakesAHypertableWithNoDimensionReported keeps a shape the
// view declares nullable from failing the whole read.
//
// primary_dimension and primary_dimension_type are nullable columns. A
// hypertable always has one today, and scanning into a plain string would turn
// the day that stops being true into a failed description rather than a note
// with one detail missing.
func TestReadHypertables_TakesAHypertableWithNoDimensionReported(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, dimensionlessHypertable)
	reader := NewPostgreSQLReader(db.SQL, "public")

	hypertables, err := reader.readHypertables(timescaleInstalled())

	c.Assert(err, qt.IsNil)
	c.Assert(hypertables, qt.DeepEquals, []types.DBHypertable{{
		Name: "conditions", Dimensions: 1,
	}})
}

// TestHypertableQuery_ReadsTheExtensionsOwnCatalog pins where the answer comes
// from, because nothing else can give it.
//
// Measured on 2.29.2: after create_hypertable, pg_class reports relkind 'r'
// and pg_depend reports no extension ownership for the table, so a rule built
// on either would find nothing. The auto-created index is no help either --
// its pg_depend deptype is 'a', which is what an ordinary user index on an
// ordinary table has too.
func TestHypertableQuery_ReadsTheExtensionsOwnCatalog(t *testing.T) {
	tests := []struct {
		name     string
		fragment string
	}{
		{name: "the catalog", fragment: "timescaledb_information.hypertables"},
		{name: "the primary dimension", fragment: "h.primary_dimension"},
		{name: "the dimension count", fragment: "h.num_dimensions"},
		{name: "scoped to one schema", fragment: "h.hypertable_schema = $1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(hypertableQuery, qt.Contains, test.fragment)
			c.Assert(hypertableQuery, qt.Not(qt.Contains), "pg_depend")
		})
	}
}

func answeringHypertables(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	return hypertableAnswer(query, nil, hypertableRows())
}

func faultingHypertables(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	return hypertableAnswer(query,
		sqlStateError{state: "42501", message: "permission denied for schema timescaledb_information"},
		nil)
}

func dimensionlessHypertable(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	return hypertableAnswer(query, nil, [][]driver.Value{{"public", "conditions", nil, nil, int64(1)}})
}

func hypertableRows() [][]driver.Value {
	return [][]driver.Value{{"public", "conditions", "time", "timestamp with time zone", int64(1)}}
}

// hypertableAnswer answers the catalog with the five columns the read scans, so
// a query that stopped selecting one fails here rather than being handed the
// same rows.
func hypertableAnswer(query string, refusal error, rows [][]driver.Value) (dbtest.QueryResult, error) {
	if !strings.Contains(query, "timescaledb_information.hypertables") {
		return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
	}
	if refusal != nil {
		return dbtest.QueryResult{}, refusal
	}
	return dbtest.QueryResult{
		Columns: []string{
			"hypertable_schema", "hypertable_name",
			"primary_dimension", "primary_dimension_type", "num_dimensions",
		},
		Rows: rows,
	}, nil
}
