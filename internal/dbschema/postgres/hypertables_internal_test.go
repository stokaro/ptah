package postgres

// White-box testing required: whether the hypertable catalog is asked at all is
// decided inside the reader from the extension list, and the exported read
// returns the same description either way -- for a PostgreSQL server that has
// no TimescaleDB, and for one whose catalog answered nothing.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/internal/dbschema/dbtest"
)

// TestReadHypertables_CarriesThePrimaryDimension pins what the read takes from
// the catalog and where it puts it: the settings of the table it partitions.
//
// The primary dimension is the whole reason this read exists rather than a
// bare list of names: a declaration has to say which column the table is
// partitioned on. Measured on TimescaleDB 2.29.2 / PostgreSQL 17.11,
// timescaledb_information.hypertables answers exactly these columns for
// `create_hypertable('conditions', by_range('time'))`.
func TestReadHypertables_CarriesThePrimaryDimension(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringHypertables)
	reader := NewPostgreSQLReader(db.SQL, "public")
	schema := &catalog.Database{Tables: []catalog.Table{{Name: "conditions"}, {Name: "plain"}}}

	hypertables, err := reader.readHypertables(t.Context(), timescaleInstalled())
	c.Assert(err, qt.IsNil)
	c.Assert(reader.attachTimescale(schema, hypertables, nil), qt.IsNil)

	c.Assert(observedHypertable(c, schema.Tables[0]), qt.DeepEquals, &tsschema.ObservedHypertable{
		Column: "time", ColumnType: "timestamp with time zone", ChunkInterval: "7 days", Dimensions: 1,
	})
	c.Assert(observedHypertable(c, schema.Tables[1]), qt.IsNil)
	c.Assert(schema.FeatureCoverage.Lookup(tsschema.HypertableKind, tableRef("plain")).State, qt.Equals, schemaext.Complete)
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

	hypertables, err := reader.readHypertables(t.Context(), []catalog.Extension{{Name: "plpgsql"}})

	c.Assert(err, qt.IsNil)
	c.Assert(hypertables, qt.HasLen, 0)
	c.Assert(asked, qt.HasLen, 0)
}

// TestReadHypertables_AFailureWithTheExtensionIsSurfaced is the control the
// gate needs.
//
// Once the extension IS installed the catalog is there, so a failure means
// something else, and an empty answer would claim that no table on this server
// is partitioned -- a claim a comparison would act on by declaring every
// hypertable an ordinary table.
func TestReadHypertables_AFailureWithTheExtensionIsSurfaced(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, faultingHypertables)
	reader := NewPostgreSQLReader(db.SQL, "public")

	_, err := reader.readHypertables(t.Context(), timescaleInstalled())

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "permission denied")
}

// TestReadHypertables_TakesAHypertableWithNoDimensionReported keeps a shape the
// view declares nullable from failing the whole read.
//
// column_name and column_type are nullable through the LEFT JOIN. A hypertable
// always has a dimension today, and scanning into a plain string would turn
// the day that stops being true into a failed description. A declaration needs
// the column, so the table carries no settings and its partitioning is
// recorded as something the read could not represent: a comparison then
// decides nothing about it, rather than planning it into an ordinary table.
func TestReadHypertables_TakesAHypertableWithNoDimensionReported(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, dimensionlessHypertable)
	reader := NewPostgreSQLReader(db.SQL, "public")
	schema := &catalog.Database{Tables: []catalog.Table{{Name: "conditions"}}}

	hypertables, err := reader.readHypertables(t.Context(), timescaleInstalled())
	c.Assert(err, qt.IsNil)
	c.Assert(reader.attachTimescale(schema, hypertables, nil), qt.IsNil)

	c.Assert(observedHypertable(c, schema.Tables[0]), qt.IsNil)
	c.Assert(schema.FeatureCoverage.Lookup(tsschema.HypertableKind, tableRef("conditions")).State, qt.Equals, schemaext.Unrepresentable)
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
		{name: "the dimension catalog", fragment: "timescaledb_information.dimensions"},
		{name: "the primary dimension", fragment: "d.column_name"},
		{name: "the chunk interval", fragment: "d.time_interval::text"},
		{name: "its type", fragment: "d.column_type::text"},
		{name: "the first dimension only", fragment: "d.dimension_number = 1"},
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

// TestHypertableQuery_NamesNoColumnTheOlderExtensionLacks is the version gate
// this read has instead of a version check.
//
// `primary_dimension` and `primary_dimension_type` were added to the hypertable
// view after 2.14.2. Measured through `information_schema.columns` in the same
// database on each release, the view answers seven columns on 2.14.2 and nine
// on 2.29.2, so naming either of the two fails on the older one with
// `column h.primary_dimension does not exist`.
//
// That is worse than a missing detail. A failed statement aborts the enclosing
// PostgreSQL transaction, so every read AFTER this one answers SQLSTATE 25P02
// and the description fails somewhere else entirely.
//
// CI runs one TimescaleDB version, so nothing else in this suite can see the
// older shape. This is a claim about the query's text for exactly that reason.
func TestHypertableQuery_NamesNoColumnTheOlderExtensionLacks(t *testing.T) {
	tests := []struct {
		name   string
		column string
	}{
		{name: "the primary dimension", column: "primary_dimension"},
		{name: "its type", column: "primary_dimension_type"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(hypertableQuery, qt.Not(qt.Contains), test.column)
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
	return hypertableAnswer(query, nil, [][]driver.Value{{"public", "conditions", nil, nil, nil, int64(1)}})
}

func hypertableRows() [][]driver.Value {
	return [][]driver.Value{{"public", "conditions", "time", "timestamp with time zone", "7 days", int64(1)}}
}

// hypertableAnswer answers the catalog with the six columns the read scans, so
// a query that stopped selecting one fails here rather than being handed the
// same rows.
//
// Both views are required in the text. The primary dimension comes from
// `timescaledb_information.dimensions` because the hypertable view does not
// carry it on every supported release, and a query that went back to the newer
// projection would otherwise be answered these rows anyway.
func hypertableAnswer(query string, refusal error, rows [][]driver.Value) (dbtest.QueryResult, error) {
	for _, view := range []string{
		"timescaledb_information.hypertables",
		"timescaledb_information.dimensions",
	} {
		if !strings.Contains(query, view) {
			return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
		}
	}
	if refusal != nil {
		return dbtest.QueryResult{}, refusal
	}
	return dbtest.QueryResult{
		Columns: []string{
			"hypertable_schema", "hypertable_name",
			"column_name", "column_type", "time_interval", "num_dimensions",
		},
		Rows: rows,
	}, nil
}

// observedHypertable answers the settings a table carries, or nil.
func observedHypertable(c *qt.C, table catalog.Table) *tsschema.ObservedHypertable {
	c.Helper()
	value, _, err := schemaext.FacetAs[*tsschema.ObservedHypertable](table.Facets, tsschema.HypertableKind)
	c.Assert(err, qt.IsNil)
	return value
}

// tableRef names a table of the read's own schema the way the read does.
func tableRef(name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect(platform.Postgres)).TableParts("", name)
}
