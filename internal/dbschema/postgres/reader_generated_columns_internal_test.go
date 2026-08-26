package postgres

// White-box testing required: readColumnsForSchema is unexported, and the fact
// under test is which catalog columns its SELECT names. A server that does not
// have pg_attribute.attgenerated refuses the whole query, so there is no result
// downstream to assert on -- the read fails before a single table is returned.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// generatedColumnRow is the one row the fake servers below answer with: a
// column declared GENERATED ALWAYS AS (n + 1) STORED, as PostgreSQL 18.4
// reports it.
var generatedColumnRow = []driver.Value{
	"t", "g", "integer", "int4", "",
	"", "", "YES", nil, nil,
	nil, nil, int64(1),
	"s", "(n + 1)", "",
	"", "", "",
}

// columnQueryColumns is the result shape readColumnsForSchema scans, in order.
var columnQueryColumns = []string{
	"table_name", "column_name", "data_type", "udt_name", "formatted_type",
	"domain_name", "domain_schema", "is_nullable", "column_default", "character_maximum_length",
	"numeric_precision", "numeric_scale", "ordinal_position",
	"generated_kind", "generated_expression", "identity_kind",
	"column_comment", "not_null_constraint_name", "owned_sequence_name",
}

// servePostgres11 answers the column query the way a server whose engine is
// still PostgreSQL 11 does.
//
// Measured on `PostgreSQL 11.2-YB-2024.2.10.0-b0`: pg_attribute has no
// attgenerated -- the column arrived with the GENERATED ... STORED spelling in
// PostgreSQL 12 -- while attidentity, added in PostgreSQL 10, is present. So
// the refusal is specific to one column rather than to pg_attribute, and the
// fixture refuses exactly that one.
func servePostgres11(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if strings.Contains(stripSQLComments(query), "attgenerated") {
		return dbtest.QueryResult{}, fmt.Errorf(
			`ERROR: column a.attgenerated does not exist (SQLSTATE 42703)`)
	}
	// Past the refusal the two servers answer alike, and here that is not a
	// shortcut: an engine with no attgenerated has no generated column to
	// report, so the constants the gated projections select are also the truth.
	return servePostgres12(query, nil)
}

// projectionReadsAttgenerated reports whether the projection under alias reads
// pg_attribute.attgenerated rather than answering a constant.
//
// Per projection, not per query, and that is the whole point. A fixture that
// looked at the query as a whole answers the catalog value to a blanked
// generated_kind as long as generated_expression still names the column, and a
// mutant that blanks the kind everywhere then passes -- measured, it did.
func projectionReadsAttgenerated(query, alias string) bool {
	projection, ok := selectListItem(query, alias, "FROM information_schema.columns")
	if !ok {
		return false
	}
	return strings.Contains(projection, "attgenerated")
}

// servePostgres12 answers it the way every later PostgreSQL-family server does:
// attgenerated is there, so the query is accepted either way and what the
// reader gets back depends only on whether it asked.
func servePostgres12(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	// A projection that does not read the column cannot learn what it holds on
	// a real server either, so each one is answered from the catalog only when
	// it asks, and gets the constant its gated branch selects otherwise.
	row := append([]driver.Value(nil), generatedColumnRow...)
	if !projectionReadsAttgenerated(query, "generated_kind") {
		row[13] = ""
	}
	if !projectionReadsAttgenerated(query, "generated_expression") {
		row[14] = ""
	}
	return dbtest.QueryResult{Columns: columnQueryColumns, Rows: [][]driver.Value{row}}, nil
}

// generatedExpressionOf flattens the reader's optional expression so a row can
// state the absent case as a value rather than as a branch.
//
// Nothing is lost by dropping the pointer: readColumnsForSchema takes the
// address only when the value it read is non-empty, so a non-nil pointer to the
// empty string is a state it cannot produce.
func generatedExpressionOf(column types.DBColumn) string {
	if column.GeneratedExpression == nil {
		return ""
	}
	return *column.GeneratedExpression
}

// TestReadColumnsForSchema_AsksForAttgeneratedOnlyWhereItExists pins both
// directions of the gate.
//
// The YugabyteDB 2024 LTS line is the one target this reader serves whose
// preset says no generated columns, and the reason is the engine: 2024.2
// reports PostgreSQL 11 while 2025.1 and later report PostgreSQL 15. Before the
// gate the projection was unconditional, and the nightly capability matrix had
// recorded the consequence on ten consecutive nights -- every table read on
// that cell dying with `column a.attgenerated does not exist` before returning
// a table (stokaro/ptah#1901).
//
// The PostgreSQL 12 rows are the control. A gate that blanks the projection
// everywhere would pass the first row and lose the generated column on every
// other target, which is the mutant this table exists to catch.
func TestReadColumnsForSchema_AsksForAttgeneratedOnlyWhereItExists(t *testing.T) {
	tests := []struct {
		name  string
		serve func(string, []driver.NamedValue) (dbtest.QueryResult, error)
		caps  capability.Capabilities
		// expectedKind is the generated kind the reader carries forward, empty
		// when it asked for none.
		expectedKind string
		// expectedExpression is the generated expression beside it. The reader
		// only records a kind when it has an expression, so the two move
		// together.
		expectedExpression string
	}{
		{
			name:  "a PostgreSQL 11 engine is not asked for a PostgreSQL 12 column",
			serve: servePostgres11,
			caps:  capability.YugabyteDB24(),
		},
		{
			name:               "a target with generated columns still reads them",
			serve:              servePostgres12,
			caps:               capability.Postgres16(),
			expectedKind:       "STORED",
			expectedExpression: "(n + 1)",
		},
		{
			name:  "the key rather than the server decides",
			serve: servePostgres12,
			caps:  capability.Postgres16().With(capability.GeneratedColumns, false),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, test.serve)

			columnsByTable, err := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", test.caps).
				readColumnsForSchema(t.Context(), "public")
			c.Assert(err, qt.IsNil)
			c.Assert(columnsByTable["t"], qt.HasLen, 1)

			column := columnsByTable["t"][0]
			c.Assert(column.GeneratedKind, qt.Equals, test.expectedKind)
			c.Assert(generatedExpressionOf(column), qt.Equals, test.expectedExpression)
		})
	}
}

// TestReadColumnsForSchema_APostgres11EngineRefusesTheUngatedQuery is the
// fixture's own control.
//
// Without it the table above proves nothing about the first row: a fake server
// that accepted everything would pass it whether or not the gate exists. This
// asserts the refusal is real by asking the same server with a preset that
// declares generated columns, which is what the reader did on that cell before
// this change.
func TestReadColumnsForSchema_APostgres11EngineRefusesTheUngatedQuery(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, servePostgres11)

	_, err := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16()).
		readColumnsForSchema(t.Context(), "public")
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "column a.attgenerated does not exist")
}
