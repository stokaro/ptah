package postgres

// White-box testing required: the projection under test is a fragment of the
// SQL readTablesForSchema builds, and both answers produce the same
// []types.DBTable for the rows a fake server hands back. Only the text of the
// query the reader issued says which projection it chose, and
// readTablesForSchema is unexported.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// relationSizeProbeMarker is the fragment that identifies the reader's
// capability probe among the queries a fake server is asked.
const relationSizeProbeMarker = "p.proname = 'pg_relation_size'"

// rowStatsFakeServer answers the three queries readTablesForSchema issues.
// The probe is answered with hasRelationSize; the tables query is recorded in
// tablesQuery so a test can read the projection the reader chose.
func rowStatsFakeServer(
	hasRelationSize bool,
	tablesQuery *string,
) func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case strings.Contains(query, relationSizeProbeMarker):
			return dbtest.QueryResult{
				Columns: []string{"exists"},
				Rows:    [][]driver.Value{{hasRelationSize}},
			}, nil
		case strings.Contains(query, "FROM information_schema.columns"):
			return dbtest.QueryResult{Columns: []string{"table_name"}}, nil
		case strings.Contains(query, "FROM information_schema.tables"):
			*tablesQuery = query
			return dbtest.QueryResult{
				Columns: []string{
					"table_schema",
					"table_name",
					"table_type",
					"table_comment",
					"estimated_rows",
					"row_stats_unknown",
					"partitioned",
					"rls_enabled",
				},
				Rows: [][]driver.Value{
					{"public", "members", "BASE TABLE", "", int64(0), false, false, false},
				},
			}, nil
		default:
			return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
		}
	}
}

// rowStatsReadResult is everything one fake-server table read produced.
type rowStatsReadResult struct {
	tables      []types.DBTable
	tablesQuery string
	queries     int
	err         error
}

func readTablesThroughRowStatsFakeServer(t *testing.T, hasRelationSize bool) rowStatsReadResult {
	t.Helper()
	var out rowStatsReadResult
	db := dbtest.Open(t, rowStatsFakeServer(hasRelationSize, &out.tablesQuery))
	reader := NewPostgreSQLReader(db.SQL, "public")
	out.tables, out.err = reader.readTablesForSchema("public")
	out.queries = db.QueryCount()
	return out
}

// TestReadTablesForSchema_AsksWhetherTheServerHasRelationSize pins the probe
// that keeps the table read working on a PostgreSQL-family server without
// pg_relation_size.
//
// The storage half of row_stats_unknown cannot be written unconditionally.
// CockroachDB does not implement pg_relation_size -- CCL v26.2.5 answers
// pg_relation_size('t'::regclass) with "unknown function: pg_relation_size()"
// (SQLSTATE 42883) -- and because an unknown function is a planning error, the
// whole table read fails rather than one column. A runtime CASE cannot rescue
// that; the reader has to know before it writes the query, so it asks
// pg_catalog.pg_proc, which both servers answer.
//
// The two rows below are the two answers. Each asserts what the tables query
// must contain AND what it must not, because an implementation that always
// emits the conjunct passes the first row on its content assertion alone, and
// one that never emits it passes the second.
func TestReadTablesForSchema_AsksWhetherTheServerHasRelationSize(t *testing.T) {
	tests := []struct {
		name            string
		serverHasProbe  bool
		wantInTables    []string
		wantNotInTables []string
	}{
		{
			name:           "a server with pg_relation_size measures storage",
			serverHasProbe: true,
			wantInTables: []string{
				"pg_relation_size(c.oid)",
				"NOT COALESCE(c.reltuples >= 0",
			},
			wantNotInTables: nil,
		},
		{
			name:           "a server without pg_relation_size falls back to statistics",
			serverHasProbe: false,
			wantInTables: []string{
				"NOT COALESCE(c.reltuples >= 0",
			},
			wantNotInTables: []string{"pg_relation_size(c.oid)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got := readTablesThroughRowStatsFakeServer(t, tt.serverHasProbe)

			c.Assert(got.err, qt.IsNil)
			c.Assert(got.tables, qt.HasLen, 1)
			c.Assert(got.queries, qt.Equals, 3)
			for _, want := range tt.wantInTables {
				c.Assert(got.tablesQuery, qt.Contains, want)
			}
			for _, unwanted := range tt.wantNotInTables {
				c.Assert(got.tablesQuery, qt.Not(qt.Contains), unwanted)
			}
		})
	}
}

// probeCountingServer answers only the capability probe and counts how often it
// is asked.
func probeCountingServer(probes *int) func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case strings.Contains(query, relationSizeProbeMarker):
			*probes++
			return dbtest.QueryResult{
				Columns: []string{"exists"},
				Rows:    [][]driver.Value{{true}},
			}, nil
		default:
			return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
		}
	}
}

// TestSupportsRelationSize_AsksOncePerReader pins the caching, which is what
// keeps the probe from adding one round trip per schema read.
func TestSupportsRelationSize_AsksOncePerReader(t *testing.T) {
	c := qt.New(t)
	probes := 0
	db := dbtest.Open(t, probeCountingServer(&probes))
	reader := NewPostgreSQLReader(db.SQL, "public")

	first, firstErr := reader.supportsRelationSize()
	second, secondErr := reader.supportsRelationSize()

	c.Assert(firstErr, qt.IsNil)
	c.Assert(secondErr, qt.IsNil)
	c.Assert(first, qt.IsTrue)
	c.Assert(second, qt.IsTrue)
	c.Assert(probes, qt.Equals, 1)
}
