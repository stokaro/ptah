package postgres

// White-box testing required: which projection the reader chooses is visible
// only in the query text and only per dialect, and the entry point that carries
// the dialect that far is readTablesForSchema. Driving this through ReadSchema
// would need a fake answering every unrelated read the schema walk performs --
// columns, indexes, constraints, enums, routines -- which tests those rather
// than this projection.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
)

// tableReadCapture answers a table read and records the query it was asked.
//
// The query text is the assertion target rather than the returned value,
// because this fake answers by column name: it would hand back the same
// `unlogged` column whether the reader projected pg_class.relpersistence or the
// constant, so a test that only read the result could not tell the two apart.
func tableReadCapture(query *string) func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
	return func(sql string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case strings.Contains(sql, "FROM information_schema.tables"):
			*query = sql
			return dbtest.QueryResult{
				Columns: []string{
					"table_schema", "table_name", "table_type", "table_comment",
					"estimated_rows", "row_stats_unknown", "partitioned",
					"rls_enabled", "rls_forced", "unlogged",
					"row_ttl_options", "row_deletion_policy",
				},
				Rows: [][]driver.Value{
					{"public", "cache", "BASE TABLE", "", int64(0), false, false, false, false, true, "[]", ""},
				},
			}, nil
		case strings.Contains(sql, "information_schema.columns"):
			return dbtest.QueryResult{}, nil
		case strings.Contains(sql, "SELECT"):
			return dbtest.QueryResult{Columns: []string{"ok"}, Rows: [][]driver.Value{{true}}}, nil
		default:
			return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", sql)
		}
	}
}

// TestReadTablesForSchema_AsksPostgresForRelationPersistence pins that the reader looks
// for an unlogged table where one can exist.
//
// Without the projection the renderer's half of the round trip stands alone: a
// declared unlogged table applies, reads back as logged, and the comparator has
// a difference to propose on every run.
func TestReadTablesForSchema_AsksPostgresForRelationPersistence(t *testing.T) {
	for _, dialect := range []string{"postgres", "yugabytedb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			var query string
			db := dbtest.Open(t, tableReadCapture(&query))
			reader := NewPostgreSQLWireReaderWithCapabilities(
				db.SQL, "public", dialect, capability.Postgres16())

			tables, err := reader.readTablesForSchema(t.Context(), "public")

			c.Assert(err, qt.IsNil)
			c.Assert(query, qt.Contains, "c.relpersistence = 'u'")
			c.Assert(tables, qt.HasLen, 1)
			c.Assert(tables[0].Unlogged, qt.IsTrue)
		})
	}
}

// TestReadTablesForSchema_DoesNotAskForRelationPersistenceWhereNoTableCanBeUnlogged is
// the control, and the reason the projection is conditional at all.
//
// CockroachDB and Spanner reach this same reader over the PostgreSQL wire
// protocol, and neither creates an unlogged table. A column one of those
// catalogs does not carry fails the whole table read rather than the single
// projection, so the reader asks only where the answer could be true.
func TestReadTablesForSchema_DoesNotAskForRelationPersistenceWhereNoTableCanBeUnlogged(t *testing.T) {
	for _, dialect := range []string{"cockroachdb", "spanner"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			var query string
			db := dbtest.Open(t, tableReadCapture(&query))
			reader := NewPostgreSQLWireReaderWithCapabilities(
				db.SQL, "public", dialect, capability.Postgres16())

			_, err := reader.readTablesForSchema(t.Context(), "public")

			c.Assert(err, qt.IsNil)
			c.Assert(query, qt.Not(qt.Contains), "relpersistence")
			c.Assert(query, qt.Contains, "false AS unlogged",
				qt.Commentf("the column has to stay in the result set so the scan target list does not move"))
		})
	}
}
