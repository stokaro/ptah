package clickhouse_test

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/clickhouse"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// The table read now names 'MaterializedView' too, inside the subquery that
// subtracts materialized-view storage tables, so the cases are ordered
// most-specific first: the table query is recognized by its engine allowlist
// before the materialized-view query is recognized by the engine equality that
// both statements contain.
func clickHouseViewReaderQuery(
	query string,
	_ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	switch {
	case strings.Contains(query, "FROM system.columns"):
		return dbtest.QueryResult{
			Columns: []string{"table", "name", "type", "default_kind", "default_expression", "position", "comment"},
		}, nil
	case strings.Contains(query, "name = 'data_skipping_indices'"):
		return dbtest.QueryResult{
			Columns: []string{"count()"},
			Rows:    [][]driver.Value{{uint64(0)}},
		}, nil
	case strings.Contains(query, "engine LIKE '%MergeTree'"):
		return dbtest.QueryResult{Columns: []string{"name", "comment"}}, nil
	case strings.Contains(query, "engine = 'View'"):
		return dbtest.QueryResult{
			Columns: []string{"name", "as_select", "comment"},
			Rows: [][]driver.Value{{
				"active_users",
				"SELECT id, name FROM analytics.users WHERE active = true",
				"Current active users",
			}},
		}, nil
	case strings.Contains(query, "engine = 'MaterializedView'"):
		return dbtest.QueryResult{
			Columns: []string{"name", "as_select", "comment"},
			Rows: [][]driver.Value{{
				"user_counts",
				"SELECT count() AS c FROM analytics.users",
				"Rolled up per user",
			}},
		}, nil
	default:
		return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
	}
}

func clickHouseViewReaderFailureQuery(
	query string,
	args []driver.NamedValue,
) (dbtest.QueryResult, error) {
	if strings.Contains(query, "engine = 'View'") {
		return dbtest.QueryResult{}, fmt.Errorf("catalog unavailable")
	}
	return clickHouseViewReaderQuery(query, args)
}

func clickHouseMaterializedViewReaderFailureQuery(
	query string,
	args []driver.NamedValue,
) (dbtest.QueryResult, error) {
	if strings.Contains(query, "engine = 'MaterializedView'") &&
		!strings.Contains(query, "engine LIKE '%MergeTree'") {
		return dbtest.QueryResult{}, fmt.Errorf("catalog unavailable")
	}
	return clickHouseViewReaderQuery(query, args)
}

func TestReaderReadSchema_LoadsPlainViews(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, clickHouseViewReaderQuery)
	reader := clickhouse.NewClickHouseReader(db.SQL, "analytics")

	schema, err := reader.ReadSchema()

	c.Assert(err, qt.IsNil)
	c.Assert(db.QueryCount(), qt.Equals, 5)
	c.Assert(schema.Views, qt.DeepEquals, []types.DBView{{
		Name:        "active_users",
		Schema:      "analytics",
		Body:        "SELECT id, name FROM analytics.users WHERE active = true",
		CheckOption: "NONE",
		Comment:     "Current active users",
	}})
}

// TestReaderReadSchema_LoadsMaterializedViews pins that a materialized view
// arrives as a materialized view and not as a plain view: the two reads differ
// only by the engine they select on, so a reader that answered the wrong query
// would still return the same name and body under the wrong key.
func TestReaderReadSchema_LoadsMaterializedViews(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, clickHouseViewReaderQuery)
	reader := clickhouse.NewClickHouseReader(db.SQL, "analytics")

	schema, err := reader.ReadSchema()

	c.Assert(err, qt.IsNil)
	c.Assert(schema.MatViews, qt.DeepEquals, []types.DBMatView{{
		Name:            "user_counts",
		Schema:          "analytics",
		Body:            "SELECT count() AS c FROM analytics.users",
		RefreshStrategy: "manual",
		Comment:         "Rolled up per user",
	}})
	c.Assert(schema.Views, qt.HasLen, 1)
	c.Assert(schema.Views[0].Name, qt.Equals, "active_users")
}

func TestReaderReadSchema_MaterializedViewCatalogFailurePath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, clickHouseMaterializedViewReaderFailureQuery)
	reader := clickhouse.NewClickHouseReader(db.SQL, "analytics")

	schema, err := reader.ReadSchema()

	c.Assert(err, qt.ErrorMatches, `clickhouse: read materialized views: catalog unavailable`)
	c.Assert(schema, qt.IsNil)
}

func TestReaderReadSchema_ViewCatalogFailurePath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, clickHouseViewReaderFailureQuery)
	reader := clickhouse.NewClickHouseReader(db.SQL, "analytics")

	schema, err := reader.ReadSchema()

	c.Assert(err, qt.ErrorMatches, `clickhouse: read views: catalog unavailable`)
	c.Assert(schema, qt.IsNil)
}
