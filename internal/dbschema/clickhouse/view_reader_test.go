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

func clickHouseViewReaderQuery(
	query string,
	_ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	switch {
	case strings.Contains(query, "engine = 'View'"):
		return dbtest.QueryResult{
			Columns: []string{"name", "as_select", "comment"},
			Rows: [][]driver.Value{{
				"active_users",
				"SELECT id, name FROM analytics.users WHERE active = true",
				"Current active users",
			}},
		}, nil
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

func TestReaderReadSchema_LoadsPlainViews(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, clickHouseViewReaderQuery)
	reader := clickhouse.NewClickHouseReader(db.SQL, "analytics")

	schema, err := reader.ReadSchema()

	c.Assert(err, qt.IsNil)
	c.Assert(db.QueryCount(), qt.Equals, 4)
	c.Assert(schema.Views, qt.DeepEquals, []types.DBView{{
		Name:        "active_users",
		Schema:      "analytics",
		Body:        "SELECT id, name FROM analytics.users WHERE active = true",
		CheckOption: "NONE",
		Comment:     "Current active users",
	}})
}

func TestReaderReadSchema_ViewCatalogFailurePath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, clickHouseViewReaderFailureQuery)
	reader := clickhouse.NewClickHouseReader(db.SQL, "analytics")

	schema, err := reader.ReadSchema()

	c.Assert(err, qt.ErrorMatches, `clickhouse: read views: catalog unavailable`)
	c.Assert(schema, qt.IsNil)
}
