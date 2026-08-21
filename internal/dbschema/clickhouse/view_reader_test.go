package clickhouse_test

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/chrefresh"
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
	// The materialized-view read also selects create_table_query, because the
	// refresh schedule of a refreshable view survives nowhere else
	// (stokaro/ptah#1802). This row is a PLAIN view, so the statement carries
	// no REFRESH clause and the read must report no schedule.
	case strings.Contains(query, "engine = 'MaterializedView'"):
		return dbtest.QueryResult{
			Columns: []string{"name", "as_select", "comment", "create_table_query"},
			Rows: [][]driver.Value{{
				"user_counts",
				"SELECT count() AS c FROM analytics.users",
				"Rolled up per user",
				"CREATE MATERIALIZED VIEW analytics.user_counts (`c` UInt64) " +
					"ENGINE = MergeTree ORDER BY tuple() AS SELECT count() AS c FROM analytics.users",
			}},
		}, nil
	// system.view_refreshes lists the refreshable views and only those. An
	// empty answer is what a database of plain views gives.
	case strings.Contains(query, "FROM system.view_refreshes"):
		return dbtest.QueryResult{Columns: []string{"view"}}, nil
	// The reader also describes roles and grants now that ClickHouse carries
	// capability.RoleManagement. These tests are about views, so the catalogs
	// answer empty rather than being left to the unexpected-query arm — which
	// would report a view failure for an RBAC statement (stokaro/ptah#1025).
	case strings.Contains(query, "FROM system.roles"):
		return dbtest.QueryResult{Columns: []string{"name", "storage"}}, nil
	case strings.Contains(query, "FROM system.grants"):
		return dbtest.QueryResult{Columns: []string{
			"grantee", "privilege", "database_name", "table_name", "is_partial_revoke", "grant_option",
		}}, nil
	case strings.Contains(query, "FROM system.row_policies"):
		return dbtest.QueryResult{Columns: []string{
			"short_name", "table", "select_filter", "apply_to_all", "apply_to_list", "apply_to_except",
		}}, nil
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
	// Nine: the five catalog reads this test has always made, the two RBAC
	// reads — system.roles and system.grants — that a ClickHouse reader makes
	// because the dialect carries capability.RoleManagement,
	// system.row_policies, which it now makes for capability.RowLevelSecurity
	// (stokaro/ptah#1736), and system.view_refreshes, which tells a refreshable
	// materialized view from a plain one (stokaro/ptah#1802). The count is
	// asserted rather than ignored because it is what would catch the reader
	// issuing one statement per role, per policy — or per view
	// (stokaro/ptah#1025).
	c.Assert(db.QueryCount(), qt.Equals, 9)
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
		Name:    "user_counts",
		Schema:  "analytics",
		Body:    "SELECT count() AS c FROM analytics.users",
		Comment: "Rolled up per user",
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

// clickHouseRefreshableViewReaderQuery answers with one refreshable
// materialized view and one whose statement carries a REFRESH clause while the
// server does not list it as refreshable.
//
// The second row is the one worth having. A statement is not the authority on
// whether a view is scheduled -- system.view_refreshes is -- and a reader that
// trusted the text alone would report a schedule for a view that has none, then
// plan a change to an object that is already right (stokaro/ptah#1802).
func clickHouseRefreshableViewReaderQuery(
	query string,
	args []driver.NamedValue,
) (dbtest.QueryResult, error) {
	switch {
	case strings.Contains(query, "FROM system.view_refreshes"):
		return dbtest.QueryResult{
			Columns: []string{"view"},
			Rows:    [][]driver.Value{{"scheduled"}},
		}, nil
	case strings.Contains(query, "engine = 'MaterializedView'"):
		return dbtest.QueryResult{
			Columns: []string{"name", "as_select", "comment", "create_table_query"},
			Rows: [][]driver.Value{
				{
					"scheduled",
					"SELECT count() AS c FROM analytics.users",
					"",
					"CREATE MATERIALIZED VIEW analytics.scheduled REFRESH EVERY 1 HOUR " +
						"(`c` UInt64) ENGINE = MergeTree AS SELECT count() AS c FROM analytics.users",
				},
				{
					"unlisted",
					"SELECT count() AS c FROM analytics.users",
					"",
					"CREATE MATERIALIZED VIEW analytics.unlisted REFRESH EVERY 2 HOUR " +
						"(`c` UInt64) ENGINE = MergeTree AS SELECT count() AS c FROM analytics.users",
				},
			},
		}, nil
	default:
		return clickHouseViewReaderQuery(query, args)
	}
}

// TestReaderReadSchema_ReadsAScheduleOnlyForAViewTheServerSchedules pins the
// gate: the catalog of refreshable views decides, and the statement supplies
// the schedule for the ones it names.
func TestReaderReadSchema_ReadsAScheduleOnlyForAViewTheServerSchedules(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, clickHouseRefreshableViewReaderQuery)
	reader := clickhouse.NewClickHouseReader(db.SQL, "analytics")

	schema, err := reader.ReadSchema()

	c.Assert(err, qt.IsNil)
	c.Assert(schema.MatViews, qt.HasLen, 2)
	byName := make(map[string]types.DBMatView, len(schema.MatViews))
	for _, view := range schema.MatViews {
		byName[view.Name] = view
	}
	c.Assert(byName["scheduled"].Refresh, qt.IsNotNil)
	c.Assert(chrefresh.Clause(byName["scheduled"].Refresh), qt.Equals, "EVERY 1 HOUR")
	// Listed by no catalog, so it has no schedule whatever its statement says.
	c.Assert(byName["unlisted"].Refresh, qt.IsNil)
}
