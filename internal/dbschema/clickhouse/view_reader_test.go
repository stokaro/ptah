package clickhouse_test

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/dbschema/clickhouse"
	"ptah.run/internal/dbschema/dbtest"
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
		return dbtest.QueryResult{Columns: []string{
			"name", "comment", "sorting_key", "primary_key",
			"engine_full", "partition_key", "sampling_key",
		}}, nil
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

	schema, err := reader.ReadSchemaContext(t.Context())

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
	c.Assert(schema.Views, qt.DeepEquals, []catalog.View{{
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

	schema, err := reader.ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(schema.MatViews, qt.DeepEquals, []catalog.MaterializedView{{
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

	schema, err := reader.ReadSchemaContext(t.Context())

	c.Assert(err, qt.ErrorMatches, `clickhouse: read materialized views: catalog unavailable`)
	c.Assert(schema, qt.IsNil)
}

func TestReaderReadSchema_ViewCatalogFailurePath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, clickHouseViewReaderFailureQuery)
	reader := clickhouse.NewClickHouseReader(db.SQL, "analytics")

	schema, err := reader.ReadSchemaContext(t.Context())

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
	// The table read must be matched BEFORE the materialized-view arm. Its
	// query excludes a materialized view's inner storage by name, and that
	// subquery contains `engine = 'MaterializedView'` -- so a substring match on
	// that alone answered the TABLE read with a view's columns. It went
	// unnoticed because the two column counts happened to agree; they stopped
	// agreeing the moment the table read asked for one more (stokaro/ptah#2198).
	case strings.Contains(query, "engine LIKE '%MergeTree'"):
		return clickHouseViewReaderQuery(query, args)
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
// the schedule, as the ClickHouse owner's observation, for the ones it names.
// Both views have a known schedule, the second a known absence.
func TestReaderReadSchema_ReadsAScheduleOnlyForAViewTheServerSchedules(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, clickHouseRefreshableViewReaderQuery)
	reader := clickhouse.NewClickHouseReader(db.SQL, "analytics")

	schema, err := reader.ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(schema.MatViews, qt.HasLen, 2)
	byName := make(map[string]catalog.MaterializedView, len(schema.MatViews))
	for _, view := range schema.MatViews {
		byName[view.Name] = view
	}
	schedule, found, err := schemaext.FacetAs[*chschema.ObservedRefresh](byName["scheduled"].Facets, chschema.RefreshKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(schedule.Clause(), qt.Equals, "EVERY 1 HOUR")
	c.Assert(byName["scheduled"].Facets.TargetScope(chschema.RefreshKind), qt.DeepEquals, []string{"clickhouse"})
	// Listed by no catalog, so it has no schedule whatever its statement says.
	c.Assert(byName["unlisted"].Facets.IsZero(), qt.IsTrue)
	for _, name := range []string{"scheduled", "unlisted"} {
		c.Assert(schema.FeatureCoverage.Lookup(chschema.RefreshKind, matViewSubject(name)).State, qt.Equals, schemaext.Complete)
	}
}

func matViewSubject(name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).SchemaScopedParts(objectidentity.KindMatView, "analytics", name)
}

// unreadableRefreshQuery lists a refreshable view whose stored statement has a
// REFRESH clause this reader cannot read.
func unreadableRefreshQuery(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	switch {
	case strings.Contains(query, "engine LIKE '%MergeTree'"):
		return clickHouseViewReaderQuery(query, args)
	case strings.Contains(query, "FROM system.view_refreshes"):
		return dbtest.QueryResult{Columns: []string{"view"}, Rows: [][]driver.Value{{"scheduled"}}}, nil
	case strings.Contains(query, "engine = 'MaterializedView'"):
		return dbtest.QueryResult{
			Columns: []string{"name", "as_select", "comment", "create_table_query"},
			Rows: [][]driver.Value{{"scheduled", "SELECT 1", "",
				"CREATE MATERIALIZED VIEW analytics.scheduled REFRESH EVERY FORTNIGHT (`c` UInt64) ENGINE = MergeTree AS SELECT 1"}},
		}, nil
	default:
		return clickHouseViewReaderQuery(query, args)
	}
}

// A refreshable view whose schedule this reader cannot read keeps no
// observation, and its schedule is reported unknown rather than absent: read
// as absent, it would plan a replacement that drops the view's rows.
func TestReaderReadSchema_AnUnreadableScheduleIsUnknown(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, unreadableRefreshQuery)

	schema, err := clickhouse.NewClickHouseReader(db.SQL, "analytics").ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(schema.MatViews, qt.HasLen, 1)
	c.Assert(schema.MatViews[0].Facets.IsZero(), qt.IsTrue)
	knowledge := schema.FeatureCoverage.Lookup(chschema.RefreshKind, matViewSubject("scheduled"))
	c.Assert(knowledge.State, qt.Equals, schemaext.Unrepresentable)
	c.Assert(knowledge.Reason, qt.Contains, "could not be read")
}

// errViewRefreshes is a failed read of system.view_refreshes.
var errViewRefreshes = errors.New("code: 60, unknown table")

// refreshTableQuery fails the read of system.view_refreshes and answers the
// existence probe that follows with present tables.
func refreshTableQuery(present uint64) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case strings.Contains(query, "FROM system.view_refreshes"):
			return dbtest.QueryResult{}, errViewRefreshes
		case strings.Contains(query, "name = 'view_refreshes'"):
			return dbtest.QueryResult{Columns: []string{"count()"}, Rows: [][]driver.Value{{present}}}, nil
		default:
			return clickHouseViewReaderQuery(query, args)
		}
	}
}

// A server without system.view_refreshes predates refreshable views, so its
// views are plain and their schedules known to be absent.
func TestReaderReadSchema_AServerWithoutRefreshableViewsHasPlainViews(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, refreshTableQuery(0))

	schema, err := clickhouse.NewClickHouseReader(db.SQL, "analytics").ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(schema.MatViews, qt.HasLen, 1)
	c.Assert(schema.MatViews[0].Facets.IsZero(), qt.IsTrue)
	c.Assert(schema.FeatureCoverage.Lookup(chschema.RefreshKind, matViewSubject("user_counts")).State, qt.Equals, schemaext.Complete)
}

// Any other failure to read system.view_refreshes fails the read: answered
// with an empty set, every schedule would read as absent and plan replacements
// that drop the views' rows.
func TestReaderReadSchema_AFailedRefreshCatalogRead_FailurePath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, refreshTableQuery(1))

	schema, err := clickhouse.NewClickHouseReader(db.SQL, "analytics").ReadSchemaContext(t.Context())

	c.Assert(err, qt.ErrorIs, errViewRefreshes)
	c.Assert(schema, qt.IsNil)
}
