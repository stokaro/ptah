package clickhouse_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/clickhouse"
	"ptah.run/internal/dbschema/dbtest"
)

// A database read must give views the same local scope as tables. An alias
// named like that database remains an alias while its source becomes local.
func TestReaderReadSchema_ViewScope(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "local source", body: "SELECT id FROM analytics.users", want: "SELECT id FROM users"},
		{name: "same-name alias", body: "SELECT analytics.id FROM analytics.users AS analytics", want: "SELECT analytics.id FROM users AS analytics"},
		{name: "quoted alias", body: "SELECT `analytics`.`id` FROM `analytics`.`users` `analytics`", want: "SELECT `analytics`.`id` FROM `users` `analytics`"},
		{name: "double quotes", body: `SELECT "analytics"."id" FROM "analytics"."users" AS "analytics"`, want: `SELECT "analytics"."id" FROM "users" AS "analytics"`},
		{name: "foreign source", body: "SELECT id FROM other.users", want: "SELECT id FROM other.users"},
		{name: "mixed sources", body: "SELECT a.id FROM analytics.users a JOIN other.users b ON a.id = b.id", want: "SELECT a.id FROM users a JOIN other.users b ON a.id = b.id"},
		{name: "literal and comment", body: "SELECT 'analytics.users' AS label FROM analytics /* analytics.users */ . users", want: "SELECT 'analytics.users' AS label FROM  /* analytics.users */  users"},
		{name: "nested alias", body: "SELECT analytics.id FROM (SELECT id FROM analytics.users) analytics", want: "SELECT analytics.id FROM (SELECT id FROM users) analytics"},
		{name: "tuple column", body: "SELECT analytics.id FROM analytics.users", want: "SELECT analytics.id FROM users"},
		{name: "update column", body: "SELECT update, analytics.id FROM analytics.users AS analytics", want: "SELECT update, analytics.id FROM users AS analytics"},
		{name: "prewhere array", body: "SELECT id FROM analytics.users PREWHERE [1, analytics.id] = [1, 7]", want: "SELECT id FROM users PREWHERE [1, analytics.id] = [1, 7]"},
		{name: "qualify array", body: "SELECT id FROM analytics.users QUALIFY [1, analytics.id] = [1, 7]", want: "SELECT id FROM users QUALIFY [1, analytics.id] = [1, 7]"},
		{name: "array join", body: "SELECT analytics.id FROM analytics.users AS analytics ARRAY JOIN analytics.items AS item", want: "SELECT analytics.id FROM users AS analytics ARRAY JOIN analytics.items AS item"},
		{name: "tuple under distinct alias", body: "SELECT a.users.id FROM analytics.users AS a", want: "SELECT a.users.id FROM users AS a"},
		{name: "extract expression", body: "SELECT EXTRACT(DAY FROM analytics.ts) FROM analytics.users AS analytics", want: "SELECT EXTRACT(DAY FROM analytics.ts) FROM users AS analytics"},
		{name: "trim expression", body: "SELECT TRIM(LEADING 'x' FROM analytics.name) FROM analytics.users AS analytics", want: "SELECT TRIM(LEADING 'x' FROM analytics.name) FROM users AS analytics"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, scopedViewQuery(test.body, test.body))

			state, err := clickhouse.NewClickHouseReader(db.SQL, "analytics").ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(state.Views, qt.HasLen, 1)
			c.Check(state.Views[0].Schema, qt.Equals, "")
			c.Check(state.Views[0].Body, qt.Equals, test.want)
			c.Assert(state.MatViews, qt.HasLen, 1)
			c.Check(state.MatViews[0].Schema, qt.Equals, "")
			c.Check(state.MatViews[0].Body, qt.Equals, test.want)
		})
	}
}

func scopedViewQuery(plain, materialized string) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		result, err := clickHouseViewReaderQuery(query, args)
		if err == nil && strings.Contains(query, "engine = 'View'") {
			result.Rows[0][1] = plain
		}
		if err == nil && strings.Contains(query, "engine = 'MaterializedView'") && !strings.Contains(query, "engine LIKE '%MergeTree'") {
			result.Rows[0][1] = materialized
		}
		return result, err
	}
}

// A database-qualified column can turn into a tuple field after removing the
// database name. The read refuses that ambiguity rather than emitting a query
// with different results or one that still reads the source database.
func TestReaderReadSchema_AmbiguousViewScope(t *testing.T) {
	for _, test := range []struct {
		name, plain, materialized, object string
	}{
		{name: "qualified column", plain: "SELECT analytics.users.id FROM analytics.users", materialized: "SELECT 1", object: `view "active_users"`},
		{name: "materialized column", plain: "SELECT 1", materialized: "SELECT analytics.users.id FROM analytics.users", object: `materialized view "user_counts"`},
		{name: "scalar alias", plain: "WITH tuple(tuple(7)) AS analytics SELECT analytics.users.id FROM analytics.users", materialized: "SELECT 1", object: `view "active_users"`},
		{name: "nested alias only", plain: "SELECT analytics.users.id FROM analytics.users WHERE EXISTS (SELECT 1 FROM analytics.other AS analytics WHERE analytics.id > 0)", materialized: "SELECT 1", object: `view "active_users"`},
		{name: "case-distinct alias", plain: "SELECT analytics.users.id FROM analytics.users AS ANALYTICS", materialized: "SELECT 1", object: `view "active_users"`},
		{name: "implicit table name", plain: "SELECT analytics.analytics.id FROM analytics.analytics", materialized: "SELECT 1", object: `view "active_users"`},
		{name: "same-name alias", plain: "SELECT analytics.users.id FROM analytics.users AS analytics", materialized: "SELECT 1", object: `view "active_users"`},
		{name: "same-name alias tuple", plain: "SELECT analytics.payload.id FROM analytics.users AS analytics", materialized: "SELECT 1", object: `view "active_users"`},
		{name: "prewhere array", plain: "SELECT id FROM analytics.users PREWHERE [1, analytics.users.id] = [1, 1]", materialized: "SELECT 1", object: `view "active_users"`},
		{name: "qualify array", plain: "SELECT id FROM analytics.users QUALIFY [1, analytics.users.id] = [1, 1]", materialized: "SELECT 1", object: `view "active_users"`},
		{name: "join column", plain: "SELECT join, analytics.users.id FROM analytics.users", materialized: "SELECT 1", object: `view "active_users"`},
		{name: "call column", plain: "SELECT call, analytics.users.id FROM analytics.users", materialized: "SELECT 1", object: `view "active_users"`},
		{name: "table column", plain: "SELECT table, analytics.users.id FROM analytics.users", materialized: "SELECT 1", object: `view "active_users"`},
		{name: "into column", plain: "SELECT into, analytics.users.id FROM analytics.users", materialized: "SELECT 1", object: `view "active_users"`},
		{name: "update column", plain: "SELECT update, analytics.users.id FROM analytics.users", materialized: "SELECT 1", object: `view "active_users"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, scopedViewQuery(test.plain, test.materialized))

			state, err := clickhouse.NewClickHouseReader(db.SQL, "analytics").ReadSchemaContext(t.Context())

			c.Assert(err, qt.ErrorMatches, `.*cannot make database-qualified expression.*use an explicit table alias different from the database name.*`)
			c.Assert(err.Error(), qt.Contains, test.object)
			c.Assert(state, qt.IsNil)
		})
	}
}

func TestReaderReadSchema_QuotedDatabaseScope(t *testing.T) {
	for _, test := range []struct {
		name, database, body, want string
	}{
		{name: "escaped delimiter", database: "an`alytics", body: "SELECT id FROM `an\\`alytics`.users", want: "SELECT id FROM users"},
		{name: "escaped backslash", database: `an\alytics`, body: "SELECT id FROM `an\\\\alytics`.users", want: "SELECT id FROM users"},
		{name: "escaped alias", database: "an`alytics", body: "SELECT `an\\`alytics`.id FROM `an\\`alytics`.users AS `an\\`alytics`", want: "SELECT `an\\`alytics`.id FROM users AS `an\\`alytics`"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, scopedViewQuery(test.body, test.body))

			state, err := clickhouse.NewClickHouseReader(db.SQL, test.database).ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(state.Views[0].Schema, qt.Equals, "")
			c.Assert(state.Views[0].Body, qt.Equals, test.want)
			c.Assert(state.MatViews[0].Schema, qt.Equals, "")
			c.Assert(state.MatViews[0].Body, qt.Equals, test.want)
		})
	}
}
