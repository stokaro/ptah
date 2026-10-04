package query_test

import (
	"database/sql"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/query"
)

// ydbRender is one statement rendered for one YDB release line.
type ydbRender struct {
	name   string
	caps   capability.Capabilities
	render func(caps capability.Capabilities) (string, []any, error)
}

func selectFor(builder *query.SelectBuilder) func(capability.Capabilities) (string, []any, error) {
	return func(caps capability.Capabilities) (string, []any, error) {
		return query.RenderSelectWithCapabilities(builder.Build(), platform.YDB, caps)
	}
}

func insertFor(builder *query.InsertBuilder) func(capability.Capabilities) (string, []any, error) {
	return func(caps capability.Capabilities) (string, []any, error) {
		return query.RenderInsertWithCapabilities(builder.Build(), platform.YDB, caps)
	}
}

func updateFor(builder *query.UpdateBuilder) func(capability.Capabilities) (string, []any, error) {
	return func(caps capability.Capabilities) (string, []any, error) {
		return query.RenderUpdateWithCapabilities(builder.Build(), platform.YDB, caps)
	}
}

func deleteFor(builder *query.DeleteBuilder) func(capability.Capabilities) (string, []any, error) {
	return func(caps capability.Capabilities) (string, []any, error) {
		return query.RenderDeleteWithCapabilities(builder.Build(), platform.YDB, caps)
	}
}

func usersOrdersJoin(join func(*query.SelectBuilder, string, string, query.Expression) *query.SelectBuilder) *query.SelectBuilder {
	return join(
		query.Select().Columns(query.Col("u", "name"), query.Col("o", "total")).FromAs("users", "u"),
		"orders", "o", query.Col("o", "user_id").EqCol(query.Col("u", "id")),
	)
}

// Every statement YDB runs renders with backticked names, YQL's named
// parameters, and arguments that carry the parameter names.
func TestRenderYDB_HappyPath(t *testing.T) {
	tests := []struct {
		ydbRender
		wantSQL  string
		wantArgs []any
	}{
		{
			ydbRender: ydbRender{name: "limit and offset bind as Uint64", caps: capability.YDB262(),
				render: selectFor(query.Select("id").From("users").Where(query.Gt("age", int64(30))).
					OrderBy(query.Asc("id")).Limit(10).Offset(20))},
			wantSQL: "SELECT `id` FROM `users` WHERE `age` > $p1 ORDER BY `id` ASC LIMIT $p2 OFFSET $p3",
			wantArgs: []any{sql.Named("p1", int64(30)), sql.Named("p2", uint64(10)),
				sql.Named("p3", uint64(20))},
		},
		{
			ydbRender: ydbRender{name: "a table path is one name", caps: capability.YDB262(),
				render: selectFor(query.Select("id").From("app/users"))},
			wantSQL: "SELECT `id` FROM `app/users`",
		},
		{
			ydbRender: ydbRender{name: "inner join on an equality", caps: capability.YDB262(),
				render: selectFor(usersOrdersJoin((*query.SelectBuilder).InnerJoin))},
			wantSQL: "SELECT `u`.`name`, `o`.`total` FROM `users` `u` INNER JOIN `orders` `o` ON `o`.`user_id` = `u`.`id`",
		},
		{
			ydbRender: ydbRender{name: "left join", caps: capability.YDB262(),
				render: selectFor(usersOrdersJoin((*query.SelectBuilder).LeftJoin))},
			wantSQL: "SELECT `u`.`name`, `o`.`total` FROM `users` `u` LEFT JOIN `orders` `o` ON `o`.`user_id` = `u`.`id`",
		},
		{
			ydbRender: ydbRender{name: "right join", caps: capability.YDB262(),
				render: selectFor(usersOrdersJoin((*query.SelectBuilder).RightJoin))},
			wantSQL: "SELECT `u`.`name`, `o`.`total` FROM `users` `u` RIGHT JOIN `orders` `o` ON `o`.`user_id` = `u`.`id`",
		},
		{
			ydbRender: ydbRender{name: "full join", caps: capability.YDB262(),
				render: selectFor(usersOrdersJoin((*query.SelectBuilder).FullJoin))},
			wantSQL: "SELECT `u`.`name`, `o`.`total` FROM `users` `u` FULL OUTER JOIN `orders` `o` ON `o`.`user_id` = `u`.`id`",
		},
		{
			ydbRender: ydbRender{name: "join on a conjunction of equalities", caps: capability.YDB262(),
				render: selectFor(query.Select().Columns(query.Col("u", "name")).FromAs("users", "u").
					InnerJoin("orders", "o", query.And(
						query.Col("o", "user_id").EqCol(query.Col("u", "id")),
						query.Col("o", "region").EqCol(query.Col("u", "region")),
					)))},
			wantSQL: "SELECT `u`.`name` FROM `users` `u` INNER JOIN `orders` `o` " +
				"ON (`o`.`user_id` = `u`.`id` AND `o`.`region` = `u`.`region`)",
		},
		{
			ydbRender: ydbRender{name: "a subquery that reads only its own table", caps: capability.YDB262(),
				render: selectFor(query.Select("id").From("users").Where(query.And(
					query.InQuery("id", query.Select("user_id").From("orders").Where(query.Eq("status", "paid"))),
					query.Exists(query.Select("id").FromAs("orders", "o").Where(query.Col("o", "total").Gt(int64(5)))),
				)))},
			wantSQL: "SELECT `id` FROM `users` WHERE (`id` IN (SELECT `user_id` FROM `orders` WHERE `status` = $p1) " +
				"AND EXISTS (SELECT `id` FROM `orders` `o` WHERE `o`.`total` > $p2))",
			wantArgs: []any{sql.Named("p1", "paid"), sql.Named("p2", int64(5))},
		},
		{
			ydbRender: ydbRender{name: "a WITH clause where the target has the key", render: selectFor(
				query.Select("id").With("c", query.Select("id").From("users")).From("c")),
				caps: capability.YDB262().With(capability.CommonTableExpressions, true)},
			wantSQL: "WITH `c` AS (SELECT `id` FROM `users`) SELECT `id` FROM `c`",
		},
		{
			ydbRender: ydbRender{name: "an OFFSET alone where the target has the key", render: selectFor(
				query.Select("id").From("users").Offset(5)),
				caps: capability.YDB262().With(capability.OffsetWithoutLimit, true)},
			wantSQL:  "SELECT `id` FROM `users` OFFSET $p1",
			wantArgs: []any{sql.Named("p1", uint64(5))},
		},
		{
			ydbRender: ydbRender{name: "an upsert", caps: capability.YDB262(),
				render: insertFor(query.UpsertInto("users").Columns("id", "name").
					Values(int64(1), "alice").Values(int64(2), "bob"))},
			wantSQL: "UPSERT INTO `users` (`id`, `name`) VALUES ($p1, $p2), ($p3, $p4)",
			wantArgs: []any{sql.Named("p1", int64(1)), sql.Named("p2", "alice"),
				sql.Named("p3", int64(2)), sql.Named("p4", "bob")},
		},
		{
			ydbRender: ydbRender{name: "an upsert from a query, returning", caps: capability.YDB262(),
				render: insertFor(query.UpsertInto("archive").Columns("id", "name").
					FromSelect(query.Select("id", "name").From("users")).Returning("id"))},
			wantSQL: "UPSERT INTO `archive` (`id`, `name`) SELECT `id`, `name` FROM `users` RETURNING `id`",
		},
		{
			ydbRender: ydbRender{name: "an update returning", caps: capability.YDB262(),
				render: updateFor(query.Update("users").Set("name", "bob").Where(query.Eq("id", int64(7))).
					Returning("id", "name"))},
			wantSQL:  "UPDATE `users` SET `name` = $p1 WHERE `id` = $p2 RETURNING `id`, `name`",
			wantArgs: []any{sql.Named("p1", "bob"), sql.Named("p2", int64(7))},
		},
		{
			ydbRender: ydbRender{name: "a delete returning on 25.3", caps: capability.YDB253(),
				render: deleteFor(query.DeleteFrom("users").Where(query.Eq("id", int64(7))).Returning("id"))},
			wantSQL:  "DELETE FROM `users` WHERE `id` = $p1 RETURNING `id`",
			wantArgs: []any{sql.Named("p1", int64(7))},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, args, err := test.render(test.caps)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.wantSQL)
			c.Assert(args, argsEqual, test.wantArgs)
		})
	}
}

// A construct the target lacks the key for is refused with a capability error
// naming the key, before any SQL exists.
func TestRenderYDB_FailurePath_RefusesWhatTheTargetLacks(t *testing.T) {
	tests := []struct {
		ydbRender
		wantErr string
	}{
		{
			ydbRender: ydbRender{name: "a WITH clause", caps: capability.YDB262(),
				render: selectFor(query.Select("id").With("c", query.Select("id").From("users")).From("c"))},
			wantErr: "renderer: a WITH clause, which requires target capability common_table_expressions, " +
				"unavailable on this ydb target",
		},
		{
			ydbRender: ydbRender{name: "a correlated EXISTS", caps: capability.YDB262(),
				render: selectFor(query.Select("id").FromAs("users", "u").Where(query.Exists(
					query.Select("id").FromAs("orders", "o").Where(query.Col("o", "user_id").EqCol(query.Col("u", "id"))))))},
			wantErr: `renderer: a subquery that reads "u" from the query around it, which requires target ` +
				"capability correlated_subqueries, unavailable on this ydb target",
		},
		{
			ydbRender: ydbRender{name: "a correlated IN", caps: capability.YDB262(),
				render: selectFor(query.Select("id").FromAs("users", "u").Where(query.InQuery("id",
					query.Select("user_id").FromAs("orders", "o").Where(query.Col("u", "active").Eq(true)))))},
			wantErr: `renderer: a subquery that reads "u" from the query around it, which requires target ` +
				"capability correlated_subqueries, unavailable on this ydb target",
		},
		{
			ydbRender: ydbRender{name: "a subquery whose own subquery reads the outer query", caps: capability.YDB262(),
				render: selectFor(query.Select("id").FromAs("users", "u").Where(query.Exists(
					query.Select("id").FromAs("orders", "o").Where(query.NotExists(
						query.Select("id").FromAs("refunds", "r").Where(query.Col("r", "user_id").EqCol(query.Col("u", "id")))))))),
			},
			wantErr: `renderer: a subquery that reads "u" from the query around it, which requires target ` +
				"capability correlated_subqueries, unavailable on this ydb target",
		},
		{
			ydbRender: ydbRender{name: "a join on an inequality", caps: capability.YDB262(),
				render: selectFor(query.Select().Columns(query.Col("u", "id")).FromAs("users", "u").
					InnerJoin("orders", "o", &query.Comparison{Left: &query.ColumnRef{Qualifier: "o", Name: "total"},
						Operator: query.OpLessThan, Right: &query.ColumnRef{Qualifier: "u", Name: "limit"}}))},
			wantErr: "renderer: a JOIN condition other than equalities between columns of the joined tables, " +
				"which requires target capability non_equi_joins, unavailable on this ydb target",
		},
		{
			ydbRender: ydbRender{name: "a join on a value", caps: capability.YDB262(),
				render: selectFor(query.Select().Columns(query.Col("u", "id")).FromAs("users", "u").
					InnerJoin("orders", "o", query.And(
						query.Col("o", "user_id").EqCol(query.Col("u", "id")),
						query.Col("o", "status").Eq("paid"),
					)))},
			wantErr: "renderer: a JOIN condition other than equalities between columns of the joined tables, " +
				"which requires target capability non_equi_joins, unavailable on this ydb target",
		},
		{
			ydbRender: ydbRender{name: "a join on a disjunction", caps: capability.YDB262(),
				render: selectFor(query.Select().Columns(query.Col("u", "id")).FromAs("users", "u").
					InnerJoin("orders", "o", query.Or(
						query.Col("o", "user_id").EqCol(query.Col("u", "id")),
						query.Col("o", "owner_id").EqCol(query.Col("u", "id")),
					)))},
			wantErr: "renderer: a JOIN condition other than equalities between columns of the joined tables, " +
				"which requires target capability non_equi_joins, unavailable on this ydb target",
		},
		{
			ydbRender: ydbRender{name: "an OFFSET without a LIMIT", caps: capability.YDB262(),
				render: selectFor(query.Select("id").From("users").OrderBy(query.Asc("id")).Offset(5))},
			wantErr: "renderer: an OFFSET without a LIMIT, which requires target capability offset_without_limit, " +
				"unavailable on this ydb target",
		},
		{
			ydbRender: ydbRender{name: "an insert returning on 25.1", caps: capability.YDB251(),
				render: insertFor(query.InsertInto("users").Columns("id").Values(int64(1)).Returning("id"))},
			wantErr: "renderer: RETURNING, which requires target capability returning_clause, unavailable on this ydb target",
		},
		{
			ydbRender: ydbRender{name: "an update returning on 25.2", caps: capability.YDB252(),
				render: updateFor(query.Update("users").Set("name", "x").Where(query.Eq("id", int64(1))).Returning("id"))},
			wantErr: "renderer: RETURNING, which requires target capability returning_clause, unavailable on this ydb target",
		},
		{
			ydbRender: ydbRender{name: "a delete returning on 25.1", caps: capability.YDB251(),
				render: deleteFor(query.DeleteFrom("users").Where(query.Eq("id", int64(1))).Returning("id"))},
			wantErr: "renderer: RETURNING, which requires target capability returning_clause, unavailable on this ydb target",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, args, err := test.render(test.caps)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(got, qt.Equals, "")
			c.Assert(args, qt.IsNil)
		})
	}
}

// The statements YQL has no spelling for, and the bounds no Uint64 holds, are
// refused with the reason.
func TestRenderYDB_FailurePath_RefusesWhatYQLHasNoSpellingFor(t *testing.T) {
	tests := []struct {
		ydbRender
		wantErr string
	}{
		{
			ydbRender: ydbRender{name: "on conflict do nothing", caps: capability.YDB262(),
				render: insertFor(query.InsertInto("users").Columns("id").Values(int64(1)).OnConflictDoNothing())},
			wantErr: "renderer: YDB has no ON CONFLICT: UpsertInto writes each row over the row with the same " +
				"primary key, and the server refuses INSERT OR IGNORE",
		},
		{
			ydbRender: ydbRender{name: "on conflict do update", caps: capability.YDB262(),
				render: insertFor(query.InsertInto("users").Columns("id", "name").Values(int64(1), "a").
					OnConflictDoUpdate([]string{"id"}, "name"))},
			wantErr: "renderer: YDB has no ON CONFLICT: UpsertInto writes each row over the row with the same " +
				"primary key, and the server refuses INSERT OR IGNORE",
		},
		{
			ydbRender: ydbRender{name: "an upsert with an on conflict clause", caps: capability.YDB262(),
				render: insertFor(query.UpsertInto("users").Columns("id", "name").Values(int64(1), "a").
					OnConflictDoNothing())},
			wantErr: "renderer: an UPSERT carries no ON CONFLICT clause: it already writes each row over the row " +
				"with the same primary key",
		},
		{
			ydbRender: ydbRender{name: "a negative limit", caps: capability.YDB262(),
				render: selectFor(query.Select("id").From("users").Limit(-1))},
			wantErr: "renderer: YDB takes LIMIT as a Uint64, and -1 is negative",
		},
		{
			ydbRender: ydbRender{name: "a negative offset", caps: capability.YDB262(),
				render: selectFor(query.Select("id").From("users").Limit(1).Offset(-2))},
			wantErr: "renderer: YDB takes OFFSET as a Uint64, and -2 is negative",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, args, err := test.render(test.caps)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(got, qt.Equals, "")
			c.Assert(args, qt.IsNil)
		})
	}
}

// UPSERT INTO is YDB's statement. Every other dialect names the key an upsert
// watches, which the builder does not know for a table, so it refuses rather
// than guess one.
func TestRenderInsert_FailurePath_UpsertOutsideYDB(t *testing.T) {
	for _, dialect := range []string{platform.Postgres, platform.MySQL, platform.SQLite, platform.SQLServer} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			got, args, err := query.RenderInsert(
				query.UpsertInto("users").Columns("id", "name").Values(int64(1), "a").Build(), dialect)
			c.Assert(err, qt.ErrorMatches, "renderer: "+dialect+" has no UPSERT statement: UPSERT INTO is YDB's, "+
				"and here an upsert names the key it watches; use OnConflictDoUpdate")
			c.Assert(got, qt.Equals, "")
			c.Assert(args, qt.IsNil)
		})
	}
}

// RETURNING follows the target's key on every dialect: SQLite takes it from
// 3.35, and a caller that names the older line is refused.
func TestRenderInsertWithCapabilities_FailurePath_ReturningOnOldSQLite(t *testing.T) {
	c := qt.New(t)
	got, args, err := query.RenderInsertWithCapabilities(
		query.InsertInto("t").Columns("a").Values(int64(1)).Returning("id").Build(),
		platform.SQLite, capability.SQLite324())
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, "renderer: RETURNING, which requires target capability returning_clause, "+
		"unavailable on this sqlite target")
	c.Assert(got, qt.Equals, "")
	c.Assert(args, qt.IsNil)
}
