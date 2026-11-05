package registeredtest_test

import (
	"database/sql"
	"regexp"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp/cmpopts"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/ptaherr"
	"ptah.run/core/query"
)

// The YDB provider (stokaro/ptah-ydb-plugin) declares the query dialect
// yqlSpelling returns, and the host registers it under the engine's name when
// it starts the provider. These tests render through that registration: the
// texts the happy path pins are the ones the provider's tests run on YDB 25.1
// and 26.2 (its integration/query_builder_live_test.go), so the two halves
// meet at the text.

// yqlSpelling is the YDB provider's declared query dialect: $p1, $p2 and on,
// named after the parameter; every name in YQL's backticks, a table path as
// one name; the builder decides a WITH clause, a correlated subquery, a JOIN
// condition other than column equalities and an OFFSET without a LIMIT by the
// target's capabilities, and binds LIMIT and OFFSET unsigned; an upsert is
// UPSERT INTO, and ON CONFLICT is refused.
func yqlSpelling() query.Spelling {
	return query.Spelling{
		Placeholder: descriptor.PlaceholderDollarNamed,
		Quoting:     descriptor.Quoting{Style: descriptor.QuoteYQL, Bare: descriptor.BareNever, Paths: true},
		Query: descriptor.Query{ChecksCapabilities: true, UnsignedBounds: true, Upsert: descriptor.UpsertInto,
			UpsertRefusal: "YDB has no ON CONFLICT: UpsertInto writes each row over the row with the same primary key, " +
				"and the server refuses INSERT OR IGNORE"},
		DefaultCapabilities: map[capability.Capability]bool{
			capability.CommonTableExpressions: false,
			capability.CorrelatedSubqueries:   false,
			capability.NonEquiJoins:           false,
			capability.OffsetWithoutLimit:     false,
			capability.ReturningClause:        true,
		},
	}
}

// yqlEngine is the engine the YDB provider serves, the name the host
// registers its spelling under. No dialect the process registry describes
// has it.
const yqlEngine = "ydb"

// registerYQL registers yqlSpelling once for the test binary.
var registerYQL = sync.OnceValue(func() error { return query.RegisterDialect(yqlEngine, yqlSpelling()) })

// The capabilities the YDB provider establishes for the keys the builder
// reads: RETURNING from 25.3 on, and none of the other four on any line.
var (
	yql262 = capability.Of(map[capability.Capability]bool{capability.ReturningClause: true, capability.CommonTableExpressions: false,
		capability.CorrelatedSubqueries: false, capability.NonEquiJoins: false, capability.OffsetWithoutLimit: false})
	yql251 = yql262.With(capability.ReturningClause, false)
)

// argsEqual compares rendered arguments: each is a sql.NamedArg, whose
// unexported marker field qt.DeepEquals cannot walk, so they are compared
// with ==, which compares the name and the value.
var argsEqual = qt.CmpEquals(cmpopts.EquateComparable(sql.NamedArg{}))

// yqlRender renders one statement for the YQL engine on a target with caps.
type yqlRender func(caps capability.Capabilities) (string, []any, error)

func selectFor(builder *query.SelectBuilder) yqlRender {
	return func(caps capability.Capabilities) (string, []any, error) {
		return query.RenderSelectWithCapabilities(builder.Build(), yqlEngine, caps)
	}
}

func insertFor(builder *query.InsertBuilder) yqlRender {
	return func(caps capability.Capabilities) (string, []any, error) {
		return query.RenderInsertWithCapabilities(builder.Build(), yqlEngine, caps)
	}
}

func updateFor(builder *query.UpdateBuilder) yqlRender {
	return func(caps capability.Capabilities) (string, []any, error) {
		return query.RenderUpdateWithCapabilities(builder.Build(), yqlEngine, caps)
	}
}

func deleteFor(builder *query.DeleteBuilder) yqlRender {
	return func(caps capability.Capabilities) (string, []any, error) {
		return query.RenderDeleteWithCapabilities(builder.Build(), yqlEngine, caps)
	}
}

// usersOrdersJoin joins app/orders to app/users with join, on the order's
// user.
func usersOrdersJoin(join func(*query.SelectBuilder, string, string, query.Expression) *query.SelectBuilder) *query.SelectBuilder {
	return join(
		query.Select().Columns(query.Col("u", "name"), query.Col("o", "total")).FromAs("app/users", "u"),
		"app/orders", "o", query.Col("o", "user_id").EqCol(query.Col("u", "id")),
	)
}

// Every kind of statement the YDB provider's tests run renders as they run
// it: an INSERT, an UPSERT over existing and new rows, a join paged with
// LIMIT and OFFSET, an IN subquery, an uncorrelated EXISTS and a grouped
// count, with the names in backticks, a table path as one name, and each
// argument named after its parameter at the type the caller gave. The joins,
// the subqueries that read only their own tables, and the statements a
// target's key admits render the same way.
func TestRegisteredYQLDialect_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(registerYQL(), qt.IsNil)
	for _, test := range []struct {
		name     string
		caps     capability.Capabilities
		render   yqlRender
		wantSQL  string
		wantArgs []any
	}{
		{name: "an insert of users", caps: yql262,
			render: insertFor(query.InsertInto("app/users").Columns("id", "name", "age").
				Values(int64(1), "alice", int32(30)).Values(int64(2), "bob", int32(40))),
			wantSQL: "INSERT INTO `app/users` (`id`, `name`, `age`) VALUES ($p1, $p2, $p3), ($p4, $p5, $p6)",
			wantArgs: []any{sql.Named("p1", int64(1)), sql.Named("p2", "alice"), sql.Named("p3", int32(30)),
				sql.Named("p4", int64(2)), sql.Named("p5", "bob"), sql.Named("p6", int32(40))}},
		{name: "an insert of orders", caps: yql262,
			render: insertFor(query.InsertInto("app/orders").Columns("id", "user_id", "total").
				Values(int64(10), int64(1), 5.5).Values(int64(11), int64(1), 7.0).Values(int64(12), int64(2), 1.0)),
			wantSQL: "INSERT INTO `app/orders` (`id`, `user_id`, `total`) VALUES ($p1, $p2, $p3), ($p4, $p5, $p6), ($p7, $p8, $p9)",
			wantArgs: []any{sql.Named("p1", int64(10)), sql.Named("p2", int64(1)), sql.Named("p3", 5.5),
				sql.Named("p4", int64(11)), sql.Named("p5", int64(1)), sql.Named("p6", 7.0),
				sql.Named("p7", int64(12)), sql.Named("p8", int64(2)), sql.Named("p9", 1.0)}},
		{name: "an upsert over an existing and a new row", caps: yql262,
			render:   insertFor(query.UpsertInto("app/users").Columns("id", "name").Values(int64(1), "alicia").Values(int64(3), "carol")),
			wantSQL:  "UPSERT INTO `app/users` (`id`, `name`) VALUES ($p1, $p2), ($p3, $p4)",
			wantArgs: []any{sql.Named("p1", int64(1)), sql.Named("p2", "alicia"), sql.Named("p3", int64(3)), sql.Named("p4", "carol")}},
		{name: "a join paged with limit and offset", caps: yql262,
			render: selectFor(usersOrdersJoin((*query.SelectBuilder).InnerJoin).
				Where(query.Col("o", "total").Gt(2.0)).OrderBy(query.Col("o", "total").Desc()).Limit(1).Offset(1)),
			wantSQL: "SELECT `u`.`name`, `o`.`total` FROM `app/users` `u` INNER JOIN `app/orders` `o` ON `o`.`user_id` = `u`.`id` " +
				"WHERE `o`.`total` > $p1 ORDER BY `o`.`total` DESC LIMIT $p2 OFFSET $p3",
			wantArgs: []any{sql.Named("p1", 2.0), sql.Named("p2", uint64(1)), sql.Named("p3", uint64(1))}},
		{name: "an IN subquery", caps: yql262,
			render: selectFor(query.Select("id").From("app/users").Where(query.InQuery("id", query.Select("user_id").From("app/orders"))).
				OrderBy(query.Asc("id"))),
			wantSQL: "SELECT `id` FROM `app/users` WHERE `id` IN (SELECT `user_id` FROM `app/orders`) ORDER BY `id` ASC"},
		{name: "an uncorrelated EXISTS", caps: yql262,
			render: selectFor(query.Select("id").From("app/users").Where(query.Exists(query.Select("id").From("app/orders").Where(query.Gt("total", 6.0)))).
				OrderBy(query.Asc("id"))),
			wantSQL:  "SELECT `id` FROM `app/users` WHERE EXISTS (SELECT `id` FROM `app/orders` WHERE `total` > $p1) ORDER BY `id` ASC",
			wantArgs: []any{sql.Named("p1", 6.0)}},
		{name: "a grouped count", caps: yql262,
			render: selectFor(query.Select().Columns(query.Col("o", "user_id")).ExprAs(query.CountStar(), "n").FromAs("app/orders", "o").
				GroupBy(query.Col("o", "user_id")).Having(query.Expr(query.CountStar()).Gt(uint64(1)))),
			wantSQL:  "SELECT `o`.`user_id`, COUNT(*) AS `n` FROM `app/orders` `o` GROUP BY `o`.`user_id` HAVING COUNT(*) > $p1",
			wantArgs: []any{sql.Named("p1", uint64(1))}},
		{name: "a left join", caps: yql262, render: selectFor(usersOrdersJoin((*query.SelectBuilder).LeftJoin)),
			wantSQL: "SELECT `u`.`name`, `o`.`total` FROM `app/users` `u` LEFT JOIN `app/orders` `o` ON `o`.`user_id` = `u`.`id`"},
		{name: "a right join", caps: yql262, render: selectFor(usersOrdersJoin((*query.SelectBuilder).RightJoin)),
			wantSQL: "SELECT `u`.`name`, `o`.`total` FROM `app/users` `u` RIGHT JOIN `app/orders` `o` ON `o`.`user_id` = `u`.`id`"},
		{name: "a full join", caps: yql262, render: selectFor(usersOrdersJoin((*query.SelectBuilder).FullJoin)),
			wantSQL: "SELECT `u`.`name`, `o`.`total` FROM `app/users` `u` FULL OUTER JOIN `app/orders` `o` ON `o`.`user_id` = `u`.`id`"},
		{name: "a join on a conjunction of equalities", caps: yql262,
			render: selectFor(query.Select().Columns(query.Col("u", "name")).FromAs("app/users", "u").InnerJoin("app/orders", "o", query.And(
				query.Col("o", "user_id").EqCol(query.Col("u", "id")),
				query.Col("o", "region").EqCol(query.Col("u", "region")),
			))),
			wantSQL: "SELECT `u`.`name` FROM `app/users` `u` INNER JOIN `app/orders` `o` ON (`o`.`user_id` = `u`.`id` AND `o`.`region` = `u`.`region`)"},
		{name: "a subquery that reads its own joined table", caps: yql262,
			render: selectFor(query.Select("id").From("app/users").Where(query.Exists(
				query.Select().Columns(query.Col("o", "id")).FromAs("app/orders", "o").
					InnerJoin("app/refunds", "r", query.Col("r", "order_id").EqCol(query.Col("o", "id"))).
					Where(query.Col("r", "amount").Gt(int64(1)))))),
			wantSQL: "SELECT `id` FROM `app/users` WHERE EXISTS (SELECT `o`.`id` FROM `app/orders` `o` " +
				"INNER JOIN `app/refunds` `r` ON `r`.`order_id` = `o`.`id` WHERE `r`.`amount` > $p1)",
			wantArgs: []any{sql.Named("p1", int64(1))}},
		{name: "a WITH clause where the target has the key", caps: yql262.With(capability.CommonTableExpressions, true),
			render:  selectFor(query.Select("id").With("c", query.Select("id").From("app/users")).From("c")),
			wantSQL: "WITH `c` AS (SELECT `id` FROM `app/users`) SELECT `id` FROM `c`"},
		{name: "an OFFSET alone where the target has the key", caps: yql262.With(capability.OffsetWithoutLimit, true),
			render:   selectFor(query.Select("id").From("app/users").Offset(5)),
			wantSQL:  "SELECT `id` FROM `app/users` OFFSET $p1",
			wantArgs: []any{sql.Named("p1", uint64(5))}},
		{name: "an upsert from a query, returning", caps: yql262,
			render:  insertFor(query.UpsertInto("app/archive").Columns("id", "name").FromSelect(query.Select("id", "name").From("app/users")).Returning("id")),
			wantSQL: "UPSERT INTO `app/archive` (`id`, `name`) SELECT `id`, `name` FROM `app/users` RETURNING `id`"},
		{name: "an update returning", caps: yql262,
			render:   updateFor(query.Update("app/users").Set("name", "bob").Where(query.Eq("id", int64(7))).Returning("id", "name")),
			wantSQL:  "UPDATE `app/users` SET `name` = $p1 WHERE `id` = $p2 RETURNING `id`, `name`",
			wantArgs: []any{sql.Named("p1", "bob"), sql.Named("p2", int64(7))}},
		{name: "a delete returning", caps: yql262,
			render:   deleteFor(query.DeleteFrom("app/users").Where(query.Eq("id", int64(7))).Returning("id")),
			wantSQL:  "DELETE FROM `app/users` WHERE `id` = $p1 RETURNING `id`",
			wantArgs: []any{sql.Named("p1", int64(7))}},
	} {
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
func TestRegisteredYQLDialect_FailurePath_RefusesWhatTheTargetLacks(t *testing.T) {
	c := qt.New(t)
	c.Assert(registerYQL(), qt.IsNil)
	for _, test := range []struct {
		name    string
		caps    capability.Capabilities
		render  yqlRender
		wantErr string
	}{
		{name: "a WITH clause", caps: yql262,
			render:  selectFor(query.Select("id").With("c", query.Select("id").From("app/users")).From("c")),
			wantErr: "renderer: a WITH clause, which requires target capability common_table_expressions, unavailable on this ydb target"},
		{name: "a correlated EXISTS", caps: yql262,
			render: selectFor(query.Select("id").FromAs("app/users", "u").Where(query.Exists(
				query.Select("id").FromAs("app/orders", "o").Where(query.Col("o", "user_id").EqCol(query.Col("u", "id")))))),
			wantErr: `renderer: a subquery that reads "u" from the query around it, which requires target capability correlated_subqueries, unavailable on this ydb target`},
		{name: "a correlated IN", caps: yql262,
			render: selectFor(query.Select("id").FromAs("app/users", "u").Where(query.InQuery("id",
				query.Select("user_id").FromAs("app/orders", "o").Where(query.Col("u", "active").Eq(true))))),
			wantErr: `renderer: a subquery that reads "u" from the query around it, which requires target capability correlated_subqueries, unavailable on this ydb target`},
		{name: "a subquery whose own subquery reads the outer query", caps: yql262,
			render: selectFor(query.Select("id").FromAs("app/users", "u").Where(query.Exists(
				query.Select("id").FromAs("app/orders", "o").Where(query.NotExists(
					query.Select("id").FromAs("app/refunds", "r").Where(query.Col("r", "user_id").EqCol(query.Col("u", "id")))))))),
			wantErr: `renderer: a subquery that reads "u" from the query around it, which requires target capability correlated_subqueries, unavailable on this ydb target`},
		{name: "a join on an inequality", caps: yql262,
			render: selectFor(query.Select().Columns(query.Col("u", "id")).FromAs("app/users", "u").
				InnerJoin("app/orders", "o", &query.Comparison{Left: &query.ColumnRef{Qualifier: "o", Name: "total"},
					Operator: query.OpLessThan, Right: &query.ColumnRef{Qualifier: "u", Name: "limit"}})),
			wantErr: "renderer: a JOIN condition other than equalities between columns of the joined tables, " +
				"which requires target capability non_equi_joins, unavailable on this ydb target"},
		{name: "a join on a value", caps: yql262,
			render: selectFor(query.Select().Columns(query.Col("u", "id")).FromAs("app/users", "u").InnerJoin("app/orders", "o", query.And(
				query.Col("o", "user_id").EqCol(query.Col("u", "id")),
				query.Col("o", "status").Eq("paid"),
			))),
			wantErr: "renderer: a JOIN condition other than equalities between columns of the joined tables, " +
				"which requires target capability non_equi_joins, unavailable on this ydb target"},
		{name: "a join on a disjunction", caps: yql262,
			render: selectFor(query.Select().Columns(query.Col("u", "id")).FromAs("app/users", "u").InnerJoin("app/orders", "o", query.Or(
				query.Col("o", "user_id").EqCol(query.Col("u", "id")),
				query.Col("o", "owner_id").EqCol(query.Col("u", "id")),
			))),
			wantErr: "renderer: a JOIN condition other than equalities between columns of the joined tables, " +
				"which requires target capability non_equi_joins, unavailable on this ydb target"},
		{name: "an OFFSET without a LIMIT", caps: yql262,
			render:  selectFor(query.Select("id").From("app/users").OrderBy(query.Asc("id")).Offset(5)),
			wantErr: "renderer: an OFFSET without a LIMIT, which requires target capability offset_without_limit, unavailable on this ydb target"},
		{name: "an insert returning on 25.1", caps: yql251,
			render:  insertFor(query.InsertInto("app/users").Columns("id").Values(int64(1)).Returning("id")),
			wantErr: "renderer: RETURNING, which requires target capability returning_clause, unavailable on this ydb target"},
		{name: "an update returning on 25.1", caps: yql251,
			render:  updateFor(query.Update("app/users").Set("name", "x").Where(query.Eq("id", int64(1))).Returning("id")),
			wantErr: "renderer: RETURNING, which requires target capability returning_clause, unavailable on this ydb target"},
		{name: "a delete returning on 25.1", caps: yql251,
			render:  deleteFor(query.DeleteFrom("app/users").Where(query.Eq("id", int64(1))).Returning("id")),
			wantErr: "renderer: RETURNING, which requires target capability returning_clause, unavailable on this ydb target"},
	} {
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
func TestRegisteredYQLDialect_FailurePath_RefusesWhatYQLHasNoSpellingFor(t *testing.T) {
	c := qt.New(t)
	c.Assert(registerYQL(), qt.IsNil)
	for _, test := range []struct {
		name    string
		render  yqlRender
		wantErr string
	}{
		{name: "on conflict do nothing",
			render:  insertFor(query.InsertInto("app/users").Columns("id").Values(int64(1)).OnConflictDoNothing()),
			wantErr: "renderer: YDB has no ON CONFLICT: UpsertInto writes each row over the row with the same primary key, and the server refuses INSERT OR IGNORE"},
		{name: "on conflict do update",
			render:  insertFor(query.InsertInto("app/users").Columns("id", "name").Values(int64(1), "a").OnConflictDoUpdate([]string{"id"}, "name")),
			wantErr: "renderer: YDB has no ON CONFLICT: UpsertInto writes each row over the row with the same primary key, and the server refuses INSERT OR IGNORE"},
		{name: "an upsert with an on conflict clause",
			render:  insertFor(query.UpsertInto("app/users").Columns("id", "name").Values(int64(1), "a").OnConflictDoNothing()),
			wantErr: "renderer: an UPSERT carries no ON CONFLICT clause: it already writes each row over the row with the same primary key"},
		{name: "a negative limit",
			render:  selectFor(query.Select("id").From("app/users").Limit(-1)),
			wantErr: "renderer: ydb binds LIMIT as an unsigned integer, and -1 is negative"},
		{name: "a negative offset",
			render:  selectFor(query.Select("id").From("app/users").Limit(1).Offset(-2)),
			wantErr: "renderer: ydb binds OFFSET as an unsigned integer, and -2 is negative"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, args, err := test.render(yql262)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(got, qt.Equals, "")
			c.Assert(args, qt.IsNil)
		})
	}
}
