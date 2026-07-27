// Package query provides a fluent, dialect-aware builder for parameterized SQL
// SELECT statements.
//
// It is the DML counterpart to the DDL construction helpers in
// ptah/internal/astbuilder: where those build CREATE TABLE and friends, this
// package builds read queries. A builder produces an *ast.SelectStatement, which
// renderer.RenderSelect turns into a SQL string plus its positional arguments
// for PostgreSQL, MySQL, MariaDB, and SQLite.
//
// # Scope
//
// This package models a SELECT with a FROM table, INNER / LEFT / RIGHT / FULL
// OUTER joins, a composable WHERE expression tree, DISTINCT, GROUP BY, HAVING,
// aggregate functions (COUNT / SUM / AVG / MIN / MAX), ORDER BY, LIMIT, and
// OFFSET. Tables can be aliased (FromAs and the alias argument of the join
// methods) and columns can be qualified by a table or alias with Col, so a
// projection, WHERE, join ON, GROUP BY, HAVING, or ORDER BY term can render as
// "alias"."col". Non-aggregate function calls, arithmetic, LIKE, subqueries,
// window functions, and the INSERT / UPDATE / DELETE family are intentionally not
// implemented yet and are tracked as follow-up phases.
//
// # Safety model
//
// The builder separates identifiers from values structurally, so the classic
// "string-concatenate a value into SQL" injection cannot happen through this
// API:
//
//   - Values passed to Eq, In, and the other comparison helpers are typed as
//     any and always travel to the database as bound parameters. They are never
//     interpolated into the SQL text; the renderer emits a placeholder ($1, $2,
//     … for PostgreSQL; ? for MySQL/MariaDB/SQLite) and appends the value to the
//     returned argument slice. LIMIT and OFFSET values are bound the same way.
//   - Identifiers (table names, column names) are always emitted through
//     dialect-aware quoting, so an attacker-shaped identifier cannot terminate
//     the quoted identifier and inject SQL. As with Ptah's DDL rendering,
//     restricting which identifiers a caller may supply (for example, an
//     allow-list of sortable columns) remains the caller's responsibility; the
//     builder guarantees quoting and the absence of value interpolation.
//
// Placeholder numbering is assigned by the renderer in a single left-to-right
// pass, so argument order always matches placeholder order and callers never
// manage indices by hand.
//
// # Usage
//
//	stmt := query.Select("id", "name").
//		From("commodities").
//		Where(query.And(
//			query.Eq("draft", false),
//			query.In("status", []string{"in_use", "sold"}),
//			query.Or(
//				query.IsNotNull("deleted_at"),
//				query.Not(query.Gt("count", int64(10))),
//			),
//		)).
//		OrderBy(query.Asc("name"), query.Asc("id")).
//		Limit(24).
//		Offset(0).
//		Build()
//
//	sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
//	// sql:  SELECT "id", "name" FROM "commodities" WHERE (...) ORDER BY ... LIMIT $5 OFFSET $6
//	// args: []any{false, "in_use", "sold", int64(10), int64(24), int64(0)}
//
// A WHERE expression can be built once and shared between statements (for
// example, a COUNT(*) and the paged SELECT that share the same filter), because
// the expression constructors return plain *ast expression nodes.
//
// # Joins
//
// Alias the source table with FromAs, join with InnerJoin, LeftJoin, RightJoin,
// or FullJoin, and qualify columns with Col so they render as "alias"."col". A
// join ON condition is an ordinary expression, so an equi-join is Col(…).EqCol
// and richer predicates compose with And, Or, and Not:
//
//	stmt := query.Select().
//		Columns(query.Col("u", "id"), query.Col("u", "name"), query.Col("o", "total")).
//		FromAs("users", "u").
//		InnerJoin("orders", "o", query.Col("o", "user_id").EqCol(query.Col("u", "id"))).
//		Where(query.And(
//			query.Col("o", "status").Eq("paid"),
//			query.Col("u", "active").Eq(true),
//		)).
//		OrderBy(query.Col("u", "name").Asc()).
//		Limit(20).
//		Build()
//
//	sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
//	// sql:  SELECT "u"."id", "u"."name", "o"."total" FROM "users" "u"
//	//       INNER JOIN "orders" "o" ON "o"."user_id" = "u"."id"
//	//       WHERE ("o"."status" = $1 AND "u"."active" = $2)
//	//       ORDER BY "u"."name" ASC LIMIT $3
//	// args: []any{"paid", true, int64(20)}
//
// A join ON value is bound before any WHERE value, because joins render before
// WHERE. Aliases and qualifiers are quoted through the same dialect-aware path
// as every other identifier.
//
// Not every dialect can express every join type, and RenderSelect rejects an
// unsupported one at render time rather than emit SQL the database would reject:
// SQLite could not express RIGHT or FULL OUTER joins before version 3.39, and
// MySQL and MariaDB have no FULL OUTER JOIN in any version. In short, FULL OUTER
// renders only on the PostgreSQL family; RIGHT renders everywhere except SQLite;
// INNER and LEFT render on every supported dialect.
//
// # Aggregates, GROUP BY, and HAVING
//
// Distinct renders SELECT DISTINCT. GroupBy adds GROUP BY columns (qualified with
// Col, or bare with Col("", name)). The aggregate constructors — CountStar,
// Count, CountDistinct, Sum, Avg, Min, and Max, plus the matching methods on Col
// for qualified columns — return expressions usable in two places: a projection
// (via Exprs, or ExprAs to attach an AS alias) and a HAVING predicate. Because a
// HAVING predicate compares an aggregate against a value, wrap the aggregate with
// Expr to reach the comparison helpers: Expr(CountStar()).Gt(int64(5)).
//
//	stmt := query.Select("status").
//		ExprAs(query.CountStar(), "n").
//		From("orders").
//		Where(query.Eq("tenant_id", tenantID)).
//		GroupBy(query.Col("", "status")).
//		Having(query.Expr(query.CountStar()).Gt(int64(5))).
//		OrderBy(query.Asc("status")).
//		Limit(10).
//		Build()
//
//	sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
//	// sql:  SELECT "status", COUNT(*) AS "n" FROM "orders" WHERE "tenant_id" = $1
//	//       GROUP BY "status" HAVING COUNT(*) > $2 ORDER BY "status" ASC LIMIT $3
//	// args: []any{tenantID, int64(5), int64(10)}
//
// GROUP BY carries only identifiers and binds nothing; a HAVING value is bound
// after every WHERE value and before LIMIT/OFFSET, so placeholder numbering still
// follows left-to-right emission order. A function name (COUNT, SUM, …) is a
// keyword emitted verbatim and never quoted; the renderer rejects a name that is
// not a simple identifier rather than emit it.
package query
