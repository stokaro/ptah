// Package query provides a fluent, dialect-aware builder for parameterized SQL
// SELECT statements.
//
// It is the DML counterpart to the DDL construction helpers in
// ptah/internal/astbuilder: where those build CREATE TABLE and friends, this
// package builds read queries. A builder produces an *ast.SelectStatement, which
// renderer.RenderSelect turns into a SQL string plus its positional arguments
// for PostgreSQL, MySQL, MariaDB, SQLite, ClickHouse, and SQL Server.
//
// # Scope
//
// This package models a SELECT with a FROM table, INNER / LEFT / RIGHT / FULL
// OUTER joins, a composable WHERE expression tree, DISTINCT, GROUP BY, HAVING,
// aggregate functions (COUNT / SUM / AVG / MIN / MAX), ORDER BY, LIMIT, and
// OFFSET. Tables can be aliased (FromAs and the alias argument of the join
// methods) and columns can be qualified by a table or alias with Col, so a
// projection, WHERE, join ON, GROUP BY, HAVING, or ORDER BY term can render as
// "alias"."col".
//
// The write side of DML is covered too: InsertInto, Update, and DeleteFrom build
// single-table INSERT / UPDATE / DELETE statements, each rendered by its own
// renderer entry point (RenderInsert, RenderUpdate, RenderDelete). See the
// "Writes" section below.
//
// ClickHouse is the one dialect that does not render all four: UPDATE and
// DELETE are mutations there -- spelled ALTER TABLE … UPDATE and applied
// asynchronously outside a transaction -- so they are refused with that reason
// rather than emitted in a portable spelling the server does not parse. SELECT
// and INSERT are ordinary statements there and render normally.
//
// LIKE and NOT LIKE are available through Like and NotLike. Their pattern is a
// bound value like any other, so it can carry no SQL; its wildcards are the
// caller's to write and to escape. Case sensitivity is the server's -- see
// [Like].
//
// Upsert is available through InsertBuilder.OnConflictDoNothing and
// OnConflictDoUpdate. The engines disagree about MEANING rather than spelling
// here, so the builder refuses the combinations they cannot express: PostgreSQL
// and SQLite require the conflict target for DO UPDATE, MySQL and MariaDB
// accept no target at all because ON DUPLICATE KEY UPDATE fires for every
// unique key, and SQL Server (MERGE) and ClickHouse (no upsert statement) are
// refused by name.
//
// Subqueries reach WHERE through InQuery, Exists and NotExists, and a
// non-recursive common table expression through SelectBuilder.With. Both bind
// their values in the order they are emitted, so a driver reading positionally
// sees CTE values, then the outer query's, then a subquery's.
//
// Func calls a function this package has no named helper for, and Add, Sub,
// Mul, Div and Mod build arithmetic. Every arithmetic node renders
// parenthesized, so the tree the caller built is the expression the server
// evaluates rather than one its precedence rules recover.
//
// InsertBuilder.FromSelect supplies the inserted rows from a query instead of
// from literal values. It is mutually exclusive with Values, and the query must
// project one expression per target column, explicitly -- a star projection
// supplies whatever the source table has today.
//
// Window functions are intentionally not implemented yet and are tracked as a
// follow-up phase of stokaro/ptah#941.
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
//     … for PostgreSQL; ? for MySQL/MariaDB/SQLite/ClickHouse; @p1, @p2, … for
//     SQL Server) and appends the value to the returned argument slice. LIMIT
//     and OFFSET values are bound the same way, including the OFFSET/FETCH
//     bounds SQL Server pages with.
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
//
// # Writes (INSERT, UPDATE, DELETE)
//
// InsertInto, Update, and DeleteFrom build the write-side statements. Values
// passed to Values and Set are bound exactly like WHERE values — the classic
// "concatenate a value into SQL" injection cannot happen through this API — and
// column and table names are quoted. Each has its own renderer entry point:
// RenderInsert, RenderUpdate, RenderDelete, all returning (sql, args, err).
//
//	ins := query.InsertInto("users").
//		Columns("id", "name").
//		Values(int64(1), "alice").
//		Values(int64(2), "bob").
//		Returning("id").
//		Build()
//	sql, args, err := renderer.RenderInsert(ins, platform.Postgres)
//	// sql:  INSERT INTO "users" ("id", "name") VALUES ($1, $2), ($3, $4) RETURNING "id"
//	// args: []any{int64(1), "alice", int64(2), "bob"}
//
//	upd := query.Update("users").
//		Set("name", "bob").
//		Where(query.Eq("id", int64(7))).
//		Build()
//	sql, args, err = renderer.RenderUpdate(upd, platform.Postgres)
//	// sql:  UPDATE "users" SET "name" = $1 WHERE "id" = $2
//	// args: []any{"bob", int64(7)}
//
// An UPDATE's SET values are numbered before its WHERE values, matching
// left-to-right emission order, and an INSERT numbers values row by row, so
// placeholder order always follows the SQL.
//
// Two safety rules apply. First, a whole-table UPDATE or DELETE — one with no
// WHERE clause — is rejected at render time unless the builder marks it
// Unconditional, so a missing filter cannot silently rewrite or delete every row.
// Second, RETURNING renders only on dialects that can execute it: the PostgreSQL
// family and SQLite (3.35+). MySQL and MariaDB have no portable RETURNING across
// all three statements, so RenderInsert / RenderUpdate / RenderDelete reject a
// non-empty RETURNING there rather than emit SQL the engine cannot run.
package query
