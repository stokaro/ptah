package planner_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// routineOrderCase is a schema and the statement heads a plan creating it
// from nothing has to emit in this order.
type routineOrderCase struct {
	name  string
	sql   string
	order []string
}

// routineOrderCases are the shapes PostgreSQL refuses in the wrong order,
// measured on PostgreSQL 18.6, plus the two that must stay early.
var routineOrderCases = []routineOrderCase{
	{
		name: "a LANGUAGE sql body reads a table",
		sql: `CREATE TABLE orders (id bigint PRIMARY KEY, total integer NOT NULL);
CREATE FUNCTION order_count() RETURNS bigint LANGUAGE sql STABLE AS $$ SELECT count(*) FROM orders $$;`,
		order: []string{`CREATE TABLE "orders"`, `FUNCTION "order_count"`},
	},
	{
		name: "a SQL-standard body reads a table",
		sql: `CREATE TABLE orders (id bigint PRIMARY KEY, total integer NOT NULL);
CREATE FUNCTION order_total(order_id bigint) RETURNS integer LANGUAGE sql STABLE RETURN (SELECT total FROM orders WHERE id = order_id);`,
		order: []string{`CREATE TABLE "orders"`, `FUNCTION "order_total"`},
	},
	{
		name: "a signature returns a table's row type",
		sql: `CREATE TABLE orders (id bigint PRIMARY KEY, total integer NOT NULL);
CREATE FUNCTION big_orders() RETURNS SETOF orders LANGUAGE plpgsql STABLE AS $$ BEGIN RETURN QUERY SELECT * FROM orders; END $$;`,
		order: []string{`CREATE TABLE "orders"`, `FUNCTION "big_orders"`},
	},
	{
		name: "a signature returns an enum a column default calls it for",
		sql: `CREATE TYPE mood AS ENUM ('happy', 'sad');
CREATE FUNCTION default_mood() RETURNS mood LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN RETURN 'happy'; END $$;
CREATE TABLE people (id bigint PRIMARY KEY, feeling mood NOT NULL DEFAULT default_mood());`,
		order: []string{`CREATE TYPE "mood"`, `FUNCTION "default_mood"`, `CREATE TABLE "people"`},
	},
	{
		name: "a column default calls a routine that names nothing",
		sql: `CREATE FUNCTION next_code() RETURNS text LANGUAGE sql VOLATILE AS $$ SELECT md5(random()::text) $$;
CREATE TABLE codes (id bigint PRIMARY KEY, code text NOT NULL DEFAULT next_code());`,
		order: []string{`FUNCTION "next_code"`, `CREATE TABLE "codes"`},
	},
	{
		name: "a PL/pgSQL body reading a table stays first",
		sql: `CREATE TABLE orders (id bigint PRIMARY KEY, total integer NOT NULL);
CREATE FUNCTION order_count() RETURNS bigint LANGUAGE plpgsql STABLE AS $$ BEGIN RETURN (SELECT count(*) FROM orders); END $$;`,
		order: []string{`FUNCTION "order_count"`, `CREATE TABLE "orders"`},
	},
	{
		name: "a routine reads a view another view reads it through",
		sql: `CREATE TABLE orders (id bigint PRIMARY KEY, total integer NOT NULL);
CREATE VIEW big AS SELECT id, total FROM orders WHERE total > 100;
CREATE FUNCTION big_count() RETURNS bigint LANGUAGE sql STABLE AS $$ SELECT count(*) FROM big $$;
CREATE VIEW summary AS SELECT big_count() AS n;`,
		order: []string{`CREATE TABLE "orders"`, `CREATE VIEW "big"`, `FUNCTION "big_count"`, `CREATE VIEW "summary"`},
	},
	{
		name: "a policy calls a routine reading another table",
		sql: `CREATE TABLE members (user_name text PRIMARY KEY);
CREATE TABLE docs (id bigint PRIMARY KEY, owner text NOT NULL);
CREATE FUNCTION is_member(name text) RETURNS boolean LANGUAGE sql STABLE AS $$ SELECT EXISTS (SELECT 1 FROM members WHERE user_name = name) $$;
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY docs_member ON docs USING (is_member(owner));`,
		order: []string{`CREATE TABLE "members"`, `FUNCTION "is_member"`, `CREATE POLICY "docs_member"`},
	},
}

// positions is where each head first appears in sql, -1 for one that does not.
func positions(sql string, heads []string) []int {
	found := make([]int, 0, len(heads))
	for _, head := range heads {
		found = append(found, strings.Index(sql, head))
	}
	return found
}

// TestGenerateSchemaDiffSQL_CreatesRoutinesAfterWhatTheyName is the planner
// half of stokaro/ptah#3602: a plan creating these schemas from nothing emits
// each routine after the types and relations its definition names, and before
// the tables and policies that call it.
func TestGenerateSchemaDiffSQL_CreatesRoutinesAfterWhatTheyName(t *testing.T) {
	for _, test := range routineOrderCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, _, err := sqlschema.Read([]byte(test.sql), platform.Postgres)
			c.Assert(err, qt.IsNil)

			sql, err := planner.GenerateSchemaDiffSQL(schemadiff.Compare(&desired, &catalog.Database{}), platform.Postgres)

			c.Assert(err, qt.IsNil)
			found := positions(sql, test.order)
			c.Assert(found, qt.Not(qt.Contains), -1, qt.Commentf("%s", sql))
			c.Assert(slices.IsSorted(found), qt.IsTrue, qt.Commentf("%s", sql))
		})
	}
}
