package renderer_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/internal/sqlschema"
)

// TestGetOrderedCreateStatements_PlacesRoutinesLikeAPlan pins that a render
// places routines by the rule a migration plan uses (stokaro/ptah#3602,
// stokaro/ptah#3634). A render creates everything it declares, so a routine
// whose definition names a relation is ordered among the views, one whose
// signature names a type follows the types, and one that names nothing comes
// before the types, where a domain or a column default can call it.
func TestGetOrderedCreateStatements_PlacesRoutinesLikeAPlan(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		order []string
	}{
		{
			name: "a LANGUAGE sql body reads a table",
			sql: `CREATE TABLE orders (id bigint PRIMARY KEY, total integer NOT NULL);
CREATE FUNCTION order_count() RETURNS bigint LANGUAGE sql STABLE AS $$ SELECT count(*) FROM orders $$;`,
			order: []string{`CREATE TABLE "orders"`, `FUNCTION "order_count"`},
		},
		{
			name: "a column default calls a routine that names nothing",
			sql: `CREATE FUNCTION next_code() RETURNS text LANGUAGE sql VOLATILE AS $$ SELECT md5(random()::text) $$;
CREATE TABLE codes (id bigint PRIMARY KEY, code text NOT NULL DEFAULT next_code());`,
			order: []string{`FUNCTION "next_code"`, `CREATE TABLE "codes"`},
		},
		{
			name: "a domain CHECK calls a routine that names nothing",
			sql: `CREATE FUNCTION positive(v integer) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$ SELECT v > 0 $$;
CREATE DOMAIN pct AS integer CHECK (positive(VALUE));
CREATE TABLE items (id bigint PRIMARY KEY, share pct NOT NULL);`,
			order: []string{`FUNCTION "positive"`, `CREATE DOMAIN "pct"`, `CREATE TABLE "items"`},
		},
		{
			name: "a signature returns an enum a column default calls it for",
			sql: `CREATE TYPE mood AS ENUM ('happy', 'sad');
CREATE FUNCTION default_mood() RETURNS mood LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN RETURN 'happy'; END $$;
CREATE TABLE people (id bigint PRIMARY KEY, feeling mood NOT NULL DEFAULT default_mood());`,
			order: []string{`CREATE TYPE "mood"`, `FUNCTION "default_mood"`, `CREATE TABLE "people"`},
		},
		{
			name: "a role comes before the routines",
			sql: `CREATE ROLE app_reader NOLOGIN;
CREATE FUNCTION next_code() RETURNS text LANGUAGE sql VOLATILE AS $$ SELECT md5(random()::text) $$;`,
			order: []string{`ROLE "app_reader"`, `FUNCTION "next_code"`},
		},
		{
			name: "a column default calls a LANGUAGE sql routine reading a table declared after it",
			sql: `CREATE TABLE invoices (id bigint PRIMARY KEY, pct integer NOT NULL DEFAULT default_pct());
CREATE TABLE rates (code text PRIMARY KEY, pct integer NOT NULL);
CREATE FUNCTION default_pct() RETURNS integer LANGUAGE sql STABLE AS $$ SELECT coalesce((SELECT pct FROM rates WHERE code = 'std'), 0) $$;`,
			order: []string{`CREATE TABLE "rates"`, `FUNCTION "default_pct"`, `CREATE TABLE "invoices"`},
		},
		{
			name: "a CHECK calls a LANGUAGE sql routine reading another table",
			sql: `CREATE TABLE rates (code text PRIMARY KEY, pct integer NOT NULL);
CREATE FUNCTION has_rate(c text) RETURNS boolean LANGUAGE sql STABLE AS $$ SELECT EXISTS (SELECT 1 FROM rates WHERE code = c) $$;
CREATE TABLE invoices (id bigint PRIMARY KEY, code text NOT NULL CONSTRAINT invoices_code_rated CHECK (has_rate(code)));`,
			order: []string{`CREATE TABLE "rates"`, `FUNCTION "has_rate"`, `CREATE TABLE "invoices"`},
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

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.Read([]byte(test.sql), platform.Postgres)
			c.Assert(err, qt.IsNil)

			statements, err := renderer.GetOrderedCreateStatements(&database, platform.Postgres)

			c.Assert(err, qt.IsNil)
			sql := strings.Join(statements, "\n")
			found := make([]int, 0, len(test.order))
			for _, head := range test.order {
				found = append(found, strings.Index(sql, head))
			}
			c.Assert(found, qt.Not(qt.Contains), -1, qt.Commentf("%s", sql))
			c.Assert(slices.IsSorted(found), qt.IsTrue, qt.Commentf("%s", sql))
		})
	}
}

// TestGetOrderedCreateStatements_CreatesATableCalledRoutineOnce pins that a
// routine the render creates between the tables is not created again among the
// views.
func TestGetOrderedCreateStatements_CreatesATableCalledRoutineOnce(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.Read([]byte(`CREATE TABLE rates (code text PRIMARY KEY, pct integer NOT NULL);
CREATE FUNCTION default_pct() RETURNS integer LANGUAGE sql STABLE AS $$ SELECT coalesce((SELECT pct FROM rates WHERE code = 'std'), 0) $$;
CREATE TABLE invoices (id bigint PRIMARY KEY, pct integer NOT NULL DEFAULT default_pct());`), platform.Postgres)
	c.Assert(err, qt.IsNil)

	statements, err := renderer.GetOrderedCreateStatements(&database, platform.Postgres)

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(strings.Count(sql, `FUNCTION "default_pct"`), qt.Equals, 1, qt.Commentf("%s", sql))
}
