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

// TestGetOrderedCreateStatements_CreatesRoutinesAfterTheRelationsTheyName is
// the render half of stokaro/ptah#3602. A render creates every declared
// relation, so a routine whose definition names one is ordered among the views:
// after a view it reads and before a view that calls it. The planner places
// routines by the same rule.
func TestGetOrderedCreateStatements_CreatesRoutinesAfterTheRelationsTheyName(t *testing.T) {
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
