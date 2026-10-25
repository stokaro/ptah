//go:build integration

package integration_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5/pgconn"

	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// reviewSteps is a list of desired states diffed one after another, named s1,
// s2, and so on.
func reviewSteps(schemas ...string) []statementStyleStep {
	steps := make([]statementStyleStep, 0, len(schemas))
	for i, schema := range schemas {
		steps = append(steps, statementStyleStep{name: "s" + string(rune('1'+i)), schema: schema})
	}
	return steps
}

// TestCompatMigrateDiffStatementStyleE2E_ReliedOnDropsApplyBothWays diffs the
// shapes where the exact style keeps CASCADE or a guard, or orders its drops
// so it needs neither, and runs every file forward, back to an empty database,
// and forward again on PostgreSQL. Without the reason the plan relies on, each
// of these stops the server at a drop: a removed routine that returns or takes
// a removed view's row, a policy of a removed table that reads a removed table
// or view in a first migration's rollback, an enum a column leaves with its
// default and the rollback that brings it back, a trigger on a dropped column,
// a generated column computed from one, an index on a materialized view a
// column type change rebuilds, and a materialized view put back under a view
// dropped with CASCADE.
func TestCompatMigrateDiffStatementStyleE2E_ReliedOnDropsApplyBothWays(t *testing.T) {
	tests := []struct {
		name  string
		steps []statementStyleStep
	}{
		{name: "a view removed with a routine that returns its rows", steps: reviewSteps(
			`CREATE TABLE t (id bigint PRIMARY KEY, a int);
CREATE VIEW v AS SELECT id, a FROM t;
CREATE FUNCTION list_v() RETURNS SETOF v LANGUAGE sql STABLE AS $$ SELECT * FROM v $$;
`, "CREATE TABLE t (id bigint PRIMARY KEY, a int);\n")},
		{name: "a view removed with a routine that takes its row", steps: reviewSteps(
			`CREATE TABLE t (id bigint PRIMARY KEY, a int);
CREATE VIEW v AS SELECT id, a FROM t;
CREATE FUNCTION a_of(r v) RETURNS int LANGUAGE sql AS $$ SELECT r.a $$;
`, "CREATE TABLE t (id bigint PRIMARY KEY, a int);\n")},
		{name: "a policy that reads a table the rollback removes", steps: reviewSteps(
			`CREATE TABLE allowed (id bigint PRIMARY KEY, who text);
CREATE TABLE t (id bigint PRIMARY KEY, a int);
ALTER TABLE t ENABLE ROW LEVEL SECURITY;
CREATE POLICY p_allowed ON t USING (EXISTS (SELECT 1 FROM allowed WHERE allowed.who = current_user));
`)},
		{name: "a policy that reads a view the rollback removes", steps: reviewSteps(
			`CREATE TABLE members (id bigint PRIMARY KEY, who text);
CREATE VIEW current_members AS SELECT id FROM members WHERE who = current_user;
CREATE TABLE docs (id bigint PRIMARY KEY, member_id bigint);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p_docs ON docs USING (member_id IN (SELECT id FROM current_members));
`)},
		{name: "a column that leaves an enum with its default", steps: reviewSteps(
			"CREATE TYPE status AS ENUM ('a', 'b');\nCREATE TABLE t (id bigint PRIMARY KEY, s status DEFAULT 'a');\n",
			"CREATE TABLE t (id bigint PRIMARY KEY, s text DEFAULT 'a');\n")},
		{name: "a column that becomes an enum with its default", steps: reviewSteps(
			"CREATE TABLE t (id bigint PRIMARY KEY, s text DEFAULT 'a');\n",
			"CREATE TYPE status AS ENUM ('a', 'b');\nCREATE TABLE t (id bigint PRIMARY KEY, s status DEFAULT 'a');\n")},
		{name: "a column dropped under a trigger the declaration removes", steps: reviewSteps(
			`CREATE FUNCTION trg() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TABLE t (id bigint PRIMARY KEY, a int, b int);
CREATE TRIGGER tr BEFORE UPDATE OF a ON t FOR EACH ROW EXECUTE FUNCTION trg();
`, `CREATE FUNCTION trg() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TABLE t (id bigint PRIMARY KEY, b int);
`)},
		{name: "a column dropped with the generated column computed from it", steps: reviewSteps(
			"CREATE TABLE t (id bigint PRIMARY KEY, a int, g int GENERATED ALWAYS AS (a * 2) STORED);\n",
			"CREATE TABLE t (id bigint PRIMARY KEY);\n")},
		{name: "an index removed from a materialized view a column type change rebuilds", steps: reviewSteps(
			"CREATE TABLE t (id bigint PRIMARY KEY, a int);\nCREATE MATERIALIZED VIEW m AS SELECT id, a FROM t;\nCREATE INDEX m_a_idx ON m (a);\n",
			"CREATE TABLE t (id bigint PRIMARY KEY, a bigint);\nCREATE MATERIALIZED VIEW m AS SELECT id, a FROM t;\n")},
		{name: "a materialized view under a view dropped with CASCADE", steps: reviewSteps(
			"CREATE TABLE t (id bigint PRIMARY KEY, a int, b int);\nCREATE VIEW v AS SELECT id, a, b FROM t;\nCREATE MATERIALIZED VIEW m AS SELECT id, a FROM v;\n",
			"CREATE TABLE t (id bigint PRIMARY KEY, a int, b int);\nCREATE VIEW v AS SELECT id, a FROM t;\nCREATE MATERIALIZED VIEW m AS SELECT id, a FROM v;\n")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
			pairs := readMigrationPairs(c, diffStatementStyleSteps(c, test.steps))
			target := statementStyleTarget(c)

			for _, pair := range pairs {
				_, err := target.ExecContext(c.Context(), pair.Up)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", pair.Up))
			}
			for _, pair := range slices.Backward(pairs) {
				_, err := target.ExecContext(c.Context(), pair.Down)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", pair.Down))
			}
			c.Assert(publicRelations(c, target), qt.HasLen, 0)
			for _, pair := range pairs {
				_, err := target.ExecContext(c.Context(), pair.Up)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", pair.Up))
			}
		})
	}
}

// TestCompatMigrateDiffStatementStyleE2E_AnUndeclaredReaderStopsTheDrop runs
// the second file of each shape against a database where a view in a schema
// the project does not declare reads what the file drops. The exact style keeps
// CASCADE only where a dependent the plan knows of outlives the drop, so the
// file stops at the drop with 2BP01 and the undeclared view is still there.
// Each shape kept CASCADE before for a reason that had nothing to do with the
// dropped object: a removed view that read the column and was dropped after
// it, a domain the plan adds, and a removed routine that reads nothing.
func TestCompatMigrateDiffStatementStyleE2E_AnUndeclaredReaderStopsTheDrop(t *testing.T) {
	tests := []struct {
		name   string
		steps  []statementStyleStep
		reader string
	}{
		{name: "a column a removed view reads", steps: reviewSteps(
			"CREATE TABLE t (id bigint PRIMARY KEY, a int, b int);\nCREATE VIEW v AS SELECT id, a FROM t;\n",
			"CREATE TABLE t (id bigint PRIMARY KEY, b int);\n"),
			reader: "CREATE VIEW reporting.copy AS SELECT id FROM public.v"},
		{name: "a column dropped beside a domain the plan adds", steps: reviewSteps(
			"CREATE TABLE t (id bigint PRIMARY KEY, a int);\n",
			"CREATE DOMAIN posint AS integer CHECK (VALUE > 0);\nCREATE TABLE t (id bigint PRIMARY KEY);\n"),
			reader: "CREATE VIEW reporting.copy AS SELECT id, a FROM public.t"},
		{name: "a column dropped beside a routine that reads nothing", steps: reviewSteps(
			"CREATE TABLE t (id bigint PRIMARY KEY, a int);\nCREATE FUNCTION unrelated() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;\n",
			"CREATE TABLE t (id bigint PRIMARY KEY);\n"),
			reader: "CREATE VIEW reporting.copy AS SELECT id, a FROM public.t"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
			pairs := readMigrationPairs(c, diffStatementStyleSteps(c, test.steps))
			target := statementStyleTarget(c)
			_, err := target.ExecContext(c.Context(), pairs[0].Up)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", pairs[0].Up))
			_, err = target.ExecContext(c.Context(), "CREATE SCHEMA reporting; "+test.reader)
			c.Assert(err, qt.IsNil)

			_, stopped := target.ExecContext(c.Context(), pairs[1].Up)

			var dependent *pgconn.PgError
			c.Assert(stopped, qt.ErrorAs, &dependent, qt.Commentf("%s", pairs[1].Up))
			c.Assert(dependent.Code, qt.Equals, "2BP01")
			var copies int
			c.Assert(target.QueryRowContext(c.Context(),
				`SELECT count(*) FROM information_schema.views WHERE table_schema = 'reporting' AND table_name = 'copy'`,
			).Scan(&copies), qt.IsNil)
			c.Assert(copies, qt.Equals, 1)
		})
	}
}
