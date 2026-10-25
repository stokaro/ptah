//go:build integration

package integration_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5/pgconn"

	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// exactSteps is a list of desired states diffed one after another, named s1,
// s2, and so on.
func exactSteps(schemas ...string) []statementStyleStep {
	steps := make([]statementStyleStep, 0, len(schemas))
	for i, schema := range schemas {
		steps = append(steps, statementStyleStep{name: fmt.Sprintf("s%d", i+1), schema: schema})
	}
	return steps
}

// applyBothWays runs every file of pairs forward, back to an empty public
// schema, and forward again on target.
func applyBothWays(c *qt.C, target *sql.DB, pairs []migrationPair) {
	c.Helper()
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
}

// TestCompatMigrateDiffStatementStyleE2E_ReliedOnDropsApplyBothWays diffs the
// shapes where the exact style keeps CASCADE or a guard, or orders its drops
// so it needs neither, and runs every file forward, back to an empty database,
// and forward again on PostgreSQL. Without the order or the reason the plan
// relies on, each of these stops the server at a statement: a removed routine
// that returns, takes or reads a removed relation's row, a policy of a removed
// table that reads a removed table or view, a column that changes type across
// an enum, an array of one or a domain over one with its default, a trigger on
// a dropped column, a generated column computed from one, an index on a
// materialized view a column type change rebuilds, and a materialized view put
// back under a view dropped with CASCADE. Two removed tables whose policies
// read each other go in one DROP TABLE, which the unit tests pin: the dev
// database cleanup cannot empty such a pair yet (stokaro/ptah#5002).
func TestCompatMigrateDiffStatementStyleE2E_ReliedOnDropsApplyBothWays(t *testing.T) {
	const status = "CREATE TYPE status AS ENUM ('a', 'b');\n"
	tests := []struct {
		name  string
		steps []statementStyleStep
	}{
		{name: "a view removed with a routine that returns its rows", steps: exactSteps(
			`CREATE TABLE t (id bigint PRIMARY KEY, a int);
CREATE VIEW v AS SELECT id, a FROM t;
CREATE FUNCTION list_v() RETURNS SETOF v LANGUAGE sql STABLE AS $$ SELECT * FROM v $$;
`, "CREATE TABLE t (id bigint PRIMARY KEY, a int);\n")},
		{name: "a view removed with a routine that takes its row", steps: exactSteps(
			`CREATE TABLE t (id bigint PRIMARY KEY, a int);
CREATE VIEW v AS SELECT id, a FROM t;
CREATE FUNCTION a_of(r v) RETURNS int LANGUAGE sql AS $$ SELECT r.a $$;
`, "CREATE TABLE t (id bigint PRIMARY KEY, a int);\n")},
		{name: "a table removed with a routine whose SQL-standard body reads it", steps: exactSteps(
			`CREATE TABLE keep (id bigint PRIMARY KEY);
CREATE TABLE t (id bigint PRIMARY KEY, a int);
CREATE FUNCTION count_t() RETURNS bigint LANGUAGE sql STABLE RETURN (SELECT count(*) FROM t);
`, "CREATE TABLE keep (id bigint PRIMARY KEY);\n")},
		{name: "a policy that reads a table the rollback removes", steps: exactSteps(
			`CREATE TABLE allowed (id bigint PRIMARY KEY, who text);
CREATE TABLE t (id bigint PRIMARY KEY, a int);
ALTER TABLE t ENABLE ROW LEVEL SECURITY;
CREATE POLICY p_allowed ON t USING (EXISTS (SELECT 1 FROM allowed WHERE allowed.who = current_user));
`)},
		{name: "a policy that reads a view the rollback removes", steps: exactSteps(
			`CREATE TABLE members (id bigint PRIMARY KEY, who text);
CREATE VIEW current_members AS SELECT id FROM members WHERE who = current_user;
CREATE TABLE docs (id bigint PRIMARY KEY, member_id bigint);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p_docs ON docs USING (member_id IN (SELECT id FROM current_members));
`)},
		{name: "a column that leaves an enum with its default", steps: exactSteps(
			status+"CREATE TABLE t (id bigint PRIMARY KEY, s status DEFAULT 'a');\n",
			"CREATE TABLE t (id bigint PRIMARY KEY, s text DEFAULT 'a');\n")},
		{name: "a column that becomes an enum with its default", steps: exactSteps(
			"CREATE TABLE t (id bigint PRIMARY KEY, s text DEFAULT 'a');\n",
			status+"CREATE TABLE t (id bigint PRIMARY KEY, s status DEFAULT 'a');\n")},
		{name: "a column that becomes another enum", steps: exactSteps(
			status+"CREATE TABLE t (id bigint PRIMARY KEY, s status DEFAULT 'a');\n",
			"CREATE TYPE grade AS ENUM ('a', 'b');\nCREATE TABLE t (id bigint PRIMARY KEY, s grade DEFAULT 'a');\n")},
		{name: "a column that leaves an array of an enum with its default", steps: exactSteps(
			status+"CREATE TABLE t (id bigint PRIMARY KEY, s status[] DEFAULT '{a}');\n",
			"CREATE TABLE t (id bigint PRIMARY KEY, s text[] DEFAULT '{a}');\n")},
		{name: "a column that leaves a domain over an enum with its default", steps: exactSteps(
			status+"CREATE DOMAIN dstatus AS status;\nCREATE TABLE t (id bigint PRIMARY KEY, s dstatus DEFAULT 'a');\n",
			"CREATE TABLE t (id bigint PRIMARY KEY, s text DEFAULT 'a');\n")},
		{name: "a column dropped under a trigger the declaration removes", steps: exactSteps(
			`CREATE FUNCTION trg() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TABLE t (id bigint PRIMARY KEY, a int, b int);
CREATE TRIGGER tr BEFORE UPDATE OF a ON t FOR EACH ROW EXECUTE FUNCTION trg();
`, `CREATE FUNCTION trg() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TABLE t (id bigint PRIMARY KEY, b int);
`)},
		{name: "a column dropped with the generated column computed from it", steps: exactSteps(
			"CREATE TABLE t (id bigint PRIMARY KEY, a int, g int GENERATED ALWAYS AS (a * 2) STORED);\n",
			"CREATE TABLE t (id bigint PRIMARY KEY);\n")},
		{name: "an index removed from a materialized view a column type change rebuilds", steps: exactSteps(
			"CREATE TABLE t (id bigint PRIMARY KEY, a int);\nCREATE MATERIALIZED VIEW m AS SELECT id, a FROM t;\nCREATE INDEX m_a_idx ON m (a);\n",
			"CREATE TABLE t (id bigint PRIMARY KEY, a bigint);\nCREATE MATERIALIZED VIEW m AS SELECT id, a FROM t;\n")},
		{name: "a materialized view under a view dropped with CASCADE", steps: exactSteps(
			"CREATE TABLE t (id bigint PRIMARY KEY, a int, b int);\nCREATE VIEW v AS SELECT id, a, b FROM t;\nCREATE MATERIALIZED VIEW m AS SELECT id, a FROM v;\n",
			"CREATE TABLE t (id bigint PRIMARY KEY, a int, b int);\nCREATE VIEW v AS SELECT id, a FROM t;\nCREATE MATERIALIZED VIEW m AS SELECT id, a FROM v;\n")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
			pairs := readMigrationPairs(c, diffStatementStyleSteps(c, test.steps))

			applyBothWays(c, statementStyleTarget(c), pairs)
		})
	}
}

// TestCompatMigrateDiffStatementStyleE2E_AnUndeclaredReaderStopsTheDrop runs
// the second file of each shape against a database where a view in a schema
// the project does not declare reads what the file drops. The exact style keeps
// CASCADE only where a dependent the plan knows of outlives the drop, so the
// file stops at the drop with 2BP01 and the undeclared view is still there.
// The plan holds a removed object beside each drop that does not hold the drop
// back: a removed view that read the column and goes first, a domain the plan
// adds or removes, a composite type it removes that names no table, a routine
// that reads nothing, and a routine that returns the dropped view's rows,
// which goes first.
func TestCompatMigrateDiffStatementStyleE2E_AnUndeclaredReaderStopsTheDrop(t *testing.T) {
	tests := []struct {
		name   string
		steps  []statementStyleStep
		reader string
	}{
		{name: "a column a removed view reads", steps: exactSteps(
			"CREATE TABLE t (id bigint PRIMARY KEY, a int, b int);\nCREATE VIEW v AS SELECT id, a FROM t;\n",
			"CREATE TABLE t (id bigint PRIMARY KEY, b int);\n"),
			reader: "CREATE VIEW reporting.copy AS SELECT id FROM public.v"},
		{name: "a column dropped beside a domain the plan adds", steps: exactSteps(
			"CREATE TABLE t (id bigint PRIMARY KEY, a int);\n",
			"CREATE DOMAIN posint AS integer CHECK (VALUE > 0);\nCREATE TABLE t (id bigint PRIMARY KEY);\n"),
			reader: "CREATE VIEW reporting.copy AS SELECT id, a FROM public.t"},
		{name: "a column dropped beside a domain the plan removes", steps: exactSteps(
			"CREATE DOMAIN posint AS integer CHECK (VALUE > 0);\nCREATE TABLE t (id bigint PRIMARY KEY, a int);\n",
			"CREATE TABLE t (id bigint PRIMARY KEY);\n"),
			reader: "CREATE VIEW reporting.copy AS SELECT id, a FROM public.t"},
		{name: "a table dropped beside a composite type the plan removes", steps: exactSteps(
			"CREATE TABLE keep (id bigint PRIMARY KEY);\nCREATE TABLE t (id bigint PRIMARY KEY, a int);\nCREATE TYPE pair AS (x int, y text);\n",
			"CREATE TABLE keep (id bigint PRIMARY KEY);\n"),
			reader: "CREATE VIEW reporting.copy AS SELECT id, a FROM public.t"},
		{name: "a column dropped beside a routine that reads nothing", steps: exactSteps(
			"CREATE TABLE t (id bigint PRIMARY KEY, a int);\nCREATE FUNCTION unrelated() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;\n",
			"CREATE TABLE t (id bigint PRIMARY KEY);\n"),
			reader: "CREATE VIEW reporting.copy AS SELECT id, a FROM public.t"},
		{name: "a view removed with a routine that returns its rows", steps: exactSteps(
			`CREATE TABLE t (id bigint PRIMARY KEY, a int);
CREATE VIEW v AS SELECT id, a FROM t;
CREATE FUNCTION list_v() RETURNS SETOF v LANGUAGE sql STABLE AS $$ SELECT * FROM v $$;
`, "CREATE TABLE t (id bigint PRIMARY KEY, a int);\n"),
			reader: "CREATE VIEW reporting.copy AS SELECT id FROM public.v"},
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

// handWrittenRoutines is a first migration someone wrote by hand: routines
// with SQL-standard bodies, which PostgreSQL keeps parsed in prosqlbody, one
// reading a column and one a view.
const handWrittenRoutines = `CREATE TABLE t (id bigint PRIMARY KEY, a int, b int);
CREATE VIEW v AS SELECT id, b FROM t;
CREATE FUNCTION count_a() RETURNS bigint LANGUAGE sql STABLE BEGIN ATOMIC SELECT count(a) FROM t; END;
CREATE FUNCTION count_v() RETURNS bigint LANGUAGE sql STABLE RETURN (SELECT count(*) FROM v);
`

// TestCompatMigrateDiffStatementStyleE2E_SQLStandardBodiesFromAHandWrittenMigration
// diffs a declaration that removes the column, the view and both routines of
// a hand-written first migration. The plan reads each body from the server:
// the routine that reads the view is dropped ahead of it, so the view drops
// without CASCADE; the one that reads the column is dropped after it, so the
// column drop relies on CASCADE. The rollback creates both routines again with
// their bodies, after the column and the view they read, and the files run
// forward, back and forward again.
func TestCompatMigrateDiffStatementStyleE2E_SQLStandardBodiesFromAHandWrittenMigration(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_s1.up.sql"), []byte(handWrittenRoutines), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_s1.down.sql"),
		[]byte("DROP FUNCTION count_v(); DROP FUNCTION count_a(); DROP VIEW v; DROP TABLE t;\n"), 0o600), qt.IsNil)
	dirURL := "file://" + filepath.ToSlash(dir) + "?format=golang-migrate"
	output, err := runCompatVerb("migrate", "hash", "--dir", dirURL)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	schema := filepath.Join(c.TempDir(), "s2.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE t (id bigint PRIMARY KEY, b int);\n"), 0o600), qt.IsNil)

	output, err = runCompatVerb("migrate", "diff", "s2", "--dir", dirURL, "--to", "file://"+filepath.ToSlash(schema),
		"--dev-url", pinnedDevURL(c, postgresScratchDevURL(c, "ptah_style_dev"), "public"), "--format", `{{ sql . "  " }}`)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	pairs := readMigrationPairs(c, dir)
	c.Assert(pairs, qt.HasLen, 2)
	c.Assert(pairs[1].Up, qt.Contains, "DROP FUNCTION \"count_v\"();\n-- drop \"v\" view\nDROP VIEW \"v\";\n")
	c.Assert(pairs[1].Up, qt.Contains, `ALTER TABLE "t" DROP COLUMN "a" CASCADE;`)
	c.Assert(pairs[1].Down, qt.Contains, "BEGIN ATOMIC\n SELECT count(t.a) AS count\n    FROM t;\nEND;")
	c.Assert(pairs[1].Down, qt.Contains, "RETURN (SELECT count(*) AS count FROM v);")
	applyBothWays(c, statementStyleTarget(c), pairs)
}

// TestCompatMigrateDiffStatementStyleE2E_RemovedRoutineComesBackWithItsProperties
// removes a SECURITY DEFINER function with a pinned search_path and other
// properties, runs the rollback, and reads the function back: it holds every
// property it had. Without them the function would run with the caller's
// search_path.
func TestCompatMigrateDiffStatementStyleE2E_RemovedRoutineComesBackWithItsProperties(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
	pairs := readMigrationPairs(c, diffStatementStyleSteps(c, exactSteps(
		"CREATE TABLE keep (id bigint PRIMARY KEY);\n"+
			"CREATE FUNCTION f(x int) RETURNS int LANGUAGE sql IMMUTABLE STRICT LEAKPROOF PARALLEL SAFE SECURITY DEFINER "+
			"SET search_path = pg_catalog AS $$ SELECT x $$;\n",
		"CREATE TABLE keep (id bigint PRIMARY KEY);\n")))
	target := statementStyleTarget(c)
	for _, statement := range []string{pairs[0].Up, pairs[1].Up, pairs[1].Down} {
		_, err := target.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}

	var strict, leakproof, definer bool
	var parallel, config string
	err := target.QueryRowContext(c.Context(), `SELECT proisstrict, proleakproof, prosecdef, proparallel::text,
		array_to_string(proconfig, ',') FROM pg_proc WHERE proname = 'f'`).Scan(&strict, &leakproof, &definer, &parallel, &config)

	c.Assert(err, qt.IsNil)
	c.Assert([]any{strict, leakproof, definer, parallel, config}, qt.DeepEquals, []any{true, true, true, "s", "search_path=pg_catalog"})
}
