//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
)

// exclusionDropRows remove an exclusion constraint and a column it reads. The
// server drops the constraint with the last column it covers, so a plan that
// named the constraint after the column stopped at `constraint "t_ex" of
// relation "t" does not exist`.
var exclusionDropRows = []struct {
	name, built, desired string
	// remaining is the constraints the table holds afterward, one name each.
	remaining []string
	// columns counts the columns the table holds afterward.
	columns int
}{
	{
		name: "the one column it covers",
		built: "CREATE TABLE t (id bigint PRIMARY KEY, r int4range, b int, CONSTRAINT t_ex EXCLUDE USING gist (r WITH &&));\n" +
			"INSERT INTO t VALUES (1, '[1,2)', 3);\n",
		desired:   "CREATE TABLE t (id bigint PRIMARY KEY, b int);\n",
		remaining: []string{"t_pkey"},
		columns:   2,
	},
	{
		name: "one of two columns it covers",
		built: "CREATE TABLE t (id bigint PRIMARY KEY, r int4range, s int4range, b int, CONSTRAINT t_ex EXCLUDE USING gist (r WITH &&, s WITH &&));\n" +
			"INSERT INTO t VALUES (1, '[1,2)', '[1,2)', 3);\n",
		desired:   "CREATE TABLE t (id bigint PRIMARY KEY, s int4range, b int);\n",
		remaining: []string{"t_pkey"},
		columns:   3,
	},
	{
		name: "a column only its predicate reads",
		built: "CREATE TABLE t (id bigint PRIMARY KEY, r int4range, active boolean, b int, CONSTRAINT t_ex EXCLUDE USING gist (r WITH &&) WHERE (active));\n" +
			"INSERT INTO t VALUES (1, '[1,2)', true, 3);\n",
		desired:   "CREATE TABLE t (id bigint PRIMARY KEY, r int4range, b int);\n",
		remaining: []string{"t_pkey"},
		columns:   3,
	},
	{
		name: "the constraint renamed over a new column",
		built: "CREATE TABLE t (id bigint PRIMARY KEY, r int4range, b int, CONSTRAINT t_ex EXCLUDE USING gist (r WITH &&));\n" +
			"INSERT INTO t VALUES (1, '[1,2)', 3);\n",
		desired:   "CREATE TABLE t (id bigint PRIMARY KEY, s int4range, b int, CONSTRAINT t_ex2 EXCLUDE USING gist (s WITH &&));\n",
		remaining: []string{"t_ex2", "t_pkey"},
		columns:   3,
	},
}

// exclusionDropConstraints lists the constraints of public.t other than NOT
// NULL, which PostgreSQL 18 catalogs as constraints too, in name order.
func exclusionDropConstraints(c *qt.C, target string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var names string
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT string_agg(conname, ',' ORDER BY conname) FROM pg_constraint WHERE conrelid = 'public.t'::regclass AND contype <> 'n'",
	).Scan(&names), qt.IsNil)
	return strings.Split(names, ",")
}

// exclusionDropApplyAgain runs `schema apply` of desired over the database at
// target a second time.
func exclusionDropApplyAgain(c *qt.C, target, desired string) clirun.Result {
	c.Helper()
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(desired), 0o600), qt.IsNil)
	return runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--schemas", "public", "--auto-approve")
}

// TestSchemaApplyExclusionDropE2E_HappyPath reads the constraints, the columns
// and a value back from the server, and a second run finding nothing proves the
// plan ended in the declared state.
func TestSchemaApplyExclusionDropE2E_HappyPath(t *testing.T) {
	for _, test := range exclusionDropRows {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			target, run := runExactApply(c, test.built, test.desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(exclusionDropConstraints(c, target), qt.DeepEquals, test.remaining)
			c.Assert(readInt(c, target, "SELECT count(*) FROM pg_attribute WHERE attrelid = 'public.t'::regclass AND attnum > 0 AND NOT attisdropped"), qt.Equals, test.columns)
			c.Assert(readInt(c, target, "SELECT b FROM t WHERE id = 1"), qt.Equals, 3)
			again := exclusionDropApplyAgain(c, target, test.desired)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyExclusionDropE2E_AnUndeclaredReaderKeepsTheConstraint gives the
// removed column a view in a schema the run leaves out. The plan drops the
// constraint first, the server refuses the column drop and names the view, and
// the plan's one transaction takes the constraint back.
func TestSchemaApplyExclusionDropE2E_AnUndeclaredReaderKeepsTheConstraint(t *testing.T) {
	c := qt.New(t)

	target, run := runExactApply(c,
		"CREATE TABLE t (id bigint PRIMARY KEY, r int4range, b int, CONSTRAINT t_ex EXCLUDE USING gist (r WITH &&));\n"+
			"CREATE SCHEMA reporting;\nCREATE VIEW reporting.v AS SELECT id, r FROM public.t;\n",
		"CREATE TABLE t (id bigint PRIMARY KEY, b int);\n")

	c.Assert(run.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(run.Stdout, qt.Contains, "ALTER TABLE \"t\" DROP CONSTRAINT \"t_ex\";\nALTER TABLE \"t\" DROP COLUMN \"r\";")
	c.Assert(run.Stderr, qt.Contains, "cannot drop column r of table t because other objects depend on it")
	c.Assert(exclusionDropConstraints(c, target), qt.DeepEquals, []string{"t_ex", "t_pkey"})
}
