//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// TestSchemaApplyCockroachDBSequenceDefaultE2E_Converges declares a column default on a
// sequence. CockroachDB reads `nextval('s')` back as `nextval('public.s'::REGCLASS)`,
// so the plan set the default again on every run and never reported the schema
// synced. Each row applies the declaration, and the next run has nothing to do.
func TestSchemaApplyCockroachDBSequenceDefaultE2E_Converges(t *testing.T) {
	tests := []struct {
		name, built, desired string
		// plansFirst is a statement of the first run, and empty where it has none.
		plansFirst string
		// reads is what information_schema holds for the column afterward.
		reads string
	}{
		{
			name:    "the declared default the database already holds",
			built:   "CREATE SEQUENCE s;\nCREATE TABLE t (id INT8 PRIMARY KEY, n INT8 DEFAULT nextval('s'));\n",
			desired: "CREATE SEQUENCE s;\nCREATE TABLE t (id INT8 PRIMARY KEY, n INT8 DEFAULT nextval('s'));\n",
			reads:   "nextval('public.s'::REGCLASS)",
		},
		{
			name:       "a default moved to a new sequence",
			built:      "CREATE SEQUENCE s;\nCREATE TABLE t (id INT8 PRIMARY KEY, n INT8 DEFAULT nextval('s'));\n",
			desired:    "CREATE SEQUENCE s2;\nCREATE TABLE t (id INT8 PRIMARY KEY, n INT8 DEFAULT nextval('s2'));\n",
			plansFirst: `DROP SEQUENCE "s"`,
			reads:      "nextval('public.s2'::REGCLASS)",
		},
		{
			name:       "a default added to a column",
			built:      "CREATE SEQUENCE s;\nCREATE TABLE t (id INT8 PRIMARY KEY, n INT8);\n",
			desired:    "CREATE SEQUENCE s;\nCREATE TABLE t (id INT8 PRIMARY KEY, n INT8 DEFAULT nextval('s'));\n",
			plansFirst: `ALTER COLUMN "n" SET DEFAULT nextval('s')`,
			reads:      "nextval('public.s'::REGCLASS)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := cockroachVisibilityDatabase(c, c.Context(), test.built)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(test.desired), 0o600), qt.IsNil)

			first := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--schemas", "public", "--auto-approve")

			c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
			c.Assert(first.Stdout, qt.Contains, test.plansFirst)
			c.Assert(cockroachColumnDefault(c, target), qt.Equals, test.reads)
			again := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--schemas", "public", "--auto-approve")
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyCockroachDBSequenceDefaultE2E_ADatabaseReadWritesTheSameSpelling holds
// `db read` to the spelling a comparison reads, the one PostgreSQL's read writes.
func TestSchemaApplyCockroachDBSequenceDefaultE2E_ADatabaseReadWritesTheSameSpelling(t *testing.T) {
	c := qt.New(t)
	target, _ := cockroachVisibilityDatabase(c, c.Context(),
		"CREATE SEQUENCE s;\nCREATE TABLE t (id INT8 PRIMARY KEY, n INT8 DEFAULT nextval('s'));\n")

	read := runPtahIn(c, "db", "read", "--db-url", target, "--schemas", "public")

	c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", read.Stdout, read.Stderr))
	c.Assert(read.Stdout, qt.Contains, `DEFAULT nextval('s'::regclass)`)
}

// cockroachColumnDefault reads the default of public.t.n as the server prints it.
func cockroachColumnDefault(c *qt.C, target string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var printed string
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT column_default FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 't' AND column_name = 'n'",
	).Scan(&printed), qt.IsNil)
	return printed
}
