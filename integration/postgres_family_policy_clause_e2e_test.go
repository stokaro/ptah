//go:build integration

package integration_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// policyClauseEngine is one engine of the PostgreSQL family that holds row-level
// security. PostgreSQL and YugabyteDB answer the probe a comparison asks, and
// CockroachDB refuses it, so the comparison reads what the engine stored.
type policyClauseEngine struct {
	name   string
	engine dbtarget.Engine
}

var policyClauseEngines = []policyClauseEngine{
	{name: "postgresql", engine: dbtarget.PostgreSQL},
	{name: "cockroachdb", engine: dbtarget.CockroachDB},
	{name: "yugabytedb", engine: dbtarget.YugabyteDB},
}

// policyClauseTable is the table each policy sits on.
const policyClauseTable = "CREATE TABLE docs (id integer PRIMARY KEY, owner_name text NOT NULL, created timestamptz, n integer, tenant text);\n" +
	"ALTER TABLE docs ENABLE ROW LEVEL SECURITY;\n"

// policyClauseSchema writes a schema declaring one policy per clause, named p0,
// p1 and so on, and returns its path.
func policyClauseSchema(c *qt.C, clauses []string) string {
	c.Helper()
	var schema strings.Builder
	schema.WriteString(policyClauseTable)
	for i, clause := range clauses {
		fmt.Fprintf(&schema, "CREATE POLICY p%d ON docs USING (%s);\n", i, clause)
	}
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(schema.String()), 0o600), qt.IsNil)
	return path
}

// newPolicyClauseDatabase creates a database, dropped when the test ends, and
// returns its URL.
func newPolicyClauseDatabase(c *qt.C, engine dbtarget.Engine) string {
	c.Helper()
	adminURL := dbtarget.URL(c, engine)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Assert(admin.Close(), qt.IsNil) })
	name := fmt.Sprintf("ptah_policy_clause_%d", time.Now().UnixNano())
	createE2EDatabase(c, c.Context(), admin, name)
	c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, name) })
	return replaceDatabaseName(c, adminURL, name)
}

// storedPolicyClause reads back what the engine stores for one policy's USING.
func storedPolicyClause(c *qt.C, database, policy string) string {
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, database))
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	var clause string
	c.Assert(db.QueryRowContext(c.Context(),
		`SELECT pg_get_expr(polqual, polrelid) FROM pg_policy WHERE polname = $1`, policy).Scan(&clause), qt.IsNil)
	return clause
}

// TestPolicyClauseIsSyncedAfterItsFirstApplyE2E declares a clause in each spelling
// an author writes and applies the schema twice. The second plan has to be empty:
// an engine that rewrites what it stores must not read as a change nobody made.
// CockroachDB rewrites nearly all of these, and refuses the probe a comparison
// would ask it. It alone takes `current_user()` and `user` as written here;
// PostgreSQL answers a syntax error to the call.
func TestPolicyClauseIsSyncedAfterItsFirstApplyE2E(t *testing.T) {
	everyEngine := []string{
		"owner_name = current_user",
		"owner_name = CURRENT_USER",
		"owner_name = current_role",
		"owner_name = session_user",
		"owner_name = current_schema",
		"(owner_name = current_user)",
		"docs.owner_name = current_user",
		"owner_name = 'x'",
		"owner_name = 'x'::text",
		"owner_name = current_setting('app.who')",
		"n IN (1, 2)",
		"n <> 3",
		"n > 0 OR n IS NULL",
		"n > 0 AND n < 5 AND n <> 3",
		"n > 0 AND n < 5 OR n = 9",
		"n > 0 AND (n < 5 OR n = 9)",
		"NOT (n > 0)",
		"n BETWEEN 1 AND 5",
		"NOT n BETWEEN 1 AND 5",
		"n NOT BETWEEN 1 AND 5",
		"n BETWEEN 1 AND 5 AND n <> 3",
		"NOT (n BETWEEN 1 AND 5) AND n <> 3",
		"tenant = current_setting('app.t', true) AND owner_name = current_user",
		"lower(owner_name) = 'x'",
		"created < current_timestamp",
		"created < now()",
		"true",
	}
	tests := []struct {
		engine  policyClauseEngine
		clauses []string
	}{
		{engine: policyClauseEngines[0], clauses: everyEngine},
		{engine: policyClauseEngines[1], clauses: append([]string{"owner_name = current_user()", "owner_name = user"}, everyEngine...)},
		// Its probe is the one PostgreSQL answers, and it takes seconds a clause
		// there, so this row holds one clause of each kind of rewrite.
		{engine: policyClauseEngines[2], clauses: []string{
			"owner_name = current_user",
			"owner_name = 'x'",
			"n > 0 AND n < 5 OR n = 9",
			"NOT (n > 0)",
		}},
	}
	for _, test := range tests {
		t.Run(test.engine.name, func(t *testing.T) {
			c := qt.New(t)
			database := newPolicyClauseDatabase(c, test.engine.engine)
			schema := policyClauseSchema(c, test.clauses)

			first := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve")

			c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))

			second := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--dry-run")

			c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", second.Stdout, second.Stderr))
			c.Assert(second.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// TestPolicyClauseChangeStillPlansE2E is the control. Each policy changes to a
// clause that reads close to the one the database holds, on one database per
// engine. Every change has to plan, the server has to take it, and the next plan
// has to be empty. What is read back comes from the engine, which spells it its
// own way, so a change that was only reported would fail here.
func TestPolicyClauseChangeStillPlansE2E(t *testing.T) {
	from := []string{"owner_name = current_user", "owner_name = 'x'", "n > 0 AND n < 5", "n > 0 AND n < 5 OR n = 9"}
	to := []string{"owner_name = session_user", "owner_name = 'y'", "n > 0 OR n < 5", "n > 0 AND (n < 5 OR n = 9)"}
	postgresSpelling := []string{"(owner_name = SESSION_USER)", "(owner_name = 'y'::text)", "((n > 0) OR (n < 5))", "((n > 0) AND ((n < 5) OR (n = 9)))"}
	tests := []struct {
		engine policyClauseEngine
		want   []string
	}{
		{engine: policyClauseEngines[0], want: postgresSpelling},
		{engine: policyClauseEngines[1], want: []string{
			"owner_name = session_user()", "owner_name = 'y':::STRING", "(n > 0:::INT8) OR (n < 5:::INT8)",
			"(n > 0:::INT8) AND ((n < 5:::INT8) OR (n = 9:::INT8))",
		}},
		{engine: policyClauseEngines[2], want: postgresSpelling},
	}
	for _, test := range tests {
		t.Run(test.engine.name, func(t *testing.T) {
			c := qt.New(t)
			database := newPolicyClauseDatabase(c, test.engine.engine)
			declaring, changing := policyClauseSchema(c, from), policyClauseSchema(c, to)
			first := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", declaring, "--auto-approve")
			c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))

			planned := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", changing, "--dry-run")

			c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
			for i := range to {
				c.Assert(planned.Stdout, qt.Contains, fmt.Sprintf(`POLICY "p%d"`, i))
			}

			applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", changing, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			for i, want := range test.want {
				c.Assert(storedPolicyClause(c, database, fmt.Sprintf("p%d", i)), qt.Equals, want)
			}

			settled := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", changing, "--dry-run")

			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}
