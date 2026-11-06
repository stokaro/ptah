//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

const (
	// policyRebuildBuilt is a table whose policy reads the column the declarations retype.
	policyRebuildBuilt = "CREATE TABLE t (id int PRIMARY KEY, owner int, n int);\n" +
		"ALTER TABLE t ENABLE ROW LEVEL SECURITY;\n"
	// policyRebuildKept is the table the declarations hold, with the column retyped.
	policyRebuildKept = "CREATE TABLE t (id int PRIMARY KEY, owner bigint, n int);\n" +
		"ALTER TABLE t ENABLE ROW LEVEL SECURITY;\n"
	// policyRebuildRead prints the policies of public with their expressions, their
	// kind and their comments, and the type of column owner, as the server does.
	policyRebuildRead = "SELECT coalesce((SELECT string_agg(polname || ' ' || polrelid::regclass::text || ' ' || " +
		"CASE WHEN polpermissive THEN 'permissive' ELSE 'restrictive' END || ' ' || coalesce(pg_get_expr(polqual, polrelid), '') || " +
		"coalesce(' ' || obj_description(oid, 'pg_policy'), ''), '; ' ORDER BY polname) FROM pg_policy), '') || ' | ' || " +
		"(SELECT data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 't' AND column_name = 'owner')"
	// policyRebuildIdentity prints the catalog identity of the policy p.
	policyRebuildIdentity = "SELECT oid::text FROM pg_policy WHERE polname = 'p'"
)

// TestSchemaApplyPolicyOverRetypedColumnE2E_HappyPath retypes a column a policy names,
// with the policy kept, changed or swapped. The server refuses the column change while
// the policy stands, and the plan stopped there. Each row reads the policies and the type
// back, and a second run finds nothing to do.
func TestSchemaApplyPolicyOverRetypedColumnE2E_HappyPath(t *testing.T) {
	tests := []struct{ name, built, desired, want string }{
		{
			name:    "a policy that stays",
			built:   policyRebuildBuilt + "CREATE POLICY p ON t USING (owner = 1);\n",
			desired: policyRebuildKept + "CREATE POLICY p ON t USING (owner = 1);\n",
			want:    "p t permissive (owner = 1) | bigint",
		},
		{
			name:    "a restrictive policy that stays",
			built:   policyRebuildBuilt + "CREATE POLICY p ON t AS RESTRICTIVE USING (owner = 1);\n",
			desired: policyRebuildKept + "CREATE POLICY p ON t AS RESTRICTIVE USING (owner = 1);\n",
			want:    "p t restrictive (owner = 1) | bigint",
		},
		{
			name:    "a policy with a comment",
			built:   policyRebuildBuilt + "CREATE POLICY p ON t USING (owner = 1);\nCOMMENT ON POLICY p ON t IS 'tenants';\n",
			desired: policyRebuildKept + "CREATE POLICY p ON t USING (owner = 1);\nCOMMENT ON POLICY p ON t IS 'tenants';\n",
			want:    "p t permissive (owner = 1) tenants | bigint",
		},
		{
			name:    "a policy changed in place",
			built:   policyRebuildBuilt + "CREATE POLICY p ON t USING (owner = 1);\n",
			desired: policyRebuildKept + "CREATE POLICY p ON t USING (owner = 2);\n",
			want:    "p t permissive (owner = 2) | bigint",
		},
		{
			name:    "a policy that stops reading the column",
			built:   policyRebuildBuilt + "CREATE POLICY p ON t USING (owner = 1);\n",
			desired: policyRebuildKept + "CREATE POLICY p ON t USING (n = 1);\n",
			want:    "p t permissive (n = 1) | bigint",
		},
		{
			name:    "a restrictive policy swapped for another",
			built:   policyRebuildBuilt + "CREATE POLICY p ON t AS RESTRICTIVE USING (owner = 1);\n",
			desired: policyRebuildKept + "CREATE POLICY q ON t AS RESTRICTIVE USING (n = 1);\n",
			want:    "q t restrictive (n = 1) | bigint",
		},
		{
			name: "a policy on a table that reads the column",
			built: policyRebuildBuilt + "CREATE TABLE r (id int PRIMARY KEY);\nALTER TABLE r ENABLE ROW LEVEL SECURITY;\n" +
				"CREATE POLICY p ON r USING (EXISTS (SELECT 1 FROM t WHERE t.owner = r.id));\n",
			desired: policyRebuildKept + "CREATE TABLE r (id int PRIMARY KEY);\nALTER TABLE r ENABLE ROW LEVEL SECURITY;\n" +
				"CREATE POLICY p ON r USING (EXISTS (SELECT 1 FROM t WHERE t.owner = r.id));\n",
			want: "p r permissive (EXISTS ( SELECT 1\n   FROM t\n  WHERE (t.owner = r.id))) | bigint",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := databaseBuiltFrom(c, test.built)

			run := keyedApply(c, target, test.desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(columnKeyString(c, target, policyRebuildRead), qt.Equals, test.want)
			again := keyedApply(c, target, test.desired)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyPolicyOverRetypedColumnE2E_APolicyOnAnotherColumnStays is the control: a
// policy that names another column is not rebuilt, so it keeps its catalog identity.
func TestSchemaApplyPolicyOverRetypedColumnE2E_APolicyOnAnotherColumnStays(t *testing.T) {
	c := qt.New(t)
	target := databaseBuiltFrom(c, policyRebuildBuilt+"CREATE POLICY p ON t USING (n = 1);\n")
	before := columnKeyString(c, target, policyRebuildIdentity)

	run := keyedApply(c, target, policyRebuildKept+"CREATE POLICY p ON t USING (n = 1);\n")

	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(columnKeyString(c, target, policyRebuildIdentity), qt.Equals, before)
	c.Assert(columnKeyString(c, target, policyRebuildRead), qt.Equals, "p t permissive (n = 1) | bigint")
}

// TestSchemaApplyPolicyOverRetypedColumnE2E_WithoutATransaction runs the rebuild with no
// transaction around it. It runs as a view rebuild does: the plan is not refused, and
// each statement commits on its own.
func TestSchemaApplyPolicyOverRetypedColumnE2E_WithoutATransaction(t *testing.T) {
	c := qt.New(t)
	target := databaseBuiltFrom(c, policyRebuildBuilt+"CREATE POLICY p ON t USING (owner = 1);\n")
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(policyRebuildKept+"CREATE POLICY p ON t USING (owner = 1);\n"), 0o600), qt.IsNil)

	run := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve", "--tx-mode", "none")

	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(columnKeyString(c, target, policyRebuildRead), qt.Equals, "p t permissive (owner = 1) | bigint")
}

// TestSchemaApplyPolicyOverRetypedColumnE2E_CockroachDB rebuilds the policy on CockroachDB,
// whose planner keeps a surviving table's features apart from the plan graph. The server
// answers the column change with `cannot alter type of column "owner" because it is
// referenced in a policy expression`.
func TestSchemaApplyPolicyOverRetypedColumnE2E_CockroachDB(t *testing.T) {
	const (
		built   = "CREATE TABLE t (id INT8 PRIMARY KEY, owner INT8);\nALTER TABLE t ENABLE ROW LEVEL SECURITY;\nCREATE POLICY p ON t USING (owner = 1);\n"
		desired = "CREATE TABLE t (id INT8 PRIMARY KEY, owner text);\nALTER TABLE t ENABLE ROW LEVEL SECURITY;\nCREATE POLICY p ON t USING (owner = 'a');\n"
	)
	c := qt.New(t)
	target, _ := cockroachVisibilityDatabase(c, c.Context(), built)

	run := keyedApply(c, target, desired)

	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(columnKeyString(c, target, "SELECT count(*)::text FROM pg_policy WHERE polname = 'p'"), qt.Equals, "1")
	c.Assert(columnKeyString(c, target, "SELECT data_type FROM information_schema.columns WHERE table_name = 't' AND column_name = 'owner'"), qt.Equals, "text")
	again := keyedApply(c, target, desired)
	c.Assert(again.Stdout, qt.Contains, "Schema is synced")
}

// TestMigrationsPolicyOverRetypedColumnE2E_TheRollbackRebuildsThePolicy rolls the plan back:
// the column takes its type back under a policy that stays, so the rollback rebuilds it too.
func TestMigrationsPolicyOverRetypedColumnE2E_TheRollbackRebuildsThePolicy(t *testing.T) {
	c := qt.New(t)
	target := databaseBuiltFrom(c, policyRebuildBuilt+"CREATE POLICY p ON t USING (owner = 1);\n")
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(policyRebuildKept+"CREATE POLICY p ON t USING (owner = 1);\n"), 0o600), qt.IsNil)

	generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", schema, "--migrations-dir", dir, "--name", "retype")
	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(columnKeyString(c, target, policyRebuildRead), qt.Equals, "p t permissive (owner = 1) | bigint")

	down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
	c.Assert(columnKeyString(c, target, policyRebuildRead), qt.Equals, "p t permissive (owner = 1) | integer")
}
