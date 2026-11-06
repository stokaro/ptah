//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

const (
	// retypedPolicyBuilt is a table whose policy reads the column the declarations retype.
	retypedPolicyBuilt = "CREATE TABLE t (id int PRIMARY KEY, owner int);\n" +
		"ALTER TABLE t ENABLE ROW LEVEL SECURITY;\n"
	// retypedPolicyKept is the table the declarations keep, with the column retyped.
	retypedPolicyKept = "CREATE TABLE t (id int PRIMARY KEY, owner text);\n" +
		"ALTER TABLE t ENABLE ROW LEVEL SECURITY;\n"
	// retypedPolicyRead prints the policies of public.t and the type of its column
	// owner as the server does.
	retypedPolicyRead = "SELECT coalesce((SELECT string_agg(policyname, ',' ORDER BY policyname) FROM pg_policies " +
		"WHERE schemaname = 'public' AND tablename = 't'), '') || ' | ' || " +
		"(SELECT data_type FROM information_schema.columns " +
		"WHERE table_schema = 'public' AND table_name = 't' AND column_name = 'owner')"
)

// TestSchemaApplyRemovedPolicyOverRetypedColumnE2E_HappyPath removes a policy that
// reads a column the same plan retypes. The plan dropped the policy after the column
// change, and the server stopped it at `cannot alter type of a column used in a policy
// definition`. Each row reads the policies and the type back, and a second run finds
// nothing to do.
func TestSchemaApplyRemovedPolicyOverRetypedColumnE2E_HappyPath(t *testing.T) {
	tests := []struct{ name, built, desired, want string }{
		{
			name:    "a permissive policy",
			built:   retypedPolicyBuilt + "CREATE POLICY p ON t USING (owner = 1);\n",
			desired: retypedPolicyKept,
			want:    " | text",
		},
		{
			name:    "a restrictive policy",
			built:   retypedPolicyBuilt + "CREATE POLICY p ON t AS RESTRICTIVE USING (owner = 1);\n",
			desired: retypedPolicyKept,
			want:    " | text",
		},
		{
			name:    "a permissive policy and its replacement",
			built:   retypedPolicyBuilt + "CREATE POLICY p ON t USING (owner = 1);\n",
			desired: retypedPolicyKept + "CREATE POLICY q ON t USING (id = 1);\n",
			want:    "q | text",
		},
		{
			name:    "the replacement reads the retyped column",
			built:   retypedPolicyBuilt + "CREATE POLICY p ON t USING (owner = 1);\n",
			desired: retypedPolicyKept + "CREATE POLICY q ON t USING (owner = 'a');\n",
			want:    "q | text",
		},
		{
			name:    "the table leaves row security",
			built:   retypedPolicyBuilt + "CREATE POLICY p ON t USING (owner = 1);\n",
			desired: "CREATE TABLE t (id int PRIMARY KEY, owner text);\n",
			want:    " | text",
		},
		{
			name:    "the policy reads another column, control",
			built:   retypedPolicyBuilt + "CREATE POLICY p ON t USING (id = 1);\n",
			desired: retypedPolicyKept,
			want:    " | text",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := databaseBuiltFrom(c, test.built)

			run := keyedApply(c, target, test.desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(columnKeyString(c, target, retypedPolicyRead), qt.Equals, test.want)
			again := keyedApply(c, target, test.desired)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyRemovedPolicyOverRetypedColumnE2E_CockroachDB removes the policy on
// CockroachDB, whose planner keeps a surviving table's features apart from the plan
// graph. The server answers the column change with `cannot alter type of column "owner"
// because it is referenced in a policy expression`.
func TestSchemaApplyRemovedPolicyOverRetypedColumnE2E_CockroachDB(t *testing.T) {
	c := qt.New(t)
	target, _ := cockroachVisibilityDatabase(c, c.Context(), retypedPolicyBuilt+"CREATE POLICY p ON t USING (owner = 1);\n")

	run := keyedApply(c, target, retypedPolicyKept)

	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(columnKeyString(c, target, retypedPolicyRead), qt.Equals, " | text")
	again := keyedApply(c, target, retypedPolicyKept)
	c.Assert(again.Stdout, qt.Contains, "Schema is synced")
}

// TestMigrationsRemovedPolicyOverRetypedColumnE2E_TheRollbackRestoresThePolicy rolls the
// plan back: the column takes its type back ahead of the policy that compares it.
func TestMigrationsRemovedPolicyOverRetypedColumnE2E_TheRollbackRestoresThePolicy(t *testing.T) {
	c := qt.New(t)
	built := retypedPolicyBuilt + "CREATE POLICY p ON t USING (owner = 1);\n"
	target := databaseBuiltFrom(c, built)
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE t (id int PRIMARY KEY, owner bigint);\nALTER TABLE t ENABLE ROW LEVEL SECURITY;\n"), 0o600), qt.IsNil)

	generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", schema, "--migrations-dir", dir, "--name", "retype")
	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(columnKeyString(c, target, retypedPolicyRead), qt.Equals, " | bigint")

	down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
	c.Assert(columnKeyString(c, target, retypedPolicyRead), qt.Equals, "p | integer")
	again := keyedApply(c, target, built)
	c.Assert(again.Stdout, qt.Contains, "Schema is synced")
}
