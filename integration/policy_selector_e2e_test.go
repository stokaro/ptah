//go:build integration

package integration_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// PostgreSQL scopes a policy's name to its table, so notes and other each
// hold a policy named p. An exclusion names a policy through its table, as it
// names an index: `notes.p[type=policy]`, or `public.notes.p[type=policy]` on
// a URL that names no schema. These tests change both policies at once and
// run the verbs of both shipped binaries that take --exclude with a selector
// that names the policy on notes. The comparison reports the change to the
// policy on other alone, and an apply makes that change and leaves the
// policy on notes as it was, which each test reads back from pg_policies.
//
// internal/atlasfilter/policy_selector_test.go holds the same selectors to
// the filter without a server.

const (
	policySelectorLive = "CREATE POLICY p ON public.notes FOR SELECT USING (true);\n" +
		"CREATE POLICY p ON public.other FOR SELECT USING (true);\n"
	policySelectorDesired = "CREATE POLICY p ON public.notes FOR SELECT USING (id > 1);\n" +
		"CREATE POLICY p ON public.other FOR SELECT USING (id > 2);\n"
	// notesPolicyP and otherPolicyP are the statements that change each
	// policy, and plannedOtherPolicyP the second as a plan document spells it.
	notesPolicyP        = `ALTER POLICY "p" ON "public"."notes" TO PUBLIC`
	otherPolicyP        = `ALTER POLICY "p" ON "public"."other" TO PUBLIC`
	plannedNotesPolicyP = `ALTER POLICY \"p\" ON \"public\".\"notes\" TO PUBLIC`
	plannedOtherPolicyP = `ALTER POLICY \"p\" ON \"public\".\"other\" TO PUBLIC`
	// policySelectorReadBack answers each table's policy p and its USING
	// expression, which policySelectorBefore is on the database as created and
	// policySelectorAfter once the change the selector leaves in is applied.
	policySelectorReadBack = "SELECT string_agg(tablename || '=' || qual, ',' ORDER BY tablename) FROM pg_policies " +
		"WHERE schemaname = 'public' AND policyname = 'p'"
	policySelectorBefore = "notes=true,other=true"
	policySelectorAfter  = "notes=true,other=(id > 2)"
)

// policySelectorRows name the policy p on notes, on a URL that names no
// schema.
var policySelectorRows = []struct {
	name, exclude string
}{
	{name: "table and policy", exclude: "notes.p[type=policy]"},
	{name: "schema, table and policy", exclude: "public.notes.p[type=policy]"},
}

func (f includeChildFixture) plan(c *qt.C, flags ...string) clirun.Result {
	c.Helper()
	return runPtahIn(c, append([]string{"schema", "plan", "--db-url", f.database, "--schema-file", f.schema}, flags...)...)
}

// compatDryRun is [includeChildFixture.compatApply] with --dry-run, which
// refuses --auto-approve beside it.
func (f includeChildFixture) compatDryRun(c *qt.C, flags ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, append([]string{"schema", "apply",
		"-u", f.database, "--to", f.schemaURL(), "--dev-url", postgresScratchDevURL(c, "ptah_include_dev"), "--dry-run"}, flags...)...)
}

// TestExcludeReportsThePolicyOfTheOtherTableE2E runs every comparison verb and
// both dry runs with the selector: each reports the change to the policy on
// other, and none the change to the policy on notes.
func TestExcludeReportsThePolicyOfTheOtherTableE2E(t *testing.T) {
	for _, row := range policySelectorRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newIncludeChildFixture(c, "", policySelectorLive, policySelectorDesired)

			compared := fixture.compare(c, "--exclude", row.exclude)
			drifted := fixture.drift(c, "--exclude", row.exclude)
			planned := fixture.plan(c, "--exclude", row.exclude, "--dry-run")
			rehearsed := fixture.apply(c, "--exclude", row.exclude, "--dry-run")
			diffed := fixture.compatDiff(c, "--exclude", row.exclude)
			compatRehearsed := fixture.compatDryRun(c, "--exclude", row.exclude)

			c.Assert(compared.ExitCode, qt.Equals, 1, historyComment(compared))
			c.Assert(compared.Stdout, qt.Contains, otherPolicyP)
			c.Assert(compared.Stdout, qt.Not(qt.Contains), notesPolicyP)
			c.Assert(compared.Stderr, qt.Equals, "")
			c.Assert(drifted.ExitCode, qt.Equals, 1, historyComment(drifted))
			c.Assert(drifted.Stdout, qt.Contains, "- feature_changes:ptah.run/pgpolicy/policy-change: 1 (warning)\n")
			c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
			c.Assert(planned.Stdout, qt.Contains, plannedOtherPolicyP)
			c.Assert(planned.Stdout, qt.Not(qt.Contains), plannedNotesPolicyP)
			c.Assert(rehearsed.ExitCode, qt.Equals, 0, historyComment(rehearsed))
			c.Assert(rehearsed.Stdout, qt.Contains, otherPolicyP)
			c.Assert(rehearsed.Stdout, qt.Not(qt.Contains), notesPolicyP)
			c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
			c.Assert(diffed.Stdout, qt.Contains, otherPolicyP)
			c.Assert(diffed.Stdout, qt.Not(qt.Contains), notesPolicyP)
			c.Assert(compatRehearsed.ExitCode, qt.Equals, 0, historyComment(compatRehearsed))
			c.Assert(compatRehearsed.Stdout, qt.Contains, otherPolicyP)
			c.Assert(compatRehearsed.Stdout, qt.Not(qt.Contains), notesPolicyP)
			c.Assert(fixture.read(c, policySelectorReadBack), qt.Equals, policySelectorBefore)
		})
	}
}

// TestExcludeAppliesThePolicyOfTheOtherTableE2E applies with the selector
// through `ptah schema apply`, a plan `ptah schema plan` saved, and
// `ptah-compat schema apply`, and reads both policies back: the policy on
// other holds the declared expression and the policy on notes its own. A
// second comparison with the selector finds nothing left to do.
func TestExcludeAppliesThePolicyOfTheOtherTableE2E(t *testing.T) {
	for _, row := range policySelectorRows {
		t.Run(row.name+"/ptah", func(t *testing.T) {
			c := qt.New(t)
			fixture := newIncludeChildFixture(c, "", policySelectorLive, policySelectorDesired)

			applied := fixture.apply(c, "--exclude", row.exclude)

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(applied.Stdout, qt.Contains, otherPolicyP)
			c.Assert(applied.Stdout, qt.Not(qt.Contains), notesPolicyP)
			c.Assert(fixture.read(c, policySelectorReadBack), qt.Equals, policySelectorAfter)
			converged := fixture.compare(c, "--exclude", row.exclude)
			c.Assert(converged.ExitCode, qt.Equals, 0, historyComment(converged))
			c.Assert(comparisonBody(c, converged.Stdout), qt.Equals, includeNoDiffs)
		})
		t.Run(row.name+"/ptah plan", func(t *testing.T) {
			c := qt.New(t)
			fixture := newIncludeChildFixture(c, "", policySelectorLive, policySelectorDesired)
			planFile := filepath.Join(c.TempDir(), "policy.plan.json")

			planned := fixture.plan(c, "--exclude", row.exclude, "--output", planFile)
			applied := runPtahIn(c, "schema", "apply", "--db-url", fixture.database, "--plan", planFile, "--auto-approve")

			c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(fixture.read(c, policySelectorReadBack), qt.Equals, policySelectorAfter)
		})
		t.Run(row.name+"/ptah-compat", func(t *testing.T) {
			c := qt.New(t)
			fixture := newIncludeChildFixture(c, "", policySelectorLive, policySelectorDesired)

			applied := fixture.compatApply(c, "--exclude", row.exclude)

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(applied.Stdout, qt.Contains, otherPolicyP)
			c.Assert(applied.Stdout, qt.Not(qt.Contains), notesPolicyP)
			c.Assert(fixture.read(c, policySelectorReadBack), qt.Equals, policySelectorAfter)
			converged := fixture.compatDiff(c, "--exclude", row.exclude)
			c.Assert(converged.ExitCode, qt.Equals, 0, historyComment(converged))
			c.Assert(converged.Stdout, qt.Equals, includeSynced)
		})
	}
}

// TestExcludeOfAPolicyThatNamesNothingE2E_FailurePath names a policy with its
// schema and no table, a table that does not exist, and a policy notes does
// not hold. None names a policy, so the comparisons report both changes and
// warn about the selector, both applies refuse it, and both policies stay as
// they were.
func TestExcludeOfAPolicyThatNamesNothingE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name, exclude string
		// warning is the line the comparisons write on standard error, and
		// refusal the line `ptah schema apply` writes there.
		warning, refusal string
	}{
		{
			name:    "schema and policy",
			exclude: "public.p[type=policy]",
			warning: `Warning: the --exclude selection matched no objects: "public.p[type=policy]".` + "\n",
			refusal: `error: the --exclude selection matched no objects: "public.p[type=policy]"; `,
		},
		{
			name:    "a table that does not exist",
			exclude: "public.missing.p[type=policy]",
			warning: `Warning: the --exclude selection matched no objects: "public.missing.p[type=policy]".` + "\n",
			refusal: `error: the --exclude selection matched no objects: "public.missing.p[type=policy]"; `,
		},
		{
			name:    "a policy the table does not hold",
			exclude: "notes.missing[type=policy]",
			warning: `Warning: the --exclude selection matched no objects: "notes.missing[type=policy]".` + "\n",
			refusal: `error: the --exclude selection matched no objects: "notes.missing[type=policy]"; `,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newIncludeChildFixture(c, "", policySelectorLive, policySelectorDesired)

			compared := fixture.compare(c, "--exclude", test.exclude)
			diffed := fixture.compatDiff(c, "--exclude", test.exclude)
			applied := fixture.apply(c, "--exclude", test.exclude)
			compatApplied := fixture.compatApply(c, "--exclude", test.exclude)

			c.Assert(compared.ExitCode, qt.Equals, 1, historyComment(compared))
			c.Assert(compared.Stdout, qt.Contains, notesPolicyP)
			c.Assert(compared.Stdout, qt.Contains, otherPolicyP)
			c.Assert(compared.Stderr, qt.Equals, test.warning)
			c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
			c.Assert(diffed.Stdout, qt.Contains, notesPolicyP)
			c.Assert(diffed.Stderr, qt.Equals, test.warning)
			c.Assert(applied.ExitCode, qt.Equals, 2, historyComment(applied))
			c.Assert(applied.Stderr, qt.Contains, test.refusal)
			c.Assert(compatApplied.ExitCode, qt.Equals, 1, historyComment(compatApplied))
			c.Assert(fixture.read(c, policySelectorReadBack), qt.Equals, policySelectorBefore)
		})
	}
}

// TestExcludeOfATableAndOneOfItsPoliciesE2E excludes notes and its policy p.
// The policy leaves with its table, and the policy selector still named it,
// so `schema apply` takes both selectors, changes the policy on other and
// leaves the policy on notes as it was.
func TestExcludeOfATableAndOneOfItsPoliciesE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newIncludeChildFixture(c, "", policySelectorLive, policySelectorDesired)

	applied := fixture.apply(c, "--exclude", includeChildTable, "--exclude", "notes.p[type=policy]")

	c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
	c.Assert(applied.Stderr, qt.Equals, "")
	c.Assert(fixture.read(c, policySelectorReadBack), qt.Equals, policySelectorAfter)
}
