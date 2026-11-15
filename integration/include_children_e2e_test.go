//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// An --include names a table, and the table's children ride along with it.
// These tests change one child of the included table at a time -- a policy's
// USING expression, a trigger's event, a grant's privileges -- and run the
// comparison and apply verbs of both shipped binaries against PostgreSQL. The
// comparison reports the change, and the apply makes it, which each test reads
// back from the catalog. An include of the other table is the control: it
// reports nothing, and an apply leaves the child as it was.
//
// internal/atlasfilter/include_children_internal_test.go holds these kinds,
// and the rest of a table's children, to the filter without a server.

const (
	includeChildTable = "public.notes"
	includeOtherTable = "public.other"
	includeNoDiffs    = "No schema differences detected.\n"
	includeSynced     = "Schemas are synced, no changes to be made.\n"
)

// includeChildBase is what every row's database and desired schema hold
// besides the row's child.
const includeChildBase = `CREATE TABLE public.notes (id int PRIMARY KEY, owner text NOT NULL);
ALTER TABLE public.notes ENABLE ROW LEVEL SECURITY;
CREATE TABLE public.other (id int PRIMARY KEY);
`

// notesPolicyUsing reads the USING expression of the policy on notes, which
// notesPolicyLive creates and notesPolicyDesired declares.
const (
	notesPolicyUsing   = "SELECT pg_get_expr(polqual, polrelid) FROM pg_policy WHERE polname = 'notes_owner'"
	notesPolicyLive    = "CREATE POLICY notes_owner ON public.notes FOR SELECT USING (true);\n"
	notesPolicyDesired = "CREATE POLICY notes_owner ON public.notes FOR SELECT USING (owner = current_user);\n"
)

// includeChildE2ERows are the children an include of notes carries. {role}
// stands for the role the test creates, and is a bare identifier.
//
// The trigger executes a function PostgreSQL ships. A function of the
// schema's own is not selected by an include of notes, so the baseline that
// ptah-compat schema apply rehearses on its dev database holds the trigger
// without the function, and the rehearsal refuses the trigger.
var includeChildE2ERows = []struct {
	name string
	// live is the child as the database holds it, desired as the schema file
	// declares it.
	live, desired string
	// reported is the statement both binaries print for the change.
	reported string
	// readBack asks the catalog for the child, and before and after are its
	// answer on the database as created and once the change is applied.
	readBack, before, after string
}{
	{
		name:     "policy",
		live:     notesPolicyLive,
		desired:  notesPolicyDesired,
		reported: `ALTER POLICY "notes_owner" ON "public"."notes" TO PUBLIC`,
		readBack: notesPolicyUsing,
		before:   "true",
		after:    "(owner = CURRENT_USER)",
	},
	{
		name:     "trigger",
		live:     "CREATE TRIGGER notes_touch BEFORE INSERT ON public.notes FOR EACH ROW EXECUTE FUNCTION suppress_redundant_updates_trigger();\n",
		desired:  "CREATE TRIGGER notes_touch BEFORE UPDATE ON public.notes FOR EACH ROW EXECUTE FUNCTION suppress_redundant_updates_trigger();\n",
		reported: `CREATE OR REPLACE TRIGGER "notes_touch" BEFORE UPDATE ON "public"."notes" FOR EACH ROW EXECUTE FUNCTION "suppress_redundant_updates_trigger"();`,
		readBack: "SELECT string_agg(event_manipulation, ',' ORDER BY event_manipulation) FROM information_schema.triggers WHERE trigger_name = 'notes_touch'",
		before:   "INSERT",
		after:    "UPDATE",
	},
	{
		name:     "grant",
		live:     "GRANT SELECT ON public.notes TO {role};\n",
		desired:  "GRANT SELECT, INSERT ON public.notes TO {role};\n",
		reported: `GRANT INSERT ON TABLE "public"."notes" TO "{role}";`,
		readBack: "SELECT string_agg(privilege_type, ',' ORDER BY privilege_type) FROM information_schema.role_table_grants " +
			"WHERE table_schema = 'public' AND table_name = 'notes' AND grantee = '{role}'",
		before: "SELECT",
		after:  "INSERT,SELECT",
	},
}

// includeChildRole creates a role of the test's own, dropped when the test
// ends. It is registered before any database is created, so it runs after
// they are dropped and nothing holds a privilege for the role any more.
func includeChildRole(c *qt.C) string {
	c.Helper()
	admin, err := sql.Open("pgx", dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	role := fmt.Sprintf("ptah_include_reader_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), "CREATE ROLE "+role+" NOLOGIN")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	return role
}

// includeChildFixture is a database of the test's own holding the base and a
// row's child as it is, and a schema file declaring the child as it should be.
type includeChildFixture struct {
	role, database, schema string
}

func newIncludeChildFixture(c *qt.C, role, live, desired string) includeChildFixture {
	c.Helper()
	fixture := includeChildFixture{role: role, database: postgresScratchDevURL(c, "ptah_include_child")}
	execPostgresFamilySQL(c, c.Context(), fixture.database, fixture.expand(includeChildBase+live))
	fixture.schema = filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(fixture.schema, []byte(fixture.expand(includeChildBase+desired)), 0o600), qt.IsNil)
	return fixture
}

func (f includeChildFixture) expand(text string) string {
	return strings.ReplaceAll(text, "{role}", f.role)
}

func (f includeChildFixture) schemaURL() string {
	return "file://" + filepath.ToSlash(f.schema)
}

func (f includeChildFixture) compare(c *qt.C, flags ...string) clirun.Result {
	c.Helper()
	return runPtahIn(c, append([]string{"schema", "compare", "--db-url", f.database, "--schema-file", f.schema, "--exit-code"}, flags...)...)
}

func (f includeChildFixture) drift(c *qt.C, flags ...string) clirun.Result {
	c.Helper()
	return runPtahIn(c, append([]string{"schema", "drift", "--db-url", f.database, "--schema-file", f.schema}, flags...)...)
}

func (f includeChildFixture) apply(c *qt.C, flags ...string) clirun.Result {
	c.Helper()
	return runPtahIn(c, append([]string{"schema", "apply", "--db-url", f.database, "--schema-file", f.schema, "--auto-approve"}, flags...)...)
}

func (f includeChildFixture) compatDiff(c *qt.C, flags ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, append([]string{"schema", "diff",
		"--from", f.database, "--to", f.schemaURL(), "--dev-url", postgresScratchDevURL(c, "ptah_include_dev")}, flags...)...)
}

func (f includeChildFixture) compatApply(c *qt.C, flags ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, append([]string{"schema", "apply",
		"-u", f.database, "--to", f.schemaURL(), "--dev-url", postgresScratchDevURL(c, "ptah_include_dev"), "--auto-approve"}, flags...)...)
}

// read runs a row's catalog query, which answers one text value.
func (f includeChildFixture) read(c *qt.C, query string) string {
	c.Helper()
	db := openReadScopeDatabase(c, f.database)
	var value string
	c.Assert(db.QueryRowContext(c.Context(), f.expand(query)).Scan(&value), qt.IsNil)
	return value
}

// TestIncludeReportsTheChangeToAChildOfTheTableE2E runs both comparison verbs
// with an include of notes: each reports the change to the child.
func TestIncludeReportsTheChangeToAChildOfTheTableE2E(t *testing.T) {
	c := qt.New(t)
	role := includeChildRole(c)
	for _, row := range includeChildE2ERows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newIncludeChildFixture(c, role, row.live, row.desired)

			compared := fixture.compare(c, "--include", includeChildTable)
			drifted := fixture.drift(c, "--include", includeChildTable)
			diffed := fixture.compatDiff(c, "--include", includeChildTable)

			c.Assert(compared.ExitCode, qt.Equals, 1, historyComment(compared))
			c.Assert(compared.Stdout, qt.Contains, fixture.expand(row.reported))
			c.Assert(drifted.ExitCode, qt.Equals, 1, historyComment(drifted))
			c.Assert(drifted.Stdout, qt.Contains, "Schema drift detected")
			c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
			c.Assert(diffed.Stdout, qt.Contains, fixture.expand(row.reported))
			c.Assert(fixture.read(c, row.readBack), qt.Equals, row.before)
		})
	}
}

// TestIncludeAppliesTheChangeToAChildOfTheTableE2E applies with an include of
// notes through each binary and reads the child back: it holds the declared
// value, and a second comparison finds nothing left to do.
func TestIncludeAppliesTheChangeToAChildOfTheTableE2E(t *testing.T) {
	c := qt.New(t)
	role := includeChildRole(c)
	for _, row := range includeChildE2ERows {
		t.Run(row.name+"/ptah", func(t *testing.T) {
			c := qt.New(t)
			fixture := newIncludeChildFixture(c, role, row.live, row.desired)

			applied := fixture.apply(c, "--include", includeChildTable)

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(applied.Stdout, qt.Contains, fixture.expand(row.reported))
			c.Assert(fixture.read(c, row.readBack), qt.Equals, row.after)
			converged := fixture.compare(c, "--include", includeChildTable)
			c.Assert(converged.ExitCode, qt.Equals, 0, historyComment(converged))
			c.Assert(comparisonBody(c, converged.Stdout), qt.Equals, includeNoDiffs)
		})
		t.Run(row.name+"/ptah-compat", func(t *testing.T) {
			c := qt.New(t)
			fixture := newIncludeChildFixture(c, role, row.live, row.desired)

			applied := fixture.compatApply(c, "--include", includeChildTable)

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(applied.Stdout, qt.Contains, fixture.expand(row.reported))
			c.Assert(fixture.read(c, row.readBack), qt.Equals, row.after)
			converged := fixture.compatDiff(c, "--include", includeChildTable)
			c.Assert(converged.ExitCode, qt.Equals, 0, historyComment(converged))
			c.Assert(converged.Stdout, qt.Equals, includeSynced)
		})
	}
}

// TestIncludeOfAnotherTableLeavesTheChildAloneE2E is the control for the two
// tests above: an include of the other table reports no change to a child of
// notes, and an apply through either binary leaves the child as it was.
func TestIncludeOfAnotherTableLeavesTheChildAloneE2E(t *testing.T) {
	c := qt.New(t)
	role := includeChildRole(c)
	for _, row := range includeChildE2ERows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newIncludeChildFixture(c, role, row.live, row.desired)

			compared := fixture.compare(c, "--include", includeOtherTable)
			diffed := fixture.compatDiff(c, "--include", includeOtherTable)
			applied := fixture.apply(c, "--include", includeOtherTable)
			compatApplied := fixture.compatApply(c, "--include", includeOtherTable)

			c.Assert(compared.ExitCode, qt.Equals, 0, historyComment(compared))
			c.Assert(comparisonBody(c, compared.Stdout), qt.Equals, includeNoDiffs)
			c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
			c.Assert(diffed.Stdout, qt.Equals, includeSynced)
			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(compatApplied.ExitCode, qt.Equals, 0, historyComment(compatApplied))
			c.Assert(fixture.read(c, row.readBack), qt.Equals, row.before)
		})
	}
}

// TestIncludeOfAChildByItsOwnNameE2E_FailurePath names the policy on notes in
// an include, with its type and without it. A policy rides along with its
// table, so the typed selector is refused, and the bare name names no object.
// Compare and apply both exit 2, and the policy stays as it was.
func TestIncludeOfAChildByItsOwnNameE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name, include string
		// stderr is what the compare writes there.
		stderr string
	}{
		{
			name:    "with its type",
			include: "notes_owner[type=policy]",
			stderr: `error: unsupported Atlas include selector "notes_owner[type=policy]": ` +
				"policy resources ride along with their parent and cannot be included on their own\n",
		},
		{
			name:    "by its name",
			include: "notes_owner",
			stderr:  `error: the --include selection matched no objects: "notes_owner"` + "\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newIncludeChildFixture(c, "", notesPolicyLive, notesPolicyDesired)

			compared := fixture.compare(c, "--include", test.include)
			applied := fixture.apply(c, "--include", test.include)

			c.Assert(compared.ExitCode, qt.Equals, 2, historyComment(compared))
			c.Assert(compared.Stderr, qt.Equals, test.stderr)
			c.Assert(applied.ExitCode, qt.Equals, 2, historyComment(applied))
			c.Assert(fixture.read(c, notesPolicyUsing), qt.Equals, "true")
		})
	}
}

// An include keeps out what it does not select, too. The database holds
// objects the schema file does not declare -- an auth function, and a policy
// on the other table -- and the policy on notes calls that function.
const (
	unmanagedLive = "CREATE SCHEMA auth;\n" +
		"CREATE FUNCTION auth.uid() RETURNS text LANGUAGE sql STABLE AS $$SELECT current_user::text$$;\n" +
		"CREATE POLICY notes_owner ON public.notes FOR SELECT USING (true);\n" +
		"ALTER TABLE public.other ENABLE ROW LEVEL SECURITY;\n" +
		"CREATE POLICY other_all ON public.other FOR SELECT USING (true);\n"
	unmanagedDesired = "CREATE POLICY notes_owner ON public.notes FOR SELECT USING (owner = auth.uid());\n"
	// unmanagedScopedDiffs is the whole comparison an include of notes reports.
	unmanagedScopedDiffs = "Differences detected (1 category):\n  tables_modified (1): public.notes\n\nReconciling SQL:\n" +
		"ALTER POLICY \"notes_owner\" ON \"public\".\"notes\" TO PUBLIC\n    USING (owner = auth.uid())\n;\n"
	// unmanagedObjects counts the auth function and the policy on other.
	unmanagedObjects = "SELECT (SELECT count(*) FROM pg_proc WHERE proname = 'uid' AND pronamespace = 'auth'::regnamespace) || ',' || " +
		"(SELECT count(*) FROM pg_policy WHERE polname = 'other_all')"
)

// TestIncludeLeavesOutTheObjectsItDoesNotSelectE2E compares with and without
// an include of notes. Without it the comparison drops the auth function and
// the policy on other, which the schema file does not declare. With it the
// comparison reports the policy on notes alone, and an apply changes that
// policy and leaves the function and the other policy in place.
func TestIncludeLeavesOutTheObjectsItDoesNotSelectE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newIncludeChildFixture(c, "", unmanagedLive, unmanagedDesired)

	unscoped := fixture.compare(c)
	compared := fixture.compare(c, "--include", includeChildTable)
	applied := fixture.apply(c, "--include", includeChildTable)
	converged := fixture.compare(c, "--include", includeChildTable)

	c.Assert(unscoped.ExitCode, qt.Equals, 1, historyComment(unscoped))
	c.Assert(unscoped.Stdout, qt.Contains, `DROP FUNCTION IF EXISTS "auth"."uid"();`)
	c.Assert(unscoped.Stdout, qt.Contains, `DROP POLICY "other_all"`)
	c.Assert(compared.ExitCode, qt.Equals, 1, historyComment(compared))
	c.Assert(comparisonBody(c, compared.Stdout), qt.Equals, unmanagedScopedDiffs)
	c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
	c.Assert(fixture.read(c, notesPolicyUsing), qt.Equals, "(owner = auth.uid())")
	c.Assert(fixture.read(c, unmanagedObjects), qt.Equals, "1,1")
	c.Assert(converged.ExitCode, qt.Equals, 0, historyComment(converged))
	c.Assert(comparisonBody(c, converged.Stdout), qt.Equals, includeNoDiffs)
}

// The exclusion path shares the filter with the include path. Two policies on
// notes change at once, so a selector that excludes one shows the other still
// reported.
const (
	excludeChildLive = "CREATE POLICY notes_owner ON public.notes FOR SELECT USING (true);\n" +
		"CREATE POLICY notes_writer ON public.notes FOR INSERT WITH CHECK (true);\n"
	excludeChildDesired = "CREATE POLICY notes_owner ON public.notes FOR SELECT USING (owner = current_user);\n" +
		"CREATE POLICY notes_writer ON public.notes FOR INSERT WITH CHECK (owner = current_user);\n"
	excludeOwnerPolicy  = `ALTER POLICY "notes_owner" ON "public"."notes" TO PUBLIC`
	excludeWriterPolicy = `ALTER POLICY "notes_writer" ON "public"."notes" TO PUBLIC`
)

// TestExcludeKeepsThePolicyChangesItDoesNotNameE2E excludes the other table, or
// one policy of notes, and runs compare, drift and a dry-run apply: each still
// reports every policy change the selector does not name.
func TestExcludeKeepsThePolicyChangesItDoesNotNameE2E(t *testing.T) {
	tests := []struct {
		name     string
		exclude  string
		reported []string
		absent   []string
		// finding is the drift finding the changes add up to.
		finding string
	}{
		{
			name:     "another table",
			exclude:  includeOtherTable,
			reported: []string{excludeOwnerPolicy, excludeWriterPolicy},
			finding:  "- feature_changes:ptah.run/pgpolicy/policy-change: 2 (warning)\n",
		},
		{
			name:     "one policy",
			exclude:  "public.notes_owner[type=policy]",
			reported: []string{excludeWriterPolicy},
			absent:   []string{excludeOwnerPolicy},
			finding:  "- feature_changes:ptah.run/pgpolicy/policy-change: 1 (warning)\n",
		},
	}
	c := qt.New(t)
	role := includeChildRole(c)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newIncludeChildFixture(c, role, excludeChildLive, excludeChildDesired)

			compared := fixture.compare(c, "--exclude", test.exclude)
			drifted := fixture.drift(c, "--exclude", test.exclude)
			planned := fixture.apply(c, "--exclude", test.exclude, "--dry-run")

			c.Assert(compared.ExitCode, qt.Equals, 1, historyComment(compared))
			c.Assert(drifted.ExitCode, qt.Equals, 1, historyComment(drifted))
			c.Assert(drifted.Stdout, qt.Contains, test.finding)
			c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
			for _, statement := range test.reported {
				c.Assert(compared.Stdout, qt.Contains, statement)
				c.Assert(planned.Stdout, qt.Contains, statement)
			}
			for _, statement := range test.absent {
				c.Assert(compared.Stdout, qt.Not(qt.Contains), statement)
				c.Assert(planned.Stdout, qt.Not(qt.Contains), statement)
			}
		})
	}
}

// TestExcludeOfTheTableTakesItsPoliciesE2E excludes notes itself: its policies
// leave with it, so no verb reports a difference.
func TestExcludeOfTheTableTakesItsPoliciesE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newIncludeChildFixture(c, includeChildRole(c), excludeChildLive, excludeChildDesired)

	compared := fixture.compare(c, "--exclude", includeChildTable)
	drifted := fixture.drift(c, "--exclude", includeChildTable)
	planned := fixture.apply(c, "--exclude", includeChildTable, "--dry-run")

	c.Assert(compared.ExitCode, qt.Equals, 0, historyComment(compared))
	c.Assert(comparisonBody(c, compared.Stdout), qt.Equals, includeNoDiffs)
	c.Assert(drifted.ExitCode, qt.Equals, 0, historyComment(drifted))
	c.Assert(drifted.Stdout, qt.Equals, "No schema drift detected.\n")
	c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
	c.Assert(planned.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
}

// policyDriftMigrations is a history whose first migration creates notes with
// its policy and the other table, and whose second, pending, indexes the other
// table.
var policyDriftMigrations = map[string]string{
	"0000000001_notes.up.sql": "CREATE TABLE notes (id int PRIMARY KEY, owner text NOT NULL);\n" +
		"ALTER TABLE notes ENABLE ROW LEVEL SECURITY;\n" +
		"CREATE POLICY notes_owner ON notes FOR SELECT USING (true);\n" +
		"CREATE TABLE other (id int PRIMARY KEY);\n",
	"0000000001_notes.down.sql":     "DROP TABLE other;\nDROP TABLE notes;\n",
	"0000000002_other_idx.up.sql":   "CREATE INDEX other_id_idx ON other (id);\n",
	"0000000002_other_idx.down.sql": "DROP INDEX other_id_idx;\n",
}

// policyDriftRun is a target at version 1 whose policy was changed out of
// band, and an empty shadow database of its own.
type policyDriftRun struct {
	migrationsDir, target, shadow string
}

func newPolicyDriftRun(c *qt.C) policyDriftRun {
	c.Helper()
	run := policyDriftRun{migrationsDir: c.TempDir(),
		target: postgresScratchDevURL(c, "ptah_policy_drift_target"), shadow: postgresScratchDevURL(c, "ptah_policy_drift_shadow")}
	for name, content := range policyDriftMigrations {
		c.Assert(os.WriteFile(filepath.Join(run.migrationsDir, name), []byte(content), 0o600), qt.IsNil)
	}
	setup := runPtahIn(c, "migrations", "up", "--db-url", run.target, "--migrations-dir", run.migrationsDir, "--to-version", "1")
	c.Assert(setup.ExitCode, qt.Equals, 0, historyComment(setup))
	execPostgresFamilySQL(c, c.Context(), run.target, "ALTER POLICY notes_owner ON notes USING (owner = current_user)")
	return run
}

// check runs the pending migration behind a drift check that leaves exclude
// out, and decodes the run document.
func (r policyDriftRun) check(c *qt.C, exclude string) (migrator.RunResult, clirun.Result) {
	c.Helper()
	got := runPtahIn(c, "migrations", "up", "--db-url", r.target, "--migrations-dir", r.migrationsDir,
		"--check-drift", "--shadow-db", r.shadow, "--drift-exclude", exclude, "--json")
	var result migrator.RunResult
	c.Assert(json.Unmarshal([]byte(got.Stdout), &result), qt.IsNil, historyComment(got))
	return result, got
}

func (r policyDriftRun) appliedVersion(c *qt.C) int {
	c.Helper()
	var version int
	query := "SELECT COALESCE(MAX(version), 0) FROM schema_migrations WHERE state = 'applied'"
	c.Assert(openReadScopeDatabase(c, r.target).QueryRowContext(c.Context(), query).Scan(&version), qt.IsNil)
	return version
}

// TestCheckDriftReportsAPolicyChangeOutsideTheDriftExcludeE2E_FailurePath
// leaves the other table out of the drift check: the changed policy on notes
// is still drift, and the pending migration does not run.
func TestCheckDriftReportsAPolicyChangeOutsideTheDriftExcludeE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	run := newPolicyDriftRun(c)

	result, got := run.check(c, includeOtherTable)

	c.Assert(got.ExitCode, qt.Equals, 1, historyComment(got))
	c.Assert(result.DriftCheck.Outcome, qt.Equals, migrator.DriftDrifted)
	c.Assert(result.DriftCheck.Findings, qt.DeepEquals, []migrator.DriftFinding{{
		Kind:    "feature_mismatch",
		Object:  "ptah.run/pgpolicy/policy public.notes.notes_owner",
		Table:   "notes",
		Message: "feature mismatch ptah.run/pgpolicy/policy public.notes.notes_owner: ptah.run/pgpolicy/policy-change changes a row-security policy",
	}})
	c.Assert(run.appliedVersion(c), qt.Equals, 1)
}

// TestCheckDriftLeavesOutThePoliciesOfAnExcludedTableE2E_HappyPath is the
// control: excluding notes takes its policy out of the check, which passes, and
// the pending migration runs.
func TestCheckDriftLeavesOutThePoliciesOfAnExcludedTableE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	run := newPolicyDriftRun(c)

	result, got := run.check(c, includeChildTable)

	c.Assert(got.ExitCode, qt.Equals, 0, historyComment(got))
	c.Assert(result.DriftCheck.Outcome, qt.Equals, migrator.DriftClean)
	c.Assert(result.DriftCheck.Findings, qt.HasLen, 0)
	c.Assert(run.appliedVersion(c), qt.Equals, 2)
}
