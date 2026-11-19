//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A selection narrows what schema apply compares, not what the target holds.
// The statements the dev database runs to recreate the compared state, and
// the plan after them, can name an object the selection left out: a kept
// policy that calls auth.uid() with auth excluded, a planned foreign key to a
// table the selection does not hold. The target applies them. The rehearsal
// recreates those objects on the dev database too, read from the target, so
// it accepts what the target accepts; without them the dev database refuses
// with `schema "auth" does not exist`.
//
// Each row runs on both binaries, against a URL that reaches the whole
// database and one bound to public by search_path. The bound URL reads public
// and the schemas the desired state declares, so it needs the same recreation
// with no selection at all.

// scopedRehearsalCase is one target state, one desired state and the
// selection between them. {role} names a role the test creates, since a role
// belongs to the server the dev database shares with the target.
type scopedRehearsalCase struct {
	name    string
	target  string
	desired string
	args    []string
	// want is a statement the plan prints on both binaries.
	want string
	// dev is what the dev database holds before the run, written through a
	// URL that pins nothing.
	dev string
}

// scopedRehearsalNotes is the --include case: the policy of the one included
// table calls auth.uid(), and the plan adds a column to the table.
const scopedRehearsalNotes = `CREATE SCHEMA auth;
CREATE FUNCTION auth.uid() RETURNS uuid LANGUAGE sql STABLE AS $$ SELECT nullif(current_setting('request.jwt.claim.sub', true), '')::uuid $$;
CREATE TABLE public.notes (id int PRIMARY KEY, owner uuid%s);
ALTER TABLE public.notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY notes_owner ON public.notes FOR SELECT TO {role} USING (owner = auth.uid());
`

// scopedRehearsalClaims is the --include case with one more function: the
// policy calls auth.uid(), whose SQL body calls auth.verified_claims(). The
// callee's name sorts after the caller's, and PostgreSQL resolves a SQL body
// when the function is created, so the callee has to be recreated first.
const scopedRehearsalClaims = `CREATE SCHEMA auth;
CREATE FUNCTION auth.verified_claims() RETURNS jsonb LANGUAGE sql STABLE AS $$ SELECT coalesce(nullif(current_setting('request.jwt.claims', true), ''), '{}')::jsonb $$;
CREATE FUNCTION auth.uid() RETURNS uuid LANGUAGE sql STABLE AS $$ SELECT nullif(auth.verified_claims() ->> 'sub', '')::uuid $$;
CREATE TABLE public.notes (id int PRIMARY KEY, owner uuid%s);
ALTER TABLE public.notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY notes_owner ON public.notes FOR SELECT TO {role} USING (owner = auth.uid());
`

// scopedRehearsalFunctions are the functions Rebase generates into its own
// schema, which a project excludes because it does not own them.
const scopedRehearsalFunctions = `CREATE SCHEMA IF NOT EXISTS "rebase";
CREATE OR REPLACE FUNCTION rebase.uid() RETURNS text AS $$
    SELECT COALESCE(NULLIF(current_setting('app.uid', true), ''), NULLIF(current_setting('app.user_id', true), ''));
$$ LANGUAGE sql STABLE PARALLEL SAFE;
CREATE OR REPLACE FUNCTION rebase.jwt() RETURNS jsonb AS $$
    SELECT COALESCE(NULLIF(current_setting('app.jwt', true), ''), '{}')::jsonb;
$$ LANGUAGE sql STABLE PARALLEL SAFE;
CREATE OR REPLACE FUNCTION rebase.roles() RETURNS text AS $$
    SELECT COALESCE(NULLIF(current_setting('app.user_roles', true), ''), '');
$$ LANGUAGE sql STABLE PARALLEL SAFE;
`

// scopedRehearsalTasks is the Rebase document's table and policies.
const scopedRehearsalTasks = `CREATE TABLE "public"."tasks" ("id" UUID PRIMARY KEY DEFAULT gen_random_uuid(), "title" TEXT NOT NULL);
ALTER TABLE "public"."tasks" ENABLE ROW LEVEL SECURITY;
CREATE POLICY "tasks_default_admin_read" ON "public"."tasks" AS PERMISSIVE FOR SELECT TO "{role}" USING ((SELECT (rebase.uid() IS NULL) OR (string_to_array(rebase.roles(), ',') && ARRAY['admin'])));
CREATE POLICY "tasks_default_admin_write_insert" ON "public"."tasks" AS PERMISSIVE FOR INSERT TO "{role}" WITH CHECK ((SELECT (rebase.uid() IS NULL) OR (string_to_array(rebase.roles(), ',') && ARRAY['admin'])));
`

// scopedRehearsalUID is one function in a schema of its own, which the view
// and default rows reference from public. Atlas CE models a column
// default and plans these rows under --exclude; it does not model a view, and
// it refuses --include.
const scopedRehearsalUID = `CREATE SCHEMA rebase;
CREATE FUNCTION rebase.uid() RETURNS text LANGUAGE sql STABLE AS $$ SELECT current_setting('rebase.uid', true) $$;
`

// scopedRehearsalWhoami is public.tasks with columns, and a view in public
// that selects rebase.uid().
func scopedRehearsalWhoami(columns string) string {
	return "CREATE TABLE public.tasks (id int PRIMARY KEY, " + columns + ");\n" +
		"CREATE VIEW public.whoami AS SELECT rebase.uid() AS uid;\n"
}

// scopedRehearsalExcludeRebase leaves rebase and everything in it out.
var scopedRehearsalExcludeRebase = []string{"--exclude", "rebase", "--exclude", "rebase.*"}

// scopedRehearsalRebaseExcludes are the selectors a Rebase project passes.
// Only rebase and rebase.* match here; the others are unmatched, which
// PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE accepts.
var scopedRehearsalRebaseExcludes = []string{
	"--exclude", "tasks.embedding", "--exclude", "atlas_schema_revisions.*",
	"--exclude", "auth", "--exclude", "auth.*", "--exclude", "rebase", "--exclude", "rebase.*",
}

var scopedRehearsalCases = []scopedRehearsalCase{
	{
		name:    "an included table's policy calls a function in a schema the selection leaves out",
		target:  fmt.Sprintf(scopedRehearsalNotes, ""),
		desired: "CREATE ROLE {role};\n" + fmt.Sprintf(scopedRehearsalNotes, ", title text"),
		args:    []string{"--include", "public.notes"},
		want:    `ADD COLUMN "title"`,
	},
	{
		name:    "the function an included table's policy calls calls another function the selection leaves out",
		target:  fmt.Sprintf(scopedRehearsalClaims, ""),
		desired: "CREATE ROLE {role};\n" + fmt.Sprintf(scopedRehearsalClaims, ", title text"),
		args:    []string{"--include", "public.notes"},
		want:    `ADD COLUMN "title"`,
	},
	{
		name:    "a planned column default calls a function of an excluded schema",
		target:  scopedRehearsalUID + "CREATE TABLE public.tasks (id int PRIMARY KEY, title text);\n",
		desired: scopedRehearsalUID + "CREATE TABLE public.tasks (id int PRIMARY KEY, title text, owner text DEFAULT rebase.uid());\n",
		args:    scopedRehearsalExcludeRebase,
		want:    `ADD COLUMN "owner" text`,
	},
	{
		name:    "a kept column default calls a function of an excluded schema",
		target:  scopedRehearsalUID + "CREATE TABLE public.tasks (id int PRIMARY KEY, title text, owner text DEFAULT rebase.uid());\n",
		desired: scopedRehearsalUID + "CREATE TABLE public.tasks (id int PRIMARY KEY, title text, owner text DEFAULT rebase.uid(), done boolean);\n",
		args:    scopedRehearsalExcludeRebase,
		want:    `ADD COLUMN "done" boolean`,
	},
	{
		name:    "a kept view selects a function of an excluded schema",
		target:  scopedRehearsalUID + scopedRehearsalWhoami("title text"),
		desired: scopedRehearsalUID + scopedRehearsalWhoami("title text, done boolean"),
		args:    scopedRehearsalExcludeRebase,
		want:    `ADD COLUMN "done" boolean`,
	},
	{
		name:    "a kept view selects a function in a schema the selection leaves out",
		target:  scopedRehearsalUID + scopedRehearsalWhoami("title text"),
		desired: scopedRehearsalUID + scopedRehearsalWhoami("title text, done boolean"),
		args:    []string{"--include", "public.*"},
		want:    `ADD COLUMN "done" boolean`,
	},
	{
		name:    "the Rebase document with the excludes its project passes",
		target:  scopedRehearsalFunctions,
		desired: scopedRehearsalFunctions + "CREATE ROLE {role} NOLOGIN NOINHERIT;\n" + scopedRehearsalTasks,
		args:    scopedRehearsalRebaseExcludes,
		want:    `CREATE POLICY "tasks_default_admin_write_insert"`,
	},
	{
		name: "a planned foreign key and trigger name a table and a function the selection leaves out",
		target: `CREATE SCHEMA rebase;
CREATE TABLE rebase.users (id int PRIMARY KEY, org_id int);
CREATE TABLE rebase.orgs (id int PRIMARY KEY);
ALTER TABLE rebase.users ADD FOREIGN KEY (org_id) REFERENCES rebase.orgs (id);
CREATE FUNCTION rebase.touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.title := coalesce(NEW.title, ''); RETURN NEW; END $$;
CREATE TABLE public.tasks (id int PRIMARY KEY, title text);
`,
		desired: `CREATE TABLE public.tasks (id int PRIMARY KEY, title text, owner_id int REFERENCES rebase.users (id));
CREATE TRIGGER tasks_touch BEFORE INSERT ON public.tasks FOR EACH ROW EXECUTE FUNCTION rebase.touch();
`,
		args: []string{"--include", "public.tasks"},
		want: `REFERENCES "rebase"."users"`,
	},
}

// scopedRehearsalSurface is one binary and how it spells schema apply.
type scopedRehearsalSurface struct {
	name   string
	target clirun.Target
	// args are schema apply's arguments for a target, a desired file and a
	// dev database.
	args func(target, desired, dev string) []string
}

var scopedRehearsalSurfaces = []scopedRehearsalSurface{
	{
		name:   "ptah-compat",
		target: clirun.Compat,
		args: func(target, desired, dev string) []string {
			return []string{"schema", "apply", "--url", target, "--to", "file://" + filepath.ToSlash(desired), "--dev-url", dev, "--dry-run"}
		},
	},
	{
		name:   "ptah",
		target: clirun.Ptah,
		args: func(target, desired, dev string) []string {
			return []string{"schema", "apply", "--db-url", target, "--schema-file", desired, "--dev-url", dev, "--dry-run"}
		},
	},
}

// scopedRehearsalForm is a URL form: the query parameters both URLs carry.
type scopedRehearsalForm struct {
	name       string
	parameters map[string]string
}

// scopedRehearsalRealm is a URL that reaches the whole database.
var scopedRehearsalRealm = scopedRehearsalForm{name: "realm URL"}

// scopedRehearsalBound is a URL bound to public by search_path, which reads
// public alone unless the desired state declares another schema.
var scopedRehearsalBound = scopedRehearsalForm{name: "URL bound to public", parameters: map[string]string{"search_path": "public"}}

// scopedRehearsalForms are both URL forms.
var scopedRehearsalForms = []scopedRehearsalForm{scopedRehearsalRealm, scopedRehearsalBound}

// TestScopedRehearsalRecreatesWhatThePlanNeedsE2E rehearses each plan on a
// dev database. Each must pass, as the apply to the target would, and leave
// the dev database holding public alone.
func TestScopedRehearsalRecreatesWhatThePlanNeedsE2E(t *testing.T) {
	for _, tc := range scopedRehearsalCases {
		for _, surface := range scopedRehearsalSurfaces {
			for _, form := range scopedRehearsalForms {
				t.Run(tc.name+"/"+surface.name+"/"+form.name, func(t *testing.T) {
					c := qt.New(t)
					env := newScopedRehearsalEnv(c, tc, form)

					got := env.apply(c, surface, tc.args)

					c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
					c.Assert(got.Stdout, qt.Contains, tc.want)
					c.Assert(scopedRehearsalSchemas(c, env.dev), qt.Equals, "public")
				})
			}
		}
	}
}

// TestScopedRehearsalRunsTwiceThroughOneDevDatabaseE2E rehearses the same
// plan twice through one dev database. The first rehearsal creates auth beside
// the schema a bound URL pins, and its cleanup must drop it: left behind, the
// second rehearsal's CREATE SCHEMA auth is refused.
func TestScopedRehearsalRunsTwiceThroughOneDevDatabaseE2E(t *testing.T) {
	tc := scopedRehearsalCases[0]
	for _, surface := range scopedRehearsalSurfaces {
		for _, form := range scopedRehearsalForms {
			t.Run(surface.name+"/"+form.name, func(t *testing.T) {
				c := qt.New(t)
				env := newScopedRehearsalEnv(c, tc, form)

				first := env.apply(c, surface, tc.args)
				second := env.apply(c, surface, tc.args)

				c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
				c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", second.Stdout, second.Stderr))
				c.Assert(second.Stdout, qt.Contains, tc.want)
				c.Assert(scopedRehearsalSchemas(c, env.dev), qt.Equals, "public")
			})
		}
	}
}

// TestScopedRehearsalOnASharedServerRefusesTheRoutineE2E rehearses the
// --include case on a dev database whose server nothing declares disposable.
// The function the policy calls is recreated through the same guard as the
// rest of the baseline, and a routine body can act outside the dev database
// when it runs, so the rehearsal refuses it by name and leaves the target and
// the dev database as they were.
func TestScopedRehearsalOnASharedServerRefusesTheRoutineE2E(t *testing.T) {
	tc := scopedRehearsalCases[0]
	for _, surface := range scopedRehearsalSurfaces {
		for _, form := range scopedRehearsalForms {
			t.Run(surface.name+"/"+form.name, func(t *testing.T) {
				c := qt.New(t)
				env := newScopedRehearsalEnv(c, tc, form)

				got := env.applyIn(c, surface, tc.args, scopedRehearsalShared)

				c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(got.Stderr, qt.Contains, `(CREATE FUNCTION "auth"."uid"() RETURNS uuid AS $$) cannot be rehearsed`)
				c.Assert(got.Stderr, qt.Contains, "refuses CREATE routine definition")
				c.Assert(got.Stderr, qt.Contains, `need them: function "auth.uid", schema "auth"`)
				c.Assert(got.Stderr, qt.Contains, "the plan was not applied to the target database")
				c.Assert(scopedRehearsalSchemas(c, env.dev), qt.Equals, "public")
			})
		}
	}
}

// TestScopedRehearsalOnABoundURLRecreatesWhatItDoesNotReadE2E rehearses a plan
// with no selection through URLs bound to public. The bound read holds public
// alone, and the policies the plan creates call rebase.uid() and
// rebase.roles(), so the rehearsal recreates them from the target. Through a
// realm URL the same desired file plans to drop them, which the server refuses
// because the new policies use them, so the case has no realm row.
func TestScopedRehearsalOnABoundURLRecreatesWhatItDoesNotReadE2E(t *testing.T) {
	tc := scopedRehearsalCase{
		target:  scopedRehearsalFunctions + "CREATE TABLE public.tasks (id uuid PRIMARY KEY, title text);\n",
		desired: scopedRehearsalTasks,
		want:    `CREATE POLICY "tasks_default_admin_read"`,
	}
	for _, surface := range scopedRehearsalSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)
			env := newScopedRehearsalEnv(c, tc, scopedRehearsalBound)

			got := env.apply(c, surface, nil)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, tc.want)
			c.Assert(scopedRehearsalSchemas(c, env.dev), qt.Equals, "public")
		})
	}
}

// TestScopedPlanOnASharedServerLeavesTheDevDatabaseAloneE2E is the control
// for schema plan, which rehearses nothing: it reads the dev URL for the
// dialect and the schema an unqualified file loads into, and never recreates
// the target on it. So the plan for a changed policy that calls auth.uid(),
// with auth excluded, is saved through a dev database on a shared server with
// no disposable declaration, and the dev database is left empty.
func TestScopedPlanOnASharedServerLeavesTheDevDatabaseAloneE2E(t *testing.T) {
	tc := scopedRehearsalCase{
		target: fmt.Sprintf(scopedRehearsalNotes, ""),
		desired: `CREATE TABLE public.notes (id int PRIMARY KEY, owner uuid, body text);
ALTER TABLE public.notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY notes_owner ON public.notes FOR SELECT TO {role} USING (owner = auth.uid() AND body IS NOT NULL);
`,
	}
	plan := scopedRehearsalSurface{
		name:   "ptah schema plan",
		target: clirun.Ptah,
		args: func(target, desired, dev string) []string {
			output := filepath.Join(filepath.Dir(desired), "notes"+atlasschema.PlanFileSuffix)
			return []string{"schema", "plan", "--db-url", target, "--schema-file", desired, "--dev-url", dev, "--output", output}
		},
	}
	for _, form := range scopedRehearsalForms {
		t.Run(form.name, func(t *testing.T) {
			c := qt.New(t)
			env := newScopedRehearsalEnv(c, tc, form)

			got := env.applyIn(c, plan, []string{"--exclude", "auth"}, nil)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, `ALTER POLICY "notes_owner" ON "public"."notes"`)
			c.Assert(got.Stdout, qt.Contains, "USING (owner = auth.uid() AND body IS NOT NULL)")
			saved, err := os.ReadFile(filepath.Join(filepath.Dir(env.desired), "notes"+atlasschema.PlanFileSuffix))
			c.Assert(err, qt.IsNil)
			c.Assert(string(saved), qt.Contains, "ALTER POLICY")
			c.Assert(scopedRehearsalSchemas(c, env.dev), qt.Equals, "public")
			c.Assert(scopedRehearsalRelations(c, env.dev), qt.Equals, 0)
		})
	}
}

// scopedRehearsalEnv is a target database seeded with a case's target state,
// an empty dev database beside it on the same server, and the case's desired
// file.
type scopedRehearsalEnv struct {
	target, dev, desired string
}

// newScopedRehearsalEnv creates the role, the two databases and the desired
// file, and gives both URLs the parameters of form. The role is dropped last,
// once the databases that name it are gone.
func newScopedRehearsalEnv(c *qt.C, tc scopedRehearsalCase, form scopedRehearsalForm) scopedRehearsalEnv {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	suffix := time.Now().UnixNano()
	role := fmt.Sprintf("ptah_rehearsal_role_%d", suffix)
	_, err = admin.ExecContext(c.Context(), "CREATE ROLE "+quoteE2EIdent(role)+" NOLOGIN")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+quoteE2EIdent(role))
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	urls := make([]string, 0, 2)
	for _, kind := range []string{"target", "dev"} {
		name := fmt.Sprintf("ptah_rehearsal_%s_%d", kind, suffix)
		createE2EDatabase(c, c.Context(), admin, name)
		c.Cleanup(func() { dropE2EDatabase(c, context.Background(), admin, name) })
		urls = append(urls, replaceDatabaseName(c, adminURL, name))
	}
	// The target is seeded through a URL that pins nothing, whatever form the
	// command is given.
	execPostgresFamilySQL(c, c.Context(), urls[0], strings.ReplaceAll(tc.target, "{role}", role))
	if tc.dev != "" {
		execPostgresFamilySQL(c, c.Context(), urls[1], tc.dev)
	}
	desired := filepath.Join(c.TempDir(), "desired.sql")
	c.Assert(os.WriteFile(desired, []byte(strings.ReplaceAll(tc.desired, "{role}", role)), 0o600), qt.IsNil)
	env := scopedRehearsalEnv{target: urls[0], dev: urls[1], desired: desired}
	for name, value := range form.parameters {
		env.target = withParameter(c, env.target, name, value)
		env.dev = withParameter(c, env.dev, name, value)
	}
	return env
}

// scopedRehearsalDisposable is the environment the Rebase project runs with:
// its dev server declared disposable, and unmatched excludes accepted.
var scopedRehearsalDisposable = []string{"PTAH_DEV_SERVER_DISPOSABLE=1", "PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE=1"}

// scopedRehearsalShared is the same environment on a server nothing declares
// disposable, which the dev database shares with other databases.
var scopedRehearsalShared = []string{"PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE=1"}

// apply runs schema apply on surface with the disposable environment.
func (e scopedRehearsalEnv) apply(c *qt.C, surface scopedRehearsalSurface, selectors []string) clirun.Result {
	c.Helper()
	return e.applyIn(c, surface, selectors, scopedRehearsalDisposable)
}

// applyIn runs schema apply on surface with variables added to the
// environment, from which any PTAH_DEV_SERVER_DISPOSABLE is taken out first.
func (e scopedRehearsalEnv) applyIn(c *qt.C, surface scopedRehearsalSurface, selectors, variables []string) clirun.Result {
	c.Helper()
	env := make([]string, 0, len(os.Environ())+len(variables))
	for _, variable := range os.Environ() {
		if !strings.HasPrefix(variable, "PTAH_DEV_SERVER_DISPOSABLE=") {
			env = append(env, variable)
		}
	}
	args := append(surface.args(e.target, e.desired, e.dev), selectors...)
	return clirun.Run(c, surface.target, clirun.Options{Dir: c.TempDir(), Env: append(env, variables...)}, args...)
}

// scopedRehearsalSchemas lists the user schemas of database, comma-separated
// in name order.
func scopedRehearsalSchemas(c *qt.C, database string) string {
	c.Helper()
	var schemas string
	err := openReadScopeDatabase(c, database).QueryRowContext(c.Context(), `
		SELECT COALESCE(string_agg(nspname, ',' ORDER BY nspname), '')
		FROM pg_namespace
		WHERE nspname NOT LIKE 'pg\_%' AND nspname <> 'information_schema'`).Scan(&schemas)
	c.Assert(err, qt.IsNil)
	return schemas
}

// scopedRehearsalRelations counts the relations in the public schema of
// database.
func scopedRehearsalRelations(c *qt.C, database string) int {
	c.Helper()
	var relations int
	err := openReadScopeDatabase(c, database).QueryRowContext(c.Context(), `
		SELECT count(*)
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public'`).Scan(&relations)
	c.Assert(err, qt.IsNil)
	return relations
}

// scopedRehearsalColumns lists the columns of public.tasks in database,
// comma-separated in their order.
func scopedRehearsalColumns(c *qt.C, database string) string {
	c.Helper()
	var columns string
	err := openReadScopeDatabase(c, database).QueryRowContext(c.Context(), `
		SELECT COALESCE(string_agg(column_name, ',' ORDER BY ordinal_position), '')
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'tasks'`).Scan(&columns)
	c.Assert(err, qt.IsNil)
	return columns
}
