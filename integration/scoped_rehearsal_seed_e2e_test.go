//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// The rehearsal recreates the objects the planned statements name from
// outside the compared scope. These rows hold it to what it writes and where:
// never into a schema the dev database held before the run, which no reset
// removes; the bodies of the routines a plan creates read as the routines of
// the target are; a failure of the plan names what was recreated; and a bare
// word that is only a column recreates nothing.

// scopedRehearsalEnvironments are the server declarations a rehearsal runs
// under: shared, and declared disposable.
var scopedRehearsalEnvironments = []struct {
	name      string
	variables []string
}{
	{name: "shared server", variables: scopedRehearsalShared},
	{name: "disposable server", variables: scopedRehearsalDisposable},
}

// TestScopedRehearsalRefusesASchemaTheDevDatabaseHeldE2E rehearses through a
// URL bound to public on a dev database that holds an auth schema of its own.
// Every reset leaves that schema as the claim found it, so the functions the
// policy needs would outlive the run there, and in the second row the first of
// them would be written before the dev database's own auth.uid() fails the
// batch. The rehearsal refuses before it writes anything, on every server,
// twice, and auth holds what it held.
func TestScopedRehearsalRefusesASchemaTheDevDatabaseHeldE2E(t *testing.T) {
	tests := []struct {
		tc   scopedRehearsalCase
		want string
	}{
		{tc: scopedRehearsalCase{
			name: "a table beside the schema", target: scopedRehearsalCases[0].target, desired: scopedRehearsalCases[0].desired,
			args: scopedRehearsalCases[0].args, dev: "CREATE SCHEMA auth;\nCREATE TABLE auth.bystander (id int);\n",
		}, want: `would outlive the run: function "auth.uid";`},
		{tc: scopedRehearsalCase{
			name: "a view and a function of the same name", target: scopedRehearsalCases[1].target, desired: scopedRehearsalCases[1].desired,
			args: scopedRehearsalCases[1].args,
			dev: "CREATE SCHEMA auth;\nCREATE VIEW auth.bystander AS SELECT 1 AS id;\n" +
				"CREATE FUNCTION auth.uid() RETURNS uuid LANGUAGE sql AS $$ SELECT NULL::uuid $$;\n",
		}, want: `would outlive the run: function "auth.uid", function "auth.verified_claims";`},
	}
	for _, test := range tests {
		for _, surface := range scopedRehearsalSurfaces {
			for _, environment := range scopedRehearsalEnvironments {
				t.Run(test.tc.name+"/"+surface.name+"/"+environment.name, func(t *testing.T) {
					c := qt.New(t)
					env := newScopedRehearsalEnv(c, test.tc, scopedRehearsalBound)
					held := scopedRehearsalAuthObjects(c, env.dev)

					first := env.applyIn(c, surface, test.tc.args, environment.variables)
					second := env.applyIn(c, surface, test.tc.args, environment.variables)

					for _, got := range []clirun.Result{first, second} {
						c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
						c.Assert(got.Stderr, qt.Contains, `failed during baseline: the dev database held schema "auth" before the run`)
						c.Assert(got.Stderr, qt.Contains, test.want)
					}
					c.Assert(scopedRehearsalAuthObjects(c, env.dev), qt.Equals, held)
					c.Assert(scopedRehearsalSchemas(c, env.dev), qt.Equals, "auth,public")
				})
			}
		}
	}
}

// TestScopedRehearsalClaimRefusesARealmTheDevDatabaseHoldsE2E is the control
// for a URL that pins nothing: the claim judges every schema, and refuses the
// dev database before the rehearsal reaches it.
func TestScopedRehearsalClaimRefusesARealmTheDevDatabaseHoldsE2E(t *testing.T) {
	tc := scopedRehearsalCases[0]
	tc.dev = "CREATE SCHEMA auth;\nCREATE TABLE auth.bystander (id int);\n"
	for _, surface := range scopedRehearsalSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)
			env := newScopedRehearsalEnv(c, tc, scopedRehearsalRealm)

			got := env.apply(c, surface, tc.args)

			c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, "connected database is not clean")
			c.Assert(scopedRehearsalAuthObjects(c, env.dev), qt.Equals, "bystander")
		})
	}
}

// scopedRehearsalAuthUID is a target whose auth schema holds uid(), beside a
// table in public.
const scopedRehearsalAuthUID = `CREATE SCHEMA auth;
CREATE FUNCTION auth.uid() RETURNS uuid LANGUAGE sql STABLE AS $$ SELECT NULL::uuid $$;
CREATE TABLE public.t (id int PRIMARY KEY);
`

// TestScopedRehearsalReadsTheBodiesThePlanCreatesE2E rehearses a plan that
// creates a routine in public whose body calls auth.uid(), with auth left out.
// PostgreSQL resolves an SQL body when the routine is created, so the dev
// database refuses the plan unless the rehearsal recreated auth.uid() first;
// it reads the body however the routine spells it.
func TestScopedRehearsalReadsTheBodiesThePlanCreatesE2E(t *testing.T) {
	routines := []struct{ name, routine string }{
		{name: "an SQL body", routine: `CREATE FUNCTION public.whoami() RETURNS uuid LANGUAGE sql STABLE AS $$ SELECT auth.uid() $$;`},
		{name: "CREATE OR REPLACE", routine: `CREATE OR REPLACE FUNCTION public.whoami() RETURNS uuid LANGUAGE sql STABLE AS $$ SELECT auth.uid() $$;`},
		{name: "a procedure", routine: `CREATE PROCEDURE public.touch() LANGUAGE sql AS $$ SELECT auth.uid() $$;`},
		{name: "a body quoted as a string", routine: `CREATE FUNCTION public.whoami() RETURNS uuid LANGUAGE sql STABLE AS 'SELECT auth.uid()';`},
		{name: "BEGIN ATOMIC", routine: `CREATE FUNCTION public.whoami() RETURNS uuid LANGUAGE sql STABLE BEGIN ATOMIC SELECT auth.uid(); END;`},
		{name: "a PL/pgSQL body", routine: `CREATE FUNCTION public.whoami() RETURNS uuid LANGUAGE plpgsql STABLE AS $$ BEGIN RETURN auth.uid(); END $$;`},
	}
	for _, routine := range routines {
		tc := scopedRehearsalCase{
			target:  scopedRehearsalAuthUID,
			desired: "CREATE TABLE public.t (id int PRIMARY KEY);\n" + routine.routine + "\n",
			args:    []string{"--include", "public.*"},
		}
		for _, surface := range scopedRehearsalSurfaces {
			for _, form := range scopedRehearsalForms {
				t.Run(routine.name+"/"+surface.name+"/"+form.name, func(t *testing.T) {
					c := qt.New(t)
					env := newScopedRehearsalEnv(c, tc, form)

					got := env.apply(c, surface, tc.args)

					c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
					c.Assert(scopedRehearsalSchemas(c, env.dev), qt.Equals, "public")
				})
			}
		}
	}
}

// TestScopedRehearsalPlanFailureNamesWhatWasRecreatedE2E rehearses a plan the
// dev database refuses after the rehearsal recreated auth.uid(): the new
// function returns text from a call that returns uuid. The failure names the
// recreated objects, since the plan ran against them.
func TestScopedRehearsalPlanFailureNamesWhatWasRecreatedE2E(t *testing.T) {
	tc := scopedRehearsalCase{
		target:  scopedRehearsalAuthUID,
		desired: "CREATE TABLE public.t (id int PRIMARY KEY);\nCREATE FUNCTION public.whoami() RETURNS integer LANGUAGE sql STABLE AS $$ SELECT auth.uid() $$;\n",
		args:    []string{"--include", "public.*"},
	}
	for _, surface := range scopedRehearsalSurfaces {
		for _, form := range scopedRehearsalForms {
			t.Run(surface.name+"/"+form.name, func(t *testing.T) {
				c := qt.New(t)
				env := newScopedRehearsalEnv(c, tc, form)

				got := env.apply(c, surface, tc.args)

				c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(got.Stderr, qt.Contains, "failed during plan")
				c.Assert(got.Stderr, qt.Contains, `need them: function "auth.uid", schema "auth"; the plan was not applied to the target database`)
				c.Assert(scopedRehearsalSchemas(c, env.dev), qt.Equals, "public")
			})
		}
	}
}

// TestScopedRehearsalLeavesOutWhatOnlyAColumnNamesE2E rehearses, on a shared
// server, plans that add a column named like an object of public the selection
// leaves out: a function, and a table whose default calls auth.uid(). The
// column names neither, so nothing is recreated, and the server's refusal of a
// routine is not met.
func TestScopedRehearsalLeavesOutWhatOnlyAColumnNamesE2E(t *testing.T) {
	cases := []scopedRehearsalCase{
		{
			name:    "a column named like a function",
			target:  "CREATE TABLE public.notes (id int PRIMARY KEY);\nCREATE FUNCTION public.title() RETURNS text LANGUAGE sql AS $$ SELECT 'x' $$;\n",
			desired: "CREATE TABLE public.notes (id int PRIMARY KEY, title text);\n",
			args:    []string{"--include", "public.notes"},
			want:    `ADD COLUMN "title" text`,
		},
		{
			name: "a column named like a table",
			target: scopedRehearsalAuthUID + "CREATE TABLE public.notes (id int PRIMARY KEY);\n" +
				"CREATE TABLE public.items (id int PRIMARY KEY, owner uuid DEFAULT auth.uid());\n",
			desired: "CREATE TABLE public.notes (id int PRIMARY KEY, items jsonb);\n",
			args:    []string{"--include", "public.notes"},
			want:    `ADD COLUMN "items" jsonb`,
		},
	}
	for _, tc := range cases {
		for _, surface := range scopedRehearsalSurfaces {
			for _, form := range scopedRehearsalForms {
				t.Run(tc.name+"/"+surface.name+"/"+form.name, func(t *testing.T) {
					c := qt.New(t)
					env := newScopedRehearsalEnv(c, tc, form)

					got := env.applyIn(c, surface, tc.args, scopedRehearsalShared)

					c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
					c.Assert(got.Stdout, qt.Contains, tc.want)
					c.Assert(scopedRehearsalSchemas(c, env.dev), qt.Equals, "public")
				})
			}
		}
	}
}

// scopedRehearsalAuthObjects lists the relations and routines of the auth
// schema of database, a routine with a digest of its body, comma-separated in
// name order.
func scopedRehearsalAuthObjects(c *qt.C, database string) string {
	c.Helper()
	var objects string
	err := openReadScopeDatabase(c, database).QueryRowContext(c.Context(), `
		SELECT COALESCE(string_agg(name, ',' ORDER BY name), '') FROM (
			SELECT c.relname AS name FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'auth'
			UNION ALL
			SELECT p.proname || ' ' || md5(p.prosrc) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'auth'
		) objects`).Scan(&objects)
	c.Assert(err, qt.IsNil)
	return objects
}
