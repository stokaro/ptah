//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/replaytrust"
)

// A Rebase project keeps its own routines in schemas the comparison leaves
// out, and reaches the dev database through a sibling database on the server
// its target lives on. Two of its commands run that SQL on the dev database.
// schema apply recreates the routines a kept policy calls before it rehearses
// the plan. migrate diff replays the migration history, which creates the
// routines and, in a DO block, a role. Neither server is the run's own, so the
// strict default refuses the routine, and PTAH_TRUST_REPLAY_SQL=1 runs it
// without claiming the server.

// trustedRehearsalShared is the environment of a Rebase project on a server
// nothing declares disposable, with the SQL trusted.
var trustedRehearsalShared = append(slices.Clone(scopedRehearsalShared), replaytrust.EnvVar+"=1")

// trustedRehearsalCases are the plans whose rehearsal recreates a routine from
// a schema the selection leaves out: auth.uid() behind --include, and the
// rebase routines behind the excludes a Rebase project passes. Each declares
// the role as the test created it, so the plan alters no role and the only
// statements the guard refuses without trust are the recreated routines.
var trustedRehearsalCases = []scopedRehearsalCase{
	{
		name:    "a policy of an included table calls auth.uid()",
		target:  fmt.Sprintf(scopedRehearsalNotes, ""),
		desired: "CREATE ROLE {role};\n" + fmt.Sprintf(scopedRehearsalNotes, ", title text"),
		args:    []string{"--include", "public.notes"},
		want:    `ADD COLUMN "title"`,
	},
	{
		name:    "policies call rebase.uid() and rebase.roles() with rebase excluded",
		target:  scopedRehearsalFunctions,
		desired: scopedRehearsalFunctions + "CREATE ROLE {role} NOLOGIN;\n" + scopedRehearsalTasks,
		args:    scopedRehearsalRebaseExcludes,
		want:    `CREATE POLICY "tasks_default_admin_write_insert"`,
	},
}

// TestTrustedRehearsalRecreatesTheRoutinesAScopedPlanNeedsE2E rehearses each
// plan on a shared server with the SQL trusted. The recreated routines pass
// the guard that refuses them without trust, the rehearsal passes, and the
// cleanup leaves the dev database holding public alone.
func TestTrustedRehearsalRecreatesTheRoutinesAScopedPlanNeedsE2E(t *testing.T) {
	for _, tc := range trustedRehearsalCases {
		for _, surface := range scopedRehearsalSurfaces {
			for _, form := range scopedRehearsalForms {
				t.Run(tc.name+"/"+surface.name+"/"+form.name, func(t *testing.T) {
					c := qt.New(t)
					env := newScopedRehearsalEnv(c, tc, form)

					got := env.applyIn(c, surface, tc.args, trustedRehearsalShared)

					c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
					c.Assert(got.Stdout, qt.Contains, tc.want)
					c.Assert(scopedRehearsalSchemas(c, env.dev), qt.Equals, "public")
				})
			}
		}
	}
}

// TestScopedRehearsalOfTheRebaseDocumentOnASharedServerRefusesTheRoutineE2E
// is the control for the trusted rows: without a declaration the same rebase
// routines are refused by the guard, which names the trust declaration among
// its remedies.
func TestScopedRehearsalOfTheRebaseDocumentOnASharedServerRefusesTheRoutineE2E(t *testing.T) {
	tc := trustedRehearsalCases[1]
	for _, surface := range scopedRehearsalSurfaces {
		for _, form := range scopedRehearsalForms {
			t.Run(surface.name+"/"+form.name, func(t *testing.T) {
				c := qt.New(t)
				env := newScopedRehearsalEnv(c, tc, form)

				got := env.applyIn(c, surface, tc.args, scopedRehearsalShared)

				c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(got.Stderr, qt.Contains, "refuses CREATE routine definition")
				c.Assert(got.Stderr, qt.Contains, replaytrust.Remedy)
				c.Assert(got.Stderr, qt.Contains, `function "rebase.roles", function "rebase.uid", schema "rebase"`)
				c.Assert(got.Stderr, qt.Contains, "the plan was not applied to the target database")
				c.Assert(scopedRehearsalSchemas(c, env.dev), qt.Equals, "public")
			})
		}
	}
}

// rebaseHistoryInit is the first migration of the history: the table, and
// the index the desired file also declares.
const rebaseHistoryInit = `CREATE TABLE "public"."tasks" (
  "id" uuid NOT NULL DEFAULT gen_random_uuid(),
  "title" text NOT NULL,
  "owner" text NULL,
  "body" text NULL,
  "embedding" real[] NULL,
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id")
);
CREATE INDEX "tasks_owner_idx" ON "public"."tasks" ("owner");
`

// rebaseHistoryBootstrap is the migration Rebase appends: its schema and
// routines, a DO block that creates {role} only when pg_roles lacks it, the
// policies that call the routines, two more indexes, a trigger and a view.
// The integration server has no pgvector, so the embedding is a real[] under
// GIN rather than a vector under HNSW; the shape needs an index the history
// builds and the desired file declares, not that access method.
const rebaseHistoryBootstrap = `CREATE SCHEMA IF NOT EXISTS rebase;
CREATE OR REPLACE FUNCTION rebase.jwt() RETURNS jsonb LANGUAGE sql STABLE AS $$
  SELECT coalesce(nullif(current_setting('request.jwt.claims', true), ''), '{}')::jsonb
$$;
CREATE OR REPLACE FUNCTION rebase.uid() RETURNS text LANGUAGE sql STABLE AS $$
  SELECT rebase.jwt() ->> 'sub'
$$;
CREATE OR REPLACE FUNCTION rebase.roles() RETURNS text[] LANGUAGE sql STABLE AS $$
  SELECT coalesce(array(SELECT jsonb_array_elements_text(rebase.jwt() -> 'roles')), '{}'::text[])
$$;
CREATE OR REPLACE FUNCTION rebase.is_anonymous() RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT rebase.uid() IS NULL
$$;
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
    CREATE ROLE {role} NOLOGIN NOSUPERUSER NOBYPASSRLS NOINHERIT;
  END IF;
EXCEPTION WHEN duplicate_object THEN
  NULL;
END
$$;
ALTER TABLE "public"."tasks" ENABLE ROW LEVEL SECURITY;
CREATE POLICY "tasks_owner_read" ON "public"."tasks" FOR SELECT USING ("owner" = rebase.uid());
CREATE POLICY "tasks_admin_all" ON "public"."tasks" TO {role} USING ('admin' = ANY (rebase.roles()));
CREATE INDEX "tasks_search_idx" ON "public"."tasks" USING gin (to_tsvector('english', coalesce("title", '') || ' ' || coalesce("body", '')));
CREATE INDEX "tasks_embedding_idx" ON "public"."tasks" USING gin ("embedding");
CREATE OR REPLACE FUNCTION public.tasks_touch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END
$$;
CREATE TRIGGER "tasks_touch" BEFORE UPDATE ON "public"."tasks" FOR EACH ROW EXECUTE FUNCTION public.tasks_touch();
CREATE VIEW "public"."my_tasks" AS SELECT "id", "title" FROM "public"."tasks" WHERE "owner" = rebase.uid();
`

// rebaseHistorySchema is the desired file: tables, columns and indexes, with
// one column the history lacks.
const rebaseHistorySchema = `CREATE TABLE "public"."tasks" (
  "id" uuid NOT NULL DEFAULT gen_random_uuid(),
  "title" text NOT NULL,
  "owner" text NULL,
  "body" text NULL,
  "embedding" real[] NULL,
  "done" boolean NOT NULL DEFAULT false,
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id")
);
CREATE INDEX "tasks_owner_idx" ON "public"."tasks" ("owner");
CREATE INDEX "tasks_search_idx" ON "public"."tasks" USING gin (to_tsvector('english', coalesce("title", '') || ' ' || coalesce("body", '')));
CREATE INDEX "tasks_embedding_idx" ON "public"."tasks" USING gin ("embedding");
`

// rebaseHistoryAddDone is the statement migrate diff and schema apply plan
// for the column the history lacks.
const rebaseHistoryAddDone = `ADD COLUMN "done" boolean NOT NULL DEFAULT false;`

// rebaseHistoryDiffExclude is what a Rebase project leaves out of the
// comparison, in the spelling migrate diff reads: neither Atlas edition
// registers --exclude on that command, so ptah-compat refuses the flag there
// as the pinned community binary does.
const rebaseHistoryDiffExclude = "*[type=policy],*[type=function],*[type=trigger],*[type=view],*[type=extension]"

// rebaseHistoryApplyExcludes are the same selectors as schema apply's flags,
// and the revision table. A URL bound to public holds that table in the
// compared schema, so without the selector both ptah-compat and Atlas CE
// v1.3.0 plan DROP TABLE "atlas_schema_revisions".
var rebaseHistoryApplyExcludes = []string{
	"--exclude", "*[type=policy]", "--exclude", "*[type=function]", "--exclude", "*[type=trigger]",
	"--exclude", "*[type=view]", "--exclude", "*[type=extension]", "--exclude", "atlas_schema_revisions",
}

// rebaseHistoryVariables are the variables a row's environment sets itself,
// so none of them reaches a run from the environment the test inherited.
var rebaseHistoryVariables = []string{
	"PTAH_DEV_SERVER_DISPOSABLE", replaytrust.EnvVar,
	"PTAH_ATLAS_MIGRATE_DIFF_EXCLUDE", "PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE",
}

// rebaseHistoryPushed is the target after the history and one push: the
// revision rows, policies, routines, trigger and view the excludes leave out
// are where the history put them, and the column the push planned is added.
const rebaseHistoryPushed = "revisions=2 policies=tasks_admin_all,tasks_owner_read " +
	"functions=public.tasks_touch,rebase.is_anonymous,rebase.jwt,rebase.roles,rebase.uid " +
	"triggers=tasks_touch views=my_tasks rls=t " +
	"indexes=tasks_embedding_idx,tasks_owner_idx,tasks_pkey,tasks_search_idx " +
	"columns=id,title,owner,body,embedding,updated_at,done"

// rebaseHistoryDeclarations are the two ways to let migrate diff replay the
// history on a shared server. roles is how many of the bootstrap's role the
// server holds after the first replay: trust claims nothing, so the role the
// trusted replay created stays, and a disposable server's cleanup drops it.
var rebaseHistoryDeclarations = []struct {
	name      string
	variables []string
	roles     int
}{
	{name: "trusted SQL", variables: []string{replaytrust.EnvVar + "=1"}, roles: 1},
	{name: "disposable server", variables: []string{"PTAH_DEV_SERVER_DISPOSABLE=1"}, roles: 0},
}

// rebasePushDeclarations are the environments schema apply runs in. Its
// rehearsal recreates nothing the excludes leave out, because the plan names
// none of it, so it needs no declaration.
var rebasePushDeclarations = []struct {
	name      string
	variables []string
}{
	{name: "no declaration"},
	{name: "trusted SQL", variables: []string{replaytrust.EnvVar + "=1"}},
	{name: "disposable server", variables: []string{"PTAH_DEV_SERVER_DISPOSABLE=1"}},
}

// TestRebaseHistoryReplayOnASharedServerRefusesTheRoutineE2E is the strict
// default: migrate diff replays the history on a dev database whose server
// nothing declares, and the guard refuses the first routine, names the trust
// declaration, and leaves no migration, no role and no rebase schema behind.
func TestRebaseHistoryReplayOnASharedServerRefusesTheRoutineE2E(t *testing.T) {
	c := qt.New(t)
	env := newRebaseHistoryEnv(c)

	got := env.diff(c, nil)

	c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, "replay migration 20260101000001 on dev database")
	c.Assert(got.Stderr, qt.Contains, "postgres migration replay rejects CREATE routine definition")
	c.Assert(got.Stderr, qt.Contains, replaytrust.Remedy)
	c.Assert(got.Stderr, qt.Contains, "SQL: CREATE OR REPLACE FUNCTION rebase.jwt()")
	c.Assert(env.generated(c), qt.HasLen, 0)
	c.Assert(env.roles(c), qt.Equals, 0)
	c.Assert(scopedRehearsalSchemas(c, env.dev), qt.Equals, "public")
}

// TestRebaseHistoryReplaysUnderADeclarationE2E runs migrate diff twice under
// each declaration. The first replay plans the missing column; the second
// replays the history and that migration, meets the role the first left
// under trust, and plans nothing.
func TestRebaseHistoryReplaysUnderADeclarationE2E(t *testing.T) {
	for _, test := range rebaseHistoryDeclarations {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			env := newRebaseHistoryEnv(c)

			first := env.diff(c, test.variables)
			roles := env.roles(c)
			second := env.diff(c, test.variables)

			c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
			generated := env.generated(c)
			c.Assert(generated, qt.HasLen, 1)
			c.Assert(generated[0], qt.Contains, rebaseHistoryAddDone)
			c.Assert(generated[0], qt.Not(qt.Contains), "DROP")
			c.Assert(roles, qt.Equals, test.roles)
			c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", second.Stdout, second.Stderr))
			c.Assert(second.Stdout, qt.Contains, "The migration directory is synced with the desired state, no changes to be made")
			c.Assert(env.generated(c), qt.HasLen, 1)
			c.Assert(scopedRehearsalSchemas(c, env.dev), qt.Equals, "public")
		})
	}
}

// TestRebasePushKeepsWhatItsExcludesLeaveOutE2E applies the history to the
// target and pushes the desired file with schema apply: a dry run, the apply,
// and the same apply again. The push adds the column, keeps everything the
// excludes leave out, converges, and leaves the database beside the target
// and the dev database as they were.
func TestRebasePushKeepsWhatItsExcludesLeaveOutE2E(t *testing.T) {
	for _, test := range rebasePushDeclarations {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			env := newRebaseHistoryEnv(c)
			history := env.run(c, nil, "migrate", "apply", "--dir", env.dirURL(), "--url", env.target)
			c.Assert(history.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", history.Stdout, history.Stderr))

			dry := env.push(c, test.variables, "--dry-run")
			applied := env.push(c, test.variables, "--auto-approve")
			repeat := env.push(c, test.variables, "--auto-approve")

			c.Assert(dry.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", dry.Stdout, dry.Stderr))
			c.Assert(dry.Stdout, qt.Contains, rebaseHistoryAddDone)
			c.Assert(dry.Stdout, qt.Not(qt.Contains), "DROP")
			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(repeat.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", repeat.Stdout, repeat.Stderr))
			c.Assert(repeat.Stdout, qt.Contains, "Schema is synced, no changes to be made")
			c.Assert(env.pushed(c), qt.Equals, rebaseHistoryPushed)
			c.Assert(env.bystanderRows(c), qt.Equals, 1)
			c.Assert(scopedRehearsalSchemas(c, env.dev), qt.Equals, "public")
		})
	}
}

// rebaseHistoryEnv is a target database, its dev database beside it as a
// Rebase project names it, an unrelated database on the same server, the
// migration history with its atlas.sum, and the desired file.
type rebaseHistoryEnv struct {
	root, dir, schema      string
	target, dev, bystander string
	role                   string
}

// newRebaseHistoryEnv creates the three databases and the files. Both URLs
// are bound to public by search_path, as a Rebase project binds them. The
// role the bootstrap names is dropped last, once the databases that name it
// are gone.
func newRebaseHistoryEnv(c *qt.C) rebaseHistoryEnv {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	suffix := time.Now().UnixNano()
	env := rebaseHistoryEnv{root: c.TempDir(), role: fmt.Sprintf("ptah_rebase_admin_%d", suffix)}
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+quoteE2EIdent(env.role))
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	target := fmt.Sprintf("ptah_rebase_%d", suffix)
	urls := make([]string, 0, 3)
	for _, name := range []string{target, target + "_dev_diff", target + "_bystander"} {
		createE2EDatabase(c, c.Context(), admin, name)
		c.Cleanup(func() { dropE2EDatabase(c, context.Background(), admin, name) })
		urls = append(urls, replaceDatabaseName(c, adminURL, name))
	}
	env.target = withParameter(c, urls[0], "search_path", "public")
	env.dev = withParameter(c, urls[1], "search_path", "public")
	env.bystander = urls[2]
	execPostgresFamilySQL(c, c.Context(), env.bystander, "CREATE TABLE keep (id int PRIMARY KEY);\nINSERT INTO keep VALUES (1);\n")

	env.dir = filepath.Join(env.root, "migrations")
	env.schema = filepath.Join(env.root, "schema.sql")
	c.Assert(os.Mkdir(env.dir, 0o700), qt.IsNil)
	for name, body := range map[string]string{
		"20260101000000_init.sql":             rebaseHistoryInit,
		"20260101000001_rebase_bootstrap.sql": strings.ReplaceAll(rebaseHistoryBootstrap, "{role}", env.role),
	} {
		c.Assert(os.WriteFile(filepath.Join(env.dir, name), []byte(body), 0o600), qt.IsNil)
	}
	c.Assert(os.WriteFile(env.schema, []byte(rebaseHistorySchema), 0o600), qt.IsNil)
	hashed := env.run(c, nil, "migrate", "hash", "--dir", env.dirURL())
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", hashed.Stdout, hashed.Stderr))
	return env
}

// dirURL is the migration directory as a file URL.
func (e rebaseHistoryEnv) dirURL() string {
	return "file://" + filepath.ToSlash(e.dir)
}

// run runs ptah-compat in the project directory with the environment a
// Rebase project sets and variables added to it.
func (e rebaseHistoryEnv) run(c *qt.C, variables []string, args ...string) clirun.Result {
	c.Helper()
	env := make([]string, 0, len(os.Environ())+len(variables)+2)
	for _, variable := range os.Environ() {
		name, _, _ := strings.Cut(variable, "=")
		if !slices.Contains(rebaseHistoryVariables, name) {
			env = append(env, variable)
		}
	}
	env = append(env, "PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE=1", "PTAH_ATLAS_MIGRATE_DIFF_EXCLUDE="+rebaseHistoryDiffExclude)
	return clirun.Run(c, clirun.Compat, clirun.Options{Dir: e.root, Env: append(env, variables...)}, args...)
}

// diff runs Rebase's migrate diff: no --url, the history replayed on the dev
// database, the desired file compared with what it built.
func (e rebaseHistoryEnv) diff(c *qt.C, variables []string) clirun.Result {
	c.Helper()
	return e.run(c, variables, "migrate", "diff", "add_done",
		"--dir", e.dirURL(), "--to", "file://"+filepath.ToSlash(e.schema), "--dev-url", e.dev)
}

// push runs Rebase's schema apply with mode, --dry-run or --auto-approve.
func (e rebaseHistoryEnv) push(c *qt.C, variables []string, mode string) clirun.Result {
	c.Helper()
	args := []string{"schema", "apply", "--to", "file://" + filepath.ToSlash(e.schema), mode, "--url", e.target, "--dev-url", e.dev}
	return e.run(c, variables, append(args, rebaseHistoryApplyExcludes...)...)
}

// generated returns the contents of the migrations migrate diff wrote.
func (e rebaseHistoryEnv) generated(c *qt.C) []string {
	c.Helper()
	paths, err := filepath.Glob(filepath.Join(e.dir, "*_add_done.sql"))
	c.Assert(err, qt.IsNil)
	contents := make([]string, 0, len(paths))
	for _, path := range paths {
		body, readErr := os.ReadFile(path)
		c.Assert(readErr, qt.IsNil)
		contents = append(contents, string(body))
	}
	return contents
}

// roles counts the server's roles named as the bootstrap's.
func (e rebaseHistoryEnv) roles(c *qt.C) int {
	c.Helper()
	var roles int
	err := openReadScopeDatabase(c, e.bystander).QueryRowContext(c.Context(),
		"SELECT count(*) FROM pg_roles WHERE rolname = $1", e.role).Scan(&roles)
	c.Assert(err, qt.IsNil)
	return roles
}

// bystanderRows counts the rows of the table in the unrelated database.
func (e rebaseHistoryEnv) bystanderRows(c *qt.C) int {
	c.Helper()
	var rows int
	err := openReadScopeDatabase(c, e.bystander).QueryRowContext(c.Context(), "SELECT count(*) FROM keep").Scan(&rows)
	c.Assert(err, qt.IsNil)
	return rows
}

// pushed describes the target: the revision rows, and the policies, routines,
// triggers, views, row security, indexes and columns of public.tasks.
func (e rebaseHistoryEnv) pushed(c *qt.C) string {
	c.Helper()
	var state string
	err := openReadScopeDatabase(c, e.target).QueryRowContext(c.Context(), `
		SELECT format('revisions=%s policies=%s functions=%s triggers=%s views=%s rls=%s indexes=%s columns=%s',
			(SELECT count(*) FROM public.atlas_schema_revisions),
			(SELECT string_agg(policyname, ',' ORDER BY policyname) FROM pg_policies WHERE schemaname = 'public' AND tablename = 'tasks'),
			(SELECT string_agg(n.nspname || '.' || p.proname, ',' ORDER BY n.nspname, p.proname)
				FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname IN ('public', 'rebase')),
			(SELECT string_agg(tgname, ',' ORDER BY tgname) FROM pg_trigger WHERE tgrelid = 'public.tasks'::regclass AND NOT tgisinternal),
			(SELECT string_agg(viewname, ',' ORDER BY viewname) FROM pg_views WHERE schemaname = 'public'),
			(SELECT relrowsecurity FROM pg_class WHERE oid = 'public.tasks'::regclass),
			(SELECT string_agg(indexname, ',' ORDER BY indexname) FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'tasks'),
			(SELECT string_agg(column_name, ',' ORDER BY ordinal_position)
				FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'tasks'))`).Scan(&state)
	c.Assert(err, qt.IsNil)
	return state
}
