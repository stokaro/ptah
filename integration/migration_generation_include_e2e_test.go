//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// Both shipped binaries plan a migration from a replayed directory under a
// positive selection: `ptah migrations generate --include` and `ptah-compat
// migrate diff` with PTAH_ATLAS_MIGRATE_DIFF_INCLUDE. The history created a
// table with an index, a trigger and a policy the declaration does not name,
// and a view the declaration does not own. Each row applies the history and
// the generated migration to a database of its own and reads the catalog back.
//
// The row without a selection is the control: the migration drops all four.
// An include of the declared objects keeps the view, which it does not
// select, and still drops the table's undeclared children, which ride along
// with the table. An exclusion of those children keeps them too.

// generationIncludeHistory is the directory's first migration.
const generationIncludeHistory = `CREATE TABLE public.tasks (id int PRIMARY KEY, title text);
CREATE INDEX tasks_title_idx ON public.tasks (title);
ALTER TABLE public.tasks ENABLE ROW LEVEL SECURITY;
CREATE POLICY tasks_user ON public.tasks FOR SELECT USING (true);
CREATE TRIGGER tasks_touch BEFORE UPDATE ON public.tasks FOR EACH ROW EXECUTE FUNCTION suppress_redundant_updates_trigger();
CREATE VIEW public.unmanaged_v AS SELECT 1 AS x;
`

// generationIncludeDesired keeps the table, declares none of its children,
// and adds a view of its own.
const generationIncludeDesired = `CREATE TABLE public.tasks (id int PRIMARY KEY, title text);
ALTER TABLE public.tasks ENABLE ROW LEVEL SECURITY;
CREATE VIEW public.tasks_v AS SELECT id FROM public.tasks;
`

// generationIncludeCatalog names the objects on tasks and the views of the
// public schema, sorted.
const generationIncludeCatalog = `SELECT string_agg(name, ',' ORDER BY name) FROM (
  SELECT 'index:' || indexname FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'tasks'
  UNION ALL SELECT 'policy:' || polname FROM pg_policy WHERE polrelid = 'public.tasks'::regclass
  UNION ALL SELECT 'trigger:' || tgname FROM pg_trigger WHERE tgrelid = 'public.tasks'::regclass AND NOT tgisinternal
  UNION ALL SELECT 'view:' || viewname FROM pg_views WHERE schemaname = 'public'
) objects(name)`

var generationIncludeRows = []struct {
	name string
	// native and compat are the selection each binary is given: flags for
	// ptah, and the variables for ptah-compat, whose dev URL names no schema,
	// so its patterns carry one.
	native, compat []string
	want           string
}{
	{
		name: "nothing selected",
		want: "index:tasks_pkey,view:tasks_v",
	},
	{
		name:   "the declared objects",
		native: []string{"--include", "tasks", "--include", "tasks_v"},
		compat: []string{"PTAH_ATLAS_MIGRATE_DIFF_INCLUDE=public.tasks,public.tasks_v"},
		want:   "index:tasks_pkey,view:tasks_v,view:unmanaged_v",
	},
	{
		name: "the declared objects without the table's children",
		native: []string{
			"--include", "tasks", "--include", "tasks_v",
			"--exclude", "tasks.tasks_title_idx", "--exclude", "tasks.tasks_user[type=policy]", "--exclude", "tasks.tasks_touch[type=trigger]",
		},
		compat: []string{
			"PTAH_ATLAS_MIGRATE_DIFF_INCLUDE=public.tasks,public.tasks_v",
			"PTAH_ATLAS_MIGRATE_DIFF_EXCLUDE=public.tasks.tasks_title_idx,public.tasks.tasks_user[type=policy],public.tasks.tasks_touch[type=trigger]",
		},
		want: "index:tasks_pkey,index:tasks_title_idx,policy:tasks_user,trigger:tasks_touch,view:tasks_v,view:unmanaged_v",
	},
}

func TestMigrationsGenerateIncludeKeepsWhatTheHistoryOwnsE2E(t *testing.T) {
	for _, test := range generationIncludeRows {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			migrations := filepath.Join(dir, "migrations")
			c.Assert(os.MkdirAll(migrations, 0o755), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(migrations, "0000000001_init.up.sql"), []byte(generationIncludeHistory), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(migrations, "0000000001_init.down.sql"),
				[]byte("DROP VIEW public.unmanaged_v;\nDROP TABLE public.tasks;\n"), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(generationIncludeDesired), 0o600), qt.IsNil)
			env := append(environmentWithoutPtahVariables(), "PTAH_TRUST_REPLAY_SQL=1")

			generated := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: env}, append([]string{
				"migrations", "generate", "--replay", "--dev-url", postgresScratchDevURL(c, "ptah_generation_include_dev"),
				"--migrations-dir", migrations, "--schema-file", filepath.Join(dir, "schema.sql"), "--name", "next",
			}, test.native...)...)
			c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
			target := postgresScratchDevURL(c, "ptah_generation_include_target")
			applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
				"migrations", "up", "--db-url", target, "--migrations-dir", migrations, "--allow-destructive")
			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))

			c.Assert(readGenerationIncludeCatalog(c, target), qt.Equals, test.want)
		})
	}
}

func TestAtlasCompatMigrateDiffIncludeKeepsWhatTheHistoryOwnsE2E(t *testing.T) {
	for _, test := range generationIncludeRows {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			c.Assert(os.MkdirAll(filepath.Join(dir, "migrations"), 0o755), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "migrations", "20240101000000_init.sql"), []byte(generationIncludeHistory), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(generationIncludeDesired), 0o600), qt.IsNil)
			plain := environmentWithoutPtahVariables()
			hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: plain}, "migrate", "hash", "--dir", "file://migrations")
			c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", hashed.Stderr))
			env := append(append(environmentWithoutPtahVariables(), "PTAH_TRUST_REPLAY_SQL=1"), test.compat...)

			generated := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: env},
				"migrate", "diff", "next", "--dir", "file://migrations", "--to", "file://schema.sql",
				"--dev-url", postgresScratchDevURL(c, "ptah_generation_include_dev"))
			c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
			target := postgresScratchDevURL(c, "ptah_generation_include_target")
			applied := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: plain},
				"migrate", "apply", "--url", target, "--dir", "file://migrations")
			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))

			c.Assert(readGenerationIncludeCatalog(c, target), qt.Equals, test.want)
		})
	}
}

// readGenerationIncludeCatalog answers generationIncludeCatalog on database.
func readGenerationIncludeCatalog(c *qt.C, database string) string {
	c.Helper()
	db := openReadScopeDatabase(c, database)
	var objects string
	c.Assert(db.QueryRowContext(c.Context(), generationIncludeCatalog).Scan(&objects), qt.IsNil)
	return objects
}
