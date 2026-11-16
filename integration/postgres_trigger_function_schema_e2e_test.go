//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A rehearsal writes the target's current schema onto the dev database before
// it runs the plan there, triggers included. A trigger that runs a function of
// another schema has to name that schema when it is written back: the dev
// session resolves a bare name along its search path, and the default
// `"$user", public` of the account these tests connect with does not reach
// the function's schema.

// triggerFunctionTarget is a table in public whose trigger runs a function in
// schema audit, as a migration writes them.
const triggerFunctionTarget = `CREATE SCHEMA audit;
CREATE FUNCTION audit.set_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW := jsonb_populate_record(NEW, jsonb_build_object(TG_ARGV[0], now()));
    RETURN NEW;
END;
$$;
CREATE TABLE public.tasks (id bigint PRIMARY KEY, updated_at timestamptz);
CREATE TRIGGER tasks_touch BEFORE UPDATE ON public.tasks FOR EACH ROW EXECUTE FUNCTION audit.set_updated_at('updated_at');
`

// triggerFunctionDesired is the same schema with a column added to the table.
const triggerFunctionDesired = `CREATE SCHEMA IF NOT EXISTS audit;
CREATE OR REPLACE FUNCTION audit.set_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW := jsonb_populate_record(NEW, jsonb_build_object(TG_ARGV[0], now()));
    RETURN NEW;
END;
$$;
CREATE TABLE public.tasks (id bigint PRIMARY KEY, updated_at timestamptz, done boolean);
CREATE TRIGGER tasks_touch BEFORE UPDATE ON public.tasks FOR EACH ROW EXECUTE FUNCTION audit.set_updated_at('updated_at');
`

// triggerFunctionRuns are the schema apply commands that rehearse the plan on
// a dev database: the native one and the Atlas-compatible one.
var triggerFunctionRuns = []struct {
	name    string
	program clirun.Target
	args    func(target, schema, dev string) []string
}{
	{
		name:    "ptah schema apply",
		program: clirun.Ptah,
		args: func(target, schema, dev string) []string {
			return []string{"schema", "apply", "--db-url", target, "--schema-file", schema, "--dev-url", dev, "--dry-run"}
		},
	},
	{
		name:    "ptah-compat schema apply",
		program: clirun.Compat,
		args: func(target, schema, dev string) []string {
			return []string{"schema", "apply", "--url", target, "--to", "file://" + filepath.ToSlash(schema), "--dev-url", dev, "--dry-run"}
		},
	},
}

// TestSchemaApplyRehearsesATriggerRunningAFunctionOfAnotherSchemaE2E_HappyPath
// plans the added column against a target holding the trigger, with a sibling
// dev database. The baseline creates the trigger there naming audit, so the
// rehearsal reaches the plan, and the plan changes the table alone.
func TestSchemaApplyRehearsesATriggerRunningAFunctionOfAnotherSchemaE2E_HappyPath(t *testing.T) {
	for _, run := range triggerFunctionRuns {
		t.Run(run.name, func(t *testing.T) {
			c := qt.New(t)
			server := dbtarget.URL(c, dbtarget.PostgreSQL)
			targetURL := createdDevDialectDatabase(c, server, "DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
			_, err := connectDevDialect(c, targetURL).ExecContext(c.Context(), triggerFunctionTarget)
			c.Assert(err, qt.IsNil)
			devURL := createdDevDialectDatabase(c, server, "DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(triggerFunctionDesired), 0o600), qt.IsNil)
			options := clirun.Options{Dir: c.TempDir(), Env: append(os.Environ(), "PTAH_DEV_SERVER_DISPOSABLE=1")}

			got := clirun.Run(c, run.program, options, run.args(targetURL, schema, devURL)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, `ADD COLUMN "done" boolean`)
			c.Assert(got.Stdout, qt.Not(qt.Contains), "TRIGGER")
		})
	}
}

// TestSchemaInspectNamesTheSchemaOfATriggerFunctionE2E_HappyPath describes the
// target as SQL. The trigger names the function with its schema, so the
// description runs on a database whose search path does not reach audit.
func TestSchemaInspectNamesTheSchemaOfATriggerFunctionE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	server := dbtarget.URL(c, dbtarget.PostgreSQL)
	targetURL := createdDevDialectDatabase(c, server, "DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
	_, err := connectDevDialect(c, targetURL).ExecContext(c.Context(), triggerFunctionTarget)
	c.Assert(err, qt.IsNil)

	got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, "schema", "inspect", "--url", targetURL, "--format", "{{ sql . }}")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Contains, `EXECUTE FUNCTION "audit"."set_updated_at"('updated_at')`)
}

// generatedFunctionApplies are the schema apply commands that apply the plan
// after rehearsing it on a dev database.
var generatedFunctionApplies = []struct {
	name    string
	program clirun.Target
	args    func(target, schema, dev string) []string
}{
	{
		name:    "ptah schema apply",
		program: clirun.Ptah,
		args: func(target, schema, dev string) []string {
			return []string{"schema", "apply", "--db-url", target, "--schema-file", schema, "--dev-url", dev, "--auto-approve"}
		},
	},
	{
		name:    "ptah-compat schema apply",
		program: clirun.Compat,
		args: func(target, schema, dev string) []string {
			return []string{"schema", "apply", "--url", target, "--to", "file://" + filepath.ToSlash(schema), "--dev-url", dev, "--auto-approve"}
		},
	},
}

// TestSchemaApplyDropsAGeneratedTriggerFunctionOfAnotherSchemaE2E_HappyPath
// removes a trigger whose function Ptah generated, held in the trigger
// table's schema rather than the one the session resolves names in. The read
// names the function with its schema and it is still the trigger's own: the
// rehearsal recreates the trigger with its body, the plan drops the function
// with the trigger by its schema, and both are gone from the target. A bare
// DROP FUNCTION resolves along the search path, misses the function, and the
// apply fails with SQLSTATE 42883.
func TestSchemaApplyDropsAGeneratedTriggerFunctionOfAnotherSchemaE2E_HappyPath(t *testing.T) {
	generated := schemamodel.Trigger{Name: "t2", Table: "app.items"}.FunctionName()
	target := "CREATE SCHEMA app;\n" +
		"CREATE TABLE app.items (id bigint PRIMARY KEY);\n" +
		"CREATE FUNCTION app." + generated + "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;\n" +
		"CREATE TRIGGER t2 BEFORE UPDATE ON app.items FOR EACH ROW EXECUTE FUNCTION app." + generated + "();\n"
	for _, run := range generatedFunctionApplies {
		t.Run(run.name, func(t *testing.T) {
			c := qt.New(t)
			server := dbtarget.URL(c, dbtarget.PostgreSQL)
			targetURL := createdDevDialectDatabase(c, server, "DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
			conn := connectDevDialect(c, targetURL)
			_, err := conn.ExecContext(c.Context(), target)
			c.Assert(err, qt.IsNil)
			devURL := createdDevDialectDatabase(c, server, "DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte("CREATE SCHEMA IF NOT EXISTS app;\nCREATE TABLE app.items (id bigint PRIMARY KEY);\n"), 0o600), qt.IsNil)
			options := clirun.Options{Dir: c.TempDir(), Env: append(os.Environ(), "PTAH_DEV_SERVER_DISPOSABLE=1")}

			got := clirun.Run(c, run.program, options, run.args(targetURL, schema, devURL)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			var functions, triggers int
			c.Assert(conn.QueryRowContext(c.Context(),
				"SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'app' AND p.proname = $1",
				generated).Scan(&functions), qt.IsNil)
			c.Assert(conn.QueryRowContext(c.Context(), "SELECT count(*) FROM pg_trigger WHERE tgname = 't2'").Scan(&triggers), qt.IsNil)
			c.Assert(functions, qt.Equals, 0)
			c.Assert(triggers, qt.Equals, 0)
		})
	}
}

// TestSchemaApplyReplacesAGeneratedTriggerFunctionWhereItIsE2E_HappyPath
// changes a trigger whose function Ptah generated, held in the trigger
// table's schema rather than the one the session resolves names in. The
// replacement rewrites that function and the trigger runs it: no second
// function of the generated name appears where a bare name resolves.
func TestSchemaApplyReplacesAGeneratedTriggerFunctionWhereItIsE2E_HappyPath(t *testing.T) {
	generated := schemamodel.Trigger{Name: "t2", Table: "app.items"}.FunctionName()
	target := "CREATE SCHEMA app;\n" +
		"CREATE TABLE app.items (id bigint PRIMARY KEY);\n" +
		"CREATE FUNCTION app." + generated + "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;\n" +
		"CREATE TRIGGER t2 BEFORE UPDATE ON app.items FOR EACH ROW EXECUTE FUNCTION app." + generated + "();\n"
	desired := "CREATE SCHEMA IF NOT EXISTS app;\n" +
		"CREATE TABLE app.items (id bigint PRIMARY KEY);\n" +
		"CREATE FUNCTION " + generated + "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;\n" +
		"CREATE TRIGGER t2 AFTER UPDATE ON app.items FOR EACH ROW EXECUTE FUNCTION " + generated + "();\n"
	for _, run := range generatedFunctionApplies {
		t.Run(run.name, func(t *testing.T) {
			c := qt.New(t)
			server := dbtarget.URL(c, dbtarget.PostgreSQL)
			targetURL := createdDevDialectDatabase(c, server, "DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
			conn := connectDevDialect(c, targetURL)
			_, err := conn.ExecContext(c.Context(), target)
			c.Assert(err, qt.IsNil)
			devURL := createdDevDialectDatabase(c, server, "DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(desired), 0o600), qt.IsNil)
			options := clirun.Options{Dir: c.TempDir(), Env: append(os.Environ(), "PTAH_DEV_SERVER_DISPOSABLE=1")}

			got := clirun.Run(c, run.program, options, run.args(targetURL, schema, devURL)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			var held string
			c.Assert(conn.QueryRowContext(c.Context(),
				"SELECT string_agg(n.nspname, ',' ORDER BY n.nspname) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE p.proname = $1",
				generated).Scan(&held), qt.IsNil)
			var runs string
			c.Assert(conn.QueryRowContext(c.Context(),
				"SELECT n.nspname || CASE WHEN t.tgtype & 2 = 0 THEN ' after' ELSE ' before' END FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid JOIN pg_namespace n ON n.oid = p.pronamespace WHERE t.tgname = 't2'",
			).Scan(&runs), qt.IsNil)
			c.Assert(held, qt.Equals, "app")
			c.Assert(runs, qt.Equals, "app after")
		})
	}
}

// TestSchemaApplyDropsAGeneratedTriggerFunctionOfAnotherTableSpellingE2E_HappyPath
// removes a trigger whose function Ptah generated when the table was written
// `public.items`, which names it ptah_trigger_public__items_t. The read
// reports the trigger on items, whose generated name would be
// ptah_trigger_items_t, and the function is still the trigger's own: the plan
// drops it with the trigger by the name it has, and both are gone from the
// target. Read as a function of its own, it is left behind, and the rehearsal
// fails with SQLSTATE 42883 because the dev database never had it.
func TestSchemaApplyDropsAGeneratedTriggerFunctionOfAnotherTableSpellingE2E_HappyPath(t *testing.T) {
	generated := schemamodel.Trigger{Name: "t", Table: "public.items"}.FunctionName()
	target := "CREATE TABLE public.items (id bigint PRIMARY KEY);\n" +
		"CREATE FUNCTION public." + generated + "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;\n" +
		"CREATE TRIGGER t BEFORE UPDATE ON public.items FOR EACH ROW EXECUTE FUNCTION public." + generated + "();\n"
	for _, run := range generatedFunctionApplies {
		t.Run(run.name, func(t *testing.T) {
			c := qt.New(t)
			server := dbtarget.URL(c, dbtarget.PostgreSQL)
			targetURL := createdDevDialectDatabase(c, server, "DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
			conn := connectDevDialect(c, targetURL)
			_, err := conn.ExecContext(c.Context(), target)
			c.Assert(err, qt.IsNil)
			devURL := createdDevDialectDatabase(c, server, "DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte("CREATE TABLE items (id bigint PRIMARY KEY);\n"), 0o600), qt.IsNil)
			options := clirun.Options{Dir: c.TempDir(), Env: append(os.Environ(), "PTAH_DEV_SERVER_DISPOSABLE=1")}

			got := clirun.Run(c, run.program, options, run.args(targetURL, schema, devURL)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			var functions, triggers int
			c.Assert(conn.QueryRowContext(c.Context(), "SELECT count(*) FROM pg_proc WHERE proname = $1", generated).Scan(&functions), qt.IsNil)
			c.Assert(conn.QueryRowContext(c.Context(), "SELECT count(*) FROM pg_trigger WHERE tgname = 't'").Scan(&triggers), qt.IsNil)
			c.Assert(functions, qt.Equals, 0)
			c.Assert(triggers, qt.Equals, 0)
		})
	}
}

// TestSchemaApplyReplacesAGeneratedTriggerFunctionOfAnotherTableSpellingE2E_HappyPath
// changes a trigger whose generated function the target holds in schema fns
// as ptah_trigger_items_t, the name of the table written bare, while the
// desired state's spelling of the table gives another name. The replacement
// rewrites the function the trigger runs, and no second ptah_trigger_
// function appears beside it.
func TestSchemaApplyReplacesAGeneratedTriggerFunctionOfAnotherTableSpellingE2E_HappyPath(t *testing.T) {
	const target = "CREATE SCHEMA fns;\n" +
		"CREATE TABLE public.items (id bigint PRIMARY KEY);\n" +
		"CREATE FUNCTION fns.ptah_trigger_items_t() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;\n" +
		"CREATE TRIGGER t BEFORE UPDATE ON public.items FOR EACH ROW EXECUTE FUNCTION fns.ptah_trigger_items_t();\n"
	const desired = "CREATE SCHEMA IF NOT EXISTS fns;\n" +
		"CREATE TABLE public.items (id bigint PRIMARY KEY);\n" +
		"CREATE FUNCTION ptah_trigger_public__items_t() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;\n" +
		"CREATE TRIGGER t AFTER UPDATE ON public.items FOR EACH ROW EXECUTE FUNCTION ptah_trigger_public__items_t();\n"
	for _, run := range generatedFunctionApplies {
		t.Run(run.name, func(t *testing.T) {
			c := qt.New(t)
			server := dbtarget.URL(c, dbtarget.PostgreSQL)
			targetURL := createdDevDialectDatabase(c, server, "DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
			conn := connectDevDialect(c, targetURL)
			_, err := conn.ExecContext(c.Context(), target)
			c.Assert(err, qt.IsNil)
			devURL := createdDevDialectDatabase(c, server, "DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(desired), 0o600), qt.IsNil)
			options := clirun.Options{Dir: c.TempDir(), Env: append(os.Environ(), "PTAH_DEV_SERVER_DISPOSABLE=1")}

			got := clirun.Run(c, run.program, options, run.args(targetURL, schema, devURL)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			var held string
			c.Assert(conn.QueryRowContext(c.Context(),
				`SELECT string_agg(n.nspname || '.' || p.proname, ',' ORDER BY 1) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE p.proname LIKE 'ptah\_trigger\_%'`,
			).Scan(&held), qt.IsNil)
			var runs string
			c.Assert(conn.QueryRowContext(c.Context(),
				"SELECT n.nspname || '.' || p.proname || CASE WHEN t.tgtype & 2 = 0 THEN ' after' ELSE ' before' END FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid JOIN pg_namespace n ON n.oid = p.pronamespace WHERE t.tgname = 't'",
			).Scan(&runs), qt.IsNil)
			c.Assert(held, qt.Equals, "fns.ptah_trigger_items_t")
			c.Assert(runs, qt.Equals, "fns.ptah_trigger_items_t after")
		})
	}
}
