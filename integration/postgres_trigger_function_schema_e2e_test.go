//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

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
