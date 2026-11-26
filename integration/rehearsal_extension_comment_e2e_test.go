//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// A plan rehearsal runs the target's current schema and then the plan on the
// dev database. Each carries a comment on the extension and the schema it
// creates, and every cleanup of the dev database drops what the run created
// with its comment, so the rehearsal accepts a comment on an extension the dev
// database did not hold when the run claimed it, as a replay of a migration
// history does. CockroachDB has no row: it does not implement COMMENT ON
// EXTENSION.

// rehearsalSchemaSQL is the extension and the application schema of
// pgDumpEntitiesBase, with their comments, as an SQL desired state.
const rehearsalSchemaSQL = `CREATE SCHEMA app;
COMMENT ON SCHEMA app IS 'Fixture application schema';
CREATE EXTENSION pg_trgm;
COMMENT ON EXTENSION pg_trgm IS 'Fixture extension';
CREATE TABLE app.notes (id integer PRIMARY KEY);
`

// rehearsalVerb is one command that rehearses a plan on the dev database. Its
// arguments name {target}, {dev}, {base}, {next}, {schema} and {dir}, which
// rehearsalArgs fills in. before runs first, with the same names, to give the
// target the extension the baseline then recreates on the dev database.
type rehearsalVerb struct {
	name   string
	target clirun.Target
	before [][]string
	args   []string
}

var rehearsalVerbs = []rehearsalVerb{
	{name: "schema apply --dry-run", target: clirun.Ptah,
		args: []string{"schema", "apply", "--db-url", "{target}", "--dev-url", "{dev}", "--root-dir", "{base}", "--dry-run"}},
	{name: "schema apply --dry-run on a target that holds the extension", target: clirun.Ptah,
		before: [][]string{{"schema", "apply", "--db-url", "{target}", "--root-dir", "{base}", "--auto-approve"}},
		args:   []string{"schema", "apply", "--db-url", "{target}", "--dev-url", "{dev}", "--root-dir", "{next}", "--dry-run"}},
	{name: "compat schema apply --dry-run", target: clirun.Compat,
		args: []string{"schema", "apply", "--url", "{target}", "--to", "file://{schema}", "--dev-url", "{dev}", "--dry-run"}},
	{name: "compat migrate diff", target: clirun.Compat,
		args: []string{"migrate", "diff", "fixture", "--dir", "file://{dir}", "--to", "file://{schema}", "--dev-url", "{dev}"}},
}

// rehearsalArgs is args with each name in braces replaced by its value.
func rehearsalArgs(args []string, values map[string]string) []string {
	filled := make([]string, len(args))
	for index, arg := range args {
		for name, value := range values {
			arg = strings.ReplaceAll(arg, "{"+name+"}", value)
		}
		filled[index] = arg
	}
	return filled
}

// rehearsalValues writes the entities and the SQL desired state into the
// history's root and returns the values rehearsalArgs fills in.
func rehearsalValues(c *qt.C, h postgresHistory, dev string) map[string]string {
	c.Helper()
	schema := filepath.Join(h.root, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(rehearsalSchemaSQL), 0o600), qt.IsNil) // #nosec G703 -- the path is c.TempDir() plus a constant name
	return map[string]string{
		"target": h.target,
		"dev":    dev,
		"base":   writePgDumpEntities(c, h, "base", pgDumpEntitiesBase),
		"next":   writePgDumpEntities(c, h, "next", pgDumpEntitiesNext),
		"schema": schema,
		"dir":    h.dir,
	}
}

// rehearse runs args with target in the history's root, with no PTAH_
// variable.
func rehearse(c *qt.C, h postgresHistory, target clirun.Target, args []string) clirun.Result {
	c.Helper()
	return clirun.Run(c, target, clirun.Options{Dir: h.root, Env: environmentWithoutPtahVariables()}, args...)
}

// TestRehearsalExtensionCommentE2E_HappyPath rehearses a plan that creates
// pg_trgm with a comment, and one whose baseline recreates it with its
// comment, on a fresh dev database with no trust. Each succeeds, and the dev
// database ends without the schema and the extension the rehearsal created.
func TestRehearsalExtensionCommentE2E_HappyPath(t *testing.T) {
	for _, engine := range pgDumpEngines {
		for _, verb := range rehearsalVerbs {
			t.Run(engine.name+"/"+verb.name, func(t *testing.T) {
				c := qt.New(t)
				h, dev := newPgDumpHistory(c, engine.engine)
				values := rehearsalValues(c, h, dev)
				for _, before := range verb.before {
					prepared := rehearse(c, h, verb.target, rehearsalArgs(before, values))
					c.Assert(prepared.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", prepared.Stdout, prepared.Stderr))
				}

				got := rehearse(c, h, verb.target, rehearsalArgs(verb.args, values))

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(pgDumpQuery[int](c, dev, pgDumpReplayCreated), qt.Equals, 0)
			})
		}
	}
}

// TestRehearsalExtensionCommentE2E_FailurePath is the control for the test
// above. On a dev database that held pg_trgm when the rehearsal claimed it,
// the cleanup keeps the extension, so a comment on it would outlive the run:
// the rehearsal refuses the plan, or the baseline that recreates a target
// holding the extension, before it runs a statement, and the comment reads as
// before.
func TestRehearsalExtensionCommentE2E_FailurePath(t *testing.T) {
	verbs := []struct {
		rehearsalVerb
		wantExit int
	}{
		{wantExit: 2, rehearsalVerb: rehearsalVerb{name: "schema apply --dry-run", target: clirun.Ptah,
			args: []string{"schema", "apply", "--db-url", "{target}", "--dev-url", "{dev}", "--root-dir", "{base}", "--dry-run"}}},
		{wantExit: 2, rehearsalVerb: rehearsalVerb{name: "schema apply --dry-run on a target that holds the extension", target: clirun.Ptah,
			before: [][]string{{"schema", "apply", "--db-url", "{target}", "--root-dir", "{base}", "--auto-approve"}},
			args:   []string{"schema", "apply", "--db-url", "{target}", "--dev-url", "{dev}", "--root-dir", "{next}", "--dry-run"}}},
		{wantExit: 1, rehearsalVerb: rehearsalVerb{name: "compat schema apply --dry-run", target: clirun.Compat,
			args: []string{"schema", "apply", "--url", "{target}", "--to", "file://{schema}", "--dev-url", "{dev}", "--dry-run"}}},
	}
	for _, engine := range pgDumpEngines {
		for _, verb := range verbs {
			t.Run(engine.name+"/"+verb.name, func(t *testing.T) {
				c := qt.New(t)
				h, dev := newPgDumpHistory(c, engine.engine)
				values := rehearsalValues(c, h, dev)
				compatDriftExec(c, dev, "CREATE EXTENSION pg_trgm")
				before := pgDumpQuery[string](c, dev, pgDumpExtensionComment)
				for _, step := range verb.before {
					prepared := rehearse(c, h, verb.target, rehearsalArgs(step, values))
					c.Assert(prepared.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", prepared.Stdout, prepared.Stderr))
				}

				got := rehearse(c, h, verb.target, rehearsalArgs(verb.args, values))

				c.Assert(got.ExitCode, qt.Equals, verb.wantExit, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(got.Stdout+got.Stderr, qt.Contains, "COMMENT ON global metadata because its effects cannot be confined")
				c.Assert(pgDumpQuery[string](c, dev, pgDumpExtensionComment), qt.Equals, before)
			})
		}
	}
}
