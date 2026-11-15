//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/envbool/envbooltest"
	"ptah.run/internal/replaytrust"
)

// A run that trusts the SQL it replays on the dev database, through
// --trust-replay-sql on the native binary or PTAH_TRUST_REPLAY_SQL on either,
// replays a routine, a DO block and a CREATE ROLE on a server it neither
// provisioned nor was told is disposable. Trust claims nothing about the
// server: the cleanup still empties only the dev database the run claimed, so
// a role the SQL created stays, and a bare CREATE ROLE replayed a second time
// fails as the server answers it.

// nativeTrustDeclarations are the two ways to tell the native binary to trust
// the SQL: the flag, with the variable absent, and the variable the flag binds.
var nativeTrustDeclarations = []struct {
	name  string
	setup func(testing.TB)
	args  []string
}{
	{"flag", envbooltest.Unset(replaytrust.EnvVar), []string{"--trust-replay-sql"}},
	{"variable", envbooltest.Set(replaytrust.EnvVar, "1"), nil},
}

// serverWideRefusal is the refusal of the directory [writeServerWideReplayDir]
// writes, on a server the run neither owns nor trusts the SQL on. It names the
// two ways to the server realm and the trust declaration.
const serverWideRefusal = `(?s).*rejects DO sublanguage because its effects cannot be confined to the (disposable|dev) database realm; ` +
	`if nothing else uses this server, declare it disposable with PTAH_DEV_SERVER_DISPOSABLE=1, or use a docker:// or docker\+<driver>:// dev URL; ` +
	`if you trust this SQL to run with the dev connection's privileges, set PTAH_TRUST_REPLAY_SQL=1\nSQL: DO .*`

// TestDevReplayOnANamedServerRefusesEveryServerWideReplayCompat is the strict
// default for every ptah-compat command that replays the directory: each one
// reaches the guard, is refused at the DO block, and creates no role. It is
// the control for the trusted rows below, which would otherwise pass for a
// command that never replayed the directory at all.
func TestDevReplayOnANamedServerRefusesEveryServerWideReplayCompat(t *testing.T) {
	for _, test := range serverWideReplayCompatCommands {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newServerWideReplayFixture(c, "ptah_replay_untrusted")

			out, err := runCompatVerbWithPolicy(test.policy, test.args(fx)...)

			c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
			c.Assert(out+err.Error(), qt.Matches, serverWideRefusal)
			c.Assert(fx.roles(c), qt.Equals, 0)
		})
	}
}

// TestDevReplayOnANamedServerRefusesEveryServerWideReplayNative is the same
// for the native commands.
func TestDevReplayOnANamedServerRefusesEveryServerWideReplayNative(t *testing.T) {
	for _, test := range serverWideReplayNativeCommands {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newServerWideReplayFixture(c, "ptah_replay_untrusted")

			out, err := runPtahNativeWithError(test.args(fx)...)

			c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
			c.Assert(out+err.Error(), qt.Matches, serverWideRefusal)
			c.Assert(fx.roles(c), qt.Equals, 0)
		})
	}
}

// TestDevReplayOnATrustedServerRunsServerWideStatementsCompat drives every
// ptah-compat command that replays the directory with PTAH_TRUST_REPLAY_SQL
// set and PTAH_DEV_SERVER_DISPOSABLE absent. Each replays both migrations; the
// dev database is cleaned as on any run, and the role the DO block created is
// still on the server, since trust widens nothing the cleanup drops.
func TestDevReplayOnATrustedServerRunsServerWideStatementsCompat(t *testing.T) {
	for _, test := range serverWideReplayCompatCommands {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newServerWideReplayFixture(c, "ptah_replay_trusted")
			envbooltest.Set(replaytrust.EnvVar, "1")(c)

			out, err := runCompatVerbWithPolicy(test.policy, test.args(fx)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(fx.roles(c), qt.Equals, 1)
			c.Assert(fx.devObjects(c), qt.Equals, 0)
		})
	}
}

// TestDevReplayOnATrustedServerRunsServerWideStatementsNative is the same for
// the native commands, told to trust the SQL by the flag and by the variable.
func TestDevReplayOnATrustedServerRunsServerWideStatementsNative(t *testing.T) {
	for _, declaration := range nativeTrustDeclarations {
		for _, test := range serverWideReplayNativeCommands {
			t.Run(declaration.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				fx := newServerWideReplayFixture(c, "ptah_replay_trusted")
				declaration.setup(c)

				out, err := runPtahNativeWithError(append(test.args(fx), declaration.args...)...)

				c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
				c.Assert(fx.roles(c), qt.Equals, 1)
				c.Assert(fx.devObjects(c), qt.Equals, 0)
			})
		}
	}
}

// writeRebaseShapedReplayDir writes the shape a generated application schema
// ships: SQL helper routines its policies call, a DO block that creates the
// application's admin role when the server lacks it, and a table whose
// policies and view name both. Every statement is ordinary SQL a dev database
// can hold, and none of it can be confined to the dev database realm.
func writeRebaseShapedReplayDir(c *qt.C, role string) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	files := map[string]string{
		"20240101000000_rebase.sql": fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "rebase";
CREATE OR REPLACE FUNCTION rebase.uid() RETURNS text AS $$
    SELECT NULLIF(current_setting('app.uid', true), '');
$$ LANGUAGE sql STABLE PARALLEL SAFE;
CREATE OR REPLACE FUNCTION rebase.roles() RETURNS text AS $$
    SELECT COALESCE(NULLIF(current_setting('app.user_roles', true), ''), '');
$$ LANGUAGE sql STABLE PARALLEL SAFE;
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '%[1]s') THEN
    CREATE ROLE %[1]s NOLOGIN NOSUPERUSER NOBYPASSRLS NOINHERIT;
  END IF;
EXCEPTION WHEN duplicate_object THEN NULL;
END
$$;
`, role),
		"20240101000001_tasks.sql": fmt.Sprintf(`CREATE TABLE "public"."tasks" (
  "id" bigint PRIMARY KEY,
  "title" text NOT NULL
);
ALTER TABLE "public"."tasks" ENABLE ROW LEVEL SECURITY;
CREATE POLICY "tasks_admin_read" ON "public"."tasks" AS PERMISSIVE FOR SELECT TO "%[1]s"
  USING ((SELECT (rebase.uid() IS NULL) OR (string_to_array(rebase.roles(), ',') && ARRAY['admin'])));
CREATE VIEW "public"."my_tasks" AS SELECT id, title FROM "public"."tasks" WHERE rebase.uid() IS NOT NULL;
`, role),
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	out, err := runCompatVerb("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return dir
}

// newRebaseShapedReplayFixture is the fixture above with the directory
// [writeRebaseShapedReplayDir] writes in place of the server-wide one.
func newRebaseShapedReplayFixture(c *qt.C) serverWideReplayFixture {
	c.Helper()
	fx := newServerWideReplayFixture(c, "ptah_rebase_admin")
	fx.dir = writeRebaseShapedReplayDir(c, fx.role)
	return fx
}

// rebaseShapedRefusal is the refusal of that directory on a server the run
// neither owns nor trusts the SQL on, from the replay prefix on. The schema is
// accepted, and the first routine is refused.
const rebaseShapedRefusal = "replay migration 20240101000000 on dev database: " +
	"failed to validate migration SQL in 20240101000000_rebase.sql: " +
	"postgres migration replay rejects CREATE routine definition because its effects cannot be confined to the disposable database realm; " +
	"if nothing else uses this server, declare it disposable with PTAH_DEV_SERVER_DISPOSABLE=1, or use a docker:// or docker+<driver>:// dev URL; " +
	"if you trust this SQL to run with the dev connection's privileges, set PTAH_TRUST_REPLAY_SQL=1\n" +
	"SQL: CREATE OR REPLACE FUNCTION rebase.uid()"

// rebaseShapedValidations replay the directory with each binary.
var rebaseShapedValidations = []struct {
	name string
	run  func(args ...string) (string, error)
	args func(fx serverWideReplayFixture) []string
}{
	{
		name: "ptah-compat migrate validate",
		run:  runCompatVerb,
		args: func(fx serverWideReplayFixture) []string {
			return []string{"migrate", "validate", "--dir", "file://" + fx.dir, "--dev-url", fx.devURL}
		},
	},
	{
		name: "ptah migrations validate",
		run:  runPtahNativeWithError,
		args: func(fx serverWideReplayFixture) []string {
			return []string{"migrations", "validate", "--dir", fx.dir, "--dir-format", "atlas", "--dev-url", fx.devURL}
		},
	},
}

// TestTrustedReplayRebaseShapeIsRefusedByDefaultE2E pins the strict default
// for the shape: without the declaration each binary refuses at the first
// routine, in the same words, and the DO block never runs.
func TestTrustedReplayRebaseShapeIsRefusedByDefaultE2E(t *testing.T) {
	for _, test := range rebaseShapedValidations {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newRebaseShapedReplayFixture(c)

			out, err := test.run(test.args(fx)...)

			c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
			c.Assert(err, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(rebaseShapedRefusal)+`.*`)
			c.Assert(fx.roles(c), qt.Equals, 0)
		})
	}
}

// TestTrustedReplayRebaseShapeRunsTwiceE2E replays the shape twice on the same
// dev database of a server that is not disposable, trusted by each
// declaration. Both runs pass: the DO block creates the role on the first and
// finds it on the second. The role is on the server afterwards, and the dev
// database holds nothing.
func TestTrustedReplayRebaseShapeRunsTwiceE2E(t *testing.T) {
	tests := []struct {
		name  string
		setup func(testing.TB)
		run   func(args ...string) (string, error)
		args  func(fx serverWideReplayFixture) []string
	}{
		{
			name:  "ptah-compat with the variable",
			setup: envbooltest.Set(replaytrust.EnvVar, "1"),
			run:   rebaseShapedValidations[0].run,
			args:  rebaseShapedValidations[0].args,
		},
		{
			name:  "ptah with the flag",
			setup: envbooltest.Unset(replaytrust.EnvVar),
			run:   rebaseShapedValidations[1].run,
			args: func(fx serverWideReplayFixture) []string {
				return append(rebaseShapedValidations[1].args(fx), "--trust-replay-sql")
			},
		},
		{
			name:  "ptah with the variable",
			setup: envbooltest.Set(replaytrust.EnvVar, "1"),
			run:   rebaseShapedValidations[1].run,
			args:  rebaseShapedValidations[1].args,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newRebaseShapedReplayFixture(c)
			test.setup(c)

			first, err := test.run(test.args(fx)...)
			c.Assert(err, qt.IsNil, qt.Commentf("first run:\n%s", first))
			c.Assert(fx.roles(c), qt.Equals, 1)

			second, err := test.run(test.args(fx)...)
			c.Assert(err, qt.IsNil, qt.Commentf("second run:\n%s", second))
			c.Assert(fx.roles(c), qt.Equals, 1)
			c.Assert(fx.devObjects(c), qt.Equals, 0)
		})
	}
}

// TestTrustedReplayBareCreateRoleFailsOnTheSecondRunE2E pins what trust does
// not do. A bare CREATE ROLE runs on the first trusted run, and the role stays
// on the server; the second run of the same directory fails with the server's
// own SQLSTATE 42710, which Ptah neither rewrites nor follows with a remedy:
// the role is the SQL author's to make idempotent, as the DO block above does.
func TestTrustedReplayBareCreateRoleFailsOnTheSecondRunE2E(t *testing.T) {
	tests := []struct {
		name    string
		run     func(args ...string) (string, error)
		args    func(fx serverWideReplayFixture) []string
		wantErr string
	}{
		{
			name: "ptah-compat migrate validate",
			run:  runCompatVerb,
			args: rebaseShapedValidations[0].args,
			wantErr: `(?s).*executing statement "CREATE ROLE %[1]s NOLOGIN NOINHERIT;" from version "20240101000000": ` +
				`ERROR: role "%[1]s" already exists \(SQLSTATE 42710\)`,
		},
		{
			name: "ptah migrations validate",
			run:  runPtahNativeWithError,
			args: rebaseShapedValidations[1].args,
			wantErr: `(?s).*replay migration 20240101000000 on dev database: failed to execute migration SQL: SQL execution failed: ` +
				`ERROR: role "%[1]s" already exists \(SQLSTATE 42710\)\nSQL: CREATE ROLE %[1]s NOLOGIN NOINHERIT`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newServerWideReplayFixture(c, "ptah_replay_bare")
			fx.dir = filepath.Join(c.TempDir(), "migrations")
			c.Assert(os.MkdirAll(fx.dir, 0o755), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(fx.dir, "20240101000000_role.sql"),
				[]byte(fmt.Sprintf("CREATE ROLE %s NOLOGIN NOINHERIT;\nCREATE TABLE t1 (id int);\n", fx.role)), 0o600), qt.IsNil)
			hashed, err := runCompatVerb("migrate", "hash", "--dir", "file://"+fx.dir)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", hashed))
			envbooltest.Set(replaytrust.EnvVar, "1")(c)

			first, err := test.run(test.args(fx)...)
			c.Assert(err, qt.IsNil, qt.Commentf("first run:\n%s", first))
			c.Assert(fx.roles(c), qt.Equals, 1)

			_, err = test.run(test.args(fx)...)
			c.Assert(err, qt.ErrorMatches, fmt.Sprintf(test.wantErr, fx.role))
			c.Assert(fx.roles(c), qt.Equals, 1)
		})
	}
}

// TestTrustedReplayRefusesAMalformedDeclarationE2E pins the strict boolean on
// both binaries: a value that is not a boolean is refused before the run
// connects, so the directory is not replayed and creates no role.
func TestTrustedReplayRefusesAMalformedDeclarationE2E(t *testing.T) {
	for _, test := range rebaseShapedValidations {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newRebaseShapedReplayFixture(c)
			envbooltest.Set(replaytrust.EnvVar, "maybe")(c)

			out, err := test.run(test.args(fx)...)

			c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
			c.Assert(out+err.Error(), qt.Contains, `invalid boolean value "maybe" for PTAH_TRUST_REPLAY_SQL`)
			c.Assert(fx.roles(c), qt.Equals, 0)
			c.Assert(fx.devObjects(c), qt.Equals, 0)
		})
	}
}

// TestProjectConfigCannotTrustReplaySQLE2E pins that ptah.yaml cannot trust
// the SQL of the repository it sits in. The file is discovered in the working
// directory, so a repository that could set the key would lift the replay
// guard for its own SQL on whatever dev server the caller points it at. A
// command that does not read ptah.yaml replays under the strict default; one
// that does refuses the file for its unknown key. Neither creates the role.
func TestProjectConfigCannotTrustReplaySQLE2E(t *testing.T) {
	tests := []struct {
		name    string
		args    func(fx serverWideReplayFixture) []string
		wantErr string
	}{
		{
			name:    "migrations validate, which reads no ptah.yaml",
			args:    rebaseShapedValidations[1].args,
			wantErr: `(?s).*` + regexp.QuoteMeta(rebaseShapedRefusal) + `.*`,
		},
		{
			name: "schema inspect, which reads ptah.yaml",
			args: func(fx serverWideReplayFixture) []string {
				return []string{"schema", "inspect", "--migrations-dir", fx.dir, "--dev-url", fx.devURL}
			},
			wantErr: `(?s).*failed to parse ptah config .*ptah\.yaml: line 1: unknown ptah\.yaml key "trust_replay_sql"; supported keys are .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newRebaseShapedReplayFixture(c)
			project := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(project, "ptah.yaml"), []byte("trust_replay_sql: true\n"), 0o600), qt.IsNil)
			t.Chdir(project)

			out, err := runPtahNativeWithError(test.args(fx)...)

			c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
			c.Assert(out+err.Error(), qt.Matches, test.wantErr)
			c.Assert(fx.roles(c), qt.Equals, 0)
		})
	}
}
