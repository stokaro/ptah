package atlas_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas/internal/atlastest"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// disposableDeclarationCompatCases are the ptah-compat commands that can replay
// a migration directory on a dev database, each invoked so that it never
// reaches a replay: dir is an empty directory, and every schema file named
// below it does not exist. Two of the runs succeed without the variable,
// which is the shape on which a typo would otherwise go unnoticed.
var disposableDeclarationCompatCases = []struct {
	name string
	args func(dir string) []string
}{
	{
		name: "migrate diff",
		args: func(dir string) []string {
			return []string{
				"migrate", "diff",
				"--dir", "file://" + dir,
				"--to", "file://" + filepath.Join(dir, "missing.sql"),
				"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
			}
		},
	},
	{
		name: "migrate lint",
		args: func(dir string) []string {
			return []string{
				"migrate", "lint",
				"--dir", "file://" + dir,
				"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
				"--latest", "1",
			}
		},
	},
	{
		name: "migrate validate, forwarded to the native command",
		args: func(dir string) []string { return []string{"migrate", "validate", "--dir", "file://" + dir} },
	},
	{
		name: "migrate validate, run directly for a foreign layout",
		args: func(dir string) []string {
			return []string{"migrate", "validate", "--dir", "file://" + dir, "--dir-format", "golang-migrate"}
		},
	},
	{
		name: "schema inspect",
		args: func(dir string) []string {
			return []string{
				"schema", "inspect",
				"--url", "file://" + filepath.Join(dir, "missing.sql"),
				"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
			}
		},
	},
	{
		name: "schema diff",
		args: func(dir string) []string {
			return []string{
				"schema", "diff",
				"--from", "file://" + filepath.Join(dir, "missing.sql"),
				"--to", "file://" + filepath.Join(dir, "missing.sql"),
				"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
			}
		},
	},
	{
		name: "schema apply",
		args: func(dir string) []string {
			return []string{
				"schema", "apply",
				"--url", "sqlite://" + filepath.Join(dir, "target.db"),
				"--to", "file://" + filepath.Join(dir, "missing.sql"),
				"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
				"--auto-approve",
			}
		},
	},
}

// TestCompatReplayCommandsRefuseAMalformedDisposableDeclaration pins that every
// ptah-compat command that can replay resolves PTAH_DEV_SERVER_DISPOSABLE
// before its own early answers, including the forwarded `migrate validate`,
// where the native command it forwards to does the resolving.
func TestCompatReplayCommandsRefuseAMalformedDisposableDeclaration(t *testing.T) {
	for _, test := range disposableDeclarationCompatCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(devdocker.DisposableServerEnvVar, "maybe")(t)

			out, err := atlastest.RunCompatCommand(t, test.args(t.TempDir())...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(out+err.Error(), qt.Contains,
				`invalid boolean value "maybe" for PTAH_DEV_SERVER_DISPOSABLE`)
		})
	}
}

// TestCompatReplayCommandsDoNotMentionAnUnsetDisposableDeclaration is the
// control: with the variable unset, the same invocations succeed or fail on
// what they were given, and none of them names the variable.
func TestCompatReplayCommandsDoNotMentionAnUnsetDisposableDeclaration(t *testing.T) {
	for _, test := range disposableDeclarationCompatCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(devdocker.DisposableServerEnvVar)(t)

			out, err := atlastest.RunCompatCommand(t, test.args(t.TempDir())...)

			c.Assert(out+fmt.Sprint(err), qt.Not(qt.Contains), devdocker.DisposableServerEnvVar)
		})
	}
}

// TestCompatHashIgnoresTheDisposableDeclaration pins that a command that never
// replays does not own the variable: `migrate hash` shares its wrapper with
// `migrate validate` and still does not read it.
func TestCompatHashIgnoresTheDisposableDeclaration(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(devdocker.DisposableServerEnvVar, "maybe")(t)

	out, err := atlastest.RunCompatCommand(t, "migrate", "hash", "--dir", "file://"+t.TempDir())

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
}

// shadowReplayCompatCases are the ptah-compat commands whose dev database
// holds a replay of the migration history, as a native command's shadow
// database does. Each runs to completion on SQLite, its replay included:
// project holds the atlas.hcl written to the run's directory, setup prepares
// the target there, and args is the run.
var shadowReplayCompatCases = []struct {
	name    string
	project map[string]string
	setup   [][]string
	args    []string
}{
	{
		name:  "migrate down --dev-url",
		setup: [][]string{{"migrate", "apply", "--url", "sqlite://target.db", "--dir", "file://migrations"}},
		args: []string{"migrate", "down", "--url", "sqlite://target.db", "--dir", "file://migrations",
			"--dev-url", "sqlite://dev.db", "--to-version", "1"},
	},
	{
		name: "migrate apply with a declared drift check",
		project: map[string]string{"atlas.hcl": "env \"local\" {\n  url = \"sqlite://target.db\"\n  dev = \"sqlite://dev.db\"\n" +
			"  migration {\n    dir = \"file://migrations\"\n  }\n  check \"migrate_apply\" {\n    drift {}\n  }\n}\n"},
		setup: [][]string{{"migrate", "apply", "--env", "local", "1"}},
		args:  []string{"migrate", "apply", "--env", "local"},
	},
	{
		name: "migrate checkpoint",
		args: []string{"migrate", "checkpoint", "--dir", "file://migrations", "--dev-url", "sqlite://dev.db"},
	},
	{
		name:  "migrate drift",
		setup: [][]string{{"migrate", "apply", "--url", "sqlite://target.db", "--dir", "file://migrations"}},
		args: []string{"migrate", "drift", "--url", "sqlite://target.db", "--dir", "file://migrations",
			"--dev-url", "sqlite://dev.db"},
	},
}

// TestCompatShadowReplayCommandsIgnoreTheDisposableDeclaration pins that a
// command whose dev database holds a replay of the history does not own
// PTAH_DEV_SERVER_DISPOSABLE: the replay is the shadow database's, which the
// variable never declares disposable, so a malformed value leaves each run,
// replay included, to succeed.
func TestCompatShadowReplayCommandsIgnoreTheDisposableDeclaration(t *testing.T) {
	for _, test := range shadowReplayCompatCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(devdocker.DisposableServerEnvVar, "maybe")(t)
			envbooltest.Set("PTAH_ATLAS_MIGRATE_DRIFT", "1")(t)
			t.Chdir(t.TempDir())
			writeShadowReplayCompatHistory(c, test.project)
			for _, args := range test.setup {
				out, err := atlastest.RunCompatOutput(args...)
				c.Assert(err, qt.IsNil, qt.Commentf("setup %v:\n%s", args, out))
			}

			out, err := atlastest.RunCompatOutput(test.args...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Not(qt.Contains), devdocker.DisposableServerEnvVar)
		})
	}
}

// writeShadowReplayCompatHistory writes a hashed two-migration history to
// ./migrations, and each file of project beside it.
func writeShadowReplayCompatHistory(c *qt.C, project map[string]string) {
	c.Helper()
	c.Assert(os.Mkdir("migrations", 0o750), qt.IsNil)
	for name, body := range map[string]string{
		"1_items.sql":      "CREATE TABLE items (id integer PRIMARY KEY);\n",
		"2_notes.sql":      "CREATE TABLE notes (id integer PRIMARY KEY);\n",
		"2_notes.down.sql": "DROP TABLE notes;\n",
	} {
		c.Assert(os.WriteFile(filepath.Join("migrations", name), []byte(body), 0o600), qt.IsNil)
	}
	for name, body := range project {
		c.Assert(os.WriteFile(name, []byte(body), 0o600), qt.IsNil)
	}
	out, err := atlastest.RunCompatOutput("migrate", "hash", "--dir", "file://migrations")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
}
