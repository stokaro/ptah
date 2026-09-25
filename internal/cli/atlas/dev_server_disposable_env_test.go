package atlas_test

import (
	"fmt"
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
