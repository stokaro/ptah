package root_test

import (
	"fmt"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// disposableDeclarationNativeCases are the native commands that can replay a
// migration directory on a dev database, each invoked so that it stops at an
// early refusal of its own and never reaches a replay. The directory they name
// does not exist, and none of them is given a dev database.
var disposableDeclarationNativeCases = []struct {
	name string
	args func(missing string) []string
}{
	{
		name: "migrations lint",
		args: func(missing string) []string { return []string{"migrations", "lint", "--dir", missing} },
	},
	{
		name: "migrations validate",
		args: func(missing string) []string { return []string{"migrations", "validate", "--dir", missing} },
	},
	{
		name: "migrations generate",
		args: func(missing string) []string { return []string{"migrations", "generate", "--migrations-dir", missing} },
	},
	{
		name: "schema inspect",
		args: func(missing string) []string { return []string{"schema", "inspect", "--migrations-dir", missing} },
	},
	{
		name: "schema diff",
		args: func(missing string) []string {
			return []string{"schema", "diff", "--from", "file://" + missing, "--to", "file://" + missing}
		},
	},
	{
		name: "schema apply",
		args: func(missing string) []string { return []string{"schema", "apply", "--to", "file://" + missing} },
	},
}

// TestNativeReplayCommandsRefuseAMalformedDisposableDeclaration pins that every
// native command that can replay resolves PTAH_DEV_SERVER_DISPOSABLE before its
// own early refusals. Each invocation would stop on a missing directory or a
// missing database URL, so a command that read the variable only on the way
// to a replay would report that instead and leave the typo unnoticed.
func TestNativeReplayCommandsRefuseAMalformedDisposableDeclaration(t *testing.T) {
	for _, test := range disposableDeclarationNativeCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(devdocker.DisposableServerEnvVar, "maybe")(t)
			missing := filepath.Join(t.TempDir(), "missing")

			stdout, stderr, err := executeRootCommand(test.args(missing)...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout+stderr+err.Error(), qt.Contains,
				`invalid boolean value "maybe" for PTAH_DEV_SERVER_DISPOSABLE`)
		})
	}
}

// TestNativeReplayCommandsReachTheirOwnRefusalWithoutTheDeclaration is the
// control for the test above: with the variable unset, the same invocations
// fail on what they were given and say nothing about the variable. The
// refusal above is therefore the variable's, not an accident of the arguments.
func TestNativeReplayCommandsReachTheirOwnRefusalWithoutTheDeclaration(t *testing.T) {
	for _, test := range disposableDeclarationNativeCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(devdocker.DisposableServerEnvVar)(t)
			missing := filepath.Join(t.TempDir(), "missing")

			stdout, stderr, err := executeRootCommand(test.args(missing)...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout+stderr+fmt.Sprint(err), qt.Not(qt.Contains), devdocker.DisposableServerEnvVar)
		})
	}
}

// TestNativeCommandsThatDoNotReplayIgnoreTheDisposableDeclaration pins the
// other half of the rule: a command that never replays does not own the
// variable, so a malformed value does not break it.
func TestNativeCommandsThatDoNotReplayIgnoreTheDisposableDeclaration(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(devdocker.DisposableServerEnvVar, "maybe")(t)
	dir := t.TempDir()

	stdout, stderr, err := executeRootCommand("migrations", "hash", "--dir", dir)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
}
