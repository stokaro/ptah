package root_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/envbool/envbooltest"
	"ptah.run/internal/replaytrust"
)

// TestNativeReplayCommandsRefuseAMalformedTrustDeclaration pins that every
// native command that runs SQL on a dev database reads PTAH_TRUST_REPLAY_SQL
// through its --trust-replay-sql binding, which parses the value before the
// command runs. Each invocation would stop on a missing directory or a missing
// database URL, so a command that read the variable only on the way to a
// replay would report that instead and leave the typo unnoticed.
func TestNativeReplayCommandsRefuseAMalformedTrustDeclaration(t *testing.T) {
	for _, test := range disposableDeclarationNativeCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(replaytrust.EnvVar, "maybe")(t)
			missing := filepath.Join(t.TempDir(), "missing")

			stdout, stderr, err := executeRootCommand(test.args(missing)...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout+stderr+err.Error(), qt.Contains, `invalid boolean value "maybe" for PTAH_TRUST_REPLAY_SQL`)
		})
	}
}

// TestNativeReplayCommandsReachTheirOwnRefusalWithoutTheTrustDeclaration is
// the control for the test above: with the variable unset, and with it set to
// a value that parses, the same invocations fail on what they were given and
// say nothing about the variable.
func TestNativeReplayCommandsReachTheirOwnRefusalWithoutTheTrustDeclaration(t *testing.T) {
	states := []struct {
		name  string
		setup func(testing.TB)
	}{
		{"unset", envbooltest.Unset(replaytrust.EnvVar)},
		{"trusted", envbooltest.Set(replaytrust.EnvVar, "1")},
	}
	for _, state := range states {
		for _, test := range disposableDeclarationNativeCases {
			t.Run(state.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				state.setup(t)
				missing := filepath.Join(t.TempDir(), "missing")

				stdout, stderr, err := executeRootCommand(test.args(missing)...)

				c.Assert(err, qt.IsNotNil)
				c.Assert(stdout+stderr+fmt.Sprint(err), qt.Not(qt.Contains), replaytrust.EnvVar)
			})
		}
	}
}

// TestNativeReplayCommandsTakeTheTrustFlag pins that every one of them takes
// --trust-replay-sql and names its variable in help, and refuses a value the
// flag cannot parse before it does any work.
func TestNativeReplayCommandsTakeTheTrustFlag(t *testing.T) {
	for _, test := range disposableDeclarationNativeCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(replaytrust.EnvVar)(t)
			missing := filepath.Join(t.TempDir(), "missing")
			command := test.args(missing)[:2]

			help, _, err := executeRootCommand(append(command, "--help")...)
			c.Assert(err, qt.IsNil)
			c.Assert(help, qt.Matches, `(?s).*--trust-replay-sql .*\[env: PTAH_TRUST_REPLAY_SQL\].*`)

			stdout, stderr, err := executeRootCommand(append(test.args(missing), "--trust-replay-sql=maybe")...)
			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout+stderr+err.Error(), qt.Contains, `invalid argument "maybe" for "--trust-replay-sql" flag`)
		})
	}
}

// TestNativeCommandsThatDoNotReplayIgnoreTheTrustDeclaration pins the other
// half of the rule: a command that never runs SQL on a dev database does not
// own the variable, so a malformed value does not break it.
func TestNativeCommandsThatDoNotReplayIgnoreTheTrustDeclaration(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(replaytrust.EnvVar, "maybe")(t)
	dir := t.TempDir()

	stdout, stderr, err := executeRootCommand("migrations", "hash", "--dir", dir)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
}

// trustInProjectConfig is a ptah.yaml that tries to trust its own SQL, at the
// top level and in an env. ptah.yaml has no such key on purpose: the file is
// discovered in the working directory and controlled by the repository whose
// SQL the replay runs, and a repository must not be able to lift the replay
// guard for its own SQL. The flag and the variable are the operator's.
var trustInProjectConfig = []struct {
	name     string
	document string
	env      []string
	wantErr  string
}{
	{"top level", "trust_replay_sql: true\n", nil,
		`(?s).*failed to parse ptah config .*ptah\.yaml: line 1: unknown ptah\.yaml key "trust_replay_sql"; supported keys are .*`},
	{"in an env", "env:\n  ci:\n    trust_replay_sql: true\n", []string{"--env", "ci"},
		`(?s).*failed to parse ptah config .*ptah\.yaml: line 3: unknown ptah\.yaml key "trust_replay_sql"; supported keys are .*`},
}

// trustExplicitConfigCommands are the native replaying commands that read
// ptah.yaml from --config, invoked so that, without the file, each would stop
// at a missing directory or a missing database URL of its own.
var trustExplicitConfigCommands = []struct {
	name string
	args []string
}{
	{"migrations generate", []string{"migrations", "generate", "--migrations-dir", "missing"}},
	{"schema inspect", []string{"schema", "inspect", "--migrations-dir", "missing"}},
	{"schema diff", []string{"schema", "diff", "--from", "file://missing.sql", "--to", "file://missing.sql"}},
	{"schema apply", []string{"schema", "apply", "--to", "file://missing.sql"}},
}

// trustDiscoveredConfigCommands are the native replaying commands that read
// a ptah.yaml they discover: the ones above, and migrations lint, whose
// --config names a lint config instead.
var trustDiscoveredConfigCommands = append([]struct {
	name string
	args []string
}{
	{"migrations lint", []string{"migrations", "lint", "--dir", "missing"}},
}, trustExplicitConfigCommands...)

// TestNativeProjectConfigCannotTrustReplaySQL_Discovered pins the decision for
// a ptah.yaml the command discovers in the working directory: the key is
// unknown, so the parser refuses the file and the command does no work. It
// does not trust the SQL and run on.
func TestNativeProjectConfigCannotTrustReplaySQL_Discovered(t *testing.T) {
	for _, config := range trustInProjectConfig {
		for _, test := range trustDiscoveredConfigCommands {
			t.Run(config.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				envbooltest.Unset(replaytrust.EnvVar)(t)
				dir := t.TempDir()
				c.Assert(os.WriteFile(filepath.Join(dir, "ptah.yaml"), []byte(config.document), 0o600), qt.IsNil)
				t.Chdir(dir)

				stdout, stderr, err := executeRootCommand(append(test.args, config.env...)...)

				c.Assert(err, qt.IsNotNil)
				c.Assert(stdout+stderr+err.Error(), qt.Matches, config.wantErr)
			})
		}
	}
}

// TestNativeProjectConfigCannotTrustReplaySQL_Explicit is the same decision for
// a ptah.yaml the operator names with --config: an explicit file is not a way
// to trust the SQL either, and is refused in the same words.
func TestNativeProjectConfigCannotTrustReplaySQL_Explicit(t *testing.T) {
	for _, config := range trustInProjectConfig {
		for _, test := range trustExplicitConfigCommands {
			t.Run(config.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				envbooltest.Unset(replaytrust.EnvVar)(t)
				dir := t.TempDir()
				path := filepath.Join(dir, "ptah.yaml")
				c.Assert(os.WriteFile(path, []byte(config.document), 0o600), qt.IsNil)
				t.Chdir(t.TempDir())
				args := slices.Concat(test.args, []string{"--config", path}, config.env)

				stdout, stderr, err := executeRootCommand(args...)

				c.Assert(err, qt.IsNotNil)
				c.Assert(stdout+stderr+err.Error(), qt.Matches, config.wantErr)
			})
		}
	}
}

// TestNativeProjectConfigCommandsReachTheirOwnRefusal is the control for the
// two tests above: with a ptah.yaml that sets nothing, the same invocations
// fail on what they were given, so the refusal above is the key's.
func TestNativeProjectConfigCommandsReachTheirOwnRefusal(t *testing.T) {
	for _, test := range trustDiscoveredConfigCommands {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(replaytrust.EnvVar)(t)
			dir := t.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "ptah.yaml"), []byte("{}\n"), 0o600), qt.IsNil)
			t.Chdir(dir)

			stdout, stderr, err := executeRootCommand(test.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout+stderr+err.Error(), qt.Not(qt.Contains), "ptah.yaml")
		})
	}
}
