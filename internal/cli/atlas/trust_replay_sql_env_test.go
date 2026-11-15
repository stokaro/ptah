package atlas_test

import (
	"fmt"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/atlas/internal/atlastest"
	"ptah.run/internal/envbool/envbooltest"
	"ptah.run/internal/replaytrust"
)

// trustDeclarationCompatCases are the ptah-compat commands that run SQL on a
// dev database: the commands that can replay a migration directory, and the
// two schema plan commands that rehearse a plan. Each is invoked so that it
// never reaches the dev database.
var trustDeclarationCompatCases = append([]struct {
	name string
	args func(dir string) []string
}{
	{
		name: "schema plan lint",
		args: func(dir string) []string {
			return []string{"schema", "plan", "lint", "--file", "file://" + filepath.Join(dir, "missing.plan.hcl"),
				"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db")}
		},
	},
	{
		name: "schema plan validate",
		args: func(dir string) []string {
			return []string{"schema", "plan", "validate", "--file", "file://" + filepath.Join(dir, "missing.plan.hcl"),
				"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db")}
		},
	},
}, disposableDeclarationCompatCases...)

// TestCompatReplayCommandsRefuseAMalformedTrustDeclaration pins that every
// ptah-compat command that runs SQL on a dev database resolves
// PTAH_TRUST_REPLAY_SQL before its own early answers. ptah-compat takes no flag
// for it, so the variable is read directly, by the strict boolean grammar.
func TestCompatReplayCommandsRefuseAMalformedTrustDeclaration(t *testing.T) {
	for _, test := range trustDeclarationCompatCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(replaytrust.EnvVar, "maybe")(t)

			out, err := atlastest.RunCompatCommand(t, test.args(t.TempDir())...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(out+err.Error(), qt.Contains, `invalid boolean value "maybe" for PTAH_TRUST_REPLAY_SQL`)
		})
	}
}

// TestCompatReplayCommandsDoNotMentionTheTrustDeclaration is the control: with
// the variable unset, with it trusting, and with it trusting under the strict
// compatibility policy, the same invocations succeed or fail on what they
// were given, and none of them names the variable. The strict row holds
// because the variable is Retained: the pinned community binary replays these
// statements without a guard, so trusting them takes nothing from it.
func TestCompatReplayCommandsDoNotMentionTheTrustDeclaration(t *testing.T) {
	states := []struct {
		name  string
		setup []func(testing.TB)
	}{
		{"unset", []func(testing.TB){envbooltest.Unset(replaytrust.EnvVar)}},
		{"trusted", []func(testing.TB){envbooltest.Set(replaytrust.EnvVar, "1")}},
		{"trusted in strict mode", []func(testing.TB){
			envbooltest.Set(replaytrust.EnvVar, "1"),
			envbooltest.Set(atlascompatpolicy.StrictCompatEnvVar, "1"),
		}},
	}
	for _, state := range states {
		for _, test := range trustDeclarationCompatCases {
			t.Run(state.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				for _, setup := range state.setup {
					setup(t)
				}

				out, err := atlastest.RunCompatCommand(t, test.args(t.TempDir())...)

				c.Assert(out+fmt.Sprint(err), qt.Not(qt.Contains), replaytrust.EnvVar)
			})
		}
	}
}

// TestCompatHashIgnoresTheTrustDeclaration pins that a command that never runs
// SQL on a dev database does not own the variable.
func TestCompatHashIgnoresTheTrustDeclaration(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(replaytrust.EnvVar, "maybe")(t)

	out, err := atlastest.RunCompatCommand(t, "migrate", "hash", "--dir", "file://"+t.TempDir())

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
}

// TestCompatReplayCommandsTakeNoTrustFlag pins that ptah-compat adds no flag
// for trust: the conformance cli-surface tier holds its flags to the pinned
// community binary's, so the variable is the only spelling.
func TestCompatReplayCommandsTakeNoTrustFlag(t *testing.T) {
	for _, test := range trustDeclarationCompatCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(replaytrust.EnvVar)(t)

			out, err := atlastest.RunCompatCommand(t, append(test.args(t.TempDir()), "--trust-replay-sql")...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(out+err.Error(), qt.Contains, "unknown flag: --trust-replay-sql")
		})
	}
}
