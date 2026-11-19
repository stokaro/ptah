//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/testutils"
)

// TestCompatUnknownCommandBeforeAnyFlagE2E runs a command neither binary
// registers, surrounded by flags, through the shipped ptah-compat and the
// pinned community binary. The community binary names the command whatever
// flags come with it, --help included, so an operator who typed a verb from a
// newer release learns that the verb is missing. A root that parses its flags
// first answers `unknown flag: --url` instead, which points at the flag and
// hides the verb.
func TestCompatUnknownCommandBeforeAnyFlagE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	tests := []struct {
		name string
		args []string
		verb string
	}{
		{name: "flag after the verbs", args: []string{"security", "scan", "--url", "sqlite://x.db"}, verb: "security"},
		{name: "flag before the verb", args: []string{"--url", "sqlite://x.db", "security", "scan"}, verb: "security"},
		{name: "help after the verb", args: []string{"security", "--help"}, verb: "security"},
		{name: "env flag after the verb", args: []string{"foo", "--env", "local"}, verb: "foo"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			want := "Error: unknown command \"" + test.verb + "\" for \"atlas\"\n" +
				"Run 'atlas --help' for usage.\n"

			communityOut, communityStderr, err := runCLIProcess(c.Context(), c.TempDir(), reference, test.args...)
			communityErr := testutils.WithoutAtlasCommunityBuildNotice(communityStderr)
			compat := clirun.Run(c, clirun.Compat, clirun.Options{
				Dir: c.TempDir(),
				Env: environmentWithoutPtahVariables(),
			}, test.args...)

			c.Assert(exitStatusOf(c, err), qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", communityOut, communityErr))
			c.Assert(communityOut, qt.Equals, "")
			c.Assert(communityErr, qt.Equals, want)
			c.Assert(compat.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", compat.Stdout, compat.Stderr))
			c.Assert(compat.Stdout, qt.Equals, "")
			c.Assert(compat.Stderr, qt.Equals, want)
		})
	}
}

// TestCompatUnknownFlagWhenNoCommandIsNamedE2E is the control for the test
// above: with no word the root reads as a command, both binaries report the
// flag, so the order above comes from naming the command and not from a root
// that stopped reporting unknown flags.
func TestCompatUnknownFlagWhenNoCommandIsNamedE2E(t *testing.T) {
	c := qt.New(t)
	reference := atlasReferenceBinary(t)
	args := []string{"--url", "sqlite://x.db"}

	communityOut, communityStderr, err := runCLIProcess(c.Context(), c.TempDir(), reference, args...)
	communityErr := testutils.WithoutAtlasCommunityBuildNotice(communityStderr)
	compat := clirun.Run(c, clirun.Compat, clirun.Options{
		Dir: c.TempDir(),
		Env: environmentWithoutPtahVariables(),
	}, args...)

	c.Assert(exitStatusOf(c, err), qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", communityOut, communityErr))
	c.Assert(communityErr, qt.Equals, "Error: unknown flag: --url\n")
	c.Assert(compat.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", compat.Stdout, compat.Stderr))
	c.Assert(compat.Stdout, qt.Equals, communityOut)
	c.Assert(compat.Stderr, qt.Equals, communityErr)
}
