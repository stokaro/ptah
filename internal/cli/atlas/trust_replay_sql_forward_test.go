package atlas_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/envbool/envbooltest"
)

// The verbs below forward to a native command that registers
// --trust-replay-sql. This surface registers no flag the community binary
// lacks, so it refuses the native flag and trusts replayed SQL through
// PTAH_TRUST_REPLAY_SQL alone, which the target goes on reading.

// trustForwardedVerbs are the forwarded verbs that replay a history on the
// dev database, each with the arguments that reach the replay settings
// before any connection.
var trustForwardedVerbs = []struct {
	name string
	args []string
}{
	{name: "migrate down", args: []string{"migrate", "down", "--dir", "file://migrations",
		"--url", "sqlite://target.db", "--dev-url", "sqlite://dev.db"}},
	{name: "migrate checkpoint", args: []string{"migrate", "checkpoint", "--dir", "file://migrations",
		"--dev-url", "sqlite://dev.db"}},
	{name: "migrate drift", args: []string{"migrate", "drift", "--dir", "file://migrations",
		"--url", "sqlite://target.db", "--dev-url", "sqlite://dev.db"}},
}

func TestCompatTrustReplaySQL_FailurePathRefusesTheNativeFlag(t *testing.T) {
	for _, verb := range trustForwardedVerbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			t.Chdir(t.TempDir())
			envbooltest.Set("PTAH_ATLAS_MIGRATE_DRIFT", "1")(t)
			envbooltest.Unset("PTAH_TRUST_REPLAY_SQL")(t)

			got := runTree(atlas.NewCompatCommand("atlas"), append(verb.args, "--trust-replay-sql")...)

			c.Assert(got, qt.Equals, compatRun{
				stderr: "Error: atlas " + verb.name + " does not accept native Ptah flag --trust-replay-sql\n",
				code:   1,
			})
		})
	}
}

// The control: the variable still reaches the forwarded command, which
// refuses a value that is not a boolean before it connects.
func TestCompatTrustReplaySQL_FailurePathReadsTheVariable(t *testing.T) {
	for _, verb := range trustForwardedVerbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			t.Chdir(t.TempDir())
			envbooltest.Set("PTAH_ATLAS_MIGRATE_DRIFT", "1")(t)
			envbooltest.Set("PTAH_TRUST_REPLAY_SQL", "sometimes")(t)

			got := runTree(atlas.NewCompatCommand("atlas"), verb.args...)

			c.Assert(got, qt.Equals, compatRun{
				stderr: "Error: invalid boolean value \"sometimes\" for PTAH_TRUST_REPLAY_SQL\n",
				code:   1,
			})
		})
	}
}
