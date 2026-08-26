package atlas_test

import (
	"bytes"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/atlas/internal/atlastest"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
)

// runCompat runs one argument list through the compatibility root and returns
// what it wrote and the exit code it asked for.
func runCompat(c *qt.C, args ...string) (stdout, stderr string, code int) {
	c.Helper()

	cmd := atlas.NewCompatCommand("ptah-compat")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)

	err := atlastest.ExecuteAtlasTestCommand(cmd)

	return out.String(), errOut.String(), exitcode.Code(err, 0)
}

// `cloud` is answered with what Ptah has locally -- stokaro/ptah#1018.
//
// The group is a client for a hosted registry, so most of it has no local
// counterpart. What Ptah does have is the two graph exports and the deployment
// history, which are database facts rather than registry ones, and a user who
// typed the command is looking for exactly those.
func TestAtlasCompatibilityRoot_CloudPointsAtWhatPtahHas(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, code := runCompat(c, "cloud")

	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Contains, "ptah schema lineage")
	c.Assert(stderr, qt.Contains, "ptah schema security")
	c.Assert(stderr, qt.Contains, "ptah migrations status")
	// It says what it does NOT have, rather than leaving the reader to assume
	// the four lines are the whole group.
	c.Assert(stderr, qt.Contains, "no hosted registry")
}

// The first two lines and the exit code are unchanged, which is the property
// that keeps this a pointer rather than a divergence.
//
// The compatibility surface answers this verb exactly as the community binary
// does, and registering a `cloud` command group would have ended that for the
// sake of a message. Appending keeps the answer and adds to it.
func TestAtlasCompatibilityRoot_CloudKeepsTheAnswerItAlreadyGave(t *testing.T) {
	c := qt.New(t)

	_, stderr, code := runCompat(c, "cloud")

	c.Assert(code, qt.Equals, 1)
	lines := strings.SplitN(stderr, "\n", 3)
	c.Assert(lines[0], qt.Equals, `Error: unknown command "cloud" for "atlas"`)
	c.Assert(lines[1], qt.Equals, "Run 'atlas --help' for usage.")
}

// The pointer is scoped to the verbs that have one, and everything else is
// untouched.
//
// Without this the change would read as "add a Ptah paragraph to every error",
// which is a different and worse thing: the compatibility surface's answer to
// an arbitrary unknown command is byte-identical to the community binary's, and
// that is worth more than a message.
func TestAtlasCompatibilityRoot_AnUnknownCommandWithNoLocalAnswerIsUnchanged(t *testing.T) {
	tests := []struct {
		name string
		verb string
	}{
		{name: "a nonsense verb", verb: "definitely-not-a-command"},
		{name: "a verb that is nearly a real one", verb: "clou"},
		{name: "a registry verb Ptah has nothing for", verb: "registry"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, stderr, code := runCompat(c, test.verb)

			c.Assert(code, qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Not(qt.Contains), "ptah schema lineage")
			c.Assert(stderr, qt.Not(qt.Contains), "no hosted registry")
		})
	}
}

// A sub-verb of the group reaches the same pointer, because that is what a user
// following the documentation types.
//
// `ptah-compat cloud database list` is the shape the release notes describe,
// and answering it with the bare unknown-command line would send the reader
// away with nothing while the pointer sat one word shorter.
func TestAtlasCompatibilityRoot_CloudSubVerbsReachThePointer(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "database list", args: []string{"cloud", "database", "list"}},
		{name: "migration list", args: []string{"cloud", "migration", "list"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, stderr, code := runCompat(c, test.args...)

			c.Assert(code, qt.Equals, 1)
			c.Assert(stderr, qt.Contains, "ptah schema lineage")
		})
	}
}
