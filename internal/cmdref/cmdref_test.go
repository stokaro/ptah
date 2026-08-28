package cmdref_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/internal/agentsurface"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	"go.5x5.cz/ptah/internal/cmdref"
)

// full and strict are the shapes the two compatibility policies produce,
// reduced to the four cases that differ. Written out rather than measured so a
// change to the real policy cannot quietly turn this test into a description
// of whatever the policy now does.
var (
	full = []agentsurface.Node{
		{Name: "migrate", Leaf: false, Runnable: true},
		{Name: "migrate apply", Leaf: true},
		{Name: "migrate ls", Leaf: true},
		{Name: "migrate rm", Leaf: true},
		{Name: "schema plan", Leaf: false, Runnable: true},
		{Name: "schema plan lint", Leaf: true},
		{Name: "script", Leaf: false},
		{Name: "script exec", Leaf: true},
	}
	strict = []agentsurface.Node{
		{Name: "migrate", Leaf: false, Runnable: true},
		{Name: "migrate apply", Leaf: true},
		{Name: "migrate rm", Leaf: true, Hidden: true},
		{Name: "schema plan", Leaf: false, Runnable: true, Hidden: true},
	}
)

func TestClassify_SeparatesTheFourAnswers(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		want     cmdref.Availability
		answers  string
		wantExit int
	}{
		{name: "registered and listed", path: "migrate apply", want: cmdref.Available, answers: "migrate apply", wantExit: 0},
		{name: "registered and hidden", path: "migrate rm", want: cmdref.Refused, answers: "migrate rm", wantExit: 1},
		{name: "below a hidden group", path: "schema plan lint", want: cmdref.RefusedByGroup, answers: "schema plan", wantExit: 1},
		{name: "below a runnable group", path: "migrate ls", want: cmdref.AbsorbedByGroup, answers: "migrate", wantExit: 0},
		{name: "below the root", path: "script exec", want: cmdref.UnknownCommand, answers: "script", wantExit: 1},
		{name: "the group itself", path: "script", want: cmdref.UnknownCommand, answers: "script", wantExit: 1},
	}

	classified := make(map[string]cmdref.Path)
	for _, path := range cmdref.Classify(full, strict) {
		classified[path.Name] = path
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := classified[test.path]

			c.Assert(path.Availability, qt.Equals, test.want)
			c.Assert(path.Answers, qt.Equals, test.answers)
			c.Assert(path.Availability.Exit(), qt.Equals, test.wantExit)
		})
	}
}

// TestClassify_CountsTheRealPoliciesApart is the measurement the reference
// prints, asserted where it can fail loudly.
//
// 12 + 10 + 2 + 4. The two that exit 0 are the ones a reader most needs told,
// because a caller testing only the status cannot tell them from success, and
// they are the pair a change to the policy is most likely to move.
func TestClassify_CountsTheRealPoliciesApart(t *testing.T) {
	tests := []struct {
		name  string
		class cmdref.Availability
		want  int
	}{
		{name: "refused by its own gate", class: cmdref.Refused, want: 12},
		{name: "refused by its group", class: cmdref.RefusedByGroup, want: 10},
		{name: "absorbed by the group", class: cmdref.AbsorbedByGroup, want: 2},
		{name: "unknown command", class: cmdref.UnknownCommand, want: 4},
	}

	counted := make(map[cmdref.Availability]int)
	for _, path := range cmdref.Classify(compatNodes(atlascompatpolicy.Full()), compatNodes(atlascompatpolicy.StrictCE())) {
		counted[path.Availability]++
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(counted[test.class], qt.Equals, test.want)
		})
	}
}

func TestRendering_HappyPath(t *testing.T) {
	c := qt.New(t)

	commands, err := cmdref.Commands(cmdref.Surface{Program: "ptah-compat", Nodes: full})
	c.Assert(err, qt.IsNil)
	c.Assert(commands, qt.Contains, "| `ptah-compat migrate` | — | group |\n")
	c.Assert(commands, qt.Contains, "| `ptah-compat script exec` | — | — |\n")

	table, err := cmdref.StrictCompat("ptah-compat", cmdref.Classify(full, strict))
	c.Assert(err, qt.IsNil)
	c.Assert(table, qt.Contains, "| `ptah-compat migrate ls` | absorbed by the group | `0` | stdout | `ptah-compat migrate` |\n")
	// Only what strict mode removes has a row; a second listing of what stays
	// could disagree with the command table.
	c.Assert(table, qt.Not(qt.Contains), "migrate apply")
	c.Assert(strings.Count(table, "\n"), qt.Equals, 8) // two header lines and six removed paths

	flags, err := cmdref.Flags([]cmdref.Surface{{Program: "ptah", Nodes: []agentsurface.Node{{
		Name:  "seed",
		Flags: []agentsurface.Flag{{Name: "env", Type: "string", Default: "a|b", Environment: "PTAH_ENV", Persistent: true, Hidden: true}},
	}}}})
	c.Assert(err, qt.IsNil)
	// The default carries the one character a table row cannot, and both notes
	// are on at once.
	c.Assert(flags, qt.Contains,
		"| `ptah seed` | `--env` | `string` | `a\\|b` | `PTAH_ENV` | inherited by subcommands, hidden |\n")
}

// TestRendering_FailurePath pins the refusal that keeps this gate from
// comparing nothing to nothing.
func TestRendering_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		render  func() (string, error)
		wantErr string
	}{
		{
			name:    "a command tree that walked to nothing",
			render:  func() (string, error) { return cmdref.Commands(cmdref.Surface{Program: "ptah"}) },
			wantErr: `cmdref: the command tree of ptah is empty; refusing to render a reference that names nothing`,
		},
		{
			name:    "a tree whose every command registers no flag",
			render:  func() (string, error) { return cmdref.Flags([]cmdref.Surface{{Program: "ptah", Nodes: full}}) },
			wantErr: `cmdref: the flag set of ptah is empty; refusing to render a reference that names nothing`,
		},
		{
			// A classified surface promises every leaf carries a
			// classification. Rendering Short for the ones that do not would
			// mix 99-character sentences with 46-character ones and say
			// nothing about which is which.
			name: "a classified leaf the classification does not name",
			render: func() (string, error) {
				return cmdref.Commands(cmdref.Surface{Program: "ptah", Nodes: full, Classified: true})
			},
			wantErr: `cmdref: ptah migrate apply is a leaf with no classification; refusing to render a ` +
				`reference whose rows come from two depths without saying which`,
		},
		{
			name:    "a strict policy that removed nothing",
			render:  func() (string, error) { return cmdref.StrictCompat("ptah-compat", cmdref.Classify(full, full)) },
			wantErr: `cmdref: the set of paths strict compatibility mode removes is empty; refusing to render a reference that names nothing`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			rendered, err := test.render()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(rendered, qt.Equals, "")
		})
	}
}

func compatNodes(policy atlascompatpolicy.Policy) []agentsurface.Node {
	return agentsurface.Nodes(atlas.NewCompatCommandWithPolicy("ptah-compat", policy))
}
