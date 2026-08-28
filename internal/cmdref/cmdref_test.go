package cmdref_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/root"
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

// nativeSurface is the tree the reference's native half is rendered from, in
// the shape the generator hands over.
func nativeSurface() cmdref.Surface {
	return cmdref.Surface{Program: "ptah", Nodes: agentsurface.Nodes(root.NewRootCommand()), Classified: true}
}

// flagRow is one (command, flag) pair, which is the shape both the page and
// these assertions are indexed by.
type flagRow struct {
	command string
	flag    agentsurface.Flag
}

// approvalRows selects the flags the page's environment-variable paragraph
// makes a claim about. It builds the data a table-driven test iterates; it
// chooses nothing about how a row is asserted.
func approvalRows(nodes []agentsurface.Node) []flagRow {
	rows := make([]flagRow, 0, len(nodes))
	for _, node := range nodes {
		for _, flag := range node.Flags {
			if flag.Name == "auto-approve" || flag.Name == "allow-database-inspect" {
				rows = append(rows, flagRow{command: node.Name, flag: flag})
			}
		}
	}
	return rows
}

// TestFlagsPage_NoNativeApprovalFlagReadsAVariable is the first half of the
// claim the page's environment-variable paragraph makes.
//
// The paragraph it replaced made the claim about every binary: "Every
// `--auto-approve` and every `--allow-database-inspect` is one of those:
// approval is not something a script can grant by exporting a variable." The
// same page's rows contradicted it four times over, so the claim is now about
// the native surface, where it holds and where cmdflags.DisableEnvBinding is
// what makes it hold.
func TestFlagsPage_NoNativeApprovalFlagReadsAVariable(t *testing.T) {
	rows := approvalRows(agentsurface.Nodes(root.NewRootCommand()))

	// A selection that matched nothing would agree with any claim at all,
	// including the one this test exists to have found false.
	qt.New(t).Assert(len(rows) > 0, qt.IsTrue)

	for _, row := range rows {
		t.Run(row.command+" --"+row.flag.Name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(row.flag.Environment, qt.Equals, "")
		})
	}
}

// TestFlagsPage_TheCompatibilityCounterexampleIsReal is the other half, and it
// is not decoration.
//
// Without it, deleting the compatibility binding would leave the page's second
// clause false and this suite green, and narrowing the claim to the native
// surface would read as caution rather than as a measurement. The binding is
// live rather than an annotation on the usage string:
// `PTAH_AUTO_APPROVE=notabool ptah-compat schema plan new` answers `invalid
// boolean value "notabool" for PTAH_AUTO_APPROVE` and exits 1, where the same
// run with the variable unset gets as far as `--from is required`.
func TestFlagsPage_TheCompatibilityCounterexampleIsReal(t *testing.T) {
	c := qt.New(t)

	indexed := make(map[string]agentsurface.Flag)
	for _, row := range approvalRows(compatNodes(atlascompatpolicy.Full())) {
		indexed[row.command+" --"+row.flag.Name] = row.flag
	}
	flag, registered := indexed["schema plan --auto-approve"]

	c.Assert(registered, qt.IsTrue)
	c.Assert(flag.Environment, qt.Equals, "PTAH_AUTO_APPROVE")
}

// TestFlagsPage_SaysBothHalves keeps the prose and the rows on one page from
// disagreeing, which is the defect the two tests above measure the ground for.
func TestFlagsPage_SaysBothHalves(t *testing.T) {
	c := qt.New(t)

	page, err := cmdref.FlagsPage([]cmdref.Surface{
		nativeSurface(),
		{Program: "ptah-compat", Nodes: compatNodes(atlascompatpolicy.Full())},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(page, qt.Contains, "no `--auto-approve` and no")
	c.Assert(page, qt.Contains, "surface's `ptah-compat schema plan` verbs do bind one")
	c.Assert(page, qt.Contains,
		"| `ptah-compat schema plan` | `--auto-approve` | `bool` | `false` | `PTAH_AUTO_APPROVE` | — |\n")
	c.Assert(page, qt.Contains,
		"| `ptah schema apply` | `--auto-approve` | `bool` | `false` | — | — |\n")
}

// TestCommands_ALeafCellIsItsClassification pins where the native index's cells
// come from, because the page says so in prose and said the opposite for one
// release.
//
// It read "the command's own one-line description — the line `ptah --help`
// prints beside it — so it is short by construction". Measured over the whole
// block, 11 of the 102 cells equal cobra's Short and they are exactly the 11
// group rows; the other 91 are the classification, which averages 99
// characters against Short's 45. The sentence had been carried over from the
// compatibility page, where every cell really is Short because nothing
// classifies that tree.
func TestCommands_ALeafCellIsItsClassification(t *testing.T) {
	rendered, indexed := renderedNativeIndex(qt.New(t))

	for _, name := range []string{"db read", "schema render", "migrations up"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			verb, classified := agentsurface.Lookup(name)
			node := indexed[name]

			c.Assert(classified, qt.IsTrue)
			c.Assert(node.Leaf, qt.IsTrue)
			// Derived rather than restated: everything after the first rune,
			// which the renderer capitalizes and nothing else touches.
			c.Assert(rendered, qt.Contains, verb.Reason[1:]+" |")
			// The two really are different strings, so the assertion above
			// cannot be satisfied by Short.
			c.Assert(verb.Reason, qt.Not(qt.Equals), node.Summary)
			c.Assert(rendered, qt.Not(qt.Contains), "| `ptah "+name+"` | "+node.Summary+" |")
		})
	}
}

// TestCommands_AGroupCellIsItsShort is the other half of the same sentence.
func TestCommands_AGroupCellIsItsShort(t *testing.T) {
	rendered, indexed := renderedNativeIndex(qt.New(t))

	for _, name := range []string{"db", "schema", "migrations"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			node := indexed[name]

			c.Assert(node.Leaf, qt.IsFalse)
			c.Assert(rendered, qt.Contains, "| `ptah "+name+"` | "+node.Summary+" | group |\n")
		})
	}
}

// renderedNativeIndex renders the native command table and indexes the nodes it
// came from, so an assertion can be made against both.
func renderedNativeIndex(c *qt.C) (string, map[string]agentsurface.Node) {
	surface := nativeSurface()
	rendered, err := cmdref.Commands(surface)
	c.Assert(err, qt.IsNil)

	indexed := make(map[string]agentsurface.Node, len(surface.Nodes))
	for _, node := range surface.Nodes {
		indexed[node.Name] = node
	}
	return rendered, indexed
}
