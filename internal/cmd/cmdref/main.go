// Command cmdref prints the generated half of the command reference: every
// command path both Ptah binaries ship, every flag they register, and what
// PTAH_ATLAS_STRICT_COMPAT=1 takes away from the compatibility surface.
//
// It constructs both trees in this process. Shelling out to a built binary
// would measure whichever one was on PATH, and reading `--help` output would
// measure a rendering rather than the tree that produced it.
//
// This is the second non-test file outside cmd/atlas that imports cmd/atlas,
// and the exception is deliberate: see AGENTS.md, "Dependency direction". A
// documentation generator that measures both surfaces is not the native
// product depending on the compatibility product, and the direction the rule
// protects -- shared capability below, two adapters above -- is untouched.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/root"
	"go.5x5.cz/ptah/internal/agentsurface"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	"go.5x5.cz/ptah/internal/cmdref"
)

const usage = `usage: cmdref native|compat|strict|flags

  native   the ptah command table
  compat   the ptah-compat command table
  strict   what PTAH_ATLAS_STRICT_COMPAT=1 removes from ptah-compat
  flags    the whole flag reference page, both binaries
`

func main() {
	if len(os.Args) != 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	rendered, err := render(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Print(rendered)
}

func render(view string) (string, error) {
	switch view {
	case "native":
		return cmdref.Commands(nativeSurface())
	case "compat":
		return cmdref.Commands(surface(compat(atlascompatpolicy.Full())))
	case "strict":
		full := surface(compat(atlascompatpolicy.Full()))
		strict := surface(compat(atlascompatpolicy.StrictCE()))
		if len(full.Nodes) == 0 || len(strict.Nodes) == 0 {
			return "", fmt.Errorf("cmdref: a compatibility tree walked to nothing; refusing to classify it")
		}
		return cmdref.StrictCompat(full.Program, cmdref.Classify(full.Nodes, strict.Nodes))
	case "flags":
		return flagsPage()
	default:
		return "", fmt.Errorf("cmdref: unknown view %q\n%s", view, usage)
	}
}

func surface(tree *cobra.Command) cmdref.Surface {
	return cmdref.Surface{Program: tree.Name(), Nodes: agentsurface.Nodes(tree)}
}

// nativeSurface is the only classified one: internal/agentsurface classifies
// the `ptah` verbs and nothing classifies the compatibility tree.
func nativeSurface() cmdref.Surface {
	native := surface(root.NewRootCommand())
	native.Classified = true
	return native
}

func compat(policy atlascompatpolicy.Policy) *cobra.Command {
	return atlas.NewCompatCommandWithPolicy("ptah-compat", policy)
}

// flagsPage is the whole flag reference. Both the tables and the prose around
// them live in internal/cmdref, so a test can measure what the prose claims;
// this function names the two surfaces and nothing else.
func flagsPage() (string, error) {
	return cmdref.FlagsPage([]cmdref.Surface{
		nativeSurface(),
		surface(compat(atlascompatpolicy.Full())),
	})
}
