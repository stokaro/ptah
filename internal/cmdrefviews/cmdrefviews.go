// Package cmdrefviews names the four command-reference renderings and builds
// the surfaces each one measures.
//
// This is the second non-test file outside cmd/atlas that imports cmd/atlas,
// and the exception is deliberate: see AGENTS.md, "Dependency direction". A
// documentation generator that measures BOTH surfaces is not the native product
// depending on the compatibility product, and the direction the rule protects
// -- shared capability below, two adapters above -- is untouched. The reason
// travels with the import: it was written above `internal/cmd/cmdref`'s package
// clause while that file held this wiring, and moved here with it.
//
// It is a package rather than a main so that both the contributor command and
// the documentation sync run the same renderings in one process
// (stokaro/ptah#2510).
package cmdrefviews

import (
	"fmt"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/root"
	"go.5x5.cz/ptah/internal/agentsurface"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	"go.5x5.cz/ptah/internal/cmdref"
)

// Views are the renderings, in the order the usage text lists them.
var Views = []string{"native", "compat", "strict", "flags"}

// Render returns one rendering by name.
func Render(view string) (string, error) {
	switch view {
	case "native":
		return cmdref.Commands(NativeSurface())
	case "compat":
		return cmdref.Commands(Surface(Compat(atlascompatpolicy.Full())))
	case "strict":
		full := Surface(Compat(atlascompatpolicy.Full()))
		strict := Surface(Compat(atlascompatpolicy.StrictCE()))
		if len(full.Nodes) == 0 || len(strict.Nodes) == 0 {
			return "", fmt.Errorf("cmdref: a compatibility tree walked to nothing; refusing to classify it")
		}
		return cmdref.StrictCompat(full.Program, cmdref.Classify(full.Nodes, strict.Nodes))
	case "flags":
		return FlagsPage()
	default:
		return "", fmt.Errorf("cmdref: unknown view %q", view)
	}
}

// Surface walks one command tree.
func Surface(tree *cobra.Command) cmdref.Surface {
	return cmdref.Surface{Program: tree.Name(), Nodes: agentsurface.Nodes(tree)}
}

// NativeSurface is the only classified one: internal/agentsurface classifies
// the `ptah` verbs and nothing classifies the compatibility tree.
func NativeSurface() cmdref.Surface {
	native := Surface(root.NewRootCommand())
	native.Classified = true
	return native
}

// Compat builds the compatibility tree under one policy.
func Compat(policy atlascompatpolicy.Policy) *cobra.Command {
	return atlas.NewCompatCommandWithPolicy("ptah-compat", policy)
}

// FlagsPage is the whole flag reference. Both the tables and the prose around
// them live in internal/cmdref, so a test can measure what the prose claims;
// this function names the two surfaces and nothing else.
func FlagsPage() (string, error) {
	return cmdref.FlagsPage([]cmdref.Surface{
		NativeSurface(),
		Surface(Compat(atlascompatpolicy.Full())),
	})
}
