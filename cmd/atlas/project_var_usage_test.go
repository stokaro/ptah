package atlas_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/atlas"
)

// TestCompatVarFlagKeepsAtlasUsage pins the Atlas-compatible spelling of --var
// on the command groups that carry it.
//
// The native --env registration also registers --var, idempotently, so
// whichever surface registers first owns the flag's help text. This surface
// must own it: its --help is compared against Atlas's, and "Value for an
// atlas.hcl variable with no default…" is not what Atlas prints. Without this
// test, swapping the two registrations in registerAtlasProjectFlags is a
// silent change to published help.
func TestCompatVarFlagKeepsAtlasUsage(t *testing.T) {
	c := qt.New(t)

	root := atlas.NewCompatCommand("ptah-compat")
	for _, group := range []string{"schema", "migrate"} {
		cmd := childCommand(c, root, group)
		flag := cmd.PersistentFlags().Lookup("var")
		c.Assert(flag, qt.IsNotNil, qt.Commentf("%s must carry --var", group))
		// A prefix, not equality: the env-binding pass appends
		// " [env: PTAH_VAR]" to every registered flag's help text, and that
		// suffix is not what this test is about.
		c.Assert(strings.HasPrefix(flag.Usage, "input variables"), qt.IsTrue, qt.Commentf(
			"%s --var help is %q, not Atlas's wording", group, flag.Usage,
		))
		c.Assert(flag.Usage, qt.Not(qt.Contains), "atlas.hcl variable with no default")
		// The value type is the community binary's own spelling of this flag,
		// and it is not cosmetic: registering it as that type is what refuses a
		// `--var` carrying no `=` at parse time (stokaro/ptah#1231 case 7).
		c.Assert(flag.Value.Type(), qt.Equals, "<name>=<value>")
		c.Assert(flag.Hidden, qt.IsFalse)
	}
}

func childCommand(c *qt.C, parent *cobra.Command, name string) *cobra.Command {
	for _, child := range parent.Commands() {
		if child.Name() == name {
			return child
		}
	}
	c.Fatalf("command %q not found", name)
	return nil
}
