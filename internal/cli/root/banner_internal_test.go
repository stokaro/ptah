package root

// White-box testing required: helpDrawsBanner is the routing half of the help
// banner, and it is unobservable from outside. Cobra hands every help call to
// one function, and banner.Wanted answers false for every writer a test can
// construct, so a black-box assertion sees no banner whether the routing rule
// selects the root, selects nothing, or selects everything.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"
)

// The identity belongs to the entry screen. Cobra routes a child's help to the
// same function, so the rule has to say no to one and yes to the other; a rule
// that answered the same for both would be invisible behind the terminal gate.
func TestHelpDrawsBanner(t *testing.T) {
	root := NewRootCommand()

	t.Run("the root's own help", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(helpDrawsBanner(root, root), qt.IsTrue)
	})

	t.Run("a namespace's help", func(t *testing.T) {
		c := qt.New(t)
		children := root.Commands()
		c.Assert(len(children) > 0, qt.IsTrue,
			qt.Commentf("the root registers no subcommand, so this case has no subject"))
		c.Assert(helpDrawsBanner(root, children[0]), qt.IsFalse)
	})

	t.Run("a command from somewhere else", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(helpDrawsBanner(root, &cobra.Command{Use: "elsewhere"}), qt.IsFalse)
	})
}
