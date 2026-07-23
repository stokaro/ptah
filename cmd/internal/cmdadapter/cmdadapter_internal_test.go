package cmdadapter

// White-box testing required: resetCommandFlags and explicitFlagNames are
// unexported primitives that protect forwarding-command flag reuse semantics.
// Their edge cases cannot be isolated through exported commands without
// coupling tests to larger Cobra execution flows.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"
)

func TestResetCommandFlagsStringArrayUsesDeclaredDefault(t *testing.T) {
	c := qt.New(t)

	target := newStringArrayTargetCommand([]string{"prod", "production"})
	c.Assert(target.Flags().Set("value", "staging"), qt.IsNil)
	values, err := target.Flags().GetStringArray("value")
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.DeepEquals, []string{"staging"})

	resetCommandFlags(target)

	values, err = target.Flags().GetStringArray("value")
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.DeepEquals, []string{"prod", "production"})
	c.Assert(target.Flags().Lookup("value").Changed, qt.IsFalse)
}

func TestExplicitFlagNamesIncludesShorthandClusters(t *testing.T) {
	c := qt.New(t)

	names := explicitFlagNames([]string{"-ab", "--value=qa", "--", "--ignored"})

	c.Assert(names, qt.DeepEquals, map[string]struct{}{
		"a":     {},
		"b":     {},
		"value": {},
	})
}

func newStringArrayTargetCommand(defaults []string) *cobra.Command {
	cmd := &cobra.Command{Use: "target"}
	cmd.Flags().StringArray("value", defaults, "Values to collect")
	return cmd
}
