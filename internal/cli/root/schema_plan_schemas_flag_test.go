package root_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"ptah.run/internal/cli/root"
)

// TestSchemaPlanSchemasFlagIsSchemaApplys holds `schema plan --schemas` to the
// flag `schema apply` declares: the same help line, type, default and
// environment variable. `schema apply --plan` runs what `schema plan` saved,
// so the two have to name a scope the same way.
func TestSchemaPlanSchemasFlagIsSchemaApplys(t *testing.T) {
	c := qt.New(t)
	cmd := root.NewRootCommand()

	plan := schemasFlagOf(c, cmd, "schema", "plan")
	apply := schemasFlagOf(c, cmd, "schema", "apply")

	c.Assert(plan.Usage, qt.Equals, apply.Usage)
	c.Assert(plan.Usage, qt.Contains, "[env: PTAH_SCHEMAS]")
	c.Assert(plan.Value.Type(), qt.Equals, apply.Value.Type())
	c.Assert(plan.DefValue, qt.Equals, apply.DefValue)
}

// schemasFlagOf is the --schemas flag of the command at path below tree.
func schemasFlagOf(c *qt.C, tree *cobra.Command, path ...string) *pflag.Flag {
	c.Helper()
	found, rest, err := tree.Find(path)
	c.Assert(err, qt.IsNil)
	c.Assert(rest, qt.HasLen, 0)
	c.Assert(found.Name(), qt.Equals, path[len(path)-1])
	flag := found.Flags().Lookup("schemas")
	c.Assert(flag, qt.IsNotNil)
	return flag
}
