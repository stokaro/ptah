package secretrotation_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/config"
	"ptah.run/core/schemaext"
	"ptah.run/internal/cli/internal/secretrotation"
)

func rotationCommand(c *qt.C, args ...string) *cobra.Command {
	c.Helper()
	cmd := &cobra.Command{Use: "plan", RunE: func(*cobra.Command, []string) error { return nil }}
	secretrotation.Register(cmd)
	c.Assert(cmd.ParseFlags(args), qt.IsNil)
	return cmd
}

// TestApply sends each path the operator named as written, for the owner of
// the engine's secrets to read, without its surrounding space and once.
func TestApply(t *testing.T) {
	c := qt.New(t)
	opts := &config.CompareOptions{}

	err := secretrotation.Apply(rotationCommand(c, "--rotate-secret", " ext/pg ", "--rotate-secret", "ext/pg", "--rotate-secret", "pw"), opts)

	c.Assert(err, qt.IsNil)
	c.Assert(opts.FeatureRequests, qt.DeepEquals, []schemaext.ChangeRequest{
		{Written: "ext/pg", Action: schemaext.RotateAction}, {Written: "pw", Action: schemaext.RotateAction},
	})
}
