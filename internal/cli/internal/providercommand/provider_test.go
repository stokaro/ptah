package providercommand_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/providercommand"
)

func TestProviderSelectionRequiresAnExplicitFlag(t *testing.T) {
	c := qt.New(t)
	c.Setenv("PTAH_PROVIDER_CONFIG", "missing.json")
	calls := 0
	cmd := providercommand.Configure(&cobra.Command{Use: "test", RunE: func(*cobra.Command, []string) error { calls++; return nil }}, func(_ *cobra.Command, err error) error { return err })
	cmd.SetArgs(nil)
	c.Assert(cmd.ExecuteContext(c.Context()), qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	cmd.SetArgs([]string{"--provider-config="})
	c.Assert(cmd.ExecuteContext(c.Context()), qt.IsNotNil)
	c.Assert(calls, qt.Equals, 1)
}
