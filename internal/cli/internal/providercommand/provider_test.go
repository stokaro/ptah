package providercommand_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

func dockerDevURLManifest(c *qt.C) string {
	c.Helper()
	executable := filepath.Join(c.TempDir(), "missing-provider")
	manifest := `{"format_version":2,"providers":[{"executable":` + strconv.Quote(executable) + `,"sha256":"` + strings.Repeat("a", 64) +
		`","provider_id":"example.org/fixture","engines":[{"id":"fixture"}]}]}`
	path := filepath.Join(c.TempDir(), "providers.json")
	c.Assert(os.WriteFile(path, []byte(manifest), 0o600), qt.IsNil)
	return path
}

func TestDockerDevURLSelectsTheProviderOfItsEngine(t *testing.T) {
	c := qt.New(t)
	calls := 0
	cmd := providercommand.Configure(&cobra.Command{Use: "test", RunE: func(*cobra.Command, []string) error { calls++; return nil }}, func(_ *cobra.Command, err error) error { return err })
	cmd.Flags().String("dev-url", "", "")
	cmd.SetArgs([]string{"--provider-config", dockerDevURLManifest(c), "--dev-url", "docker://fixture/1/dev"})
	// The provider is selected, so its missing executable stops the command.
	c.Assert(cmd.ExecuteContext(c.Context()), qt.ErrorMatches, `The configured provider executable was not found.*`)
	c.Assert(calls, qt.Equals, 0)
}

func TestDockerDevURLOfAnotherEngineIsNotOwnedBySelectedProvider(t *testing.T) {
	c := qt.New(t)
	calls := 0
	cmd := providercommand.Configure(&cobra.Command{Use: "test", RunE: func(*cobra.Command, []string) error { calls++; return nil }}, func(_ *cobra.Command, err error) error { return err })
	cmd.Flags().String("dev-url", "", "")
	cmd.Flags().String("db-url", "", "")
	cmd.SetArgs([]string{"--provider-config", dockerDevURLManifest(c), "--provider-engine", "fixture", "--dev-url", "docker://other/1"})
	c.Assert(cmd.ExecuteContext(c.Context()), qt.ErrorMatches, `(?s).*not owned by the selected provider.*`)
	c.Assert(calls, qt.Equals, 0)
}
