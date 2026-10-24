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
	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerinstall"
	"ptah.run/internal/providerrelease"
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

// isolateStore points the user cache at an empty directory, so the user store
// holds nothing the developer installed.
func isolateStore(c *qt.C) {
	c.Helper()
	c.Setenv("XDG_CACHE_HOME", c.TempDir())
	c.Setenv("HOME", c.TempDir())
	c.Setenv("LocalAppData", c.TempDir())
}

// corruptStoreCopy installs a record for the YDB provider in the user store and
// an executable whose bytes do not match it.
func corruptStoreCopy(c *qt.C) string {
	c.Helper()
	entry, found := providercatalog.Lookup("ydb")
	c.Assert(found, qt.IsTrue)
	root, err := providerinstall.DefaultRoot()
	c.Assert(err, qt.IsNil)
	platform := providerrelease.Running()
	c.Assert(providerinstall.WriteRecord(root, platform, providerinstall.Record{
		Schema: providerinstall.RecordSchema, ProviderID: entry.ProviderID, Version: "0.2.0",
		MinContract: providerrelease.HostContract(), Engines: []string{"ydb"}, SHA256: strings.Repeat("a", 64),
		Executable: "0.2.0/" + platform.OS + "_" + platform.Arch + "/ptah-plugin-ydb",
	}), qt.IsNil)
	path := filepath.Join(providerinstall.Dir(root, entry.ProviderID, "0.2.0", platform), "ptah-plugin-ydb")
	c.Assert(os.MkdirAll(filepath.Dir(path), 0o700), qt.IsNil)
	c.Assert(os.WriteFile(path, []byte("not the recorded provider"), 0o600), qt.IsNil)
	return path
}

func newProbe(calls *int) *cobra.Command {
	cmd := providercommand.Configure(&cobra.Command{Use: "test", RunE: func(*cobra.Command, []string) error { *calls++; return nil }}, func(_ *cobra.Command, err error) error { return err })
	cmd.Flags().String("db-url", "", "")
	return cmd
}

// The YDB provider is selectable only by asking for it. A URL alone keeps the
// built-in engine and never reads the store.
func TestOfficialEngineWithABuiltInImplementationIgnoresTheStoreWithoutAnExplicitRequest(t *testing.T) {
	c := qt.New(t)
	isolateStore(c)
	corruptStoreCopy(c)
	calls := 0
	cmd := newProbe(&calls)
	cmd.SetArgs([]string{"--db-url", "ydb://localhost:2136/local"})
	c.Assert(cmd.ExecuteContext(c.Context()), qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
}

func TestExplicitProviderEngineWithNothingInstalledReportsTheInstallInstruction(t *testing.T) {
	c := qt.New(t)
	isolateStore(c)
	calls := 0
	cmd := newProbe(&calls)
	cmd.SetArgs([]string{"--provider-engine", "ydb"})
	c.Assert(cmd.ExecuteContext(c.Context()), qt.ErrorMatches, `(?s).*installed separately, and no installation was found.*ptah providers install ydb.*`)
	c.Assert(calls, qt.Equals, 0)
}

func TestExplicitProviderEngineRefusesACorruptStoreCopy(t *testing.T) {
	c := qt.New(t)
	isolateStore(c)
	corruptStoreCopy(c)
	calls := 0
	cmd := newProbe(&calls)
	cmd.SetArgs([]string{"--provider-engine", "ydb"})
	c.Assert(cmd.ExecuteContext(c.Context()), qt.ErrorMatches, `(?s).*installed provider executable does not match the SHA-256 digest its install record holds.*ptah providers install ydb.*`)
	c.Assert(calls, qt.Equals, 0)
}
