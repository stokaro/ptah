package providercommand_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/providercommand"
	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerinstall"
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

// corruptPinnedStoreCopy installs bytes that do not match the pin where the
// pinned YDB provider belongs in the user store.
func corruptPinnedStoreCopy(c *qt.C) string {
	c.Helper()
	pin, found := providercatalog.LookupPin("ydb")
	c.Assert(found, qt.IsTrue)
	entry, found := providercatalog.Lookup("ydb")
	c.Assert(found, qt.IsTrue)
	platform := providercatalog.Platform{OS: runtime.GOOS, Arch: runtime.GOARCH}
	artifact, found := pin.Artifact(platform)
	c.Assert(found, qt.IsTrue)
	root, err := providerinstall.DefaultRoot()
	c.Assert(err, qt.IsNil)
	path := filepath.Join(providerinstall.Dir(root, entry.ProviderID, pin.Version, platform), artifact.Executable)
	c.Assert(os.MkdirAll(filepath.Dir(path), 0o700), qt.IsNil)
	c.Assert(os.WriteFile(path, []byte("not the pinned provider"), 0o600), qt.IsNil)
	return path
}

func newProbe(calls *int) *cobra.Command {
	cmd := providercommand.Configure(&cobra.Command{Use: "test", RunE: func(*cobra.Command, []string) error { *calls++; return nil }}, func(_ *cobra.Command, err error) error { return err })
	cmd.Flags().String("db-url", "", "")
	return cmd
}

// The pinned YDB provider is selectable only by asking for it. A URL alone
// keeps the built-in engine and never reads the bundled directory or the store.
func TestOfficialEngineWithABuiltInImplementationIgnoresTheStoreWithoutAnExplicitRequest(t *testing.T) {
	c := qt.New(t)
	isolateStore(c)
	corruptPinnedStoreCopy(c)
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
	c.Assert(cmd.ExecuteContext(c.Context()), qt.ErrorMatches, `(?s).*no bundled or installed provider was found.*ptah providers install ydb.*`)
	c.Assert(calls, qt.Equals, 0)
}

func TestExplicitProviderEngineRefusesACorruptStoreCopy(t *testing.T) {
	c := qt.New(t)
	isolateStore(c)
	corruptPinnedStoreCopy(c)
	calls := 0
	cmd := newProbe(&calls)
	cmd.SetArgs([]string{"--provider-engine", "ydb"})
	c.Assert(cmd.ExecuteContext(c.Context()), qt.ErrorMatches, `(?s).*installed provider executable does not match the SHA-256 checksum pinned.*ptah providers install ydb.*`)
	c.Assert(calls, qt.Equals, 0)
}
