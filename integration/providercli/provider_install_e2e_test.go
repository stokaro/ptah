//go:build integration

package providercli_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerarchive/archivetest"
	"ptah.run/internal/providerinstall"
	"ptah.run/internal/providerinstall/releasetest"
	"ptah.run/internal/providerrelease"
	"ptah.run/internal/testutils"
)

// installReport is the part of a `providers install --json` or `providers
// check --json` report these tests read.
type installReport struct {
	Status      string                   `json:"status"`
	Version     string                   `json:"version"`
	MinContract providerrelease.Contract `json:"min_contract"`
	Executable  string                   `json:"executable"`
	SHA256      string                   `json:"sha256"`
	Provider    struct {
		ProviderID string `json:"provider_id"`
	} `json:"provider"`
	Diagnostic struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"diagnostic"`
}

func runJSON(c *qt.C, args ...string) (installReport, int, string) {
	c.Helper()
	out, diagnostics, code := executeProviderCLI(c.Context(), append(args, "--json"))
	var report installReport
	c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil, qt.Commentf("stdout %q, stderr %q", out, diagnostics))
	return report, code, diagnostics
}

// ydbRelease is a release of the official YDB provider that carries the fixture
// executable for the running platform.
func ydbRelease(version string, minimum providerrelease.Contract, executable []byte) releasetest.Release {
	return releasetest.ProviderRelease(releasetest.Spec{
		ProviderID: "github.com/stokaro/ptah-ydb-plugin", Engine: "ydb", Version: version, MinContract: minimum,
		Platforms: []providerrelease.Platform{providerrelease.Running()}, Executable: executable,
	})
}

// TestProvidersInstallFromAMirror installs the fixture provider from a fake
// release mirror through the shipped command, selects it from the user store,
// and starts it.
func TestProvidersInstallFromAMirror(t *testing.T) {
	c := qt.New(t)
	// The build runs the go command, which reads HOME, so it comes before the
	// user cache moves to an empty directory.
	binary, err := os.ReadFile(testutils.BuildFixtureProvider(c, "ydb-release"))
	c.Assert(err, qt.IsNil)
	c.Setenv("XDG_CACHE_HOME", c.TempDir())
	c.Setenv("HOME", c.TempDir())
	c.Setenv("LocalAppData", c.TempDir())
	host := providerrelease.HostContract()
	tampered := ydbRelease("0.2.1", host, binary)
	tampered.Assets[releasetest.ArchiveName(releasetest.Spec{Engine: "ydb", Version: "0.2.1"}, providerrelease.Running())] = archivetest.TarGz(
		archivetest.Entry{Name: "ptah-plugin-ydb", Body: "not the published executable"})
	srv := releasetest.New("stokaro", "ptah-ydb-plugin")
	c.Cleanup(srv.Close)
	srv.Add(
		ydbRelease("0.3.0", providerrelease.Contract{Major: host.Major + 1}, binary),
		tampered,
		ydbRelease("0.2.0", host, binary),
	)
	root, err := providerinstall.DefaultRoot()
	c.Assert(err, qt.IsNil)

	installed, code, diagnostics := runJSON(c, "providers", "install", "ydb", "--version", "0.2.0", "--download-base-url", srv.MirrorURL())
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(installed.Status, qt.Equals, "installed")
	c.Assert(installed.Version, qt.Equals, "0.2.0")
	c.Assert(installed.MinContract, qt.Equals, host)
	c.Assert(installed.SHA256, qt.Equals, archivetest.SHA256(binary))
	stored := filepath.Join(providerinstall.Dir(root, "github.com/stokaro/ptah-ydb-plugin", "0.2.0", providerrelease.Running()),
		releasetest.ExecutableName(releasetest.Spec{Engine: "ydb"}, providerrelease.Running()))
	c.Assert(installed.Executable, qt.Equals, stored)

	t.Run("the environment names the mirror and the store is already current", func(t *testing.T) {
		c := qt.New(t)
		c.Setenv("PTAH_DOWNLOAD_BASE_URL", srv.MirrorURL())
		again, code, diagnostics := runJSON(c, "providers", "install", "ydb", "--version", "v0.2.0")
		c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
		c.Assert(again.Status, qt.Equals, "already_installed")
		c.Assert(again.Executable, qt.Equals, installed.Executable)
	})
	t.Run("a release this Ptah cannot meet is refused by name", func(t *testing.T) {
		c := qt.New(t)
		refused, code, _ := runJSON(c, "providers", "install", "ydb", "--version", "0.3.0", "--download-base-url", srv.MirrorURL())
		c.Assert(code, qt.Not(qt.Equals), 0)
		c.Assert(refused.Diagnostic.Code, qt.Equals, "provider_release_incompatible")
		c.Assert(refused.Diagnostic.Message, qt.Contains,
			"the ydb provider 0.3.0 needs contract "+providerrelease.Contract{Major: host.Major + 1}.Requirement()+"; this Ptah speaks contract "+host.String())
	})
	t.Run("an archive that differs from checksums.txt is refused and the store keeps its release", func(t *testing.T) {
		c := qt.New(t)
		refused, code, _ := runJSON(c, "providers", "install", "ydb", "--version", "0.2.1", "--download-base-url", srv.MirrorURL())
		c.Assert(code, qt.Not(qt.Equals), 0)
		c.Assert(refused.Diagnostic.Code, qt.Equals, "provider_checksum_mismatch")
		record, _, err := providerinstall.ReadRecord(root, "github.com/stokaro/ptah-ydb-plugin", providerrelease.Running())
		c.Assert(err, qt.IsNil)
		c.Assert(record.Version, qt.Equals, "0.2.0")
	})
	t.Run("a mirror without a version is refused before any request", func(t *testing.T) {
		c := qt.New(t)
		before := len(srv.Requests())
		refused, code, _ := runJSON(c, "providers", "install", "ydb", "--download-base-url", srv.MirrorURL())
		c.Assert(code, qt.Not(qt.Equals), 0)
		c.Assert(refused.Diagnostic.Code, qt.Equals, "provider_version_required")
		c.Assert(srv.Requests(), qt.HasLen, before)
	})
	t.Run("the stored installation is selected and starts", func(t *testing.T) {
		c := qt.New(t)
		checked, code, diagnostics := runJSON(c, "providers", "check", "--provider-engine", "ydb")
		c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
		c.Assert(checked.Status, qt.Equals, "compatible")
		c.Assert(checked.Executable, qt.Equals, installed.Executable)
		c.Assert(checked.Provider.ProviderID, qt.Equals, "github.com/stokaro/ptah-ydb-plugin")
	})
	t.Run("a changed stored executable is refused before it starts", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(os.WriteFile(stored, append(binary, 0), 0o600), qt.IsNil) // #nosec G703 -- stored is the user store this test placed below c.TempDir()
		refused, code, _ := runJSON(c, "providers", "check", "--provider-engine", "ydb")
		c.Assert(code, qt.Not(qt.Equals), 0)
		c.Assert(refused.Diagnostic.Code, qt.Equals, "provider_untrusted")
		c.Assert(refused.Diagnostic.Message, qt.Contains, "install record")
	})
}
