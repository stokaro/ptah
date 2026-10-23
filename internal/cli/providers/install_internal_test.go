package providers

// White-box testing required: the compiled pins name the real release's digests,
// so a fixture archive can satisfy them only through an installer whose pins and
// platform the test sets. The public constructor takes no installer by design.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerarchive/archivetest"
	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerinstall"
)

func TestInstallCommandReportsInstallThenNoOp(t *testing.T) {
	c := qt.New(t)
	t.Setenv("XDG_CACHE_HOME", c.TempDir())
	t.Setenv("HOME", c.TempDir())
	t.Setenv("LocalAppData", c.TempDir())
	platform := providercatalog.Platform{OS: "linux", Arch: "amd64"}
	archive := archivetest.TarGz(archivetest.Entry{Name: "ptah-plugin-ydb", Body: "binary"})
	name := providercatalog.ArchiveName("ydb", "1.0.0", platform)
	pin := providercatalog.Pin{EngineID: "ydb", Version: "1.0.0", Tag: "v1.0.0", Artifacts: []providercatalog.Artifact{{
		Platform: platform, Archive: name, ArchiveSHA256: archivetest.SHA256(archive),
		Executable: "ptah-plugin-ydb", ExecutableSHA256: archivetest.SHA256([]byte("binary")),
	}}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(archive) }))
	c.Cleanup(srv.Close)
	base := providerinstall.Installer{Platform: platform, Pins: func(string) (providercatalog.Pin, bool) { return pin, true }}

	run := func(args ...string) installReport {
		command := newInstallCommand(base)
		var out bytes.Buffer
		command.SetOut(&out)
		command.SetArgs(append(args, "--json", "--download-base-url", srv.URL))
		c.Assert(command.ExecuteContext(c.Context()), qt.IsNil)
		var report installReport
		c.Assert(json.Unmarshal(out.Bytes(), &report), qt.IsNil)
		return report
	}
	first := run("ydb")
	second := run("ydb")
	c.Assert(first.Status, qt.Equals, "installed")
	c.Assert(second.Status, qt.Equals, "already_installed")
	c.Assert(second.Executable, qt.Equals, first.Executable)
	c.Assert(first.SHA256, qt.Equals, archivetest.SHA256([]byte("binary")))
	c.Assert(first.Platform, qt.Equals, "linux/amd64")
	c.Assert(first.ProviderID, qt.Equals, "github.com/stokaro/ptah-ydb-plugin")
}

func TestInstallCommandPrintsTheExecutableAndDigest(t *testing.T) {
	c := qt.New(t)
	t.Setenv("XDG_CACHE_HOME", c.TempDir())
	t.Setenv("HOME", c.TempDir())
	t.Setenv("LocalAppData", c.TempDir())
	platform := providercatalog.Platform{OS: "linux", Arch: "amd64"}
	archive := archivetest.TarGz(archivetest.Entry{Name: "ptah-plugin-ydb", Body: "binary"})
	pin := providercatalog.Pin{EngineID: "ydb", Version: "1.0.0", Tag: "v1.0.0", Artifacts: []providercatalog.Artifact{{
		Platform: platform, Archive: providercatalog.ArchiveName("ydb", "1.0.0", platform), ArchiveSHA256: archivetest.SHA256(archive),
		Executable: "ptah-plugin-ydb", ExecutableSHA256: archivetest.SHA256([]byte("binary")),
	}}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(archive) }))
	c.Cleanup(srv.Close)
	command := newInstallCommand(providerinstall.Installer{Platform: platform, Pins: func(string) (providercatalog.Pin, bool) { return pin, true }})
	var out bytes.Buffer
	command.SetOut(&out)
	command.SetArgs([]string{"ydb", "--download-base-url", srv.URL})
	c.Assert(command.ExecuteContext(c.Context()), qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Installed github.com/stokaro/ptah-ydb-plugin 1.0.0 for linux/amd64 (ydb)")
	c.Assert(out.String(), qt.Contains, "sha256: "+archivetest.SHA256([]byte("binary")))
}
