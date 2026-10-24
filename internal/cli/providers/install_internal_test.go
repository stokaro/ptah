package providers

// White-box testing required: the shipped command lists releases through
// api.github.com, so a test reaches a fixture release server only through an
// installer whose release API, platform and host contract it sets. The public
// constructor takes no installer by design.

import (
	"bytes"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerarchive/archivetest"
	"ptah.run/internal/providerinstall"
	"ptah.run/internal/providerinstall/releasetest"
	"ptah.run/internal/providerrelease"
)

var testPlatform = providerrelease.Platform{OS: "linux", Arch: "amd64"}

// isolateStore points the user cache at an empty directory.
func isolateStore(c *qt.C) {
	c.Setenv("XDG_CACHE_HOME", c.TempDir())
	c.Setenv("HOME", c.TempDir())
	c.Setenv("LocalAppData", c.TempDir())
}

func releaseServer(c *qt.C) (*releasetest.Server, providerinstall.Installer) {
	srv := releasetest.New("stokaro", "ptah-ydb-plugin")
	c.Cleanup(srv.Close)
	for _, release := range []struct {
		version string
		minor   uint32
	}{{"1.1.0", 2}, {"1.0.0", 0}} {
		srv.Add(releasetest.ProviderRelease(releasetest.Spec{
			ProviderID: "github.com/stokaro/ptah-ydb-plugin", Engine: "ydb", Version: release.version,
			MinContract: providerrelease.Contract{Major: 25, Minor: release.minor},
			Platforms:   []providerrelease.Platform{testPlatform}, Executable: []byte("binary " + release.version),
		}))
	}
	return srv, providerinstall.Installer{APIURL: srv.APIURL(), Platform: testPlatform, Host: providerrelease.Contract{Major: 25, Minor: 1}}
}

func TestInstallCommandReportsInstallThenNoOp(t *testing.T) {
	c := qt.New(t)
	isolateStore(c)
	_, base := releaseServer(c)
	run := func(args ...string) installReport {
		command := newInstallCommand(base)
		var out bytes.Buffer
		command.SetOut(&out)
		command.SetArgs(append(args, "--json"))
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
	c.Assert(first.Version, qt.Equals, "1.0.0")
	c.Assert(first.MinContract, qt.Equals, providerrelease.Contract{Major: 25})
	c.Assert(first.SHA256, qt.Equals, archivetest.SHA256([]byte("binary 1.0.0")))
	c.Assert(first.Platform, qt.Equals, "linux/amd64")
	c.Assert(first.ProviderID, qt.Equals, "github.com/stokaro/ptah-ydb-plugin")
}

func TestInstallCommandPrintsTheExecutableAndDigest(t *testing.T) {
	c := qt.New(t)
	isolateStore(c)
	srv, base := releaseServer(c)
	command := newInstallCommand(base)
	var out bytes.Buffer
	command.SetOut(&out)
	command.SetArgs([]string{"ydb", "--version", "v1.0.0", "--download-base-url", srv.MirrorURL()})
	c.Assert(command.ExecuteContext(c.Context()), qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Installed github.com/stokaro/ptah-ydb-plugin 1.0.0 for linux/amd64 (ydb), minimum contract 25.0\n")
	c.Assert(out.String(), qt.Contains, "sha256: "+archivetest.SHA256([]byte("binary 1.0.0")))
}

func TestInstallCommandRefusesAVersionTheHostCannotMeet(t *testing.T) {
	c := qt.New(t)
	isolateStore(c)
	srv, base := releaseServer(c)
	command := newInstallCommand(base)
	var out, stderr bytes.Buffer
	command.SetOut(&out)
	command.SetErr(&stderr)
	command.SetArgs([]string{"ydb", "--version", "1.1.0", "--download-base-url", srv.MirrorURL(), "--json"})
	c.Assert(command.ExecuteContext(c.Context()), qt.IsNotNil)
	var report struct {
		Status     string
		Diagnostic struct{ Code, Message string }
	}
	c.Assert(json.Unmarshal(out.Bytes(), &report), qt.IsNil)
	c.Assert(report.Status, qt.Equals, "failed")
	c.Assert(report.Diagnostic.Code, qt.Equals, providerinstall.CodeReleaseIncompatible)
	c.Assert(report.Diagnostic.Message, qt.Contains, "the ydb provider 1.1.0 needs contract 25.2 or a later 25.x; this Ptah speaks contract 25.1")
}
