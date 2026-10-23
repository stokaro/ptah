package providerconfig_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerdiagnostic"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/providerinstall"
)

const (
	pinnedVersion = "9.9.9"
	goodBytes     = "pinned provider bytes"
)

var testPlatform = providercatalog.Platform{OS: "linux", Arch: "amd64"}

func digest(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

func ydbEntry(c *qt.C) providercatalog.Entry {
	c.Helper()
	entry, found := providercatalog.Lookup("ydb")
	c.Assert(found, qt.IsTrue)
	return entry
}

func fixturePin() providercatalog.Pin {
	return providercatalog.Pin{
		EngineID: "ydb", Version: pinnedVersion, Tag: "v" + pinnedVersion,
		Artifacts: []providercatalog.Artifact{{
			Platform: testPlatform, Archive: "unused.tar.gz", ArchiveSHA256: digest("archive"),
			Executable: "ptah-plugin-ydb", ExecutableSHA256: digest(goodBytes),
		}},
	}
}

// layout is a bundled root and a user store with the paths candidates live at.
type layout struct {
	resolver providerconfig.Resolver
	paths    map[string]string
}

func newLayout(c *qt.C) layout {
	c.Helper()
	entry := ydbEntry(c)
	bundled, store := c.TempDir(), c.TempDir()
	storeDir := func(version string) string {
		return filepath.Join(providerinstall.Dir(store, entry.ProviderID, version, testPlatform), "ptah-plugin-ydb")
	}
	return layout{
		resolver: providerconfig.Resolver{
			BundledRoot: bundled, StoreRoot: store, Platform: testPlatform,
			Pins: func(string) (providercatalog.Pin, bool) { return fixturePin(), true },
		},
		paths: map[string]string{
			"bundled":     providerconfig.BundledPath(bundled, entry.ProviderID, pinnedVersion, "ptah-plugin-ydb"),
			"store":       storeDir(pinnedVersion),
			"store-old-1": storeDir("0.0.1"),
			"store-old-2": storeDir("9.9.8"),
			"store-newer": storeDir("10.0.0"),
		},
	}
}

// place writes every file of a row; the key names a path of the layout.
func (l layout) place(c *qt.C, files map[string]string) {
	c.Helper()
	for name, content := range files {
		path := l.paths[name]
		c.Assert(os.MkdirAll(filepath.Dir(path), 0o700), qt.IsNil)
		c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	}
}

func manifestEntry(c *qt.C, executable, content string) providerconfig.Configuration {
	c.Helper()
	entry := ydbEntry(c)
	return providerconfig.Configuration{FormatVersion: providerconfig.FormatVersion, Providers: []providerconfig.Installation{{
		Spec:    providerhost.Spec{ProviderID: entry.ProviderID, Executable: executable, SHA256: digest(content)},
		Engines: []providerconfig.Engine{{ID: "ydb", Aliases: entry.Aliases}},
	}}}
}

func diagnosticOf(c *qt.C, err error) *providerdiagnostic.Diagnostic {
	c.Helper()
	diagnostic, ok := errors.AsType[*providerdiagnostic.Diagnostic](err)
	c.Assert(ok, qt.IsTrue, qt.Commentf("%v", err))
	return diagnostic
}

func TestSelect_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
		want  string
	}{
		{"bundled alone", map[string]string{"bundled": goodBytes}, "bundled"},
		{"bundled beats store", map[string]string{"bundled": goodBytes, "store": goodBytes}, "bundled"},
		{"store alone", map[string]string{"store": goodBytes}, "store"},
		{"store ignores older and newer versions", map[string]string{"store": goodBytes, "store-old-1": "x", "store-old-2": "y", "store-newer": "z"}, "store"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			l := newLayout(c)
			l.place(c, test.files)
			installation, target, err := l.resolver.Select(providerconfig.Configuration{}, "ydb")
			c.Assert(err, qt.IsNil)
			c.Assert(target, qt.Equals, "ydb")
			c.Assert(installation.Executable, qt.Equals, l.paths[test.want])
			c.Assert(installation.SHA256, qt.Equals, digest(goodBytes))
			c.Assert(installation.ProviderID, qt.Equals, ydbEntry(c).ProviderID)
			c.Assert(installation.Disabled, qt.IsFalse)
			c.Assert(installation.Claims(), qt.DeepEquals, map[string]string{"ydb": "ydb", "ydbs": "ydb"})
			c.Assert(installation.Verify("ydb"), qt.IsNil)
		})
	}
}

func TestSelect_ManifestEntryWinsOverBundledAndStore(t *testing.T) {
	c := qt.New(t)
	l := newLayout(c)
	l.place(c, map[string]string{"bundled": goodBytes, "store": goodBytes})
	own := filepath.Join(c.TempDir(), "own-provider")
	c.Assert(os.WriteFile(own, []byte("own"), 0o600), qt.IsNil)
	for _, selector := range []string{"ydb", "ydbs", "ydb://host/db", "github.com/stokaro/ptah-ydb-plugin"} {
		installation, target, err := l.resolver.Select(manifestEntry(c, own, "own"), selector)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", selector))
		c.Assert(target, qt.Equals, "ydb")
		c.Assert(installation.Executable, qt.Equals, own)
		c.Assert(installation.SHA256, qt.Equals, digest("own"))
	}
}

func TestSelect_ThirdPartyEngineNeedsNoCatalogEntry(t *testing.T) {
	c := qt.New(t)
	selected := installation(c, "example.org/custom", "custom")
	selected.Engines[0].Aliases = []string{"customs"}
	config := providerconfig.Configuration{Providers: []providerconfig.Installation{selected}}
	actual, target, err := providerconfig.Resolver{}.Select(config, "customs")
	c.Assert(err, qt.IsNil)
	c.Assert(target, qt.Equals, "custom")
	c.Assert(actual.ProviderID, qt.Equals, selected.ProviderID)
}

func TestSelect_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		files    map[string]string
		wantCode string
		wantText string
	}{
		{"nothing installed", nil, "provider_missing", "ptah providers install ydb"},
		{"other versions only", map[string]string{"store-old-1": goodBytes, "store-old-2": goodBytes, "store-newer": goodBytes}, "provider_missing", "ptah providers install ydb"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			l := newLayout(c)
			l.place(c, test.files)
			installation, target, err := l.resolver.Select(providerconfig.Configuration{}, "ydb")
			c.Assert(installation.Executable, qt.Equals, "")
			c.Assert(target, qt.Equals, "")
			diagnostic := diagnosticOf(c, err)
			c.Assert(diagnostic.Code, qt.Equals, test.wantCode)
			c.Assert(diagnostic.Message, qt.Contains, test.wantText)
			c.Assert(diagnostic.EngineID, qt.Equals, "ydb")
		})
	}
}

func TestSelect_FailurePath_UnknownAndUnpinned(t *testing.T) {
	c := qt.New(t)
	l := newLayout(c)
	_, _, err := l.resolver.Select(providerconfig.Configuration{}, "unknown://private-secret")
	c.Assert(err, qt.ErrorMatches, "no configured provider owns the selected engine")

	unpinned := l.resolver
	unpinned.Pins = func(string) (providercatalog.Pin, bool) { return providercatalog.Pin{}, false }
	l.place(c, map[string]string{"bundled": goodBytes, "store": goodBytes})
	_, _, err = unpinned.Select(providerconfig.Configuration{}, "ydb")
	diagnostic := diagnosticOf(c, err)
	c.Assert(diagnostic.Code, qt.Equals, "provider_missing")
	c.Assert(diagnostic.Message, qt.Contains, "pins no provider release")
	c.Assert(diagnostic.Message, qt.Not(qt.Contains), "ptah providers install")
}

func TestSelect_FailurePath_UnsupportedPlatform(t *testing.T) {
	c := qt.New(t)
	l := newLayout(c)
	l.place(c, map[string]string{"bundled": goodBytes})
	l.resolver.Platform = providercatalog.Platform{OS: "plan9", Arch: "386"}
	_, _, err := l.resolver.Select(providerconfig.Configuration{}, "ydb")
	diagnostic := diagnosticOf(c, err)
	c.Assert(diagnostic.Code, qt.Equals, providerconfig.CodeUnsupported)
	c.Assert(diagnostic.Message, qt.Contains, "plan9/386")
}

// A found candidate that fails verification ends the search. The tests drive
// Start, the entry point that verifies before it executes anything.
func TestSelect_NoFallbackAfterAFoundCandidate(t *testing.T) {
	tests := []struct {
		name       string
		files      map[string]string
		manifest   bool
		disabled   bool
		wantPath   string
		wantCode   string
		wantNotice string
	}{
		{"corrupt bundled beats good store", map[string]string{"bundled": "tampered", "store": goodBytes}, false, false, "bundled", "provider_untrusted", "No other provider is tried"},
		{"corrupt store is refused", map[string]string{"store": "tampered"}, false, false, "store", "provider_untrusted", "ptah providers install ydb"},
		{"corrupt store beats nothing newer", map[string]string{"store": "tampered", "store-newer": goodBytes}, false, false, "store", "provider_untrusted", "replace it"},
		{"good bundled passes verification", map[string]string{"bundled": goodBytes}, false, false, "bundled", "provider_start_failed", ""},
		{"good store copy passes verification", map[string]string{"store": goodBytes}, false, false, "store", "provider_start_failed", ""},
		{"selected manifest entry with missing file keeps its path", map[string]string{"bundled": goodBytes, "store": goodBytes}, true, false, "manifest", "provider_executable_missing", "was not found"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			l := newLayout(c)
			l.place(c, test.files)
			l.paths["manifest"] = filepath.Join(c.TempDir(), "absent-provider")
			config := map[bool]providerconfig.Configuration{false: {}, true: manifestEntry(c, l.paths["manifest"], "own")}[test.manifest]
			installation, target, err := l.resolver.Select(config, "ydb")
			c.Assert(err, qt.IsNil)
			c.Assert(installation.Executable, qt.Equals, l.paths[test.wantPath])
			host, err := providerconfig.Start(c.Context(), installation, target)
			c.Assert(host, qt.IsNil)
			diagnostic := diagnosticOf(c, err)
			c.Assert(diagnostic.Code, qt.Equals, test.wantCode)
			c.Assert(diagnostic.Message, qt.Contains, test.wantNotice)
		})
	}
}

func TestSelect_DisabledManifestEntryIsNeverReplaced(t *testing.T) {
	c := qt.New(t)
	l := newLayout(c)
	l.place(c, map[string]string{"bundled": goodBytes, "store": goodBytes})
	config := manifestEntry(c, filepath.Join(c.TempDir(), "p"), "own")
	config.Providers[0].Disabled = true
	_, _, err := l.resolver.Select(config, "ydb")
	c.Assert(diagnosticOf(c, err).Code, qt.Equals, "provider_disabled")
}

func TestSelect_ManifestEntryWithWrongDigestIsNeverReplaced(t *testing.T) {
	c := qt.New(t)
	l := newLayout(c)
	l.place(c, map[string]string{"bundled": goodBytes, "store": goodBytes})
	own := filepath.Join(c.TempDir(), "own-provider")
	c.Assert(os.WriteFile(own, []byte("changed"), 0o600), qt.IsNil)
	installation, target, err := l.resolver.Select(manifestEntry(c, own, "own"), "ydb")
	c.Assert(err, qt.IsNil)
	_, err = providerconfig.Start(c.Context(), installation, target)
	c.Assert(diagnosticOf(c, err).Code, qt.Equals, "provider_untrusted")
	c.Assert(err, qt.ErrorMatches, `.*before changing the manifest.*`)
}

func TestBundledPath(t *testing.T) {
	c := qt.New(t)
	root := filepath.Join(string(filepath.Separator), "opt", "ptah", "providers")
	got := providerconfig.BundledPath(root, "github.com/stokaro/ptah-ydb-plugin", "0.1.1", "ptah-plugin-ydb")
	c.Assert(got, qt.Equals, filepath.Join(root, "github.com", "stokaro", "ptah-ydb-plugin", "0.1.1", "ptah-plugin-ydb"))
	c.Assert(strings.HasPrefix(got, root), qt.IsTrue)
}

func TestDefaultResolver(t *testing.T) {
	c := qt.New(t)
	c.Setenv("XDG_CACHE_HOME", c.TempDir())
	c.Setenv("HOME", c.TempDir())
	c.Setenv("LocalAppData", c.TempDir())
	executable, err := os.Executable()
	c.Assert(err, qt.IsNil)
	resolved, err := filepath.EvalSymlinks(executable)
	c.Assert(err, qt.IsNil)
	resolver := providerconfig.DefaultResolver()
	c.Assert(resolver.BundledRoot, qt.Equals, filepath.Join(filepath.Dir(resolved), "providers"))
	wantStore, err := providerinstall.DefaultRoot()
	c.Assert(err, qt.IsNil)
	c.Assert(resolver.StoreRoot, qt.Equals, wantStore)
	c.Assert(resolver.Platform, qt.Equals, providercatalog.Platform{})
}
