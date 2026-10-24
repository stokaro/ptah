package providerconfig_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerdiagnostic"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/providerinstall"
	"ptah.run/internal/providerrelease"
)

const goodBytes = "installed provider bytes"

var (
	testPlatform = providerrelease.Platform{OS: "linux", Arch: "amd64"}
	testHost     = providerrelease.Contract{Major: 25, Minor: 1}
)

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

// store is a user store and a resolver that reads it.
type store struct {
	root     string
	resolver providerconfig.Resolver
}

func newStore(c *qt.C) store {
	c.Helper()
	root := c.TempDir()
	return store{root: root, resolver: providerconfig.Resolver{StoreRoot: root, Platform: testPlatform, Host: testHost}}
}

// executable returns where version's executable lives in the store.
func (s store) executable(c *qt.C, version string) string {
	return filepath.Join(providerinstall.Dir(s.root, ydbEntry(c).ProviderID, version, testPlatform), "ptah-plugin-ydb")
}

// file writes content as version's executable without touching the record.
func (s store) file(c *qt.C, version, content string) {
	c.Helper()
	executable := s.executable(c, version)
	c.Assert(os.MkdirAll(filepath.Dir(executable), 0o700), qt.IsNil)
	c.Assert(os.WriteFile(executable, []byte(content), 0o600), qt.IsNil)
}

// record writes the install record naming version, with the digest of
// recorded, and the minimum contract and engines given.
func (s store) record(c *qt.C, version, recorded string, minimum providerrelease.Contract, engines ...string) {
	c.Helper()
	c.Assert(providerinstall.WriteRecord(s.root, testPlatform, providerinstall.Record{
		Schema: providerinstall.RecordSchema, ProviderID: ydbEntry(c).ProviderID, Version: version,
		MinContract: minimum, Engines: engines, SHA256: digest(recorded),
		Executable: path.Join(version, "linux_amd64", "ptah-plugin-ydb"),
	}), qt.IsNil)
}

// install writes version's executable and a record that vouches for it.
func (s store) install(c *qt.C, version string) {
	c.Helper()
	s.file(c, version, goodBytes)
	s.record(c, version, goodBytes, providerrelease.Contract{Major: 25}, "ydb")
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
	for _, test := range []struct {
		name    string
		others  []string
		minimum providerrelease.Contract
	}{
		{"the recorded installation", nil, providerrelease.Contract{Major: 25}},
		{"other versions in the store are not read", []string{"0.1.0", "0.3.0", "10.0.0"}, providerrelease.Contract{Major: 25}},
		{"a minimum the host meets exactly", nil, providerrelease.Contract{Major: 25, Minor: 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			s := newStore(c)
			for _, version := range test.others {
				s.file(c, version, "another version")
			}
			s.file(c, "0.2.0", goodBytes)
			s.record(c, "0.2.0", goodBytes, test.minimum, "ydb")
			installation, target, err := s.resolver.Select(providerconfig.Configuration{}, "ydb")
			c.Assert(err, qt.IsNil)
			c.Assert(target, qt.Equals, "ydb")
			c.Assert(installation.Executable, qt.Equals, s.executable(c, "0.2.0"))
			c.Assert(installation.SHA256, qt.Equals, digest(goodBytes))
			c.Assert(installation.ProviderID, qt.Equals, ydbEntry(c).ProviderID)
			c.Assert(installation.Disabled, qt.IsFalse)
			c.Assert(installation.Claims(), qt.DeepEquals, map[string]string{"ydb": "ydb", "ydbs": "ydb"})
			c.Assert(installation.Verify("ydb"), qt.IsNil)
		})
	}
}

func TestSelect_ManifestEntryWinsOverTheStore(t *testing.T) {
	c := qt.New(t)
	s := newStore(c)
	s.install(c, "0.2.0")
	own := filepath.Join(c.TempDir(), "own-provider")
	c.Assert(os.WriteFile(own, []byte("own"), 0o600), qt.IsNil)
	for _, selector := range []string{"ydb", "ydbs", "ydb://host/db", "github.com/stokaro/ptah-ydb-plugin"} {
		installation, target, err := s.resolver.Select(manifestEntry(c, own, "own"), selector)
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

func TestSelect_FailurePath_Absent(t *testing.T) {
	for _, test := range []struct {
		name     string
		versions []string
		noStore  bool
	}{
		{"nothing installed", nil, false},
		{"executables without a record", []string{"0.1.0", "0.2.0"}, false},
		{"no store location", nil, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			s := newStore(c)
			for _, version := range test.versions {
				s.file(c, version, goodBytes)
			}
			resolver := map[bool]providerconfig.Resolver{false: s.resolver, true: {}}[test.noStore]
			installation, target, err := resolver.Select(providerconfig.Configuration{}, "ydb")
			c.Assert(installation.Executable, qt.Equals, "")
			c.Assert(target, qt.Equals, "")
			diagnostic := diagnosticOf(c, err)
			c.Assert(diagnostic.Code, qt.Equals, "provider_missing")
			c.Assert(diagnostic.Message, qt.Contains, "Run `ptah providers install ydb`")
			c.Assert(diagnostic.EngineID, qt.Equals, "ydb")
		})
	}
}

func TestSelect_FailurePath_RecordForAnotherPlatform(t *testing.T) {
	c := qt.New(t)
	s := newStore(c)
	s.install(c, "0.2.0")
	s.resolver.Platform = providerrelease.Platform{OS: "darwin", Arch: "arm64"}
	_, _, err := s.resolver.Select(providerconfig.Configuration{}, "ydb")
	c.Assert(diagnosticOf(c, err).Code, qt.Equals, "provider_missing")
}

func TestSelect_FailurePath_UnknownEngine(t *testing.T) {
	c := qt.New(t)
	_, _, err := newStore(c).resolver.Select(providerconfig.Configuration{}, "unknown://private-secret")
	c.Assert(err, qt.ErrorMatches, "no configured provider owns the selected engine")
}

// A record that exists and cannot be used ends the search.
func TestSelect_FailurePath_MalformedRecord(t *testing.T) {
	c := qt.New(t)
	s := newStore(c)
	s.install(c, "0.2.0")
	c.Assert(os.WriteFile(providerinstall.RecordPath(s.root, ydbEntry(c).ProviderID, testPlatform), []byte("{"), 0o600), qt.IsNil)
	installation, _, err := s.resolver.Select(providerconfig.Configuration{}, "ydb")
	c.Assert(installation.Executable, qt.Equals, "")
	diagnostic := diagnosticOf(c, err)
	c.Assert(diagnostic.Code, qt.Equals, providerconfig.CodeStoreInvalid)
	c.Assert(diagnostic.Message, qt.Matches,
		`The install record of the YDB provider cannot be used: .*unexpected EOF\. Run `+"`ptah providers install ydb`"+` to replace it\. No other provider is tried\.`)
}

func TestSelect_FailurePath_UnusableRecord(t *testing.T) {
	for _, test := range []struct {
		name    string
		minimum providerrelease.Contract
		engines []string
		code    string
		text    string
	}{
		{"a record naming another engine", providerrelease.Contract{Major: 25}, []string{"duckdb"}, providerconfig.CodeStoreInvalid,
			`.*the install record names engines \["duckdb"\], not "ydb".*`},
		{"a record naming an engine of another provider", providerrelease.Contract{Major: 25}, []string{"ydb", "spanner"}, providerconfig.CodeStoreInvalid,
			`.*the install record names "spanner", which is not an official engine of github.com/stokaro/ptah-ydb-plugin.*`},
		{"a newer minor than the host", providerrelease.Contract{Major: 25, Minor: 2}, []string{"ydb"}, "provider_incompatible",
			`The installed YDB provider 0.2.0 needs contract 25.2 or a later 25.x; this Ptah speaks contract 25.1\. Run ` + "`ptah providers install ydb`" + ` to install a release this Ptah can run\..*`},
		{"another major", providerrelease.Contract{Major: 24}, []string{"ydb"}, "provider_incompatible", `.*needs contract 24.0 or a later 24.x; this Ptah speaks contract 25.1.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			s := newStore(c)
			s.file(c, "0.2.0", goodBytes)
			s.record(c, "0.2.0", goodBytes, test.minimum, test.engines...)
			installation, target, err := s.resolver.Select(providerconfig.Configuration{}, "ydb")
			c.Assert(installation.Executable, qt.Equals, "")
			c.Assert(target, qt.Equals, "")
			diagnostic := diagnosticOf(c, err)
			c.Assert(diagnostic.Code, qt.Equals, test.code)
			c.Assert(diagnostic.Message, qt.Matches, `(?s)`+test.text)
		})
	}
}

// A found candidate that fails verification ends the search. The tests drive
// Start, the entry point that verifies before it executes anything.
func TestSelect_NoFallbackAfterAFoundCandidate(t *testing.T) {
	for _, test := range []struct {
		name       string
		file       string
		manifest   bool
		wantStore  bool
		wantCode   string
		wantNotice string
	}{
		{"a changed stored executable is refused", "tampered", false, true, "provider_untrusted", "does not match the SHA-256 digest its install record holds; run `ptah providers install ydb` to replace it. No other provider is tried."},
		{"a good stored executable passes verification", goodBytes, false, true, "provider_start_failed", ""},
		{"a selected manifest entry with a missing file keeps its path", goodBytes, true, false, "provider_executable_missing", "was not found"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			s := newStore(c)
			s.file(c, "0.2.0", test.file)
			s.record(c, "0.2.0", goodBytes, providerrelease.Contract{Major: 25}, "ydb")
			absent := filepath.Join(c.TempDir(), "absent-provider")
			config := map[bool]providerconfig.Configuration{false: {}, true: manifestEntry(c, absent, "own")}[test.manifest]
			installation, target, err := s.resolver.Select(config, "ydb")
			c.Assert(err, qt.IsNil)
			c.Assert(installation.Executable, qt.Equals, map[bool]string{true: s.executable(c, "0.2.0"), false: absent}[test.wantStore])
			host, err := providerconfig.Start(c.Context(), installation, target)
			c.Assert(host, qt.IsNil)
			diagnostic := diagnosticOf(c, err)
			c.Assert(diagnostic.Code, qt.Equals, test.wantCode)
			c.Assert(diagnostic.Message, qt.Contains, test.wantNotice)
		})
	}
}

func TestSelect_AStoredExecutableThatIsGoneIsRefused(t *testing.T) {
	c := qt.New(t)
	s := newStore(c)
	s.record(c, "0.2.0", goodBytes, providerrelease.Contract{Major: 25}, "ydb")
	installation, target, err := s.resolver.Select(providerconfig.Configuration{}, "ydb")
	c.Assert(err, qt.IsNil)
	_, err = providerconfig.Start(c.Context(), installation, target)
	diagnostic := diagnosticOf(c, err)
	c.Assert(diagnostic.Code, qt.Equals, "provider_executable_missing")
	c.Assert(diagnostic.Message, qt.Contains, "The installed provider executable was not found; run `ptah providers install ydb`.")
}

func TestSelect_DisabledManifestEntryIsNeverReplaced(t *testing.T) {
	c := qt.New(t)
	s := newStore(c)
	s.install(c, "0.2.0")
	config := manifestEntry(c, filepath.Join(c.TempDir(), "p"), "own")
	config.Providers[0].Disabled = true
	_, _, err := s.resolver.Select(config, "ydb")
	c.Assert(diagnosticOf(c, err).Code, qt.Equals, "provider_disabled")
}

func TestSelect_ManifestEntryWithWrongDigestIsNeverReplaced(t *testing.T) {
	c := qt.New(t)
	s := newStore(c)
	s.install(c, "0.2.0")
	own := filepath.Join(c.TempDir(), "own-provider")
	c.Assert(os.WriteFile(own, []byte("changed"), 0o600), qt.IsNil)
	installation, target, err := s.resolver.Select(manifestEntry(c, own, "own"), "ydb")
	c.Assert(err, qt.IsNil)
	_, err = providerconfig.Start(c.Context(), installation, target)
	c.Assert(diagnosticOf(c, err).Code, qt.Equals, "provider_untrusted")
	c.Assert(err, qt.ErrorMatches, `.*before changing the manifest.*`)
}

func TestDefaultResolver(t *testing.T) {
	c := qt.New(t)
	c.Setenv("XDG_CACHE_HOME", c.TempDir())
	c.Setenv("HOME", c.TempDir())
	c.Setenv("LocalAppData", c.TempDir())
	resolver := providerconfig.DefaultResolver()
	wantStore, err := providerinstall.DefaultRoot()
	c.Assert(err, qt.IsNil)
	c.Assert(resolver, qt.DeepEquals, providerconfig.Resolver{StoreRoot: wantStore})
}
