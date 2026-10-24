package providerrelease_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerrelease"
)

// published is the document the YDB plugin publishes, with a field this Ptah
// does not know.
const published = `{
  "schema": 1,
  "provider_id": "github.com/stokaro/ptah-ydb-plugin",
  "version": "0.2.0",
  "min_contract": {"major": 25, "minor": 0},
  "engines": ["ydb"],
  "artifacts": [
    {"os": "linux", "arch": "amd64", "archive": "ptah-plugin-ydb_0.2.0_linux_amd64.tar.gz", "executable": "ptah-plugin-ydb"},
    {"os": "windows", "arch": "arm64", "archive": "ptah-plugin-ydb_0.2.0_windows_arm64.zip", "executable": "ptah-plugin-ydb.exe"}
  ],
  "released_by": "a field added later"
}`

func ydb(c *qt.C) providercatalog.Entry {
	c.Helper()
	entry, found := providercatalog.Lookup("ydb")
	c.Assert(found, qt.IsTrue)
	return entry
}

func TestDecodeManifest_HappyPath(t *testing.T) {
	c := qt.New(t)
	manifest, err := providerrelease.DecodeManifest([]byte(published))
	c.Assert(err, qt.IsNil)
	c.Assert(manifest, qt.DeepEquals, providerrelease.Manifest{
		Schema: 1, ProviderID: "github.com/stokaro/ptah-ydb-plugin", Version: "0.2.0",
		MinContract: providerrelease.Contract{Major: 25}, Engines: []string{"ydb"},
		Artifacts: []providerrelease.Artifact{
			{OS: "linux", Arch: "amd64", Archive: "ptah-plugin-ydb_0.2.0_linux_amd64.tar.gz", Executable: "ptah-plugin-ydb"},
			{OS: "windows", Arch: "arm64", Archive: "ptah-plugin-ydb_0.2.0_windows_arm64.zip", Executable: "ptah-plugin-ydb.exe"},
		},
	})
	c.Assert(manifest.CheckRelease(ydb(c), "v0.2.0"), qt.IsNil)
	artifact, found := manifest.Artifact(providerrelease.Platform{OS: "windows", Arch: "arm64"})
	c.Assert(found, qt.IsTrue)
	c.Assert(artifact.Executable, qt.Equals, "ptah-plugin-ydb.exe")
	_, found = manifest.Artifact(providerrelease.Platform{OS: "linux", Arch: "riscv64"})
	c.Assert(found, qt.IsFalse)
}

// edit replaces the first occurrence of old in the published document. An
// anchor that moved leaves the document valid, and the row fails on the error
// it expected.
func edit(old, replacement string) string {
	return strings.Replace(published, old, replacement, 1)
}

func TestDecodeManifest_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name     string
		document string
		wantErr  string
	}{
		{"not JSON", `{"schema":`, `.*unexpected EOF`},
		{"two documents", published + `{}`, `.*more than one JSON document`},
		{"unknown schema", edit(`"schema": 1`, `"schema": 2`), `.*schema 2 is not one this Ptah reads.*`},
		{"no schema", edit(`"schema": 1,`, ``), `.*schema 0 is not one this Ptah reads.*`},
		{"empty provider", edit(`"provider_id": "github.com/stokaro/ptah-ydb-plugin"`, `"provider_id": " "`), `.*provider_id is empty.*`},
		{"version with a v", edit(`"version": "0.2.0"`, `"version": "v0.2.0"`), `.*version "v0.2.0" is not a semantic version.*`},
		{"version with build metadata", edit(`"version": "0.2.0"`, `"version": "0.2.0+abc"`), `.*version "0.2.0\+abc" is not a semantic version.*`},
		{"short version", edit(`"version": "0.2.0"`, `"version": "0.2"`), `.*version "0.2" is not a semantic version.*`},
		{"no minimum contract", edit(`"min_contract": {"major": 25, "minor": 0},`, ``), `.*min_contract.major must be at least 1.*`},
		{"negative minor", edit(`"minor": 0`, `"minor": -1`), `.*cannot unmarshal.*`},
		{"no engines", edit(`["ydb"]`, `[]`), `.*engines is empty.*`},
		{"engine listed twice", edit(`["ydb"]`, `["ydb", "ydb"]`), `.*engine "ydb" is listed twice.*`},
		{"engine name with a capital", edit(`["ydb"]`, `["YDB"]`), `.*engine "YDB" is not an engine name.*`},
		{"no artifacts", edit(`"artifacts": [`, `"artifacts": [], "x": [`), `.*artifacts is empty.*`},
		{"platform listed twice", edit(`"os": "windows", "arch": "arm64"`, `"os": "linux", "arch": "amd64"`), `.*platform linux/amd64 is listed twice.*`},
		{"bad platform", edit(`"arch": "amd64"`, `"arch": "x86 64"`), `.*artifact platform "linux/x86 64" is not a GOOS/GOARCH pair.*`},
		{"archive with a directory", edit(`"archive": "ptah-plugin-ydb_0.2.0_linux_amd64.tar.gz"`, `"archive": "../ptah.tar.gz"`), `.*archive "../ptah.tar.gz" is not a plain .tar.gz or .zip file name.*`},
		{"archive of another format", edit(`ptah-plugin-ydb_0.2.0_linux_amd64.tar.gz`, `ptah-plugin-ydb_0.2.0_linux_amd64.tar.xz`), `.*is not a plain .tar.gz or .zip file name.*`},
		{"executable with a directory", edit(`"executable": "ptah-plugin-ydb"`, `"executable": "bin/ptah-plugin-ydb"`), `.*executable "bin/ptah-plugin-ydb" is not a plain file name.*`},
		{"executable with a backslash", edit(`"executable": "ptah-plugin-ydb.exe"`, `"executable": "bin\\ptah-plugin-ydb.exe"`), `.*is not a plain file name.*`},
		{"too large", published + strings.Repeat(" ", providerrelease.MaxManifestSize), `.*larger than 65536 bytes`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			manifest, err := providerrelease.DecodeManifest([]byte(test.document))
			c.Assert(err, qt.ErrorIs, providerrelease.ErrInvalidManifest)
			c.Assert(err, qt.ErrorMatches, `(?s)`+test.wantErr)
			c.Assert(manifest, qt.DeepEquals, providerrelease.Manifest{})
		})
	}
}

func TestCheckRelease_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name     string
		document string
		tag      string
		wantErr  string
	}{
		{"another provider", edit(`github.com/stokaro/ptah-ydb-plugin`, `example.org/impostor`), "v0.2.0", `.*provider_id "example.org/impostor" is not "github.com/stokaro/ptah-ydb-plugin".*`},
		{"tag of another version", published, "v0.2.1", `.*version "0.2.0" does not match the release tag "v0.2.1".*`},
		{"tag without v", published, "0.2.0", `.*does not match the release tag "0.2.0".*`},
		{"engine not installed", edit(`["ydb"]`, `["duckdb"]`), "v0.2.0", `.*engines \["duckdb"\] do not include "ydb".*`},
		{"engine of another provider", edit(`["ydb"]`, `["ydb", "spanner"]`), "v0.2.0", `.*engine "spanner" is not an official engine of github.com/stokaro/ptah-ydb-plugin.*`},
		{"alias instead of the engine", edit(`["ydb"]`, `["ydb", "ydbs"]`), "v0.2.0", `.*engine "ydbs" is not an official engine.*`},
		{"engine the catalog does not know", edit(`["ydb"]`, `["ydb", "custom"]`), "v0.2.0", `.*engine "custom" is not an official engine.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			manifest, err := providerrelease.DecodeManifest([]byte(test.document))
			c.Assert(err, qt.IsNil)
			err = manifest.CheckRelease(ydb(c), test.tag)
			c.Assert(err, qt.ErrorIs, providerrelease.ErrInvalidManifest)
			c.Assert(err, qt.ErrorMatches, `(?s)`+test.wantErr)
		})
	}
}

func TestVersions(t *testing.T) {
	c := qt.New(t)
	for _, version := range []string{"0.2.0", "1.0.0-rc.1", "10.20.30"} {
		c.Assert(providerrelease.ValidVersion(version), qt.IsTrue, qt.Commentf("%s", version))
	}
	for _, version := range []string{"", "v0.2.0", "0.2", "0.2.0+meta", "latest", "01.2.3"} {
		c.Assert(providerrelease.ValidVersion(version), qt.IsFalse, qt.Commentf("%s", version))
	}
	version, found := providerrelease.VersionOfTag("v0.10.0")
	c.Assert(found, qt.IsTrue)
	c.Assert(version, qt.Equals, "0.10.0")
	_, found = providerrelease.VersionOfTag("0.10.0")
	c.Assert(found, qt.IsFalse)
	c.Assert(providerrelease.CompareVersions("0.10.0", "0.9.0") > 0, qt.IsTrue)
	c.Assert(providerrelease.CompareVersions("1.0.0-rc.1", "1.0.0") < 0, qt.IsTrue)
	c.Assert(providerrelease.Tag("0.2.0"), qt.Equals, "v0.2.0")
}

func TestPlainName(t *testing.T) {
	c := qt.New(t)
	c.Assert(providerrelease.PlainName("ptah-plugin-ydb"), qt.IsTrue)
	for _, name := range []string{"", ".", "..", "a/b", `a\b`, "a\x00b"} {
		c.Assert(providerrelease.PlainName(name), qt.IsFalse, qt.Commentf("%q", name))
	}
}
