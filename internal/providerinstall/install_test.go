package providerinstall_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/internal/providerarchive/archivetest"
	"ptah.run/internal/providerdiagnostic"
	"ptah.run/internal/providerinstall"
	"ptah.run/internal/providerinstall/releasetest"
	"ptah.run/internal/providerrelease"
)

const providerID = "github.com/stokaro/ptah-ydb-plugin"

var (
	linux   = providerrelease.Platform{OS: "linux", Arch: "amd64"}
	windows = providerrelease.Platform{OS: "windows", Arch: "amd64"}
	// host is the contract the installer under test speaks.
	host = providerrelease.Contract{Major: 25, Minor: 1}
)

func spec(version string, major, minor uint32) releasetest.Spec {
	return releasetest.Spec{
		ProviderID: providerID, Engine: "ydb", Version: version,
		MinContract: providerrelease.Contract{Major: major, Minor: minor},
		Platforms:   []providerrelease.Platform{linux, windows}, Executable: []byte("executable " + version),
	}
}

func provider(version string, major, minor uint32) releasetest.Release {
	return releasetest.ProviderRelease(spec(version, major, minor))
}

func newServer(c *qt.C, releases ...releasetest.Release) *releasetest.Server {
	srv := releasetest.New("stokaro", "ptah-ydb-plugin")
	c.Cleanup(srv.Close)
	srv.Add(releases...)
	return srv
}

// discovering lists releases through the fake API.
func discovering(c *qt.C, srv *releasetest.Server) providerinstall.Installer {
	return providerinstall.Installer{Root: c.TempDir(), APIURL: srv.APIURL(), Platform: linux, Host: host}
}

// mirrored reads the fake download layout as a mirror.
func mirrored(c *qt.C, srv *releasetest.Server) providerinstall.Installer {
	return providerinstall.Installer{Root: c.TempDir(), BaseURL: srv.MirrorURL(), APIURL: srv.APIURL(), Platform: linux, Host: host}
}

func code(c *qt.C, err error) string {
	c.Helper()
	var diagnostic *providerdiagnostic.Diagnostic
	c.Assert(err, qt.ErrorAs, &diagnostic)
	return diagnostic.Code
}

// count returns how many requests asked for a path ending in suffix.
func count(srv *releasetest.Server, suffix string) int {
	n := 0
	for _, path := range srv.Requests() {
		n += map[bool]int{true: 1, false: 0}[strings.HasSuffix(path, suffix)]
	}
	return n
}

// apiRequests returns how many requests went to the releases API.
func apiRequests(srv *releasetest.Server) int {
	n := 0
	for _, path := range srv.Requests() {
		n += map[bool]int{true: 1, false: 0}[strings.HasPrefix(path, "/api/")]
	}
	return n
}

func names(dir string) []string {
	var result []string
	entries, _ := os.ReadDir(dir) // a directory that was never created holds nothing
	for _, entry := range entries {
		result = append(result, entry.Name())
	}
	return result
}

func manyIncompatible(n int) []releasetest.Release {
	releases := make([]releasetest.Release, 0, n)
	for i := range n {
		releases = append(releases, provider(fmt.Sprintf("1.0.%d", i), 26, 0))
	}
	return releases
}

func prerelease(r releasetest.Release) releasetest.Release {
	r.Prerelease = true
	return r
}

func draft(r releasetest.Release) releasetest.Release {
	r.Draft = true
	return r
}

func withoutManifest(r releasetest.Release) releasetest.Release {
	delete(r.Assets, providerrelease.ManifestName)
	r.Rechecksum()
	return r
}

func TestInstall_HappyPath_NewestCompatible(t *testing.T) {
	for _, test := range []struct {
		name     string
		releases []releasetest.Release
		want     string
	}{
		{"newest whose minimum the host meets", []releasetest.Release{provider("0.3.0", 26, 0), provider("0.2.1", 25, 2), provider("0.2.0", 25, 1), provider("0.1.0", 25, 0)}, "0.2.0"},
		{"the newest release when it is compatible", []releasetest.Release{provider("0.1.0", 25, 0), provider("0.2.0", 25, 0)}, "0.2.0"},
		{"semantic order, not text order", []releasetest.Release{provider("0.10.0", 25, 0), provider("0.9.0", 25, 0)}, "0.10.0"},
		{"drafts and prereleases are not considered", []releasetest.Release{prerelease(provider("0.4.0", 25, 0)), draft(provider("0.3.0", 25, 0)), provider("0.2.0", 25, 0)}, "0.2.0"},
		{"a release without the manifest is skipped", []releasetest.Release{withoutManifest(provider("0.5.0", 25, 0)), provider("0.2.0", 25, 0)}, "0.2.0"},
		{"a compatible release on the second page", append(manyIncompatible(100), provider("0.0.1", 25, 0)), "0.0.1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			srv := newServer(c, test.releases...)
			i := discovering(c, srv)
			result, err := i.Install(c.Context(), "ydb", "")
			c.Assert(err, qt.IsNil)
			c.Assert(result.Version, qt.Equals, test.want)
			c.Assert(result.Installed, qt.IsTrue)
			c.Assert(result.ProviderID, qt.Equals, providerID)
			c.Assert(string(must.Must(os.ReadFile(result.Executable))), qt.Equals, "executable "+test.want)
			c.Assert(result.SHA256, qt.Equals, archivetest.SHA256([]byte("executable "+test.want)))
			c.Assert(count(srv, ".tar.gz"), qt.Equals, 1)
			record, executable, err := providerinstall.ReadRecord(i.Root, providerID, linux)
			c.Assert(err, qt.IsNil)
			c.Assert(executable, qt.Equals, result.Executable)
			c.Assert(record.Version, qt.Equals, test.want)
			c.Assert(record.SHA256, qt.Equals, result.SHA256)
			c.Assert(record.MinContract, qt.Equals, result.MinContract)
		})
	}
}

func TestInstall_HappyPath_ExplicitVersion(t *testing.T) {
	for _, version := range []string{"0.1.0", "v0.1.0"} {
		t.Run(version, func(t *testing.T) {
			c := qt.New(t)
			srv := newServer(c, provider("0.2.0", 25, 0), provider("0.1.0", 25, 1))
			result, err := mirrored(c, srv).Install(c.Context(), "ydb", version)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Version, qt.Equals, "0.1.0")
			c.Assert(result.MinContract, qt.Equals, providerrelease.Contract{Major: 25, Minor: 1})
			c.Assert(string(must.Must(os.ReadFile(result.Executable))), qt.Equals, "executable 0.1.0")
			c.Assert(apiRequests(srv), qt.Equals, 0)
		})
	}
}

func TestInstall_HappyPath_Platforms(t *testing.T) {
	for _, platform := range []providerrelease.Platform{linux, windows} {
		t.Run(platform.String(), func(t *testing.T) {
			c := qt.New(t)
			srv := newServer(c, provider("0.2.0", 25, 0))
			i := discovering(c, srv)
			i.Platform = platform
			result, err := i.Install(c.Context(), "ydb", "")
			c.Assert(err, qt.IsNil)
			c.Assert(result.Platform, qt.Equals, platform)
			c.Assert(filepath.Dir(result.Executable), qt.Equals, providerinstall.Dir(i.Root, providerID, "0.2.0", platform))
			c.Assert(filepath.Base(result.Executable), qt.Equals, releasetest.ExecutableName(spec("0.2.0", 25, 0), platform))
			// Only the executable is published: no archive, README or staging directory.
			c.Assert(names(filepath.Dir(result.Executable)), qt.DeepEquals, []string{filepath.Base(result.Executable)})
			c.Assert(names(filepath.Dir(filepath.Dir(result.Executable))), qt.DeepEquals, []string{platform.OS + "_" + platform.Arch})
		})
	}
}

func TestInstall_RepeatDownloadsNoArchive(t *testing.T) {
	c := qt.New(t)
	srv := newServer(c, provider("0.2.0", 25, 0))
	i := discovering(c, srv)
	first, err := i.Install(c.Context(), "ydb", "")
	c.Assert(err, qt.IsNil)
	second, err := i.Install(c.Context(), "YDB", "")
	c.Assert(err, qt.IsNil)
	c.Assert(first.Installed, qt.IsTrue)
	c.Assert(second.Installed, qt.IsFalse)
	c.Assert(second.Executable, qt.Equals, first.Executable)
	c.Assert(second.SHA256, qt.Equals, first.SHA256)
	c.Assert(count(srv, ".tar.gz"), qt.Equals, 1)
	// The manifest is still checked against the release's checksums.
	c.Assert(count(srv, "/checksums.txt"), qt.Equals, 2)
}

func TestInstall_TheRecordNamesTheLastInstall(t *testing.T) {
	c := qt.New(t)
	srv := newServer(c, provider("0.2.0", 25, 0), provider("0.1.0", 25, 0))
	i := mirrored(c, srv)
	_, err := i.Install(c.Context(), "ydb", "0.2.0")
	c.Assert(err, qt.IsNil)
	older, err := i.Install(c.Context(), "ydb", "0.1.0")
	c.Assert(err, qt.IsNil)
	record, executable, err := providerinstall.ReadRecord(i.Root, providerID, linux)
	c.Assert(err, qt.IsNil)
	c.Assert(record.Version, qt.Equals, "0.1.0")
	c.Assert(executable, qt.Equals, older.Executable)
	// Going back to 0.2.0 checks it again: the record no longer vouches for it.
	again, err := i.Install(c.Context(), "ydb", "0.2.0")
	c.Assert(err, qt.IsNil)
	c.Assert(again.Installed, qt.IsTrue)
	c.Assert(count(srv, "ptah-plugin-ydb_0.2.0_linux_amd64.tar.gz"), qt.Equals, 2)
	// The other version stays on disk and is not read.
	c.Assert(names(providerinstall.ProviderDir(i.Root, providerID)), qt.DeepEquals, []string{".install.lock", "0.1.0", "0.2.0", "installed-linux_amd64.json"})
}

func TestInstall_ReplacesACorruptInstallation(t *testing.T) {
	c := qt.New(t)
	srv := newServer(c, provider("0.2.0", 25, 0))
	i := discovering(c, srv)
	first, err := i.Install(c.Context(), "ydb", "")
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(first.Executable, []byte("tampered"), 0o600), qt.IsNil)
	second, err := i.Install(c.Context(), "ydb", "")
	c.Assert(err, qt.IsNil)
	c.Assert(second.Installed, qt.IsTrue)
	c.Assert(string(must.Must(os.ReadFile(second.Executable))), qt.Equals, "executable 0.2.0")
	c.Assert(count(srv, ".tar.gz"), qt.Equals, 2)
	c.Assert(names(filepath.Dir(filepath.Dir(second.Executable))), qt.DeepEquals, []string{"linux_amd64"})
}

// TestInstall_ParallelInstallsAgree starts installs of one release at once.
// Each install reads checksums.txt right before it takes the store, so the
// server holds those answers back until every install has asked, and all of
// them reach the store together. One downloads and publishes the release; each
// other one waits for the lock and finds the record naming it.
func TestInstall_ParallelInstallsAgree(t *testing.T) {
	c := qt.New(t)
	release := provider("0.2.0", 25, 0)
	srv := newServer(c, release)
	const workers = 8
	srv.Handle(providerrelease.ChecksumsName, together(workers, release.Assets[providerrelease.ChecksumsName]))
	i := discovering(c, srv)
	results := make([]providerinstall.Result, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for n := range workers {
		wg.Go(func() { results[n], errs[n] = i.Install(context.Background(), "ydb", "") })
	}
	wg.Wait()
	installed := 0
	for n := range workers {
		c.Assert(errs[n], qt.IsNil, qt.Commentf("worker %d", n))
		c.Assert(string(must.Must(os.ReadFile(results[n].Executable))), qt.Equals, "executable 0.2.0")
		installed += map[bool]int{true: 1, false: 0}[results[n].Installed]
	}
	c.Assert(installed, qt.Equals, 1)
	c.Assert(count(srv, ".tar.gz"), qt.Equals, 1)
	c.Assert(names(providerinstall.Dir(i.Root, providerID, "0.2.0", linux)), qt.DeepEquals, []string{"ptah-plugin-ydb"})
	c.Assert(names(providerinstall.ProviderDir(i.Root, providerID)), qt.DeepEquals, []string{".install.lock", "0.2.0", "installed-linux_amd64.json"})
	record, _, err := providerinstall.ReadRecord(i.Root, providerID, linux)
	c.Assert(err, qt.IsNil)
	c.Assert(record.SHA256, qt.Equals, archivetest.SHA256([]byte("executable 0.2.0")))
}

const archive020 = "ptah-plugin-ydb_0.2.0_linux_amd64.tar.gz"

// damaged returns the 0.2.0 release with one asset replaced. Checksums are not
// rewritten.
func damaged(name string, value []byte) releasetest.Release {
	r := provider("0.2.0", 25, 0)
	r.Assets[name] = value
	return r
}

// removed returns the 0.2.0 release without one asset. Checksums are not
// rewritten.
func removed(name string) releasetest.Release {
	r := provider("0.2.0", 25, 0)
	delete(r.Assets, name)
	return r
}

// rechecksummed is damaged with checksums.txt rewritten to match.
func rechecksummed(name string, value []byte) releasetest.Release {
	r := damaged(name, value)
	r.Rechecksum()
	return r
}

// checksumsWithout is the 0.2.0 release whose checksums.txt has no line for name.
func checksumsWithout(name string) releasetest.Release {
	r := provider("0.2.0", 25, 0)
	var kept []string
	for line := range strings.Lines(string(r.Assets[providerrelease.ChecksumsName])) {
		kept = append(kept, map[bool]string{true: "", false: line}[strings.HasSuffix(strings.TrimSpace(line), "  "+name)])
	}
	r.Assets[providerrelease.ChecksumsName] = []byte(strings.Join(kept, ""))
	return r
}

// checksumsWith is the 0.2.0 release with lines appended to its checksums.txt.
func checksumsWith(extra string) releasetest.Release {
	r := provider("0.2.0", 25, 0)
	r.Assets[providerrelease.ChecksumsName] = append(r.Assets[providerrelease.ChecksumsName], []byte(extra)...)
	return r
}

// checksumsReplacing is the 0.2.0 release whose checksums.txt line for name is
// line instead.
func checksumsReplacing(name, line string) releasetest.Release {
	r := checksumsWithout(name)
	r.Assets[providerrelease.ChecksumsName] = append(r.Assets[providerrelease.ChecksumsName], []byte(line)...)
	return r
}

func withManifest(edit func(*providerrelease.Manifest)) releasetest.Release {
	r := provider("0.2.0", 25, 0)
	manifest := releasetest.Manifest(spec("0.2.0", 25, 0))
	edit(&manifest)
	r.SetManifest(manifest)
	return r
}

func withArchive(data []byte) releasetest.Release { return rechecksummed(archive020, data) }

func TestInstall_FailurePath(t *testing.T) {
	executable := archivetest.Entry{Name: "ptah-plugin-ydb", Body: "executable 0.2.0"}
	for _, test := range []struct {
		name     string
		releases []releasetest.Release
		version  string
		mirror   bool
		platform providerrelease.Platform
		code     string
		text     string
	}{
		{"no release at all", nil, "", false, linux, providerinstall.CodeNoRelease, `.*has no published release with ptah-provider.json\. Select a provider.*`},
		{"only releases without the manifest", []releasetest.Release{withoutManifest(provider("0.1.1", 25, 0)), withoutManifest(provider("0.1.0", 25, 0))}, "", false, linux, providerinstall.CodeNoRelease,
			`.*releases without it cannot be installed by command: v0.1.1, v0.1.0.*`},
		{"none compatible", []releasetest.Release{provider("0.3.0", 26, 0), provider("0.2.0", 25, 2), provider("0.1.0", 24, 0)}, "", false, linux, providerinstall.CodeNoCompatibleRelease,
			`.*this Ptah speaks contract 25.1, and the newest releases need: 0.3.0 needs 26.0, 0.2.0 needs 25.2, 0.1.0 needs 24.0`},
		{"explicit version the host cannot meet", []releasetest.Release{provider("0.3.0", 26, 0)}, "0.3.0", true, linux, providerinstall.CodeReleaseIncompatible,
			`the ydb provider 0.3.0 needs contract 26.0 or a later 26.x; this Ptah speaks contract 25.1\..*`},
		{"explicit version with a newer minor", []releasetest.Release{provider("0.2.0", 25, 2)}, "0.2.0", true, linux, providerinstall.CodeReleaseIncompatible,
			`the ydb provider 0.2.0 needs contract 25.2 or a later 25.x; this Ptah speaks contract 25.1\..*`},
		{"explicit version that does not exist", []releasetest.Release{provider("0.2.0", 25, 0)}, "9.9.9", true, linux, providerinstall.CodeAssetMissing,
			`release v9.9.9 of github.com/stokaro/ptah-ydb-plugin has no ptah-provider.json at http://.*/v9.9.9/ptah-provider.json; the release does not exist or does not publish it`},
		{"explicit version without the manifest", []releasetest.Release{withoutManifest(provider("0.1.1", 25, 0))}, "0.1.1", true, linux, providerinstall.CodeAssetMissing, `release v0.1.1 .* has no ptah-provider.json at .*`},
		{"a version that is not semantic", nil, "latest", true, linux, providerinstall.CodeVersionInvalid, `--version "latest" is not a semantic version such as 0.2.0`},
		{"a mirror without a version", []releasetest.Release{provider("0.2.0", 25, 0)}, "", true, linux, providerinstall.CodeVersionRequired, `a download mirror cannot list releases, so --download-base-url needs --version.*`},
		{"a malformed manifest ends the search", []releasetest.Release{damaged(providerrelease.ManifestName, []byte("{")), provider("0.1.0", 25, 0)}, "", false, linux, providerinstall.CodeReleaseInvalid,
			`release v0.2.0 of github.com/stokaro/ptah-ydb-plugin publishes an invalid ptah-provider.json at .*unexpected EOF`},
		{"a manifest of another provider", []releasetest.Release{withManifest(func(m *providerrelease.Manifest) { m.ProviderID = "example.org/impostor" })}, "", false, linux, providerinstall.CodeReleaseInvalid,
			`.*provider_id "example.org/impostor" is not "github.com/stokaro/ptah-ydb-plugin"`},
		{"a manifest of another version", []releasetest.Release{withManifest(func(m *providerrelease.Manifest) { m.Version = "0.2.1" })}, "", false, linux, providerinstall.CodeReleaseInvalid,
			`.*version "0.2.1" does not match the release tag "v0.2.0"`},
		{"no artifact for the platform", []releasetest.Release{provider("0.2.0", 25, 0)}, "", false, providerrelease.Platform{OS: "linux", Arch: "riscv64"}, providerinstall.CodeUnsupported,
			`the YDB provider 0.2.0 publishes no archive for linux/riscv64; .*`},
		{"no checksums.txt in the listing", []releasetest.Release{removed(providerrelease.ChecksumsName)}, "", false, linux, providerinstall.CodeAssetMissing, `release v0.2.0 of .* has no checksums.txt asset`},
		{"no checksums.txt on the mirror", []releasetest.Release{removed(providerrelease.ChecksumsName)}, "0.2.0", true, linux, providerinstall.CodeAssetMissing, `release v0.2.0 of .* has no checksums.txt at .*`},
		{"no checksum entry for the archive", []releasetest.Release{checksumsWithout(archive020)}, "", false, linux, providerinstall.CodeChecksumMissing,
			`the release's checksums.txt has no entry for ` + archive020 + `; nothing was installed`},
		{"no checksum entry for the manifest", []releasetest.Release{checksumsWithout(providerrelease.ManifestName)}, "", false, linux, providerinstall.CodeChecksumMissing,
			`the release's checksums.txt has no entry for ptah-provider.json; nothing was installed`},
		{"an empty digest for the archive", []releasetest.Release{checksumsReplacing(archive020, "  "+archive020+"\n")}, "", false, linux, providerinstall.CodeChecksumInvalid,
			`the release's checksums.txt cannot verify ` + archive020 + `: .*the digest of ` + archive020 + ` is "", not a SHA-256; nothing was installed`},
		{"an archive listed twice", []releasetest.Release{checksumsWith(strings.Repeat("0", 64) + "  " + archive020 + "\n")}, "", false, linux, providerinstall.CodeChecksumInvalid,
			`the release's checksums.txt cannot verify ` + archive020 + `: .*listed 2 times.*`},
		{"an archive whose digest differs", []releasetest.Release{damaged(archive020, archivetest.TarGz(executable, archivetest.Entry{Name: "extra", Body: "x"}))}, "", false, linux, providerinstall.CodeChecksumMismatch,
			`the archive ` + archive020 + ` from .* has SHA-256 [0-9a-f]{64}, but the release's checksums.txt lists [0-9a-f]{64}; nothing was extracted.*`},
		{"a manifest whose digest differs", []releasetest.Release{damaged(providerrelease.ManifestName, append(provider("0.2.0", 25, 0).Assets[providerrelease.ManifestName], ' '))}, "", false, linux, providerinstall.CodeChecksumMismatch,
			`ptah-provider.json has SHA-256 [0-9a-f]{64}, but the release's checksums.txt lists [0-9a-f]{64}; nothing was installed.*`},
		{"an archive the listing lacks", []releasetest.Release{removed(archive020)}, "", false, linux, providerinstall.CodeAssetMissing, `release v0.2.0 of .* has no ` + archive020 + ` asset`},
		{"an archive the mirror lacks", []releasetest.Release{removed(archive020)}, "0.2.0", true, linux, providerinstall.CodeAssetMissing, `the archive ` + archive020 + ` is not at http://.*`},
		{"a path traversal entry", []releasetest.Release{withArchive(archivetest.TarGz(executable, archivetest.Entry{Name: "../../../evil", Body: "x"}))}, "", false, linux, providerinstall.CodeArchiveInvalid, `.*unsafe path.*`},
		{"an archive without the executable", []releasetest.Release{withArchive(archivetest.TarGz(archivetest.Entry{Name: "README.md", Body: "docs"}))}, "", false, linux, providerinstall.CodeArchiveInvalid, `.*does not contain the executable.*`},
		{"not an archive", []releasetest.Release{withArchive([]byte("plain"))}, "", false, linux, providerinstall.CodeArchiveInvalid, `the archive is not a valid provider archive.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			srv := newServer(c, test.releases...)
			i := map[bool]providerinstall.Installer{false: discovering(c, srv), true: mirrored(c, srv)}[test.mirror]
			i.Platform = test.platform
			result, err := i.Install(c.Context(), "ydb", test.version)
			c.Assert(code(c, err), qt.Equals, test.code)
			c.Assert(err, qt.ErrorMatches, `(?s)`+test.text)
			c.Assert(result, qt.DeepEquals, providerinstall.Result{})
			// Nothing runnable, no record and no staging leftovers remain in the store.
			_, _, err = providerinstall.ReadRecord(i.Root, providerID, test.platform)
			c.Assert(err, qt.ErrorIs, os.ErrNotExist)
			c.Assert(names(providerinstall.Dir(i.Root, providerID, "0.2.0", test.platform)), qt.HasLen, 0)
			c.Assert(names(filepath.Dir(providerinstall.Dir(i.Root, providerID, "0.2.0", test.platform))), qt.HasLen, 0)
			_, statErr := os.Stat(filepath.Join(i.Root, "evil"))
			c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
		})
	}
}

func TestInstall_FailurePath_Requests(t *testing.T) {
	c := qt.New(t)
	srv := newServer(c, provider("0.2.0", 25, 0))
	i := mirrored(c, srv)
	_, err := i.Install(c.Context(), "ydb", "")
	c.Assert(code(c, err), qt.Equals, providerinstall.CodeVersionRequired)
	c.Assert(srv.Requests(), qt.HasLen, 0)
	_, err = i.Install(c.Context(), "oracle", "")
	c.Assert(code(c, err), qt.Equals, providerinstall.CodeUnknownEngine)
	c.Assert(err, qt.ErrorMatches, `"oracle" is not an official provider engine.*`)
	c.Assert(srv.Requests(), qt.HasLen, 0)
}

func TestInstall_ListingFailures(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		code   string
		text   string
	}{
		{"rate limited", http.StatusForbidden, providerinstall.CodeDownloadFailed, `.*its hourly limit for unauthenticated requests is used up; pass --version.*`},
		{"too many requests", http.StatusTooManyRequests, providerinstall.CodeDownloadFailed, `.*hourly limit.*`},
		{"repository not found", http.StatusNotFound, providerinstall.CodeNoRelease, `the release source of github.com/stokaro/ptah-ydb-plugin, https://github.com/stokaro/ptah-ydb-plugin, was not found`},
		{"server error", http.StatusInternalServerError, providerinstall.CodeDownloadFailed, `cannot list the releases of github.com/stokaro/ptah-ydb-plugin: 500 Internal Server Error`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			srv := newServer(c, provider("0.2.0", 25, 0))
			srv.RefuseListing(test.status)
			_, err := discovering(c, srv).Install(c.Context(), "ydb", "")
			c.Assert(code(c, err), qt.Equals, test.code)
			c.Assert(err, qt.ErrorMatches, test.text)
		})
	}
}

func TestInstall_DownloadFailures(t *testing.T) {
	good := provider("0.2.0", 25, 0)
	for _, test := range []struct {
		name    string
		handler http.HandlerFunc
		text    string
	}{
		{"interrupted", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", strconv.Itoa(len(good.Assets[archive020])))
			_, _ = w.Write(good.Assets[archive020][:len(good.Assets[archive020])/2])
		}, `the download of .* was interrupted.*`},
		{"refused", func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "no", http.StatusBadGateway) }, `cannot download .*: 502 Bad Gateway`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			srv := newServer(c, good)
			srv.Handle(archive020, test.handler)
			i := discovering(c, srv)
			_, err := i.Install(c.Context(), "ydb", "")
			c.Assert(code(c, err), qt.Equals, providerinstall.CodeDownloadFailed)
			c.Assert(err, qt.ErrorMatches, test.text)
			c.Assert(names(providerinstall.Dir(i.Root, providerID, "0.2.0", linux)), qt.HasLen, 0)
		})
	}
}

func TestInstall_UnusableAddresses(t *testing.T) {
	for _, test := range []struct{ name, api, base, text string }{
		{"unreachable API", "http://127.0.0.1:1", "", `cannot list the releases of .*`},
		{"mirror that is not http", "", "ftp://example.org/x", `the download base URL "ftp://example.org/x" is not an absolute http or https URL`},
		{"relative mirror", "", "/relative", `the download base URL "/relative" is not an absolute http or https URL`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			i := providerinstall.Installer{Root: c.TempDir(), APIURL: test.api, BaseURL: test.base, Platform: linux, Host: host}
			_, err := i.Install(c.Context(), "ydb", map[bool]string{true: "", false: "0.2.0"}[test.base == ""])
			c.Assert(code(c, err), qt.Equals, providerinstall.CodeDownloadFailed)
			c.Assert(err, qt.ErrorMatches, test.text)
		})
	}
}

func TestInstall_CanceledContext(t *testing.T) {
	c := qt.New(t)
	srv := newServer(c, provider("0.2.0", 25, 0))
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	_, err := discovering(c, srv).Install(ctx, "ydb", "")
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(srv.Requests(), qt.HasLen, 0)
}

func TestInstall_DefaultsToTheHostContract(t *testing.T) {
	c := qt.New(t)
	hostContract := providerrelease.HostContract()
	srv := newServer(c, provider("0.3.0", hostContract.Major+1, 0), provider("0.2.0", hostContract.Major, hostContract.Minor))
	i := discovering(c, srv)
	i.Host = providerrelease.Contract{}
	result, err := i.Install(c.Context(), "ydb", "")
	c.Assert(err, qt.IsNil)
	c.Assert(result.Version, qt.Equals, "0.2.0")
}
