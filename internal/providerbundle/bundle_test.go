package providerbundle_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/internal/providerarchive/archivetest"
	"ptah.run/internal/providerbundle"
	"ptah.run/internal/providercatalog"
)

const providerID = "github.com/stokaro/ptah-ydb-plugin"

var (
	linux   = providercatalog.Platform{OS: "linux", Arch: "amd64"}
	windows = providercatalog.Platform{OS: "windows", Arch: "arm64"}
)

// release is a pinned release with one archive per platform and a server that
// serves them.
type release struct {
	pin     providercatalog.Pin
	bodies  map[providercatalog.Platform]string
	archive map[string][]byte
}

func newRelease(platforms ...providercatalog.Platform) release {
	result := release{
		pin:     providercatalog.Pin{EngineID: "ydb", Version: "1.2.3", Tag: "v1.2.3"},
		bodies:  make(map[providercatalog.Platform]string),
		archive: make(map[string][]byte),
	}
	for _, platform := range platforms {
		name := providercatalog.ArchiveName("ydb", "1.2.3", platform)
		executable := providercatalog.ExecutableName("ydb", platform)
		body := "executable for " + platform.String()
		pack := map[bool]func(...archivetest.Entry) []byte{true: archivetest.Zip, false: archivetest.TarGz}[platform.OS == "windows"]
		data := pack(archivetest.Entry{Name: executable, Body: body}, archivetest.Entry{Name: "README.md", Body: "docs"})
		result.bodies[platform] = body
		result.archive[name] = data
		result.pin.Artifacts = append(result.pin.Artifacts, providercatalog.Artifact{
			Platform: platform, Archive: name, ArchiveSHA256: archivetest.SHA256(data),
			Executable: executable, ExecutableSHA256: archivetest.SHA256([]byte(body)),
		})
	}
	return result
}

func (r release) lookup(string) (providercatalog.Pin, bool) { return r.pin, true }

func serve(c *qt.C, files map[string][]byte) string {
	mux := http.NewServeMux()
	for name, data := range files {
		mux.HandleFunc("GET /mirror/"+name, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(data) })
	}
	server := httptest.NewServer(mux)
	c.Cleanup(server.Close)
	return server.URL + "/mirror"
}

func listing(c *qt.C, root string) []string {
	var result []string
	c.Assert(filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		result = append(result, must.Must(filepath.Rel(root, path)))
		return nil
	}), qt.IsNil)
	return result
}

func TestStage_HappyPath(t *testing.T) {
	c := qt.New(t)
	fixture := newRelease(linux, windows)
	out := c.TempDir()
	staged, err := providerbundle.Stage(c.Context(), providerbundle.Options{
		Out: out, BaseURL: serve(c, fixture.archive), Pins: fixture.lookup,
		Platforms: []providercatalog.Platform{linux, windows},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(staged, qt.HasLen, 2)
	// Only the executable is staged, at <platform>/providers/<provider-id>/<version>/.
	c.Assert(listing(c, out), qt.DeepEquals, []string{
		filepath.FromSlash("linux_amd64/providers/" + providerID + "/1.2.3/ptah-plugin-ydb"),
		filepath.FromSlash("windows_arm64/providers/" + providerID + "/1.2.3/ptah-plugin-ydb.exe"),
	})
	for _, item := range staged {
		c.Assert(string(must.Must(os.ReadFile(item.Executable))), qt.Equals, fixture.bodies[item.Platform])
		c.Assert(item.SHA256, qt.Equals, archivetest.SHA256([]byte(fixture.bodies[item.Platform])))
	}
	info := must.Must(os.Stat(staged[0].Executable))
	c.Assert(info.Mode().Perm()&0o111, qt.Not(qt.Equals), os.FileMode(0))
}

func TestStage_ReplacesAStalePlatformDirectory(t *testing.T) {
	c := qt.New(t)
	fixture := newRelease(linux)
	out := c.TempDir()
	stale := filepath.Join(out, "linux_amd64", "providers", "old", "0.0.1", "leftover")
	c.Assert(os.MkdirAll(filepath.Dir(stale), 0o750), qt.IsNil)
	c.Assert(os.WriteFile(stale, []byte("old"), 0o600), qt.IsNil)
	_, err := providerbundle.Stage(c.Context(), providerbundle.Options{
		Out: out, BaseURL: serve(c, fixture.archive), Pins: fixture.lookup, Platforms: []providercatalog.Platform{linux},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(listing(c, out), qt.DeepEquals, []string{
		filepath.FromSlash("linux_amd64/providers/" + providerID + "/1.2.3/ptah-plugin-ydb"),
	})
}

func TestStage_FailurePath(t *testing.T) {
	t.Run("archive differs from the pin", func(t *testing.T) {
		c := qt.New(t)
		fixture := newRelease(linux)
		tampered := map[string][]byte{providercatalog.ArchiveName("ydb", "1.2.3", linux): archivetest.TarGz(archivetest.Entry{Name: "ptah-plugin-ydb", Body: "hostile"})}
		out := c.TempDir()
		staged, err := providerbundle.Stage(c.Context(), providerbundle.Options{
			Out: out, BaseURL: serve(c, tampered), Pins: fixture.lookup, Platforms: []providercatalog.Platform{linux},
		})
		c.Assert(err, qt.ErrorMatches, `(?s).*ydb for linux/amd64.*pins.*`)
		c.Assert(staged, qt.HasLen, 0)
		c.Assert(listing(c, out), qt.HasLen, 0)
	})
	t.Run("platform the pin does not cover", func(t *testing.T) {
		c := qt.New(t)
		fixture := newRelease(linux)
		_, err := providerbundle.Stage(c.Context(), providerbundle.Options{
			Out: c.TempDir(), BaseURL: serve(c, fixture.archive), Pins: fixture.lookup, Platforms: []providercatalog.Platform{windows},
		})
		c.Assert(err, qt.ErrorMatches, `(?s).*windows/arm64.*no pinned archive.*`)
	})
	t.Run("engine with no pin", func(t *testing.T) {
		c := qt.New(t)
		_, err := providerbundle.Stage(c.Context(), providerbundle.Options{
			Out: c.TempDir(), Pins: func(string) (providercatalog.Pin, bool) { return providercatalog.Pin{}, false },
			Platforms: []providercatalog.Platform{linux},
		})
		c.Assert(err, qt.ErrorMatches, `(?s).*pins no YDB provider release.*`)
	})
	t.Run("no staging root", func(t *testing.T) {
		c := qt.New(t)
		_, err := providerbundle.Stage(c.Context(), providerbundle.Options{})
		c.Assert(err, qt.ErrorMatches, `providerbundle: the staging root is empty`)
	})
}

// The release pins one archive per platform the host builds, so the real pin
// stages for every platform without a network when the archives are served.
func TestStage_CoversEveryPinnedPlatform(t *testing.T) {
	c := qt.New(t)
	pin, found := providercatalog.LookupPin("ydb")
	c.Assert(found, qt.IsTrue)
	for _, platform := range providercatalog.Platforms() {
		_, covered := pin.Artifact(platform)
		c.Assert(covered, qt.IsTrue, qt.Commentf("%s", platform))
	}
}

// hostArchive builds a host release archive that holds a ptah binary and, when
// withProvider is set, the bundled provider of the fixture.
func hostArchive(c *qt.C, dir string, fixture release, platform providercatalog.Platform, providerName, providerBody string) {
	entries := []archivetest.Entry{{Name: "ptah", Body: "host"}}
	if providerName != "" {
		entries = append(entries, archivetest.Entry{Name: providerName, Body: providerBody})
	}
	name := "ptah_1.0.0_" + platform.OS + "_" + platform.Arch + ".tar.gz"
	c.Assert(os.WriteFile(filepath.Join(dir, name), archivetest.TarGz(entries...), 0o600), qt.IsNil)
}

func TestVerifyArchives_HappyPath(t *testing.T) {
	c := qt.New(t)
	fixture := newRelease(linux)
	dir := c.TempDir()
	artifact := fixture.pin.Artifacts[0]
	hostArchive(c, dir, fixture, linux, "providers/"+providerID+"/1.2.3/"+artifact.Executable, fixture.bodies[linux])
	verified, err := providerbundle.VerifyArchives(dir, []providercatalog.Platform{linux}, fixture.lookup)
	c.Assert(err, qt.IsNil)
	c.Assert(verified, qt.HasLen, 1)
}

func TestVerifyArchives_FailurePath(t *testing.T) {
	artifact := newRelease(linux).pin.Artifacts[0]
	good := "providers/" + providerID + "/1.2.3/" + artifact.Executable
	t.Run("archive without the provider", func(t *testing.T) {
		c := qt.New(t)
		fixture := newRelease(linux)
		dir := c.TempDir()
		hostArchive(c, dir, fixture, linux, "", "")
		_, err := providerbundle.VerifyArchives(dir, []providercatalog.Platform{linux}, fixture.lookup)
		c.Assert(err, qt.ErrorMatches, `(?s).*does not contain the executable.*`)
	})
	t.Run("provider at another path", func(t *testing.T) {
		c := qt.New(t)
		fixture := newRelease(linux)
		dir := c.TempDir()
		hostArchive(c, dir, fixture, linux, "providers/"+providerID+"/9.9.9/"+artifact.Executable, fixture.bodies[linux])
		_, err := providerbundle.VerifyArchives(dir, []providercatalog.Platform{linux}, fixture.lookup)
		c.Assert(err, qt.ErrorMatches, `(?s).*does not contain the executable.*`)
	})
	t.Run("provider with another digest", func(t *testing.T) {
		c := qt.New(t)
		fixture := newRelease(linux)
		dir := c.TempDir()
		hostArchive(c, dir, fixture, linux, good, "not the pinned bytes")
		_, err := providerbundle.VerifyArchives(dir, []providercatalog.Platform{linux}, fixture.lookup)
		c.Assert(err, qt.ErrorMatches, `(?s).*the pin names.*`)
	})
	t.Run("no archive for a platform", func(t *testing.T) {
		c := qt.New(t)
		fixture := newRelease(linux)
		_, err := providerbundle.VerifyArchives(c.TempDir(), []providercatalog.Platform{linux}, fixture.lookup)
		c.Assert(err, qt.ErrorMatches, `(?s).*want exactly one host archive for linux/amd64.*found 0.*`)
	})
}
