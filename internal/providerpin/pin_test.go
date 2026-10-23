package providerpin_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/internal/providerarchive/archivetest"
	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerpin"
)

const (
	engine  = "ydb"
	version = "1.2.3"
)

// release writes the checksums listing and the archive of every platform into a
// new directory. mutate may replace or drop a file before the listing is made.
func release(c *qt.C, mutate func(files map[string][]byte)) string {
	dir := c.TempDir()
	files := make(map[string][]byte)
	for _, platform := range providercatalog.Platforms() {
		name := providercatalog.ArchiveName(engine, version, platform)
		exe := providercatalog.ExecutableName(engine, platform)
		body := "executable for " + platform.String()
		pack := map[bool]func(...archivetest.Entry) []byte{true: archivetest.Zip, false: archivetest.TarGz}[platform.OS == "windows"]
		files[name] = pack(archivetest.Entry{Name: "README.md", Body: "docs"}, archivetest.Entry{Name: exe, Body: body})
	}
	mutate(files)
	var listing strings.Builder
	for _, platform := range providercatalog.Platforms() {
		name := providercatalog.ArchiveName(engine, version, platform)
		fmt.Fprintf(&listing, "%s  %s\n", archivetest.SHA256(files[name]), name)
	}
	for name, data := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), data, 0o600), qt.IsNil)
	}
	c.Assert(os.WriteFile(filepath.Join(dir, "checksums.txt"), []byte(listing.String()), 0o600), qt.IsNil)
	return dir
}

func keep(map[string][]byte) {}

func TestBuildAndRender_HappyPath(t *testing.T) {
	c := qt.New(t)
	pin, err := providerpin.Build(engine, version, release(c, keep))
	c.Assert(err, qt.IsNil)
	c.Assert(pin.Artifacts, qt.HasLen, len(providercatalog.Platforms()))
	source, err := providerpin.Render([]providercatalog.Pin{pin})
	c.Assert(err, qt.IsNil)
	golden := must.Must(os.ReadFile(filepath.Join("testdata", "pins_gen.golden")))
	c.Assert(string(source), qt.Equals, string(golden))
}

func TestBuild_FailurePath(t *testing.T) {
	linux := providercatalog.ArchiveName(engine, version, providercatalog.Platform{OS: "linux", Arch: "amd64"})
	for _, test := range []struct {
		name   string
		mutate func(map[string][]byte)
		want   string
	}{
		{"archive without executable", func(f map[string][]byte) {
			f[linux] = archivetest.TarGz(archivetest.Entry{Name: "README.md", Body: "docs"})
		}, `linux/amd64: archive does not contain the executable`},
		{"archive with traversal", func(f map[string][]byte) {
			f[linux] = archivetest.TarGz(archivetest.Entry{Name: "../x", Body: "x"})
		}, `linux/amd64: archive entry has an unsafe path`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := providerpin.Build(engine, version, release(c, test.mutate))
			c.Assert(err, qt.ErrorMatches, `(?s).*`+test.want+`.*`)
		})
	}
}

func TestBuild_MissingArchive(t *testing.T) {
	c := qt.New(t)
	linux := providercatalog.ArchiveName(engine, version, providercatalog.Platform{OS: "linux", Arch: "amd64"})
	_, err := providerpin.Build(engine, version, release(c, func(f map[string][]byte) { delete(f, linux) }))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func TestBuild_ListingDoesNotMatchArchive(t *testing.T) {
	c := qt.New(t)
	dir := release(c, keep)
	path := filepath.Join(dir, "checksums.txt")
	listing := string(must.Must(os.ReadFile(path)))
	listing = strings.Replace(listing, listing[:1], map[bool]string{true: "0", false: "f"}[listing[:1] != "0"], 1)
	c.Assert(os.WriteFile(path, []byte(listing), 0o600), qt.IsNil) // #nosec G703 -- path is under t.TempDir
	_, err := providerpin.Build(engine, version, dir)
	c.Assert(err, qt.ErrorMatches, `(?s).*lists [0-9a-f]{64}.*`)
}

func TestBuild_ListingIsIncomplete(t *testing.T) {
	c := qt.New(t)
	dir := release(c, keep)
	c.Assert(os.WriteFile(filepath.Join(dir, "checksums.txt"), nil, 0o600), qt.IsNil)
	_, err := providerpin.Build(engine, version, dir)
	c.Assert(err, qt.ErrorMatches, `(?s).*checksums.txt does not list ptah-plugin-ydb_1.2.3_darwin_amd64.tar.gz.*`)
}

func TestParseChecksums_HappyPath(t *testing.T) {
	c := qt.New(t)
	digest := strings.Repeat("ab", 32)
	got, err := providerpin.ParseChecksums(strings.NewReader(digest + "  a.tar.gz\n\n" + digest + " *b.zip\n"))
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, map[string]string{"a.tar.gz": digest, "b.zip": digest})
}

func TestParseChecksums_FailurePath(t *testing.T) {
	digest := strings.Repeat("ab", 32)
	for _, test := range []struct{ name, input, want string }{
		{"short digest", "abcd  a.tar.gz\n", `line 1: want a lowercase SHA-256 and a file name`},
		{"uppercase digest", strings.ToUpper(digest) + "  a.tar.gz\n", `line 1: want a lowercase SHA-256 and a file name`},
		{"missing name", digest + "\n", `line 1: want a lowercase SHA-256 and a file name`},
		{"duplicate name", digest + "  a\n" + digest + "  a\n", `line 2: "a" is listed twice`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := providerpin.ParseChecksums(strings.NewReader(test.input))
			c.Assert(err, qt.ErrorMatches, `.*`+test.want)
			c.Assert(got, qt.IsNil)
		})
	}
}

func TestRender_FailurePath(t *testing.T) {
	c := qt.New(t)
	pin, err := providerpin.Build(engine, version, release(c, keep))
	c.Assert(err, qt.IsNil)
	_, err = providerpin.Render([]providercatalog.Pin{pin, pin})
	c.Assert(err, qt.ErrorMatches, `engine "ydb" is pinned twice`)
	broken := pin
	broken.Artifacts = append(slices.Clone(pin.Artifacts), pin.Artifacts[0])
	_, err = providerpin.Render([]providercatalog.Pin{broken})
	c.Assert(err, qt.ErrorMatches, `(?s)pin "ydb": .*lists platform darwin/amd64 twice.*`)
	badDigest := pin
	badDigest.Artifacts = slices.Clone(pin.Artifacts)
	badDigest.Artifacts[0].ExecutableSHA256 = "XYZ"
	_, err = providerpin.Render([]providercatalog.Pin{badDigest})
	c.Assert(err, qt.ErrorMatches, `(?s).*executable digest "XYZ" is not a lowercase SHA-256.*`)
}

func TestCheckedInPinsAreWhatTheGeneratorRenders(t *testing.T) {
	c := qt.New(t)
	checkedIn := must.Must(os.ReadFile(filepath.Join("..", "providercatalog", "pins_gen.go")))
	rendered, err := providerpin.Render(providercatalog.Pins())
	c.Assert(err, qt.IsNil)
	c.Assert(string(rendered), qt.Equals, string(checkedIn))
}

func TestCheckedInPinsAreCompleteAndValid(t *testing.T) {
	for _, pin := range providercatalog.Pins() {
		t.Run(pin.EngineID, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pin.Validate(), qt.IsNil)
			c.Assert(pin.Artifacts, qt.HasLen, len(providercatalog.Platforms()))
		})
	}
}
