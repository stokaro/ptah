package providerinstall_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/internal/providerarchive/archivetest"
	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerdiagnostic"
	"ptah.run/internal/providerinstall"
)

const providerID = "github.com/stokaro/ptah-ydb-plugin"

var (
	linux   = providercatalog.Platform{OS: "linux", Arch: "amd64"}
	windows = providercatalog.Platform{OS: "windows", Arch: "amd64"}
)

// fixture is one pinned artifact and the archive bytes that satisfy the pin.
type fixture struct {
	platform providercatalog.Platform
	archive  []byte
	pin      providercatalog.Pin
	name     string
	body     string
}

func newFixture(platform providercatalog.Platform, entries ...archivetest.Entry) fixture {
	name := providercatalog.ArchiveName("ydb", "1.2.3", platform)
	exe := providercatalog.ExecutableName("ydb", platform)
	pack := map[bool]func(...archivetest.Entry) []byte{true: archivetest.Zip, false: archivetest.TarGz}[platform.OS == "windows"]
	body := "executable for " + platform.String()
	data := pack(append([]archivetest.Entry{{Name: exe, Body: body}}, entries...)...)
	return fixture{
		platform: platform, archive: data, name: name, body: body,
		pin: pinOf(platform, name, exe, archivetest.SHA256(data), archivetest.SHA256([]byte(body))),
	}
}

func pinOf(platform providercatalog.Platform, archive, exe, archiveDigest, exeDigest string) providercatalog.Pin {
	return providercatalog.Pin{EngineID: "ydb", Version: "1.2.3", Tag: "v1.2.3", Artifacts: []providercatalog.Artifact{{
		Platform: platform, Archive: archive, ArchiveSHA256: archiveDigest, Executable: exe, ExecutableSHA256: exeDigest,
	}}}
}

// server serves files below prefix and counts the requests it answers.
func server(c *qt.C, prefix string, files map[string][]byte) (*httptest.Server, *atomic.Int32) {
	hits := new(atomic.Int32)
	mux := http.NewServeMux()
	for name, data := range files {
		mux.HandleFunc("GET "+prefix+"/"+name, func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			_, _ = w.Write(data)
		})
	}
	srv := httptest.NewServer(mux)
	c.Cleanup(srv.Close)
	return srv, hits
}

func installer(c *qt.C, fx fixture, srv *httptest.Server, prefix string) providerinstall.Installer {
	return providerinstall.Installer{
		Root: c.TempDir(), BaseURL: srv.URL + prefix, Platform: fx.platform,
		Pins: func(string) (providercatalog.Pin, bool) { return fx.pin, true },
	}
}

func names(dir string) []string {
	var result []string
	entries, _ := os.ReadDir(dir) // a directory that was never created holds nothing
	for _, entry := range entries {
		result = append(result, entry.Name())
	}
	return result
}

func versionDir(i providerinstall.Installer, fx fixture) string {
	return filepath.Dir(providerinstall.Dir(i.Root, providerID, "1.2.3", fx.platform))
}

func code(c *qt.C, err error) string {
	var diagnostic *providerdiagnostic.Diagnostic
	c.Assert(err, qt.ErrorAs, &diagnostic)
	return diagnostic.Code
}

func TestInstall_HappyPath(t *testing.T) {
	for _, platform := range []providercatalog.Platform{linux, windows} {
		t.Run(platform.String(), func(t *testing.T) {
			c := qt.New(t)
			fx := newFixture(platform, archivetest.Entry{Name: "README.md", Body: "docs"})
			srv, _ := server(c, "/mirror", map[string][]byte{fx.name: fx.archive})
			i := installer(c, fx, srv, "/mirror")
			result, err := i.Install(c.Context(), "ydb")
			c.Assert(err, qt.IsNil)
			c.Assert(result.Installed, qt.IsTrue)
			c.Assert(result.ProviderID, qt.Equals, providerID)
			c.Assert(result.Version, qt.Equals, "1.2.3")
			c.Assert(result.SHA256, qt.Equals, archivetest.SHA256([]byte(fx.body)))
			c.Assert(string(must.Must(os.ReadFile(result.Executable))), qt.Equals, fx.body)
			c.Assert(filepath.Dir(result.Executable), qt.Equals, providerinstall.Dir(i.Root, providerID, "1.2.3", platform))
			// Only the executable is published: no archive, README or staging directory.
			c.Assert(names(filepath.Dir(result.Executable)), qt.DeepEquals, []string{filepath.Base(result.Executable)})
			c.Assert(names(versionDir(i, fx)), qt.DeepEquals, []string{platform.OS + "_" + platform.Arch})
		})
	}
}

func TestInstall_RepeatIsANoOp(t *testing.T) {
	c := qt.New(t)
	fx := newFixture(linux)
	srv, hits := server(c, "", map[string][]byte{fx.name: fx.archive})
	i := installer(c, fx, srv, "")
	first, err := i.Install(c.Context(), "ydb")
	c.Assert(err, qt.IsNil)
	second, err := i.Install(c.Context(), "YDB")
	c.Assert(err, qt.IsNil)
	c.Assert(first.Installed, qt.IsTrue)
	c.Assert(second.Installed, qt.IsFalse)
	c.Assert(second.Executable, qt.Equals, first.Executable)
	c.Assert(hits.Load(), qt.Equals, int32(1))
}

func TestInstall_ReplacesACorruptInstallation(t *testing.T) {
	c := qt.New(t)
	fx := newFixture(linux)
	srv, hits := server(c, "", map[string][]byte{fx.name: fx.archive})
	i := installer(c, fx, srv, "")
	target := providerinstall.Dir(i.Root, providerID, "1.2.3", linux)
	c.Assert(os.MkdirAll(target, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(target, "ptah-plugin-ydb"), []byte("tampered"), 0o600), qt.IsNil)
	result, err := i.Install(c.Context(), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(result.Installed, qt.IsTrue)
	c.Assert(string(must.Must(os.ReadFile(result.Executable))), qt.Equals, fx.body)
	c.Assert(hits.Load(), qt.Equals, int32(1))
	c.Assert(names(versionDir(i, fx)), qt.DeepEquals, []string{"linux_amd64"})
}

func TestInstall_ParallelInstallsAgree(t *testing.T) {
	c := qt.New(t)
	fx := newFixture(linux)
	srv, _ := server(c, "", map[string][]byte{fx.name: fx.archive})
	i := installer(c, fx, srv, "")
	const workers = 8
	results := make([]providerinstall.Result, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for n := range workers {
		wg.Go(func() { results[n], errs[n] = i.Install(context.Background(), "ydb") })
	}
	wg.Wait()
	for n := range workers {
		c.Assert(errs[n], qt.IsNil, qt.Commentf("worker %d", n))
		c.Assert(string(must.Must(os.ReadFile(results[n].Executable))), qt.Equals, fx.body)
	}
	c.Assert(names(versionDir(i, fx)), qt.DeepEquals, []string{"linux_amd64"})
	c.Assert(names(providerinstall.Dir(i.Root, providerID, "1.2.3", linux)), qt.DeepEquals, []string{"ptah-plugin-ydb"})
}

func TestInstall_FailurePath(t *testing.T) {
	good := newFixture(linux)
	otherBytes := newFixture(linux, archivetest.Entry{Name: "extra", Body: "different archive"}).archive
	traversal := archivetest.TarGz(archivetest.Entry{Name: "ptah-plugin-ydb", Body: good.body}, archivetest.Entry{Name: "../../../evil", Body: "x"})
	noExecutable := archivetest.TarGz(archivetest.Entry{Name: "README.md", Body: "docs"})
	wrongExecutable := newFixture(linux)
	wrongExecutable.pin = pinOf(linux, good.name, "ptah-plugin-ydb", good.pin.Artifacts[0].ArchiveSHA256, archivetest.SHA256([]byte("not the executable")))
	for _, test := range []struct {
		name  string
		serve []byte
		fx    fixture
		code  string
		text  string
	}{
		{"archive digest differs", otherBytes, good, providerinstall.CodeArchiveUntrusted, "nothing was extracted"},
		{"executable digest differs", good.archive, wrongExecutable, providerinstall.CodeExecutableBad, "it was not installed"},
		{"path traversal entry", traversal, withArchive(good, traversal), providerinstall.CodeArchiveInvalid, "unsafe path"},
		{"missing executable", noExecutable, withArchive(good, noExecutable), providerinstall.CodeArchiveInvalid, "does not contain the executable"},
		{"not an archive", []byte("plain"), withArchive(good, []byte("plain")), providerinstall.CodeArchiveInvalid, "not a valid provider archive"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			srv, _ := server(c, "", map[string][]byte{good.name: test.serve})
			i := installer(c, test.fx, srv, "")
			result, err := i.Install(c.Context(), "ydb")
			c.Assert(code(c, err), qt.Equals, test.code)
			c.Assert(err, qt.ErrorMatches, `(?s).*`+test.text+`.*`)
			c.Assert(result, qt.DeepEquals, providerinstall.Result{})
			// Nothing runnable, and no staging leftovers, remain in the store.
			c.Assert(names(versionDir(i, good)), qt.HasLen, 0)
			_, statErr := os.Stat(filepath.Join(versionDir(i, good), "..", "evil"))
			c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
		})
	}
}

func withArchive(fx fixture, data []byte) fixture {
	fx.archive = data
	fx.pin = pinOf(fx.platform, fx.name, "ptah-plugin-ydb", archivetest.SHA256(data), archivetest.SHA256([]byte(fx.body)))
	return fx
}

func TestInstall_DownloadFailures(t *testing.T) {
	fx := newFixture(linux)
	mux := http.NewServeMux()
	// The header promises more bytes than the handler sends.
	mux.HandleFunc("GET /short/"+fx.name, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(fx.archive)))
		_, _ = w.Write(fx.archive[:len(fx.archive)/2])
	})
	mux.HandleFunc("GET /gone/"+fx.name, http.NotFound)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	for _, test := range []struct{ name, base, text string }{
		{"interrupted", srv.URL + "/short", "was interrupted"},
		{"not found", srv.URL + "/gone", "404 Not Found"},
		{"unreachable", "http://127.0.0.1:1", "cannot download"},
		{"not an http address", "ftp://example.org/x", "not an absolute http or https URL"},
		{"relative address", "/relative", "not an absolute http or https URL"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			i := installer(c, fx, srv, "")
			i.BaseURL = test.base
			_, err := i.Install(c.Context(), "ydb")
			c.Assert(code(c, err), qt.Equals, providerinstall.CodeDownloadFailed)
			c.Assert(err, qt.ErrorMatches, `.*`+test.text+`.*`)
			c.Assert(names(versionDir(i, fx)), qt.HasLen, 0)
		})
	}
}

func TestInstall_CanceledContext(t *testing.T) {
	c := qt.New(t)
	fx := newFixture(linux)
	srv, hits := server(c, "", map[string][]byte{fx.name: fx.archive})
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	_, err := installer(c, fx, srv, "").Install(ctx, "ydb")
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(hits.Load(), qt.Equals, int32(0))
}

func TestInstall_RefusesWithoutAPinnedArtifact(t *testing.T) {
	fx := newFixture(linux)
	srv, hits := server(qt.New(t), "", map[string][]byte{fx.name: fx.archive})
	for _, test := range []struct {
		name     string
		selector string
		platform providercatalog.Platform
		pins     func(string) (providercatalog.Pin, bool)
		code     string
		text     string
	}{
		{"unsupported platform", "ydb", providercatalog.Platform{OS: "linux", Arch: "riscv64"}, nil, providerinstall.CodeUnsupported, `no pinned archive for linux/riscv64`},
		{"engine without a release", "duckdb", linux, func(string) (providercatalog.Pin, bool) { return providercatalog.Pin{}, false }, providerinstall.CodeNoRelease, `pins no DuckDB provider release`},
		{"unknown engine", "oracle", linux, nil, providerinstall.CodeUnknownEngine, `"oracle" is not an official provider engine`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			i := installer(c, fx, srv, "")
			i.Platform, i.Pins = test.platform, test.pins
			_, err := i.Install(c.Context(), test.selector)
			c.Assert(code(c, err), qt.Equals, test.code)
			c.Assert(err, qt.ErrorMatches, `.*`+test.text+`.*`)
			c.Assert(names(i.Root), qt.HasLen, 0)
			c.Assert(hits.Load(), qt.Equals, int32(0))
		})
	}
}

func TestInstall_CompiledPinsCarryNoReleaseForDuckDBAndSpanner(t *testing.T) {
	for _, engine := range []string{"duckdb", "spanner"} {
		t.Run(engine, func(t *testing.T) {
			c := qt.New(t)
			_, err := providerinstall.Installer{Root: c.TempDir(), Platform: linux}.Install(c.Context(), engine)
			c.Assert(code(c, err), qt.Equals, providerinstall.CodeNoRelease)
			c.Assert(err, qt.ErrorMatches, `.*pins no .* provider release.*`)
		})
	}
}
