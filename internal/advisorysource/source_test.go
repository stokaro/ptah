package advisorysource_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/advisorysource"
	"ptah.run/internal/vulnerability"
)

// response is what the fixture server answers for one file.
type response struct {
	status int
	etag   string
	body   []byte
}

// server serves fixed responses by file name and records each request.
type server struct {
	*httptest.Server
	mu        sync.Mutex
	responses map[string]response
	requests  []*http.Request
}

func newServer(c *qt.C, responses map[string]response) *server {
	s := &server{responses: responses}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.requests = append(s.requests, r.Clone(r.Context()))
		answer, ok := s.responses[strings.TrimPrefix(r.URL.Path, "/snapshots/")]
		s.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		if answer.etag != "" {
			w.Header().Set("ETag", answer.etag)
		}
		if answer.etag != "" && r.Header.Get("If-None-Match") == answer.etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.WriteHeader(answer.status)
		_, _ = w.Write(answer.body)
	}))
	c.Cleanup(s.Close)
	return s
}

func (s *server) location(c *qt.C) *url.URL {
	location, err := advisorysource.ParseLocation(s.URL + "/snapshots")
	c.Assert(err, qt.IsNil)
	return location
}

func (s *server) set(name string, answer response) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses[name] = answer
}

// seen returns the If-None-Match header of each request, in order.
func (s *server) seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	tags := make([]string, 0, len(s.requests))
	for _, request := range s.requests {
		tags = append(tags, request.Header.Get("If-None-Match"))
	}
	return tags
}

// bundled returns the bundled snapshot of product and its retrieval time.
func bundled(c *qt.C, product string) ([]byte, time.Time) {
	_, data, ok := vulnerability.BundledSnapshot(product)
	c.Assert(ok, qt.IsTrue)
	retrieved, err := vulnerability.CheckSnapshot(product, data)
	c.Assert(err, qt.IsNil)
	return data, retrieved
}

// edited returns the bundled snapshot of product with fields replaced.
func edited(c *qt.C, product string, fields map[string]any) []byte {
	data, _ := bundled(c, product)
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var document map[string]any
	c.Assert(decoder.Decode(&document), qt.IsNil)
	maps.Copy(document, fields)
	encoded, err := json.Marshal(document)
	c.Assert(err, qt.IsNil)
	return encoded
}

// newer returns the bundled snapshot of product retrieved an hour later.
func newer(c *qt.C, product string) []byte {
	_, retrieved := bundled(c, product)
	return edited(c, product, map[string]any{"retrieved_at": retrieved.Add(time.Hour).Format(time.RFC3339Nano)})
}

// clock is a settable time source.
type clock struct{ now time.Time }

func (k *clock) Now() time.Time { return k.now }

func newSource(location *url.URL, cache string, k *clock) *advisorysource.Source {
	return advisorysource.New(advisorysource.Config{Location: location, CacheDir: cache, Now: k.Now})
}

// start is the clock every test begins at: within a day after the newest
// bundled snapshot was retrieved. The tests serve copies dated relative to the bundled
// snapshots, and the source refuses a copy dated more than
// vulnerability.MaxFutureRetrieval after its clock. Against a fixed date, the
// first refresh that dates a bundled snapshot past it fails them.
var start = newestBundledRetrieval().Truncate(time.Hour).Add(24 * time.Hour)

// stamp formats a moment the way the source writes one into a reason.
func stamp(moment time.Time) string { return moment.UTC().Format(time.RFC3339Nano) }

// newestBundledRetrieval returns the latest retrieval time among the bundled
// snapshots. It panics on a bundled snapshot that does not check, which every
// test in this package would report anyway.
func newestBundledRetrieval() time.Time {
	var newest time.Time
	for _, product := range vulnerability.BundledProducts() {
		_, data, _ := vulnerability.BundledSnapshot(product)
		retrieved, err := vulnerability.CheckSnapshot(product, data)
		if err != nil {
			panic(fmt.Sprintf("bundled %s snapshot: %v", product, err))
		}
		if retrieved.After(newest) {
			newest = retrieved
		}
	}
	return newest
}

// TestDefaultLocation_NamesTheFilesOnMaster pins the contract released
// binaries read: each bundled snapshot is served from master at the path the
// repository keeps it, and the file there is the one this binary bundles.
// Moving a file, renaming one, or changing DefaultLocation fails this test,
// because every released binary would stop finding it.
func TestDefaultLocation_NamesTheFilesOnMaster(t *testing.T) {
	c := qt.New(t)
	root := filepath.Join("..", "..") // this package sits two levels below the module root
	_, err := os.Stat(filepath.Join(root, "go.mod"))
	c.Assert(err, qt.IsNil)
	want := map[string]string{
		"clickhouse": "https://raw.githubusercontent.com/stokaro/ptah/master/internal/vulnerability/clickhouse.json",
		"mariadb":    "https://raw.githubusercontent.com/stokaro/ptah/master/internal/vulnerability/mariadb.json",
		"mysql":      "https://raw.githubusercontent.com/stokaro/ptah/master/internal/vulnerability/mysql.json",
		"postgres":   "https://raw.githubusercontent.com/stokaro/ptah/master/internal/vulnerability/postgresql.json",
		"sqlserver":  "https://raw.githubusercontent.com/stokaro/ptah/master/internal/vulnerability/sqlserver.json",
	}
	c.Assert(vulnerability.BundledProducts(), qt.DeepEquals, []string{"clickhouse", "mariadb", "mysql", "postgres", "sqlserver"})
	for _, product := range vulnerability.BundledProducts() {
		name, data, ok := vulnerability.BundledSnapshot(product)
		c.Assert(ok, qt.IsTrue)
		c.Assert(advisorysource.DefaultLocation+name, qt.Equals, want[product])
		onDisk, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(want[product],
			"https://raw.githubusercontent.com/stokaro/ptah/master/"))))
		c.Assert(err, qt.IsNil)
		c.Assert(onDisk, qt.DeepEquals, data, qt.Commentf("%s on disk differs from the bundled copy", name))
	}
	location, err := advisorysource.ParseLocation(advisorysource.Repository)
	c.Assert(err, qt.IsNil)
	c.Assert(location.String(), qt.Equals, advisorysource.DefaultLocation)
}

// TestCodeOwners_NotifyOnWhatReleasedBinariesRead holds .github/CODEOWNERS
// to the files released binaries fetch from master: every bundled snapshot is
// matched by the owned pattern, and a change to it, to the fetching code, to
// the code that validates and merges a copy, to the refresh workflow or to the
// CODEOWNERS file itself notifies the same owner. The rules notify; the scan's
// merge with the bundled snapshot is what keeps a merged change from reading
// an advisory as cleaner than the bundled snapshot reads it.
func TestCodeOwners_NotifyOnWhatReleasedBinariesRead(t *testing.T) {
	c := qt.New(t)
	data, err := os.ReadFile(filepath.Join("..", "..", ".github", "CODEOWNERS"))
	c.Assert(err, qt.IsNil)
	lines := strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n")
	for _, rule := range []string{
		"/internal/vulnerability/*.json @denisvmedia",
		"/internal/advisorysource/ @denisvmedia",
		"/internal/vulnerability/merge.go @denisvmedia",
		"/internal/vulnerability/origin.go @denisvmedia",
		"/internal/vulnerability/snapshot.go @denisvmedia",
		"/internal/vulnerability/versioned.go @denisvmedia",
		"/.github/workflows/security-snapshots.yml @denisvmedia",
		"/.github/CODEOWNERS @denisvmedia",
	} {
		c.Assert(lines, qt.Contains, rule)
	}
	c.Assert(advisorysource.DefaultLocation, qt.Equals, "https://raw.githubusercontent.com/stokaro/ptah/master/internal/vulnerability/")
	for _, product := range vulnerability.BundledProducts() {
		name, _, _ := vulnerability.BundledSnapshot(product)
		matched, err := path.Match("/internal/vulnerability/*.json", "/internal/vulnerability/"+name)
		c.Assert(err, qt.IsNil)
		c.Assert(matched, qt.IsTrue, qt.Commentf("%s is not owned", name))
	}
}

func TestParseLocation_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "the repository", value: advisorysource.Repository, want: "https://raw.githubusercontent.com/stokaro/ptah/master/internal/vulnerability/"},
		{name: "https directory", value: "https://mirror.example.org/advisories/", want: "https://mirror.example.org/advisories/"},
		{name: "https without a trailing slash", value: "https://mirror.example.org/advisories", want: "https://mirror.example.org/advisories/"},
		{name: "http on a loopback address", value: "http://127.0.0.1:8080/a", want: "http://127.0.0.1:8080/a/"},
		{name: "http on the IPv6 loopback address", value: "http://[::1]/", want: "http://[::1]/"},
		{name: "http on localhost", value: "http://localhost:9000", want: "http://localhost:9000/"},
		{name: "http on localhost in capitals", value: "HTTP://LOCALHOST:9000", want: "http://LOCALHOST:9000/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			location, err := advisorysource.ParseLocation(tt.value)
			c.Assert(err, qt.IsNil)
			c.Assert(location.String(), qt.Equals, tt.want)
		})
	}
	t.Run("bundled", func(t *testing.T) {
		c := qt.New(t)
		location, err := advisorysource.ParseLocation(advisorysource.Bundled)
		c.Assert(err, qt.IsNil)
		c.Assert(location, qt.IsNil)
	})
}

// TestParseLocation_FailurePath refuses each value that names no usable
// source. A refusal prints neither credentials nor a query, which may carry a
// token, nor a value that is not a URL at all.
func TestParseLocation_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "empty", value: "", want: `advisory source is neither "repository", "bundled" nor an absolute URL`},
		{name: "relative path", value: "advisories/", want: `advisory source is neither "repository", "bundled" nor an absolute URL`},
		{name: "a file URL", value: "file:///srv/advisories/", want: `advisory source is neither "repository", "bundled" nor an absolute URL`},
		{name: "an opaque URL", value: "mailto:security@example.org", want: `advisory source is neither "repository", "bundled" nor an absolute URL`},
		{name: "a token in an opaque URL", value: "deploy:TOKEN@mirror.example.org", want: `advisory source is neither "repository", "bundled" nor an absolute URL`},
		{name: "a value that does not parse", value: "https://TOKEN@mirror.example.org:port/", want: `advisory source is neither "repository", "bundled" nor an absolute URL`},
		{name: "another scheme", value: "ftp://mirror.example.org/", want: `advisory source ftp://mirror.example.org/: scheme "ftp" is not https`},
		{name: "http on a remote host", value: "http://mirror.example.org/", want: `advisory source http://mirror.example.org/: http is accepted only for a loopback host; use https`},
		{name: "a user name", value: "https://deploy@mirror.example.org/", want: `advisory source https://mirror.example.org/ carries credentials, a query or a fragment, which are not printed`},
		{name: "a password", value: (&url.URL{Scheme: "https", User: url.UserPassword("deploy", "TOKEN"), Host: "mirror.example.org", Path: "/a"}).String(), want: `advisory source https://mirror.example.org/a carries credentials, a query or a fragment, which are not printed`},
		{name: "a query", value: "https://mirror.example.org/?access_token=TOKEN", want: `advisory source https://mirror.example.org/ carries credentials, a query or a fragment, which are not printed`},
		{name: "a fragment", value: "https://mirror.example.org/#TOKEN", want: `advisory source https://mirror.example.org/ carries credentials, a query or a fragment, which are not printed`},
		{name: "an empty query", value: "https://mirror.example.org/x?", want: `advisory source https://mirror.example.org/x carries credentials, a query or a fragment, which are not printed`},
		{name: "a misspelled bundled", value: "Bundled", want: `advisory source is neither "repository", "bundled" nor an absolute URL`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			location, err := advisorysource.ParseLocation(tt.value)
			c.Assert(err, qt.ErrorMatches, tt.want)
			c.Assert(err.Error(), qt.Not(qt.Contains), "TOKEN")
			c.Assert(location, qt.IsNil)
		})
	}
}

// TestSource_FetchesARemoteCopy reads a copy newer than the bundled snapshot
// and reports where it came from.
func TestSource_FetchesARemoteCopy(t *testing.T) {
	c := qt.New(t)
	body := newer(c, "mysql")
	s := newServer(c, map[string]response{"mysql.json": {status: http.StatusOK, etag: `"v1"`, body: body}})
	data, origin := newSource(s.location(c), c.TempDir(), &clock{now: start}).Snapshot(c.Context(), "mysql")
	c.Assert(data, qt.DeepEquals, body)
	c.Assert(origin, qt.DeepEquals, vulnerability.Origin{Kind: "remote", URL: s.URL + "/snapshots/mysql.json", FetchedAt: start, Cache: "fetched"})
	c.Assert(s.seen(), qt.DeepEquals, []string{""})
}

// TestSource_EveryBundledEngineIsFetched reads each engine the bundled
// snapshots cover from the source, under its bundled file name.
func TestSource_EveryBundledEngineIsFetched(t *testing.T) {
	c := qt.New(t)
	responses := make(map[string]response)
	for _, product := range vulnerability.BundledProducts() {
		name, _, _ := vulnerability.BundledSnapshot(product)
		responses[name] = response{status: http.StatusOK, body: newer(c, product)}
	}
	s := newServer(c, responses)
	source := newSource(s.location(c), c.TempDir(), &clock{now: start})
	for _, product := range vulnerability.BundledProducts() {
		name, _, _ := vulnerability.BundledSnapshot(product)
		data, origin := source.Snapshot(c.Context(), product)
		c.Assert(data, qt.DeepEquals, responses[name].body)
		c.Assert(origin.Kind, qt.Equals, "remote", qt.Commentf("%s: %+v", product, origin))
	}
	c.Assert(s.seen(), qt.HasLen, len(responses))
}

// TestSource_FallbackReasons serves a copy that cannot be used for each
// reason a copy is refused. The scan then reads the bundled snapshot, and the
// origin names the file and the reason.
func TestSource_FallbackReasons(t *testing.T) {
	c := qt.New(t)
	_, retrieved := bundled(c, "mysql")
	mariadb, _ := bundled(c, "mariadb")
	tests := []struct {
		name   string
		answer response
		want   string
	}{
		{name: "missing file", answer: response{status: http.StatusNotFound, body: []byte("404: Not Found")},
			want: `the advisory source answered HTTP 404 Not Found`},
		{name: "server error", answer: response{status: http.StatusBadGateway},
			want: `the advisory source answered HTTP 502 Bad Gateway`},
		{name: "not modified with no cached copy", answer: response{status: http.StatusNotModified},
			want: `the advisory source answered HTTP 304 Not Modified`},
		{name: "a response over the limit", answer: response{status: http.StatusOK, body: bytes.Repeat([]byte(" "), advisorysource.MaxSnapshotBytes+1)},
			want: `the response is larger than 8388608 bytes`},
		{name: "not JSON", answer: response{status: http.StatusOK, body: []byte("<html>rate limited</html>")},
			want: `the snapshot is invalid: decode advisory snapshot: invalid character '<' looking for beginning of value`},
		{name: "a newer schema", answer: response{status: http.StatusOK, body: edited(c, "mysql", map[string]any{"schema_version": 3})},
			want: `the mysql snapshot has schema_version 3, and this binary reads schema_version 2`},
		{name: "a field this binary does not know", answer: response{status: http.StatusOK, body: edited(c, "mysql", map[string]any{"signatures": make([]string, 0)})},
			want: `the snapshot is invalid: decode advisory snapshot: json: unknown field "signatures"`},
		{name: "another engine's snapshot", answer: response{status: http.StatusOK, body: mariadb},
			want: `the snapshot is invalid: advisory snapshot product "mariadb" does not match "mysql"`},
		{name: "a malformed source hash", answer: response{status: http.StatusOK, body: edited(c, "mysql", map[string]any{"source_sha256": "x"})},
			want: `the snapshot is invalid: validate advisory snapshot: missing or invalid source SHA-256`},
		{name: "older than the bundled snapshot", answer: response{status: http.StatusOK,
			body: edited(c, "mysql", map[string]any{"retrieved_at": retrieved.Add(-time.Millisecond).Format(time.RFC3339Nano)})},
			want: `the snapshot was retrieved at ` + retrieved.Add(-time.Millisecond).UTC().Format(time.RFC3339Nano) +
				`, before the bundled snapshot at ` + retrieved.UTC().Format(time.RFC3339Nano)},
		{name: "retrieved more than a day ahead", answer: response{status: http.StatusOK,
			body: edited(c, "mysql", map[string]any{"retrieved_at": start.Add(24*time.Hour + time.Second).Format(time.RFC3339Nano)})},
			want: `the snapshot was retrieved at ` + stamp(start.Add(24*time.Hour+time.Second)) +
				`, more than 24 hours after this machine's clock at ` + stamp(start)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			s := newServer(c, map[string]response{"mysql.json": tt.answer})
			want, _ := bundled(c, "mysql")
			data, origin := newSource(s.location(c), c.TempDir(), &clock{now: start}).Snapshot(c.Context(), "mysql")
			c.Assert(data, qt.DeepEquals, want)
			c.Assert(origin.Kind, qt.Equals, "bundled")
			c.Assert(origin.URL, qt.Equals, s.URL+"/snapshots/mysql.json")
			c.Assert(origin.Fallback, qt.Matches, tt.want)
			c.Assert(origin.Cache, qt.Equals, "")
		})
	}
}

// TestSource_FutureDatedCopyIsNotCached serves a copy dated more than a day
// ahead of the clock, then, once that failure is no longer remembered, a
// correct copy. The first is refused and not cached: cached, its retrieval
// time would refuse every correct copy after it as older.
func TestSource_FutureDatedCopyIsNotCached(t *testing.T) {
	c := qt.New(t)
	ahead := edited(c, "mysql", map[string]any{"retrieved_at": start.Add(25 * time.Hour).Format(time.RFC3339Nano)})
	s := newServer(c, map[string]response{"mysql.json": {status: http.StatusOK, body: ahead}})
	cache, k := c.TempDir(), &clock{now: start}

	_, origin := newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")

	c.Assert(origin.Kind, qt.Equals, "bundled")
	c.Assert(origin.Fallback, qt.Equals, "the snapshot was retrieved at "+stamp(start.Add(25*time.Hour))+
		", more than 24 hours after this machine's clock at "+stamp(start))

	body := newer(c, "mysql")
	s.set("mysql.json", response{status: http.StatusOK, body: body})
	k.now = start.Add(advisorysource.Freshness)

	data, origin := newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")

	c.Assert(origin.Cache, qt.Equals, "fetched")
	c.Assert(data, qt.DeepEquals, body)
}

// TestSource_CopyDatedWithinADayIsUsed serves a copy dated 23 hours ahead of
// the clock, as a source whose clock runs ahead gives: it is used.
func TestSource_CopyDatedWithinADayIsUsed(t *testing.T) {
	c := qt.New(t)
	body := edited(c, "mysql", map[string]any{"retrieved_at": start.Add(23 * time.Hour).Format(time.RFC3339Nano)})
	s := newServer(c, map[string]response{"mysql.json": {status: http.StatusOK, body: body}})

	data, origin := newSource(s.location(c), c.TempDir(), &clock{now: start}).Snapshot(c.Context(), "mysql")

	c.Assert(origin.Cache, qt.Equals, "fetched", qt.Commentf("%s", origin.Fallback))
	c.Assert(data, qt.DeepEquals, body)
}

// rawServer answers every request with the status line status, written byte
// for byte, and returns its location.
func rawServer(c *qt.C, status string) *url.URL {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_, _ = http.ReadRequest(bufio.NewReader(conn))
			_, _ = io.WriteString(conn, status+"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
			_ = conn.Close()
		}
	}()
	location, err := advisorysource.ParseLocation("http://" + listener.Addr().String() + "/snapshots")
	c.Assert(err, qt.IsNil)
	return location
}

// TestSource_StatusLineIsNotPrinted answers with a reason phrase carrying
// terminal control characters. The reason names the status by its code and
// its standard text, never by the bytes the server sent.
func TestSource_StatusLineIsNotPrinted(t *testing.T) {
	tests := []struct {
		name   string
		status string
		want   string
	}{
		{name: "a known status", status: "HTTP/1.1 503 \x1b]0;title\x07\x1b[2J", want: "the advisory source answered HTTP 503 Service Unavailable"},
		{name: "an unknown status", status: "HTTP/1.1 599 \x1b[31mred\x07", want: "the advisory source answered HTTP 599"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			_, origin := newSource(rawServer(c, tt.status), c.TempDir(), &clock{now: start}).Snapshot(c.Context(), "mysql")

			c.Assert(origin.Kind, qt.Equals, "bundled")
			c.Assert(origin.Fallback, qt.Equals, tt.want)
		})
	}
}

// TestSource_UnreachableSourceFallsBack points at a server that has shut
// down.
func TestSource_UnreachableSourceFallsBack(t *testing.T) {
	c := qt.New(t)
	s := newServer(c, make(map[string]response))
	location := s.location(c)
	s.Close()
	want, _ := bundled(c, "postgres")
	data, origin := newSource(location, c.TempDir(), &clock{now: start}).Snapshot(c.Context(), "postgres")
	c.Assert(data, qt.DeepEquals, want)
	c.Assert(origin.Kind, qt.Equals, "bundled")
	c.Assert(origin.Fallback, qt.Matches, `(?s)fetch failed: dial tcp .*`)
}

// TestSource_SlowSourceFallsBack gives up on a server that does not answer
// within the request timeout. The context's later deadline only bounds the
// test: a client without the timeout fails on it, with another message.
func TestSource_SlowSourceFallsBack(t *testing.T) {
	c := qt.New(t)
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	c.Cleanup(slow.Close)
	c.Cleanup(func() { close(release) })
	location, err := advisorysource.ParseLocation(slow.URL)
	c.Assert(err, qt.IsNil)
	ctx, cancel := context.WithTimeout(c.Context(), 10*time.Second)
	c.Cleanup(cancel)
	source := advisorysource.New(advisorysource.Config{Location: location, Timeout: 50 * time.Millisecond, Now: (&clock{now: start}).Now})
	data, origin := source.Snapshot(ctx, "mariadb")
	want, _ := bundled(c, "mariadb")
	c.Assert(data, qt.DeepEquals, want)
	c.Assert(origin.Fallback, qt.Matches, `(?s)fetch failed: .*Client\.Timeout exceeded.*`)
}

// TestSource_RedirectLeavesHTTPS follows the default client to a redirect
// that would read the snapshot over plain HTTP from another host. The
// redirect target carries a token in its query, which the reason leaves out.
func TestSource_RedirectLeavesHTTPS(t *testing.T) {
	c := qt.New(t)
	redirect := httptest.NewServer(http.RedirectHandler("http://advisories.example.org/mysql.json?token=TOKEN", http.StatusFound))
	c.Cleanup(redirect.Close)
	location, err := advisorysource.ParseLocation(redirect.URL)
	c.Assert(err, qt.IsNil)
	data, origin := advisorysource.New(advisorysource.Config{Location: location}).Snapshot(c.Context(), "mysql")
	want, _ := bundled(c, "mysql")
	c.Assert(data, qt.DeepEquals, want)
	c.Assert(origin.Fallback, qt.Equals, `fetch failed: http is accepted only for a loopback host; use https`)
}

// TestSource_BundledMakesNoRequest reads the bundled snapshots with no
// location.
func TestSource_BundledMakesNoRequest(t *testing.T) {
	c := qt.New(t)
	s := newServer(c, map[string]response{"mysql.json": {status: http.StatusOK, body: newer(c, "mysql")}})
	data, origin := newSource(nil, c.TempDir(), &clock{now: start}).Snapshot(c.Context(), "mysql")
	want, _ := bundled(c, "mysql")
	c.Assert(data, qt.DeepEquals, want)
	c.Assert(origin, qt.DeepEquals, vulnerability.Origin{Kind: "bundled"})
	c.Assert(s.seen(), qt.HasLen, 0)
}

// TestSource_NoBundledSnapshotMakesNoRequest asks for an engine Ptah bundles
// no snapshot for: there is no file to fetch, and no baseline to hold a copy
// against.
func TestSource_NoBundledSnapshotMakesNoRequest(t *testing.T) {
	c := qt.New(t)
	s := newServer(c, map[string]response{"ydb.json": {status: http.StatusOK, body: []byte("{}")}})
	data, origin := newSource(s.location(c), c.TempDir(), &clock{now: start}).Snapshot(c.Context(), "ydb")
	c.Assert(data, qt.IsNil)
	c.Assert(origin, qt.DeepEquals, vulnerability.Origin{})
	c.Assert(s.seen(), qt.HasLen, 0)
}

// TestSource_OneFetchPerEngine asks one Source for the same engine twice, as
// two targets of one run do.
func TestSource_OneFetchPerEngine(t *testing.T) {
	c := qt.New(t)
	s := newServer(c, map[string]response{"mysql.json": {status: http.StatusOK, body: newer(c, "mysql")}})
	source := newSource(s.location(c), "", &clock{now: start})
	first, _ := source.Snapshot(c.Context(), "mysql")
	second, origin := source.Snapshot(c.Context(), "mysql")
	c.Assert(second, qt.DeepEquals, first)
	c.Assert(origin.Cache, qt.Equals, "fetched")
	c.Assert(s.seen(), qt.HasLen, 1)
}

// TestSource_CacheLifecycle runs one cache directory through the freshness
// window: a copy younger than the window is used with no request, an older
// one is revalidated with its ETag and a 304 resets the window, and a changed
// file replaces the copy.
func TestSource_CacheLifecycle(t *testing.T) {
	c := qt.New(t)
	cache := c.TempDir()
	first := newer(c, "mysql")
	s := newServer(c, map[string]response{"mysql.json": {status: http.StatusOK, etag: `"v1"`, body: first}})
	address := s.URL + "/snapshots/mysql.json"
	k := &clock{now: start}

	data, origin := newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")
	c.Assert(data, qt.DeepEquals, first)
	c.Assert(origin, qt.DeepEquals, vulnerability.Origin{Kind: "remote", URL: address, FetchedAt: start, Cache: "fetched"})

	k.now = start.Add(advisorysource.Freshness - time.Second)
	data, origin = newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")
	c.Assert(data, qt.DeepEquals, first)
	c.Assert(origin, qt.DeepEquals, vulnerability.Origin{Kind: "remote", URL: address, FetchedAt: start, Cache: "fresh"})
	c.Assert(s.seen(), qt.DeepEquals, []string{""})

	revalidated := start.Add(advisorysource.Freshness)
	k.now = revalidated
	data, origin = newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")
	c.Assert(data, qt.DeepEquals, first)
	c.Assert(origin, qt.DeepEquals, vulnerability.Origin{Kind: "remote", URL: address, FetchedAt: revalidated, Cache: "revalidated"})
	c.Assert(s.seen(), qt.DeepEquals, []string{"", `"v1"`})

	k.now = revalidated.Add(advisorysource.Freshness - time.Second)
	_, origin = newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")
	c.Assert(origin.Cache, qt.Equals, "fresh")
	c.Assert(s.seen(), qt.HasLen, 2)

	_, retrieved := bundled(c, "mysql")
	second := edited(c, "mysql", map[string]any{"retrieved_at": retrieved.Add(2 * time.Hour).Format(time.RFC3339Nano)})
	s.set("mysql.json", response{status: http.StatusOK, etag: `"v2"`, body: second})
	changed := revalidated.Add(advisorysource.Freshness)
	k.now = changed
	data, origin = newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")
	c.Assert(data, qt.DeepEquals, second)
	c.Assert(origin, qt.DeepEquals, vulnerability.Origin{Kind: "remote", URL: address, FetchedAt: changed, Cache: "fetched"})
	c.Assert(s.seen(), qt.DeepEquals, []string{"", `"v1"`, `"v1"`})
}

// TestSource_CacheIsKeptPerSource reads two sources into one cache directory
// in turn: each keeps its own copy, so the third read is fresh, and an entry
// never answers for the other source.
func TestSource_CacheIsKeptPerSource(t *testing.T) {
	c := qt.New(t)
	cache := c.TempDir()
	k := &clock{now: start}
	one := newServer(c, map[string]response{"mysql.json": {status: http.StatusOK, body: newer(c, "mysql")}})
	other := newServer(c, map[string]response{"mysql.json": {status: http.StatusOK, body: newer(c, "mysql")}})
	_, origin := newSource(one.location(c), cache, k).Snapshot(c.Context(), "mysql")
	c.Assert(origin, qt.DeepEquals, vulnerability.Origin{Kind: "remote", URL: one.URL + "/snapshots/mysql.json", FetchedAt: start, Cache: "fetched"})
	_, origin = newSource(other.location(c), cache, k).Snapshot(c.Context(), "mysql")
	c.Assert(origin, qt.DeepEquals, vulnerability.Origin{Kind: "remote", URL: other.URL + "/snapshots/mysql.json", FetchedAt: start, Cache: "fetched"})
	_, origin = newSource(one.location(c), cache, k).Snapshot(c.Context(), "mysql")
	c.Assert(origin.Cache, qt.Equals, "fresh")
	c.Assert(one.seen(), qt.HasLen, 1)
	c.Assert(other.seen(), qt.HasLen, 1)
}

// cachedNewer runs one Source against a source serving a copy an hour newer
// than the bundled snapshot, so the cache holds it, and returns the copy.
func cachedNewer(c *qt.C, s *server, cache string, k *clock) []byte {
	body := newer(c, "mysql")
	s.set("mysql.json", response{status: http.StatusOK, etag: `"v1"`, body: body})
	_, origin := newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")
	c.Assert(origin.Cache, qt.Equals, "fetched")
	return body
}

// TestSource_FailedRevalidationKeepsTheCachedCopy lets the cached copy age
// past the window while the source answers with an error. Whoever can drop
// the request must not take the scan back to the older bundled snapshot, so
// the cached copy is read, marked unconfirmed, and the origin says why.
func TestSource_FailedRevalidationKeepsTheCachedCopy(t *testing.T) {
	c := qt.New(t)
	cache := c.TempDir()
	k := &clock{now: start}
	s := newServer(c, make(map[string]response))
	body := cachedNewer(c, s, cache, k)
	s.set("mysql.json", response{status: http.StatusServiceUnavailable})
	k.now = start.Add(advisorysource.Freshness)

	data, origin := newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")

	c.Assert(data, qt.DeepEquals, body)
	c.Assert(origin, qt.DeepEquals, vulnerability.Origin{Kind: "remote", URL: s.URL + "/snapshots/mysql.json", FetchedAt: start,
		Cache: "unconfirmed", Fallback: "the advisory source answered HTTP 503 Service Unavailable"})
	c.Assert(s.seen(), qt.DeepEquals, []string{"", `"v1"`})
}

// TestSource_UnreachableRevalidationKeepsTheCachedCopy is the same with a
// source that does not answer at all.
func TestSource_UnreachableRevalidationKeepsTheCachedCopy(t *testing.T) {
	c := qt.New(t)
	cache := c.TempDir()
	k := &clock{now: start}
	s := newServer(c, make(map[string]response))
	body := cachedNewer(c, s, cache, k)
	location := s.location(c)
	s.Close()
	k.now = start.Add(advisorysource.Freshness)

	data, origin := newSource(location, cache, k).Snapshot(c.Context(), "mysql")

	c.Assert(data, qt.DeepEquals, body)
	c.Assert(origin.Kind, qt.Equals, "remote")
	c.Assert(origin.Cache, qt.Equals, "unconfirmed")
	c.Assert(origin.FetchedAt, qt.Equals, start)
	c.Assert(origin.Fallback, qt.Matches, `(?s)fetch failed: dial tcp .*`)
}

// TestSource_OlderCopyThanTheCacheFallsBack serves a copy older than the one
// cached, as a source rolled back does. The newer cached copy stays the
// floor: the older copy is refused and the cached one is read.
func TestSource_OlderCopyThanTheCacheFallsBack(t *testing.T) {
	c := qt.New(t)
	cache := c.TempDir()
	k := &clock{now: start}
	_, retrieved := bundled(c, "mysql")
	cached := edited(c, "mysql", map[string]any{"retrieved_at": retrieved.Add(2 * time.Hour).Format(time.RFC3339Nano)})
	s := newServer(c, map[string]response{"mysql.json": {status: http.StatusOK, etag: `"v2"`, body: cached}})
	_, origin := newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")
	c.Assert(origin.Cache, qt.Equals, "fetched")
	s.set("mysql.json", response{status: http.StatusOK, etag: `"v1"`, body: newer(c, "mysql")})
	k.now = start.Add(advisorysource.Freshness)

	data, origin := newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")

	c.Assert(data, qt.DeepEquals, cached)
	c.Assert(origin, qt.DeepEquals, vulnerability.Origin{Kind: "remote", URL: s.URL + "/snapshots/mysql.json", FetchedAt: start,
		Cache: "unconfirmed", Fallback: "the snapshot was retrieved at " + retrieved.Add(time.Hour).UTC().Format(time.RFC3339Nano) +
			", before the cached copy at " + retrieved.Add(2*time.Hour).UTC().Format(time.RFC3339Nano)})
}

// TestSource_CopyAsOldAsTheCacheIsUsed serves the cached copy's retrieval
// time again under a new ETag: a copy no older than the floor replaces it.
func TestSource_CopyAsOldAsTheCacheIsUsed(t *testing.T) {
	c := qt.New(t)
	cache := c.TempDir()
	k := &clock{now: start}
	s := newServer(c, make(map[string]response))
	body := cachedNewer(c, s, cache, k)
	s.set("mysql.json", response{status: http.StatusOK, etag: `"v2"`, body: body})
	k.now = start.Add(advisorysource.Freshness)

	data, origin := newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")

	c.Assert(data, qt.DeepEquals, body)
	c.Assert(origin.Cache, qt.Equals, "fetched")
	c.Assert(origin.Fallback, qt.Equals, "")
}

// TestSource_FailureIsRemembered asks a failing source once per window: the
// runs inside it make no request and say when the source is asked again, and
// the first run after it asks.
func TestSource_FailureIsRemembered(t *testing.T) {
	c := qt.New(t)
	cache := c.TempDir()
	k := &clock{now: start}
	s := newServer(c, map[string]response{"mysql.json": {status: http.StatusServiceUnavailable}})
	want, _ := bundled(c, "mysql")

	_, origin := newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")
	c.Assert(origin.Fallback, qt.Equals, "the advisory source answered HTTP 503 Service Unavailable")

	k.now = start.Add(advisorysource.Freshness - time.Second)
	data, origin := newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")
	c.Assert(data, qt.DeepEquals, want)
	c.Assert(origin, qt.DeepEquals, vulnerability.Origin{Kind: "bundled", URL: s.URL + "/snapshots/mysql.json",
		Fallback: "the advisory source answered HTTP 503 Service Unavailable, at " + stamp(start) +
			"; the source is asked again from " + stamp(start.Add(time.Hour))})
	c.Assert(s.seen(), qt.HasLen, 1)

	k.now = start.Add(advisorysource.Freshness)
	_, origin = newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")
	c.Assert(origin.Fallback, qt.Equals, "the advisory source answered HTTP 503 Service Unavailable")
	c.Assert(s.seen(), qt.HasLen, 2)
}

// TestSource_CanceledRunIsNotRemembered cancels the run before it asks: a
// canceled request says nothing about the source, so the next run asks.
func TestSource_CanceledRunIsNotRemembered(t *testing.T) {
	c := qt.New(t)
	cache := c.TempDir()
	k := &clock{now: start}
	s := newServer(c, map[string]response{"mysql.json": {status: http.StatusOK, body: newer(c, "mysql")}})
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	_, origin := newSource(s.location(c), cache, k).Snapshot(ctx, "mysql")
	c.Assert(origin.Fallback, qt.Matches, `fetch failed: context canceled`)

	_, origin = newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")

	c.Assert(origin.Cache, qt.Equals, "fetched")
	c.Assert(s.seen(), qt.HasLen, 1)
}

// TestSource_LaterClockTimesAreNotCurrent writes a copy and a failure at a
// later time than the clock reads next, as a clock moved back does. Neither
// counts as inside the window: the copy is revalidated, and the failed source
// is asked again.
func TestSource_LaterClockTimesAreNotCurrent(t *testing.T) {
	t.Run("a copy", func(t *testing.T) {
		c := qt.New(t)
		cache := c.TempDir()
		k := &clock{now: start.Add(time.Minute)}
		s := newServer(c, make(map[string]response))
		cachedNewer(c, s, cache, k)
		k.now = start

		_, origin := newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")

		c.Assert(origin.Cache, qt.Equals, "revalidated")
		c.Assert(s.seen(), qt.DeepEquals, []string{"", `"v1"`})
	})
	t.Run("a failure", func(t *testing.T) {
		c := qt.New(t)
		cache := c.TempDir()
		k := &clock{now: start.Add(time.Minute)}
		s := newServer(c, map[string]response{"mysql.json": {status: http.StatusServiceUnavailable}})
		_, origin := newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")
		c.Assert(origin.Fallback, qt.Not(qt.Equals), "")
		k.now = start

		_, origin = newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")

		c.Assert(origin.Fallback, qt.Equals, "the advisory source answered HTTP 503 Service Unavailable")
		c.Assert(s.seen(), qt.HasLen, 2)
	})
}

// TestSource_UnreadableCacheIsFetchedAgain replaces every cached entry with
// bytes that are not one: the next run fetches rather than fails.
func TestSource_UnreadableCacheIsFetchedAgain(t *testing.T) {
	c := qt.New(t)
	cache := c.TempDir()
	k := &clock{now: start}
	body := newer(c, "mysql")
	s := newServer(c, map[string]response{"mysql.json": {status: http.StatusOK, etag: `"v1"`, body: body}})
	_, origin := newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")
	c.Assert(origin.Cache, qt.Equals, "fetched")
	entries, err := os.ReadDir(cache)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 1)
	c.Assert(os.WriteFile(filepath.Join(cache, entries[0].Name()), []byte("{"), 0o600), qt.IsNil)
	data, origin := newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")
	c.Assert(data, qt.DeepEquals, body)
	c.Assert(origin.Cache, qt.Equals, "fetched")
	c.Assert(s.seen(), qt.DeepEquals, []string{"", ""})
}

func TestDefaultCacheDir(t *testing.T) {
	c := qt.New(t)
	cache, err := os.UserCacheDir()
	c.Assert(err, qt.IsNil)
	c.Assert(advisorysource.DefaultCacheDir(), qt.Equals, filepath.Join(cache, "ptah", "advisories"))
}
