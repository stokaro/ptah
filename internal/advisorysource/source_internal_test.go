package advisorysource

// White-box testing required: the cache file format is private, and a cached
// body changed without its hash is observable only by writing one. The
// default client's timeouts are observable from outside only by waiting for
// them. The bundled snapshot every copy is held to is compiled in, so a
// refresh master published against an earlier bundled snapshot is replayed
// only through the unexported check.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/vulnerability"
)

// cachedSource fetches a copy an hour newer than the bundled MySQL snapshot
// into a fresh cache and returns the copy, the configuration that read it,
// the cache file, the request count and a switch that makes the server answer
// 503. The server answers 304 to the copy's ETag, so a run that sends the
// ETag of a cached copy it did not accept reads no copy at all.
func cachedSource(c *qt.C) ([]byte, Config, string, *atomic.Int32, *atomic.Bool) {
	_, bundled, ok := vulnerability.BundledSnapshot("mysql")
	c.Assert(ok, qt.IsTrue)
	floor, err := vulnerability.CheckSnapshot("mysql", bundled)
	c.Assert(err, qt.IsNil)
	body := retrievedAt(c, bundled, floor.Add(time.Hour))
	requests, failing := new(atomic.Int32), new(atomic.Bool)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if failing.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = w.Write(body)
	}))
	c.Cleanup(server.Close)
	location, err := ParseLocation(server.URL)
	c.Assert(err, qt.IsNil)
	now := time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC)
	config := Config{Location: location, CacheDir: c.TempDir(), Now: func() time.Time { return now }}
	_, origin := New(config).Snapshot(c.Context(), "mysql")
	c.Assert(origin.Cache, qt.Equals, "fetched")
	return body, config, New(config).cachePath(origin.URL), requests, failing
}

// retrievedAt returns data with its retrieval time replaced.
func retrievedAt(c *qt.C, data []byte, moment time.Time) []byte {
	var document map[string]any
	c.Assert(json.Unmarshal(data, &document), qt.IsNil)
	document["retrieved_at"] = moment.Format(time.RFC3339Nano)
	body, err := json.Marshal(document)
	c.Assert(err, qt.IsNil)
	return body
}

// replaceBody writes the cache entry at path with body, and with the hash of
// sum rather than of body.
func replaceBody(c *qt.C, path string, body, sum []byte) {
	data, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	var entry cacheEntry
	c.Assert(json.Unmarshal(data, &entry), qt.IsNil)
	digest := sha256.Sum256(sum)
	entry.Body, entry.BodySHA256 = body, hex.EncodeToString(digest[:])
	data, err = json.Marshal(entry)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(path, data, 0o600), qt.IsNil)
}

// TestReadCache_RevalidatesACachedCopy plants a fresh entry whose copy is
// older than the bundled snapshot, as a cache written before this binary was
// installed holds: the copy is not used, and the run fetches again.
func TestReadCache_RevalidatesACachedCopy(t *testing.T) {
	c := qt.New(t)
	body, config, path, requests, _ := cachedSource(c)
	_, bundled, _ := vulnerability.BundledSnapshot("mysql")
	floor, err := vulnerability.CheckSnapshot("mysql", bundled)
	c.Assert(err, qt.IsNil)
	older := retrievedAt(c, bundled, floor.Add(-time.Hour))
	replaceBody(c, path, older, older)

	got, origin := New(config).Snapshot(c.Context(), "mysql")

	c.Assert(origin.Cache, qt.Equals, "fetched")
	c.Assert(got, qt.DeepEquals, body)
	c.Assert(requests.Load(), qt.Equals, int32(2))
}

// TestReadCache_BodyMustMatchItsHash plants a fresh entry whose body is
// another valid copy under the hash of the one fetched: the body is not used,
// and the run fetches again without the ETag.
func TestReadCache_BodyMustMatchItsHash(t *testing.T) {
	c := qt.New(t)
	body, config, path, requests, _ := cachedSource(c)
	_, bundled, _ := vulnerability.BundledSnapshot("mysql")
	floor, err := vulnerability.CheckSnapshot("mysql", bundled)
	c.Assert(err, qt.IsNil)
	replaceBody(c, path, retrievedAt(c, bundled, floor.Add(2*time.Hour)), body)

	got, origin := New(config).Snapshot(c.Context(), "mysql")

	c.Assert(origin.Cache, qt.Equals, "fetched")
	c.Assert(got, qt.DeepEquals, body)
	c.Assert(requests.Load(), qt.Equals, int32(2))
}

// TestReadCache_RefusalIsNamedInTheFallback plants a fresh entry the cache
// refuses, then makes the source fail: the bundled snapshot is read, and the
// reason says why the cached copy was not used. A copy dated more than a day
// ahead is refused from the cache as it is from the source.
func TestReadCache_RefusalIsNamedInTheFallback(t *testing.T) {
	const failed = "the advisory source answered HTTP 503 Service Unavailable; the cached copy was not used: "
	_, bundled, _ := vulnerability.BundledSnapshot("mysql")
	now := time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC)
	t.Run("a body that does not match its hash", func(t *testing.T) {
		c := qt.New(t)
		body, config, path, _, failing := cachedSource(c)
		replaceBody(c, path, retrievedAt(c, body, now.Add(-time.Minute)), body)
		failing.Store(true)

		got, origin := New(config).Snapshot(c.Context(), "mysql")

		c.Assert(got, qt.DeepEquals, bundled)
		c.Assert(origin.Fallback, qt.Equals, failed+"the copy in "+path+" does not match its SHA-256")
	})
	t.Run("a copy dated more than a day ahead", func(t *testing.T) {
		c := qt.New(t)
		_, config, path, _, failing := cachedSource(c)
		ahead := retrievedAt(c, bundled, now.Add(25*time.Hour))
		replaceBody(c, path, ahead, ahead)
		failing.Store(true)

		got, origin := New(config).Snapshot(c.Context(), "mysql")

		c.Assert(got, qt.DeepEquals, bundled)
		c.Assert(origin.Fallback, qt.Equals, failed+
			"the snapshot was retrieved at 2026-11-22T10:00:00Z, more than 24 hours after this machine's clock at 2026-11-21T09:00:00Z")
	})
}

// TestNew_DefaultClientTimeout bounds a whole request and the wait for its
// response headers, so a stalled source costs a run a bounded time.
func TestNew_DefaultClientTimeout(t *testing.T) {
	c := qt.New(t)
	client := New(Config{}).config.Client
	c.Assert(client.Timeout, qt.Equals, 15*time.Second)
	transport, ok := client.Transport.(*http.Transport)
	c.Assert(ok, qt.IsTrue)
	c.Assert(transport.ResponseHeaderTimeout, qt.Equals, 10*time.Second)
	c.Assert(New(Config{Timeout: time.Second}).config.Client.Timeout, qt.Equals, time.Second)
}

// historyProducts maps each directory of ../vulnerability/testdata/history,
// which holds every version of a bundled snapshot master has published, to
// its product.
var historyProducts = map[string]string{"clickhouse": "clickhouse", "mariadb": "mariadb", "mysql": "mysql", "postgresql": "postgres"}

// publishedRefreshes returns each refresh master has published for dir: the
// version it replaced and the version itself, oldest first.
func publishedRefreshes(c *qt.C, dir string) [][2][]byte {
	paths, err := filepath.Glob(filepath.Join("..", "vulnerability", "testdata", "history", dir, "*.json"))
	c.Assert(err, qt.IsNil)
	slices.Sort(paths)
	refreshes := make([][2][]byte, 0, len(paths))
	for index := 1; index < len(paths); index++ {
		older, err := os.ReadFile(paths[index-1])
		c.Assert(err, qt.IsNil)
		newer, err := os.ReadFile(paths[index])
		c.Assert(err, qt.IsNil)
		refreshes = append(refreshes, [2][]byte{older, newer})
	}
	return refreshes
}

// TestCheck_EveryPublishedRefreshIsAccepted replays every refresh master has
// published as a binary that bundles the version it replaced reads it, an
// hour after its retrieval: each one is accepted.
func TestCheck_EveryPublishedRefreshIsAccepted(t *testing.T) {
	refreshes := 0
	for dir, product := range historyProducts {
		for index, refresh := range publishedRefreshes(qt.New(t), dir) {
			refreshes++
			t.Run(fmt.Sprintf("%s refresh %d", dir, index+1), func(t *testing.T) {
				c := qt.New(t)
				floor, err := vulnerability.CheckSnapshot(product, refresh[0])
				c.Assert(err, qt.IsNil)
				retrieved, err := vulnerability.CheckSnapshot(product, refresh[1])
				c.Assert(err, qt.IsNil)
				base := baseline{product: product, data: refresh[0], retrieved: floor}

				got, err := base.check(refresh[1], floor, "the bundled snapshot", retrieved.Add(time.Hour))

				c.Assert(err, qt.IsNil)
				c.Assert(got, qt.Equals, retrieved)
			})
		}
	}
	c := qt.New(t)
	c.Assert(refreshes, qt.Equals, 9, qt.Commentf("the history holds every published refresh"))
}

// TestCheck_RefusesACopyDatedAhead is the control for the replay: the same
// refresh, read more than a day before its retrieval, is refused.
func TestCheck_RefusesACopyDatedAhead(t *testing.T) {
	c := qt.New(t)
	refresh := publishedRefreshes(c, "mysql")[0]
	floor, err := vulnerability.CheckSnapshot("mysql", refresh[0])
	c.Assert(err, qt.IsNil)
	retrieved, err := vulnerability.CheckSnapshot("mysql", refresh[1])
	c.Assert(err, qt.IsNil)
	base := baseline{product: "mysql", data: refresh[0], retrieved: floor}

	got, err := base.check(refresh[1], floor, "the bundled snapshot", retrieved.Add(-25*time.Hour))

	c.Assert(err, qt.ErrorMatches, `the snapshot was retrieved at `+regexp.QuoteMeta(stamp(retrieved))+`, more than 24 hours after .*`)
	c.Assert(got, qt.Equals, time.Time{})
}
