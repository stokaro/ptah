//go:build unix

package advisorysource_test

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// The permission rules read Unix ownership and mode bits. On Windows the
// default cache directory is private by its access control list, which Ptah
// does not read, so these tests have no Windows half.

// fetchTwice runs two Sources against one cache inside the freshness window
// and returns how many requests the source saw: one when the second run read
// the cache, two when it did not. prepare runs between the two.
func fetchTwice(c *qt.C, cache string, prepare func()) int {
	k := &clock{now: start}
	s := newServer(c, map[string]response{"mysql.json": {status: http.StatusOK, etag: `"v1"`, body: newer(c, "mysql")}})
	_, origin := newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")
	c.Assert(origin.Cache, qt.Equals, "fetched")
	prepare()
	_, _ = newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")
	return len(s.seen())
}

// onlyEntry returns the one cache entry in cache.
func onlyEntry(c *qt.C, cache string) string {
	entries, err := os.ReadDir(cache)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 1)
	return filepath.Join(cache, entries[0].Name())
}

// firstEntry returns the first cache entry in cache, or a path that does not
// exist when there is none, which the chmod that follows reports.
func firstEntry(cache string) string {
	entries, _ := os.ReadDir(cache)
	for _, entry := range entries {
		return filepath.Join(cache, entry.Name())
	}
	return filepath.Join(cache, "no entry")
}

// TestSource_PrivateCacheIsRead is the control: a cache directory and entry
// only their owner can write are read.
func TestSource_PrivateCacheIsRead(t *testing.T) {
	c := qt.New(t)
	cache := c.TempDir()
	c.Assert(os.Chmod(cache, 0o755), qt.IsNil)

	c.Assert(fetchTwice(c, cache, func() { c.Assert(os.Chmod(onlyEntry(c, cache), 0o644), qt.IsNil) }), qt.Equals, 1)
}

// TestSource_SharedCacheIsNotRead makes the cache writable by someone other
// than its owner after the first run: whoever can write it could plant a
// copy, so the second run fetches.
func TestSource_SharedCacheIsNotRead(t *testing.T) {
	directory := func(cache string) string { return cache }
	tests := []struct {
		name string
		// target names the path to change in the cache directory.
		target func(cache string) string
		mode   os.FileMode
	}{
		{name: "a directory its group can write", target: directory, mode: 0o770},
		{name: "a directory others can write", target: directory, mode: 0o703},
		{name: "an entry its group can write", target: firstEntry, mode: 0o620},
		{name: "an entry others can write", target: firstEntry, mode: 0o602},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			cache := c.TempDir()
			c.Cleanup(func() { _ = os.Chmod(cache, 0o700) })

			requests := fetchTwice(c, cache, func() { c.Assert(os.Chmod(tt.target(cache), tt.mode), qt.IsNil) })

			c.Assert(requests, qt.Equals, 2)
		})
	}
}

// TestSource_SharedCacheIsNotWritten runs the first fetch into a directory
// its group can write, then makes it private: nothing was written there, so
// the second run fetches too.
func TestSource_SharedCacheIsNotWritten(t *testing.T) {
	c := qt.New(t)
	cache := c.TempDir()
	c.Assert(os.Chmod(cache, 0o770), qt.IsNil)

	requests := fetchTwice(c, cache, func() { c.Assert(os.Chmod(cache, 0o700), qt.IsNil) })

	c.Assert(requests, qt.Equals, 2)
	entries, err := os.ReadDir(cache)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 1)
}

// refusedCache fetches a copy into a fresh cache, makes the source answer
// 503, and returns the source, the cache and the clock.
func refusedCache(c *qt.C) (*server, string, *clock) {
	cache, k := c.TempDir(), &clock{now: start}
	s := newServer(c, map[string]response{"mysql.json": {status: http.StatusOK, body: newer(c, "mysql")}})
	_, origin := newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")
	c.Assert(origin.Cache, qt.Equals, "fetched")
	s.set("mysql.json", response{status: http.StatusServiceUnavailable})
	return s, cache, k
}

// TestSource_RefusedCacheIsNamedInTheFallback refuses the cached copy and
// then the source: the reason says why the source gave no copy, and why the
// cached one was not used.
func TestSource_RefusedCacheIsNamedInTheFallback(t *testing.T) {
	const failed = "the advisory source answered HTTP 503 Service Unavailable; the cached copy was not used: "
	for _, mode := range []os.FileMode{0o620, 0o602} {
		t.Run("an entry with mode "+mode.String(), func(t *testing.T) {
			c := qt.New(t)
			s, cache, k := refusedCache(c)
			entry := onlyEntry(c, cache)
			c.Assert(os.Chmod(entry, mode), qt.IsNil)

			data, origin := newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")

			want, _ := bundled(c, "mysql")
			c.Assert(data, qt.DeepEquals, want)
			c.Assert(origin.Fallback, qt.Equals, failed+entry+" can be written by its group or others")
		})
	}
	t.Run("a directory its group can write", func(t *testing.T) {
		c := qt.New(t)
		s, cache, k := refusedCache(c)
		c.Assert(os.Chmod(cache, 0o770), qt.IsNil)
		c.Cleanup(func() { _ = os.Chmod(cache, 0o700) })

		_, origin := newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")

		c.Assert(origin.Fallback, qt.Equals, failed+cache+" can be written by its group or others")
	})
	t.Run("an entry reached through a link", func(t *testing.T) {
		c := qt.New(t)
		s, cache, k := refusedCache(c)
		entry := onlyEntry(c, cache)
		moved := filepath.Join(c.TempDir(), "entry.json")
		c.Assert(os.Rename(entry, moved), qt.IsNil)
		c.Assert(os.Symlink(moved, entry), qt.IsNil)

		_, origin := newSource(s.location(c), cache, k).Snapshot(c.Context(), "mysql")

		c.Assert(origin.Fallback, qt.Equals, failed+entry+" is not a regular file")
	})
}

// TestSource_LinkedCacheIsNotRead reaches the cache through a symbolic link,
// for the directory and for the entry: a link can point at a file someone
// else controls, so the second run fetches.
func TestSource_LinkedCacheIsNotRead(t *testing.T) {
	t.Run("the directory", func(t *testing.T) {
		c := qt.New(t)
		target := c.TempDir()
		k := &clock{now: start}
		s := newServer(c, map[string]response{"mysql.json": {status: http.StatusOK, body: newer(c, "mysql")}})
		_, origin := newSource(s.location(c), target, k).Snapshot(c.Context(), "mysql")
		c.Assert(origin.Cache, qt.Equals, "fetched")
		link := filepath.Join(c.TempDir(), "advisories")
		c.Assert(os.Symlink(target, link), qt.IsNil)

		_, origin = newSource(s.location(c), link, k).Snapshot(c.Context(), "mysql")

		c.Assert(origin.Cache, qt.Equals, "fetched")
		c.Assert(s.seen(), qt.HasLen, 2)
	})
	t.Run("the entry", func(t *testing.T) {
		c := qt.New(t)
		cache := c.TempDir()
		requests := fetchTwice(c, cache, func() {
			entry := onlyEntry(c, cache)
			moved := filepath.Join(c.TempDir(), "entry.json")
			c.Assert(os.Rename(entry, moved), qt.IsNil)
			c.Assert(os.Symlink(moved, entry), qt.IsNil)
		})

		c.Assert(requests, qt.Equals, 2)
	})
}
