package shard_test

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/testcontour/shard"
)

func writeShard(c *qt.C, dir string, manifest shard.Manifest, events string) {
	c.Helper()
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	payload, err := json.Marshal(manifest)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, shard.ManifestFile), payload, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, shard.EventsFile), []byte(events), 0o600), qt.IsNil)
}

// TestLoadReports_HappyPath lays out two artifacts the way download-artifact
// does, one directory each, with the second nested one level deeper.
func TestLoadReports_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	partition := gatePartition(c)
	first := shard.Manifest{Shard: 1, Shards: 2, Partition: partition}
	second := shard.Manifest{Shard: 2, Shards: 2, Partition: partition}
	writeShard(c, filepath.Join(dir, "integration-contour-1"), first, cleanShard1)
	writeShard(c, filepath.Join(dir, "nested", "integration-contour-2"), second, cleanShard2)

	loaded, err := shard.LoadReports(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(loaded, qt.HasLen, 2)
	c.Assert(loaded[0].Manifest, qt.DeepEquals, first)
	c.Assert(loaded[1].Manifest, qt.DeepEquals, second)
	events, err := io.ReadAll(loaded[1].Events)
	c.Assert(err, qt.IsNil)
	c.Assert(string(events), qt.Equals, cleanShard2)
}

// TestLoadReports_HappyPathReadsAShardWithoutEvents loads a shard that wrote
// its manifest and stopped: it loads, and the gate counts its tests missing.
func TestLoadReports_HappyPathReadsAShardWithoutEvents(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	partition := gatePartition(c)
	writeShard(c, filepath.Join(dir, "integration-contour-1"), shard.Manifest{Shard: 1, Shards: 2, Partition: partition}, cleanShard1)
	stopped := filepath.Join(dir, "integration-contour-2")
	writeShard(c, stopped, shard.Manifest{Shard: 2, Shards: 2, Partition: partition}, "")
	c.Assert(os.Remove(filepath.Join(stopped, shard.EventsFile)), qt.IsNil)

	loaded, err := shard.LoadReports(dir)
	c.Assert(err, qt.IsNil)
	_, err = shard.Verify(loaded, 4)
	c.Assert(err, qt.ErrorMatches, `2 listed tests never ran: example\.com/a:TestA2, example\.com/b:TestB1`)
}

// TestLoadReports_HappyPathFindsNothingWhereNothingWasDownloaded hands Verify
// an empty set, which it refuses.
func TestLoadReports_HappyPathFindsNothingWhereNothingWasDownloaded(t *testing.T) {
	c := qt.New(t)
	loaded, err := shard.LoadReports(filepath.Join(t.TempDir(), "absent"))
	c.Assert(err, qt.IsNil)
	c.Assert(loaded, qt.HasLen, 0)
	_, err = shard.Verify(loaded, 1)
	c.Assert(err, qt.ErrorMatches, `no shard reported; nothing proves the contour ran`)
}

func TestLoadReports_FailurePathRefusesAManifestOfAnotherShape(t *testing.T) {
	c := qt.New(t)
	dir := filepath.Join(t.TempDir(), "integration-contour-1")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, shard.ManifestFile), []byte(`{"shard": 1, "total": 2}`), 0o600), qt.IsNil)

	loaded, err := shard.LoadReports(filepath.Dir(dir))
	c.Assert(err, qt.ErrorMatches, `.*manifest\.json: decode shard manifest: json: unknown field "total"`)
	c.Assert(loaded, qt.IsNil)
}
