package testcontour_test

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/testcontour"
	"ptah.run/internal/testcontour/shard"
)

// writeFixtureShard runs one part of the two-shard contour over
// testdata/sharded and writes its manifest and report where the gate reads
// them, as the workflow's artifact download lays them out.
func writeFixtureShard(c *qt.C, reports string, index int, timingsFile string) string {
	c.Helper()
	dir := filepath.Join(reports, "integration-contour-"+strconv.Itoa(index))
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	manifest, err := os.Create(filepath.Join(dir, shard.ManifestFile))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = manifest.Close() })
	events, err := os.Create(filepath.Join(dir, shard.EventsFile))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = events.Close() })
	var stderr bytes.Buffer
	err = testcontour.Run(context.Background(), testcontour.Config{
		Package:     "./testdata/sharded/...",
		Tags:        []string{"testcontour_fixture"},
		Timeout:     time.Minute,
		Shard:       index,
		Shards:      2,
		TimingsFile: timingsFile,
		Report:      events,
		Manifest:    manifest,
		Stderr:      &stderr,
	})
	c.Assert(err, qt.IsNil)
	return stderr.String()
}

// TestVerify_HappyPathKeepsTheTimingsOfAContourThatPassed runs both parts and
// verifies them: the account says the contour passed, and the timings file
// holds a measurement of every test, attributed to the run named as source.
func TestVerify_HappyPathKeepsTheTimingsOfAContourThatPassed(t *testing.T) {
	c := qt.New(t)
	reports := c.TempDir()
	writeFixtureShard(c, reports, 1, "")
	writeFixtureShard(c, reports, 2, "")
	timingsOut := filepath.Join(c.TempDir(), "kept", "timings.json")
	var account bytes.Buffer

	err := testcontour.Verify(testcontour.VerifyConfig{
		Reports:    reports,
		Floor:      4,
		TimingsOut: timingsOut,
		Source:     "fixture run",
		Stdout:     &account,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(account.String(), qt.Contains, "Every listed test ran exactly once, on its shard, and passed.")
	file, err := os.Open(timingsOut)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = file.Close() })
	timings, err := shard.ReadTimings(file)
	c.Assert(err, qt.IsNil)
	c.Assert(timings.Source, qt.Equals, "fixture run")
	c.Assert(timings.Packages[shardedFixture+"heavy"].Tests, qt.HasLen, 3)
	c.Assert(timings.Packages[shardedFixture+"light"].Tests, qt.HasLen, 1)
}

// TestVerify_FailurePathKeepsNoTimingsOfARefusedContour verifies a contour
// whose second shard sent nothing. The refusal names the shard, and no timings
// file is written: the next run balances on the last contour that passed.
func TestVerify_FailurePathKeepsNoTimingsOfARefusedContour(t *testing.T) {
	c := qt.New(t)
	reports := c.TempDir()
	writeFixtureShard(c, reports, 1, "")
	timingsOut := filepath.Join(c.TempDir(), "timings.json")
	var account bytes.Buffer

	err := testcontour.Verify(testcontour.VerifyConfig{
		Reports:    reports,
		Floor:      4,
		TimingsOut: timingsOut,
		Source:     "fixture run",
		Stdout:     &account,
	})

	c.Assert(err, qt.ErrorMatches, `(?s).*shard 2 of 2 sent no report.*`)
	c.Assert(account.String(), qt.Contains, "The contour is refused:")
	_, err = os.Stat(timingsOut)
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
}

// TestRun_HappyPathBalancesByCountWhenTheTimingsCannotBeRead hands a shard a
// timings file that does not exist. The run goes on, balanced by test count,
// and says so on standard error and in the partition it records.
func TestRun_HappyPathBalancesByCountWhenTheTimingsCannotBeRead(t *testing.T) {
	c := qt.New(t)
	reports := c.TempDir()

	stderr := writeFixtureShard(c, reports, 1, filepath.Join(c.TempDir(), "absent.json"))

	c.Assert(stderr, qt.Contains, "testcontour: no contour timings, balancing by test count: ")
	file, err := os.Open(filepath.Join(reports, "integration-contour-1", shard.ManifestFile))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = file.Close() })
	manifest, err := shard.ReadManifest(file)
	c.Assert(err, qt.IsNil)
	c.Assert(manifest.Partition.Weighting, qt.Equals, "test count (no readable timings)")
}

// TestRun_FailurePathRefusesTimingsForTheWholeContour gives a run of the whole
// contour a timings file. Nothing would read it, so the run is refused rather
// than left to ignore the flag.
func TestRun_FailurePathRefusesTimingsForTheWholeContour(t *testing.T) {
	c := qt.New(t)

	err := testcontour.Run(context.Background(), testcontour.Config{
		Package:     "./testdata/pass",
		Tags:        []string{"testcontour_fixture"},
		Timeout:     time.Minute,
		TimingsFile: filepath.Join(c.TempDir(), "timings.json"),
	})

	c.Assert(err, qt.ErrorMatches, "contour timings balance a sharded run, and a run of the whole contour reads none")
}
