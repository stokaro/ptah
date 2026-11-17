package main

// White-box testing required: the source updater's review decision is private
// to this command and has no exported API.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/vulnerability"
)

// reviewedPage names four IDs. The snapshot pins one and excludes another, so
// CVE-2026-00001 and CVE-2026-00004 are on the page without being pinned: a
// review that compared against the pinned set would report them as changed.
var reviewedPage = []byte("CVE-2026-00001\nCVE-2026-00002\nCVE-2026-00003\nCVE-2026-00004\n")

// changedPage drops CVE-2026-00001 and adds CVE-2027-00005, named twice.
var changedPage = []byte("CVE-2026-00002\nCVE-2026-00003\nCVE-2026-00004\nCVE-2027-00005\nCVE-2027-00005\n")

var reviewPath = filepath.Join("internal", "vulnerability", "review", "mariadb.json")

func digestOf(source []byte) string {
	digest := sha256.Sum256(source)
	return hex.EncodeToString(digest[:])
}

func reviewedSnapshot(sourceIDs []string) vulnerability.VersionedSnapshot {
	return vulnerability.VersionedSnapshot{
		SchemaVersion: 2, Product: "mariadb", Source: sourceURLs["mariadb"],
		RetrievedAt:  time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		SourceSHA256: digestOf(reviewedPage), Coverage: "complete",
		OutOfScope: []vulnerability.OutOfScopeAdvisory{{ID: "CVE-2026-00003", Reason: "client library only"}},
		Advisories: []vulnerability.VersionedAdvisory{{
			ID: "CVE-2026-00002", Component: "core", Description: "example server issue",
			URL:    "https://www.cve.org/CVERecord?id=CVE-2026-00002",
			Ranges: []vulnerability.VersionRange{{Line: "11.8", From: "11.8.1", Fixed: "11.8.9"}},
		}},
		SourceCVEIDs: sourceIDs,
	}
}

func readReview(c *qt.C) (review, []byte) {
	data, err := os.ReadFile(reviewPath)
	c.Assert(err, qt.IsNil)
	var pending review
	c.Assert(json.Unmarshal(data, &pending), qt.IsNil)
	return pending, data
}

func TestReconcileVersioned_HappyPath(t *testing.T) {
	now := time.Date(2027, 1, 2, 12, 0, 0, 0, time.UTC)
	reviewedIDs := []string{"CVE-2026-00001", "CVE-2026-00002", "CVE-2026-00003", "CVE-2026-00004"}

	t.Run("changed source lists only the IDs added or removed since the review", func(t *testing.T) {
		c := qt.New(t)
		t.Chdir(t.TempDir())
		c.Assert(reconcileVersioned(reviewedSnapshot(reviewedIDs), changedPage, now), qt.IsNil)
		pending, data := readReview(c)
		c.Assert(pending.SchemaVersion, qt.Equals, reviewSchemaVersion)
		c.Assert(pending.ReviewedSHA, qt.Equals, digestOf(reviewedPage))
		c.Assert(pending.ObservedSHA, qt.Equals, digestOf(changedPage))
		c.Assert(pending.AddedIDs, qt.DeepEquals, []string{"CVE-2027-00005"})
		c.Assert(pending.RemovedIDs, qt.DeepEquals, []string{"CVE-2026-00001"})
		c.Assert(pending.ObservedIDs, qt.DeepEquals, []string{"CVE-2026-00002", "CVE-2026-00003", "CVE-2026-00004", "CVE-2027-00005"})

		c.Assert(reconcileVersioned(reviewedSnapshot(reviewedIDs), changedPage, now.Add(24*time.Hour)), qt.IsNil)
		_, again := readReview(c)
		c.Assert(again, qt.DeepEquals, data)
		_, err := os.Stat(filepath.Join("internal", "vulnerability", "mariadb.json"))
		c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
	})

	t.Run("a pending report in an older shape is written again", func(t *testing.T) {
		c := qt.New(t)
		t.Chdir(t.TempDir())
		c.Assert(os.MkdirAll(filepath.Dir(reviewPath), 0o755), qt.IsNil)
		stale := review{SchemaVersion: 1, ReviewedSHA: digestOf(reviewedPage), ObservedSHA: digestOf(changedPage)}
		c.Assert(writeJSON(reviewPath, stale), qt.IsNil)
		c.Assert(reconcileVersioned(reviewedSnapshot(reviewedIDs), changedPage, now), qt.IsNil)
		pending, _ := readReview(c)
		c.Assert(pending.SchemaVersion, qt.Equals, reviewSchemaVersion)
		c.Assert(pending.AddedIDs, qt.DeepEquals, []string{"CVE-2027-00005"})
	})

	t.Run("the reviewed source removes its review report", func(t *testing.T) {
		c := qt.New(t)
		t.Chdir(t.TempDir())
		c.Assert(os.MkdirAll(filepath.Dir(reviewPath), 0o755), qt.IsNil)
		c.Assert(os.WriteFile(reviewPath, []byte("{}\n"), 0o600), qt.IsNil)
		c.Assert(reconcileVersioned(reviewedSnapshot(reviewedIDs), reviewedPage, now), qt.IsNil)
		_, err := os.Stat(reviewPath)
		c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
	})
}

func TestReconcileVersioned_FailurePath(t *testing.T) {
	now := time.Date(2027, 1, 2, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		sourceIDs []string
		source    []byte
		wantErr   string
	}{{
		name:    "snapshot records no source IDs",
		source:  changedPage,
		wantErr: `reviewed snapshot records no source_cve_ids, so a changed source cannot show what changed`,
	}, {
		name:      "recorded IDs omit one the reviewed page names",
		sourceIDs: []string{"CVE-2026-00001", "CVE-2026-00002", "CVE-2026-00003"},
		source:    reviewedPage,
		wantErr:   `source_cve_ids differ from the reviewed source: 1 not recorded, 0 recorded but absent`,
	}, {
		name:      "recorded IDs name one the reviewed page does not",
		sourceIDs: []string{"CVE-2026-00001", "CVE-2026-00002", "CVE-2026-00003", "CVE-2026-00004", "CVE-2026-00009"},
		source:    reviewedPage,
		wantErr:   `source_cve_ids differ from the reviewed source: 0 not recorded, 1 recorded but absent`,
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Chdir(t.TempDir())
			err := reconcileVersioned(reviewedSnapshot(test.sourceIDs), test.source, now)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			_, err = os.Stat(reviewPath)
			c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
		})
	}
}

func TestBuildReview_FailurePath(t *testing.T) {
	c := qt.New(t)
	snapshot := reviewedSnapshot([]string{"CVE-2026-00001", "CVE-2026-00002", "CVE-2026-00003", "CVE-2026-00004"})
	got, err := buildReview(snapshot, []byte("Vendor page without advisory records"), "abc", time.Now())
	c.Assert(err, qt.ErrorMatches, `changed source contains no CVE IDs`)
	c.Assert(got, qt.DeepEquals, review{})
}

func TestBundledVersionedSnapshotsMatchTheirReviewedSources(t *testing.T) {
	for _, product := range []string{"mariadb", "clickhouse"} {
		t.Run(product, func(t *testing.T) {
			c := qt.New(t)
			current, err := vulnerability.ReadVersionedSnapshot(product, "")
			c.Assert(err, qt.IsNil)
			c.Assert(current.Source, qt.Equals, sourceURLs[product])
			c.Assert(current.SourceCVEIDs, qt.Not(qt.HasLen), 0)
		})
	}
}

func TestRecentSourceNeedsPastRetrievalWithinRefreshInterval(t *testing.T) {
	c := qt.New(t)
	now := time.Date(2027, 1, 2, 12, 0, 0, 0, time.UTC)
	c.Assert(recent(now.Add(-13*24*time.Hour), now), qt.IsTrue)
	c.Assert(recent(now.Add(-refreshInterval), now), qt.IsFalse)
	c.Assert(recent(now.Add(time.Hour), now), qt.IsFalse)
}
