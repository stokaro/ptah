package main

// White-box testing required: the source updater's review decision is private
// to this command and has no exported API.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/vulnerability"
)

// reviewedPage names four IDs, newest first as the vendor page does. The
// snapshot pins one and excludes another, so CVE-2026-00001 and CVE-2026-00004
// are on the page without being pinned: a review that compared against the
// pinned set would report them as changed.
var reviewedPage = []byte("CVE-2026-00004\nCVE-2026-00003\nCVE-2026-00002\nCVE-2026-00001\n")

// changedPage drops CVE-2026-00001 and adds CVE-2027-00005, named twice and
// not next to itself, so only a sorted reading removes the repeat.
var changedPage = []byte("CVE-2027-00005\nCVE-2026-00004\nCVE-2027-00005\nCVE-2026-00003\nCVE-2026-00002\n")

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
		wantErr:   `source_cve_ids differ from the reviewed source: not recorded CVE-2026-00004; recorded but absent none`,
	}, {
		name:      "recorded IDs name one the reviewed page does not",
		sourceIDs: []string{"CVE-2026-00001", "CVE-2026-00002", "CVE-2026-00003", "CVE-2026-00004", "CVE-2026-00009"},
		source:    reviewedPage,
		wantErr:   `source_cve_ids differ from the reviewed source: not recorded none; recorded but absent CVE-2026-00009`,
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

// TestBundledVersionedSnapshotsRecordSourceCVEIDs checks only that each
// bundled snapshot carries a list to diff against. Whether the list matches
// its page needs the page, so the daily source check is what catches a stale
// list.
func TestBundledVersionedSnapshotsRecordSourceCVEIDs(t *testing.T) {
	for _, product := range []string{"mariadb", "clickhouse", "mysql"} {
		t.Run(product, func(t *testing.T) {
			c := qt.New(t)
			current, err := vulnerability.ReadVersionedSnapshot(product, "")
			c.Assert(err, qt.IsNil)
			c.Assert(validSource(product, current.Source), qt.IsTrue)
			c.Assert(current.SourceCVEIDs, qt.Not(qt.HasLen), 0)
		})
	}
}

type readResult struct {
	source []byte
	err    error
}

func TestRefreshAll_HappyPath(t *testing.T) {
	c := qt.New(t)
	reads := map[string]readResult{"postgresql": {source: []byte("pg")}, "mariadb": {source: []byte("maria")}}
	var applied []string
	err := refreshAll([]string{"postgresql", "mariadb"},
		func(product string) ([]byte, error) { return reads[product].source, reads[product].err },
		func(product string, source []byte) error {
			applied = append(applied, product+"="+string(source))
			return nil
		},
	)
	c.Assert(err, qt.IsNil)
	c.Assert(applied, qt.DeepEquals, []string{"postgresql=pg", "mariadb=maria"})
}

func TestRefreshAll_FailurePath(t *testing.T) {
	c := qt.New(t)
	errOffline := errors.New("source unreachable")
	errStale := errors.New("source_cve_ids are stale")
	reads := map[string]readResult{
		"postgresql": {err: errOffline}, "mariadb": {source: []byte("maria")}, "clickhouse": {source: []byte("ch")},
	}
	applyErrors := map[string]error{"mariadb": errStale}
	var applied []string
	err := refreshAll([]string{"postgresql", "mariadb", "clickhouse"},
		func(product string) ([]byte, error) { return reads[product].source, reads[product].err },
		func(product string, source []byte) error {
			applied = append(applied, product+"="+string(source))
			return applyErrors[product]
		},
	)
	c.Assert(err, qt.ErrorMatches, "postgresql: source unreachable\nmariadb: source_cve_ids are stale")
	c.Assert(err, qt.ErrorIs, errOffline)
	c.Assert(err, qt.ErrorIs, errStale)
	c.Assert(applied, qt.DeepEquals, []string{"mariadb=maria", "clickhouse=ch"})
}

func TestIDList_HappyPath(t *testing.T) {
	twelve := make([]string, 0, 12)
	for index := range 12 {
		twelve = append(twelve, fmt.Sprintf("CVE-2026-%05d", index+1))
	}
	tests := []struct {
		name string
		ids  []string
		want string
	}{
		{name: "no ID", ids: nil, want: "none"},
		{name: "IDs within the limit", ids: twelve[:2], want: "CVE-2026-00001, CVE-2026-00002"},
		{name: "IDs at the limit", ids: twelve[:10], want: strings.Join(twelve[:10], ", ")},
		{name: "IDs past the limit", ids: twelve, want: strings.Join(twelve[:10], ", ") + " and 2 more"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(idList(test.ids), qt.Equals, test.want)
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

func TestNewestCriticalPatchUpdate_HappyPath(t *testing.T) {
	c := qt.New(t)
	index := []byte(`<a href="/security-alerts/cpuoct2026.html">October</a>
<a href="/security-alerts/cpujan2027.html">January</a>
<a href="/security-alerts/cpujul2026.html">July</a>
<a href="/security-alerts/alert-cve-2027-0001.html">Alert</a>`)
	got, err := newestCriticalPatchUpdate(index)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "https://www.oracle.com/security-alerts/cpujan2027.html")
}

func TestNewestCriticalPatchUpdate_FailurePath(t *testing.T) {
	c := qt.New(t)
	got, err := newestCriticalPatchUpdate([]byte(`<a href="/security-alerts/alert-cve-2027-0001.html">Alert</a>`))
	c.Assert(err, qt.ErrorMatches, `the Critical Patch Update index links no update`)
	c.Assert(got, qt.Equals, "")
}

// oraclePage carries a volatile script outside the MySQL matrix, the way an
// Oracle page does, and another product's appendix after it.
func oraclePage(script, matrix string) []byte {
	return []byte("<script>" + script + "</script>\n<h4 id=\"AppendixMSQL\">Oracle MySQL Risk Matrix</h4>\n" + matrix +
		"\n<h4 id=\"AppendixPS\">Oracle PeopleSoft Risk Matrix</h4>\nCVE-2026-90000\n")
}

func TestAdvisoryContent_HappyPath(t *testing.T) {
	c := qt.New(t)
	first, err := advisoryContent("mysql", oraclePage("nonce-1", "CVE-2026-00002"))
	c.Assert(err, qt.IsNil)
	second, err := advisoryContent("mysql", oraclePage("nonce-2", "CVE-2026-00002"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(first), qt.Equals, "<h4 id=\"AppendixMSQL\">Oracle MySQL Risk Matrix</h4>\nCVE-2026-00002\n")
	c.Assert(second, qt.DeepEquals, first)
	whole, err := advisoryContent("mariadb", reviewedPage)
	c.Assert(err, qt.IsNil)
	c.Assert(whole, qt.DeepEquals, reviewedPage)
}

func TestAdvisoryContent_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		page string
		want string
	}{
		{name: "no MySQL matrix", page: "<h4 id=\"AppendixPS\">PeopleSoft</h4>", want: `MySQL risk matrix heading missing from the Critical Patch Update`},
		{name: "no following appendix", page: "<h4 id=\"AppendixMSQL\">MySQL</h4>CVE-2026-00002", want: `MySQL risk matrix has no following appendix`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := advisoryContent("mysql", []byte(tt.page))
			c.Assert(err, qt.ErrorMatches, tt.want)
			c.Assert(got, qt.IsNil)
		})
	}
}

func mysqlReviewedSnapshot(c *qt.C, matrix string) vulnerability.VersionedSnapshot {
	content, err := advisoryContent("mysql", oraclePage("nonce-0", matrix))
	c.Assert(err, qt.IsNil)
	snapshot := vulnerability.VersionedSnapshot{
		SchemaVersion: 2, Product: "mysql", Source: "https://www.oracle.com/security-alerts/cpuoct2026.html",
		RetrievedAt:  time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		SourceSHA256: digestOf(oraclePage("nonce-0", matrix)), AdvisoryContentSHA256: digestOf(content),
		Coverage: "partial", CoverageNote: "one update", CoveredLines: []string{"8.4"},
		Advisories: []vulnerability.VersionedAdvisory{{
			ID: "CVE-2026-00002", Component: "core", Description: "example server issue",
			URL:    "https://www.oracle.com/security-alerts/cpuoct2026.html#AppendixMSQL",
			Ranges: []vulnerability.VersionRange{{Line: "8.4", From: "8.4.0", Fixed: "8.4.13"}},
		}},
		SourceCVEIDs: []string{"CVE-2026-00002"},
	}
	c.Assert(snapshot.Validate(), qt.IsNil)
	return snapshot
}

// TestReconcileVersioned_MySQLComparesTheMatrix holds that a page whose only
// change is its script is the reviewed page, and that a changed matrix
// leaves a review of the matrix's IDs, not the whole page's.
func TestReconcileVersioned_MySQLComparesTheMatrix(t *testing.T) {
	now := time.Date(2027, 1, 2, 12, 0, 0, 0, time.UTC)
	mysqlReview := filepath.Join("internal", "vulnerability", "review", "mysql.json")

	t.Run("a new script is not a change", func(t *testing.T) {
		c := qt.New(t)
		t.Chdir(t.TempDir())
		c.Assert(reconcileVersioned(mysqlReviewedSnapshot(c, "CVE-2026-00002"), oraclePage("nonce-9", "CVE-2026-00002"), now), qt.IsNil)
		_, err := os.Stat(mysqlReview)
		c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
	})

	t.Run("a changed matrix is reviewed by its own IDs", func(t *testing.T) {
		c := qt.New(t)
		t.Chdir(t.TempDir())
		c.Assert(reconcileVersioned(mysqlReviewedSnapshot(c, "CVE-2026-00002"), oraclePage("nonce-9", "CVE-2026-00002 CVE-2026-00003"), now), qt.IsNil)
		data, err := os.ReadFile(mysqlReview)
		c.Assert(err, qt.IsNil)
		var pending review
		c.Assert(json.Unmarshal(data, &pending), qt.IsNil)
		c.Assert(pending.AddedIDs, qt.DeepEquals, []string{"CVE-2026-00003"})
		c.Assert(pending.ObservedIDs, qt.DeepEquals, []string{"CVE-2026-00002", "CVE-2026-00003"})
		c.Assert(pending.ReviewedSource, qt.Equals, "")
	})
}

// TestReviewSuccessor_HappyPath drives the path the daily
// check takes when the index links a newer update than the bundled snapshot
// names: the review names both pages and the IDs the new one adds.
func TestReviewSuccessor_HappyPath(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	current := mysqlReviewedSnapshot(c, "CVE-2026-00002")
	newer := "https://www.oracle.com/security-alerts/cpujan2027.html"
	c.Assert(reviewSuccessor(current, newer, oraclePage("nonce-9", "CVE-2027-00001"), time.Date(2027, 1, 21, 0, 0, 0, 0, time.UTC)), qt.IsNil)
	data, err := os.ReadFile(filepath.Join("internal", "vulnerability", "review", "mysql.json"))
	c.Assert(err, qt.IsNil)
	var pending review
	c.Assert(json.Unmarshal(data, &pending), qt.IsNil)
	c.Assert(pending.Source, qt.Equals, newer)
	c.Assert(pending.ReviewedSource, qt.Equals, current.Source)
	c.Assert(pending.ReviewedSHA, qt.Equals, current.AdvisoryContentSHA256)
	c.Assert(pending.AddedIDs, qt.DeepEquals, []string{"CVE-2027-00001"})
	c.Assert(pending.RemovedIDs, qt.DeepEquals, []string{"CVE-2026-00002"})
}

func TestReviewSuccessor_FailurePath(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	err := reviewSuccessor(mysqlReviewedSnapshot(c, "CVE-2026-00002"), "https://example.org/cpujan2027.html", oraclePage("n", "CVE-2027-00001"), time.Now())
	c.Assert(err, qt.ErrorMatches, `unexpected reviewed source .* or observed source "https://example.org/cpujan2027.html"`)
}
