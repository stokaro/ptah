package main

// White-box testing required: the source updater's review decision is private
// to this command and has no exported API.

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
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

// reconcileOne reconciles a snapshot that reviews one page.
func reconcileOne(current vulnerability.VersionedSnapshot, page []byte, now time.Time) error {
	return reconcileVersioned(current, map[string][]byte{current.Source: page}, time.Time{}, now)
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
		c.Assert(reconcileOne(reviewedSnapshot(reviewedIDs), changedPage, now), qt.IsNil)
		pending, data := readReview(c)
		c.Assert(pending.SchemaVersion, qt.Equals, reviewSchemaVersion)
		c.Assert(pending.ReviewedSHA, qt.Equals, digestOf(reviewedPage))
		c.Assert(pending.ObservedSHA, qt.Equals, digestOf(changedPage))
		c.Assert(pending.AddedIDs, qt.DeepEquals, []string{"CVE-2027-00005"})
		c.Assert(pending.RemovedIDs, qt.DeepEquals, []string{"CVE-2026-00001"})
		c.Assert(pending.ObservedIDs, qt.DeepEquals, []string{"CVE-2026-00002", "CVE-2026-00003", "CVE-2026-00004", "CVE-2027-00005"})

		c.Assert(reconcileOne(reviewedSnapshot(reviewedIDs), changedPage, now.Add(24*time.Hour)), qt.IsNil)
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
		c.Assert(reconcileOne(reviewedSnapshot(reviewedIDs), changedPage, now), qt.IsNil)
		pending, _ := readReview(c)
		c.Assert(pending.SchemaVersion, qt.Equals, reviewSchemaVersion)
		c.Assert(pending.AddedIDs, qt.DeepEquals, []string{"CVE-2027-00005"})
	})

	t.Run("the reviewed source removes its review report", func(t *testing.T) {
		c := qt.New(t)
		t.Chdir(t.TempDir())
		c.Assert(os.MkdirAll(filepath.Dir(reviewPath), 0o755), qt.IsNil)
		c.Assert(os.WriteFile(reviewPath, []byte("{}\n"), 0o600), qt.IsNil)
		c.Assert(reconcileOne(reviewedSnapshot(reviewedIDs), reviewedPage, now), qt.IsNil)
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
			err := reconcileOne(reviewedSnapshot(test.sourceIDs), test.source, now)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			_, err = os.Stat(reviewPath)
			c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
		})
	}
}

func TestBuildReview_FailurePath(t *testing.T) {
	c := qt.New(t)
	snapshot := reviewedSnapshot([]string{"CVE-2026-00001", "CVE-2026-00002", "CVE-2026-00003", "CVE-2026-00004"})
	got, err := buildReview(snapshot.Product, reviewedSources(snapshot)[0], []byte("Vendor page without advisory records"), "abc", time.Now())
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
			for _, source := range reviewedSources(current) {
				c.Assert(validSource(product, source.url), qt.IsTrue, qt.Commentf("source %s", source.url))
				c.Assert(source.ids, qt.Not(qt.HasLen), 0, qt.Commentf("source %s", source.url))
			}
		})
	}
}

func TestRefreshAll_HappyPath(t *testing.T) {
	c := qt.New(t)
	var checked []string
	err := refreshAll([]string{"postgresql", "mariadb"}, func(product string) error {
		checked = append(checked, product)
		return nil
	})
	c.Assert(err, qt.IsNil)
	c.Assert(checked, qt.DeepEquals, []string{"postgresql", "mariadb"})
}

func TestRefreshAll_FailurePath(t *testing.T) {
	c := qt.New(t)
	errOffline := errors.New("source unreachable")
	errStale := errors.New("source_cve_ids are stale")
	failures := map[string]error{"postgresql": errOffline, "mariadb": errStale}
	var checked []string
	err := refreshAll([]string{"postgresql", "mariadb", "clickhouse"}, func(product string) error {
		checked = append(checked, product)
		return failures[product]
	})
	c.Assert(err, qt.ErrorMatches, "postgresql: source unreachable\nmariadb: source_cve_ids are stale")
	c.Assert(err, qt.ErrorIs, errOffline)
	c.Assert(err, qt.ErrorIs, errStale)
	c.Assert(checked, qt.DeepEquals, []string{"postgresql", "mariadb", "clickhouse"})
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

func TestUpdatesAfter_HappyPath(t *testing.T) {
	c := qt.New(t)
	index := []byte(`<a href="/security-alerts/cpuoct2026.html">October</a>
<a href="/security-alerts/cpujan2027.html">January</a>
<a href="/security-alerts/cspudec2026.html">December</a>
<a href="/security-alerts/cspunov2026.html">November</a>
<a href="/security-alerts/cspunov2026.html">November again</a>
<a href="/security-alerts/cpujul2026.html">July</a>
<a href="/security-alerts/alert-cve-2027-0001.html">Alert</a>`)
	got, err := updatesAfter(index, "https://www.oracle.com/security-alerts/cpuoct2026.html")
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{
		"https://www.oracle.com/security-alerts/cspunov2026.html",
		"https://www.oracle.com/security-alerts/cspudec2026.html",
		"https://www.oracle.com/security-alerts/cpujan2027.html",
	})
}

func TestUpdatesAfter_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		index    string
		reviewed string
		want     string
	}{
		{name: "index links no update", index: `<a href="/security-alerts/alert-cve-2027-0001.html">Alert</a>`,
			reviewed: "https://www.oracle.com/security-alerts/cpuoct2026.html", want: `the security alerts index links no update`},
		{name: "reviewed source is not an update page", index: `<a href="/security-alerts/cpuoct2026.html">October</a>`,
			reviewed: "https://example.org/cpuoct2026.html", want: `reviewed source "https://example.org/cpuoct2026.html" is not an Oracle update page`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := updatesAfter([]byte(tt.index), tt.reviewed)
			c.Assert(err, qt.ErrorMatches, tt.want)
			c.Assert(got, qt.IsNil)
		})
	}
}

// oracleIndex schedules updates the way the security alerts index does, one
// list for each kind of update, and links the given pages.
func oracleIndex(links ...string) []byte {
	var page strings.Builder
	page.WriteString("<p>Critical Patch Updates are released quarterly. The next four dates are:</p>\n<ul class=\"obullets\">\n" +
		"<li>19 January 2027</li>\n<li>20 April 2027</li>\n<li>20 July 2027</li>\n<li>19 October 2027</li>\n</ul>\n" +
		"<p>Critical Security Patch Updates are released monthly. The next four dates are:</p>\n<ul class=\"obullets\">\n" +
		"<li>15 December 2026</li>\n<li>16 February 2027</li>\n<li>16 March 2027</li>\n<li>18 May 2027</li>\n</ul>\n")
	for _, link := range links {
		fmt.Fprintf(&page, "<a href=\"%s\">update</a>\n", strings.TrimPrefix(link, "https://www.oracle.com"))
	}
	return []byte(page.String())
}

func TestScheduledUpdate_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		now  time.Time
		want time.Time
	}{
		{name: "the earliest date across both schedules", now: time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC),
			want: time.Date(2026, 12, 15, 0, 0, 0, 0, time.UTC)},
		{name: "a date that is today is still due", now: time.Date(2026, 12, 15, 23, 0, 0, 0, time.UTC),
			want: time.Date(2026, 12, 15, 0, 0, 0, 0, time.UTC)},
		{name: "a past date is passed over", now: time.Date(2026, 12, 16, 1, 0, 0, 0, time.UTC),
			want: time.Date(2027, 1, 19, 0, 0, 0, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := scheduledUpdate(oracleIndex(), tt.now)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

func TestScheduledUpdate_FailurePath(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		index string
		want  string
	}{
		{name: "no schedule", index: `<p>Updates</p>`, want: `the security alerts index has no update schedule in the expected shape`},
		{name: "a lead-in without its list", index: string(oracleIndex()) + `<p>The next four dates are:</p><p>soon</p>`,
			want: `the security alerts index has no update schedule in the expected shape`},
		{name: "an unreadable date", index: "next four dates are:</p><ul><li>mid-December</li></ul>", want: `unreadable scheduled update date "mid-December"`},
		{name: "every date past", index: "next four dates are:</p><ul><li>17 November 2026</li></ul>",
			want: `the security alerts index schedules no update from today on`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := scheduledUpdate([]byte(tt.index), now)
			c.Assert(err, qt.ErrorMatches, tt.want)
			c.Assert(got.IsZero(), qt.IsTrue)
		})
	}
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
		{name: "no MySQL matrix", page: "<h4 id=\"AppendixPS\">PeopleSoft</h4>", want: `MySQL risk matrix heading missing from the Oracle update page`},
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
		c.Assert(reconcileOne(mysqlReviewedSnapshot(c, "CVE-2026-00002"), oraclePage("nonce-9", "CVE-2026-00002"), now), qt.IsNil)
		_, err := os.Stat(mysqlReview)
		c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
	})

	t.Run("a changed matrix is reviewed by its own IDs", func(t *testing.T) {
		c := qt.New(t)
		t.Chdir(t.TempDir())
		c.Assert(reconcileOne(mysqlReviewedSnapshot(c, "CVE-2026-00002"), oraclePage("nonce-9", "CVE-2026-00002 CVE-2026-00003"), now), qt.IsNil)
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

const (
	novemberUpdate = "https://www.oracle.com/security-alerts/cspunov2026.html"
	octoberUpdate  = "https://www.oracle.com/security-alerts/cpuoct2026.html"
	decemberUpdate = "https://www.oracle.com/security-alerts/cspudec2026.html"
)

// servedPages answers the URLs it holds and refuses every other, so a check
// that reads a page it should not reach fails rather than reading nothing.
func servedPages(pages map[string][]byte) fetcher {
	return func(url string) ([]byte, error) {
		page, ok := pages[url]
		if !ok {
			return nil, fmt.Errorf("%s is not served", url)
		}
		return page, nil
	}
}

// twoPageSnapshot reviews the November update as its source and the October
// one as an additional source, each pinning one advisory.
func twoPageSnapshot(c *qt.C, retrievedAt, validUntil time.Time) vulnerability.VersionedSnapshot {
	novContent, err := advisoryContent("mysql", oraclePage("nonce-0", "CVE-2026-00002"))
	c.Assert(err, qt.IsNil)
	octContent, err := advisoryContent("mysql", oraclePage("nonce-0", "CVE-2026-00001"))
	c.Assert(err, qt.IsNil)
	snapshot := vulnerability.VersionedSnapshot{
		SchemaVersion: 2, Product: "mysql", Source: novemberUpdate, RetrievedAt: retrievedAt, ValidUntil: validUntil,
		SourceSHA256: digestOf(oraclePage("nonce-0", "CVE-2026-00002")), AdvisoryContentSHA256: digestOf(novContent),
		Coverage: "partial", CoverageNote: "two updates", CoveredLines: []string{"8.4"},
		Advisories: []vulnerability.VersionedAdvisory{{
			ID: "CVE-2026-00001", Component: "core", Description: "October server issue", URL: octoberUpdate + "#AppendixMSQL",
			Ranges: []vulnerability.VersionRange{{Line: "8.4", From: "8.4.0", Fixed: "8.4.13"}},
		}, {
			ID: "CVE-2026-00002", Component: "core", Description: "November server issue", URL: novemberUpdate + "#AppendixMSQL",
			Ranges: []vulnerability.VersionRange{{Line: "8.4", From: "8.4.0", LastAffected: "8.4.13"}},
		}},
		SourceCVEIDs: []string{"CVE-2026-00002"},
		AdditionalSources: []vulnerability.AdvisorySource{{
			URL: octoberUpdate, SHA256: digestOf(oraclePage("nonce-0", "CVE-2026-00001")), AdvisoryContentSHA256: digestOf(octContent),
			CoveredLines: []string{"8.4"}, CVEIDs: []string{"CVE-2026-00001"},
		}},
	}
	c.Assert(snapshot.Validate(), qt.IsNil)
	return snapshot
}

func readMySQLReview(c *qt.C) review {
	data, err := os.ReadFile(filepath.Join("internal", "vulnerability", "review", "mysql.json"))
	c.Assert(err, qt.IsNil)
	var pending review
	c.Assert(json.Unmarshal(data, &pending), qt.IsNil)
	return pending
}

// updateAfter names the Oracle update page due the month after source: a
// Critical Patch Update in January, April, July and October, and a Critical
// Security Patch Update in every other month.
func updateAfter(c *qt.C, source string) string {
	key, ok := updateKey(source)
	c.Assert(ok, qt.IsTrue, qt.Commentf("source %s", source))
	month := updateMonths[(key+1)%len(updateMonths)]
	kind := map[bool]string{true: "cpu", false: "cspu"}[slices.Contains([]string{"jan", "apr", "jul", "oct"}, month)]
	return fmt.Sprintf("https://www.oracle.com/security-alerts/%s%s%d.html", kind, month, (key+1)/len(updateMonths))
}

func TestUpdateAfter_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(updateAfter(c, novemberUpdate), qt.Equals, decemberUpdate)
	c.Assert(updateAfter(c, decemberUpdate), qt.Equals, "https://www.oracle.com/security-alerts/cpujan2027.html")
}

// TestCheckVersioned_NewerUpdateWithMySQLRowsIsReviewed drives the bundled
// MySQL snapshot through the daily check when the index links the next
// update with a MySQL risk matrix: the check leaves a review naming the new
// page and reads none of the reviewed pages, which the fetcher does not
// serve. The next page is derived from the bundled source, so the test holds
// after the snapshot moves to a newer update.
func TestCheckVersioned_NewerUpdateWithMySQLRowsIsReviewed(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	current, err := vulnerability.ReadVersionedSnapshot("mysql", "")
	c.Assert(err, qt.IsNil)
	next := updateAfter(c, current.Source)
	pages := map[string][]byte{
		oracleIndexURL: oracleIndex(current.Source, next),
		next:           oraclePage("nonce-9", "CVE-2099-99001 CVE-2099-99002"),
	}
	c.Assert(checkVersioned(current, servedPages(pages), current.RetrievedAt.Add(24*time.Hour)), qt.IsNil)
	pending := readMySQLReview(c)
	c.Assert(pending.Source, qt.Equals, next)
	c.Assert(pending.ReviewedSource, qt.Equals, current.Source)
	c.Assert(pending.AddedIDs, qt.DeepEquals, []string{"CVE-2099-99001", "CVE-2099-99002"})
}

// TestCheckVersioned_UpdateWithoutMySQLRowsMovesValidUntil reads a newer
// update that has nothing for MySQL: the reviewed pages are still complete,
// so the check refreshes the snapshot and records the next scheduled date,
// although the snapshot was verified a day before.
func TestCheckVersioned_UpdateWithoutMySQLRowsMovesValidUntil(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	c.Assert(os.MkdirAll(filepath.Join("internal", "vulnerability"), 0o755), qt.IsNil)
	now := time.Date(2026, 12, 16, 9, 0, 0, 0, time.UTC)
	current := twoPageSnapshot(c, now.Add(-24*time.Hour), time.Date(2026, 12, 15, 0, 0, 0, 0, time.UTC))
	pages := map[string][]byte{
		oracleIndexURL: oracleIndex(octoberUpdate, novemberUpdate, decemberUpdate),
		decemberUpdate: []byte(`<h4 id="AppendixPS">Oracle PeopleSoft Risk Matrix</h4>CVE-2026-90000`),
		novemberUpdate: oraclePage("nonce-5", "CVE-2026-00002"),
		octoberUpdate:  oraclePage("nonce-6", "CVE-2026-00001"),
	}
	c.Assert(checkVersioned(current, servedPages(pages), now), qt.IsNil)
	data, err := os.ReadFile(filepath.Join("internal", "vulnerability", "mysql.json"))
	c.Assert(err, qt.IsNil)
	refreshed, err := vulnerability.DecodeVersionedSnapshot(data)
	c.Assert(err, qt.IsNil)
	c.Assert(refreshed.ValidUntil, qt.Equals, time.Date(2027, 1, 19, 0, 0, 0, 0, time.UTC))
	c.Assert(refreshed.RetrievedAt, qt.Equals, now)
	c.Assert(refreshed.SourceSHA256, qt.Equals, digestOf(pages[novemberUpdate]))
	c.Assert(refreshed.AdditionalSources[0].SHA256, qt.Equals, digestOf(pages[octoberUpdate]))
	_, err = os.Stat(filepath.Join("internal", "vulnerability", "review", "mysql.json"))
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
}

// TestCheckVersioned_ChangedAdditionalSourceIsReviewed revises the older of
// the two reviewed pages, as Oracle does when it reissues an update: the
// review names that page and compares it with the IDs recorded for it.
func TestCheckVersioned_ChangedAdditionalSourceIsReviewed(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	now := time.Date(2026, 12, 1, 9, 0, 0, 0, time.UTC)
	current := twoPageSnapshot(c, now.Add(-24*time.Hour), time.Date(2026, 12, 15, 0, 0, 0, 0, time.UTC))
	pages := map[string][]byte{
		oracleIndexURL: oracleIndex(octoberUpdate, novemberUpdate),
		novemberUpdate: oraclePage("nonce-5", "CVE-2026-00002"),
		octoberUpdate:  oraclePage("nonce-6", "CVE-2026-00001 CVE-2026-00007"),
	}
	c.Assert(checkVersioned(current, servedPages(pages), now), qt.IsNil)
	pending := readMySQLReview(c)
	c.Assert(pending.Source, qt.Equals, octoberUpdate)
	c.Assert(pending.ReviewedSource, qt.Equals, "")
	c.Assert(pending.AddedIDs, qt.DeepEquals, []string{"CVE-2026-00007"})
	c.Assert(pending.RemovedIDs, qt.HasLen, 0)
}

func TestCheckVersioned_FailurePath(t *testing.T) {
	now := time.Date(2026, 12, 1, 9, 0, 0, 0, time.UTC)
	reviewed := map[string][]byte{
		novemberUpdate: oraclePage("nonce-5", "CVE-2026-00002"),
		octoberUpdate:  oraclePage("nonce-6", "CVE-2026-00001"),
	}
	tests := []struct {
		name       string
		omit       string
		index      []byte
		octoberIDs []string
		want       string
	}{
		{name: "an unreachable reviewed page", omit: octoberUpdate, index: oracleIndex(octoberUpdate, novemberUpdate), octoberIDs: []string{"CVE-2026-00001"},
			want: `read https://www\.oracle\.com/security-alerts/cpuoct2026\.html: .* is not served`},
		{name: "an index without a schedule", index: []byte(`<a href="/security-alerts/cspunov2026.html">November</a>`),
			octoberIDs: []string{"CVE-2026-00001"}, want: `the security alerts index has no update schedule in the expected shape`},
		{name: "the IDs recorded for the additional page differ from it", index: oracleIndex(octoberUpdate, novemberUpdate), octoberIDs: []string{"CVE-2026-00001", "CVE-2026-00008"},
			want: `the cve_ids of https://www\.oracle\.com/security-alerts/cpuoct2026\.html differ from the reviewed source: not recorded none; recorded but absent CVE-2026-00008`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			t.Chdir(t.TempDir())
			current := twoPageSnapshot(c, now.Add(-24*time.Hour), time.Date(2026, 12, 15, 0, 0, 0, 0, time.UTC))
			current.AdditionalSources[0].CVEIDs = tt.octoberIDs
			pages := maps.Clone(reviewed)
			delete(pages, tt.omit)
			pages[oracleIndexURL] = tt.index
			c.Assert(checkVersioned(current, servedPages(pages), now), qt.ErrorMatches, tt.want)
			_, err := os.Stat(filepath.Join("internal", "vulnerability", "review", "mysql.json"))
			c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
		})
	}
}
