package main

// White-box testing required: the guide reader and the release schedule are
// private to this command and have no exported API.

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/vulnerability"
)

const guideNextURL = "https://api.msrc.microsoft.com/sug/v2.0/sugodata/v2.0/en-US/affectedProduct?$filter=productFamily%20eq%20%27SQL%20Server%27&$skip=2"

// guideRows are four rows of the guide's SQL Server list: two engine rows of
// one advisory, an engine row of another, and a driver row, which is out of
// scope. nonce stands for the fields that change on every request.
func guideRows(nonce string) []map[string]any {
	kb := func(article, build string) []map[string]any {
		return []map[string]any{{"articleName": article, "fixedBuildNumber": build, "supercedence": nonce, "ordinal": 0}}
	}
	return []map[string]any{
		{"id": "row-1", "cveNumber": "CVE-2026-40001", "product": "Microsoft SQL Server 2022 for x64-based Systems (GDR)",
			"baseScore": "8.8", "releaseNumber": "2026-Nov", "temporalScore": nonce, "kbArticles": kb("5000001", "16.0.1140.6")},
		{"id": "row-2", "cveNumber": "CVE-2026-40001", "product": "Microsoft SQL Server 2022 for x64-based Systems (CU 19)",
			"baseScore": "8.8", "releaseNumber": "2026-Nov", "temporalScore": nonce, "kbArticles": kb("5000002", "16.0.4200.1")},
		{"id": "row-3", "cveNumber": "CVE-2026-40003", "product": "Microsoft ODBC Driver 18 for SQL Server on Linux",
			"baseScore": "7.5", "temporalScore": nonce, "kbArticles": kb("5000003", "18.5.1.1")},
		{"id": "row-4", "cveNumber": "CVE-2026-40002", "product": "Microsoft SQL Server 2019 for x64-based Systems (GDR)",
			"baseScore": "", "releaseNumber": "2026-Nov", "temporalScore": nonce, "kbArticles": kb("5000004", "15.0.2135.5")},
	}
}

// guidePages serves rows as the guide does: two pages, the first stating the
// row count and linking the second.
func guidePages(c *qt.C, rows []map[string]any) map[string][]byte {
	c.Helper()
	first, err := json.Marshal(map[string]any{"@odata.count": len(rows), "value": rows[:2], "@odata.nextLink": guideNextURL})
	c.Assert(err, qt.IsNil)
	second, err := json.Marshal(map[string]any{"value": rows[2:]})
	c.Assert(err, qt.IsNil)
	return map[string][]byte{guideURL: first, guideNextURL: second}
}

// cvrfIndex lists the monthly releases named.
func cvrfIndex(c *qt.C, months ...string) []byte {
	c.Helper()
	value := make([]map[string]string, 0, len(months))
	for _, month := range months {
		value = append(value, map[string]string{"ID": month, "CvrfUrl": "https://api.msrc.microsoft.com/cvrf/v3.0/cvrf/" + month})
	}
	data, err := json.Marshal(map[string]any{"value": value})
	c.Assert(err, qt.IsNil)
	return data
}

func TestGuideContent_HappyPath(t *testing.T) {
	c := qt.New(t)
	first, err := readGuide(servedPages(guidePages(c, guideRows("nonce-1"))), guideURL)
	c.Assert(err, qt.IsNil)
	again, err := readGuide(servedPages(guidePages(c, guideRows("nonce-2"))), guideURL)
	c.Assert(err, qt.IsNil)

	content, err := guideContent(first)
	c.Assert(err, qt.IsNil)
	repeated, err := guideContent(again)
	c.Assert(err, qt.IsNil)

	c.Assert(string(repeated), qt.Equals, string(content))
	c.Assert(sourceCVEIDs(content), qt.DeepEquals, []string{"CVE-2026-40001", "CVE-2026-40002"})
	var entries []guideEntry
	c.Assert(json.Unmarshal(content, &entries), qt.IsNil)
	c.Assert(entries, qt.DeepEquals, []guideEntry{
		{CVE: "CVE-2026-40001", Product: "Microsoft SQL Server 2022 for x64-based Systems (CU 19)", Month: "2026-Nov", CVSS: "8.8",
			Fixes: []guideFix{{KB: "5000002", Build: "16.0.4200.1"}}},
		{CVE: "CVE-2026-40001", Product: "Microsoft SQL Server 2022 for x64-based Systems (GDR)", Month: "2026-Nov", CVSS: "8.8",
			Fixes: []guideFix{{KB: "5000001", Build: "16.0.1140.6"}}},
		{CVE: "CVE-2026-40002", Product: "Microsoft SQL Server 2019 for x64-based Systems (GDR)", Month: "2026-Nov",
			Fixes: []guideFix{{KB: "5000004", Build: "15.0.2135.5"}}},
	})
}

// TestGuideContent_RevisedFixChangesTheContent is the control for the test
// above: a revised fixed build is a change the review must see.
func TestGuideContent_RevisedFixChangesTheContent(t *testing.T) {
	c := qt.New(t)
	rows := guideRows("nonce-1")
	original, err := guideContent(mustReadGuide(c, rows))
	c.Assert(err, qt.IsNil)
	rows[0]["kbArticles"] = []map[string]any{{"articleName": "5000001", "fixedBuildNumber": "16.0.1145.1"}}

	revised, err := guideContent(mustReadGuide(c, rows))

	c.Assert(err, qt.IsNil)
	c.Assert(string(revised), qt.Not(qt.Equals), string(original))
}

// TestGuideContent_OrderIsNotContent reads the same rows in reverse, with a
// row's two KB articles reversed too: the guide pages by row, and the review
// is about what the rows say.
func TestGuideContent_OrderIsNotContent(t *testing.T) {
	c := qt.New(t)
	twoArticles := func(first, second map[string]any) map[string]any {
		return map[string]any{"id": "row-5", "cveNumber": "CVE-2026-40004", "product": "Microsoft SQL Server 2025 for x64-based Systems (CU 4)",
			"baseScore": "7.8", "kbArticles": []map[string]any{first, second}}
	}
	cu4 := map[string]any{"articleName": "5000005", "fixedBuildNumber": "17.0.4040.2"}
	cu5 := map[string]any{"articleName": "5000006", "fixedBuildNumber": "17.0.4055.1"}
	rows := append(guideRows("nonce-1"), twoArticles(cu4, cu5))
	reversed := append(guideRows("nonce-1"), twoArticles(cu5, cu4))
	slices.Reverse(reversed)
	original, err := guideContent(mustReadGuide(c, rows))
	c.Assert(err, qt.IsNil)

	reordered, err := guideContent(mustReadGuide(c, reversed))

	c.Assert(err, qt.IsNil)
	c.Assert(string(reordered), qt.Equals, string(original))
}

func mustReadGuide(c *qt.C, rows []map[string]any) []byte {
	c.Helper()
	pages, err := readGuide(servedPages(guidePages(c, rows)), guideURL)
	c.Assert(err, qt.IsNil)
	return pages
}

func TestGuideContent_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name  string
		pages string
		want  string
	}{
		{name: "count short of the rows", pages: `{"@odata.count": 3, "value": [{"id": "a", "cveNumber": "CVE-2026-40001", "product": "Microsoft SQL Server 2022 for x64-based Systems (GDR)"}]}`,
			want: `the guide's pages hold 1 rows, which is not the row count its first page states`},
		{name: "no count", pages: `{"value": [{"id": "a", "cveNumber": "CVE-2026-40001", "product": "Microsoft SQL Server 2022 for x64-based Systems (GDR)"}]}`,
			want: `the guide's pages hold 1 rows, which is not the row count its first page states`},
		{name: "repeated row", pages: `{"@odata.count": 2, "value": [{"id": "a", "cveNumber": "CVE-2026-40001", "product": "x"}]}{"value": [{"id": "a", "cveNumber": "CVE-2026-40001", "product": "x"}]}`,
			want: `guide page 2 repeats a row or holds one with no ID or advisory: "a"`},
		{name: "row without an advisory", pages: `{"@odata.count": 1, "value": [{"id": "a", "cveNumber": "", "product": "x"}]}`,
			want: `guide page 1 repeats a row or holds one with no ID or advisory: "a"`},
		{name: "page without rows", pages: `{"@odata.count": 0}`, want: `guide page 1 has no value list`},
		{name: "no engine row", pages: `{"@odata.count": 1, "value": [{"id": "a", "cveNumber": "CVE-2026-40003", "product": "Microsoft ODBC Driver 18 for SQL Server on Linux"}]}`,
			want: `the guide lists no SQL Server engine row`},
		{name: "not JSON", pages: `<html>challenge</html>`, want: `decode guide page 1: .*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			content, err := guideContent([]byte(test.pages))
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(content, qt.IsNil)
		})
	}
}

func TestReadGuide_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, next, want string
	}{
		{name: "next page off the API", next: "https://example.org/sug/v2.0/en-US/affectedProduct?$filter=productFamily%20eq%20%27SQL%20Server%27",
			want: `the guide links a next page "https://example.org/.*" that is not the same list on its API`},
		{name: "next page of another list", next: "https://api.msrc.microsoft.com/sug/v2.0/en-US/affectedProduct?$filter=productFamily%20eq%20%27Windows%27",
			want: `the guide links a next page ".*Windows.*" that is not the same list on its API`},
		{name: "next page without TLS", next: "http://api.msrc.microsoft.com/sug/v2.0/en-US/affectedProduct?$filter=productFamily%20eq%20%27SQL%20Server%27",
			want: `the guide links a next page "http://.*" that is not the same list on its API`},
		{name: "list without end", next: guideURL, want: `the guide links more than 64 pages`},
		{name: "unserved next page", next: guideNextURL, want: `read https://api.msrc.microsoft.com/.*skip=2: .* is not served`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			page := fmt.Appendf(nil, `{"@odata.count": 1, "value": [], "@odata.nextLink": %q}`, test.next)
			pages, err := readGuide(servedPages(map[string][]byte{guideURL: page}), guideURL)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(pages, qt.IsNil)
		})
	}
}

func TestPatchTuesday_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		now  time.Time
		want time.Time
	}{
		{name: "before this month's", now: time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC), want: time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)},
		{name: "after this month's", now: time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC), want: time.Date(2026, 12, 8, 0, 0, 0, 0, time.UTC)},
		{name: "on the day", now: time.Date(2026, 12, 8, 23, 0, 0, 0, time.UTC), want: time.Date(2026, 12, 8, 0, 0, 0, 0, time.UTC)},
		{name: "into the next year", now: time.Date(2026, 12, 9, 7, 0, 0, 0, time.UTC), want: time.Date(2027, 1, 12, 0, 0, 0, 0, time.UTC)},
		{name: "month opening on a Tuesday", now: time.Date(2026, 9, 2, 7, 0, 0, 0, time.UTC), want: time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(patchTuesday(test.now), qt.Equals, test.want)
		})
	}
}

func TestConfirmPublishedRelease_HappyPath(t *testing.T) {
	c := qt.New(t)
	december := time.Date(2026, 12, 8, 0, 0, 0, 0, time.UTC)
	c.Assert(confirmPublishedRelease(december, december, cvrfIndex(c, "2026-Nov")), qt.IsNil)
	c.Assert(confirmPublishedRelease(time.Time{}, december, cvrfIndex(c, "2026-Nov")), qt.IsNil)
	c.Assert(confirmPublishedRelease(december, december.AddDate(0, 1, 4), cvrfIndex(c, "2026-Nov", "2026-Dec")), qt.IsNil)
}

func TestConfirmPublishedRelease_FailurePath(t *testing.T) {
	c := qt.New(t)
	december := time.Date(2026, 12, 8, 0, 0, 0, 0, time.UTC)
	c.Assert(confirmPublishedRelease(december, december.AddDate(0, 1, 4), cvrfIndex(c, "2026-Nov")), qt.ErrorMatches,
		`the security release index lists no 2026-Dec release, so the release due on 2026-12-08 is unread`)
	c.Assert(confirmPublishedRelease(december, december.AddDate(0, 1, 4), []byte("<html>")), qt.ErrorMatches,
		`decode the security release index: .*`)
}

// guideSnapshot reviews the guide pages rows serves, pinning both engine
// advisories.
func guideSnapshot(c *qt.C, rows []map[string]any, retrievedAt, validUntil time.Time) vulnerability.VersionedSnapshot {
	c.Helper()
	pages := mustReadGuide(c, rows)
	content, err := guideContent(pages)
	c.Assert(err, qt.IsNil)
	snapshot := vulnerability.VersionedSnapshot{
		SchemaVersion: 2, Product: "sqlserver", Source: guideURL, RetrievedAt: retrievedAt, ValidUntil: validUntil,
		SourceSHA256: digestOf(pages), AdvisoryContentSHA256: digestOf(content), Coverage: "complete",
		CoveredLines: []string{"15.0", "16.0"},
		Advisories: []vulnerability.VersionedAdvisory{{
			ID: "CVE-2026-40001", Component: "core", Description: "fixed on both branches", URL: "https://msrc.microsoft.com/update-guide/vulnerability/CVE-2026-40001",
			Ranges: []vulnerability.VersionRange{
				{Line: "16.0", Branch: "gdr", From: "16.0", Fixed: "16.0.1140.6"},
				{Line: "16.0", Branch: "cu", From: "16.0", Fixed: "16.0.4200.1"},
			},
		}, {
			ID: "CVE-2026-40002", Component: "core", Description: "fixed on the GDR branch", URL: "https://msrc.microsoft.com/update-guide/vulnerability/CVE-2026-40002",
			Ranges: []vulnerability.VersionRange{{Line: "15.0", Branch: "gdr", From: "15.0", Fixed: "15.0.2135.5"}},
		}},
		SourceCVEIDs: sourceCVEIDs(content),
	}
	c.Assert(snapshot.Validate(), qt.IsNil)
	return snapshot
}

// guideArticles records the three engine articles guideRows names, all
// released in November 2026.
var guideArticles = []kbArticle{
	{KB: "5000001", Month: "2026-Nov", Line: "16.0", Branch: "gdr", Build: "16.0.1140.6"},
	{KB: "5000002", Month: "2026-Nov", Line: "16.0", Branch: "cu", Build: "16.0.4200.1"},
	{KB: "5000004", Month: "2026-Nov", Line: "15.0", Branch: "gdr", Build: "15.0.2135.5"},
}

// guideAnswers answers every row guideRows decides by absence: no article
// lists an advisory the guide does not name it for.
var guideAnswers = []kbRow{
	{CVE: "CVE-2026-40001", KB: "5000004"},
	{CVE: "CVE-2026-40002", KB: "5000001"},
	{CVE: "CVE-2026-40002", KB: "5000002"},
}

// guideGaps answers the gaps guideRows leaves on 15.0's CU branch, which no
// November article updates: CVE-2026-40002, which the guide names on 15.0's
// GDR branch, and CVE-2026-40001, which it names on 16.0 alone.
var guideGaps = []kbGap{
	{CVE: "CVE-2026-40002", Line: "15.0", Branch: "cu", Since: &kbFirstBuild{KB: "5000005", Build: "15.0.4440.1", Released: "2026-11-20"}},
	{CVE: "CVE-2026-40001", Line: "15.0", Branch: "cu", Unlisted: true},
}

// writeKBEvidence writes the reviewed KB record where the check reads it.
func writeKBEvidence(c *qt.C, articles []kbArticle, rows []kbRow) {
	c.Helper()
	c.Assert(os.MkdirAll(filepath.Dir(kbEvidencePath), 0o755), qt.IsNil)
	data, err := json.Marshal(kbEvidence{Articles: articles, Rows: rows, Gaps: guideGaps})
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(kbEvidencePath, data, 0o600), qt.IsNil)
}

// TestCheckVersioned_UnchangedGuideMovesValidUntil reads the guide after
// Microsoft's December release added nothing for SQL Server: the reviewed
// rows are unchanged, so the check refreshes the snapshot and records the
// January release, although the snapshot was verified a day before.
func TestCheckVersioned_UnchangedGuideMovesValidUntil(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	writeKBEvidence(c, guideArticles, guideAnswers)
	now := time.Date(2026, 12, 9, 7, 23, 0, 0, time.UTC)
	current := guideSnapshot(c, guideRows("nonce-1"), now.Add(-24*time.Hour), time.Date(2026, 12, 8, 0, 0, 0, 0, time.UTC))
	pages := guidePages(c, guideRows("nonce-2"))
	pages[cvrfIndexURL] = cvrfIndex(c, "2026-Nov", "2026-Dec")

	c.Assert(checkVersioned(current, servedPages(pages), now), qt.IsNil)

	data, err := os.ReadFile(filepath.Join("internal", "vulnerability", "sqlserver.json"))
	c.Assert(err, qt.IsNil)
	refreshed, err := vulnerability.DecodeVersionedSnapshot(data)
	c.Assert(err, qt.IsNil)
	c.Assert(refreshed.ValidUntil, qt.Equals, time.Date(2027, 1, 12, 0, 0, 0, 0, time.UTC))
	c.Assert(refreshed.RetrievedAt, qt.Equals, now)
	c.Assert(refreshed.AdvisoryContentSHA256, qt.Equals, current.AdvisoryContentSHA256)
	c.Assert(refreshed.SourceSHA256, qt.Equals, digestOf(slices.Concat(pages[guideURL], pages[guideNextURL])))
	_, err = os.Stat(reviewFile("sqlserver"))
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
}

// TestCheckVersioned_NewGuideAdvisoryIsReviewed adds an engine row for a new
// advisory: the check leaves a review naming it and writes no snapshot.
func TestCheckVersioned_NewGuideAdvisoryIsReviewed(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	writeKBEvidence(c, guideArticles, guideAnswers)
	now := time.Date(2026, 12, 9, 7, 23, 0, 0, time.UTC)
	current := guideSnapshot(c, guideRows("nonce-1"), now.Add(-24*time.Hour), time.Date(2026, 12, 8, 0, 0, 0, 0, time.UTC))
	rows := append(guideRows("nonce-2"), map[string]any{"id": "row-5", "cveNumber": "CVE-2026-49999",
		"product": "Microsoft SQL Server 2025 for x64-based Systems (GDR)", "baseScore": "9.8",
		"kbArticles": []map[string]any{{"articleName": "5000005", "fixedBuildNumber": "17.0.1160.1"}}})
	pages := guidePages(c, rows)
	pages[cvrfIndexURL] = cvrfIndex(c, "2026-Nov", "2026-Dec")

	c.Assert(checkVersioned(current, servedPages(pages), now), qt.IsNil)

	data, err := os.ReadFile(reviewFile("sqlserver"))
	c.Assert(err, qt.IsNil)
	var pending review
	c.Assert(json.Unmarshal(data, &pending), qt.IsNil)
	c.Assert(pending.Source, qt.Equals, guideURL)
	c.Assert(pending.AddedIDs, qt.DeepEquals, []string{"CVE-2026-49999"})
	c.Assert(pending.RemovedIDs, qt.HasLen, 0)
	_, err = os.Stat(filepath.Join("internal", "vulnerability", "sqlserver.json"))
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
}

// TestCheckVersioned_UnpublishedReleaseKeepsValidUntil refuses to move
// valid_until past a release the index does not list yet.
func TestCheckVersioned_UnpublishedReleaseKeepsValidUntil(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	now := time.Date(2026, 12, 9, 7, 23, 0, 0, time.UTC)
	current := guideSnapshot(c, guideRows("nonce-1"), now.Add(-24*time.Hour), time.Date(2026, 12, 8, 0, 0, 0, 0, time.UTC))
	pages := guidePages(c, guideRows("nonce-2"))
	pages[cvrfIndexURL] = cvrfIndex(c, "2026-Nov")

	err := checkVersioned(current, servedPages(pages), now)

	c.Assert(err, qt.ErrorMatches, `the security release index lists no 2026-Dec release, so the release due on 2026-12-08 is unread`)
}

// TestCheckVersioned_GuideOnlySnapshotIsRefused reads an unchanged guide
// against a record whose November GDR article for 2022 lists CVE-2026-40002,
// which the guide names for 2019 only. The snapshot pins only the guide's
// build, so the check refuses it instead of refreshing it.
func TestCheckVersioned_GuideOnlySnapshotIsRefused(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	answers := slices.Clone(guideAnswers)
	answers[1].Lists = true
	writeKBEvidence(c, guideArticles, answers)
	now := time.Date(2026, 12, 9, 7, 23, 0, 0, time.UTC)
	current := guideSnapshot(c, guideRows("nonce-1"), now.Add(-24*time.Hour), time.Date(2026, 12, 8, 0, 0, 0, 0, time.UTC))
	pages := guidePages(c, guideRows("nonce-2"))
	pages[cvrfIndexURL] = cvrfIndex(c, "2026-Nov", "2026-Dec")

	err := checkVersioned(current, servedPages(pages), now)

	c.Assert(err, qt.ErrorMatches, `CVE-2026-40002 carries the fixed builds 15.0 gdr 15.0.2135.5, and the guide with the reviewed KB articles gives 15.0 gdr 15.0.2135.5, 16.0 gdr 16.0.1140.6`)
	_, err = os.Stat(filepath.Join("internal", "vulnerability", "sqlserver.json"))
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
}

// TestCheckVersioned_SnapshotRangeTheGuideDoesNotGiveIsRefused reads an
// unchanged guide against a snapshot that sets a fixed build too high or pins
// a range nothing names: either reads a server wrong.
func TestCheckVersioned_SnapshotRangeTheGuideDoesNotGiveIsRefused(t *testing.T) {
	for _, test := range []struct {
		name   string
		ranges []vulnerability.VersionRange
		want   string
	}{
		{name: "fixed build too high", ranges: []vulnerability.VersionRange{{Line: "15.0", Branch: "gdr", From: "15.0", Fixed: "15.0.2135.6"}},
			want: `CVE-2026-40002 carries the fixed builds 15.0 gdr 15.0.2135.6, and the guide with the reviewed KB articles gives 15.0 gdr 15.0.2135.5`},
		{name: "range nothing names", ranges: []vulnerability.VersionRange{
			{Line: "15.0", Branch: "gdr", From: "15.0", Fixed: "15.0.2135.5"}, {Line: "16.0", Branch: "gdr", From: "16.0", Fixed: "16.0.1140.6"},
		}, want: `CVE-2026-40002 carries the fixed builds 15.0 gdr 15.0.2135.5, 16.0 gdr 16.0.1140.6, and the guide with the reviewed KB articles gives 15.0 gdr 15.0.2135.5`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Chdir(t.TempDir())
			writeKBEvidence(c, guideArticles, guideAnswers)
			now := time.Date(2026, 12, 9, 7, 23, 0, 0, time.UTC)
			current := guideSnapshot(c, guideRows("nonce-1"), now.Add(-24*time.Hour), time.Date(2026, 12, 8, 0, 0, 0, 0, time.UTC))
			current.Advisories[1].Ranges = test.ranges
			pages := guidePages(c, guideRows("nonce-2"))
			pages[cvrfIndexURL] = cvrfIndex(c, "2026-Nov", "2026-Dec")

			err := checkVersioned(current, servedPages(pages), now)

			c.Assert(err, qt.ErrorMatches, test.want)
			_, err = os.Stat(filepath.Join("internal", "vulnerability", "sqlserver.json"))
			c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
		})
	}
}

// TestCheckVersioned_UnusedAnswerIsRefused reads an unchanged guide against a
// record that also answers a row no question reads: an article the guide
// names for the advisory. A stale answer would hide that the guide moved.
func TestCheckVersioned_UnusedAnswerIsRefused(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	writeKBEvidence(c, guideArticles, append(slices.Clone(guideAnswers), kbRow{CVE: "CVE-2026-40001", KB: "5000001", Lists: true}))
	now := time.Date(2026, 12, 9, 7, 23, 0, 0, time.UTC)
	current := guideSnapshot(c, guideRows("nonce-1"), now.Add(-24*time.Hour), time.Date(2026, 12, 8, 0, 0, 0, 0, time.UTC))
	pages := guidePages(c, guideRows("nonce-2"))
	pages[cvrfIndexURL] = cvrfIndex(c, "2026-Nov", "2026-Dec")

	err := checkVersioned(current, servedPages(pages), now)

	c.Assert(err, qt.ErrorMatches, `internal/vulnerability/sqlserver-kb.json answers what the guide no longer asks: row CVE-2026-40001 in KB5000001`)
}

// TestCheckVersioned_UnansweredRowIsRefused reads an unchanged guide against
// a record that answers one row too few.
func TestCheckVersioned_UnansweredRowIsRefused(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	writeKBEvidence(c, guideArticles, guideAnswers[:2])
	now := time.Date(2026, 12, 9, 7, 23, 0, 0, time.UTC)
	current := guideSnapshot(c, guideRows("nonce-1"), now.Add(-24*time.Hour), time.Date(2026, 12, 8, 0, 0, 0, 0, time.UTC))
	pages := guidePages(c, guideRows("nonce-2"))
	pages[cvrfIndexURL] = cvrfIndex(c, "2026-Nov", "2026-Dec")

	err := checkVersioned(current, servedPages(pages), now)

	c.Assert(err, qt.ErrorMatches, `the guide decides CVE-2026-40002 on 16.0 cu by absence, and internal/vulnerability/sqlserver-kb.json does not answer it from https://support.microsoft.com/help/5000002; unanswered rows: 1`)
}

// TestCheckVersioned_ReviewListsWhatTheRecordDoesNotCover adds two engine
// rows: one names a recorded article, so the other November articles become
// rows to answer, and one names an article the record does not hold.
func TestCheckVersioned_ReviewListsWhatTheRecordDoesNotCover(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	writeKBEvidence(c, guideArticles, guideAnswers)
	now := time.Date(2026, 12, 9, 7, 23, 0, 0, time.UTC)
	current := guideSnapshot(c, guideRows("nonce-1"), now.Add(-24*time.Hour), time.Date(2026, 12, 8, 0, 0, 0, 0, time.UTC))
	rows := append(guideRows("nonce-2"),
		map[string]any{"id": "row-6", "cveNumber": "CVE-2026-49998", "product": "Microsoft SQL Server 2022 for x64-based Systems (GDR)",
			"releaseNumber": "2026-Nov", "kbArticles": []map[string]any{{"articleName": "5000001", "fixedBuildNumber": "16.0.1140.6"}}},
		map[string]any{"id": "row-7", "cveNumber": "CVE-2026-49997", "product": "Microsoft SQL Server 2022 for x64-based Systems (CU 20)",
			"releaseNumber": "2026-Nov", "kbArticles": []map[string]any{{"articleName": "5000007", "fixedBuildNumber": "16.0.4210.2"}}})
	pages := guidePages(c, rows)
	pages[cvrfIndexURL] = cvrfIndex(c, "2026-Nov", "2026-Dec")

	c.Assert(checkVersioned(current, servedPages(pages), now), qt.IsNil)

	data, err := os.ReadFile(reviewFile("sqlserver"))
	c.Assert(err, qt.IsNil)
	var pending review
	c.Assert(json.Unmarshal(data, &pending), qt.IsNil)
	c.Assert(pending.AddedIDs, qt.DeepEquals, []string{"CVE-2026-49997", "CVE-2026-49998"})
	c.Assert(pending.UnrecordedKBArticles, qt.DeepEquals, []string{"https://support.microsoft.com/help/5000007"})
	c.Assert(pending.UnansweredKBRows, qt.DeepEquals, []kbQuestion{
		{CVE: "CVE-2026-49998", Line: "15.0", Branch: "cu", Months: []string{"2026-Nov"}, URL: "https://msrc.microsoft.com/update-guide/vulnerability/CVE-2026-49998"},
		{CVE: "CVE-2026-49998", Line: "15.0", Branch: "gdr", Months: []string{"2026-Nov"}, KB: "5000004", URL: "https://support.microsoft.com/help/5000004"},
		{CVE: "CVE-2026-49998", Line: "16.0", Branch: "cu", Months: []string{"2026-Nov"}, KB: "5000002", URL: "https://support.microsoft.com/help/5000002"},
	})
}

// TestCheckVersioned_UnrecordedArticleIsRefused reads an unchanged guide
// against a record that holds no entry for the 2019 article the guide names.
func TestCheckVersioned_UnrecordedArticleIsRefused(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	writeKBEvidence(c, guideArticles[:2], guideAnswers[1:])
	now := time.Date(2026, 12, 9, 7, 23, 0, 0, time.UTC)
	current := guideSnapshot(c, guideRows("nonce-1"), now.Add(-24*time.Hour), time.Date(2026, 12, 8, 0, 0, 0, 0, time.UTC))
	pages := guidePages(c, guideRows("nonce-2"))
	pages[cvrfIndexURL] = cvrfIndex(c, "2026-Nov", "2026-Dec")

	err := checkVersioned(current, servedPages(pages), now)

	c.Assert(err, qt.ErrorMatches, `the guide names KB articles internal/vulnerability/sqlserver-kb.json has no record of, or one the guide disagrees with: KB5000004`)
}

// TestCheckVersioned_UnpinnedAdvisoryIsRefused reads an unchanged guide
// against a snapshot that leaves out an advisory the guide names a fix for on
// a covered line.
func TestCheckVersioned_UnpinnedAdvisoryIsRefused(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	writeKBEvidence(c, guideArticles, guideAnswers)
	now := time.Date(2026, 12, 9, 7, 23, 0, 0, time.UTC)
	current := guideSnapshot(c, guideRows("nonce-1"), now.Add(-24*time.Hour), time.Date(2026, 12, 8, 0, 0, 0, 0, time.UTC))
	current.Advisories = current.Advisories[:1]
	pages := guidePages(c, guideRows("nonce-2"))
	pages[cvrfIndexURL] = cvrfIndex(c, "2026-Nov", "2026-Dec")

	err := checkVersioned(current, servedPages(pages), now)

	c.Assert(err, qt.ErrorMatches, `CVE-2026-40002 has fixed builds on a covered line, 15.0 gdr 15.0.2135.5, and the snapshot does not pin it`)
}
