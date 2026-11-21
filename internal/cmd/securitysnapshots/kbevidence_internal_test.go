package main

// White-box testing required: the KB record and the guide assessment are
// private to this command and have no exported API.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/vulnerability"
)

// kbGuide is guide content: an engine entry per advisory and product.
func kbGuide(c *qt.C, entries ...guideEntry) []byte {
	c.Helper()
	data, err := json.Marshal(entries)
	c.Assert(err, qt.IsNil)
	return data
}

func gdr2022(cve string, fixes ...guideFix) guideEntry {
	return guideEntry{CVE: cve, Product: "Microsoft SQL Server 2022 for x64-based Systems (GDR)", Fixes: fixes}
}

func cu2022(cve string, fixes ...guideFix) guideEntry {
	return guideEntry{CVE: cve, Product: "Microsoft SQL Server 2022 for x64-based Systems (CU 19)", Fixes: fixes}
}

// kbArticles are a September and a November article on each 2022 branch.
var kbArticles = []kbArticle{
	{KB: "5100001", Month: "2026-Sep", Line: "16.0", Branch: "gdr", Build: "16.0.1200.5"},
	{KB: "5100002", Month: "2026-Sep", Line: "16.0", Branch: "cu", Build: "16.0.4275.2"},
	{KB: "5100003", Month: "2026-Nov", Line: "16.0", Branch: "gdr", Build: "16.0.1220.3"},
	{KB: "5100004", Month: "2026-Nov", Line: "16.0", Branch: "cu", Build: "16.0.4315.2"},
}

func TestAssessGuide_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name    string
		entries []guideEntry
		rows    []kbRow
		gaps    []kbGap
		without string
		cve     string
		want    []vulnerability.VersionRange
	}{
		{
			name:    "article that lists the advisory adds its build",
			entries: []guideEntry{gdr2022("CVE-2026-50001", guideFix{KB: "5100003", Build: "16.0.1220.3"}), cu2022("CVE-2026-50002", guideFix{KB: "5100004", Build: "16.0.4315.2"})},
			rows:    []kbRow{{CVE: "CVE-2026-50001", KB: "5100004", Lists: true}, {CVE: "CVE-2026-50002", KB: "5100003"}},
			cve:     "CVE-2026-50001",
			want: []vulnerability.VersionRange{
				{Line: "16.0", Branch: "cu", From: "16.0", Fixed: "16.0.4315.2"},
				{Line: "16.0", Branch: "gdr", From: "16.0", Fixed: "16.0.1220.3"},
			},
		},
		{
			name:    "article that does not list the advisory adds nothing",
			entries: []guideEntry{gdr2022("CVE-2026-50001", guideFix{KB: "5100003", Build: "16.0.1220.3"}), cu2022("CVE-2026-50002", guideFix{KB: "5100004", Build: "16.0.4315.2"})},
			rows:    []kbRow{{CVE: "CVE-2026-50001", KB: "5100004"}, {CVE: "CVE-2026-50002", KB: "5100003"}},
			cve:     "CVE-2026-50001",
			want:    []vulnerability.VersionRange{{Line: "16.0", Branch: "gdr", From: "16.0", Fixed: "16.0.1220.3"}},
		},
		{
			// The guide names September builds on both branches and a second
			// CU build in November; November's GDR article lists the advisory
			// too, so a GDR server between the two builds reads uncertain.
			name: "second build on the other branch",
			entries: []guideEntry{
				gdr2022("CVE-2026-50003", guideFix{KB: "5100001", Build: "16.0.1200.5"}),
				cu2022("CVE-2026-50003", guideFix{KB: "5100002", Build: "16.0.4275.2"}, guideFix{KB: "5100004", Build: "16.0.4315.2"}),
			},
			rows: []kbRow{{CVE: "CVE-2026-50003", KB: "5100003", Lists: true}},
			cve:  "CVE-2026-50003",
			want: []vulnerability.VersionRange{
				{Line: "16.0", Branch: "cu", From: "16.0", Fixed: "16.0.4275.2"},
				{Line: "16.0", Branch: "cu", From: "16.0", Fixed: "16.0.4315.2"},
				{Line: "16.0", Branch: "gdr", From: "16.0", Fixed: "16.0.1200.5"},
				{Line: "16.0", Branch: "gdr", From: "16.0", Fixed: "16.0.1220.3"},
			},
		},
		{
			// A release line the snapshot does not cover, such as SQL Server
			// 2017's, needs no record, with a build or without one.
			name: "uncovered line",
			entries: []guideEntry{
				gdr2022("CVE-2026-50004", guideFix{KB: "5100001", Build: "16.0.1200.5"}),
				{CVE: "CVE-2026-50004", Product: "Microsoft SQL Server 2017 for x64-based Systems (GDR)", Fixes: []guideFix{{KB: "5100099", Build: "14.0.2080.1"}}},
				{CVE: "CVE-2026-50004", Product: "Microsoft SQL Server 2017 for x64-based Systems (CU 31)", Fixes: []guideFix{{KB: "5100098"}}},
			},
			rows: []kbRow{{CVE: "CVE-2026-50004", KB: "5100002"}},
			cve:  "CVE-2026-50004",
			want: []vulnerability.VersionRange{{Line: "16.0", Branch: "gdr", From: "16.0", Fixed: "16.0.1200.5"}},
		},
		{
			// The guide names the GDR branch in September, and no recorded
			// article updates the CU branch that month: the record's gap
			// keeps the guide's answer.
			name:    "gap the record answers",
			entries: []guideEntry{gdr2022("CVE-2026-50006", guideFix{KB: "5100001", Build: "16.0.1200.5"})},
			gaps:    []kbGap{{CVE: "CVE-2026-50006", Line: "16.0", Branch: "cu"}},
			without: "5100002",
			cve:     "CVE-2026-50006",
			want:    []vulnerability.VersionRange{{Line: "16.0", Branch: "gdr", From: "16.0", Fixed: "16.0.1200.5"}},
		},
		{
			// CVE-2023-38169 is the shape: the guide names an article with a
			// build the page does not ship, and both are pinned.
			name:    "build the guide and the page disagree on",
			entries: []guideEntry{cu2022("CVE-2026-50008", guideFix{KB: "5100006", Build: "16.0.4283.1"})},
			gaps:    []kbGap{{CVE: "CVE-2026-50008", Line: "16.0", Branch: "gdr"}},
			cve:     "CVE-2026-50008",
			want: []vulnerability.VersionRange{
				{Line: "16.0", Branch: "cu", From: "16.0", Fixed: "16.0.4280.7"},
				{Line: "16.0", Branch: "cu", From: "16.0", Fixed: "16.0.4283.1"},
			},
		},
		{
			// CVE-2021-1636 is the shape: the guide names the article and no
			// build, and the record gives the build.
			name:    "build the guide leaves out",
			entries: []guideEntry{gdr2022("CVE-2026-50005", guideFix{KB: "5100001"}), cu2022("CVE-2026-50005", guideFix{KB: "5100002"})},
			cve:     "CVE-2026-50005",
			want: []vulnerability.VersionRange{
				{Line: "16.0", Branch: "cu", From: "16.0", Fixed: "16.0.4275.2"},
				{Line: "16.0", Branch: "gdr", From: "16.0", Fixed: "16.0.1200.5"},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			articles := slices.DeleteFunc(append(slices.Clone(kbArticles),
				kbArticle{KB: "5100006", Month: "2026-Aug", Line: "16.0", Branch: "cu", Build: "16.0.4280.7", GuideBuild: "16.0.4283.1"}),
				func(article kbArticle) bool { return article.KB == test.without })

			got, err := assessGuide(kbGuide(c, test.entries...), []string{"16.0"}, kbEvidence{Articles: articles, Rows: test.rows, Gaps: test.gaps})

			c.Assert(err, qt.IsNil)
			c.Assert(got.unrecorded, qt.HasLen, 0)
			c.Assert(got.unanswered, qt.HasLen, 0)
			c.Assert(got.ranges[test.cve], qt.DeepEquals, test.want)
		})
	}
}

// TestAssessGuide_AsksOnlyAboutTheAdvisorysMonths leaves the September
// articles out of a November advisory's questions.
func TestAssessGuide_AsksOnlyAboutTheAdvisorysMonths(t *testing.T) {
	c := qt.New(t)

	got, err := assessGuide(kbGuide(c, gdr2022("CVE-2026-50001", guideFix{KB: "5100003", Build: "16.0.1220.3"})), []string{"16.0"},
		kbEvidence{Articles: kbArticles})

	c.Assert(err, qt.IsNil)
	c.Assert(got.unanswered, qt.DeepEquals, []kbQuestion{
		{CVE: "CVE-2026-50001", Line: "16.0", Branch: "cu", Months: []string{"2026-Nov"}, KB: "5100004", URL: "https://support.microsoft.com/help/5100004"},
	})
}

// TestAssessGuide_AsksAboutThePublishingMonth reads an advisory Microsoft
// published in September whose fix the guide names in November, as it does
// for CVE-2024-37341: the September articles may fix it too.
func TestAssessGuide_AsksAboutThePublishingMonth(t *testing.T) {
	c := qt.New(t)
	entry := gdr2022("CVE-2026-50007", guideFix{KB: "5100003", Build: "16.0.1220.3"})
	entry.Month = "2026-Sep"

	got, err := assessGuide(kbGuide(c, entry), []string{"16.0"}, kbEvidence{Articles: kbArticles})

	c.Assert(err, qt.IsNil)
	c.Assert(got.unanswered, qt.DeepEquals, []kbQuestion{
		{CVE: "CVE-2026-50007", Line: "16.0", Branch: "cu", Months: []string{"2026-Sep"}, KB: "5100002", URL: "https://support.microsoft.com/help/5100002"},
		{CVE: "CVE-2026-50007", Line: "16.0", Branch: "cu", Months: []string{"2026-Nov"}, KB: "5100004", URL: "https://support.microsoft.com/help/5100004"},
		{CVE: "CVE-2026-50007", Line: "16.0", Branch: "gdr", Months: []string{"2026-Sep"}, KB: "5100001", URL: "https://support.microsoft.com/help/5100001"},
	})
}

// TestAssessGuide_AsksAboutAGap names an advisory on the GDR branch in
// September, when no recorded article updates the CU branch: the question
// sends the reviewer to the line's build list.
func TestAssessGuide_AsksAboutAGap(t *testing.T) {
	c := qt.New(t)
	articles := slices.DeleteFunc(slices.Clone(kbArticles), func(article kbArticle) bool { return article.KB == "5100002" })

	got, err := assessGuide(kbGuide(c, gdr2022("CVE-2026-50006", guideFix{KB: "5100001", Build: "16.0.1200.5"})), []string{"16.0"},
		kbEvidence{Articles: articles})

	c.Assert(err, qt.IsNil)
	c.Assert(got.unanswered, qt.DeepEquals, []kbQuestion{{
		CVE: "CVE-2026-50006", Line: "16.0", Branch: "cu", Months: []string{"2026-Sep"},
		URL: "https://learn.microsoft.com/troubleshoot/sql/releases/sqlserver-2022/build-versions",
	}})
}

func TestAssessGuide_ListsUnrecordedArticles(t *testing.T) {
	for _, test := range []struct {
		name  string
		entry guideEntry
	}{
		{name: "article with no record", entry: gdr2022("CVE-2026-50001", guideFix{KB: "5100009", Build: "16.0.1230.1"})},
		{name: "build the record disagrees with", entry: gdr2022("CVE-2026-50001", guideFix{KB: "5100003", Build: "16.0.1221.1"})},
		{name: "branch the record disagrees with", entry: cu2022("CVE-2026-50001", guideFix{KB: "5100003", Build: "16.0.1220.3"})},
		{name: "article with no build and no record", entry: gdr2022("CVE-2026-50001", guideFix{KB: "5100009"})},
		{name: "line the record disagrees with", entry: guideEntry{CVE: "CVE-2026-50001",
			Product: "Microsoft SQL Server 2019 for x64-based Systems (GDR)", Fixes: []guideFix{{KB: "5100003"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := assessGuide(kbGuide(c, test.entry), []string{"15.0", "16.0"}, kbEvidence{Articles: kbArticles})

			c.Assert(err, qt.IsNil)
			c.Assert(got.unrecorded, qt.DeepEquals, []string{test.entry.Fixes[0].KB})
			c.Assert(got.ranges, qt.HasLen, 0)
		})
	}
}

func TestReadKBEvidence_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, data, want string
	}{
		{name: "unknown field", data: `{"articles": [], "rows": [], "note": "x"}`, want: `decode the reviewed KB articles: json: unknown field "note"`},
		{name: "month spelling", data: `{"articles": [{"kb": "1", "month": "2026-11", "line": "16.0", "branch": "gdr", "build": "16.0.1.1"}], "rows": []}`,
			want: `invalid or repeated reviewed KB article "1"`},
		{name: "guide build off its line", data: `{"articles": [{"kb": "1", "month": "2026-Nov", "line": "16.0", "branch": "gdr", "build": "16.0.1.1", "guide_build": "15.0.1.2"}], "rows": []}`,
			want: `invalid or repeated reviewed KB article "1"`},
		{name: "guide build equal to the page's", data: `{"articles": [{"kb": "1", "month": "2026-Nov", "line": "16.0", "branch": "gdr", "build": "16.0.1.1", "guide_build": "16.0.1.1"}], "rows": []}`,
			want: `invalid or repeated reviewed KB article "1"`},
		{name: "build off its line", data: `{"articles": [{"kb": "1", "month": "2026-Nov", "line": "15.0", "branch": "gdr", "build": "16.0.1.1"}], "rows": []}`,
			want: `invalid or repeated reviewed KB article "1"`},
		{name: "unknown branch", data: `{"articles": [{"kb": "1", "month": "2026-Nov", "line": "16.0", "branch": "gdrr", "build": "16.0.1.1"}], "rows": []}`,
			want: `invalid or repeated reviewed KB article "1"`},
		{name: "repeated article", data: `{"articles": [{"kb": "1", "month": "2026-Nov", "line": "16.0", "branch": "gdr", "build": "16.0.1.1"}, {"kb": "1", "month": "2026-Nov", "line": "16.0", "branch": "gdr", "build": "16.0.1.1"}], "rows": []}`,
			want: `invalid or repeated reviewed KB article "1"`},
		{name: "row naming no recorded article", data: `{"articles": [], "rows": [{"cve": "CVE-2026-50001", "kb": "2", "lists": true}]}`,
			want: `invalid or repeated reviewed KB row CVE-2026-50001 in KB2`},
		{name: "repeated row", data: `{"articles": [{"kb": "1", "month": "2026-Nov", "line": "16.0", "branch": "gdr", "build": "16.0.1.1"}], "rows": [{"cve": "CVE-2026-50001", "kb": "1", "lists": true}, {"cve": "CVE-2026-50001", "kb": "1", "lists": false}]}`,
			want: `invalid or repeated reviewed KB row CVE-2026-50001 in KB1`},
		{name: "gap on a branch the banner never names", data: `{"articles": [], "rows": [], "gaps": [{"cve": "CVE-2026-50001", "line": "16.0", "branch": "gdrr"}]}`,
			want: `invalid or repeated reviewed KB gap CVE-2026-50001 on 16.0 gdrr`},
		{name: "repeated gap", data: `{"articles": [], "rows": [], "gaps": [{"cve": "CVE-2026-50001", "line": "16.0", "branch": "cu"}, {"cve": "CVE-2026-50001", "line": "16.0", "branch": "cu"}]}`,
			want: `invalid or repeated reviewed KB gap CVE-2026-50001 on 16.0 cu`},
		{name: "trailing data", data: `{"articles": [], "rows": []} {}`, want: `the reviewed KB articles carry trailing data`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(c.TempDir(), "sqlserver-kb.json")
			c.Assert(os.WriteFile(path, []byte(test.data), 0o600), qt.IsNil)

			got, err := readKBEvidence(path)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got.Articles, qt.HasLen, 0)
		})
	}
}

// TestBundledSQLServerSnapshot_CarriesEveryReviewedKBFix reads the reviewed
// record beside the bundled snapshot: each article that lists an advisory the
// guide leaves out on its branch is a range the snapshot pins. A snapshot
// rebuilt from the guide alone fails here as well as in the daily check.
func TestBundledSQLServerSnapshot_CarriesEveryReviewedKBFix(t *testing.T) {
	c := qt.New(t)
	evidence, err := readKBEvidence(filepath.Join("..", "..", "vulnerability", "sqlserver-kb.json"))
	c.Assert(err, qt.IsNil)
	snapshot, err := vulnerability.ReadVersionedSnapshot("sqlserver", "")
	c.Assert(err, qt.IsNil)
	articles := make(map[string]kbArticle, len(evidence.Articles))
	for _, article := range evidence.Articles {
		articles[article.KB] = article
	}
	pinned := make(map[string][]vulnerability.VersionRange, len(snapshot.Advisories))
	for _, advisory := range snapshot.Advisories {
		pinned[advisory.ID] = advisory.Ranges
	}
	listed := slices.DeleteFunc(slices.Clone(evidence.Rows), func(row kbRow) bool { return !row.Lists })
	c.Assert(listed, qt.Not(qt.HasLen), 0, qt.Commentf("the record answers some row with a fix, or this test reads nothing"))
	for _, row := range listed {
		c.Assert(pinned[row.CVE], qt.Any(qt.Equals), rangeOf(articles[row.KB]), qt.Commentf("%s in KB%s", row.CVE, row.KB))
	}
}
