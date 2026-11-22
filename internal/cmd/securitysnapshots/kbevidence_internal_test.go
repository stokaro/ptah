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
	return guideEntry{CVE: cve, Product: "Microsoft SQL Server 2022 for x64-based Systems (GDR)", Month: monthOf(fixes), Fixes: fixes}
}

func cu2022(cve string, fixes ...guideFix) guideEntry {
	return guideEntry{CVE: cve, Product: "Microsoft SQL Server 2022 for x64-based Systems (CU 19)", Month: monthOf(fixes), Fixes: fixes}
}

// monthOf is the release month the guide gives an entry: its first fix's
// article's, or November 2026 for an article the fixtures do not record.
func monthOf(fixes []guideFix) string {
	for _, article := range append(slices.Clone(kbArticles), kbGuideBuildArticle) {
		if article.KB == fixes[0].KB {
			return article.Month
		}
	}
	return "2026-Nov"
}

// kbArticles are a September and a November article on each 2022 branch.
var kbArticles = []kbArticle{
	{KB: "5100001", Month: "2026-Sep", Line: "16.0", Branch: "gdr", Build: "16.0.1200.5"},
	{KB: "5100002", Month: "2026-Sep", Line: "16.0", Branch: "cu", Build: "16.0.4275.2"},
	{KB: "5100003", Month: "2026-Nov", Line: "16.0", Branch: "gdr", Build: "16.0.1220.3"},
	{KB: "5100004", Month: "2026-Nov", Line: "16.0", Branch: "cu", Build: "16.0.4315.2"},
}

// kbGuideBuildArticle is an August CU article whose page ships another build
// than the guide names.
var kbGuideBuildArticle = kbArticle{KB: "5100006", Month: "2026-Aug", Line: "16.0", Branch: "cu", Build: "16.0.4280.7", GuideBuild: "16.0.4283.1"}

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
			// The guide names the GDR branch in September, and the CU
			// branch's first build shipped after that month's release: the
			// record's gap keeps the guide's answer.
			name:    "branch whose first build came after the fix",
			entries: []guideEntry{gdr2022("CVE-2026-50006", guideFix{KB: "5100001", Build: "16.0.1200.5"})},
			gaps:    []kbGap{{CVE: "CVE-2026-50006", Line: "16.0", Branch: "cu", Since: &kbFirstBuild{KB: "5100010", Build: "16.0.4250.1", Released: "2026-09-20"}}},
			without: "5100002",
			cve:     "CVE-2026-50006",
			want:    []vulnerability.VersionRange{{Line: "16.0", Branch: "gdr", From: "16.0", Fixed: "16.0.1200.5"}},
		},
		{
			// CVE-2023-38169 is the shape: the guide names an article with a
			// build the page does not ship, and both are pinned.
			name:    "build the guide and the page disagree on",
			entries: []guideEntry{cu2022("CVE-2026-50008", guideFix{KB: "5100006", Build: "16.0.4283.1"})},
			gaps:    []kbGap{{CVE: "CVE-2026-50008", Line: "16.0", Branch: "gdr", Since: &kbFirstBuild{KB: "5100011", Build: "16.0.1205.1", Released: "2026-08-20"}}},
			cve:     "CVE-2026-50008",
			want: []vulnerability.VersionRange{
				{Line: "16.0", Branch: "cu", From: "16.0", Fixed: "16.0.4280.7"},
				{Line: "16.0", Branch: "cu", From: "16.0", Fixed: "16.0.4283.1"},
			},
		},
		{
			// CVE-2023-38169 is the shape: Microsoft fixes the advisory on
			// the CU branch only and states that GDR servers are affected, so
			// the GDR branch is pinned with the CU fixes it reaches by moving.
			name:    "branch fixed only on the other one",
			entries: []guideEntry{cu2022("CVE-2026-50009", guideFix{KB: "5100006", Build: "16.0.4283.1"})},
			gaps:    []kbGap{{CVE: "CVE-2026-50009", Line: "16.0", Branch: "gdr", Vulnerable: true}},
			cve:     "CVE-2026-50009",
			want: []vulnerability.VersionRange{
				{Line: "16.0", Branch: "cu", From: "16.0", Fixed: "16.0.4280.7"},
				{Line: "16.0", Branch: "cu", From: "16.0", Fixed: "16.0.4283.1"},
				{Line: "16.0", Branch: "gdr", From: "16.0", Fixed: "16.0.4280.7"},
				{Line: "16.0", Branch: "gdr", From: "16.0", Fixed: "16.0.4283.1"},
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

			articles := slices.DeleteFunc(append(slices.Clone(kbArticles), kbGuideBuildArticle),
				func(article kbArticle) bool { return article.KB == test.without })

			got, err := assessGuide(kbGuide(c, test.entries...), []string{"16.0"}, kbEvidence{Articles: articles, Rows: test.rows, Gaps: test.gaps})

			c.Assert(err, qt.IsNil)
			c.Assert(got.unrecorded, qt.HasLen, 0)
			c.Assert(got.unanswered, qt.HasLen, 0)
			c.Assert(got.unused, qt.HasLen, 0)
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

// TestAssessGuide_AsksAboutALineTheGuideDoesNotName names an advisory on
// 2022 only, while the snapshot also covers 2019, which was released long
// before: each 2019 branch is a question, and the advisory's page answers it.
func TestAssessGuide_AsksAboutALineTheGuideDoesNotName(t *testing.T) {
	c := qt.New(t)
	entries := []guideEntry{
		gdr2022("CVE-2026-50001", guideFix{KB: "5100003", Build: "16.0.1220.3"}),
		cu2022("CVE-2026-50001", guideFix{KB: "5100004", Build: "16.0.4315.2"}),
	}

	got, err := assessGuide(kbGuide(c, entries...), []string{"15.0", "16.0"}, kbEvidence{Articles: kbArticles})

	c.Assert(err, qt.IsNil)
	c.Assert(got.unanswered, qt.DeepEquals, []kbQuestion{
		{CVE: "CVE-2026-50001", Line: "15.0", Branch: "cu", Months: []string{"2026-Nov"}, URL: "https://msrc.microsoft.com/update-guide/vulnerability/CVE-2026-50001"},
		{CVE: "CVE-2026-50001", Line: "15.0", Branch: "gdr", Months: []string{"2026-Nov"}, URL: "https://msrc.microsoft.com/update-guide/vulnerability/CVE-2026-50001"},
	})
}

// TestAssessGuide_AsksAboutALineReleasedBeforeTheAdvisory reads an advisory
// the guide names on 2022 only against a snapshot that also covers 2025,
// released on 2025-11-18. October's and November's releases came before it,
// so 2025 carries their fixes and is not asked about; December's came after.
func TestAssessGuide_AsksAboutALineReleasedBeforeTheAdvisory(t *testing.T) {
	for _, test := range []struct {
		name, month string
		want        []kbQuestion
	}{
		{name: "October release", month: "2025-Oct"},
		{name: "November release, a week before 2025", month: "2025-Nov"},
		{name: "December release", month: "2025-Dec", want: []kbQuestion{
			{CVE: "CVE-2025-50001", Line: "17.0", Branch: "cu", Months: []string{"2025-Dec"}, URL: "https://msrc.microsoft.com/update-guide/vulnerability/CVE-2025-50001"},
			{CVE: "CVE-2025-50001", Line: "17.0", Branch: "gdr", Months: []string{"2025-Dec"}, URL: "https://msrc.microsoft.com/update-guide/vulnerability/CVE-2025-50001"},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			articles := []kbArticle{
				{KB: "5090001", Month: test.month, Line: "16.0", Branch: "gdr", Build: "16.0.1155.1"},
				{KB: "5090002", Month: test.month, Line: "16.0", Branch: "cu", Build: "16.0.4215.1"},
			}
			gdr := gdr2022("CVE-2025-50001", guideFix{KB: "5090001", Build: "16.0.1155.1"})
			cu := cu2022("CVE-2025-50001", guideFix{KB: "5090002", Build: "16.0.4215.1"})
			gdr.Month, cu.Month = test.month, test.month

			got, err := assessGuide(kbGuide(c, gdr, cu), []string{"16.0", "17.0"}, kbEvidence{Articles: articles})

			c.Assert(err, qt.IsNil)
			c.Assert(got.unanswered, qt.DeepEquals, test.want)
		})
	}
}

// TestAssessGuide_FirstBuildAfterAFixPublishedLater reads the shape of
// CVE-2023-23384: Microsoft published it in November for a fix the GDR
// branch shipped in September, and the CU branch's first build came between
// the two. It carries the fix, so the gap stands although the advisory's
// publishing month is later than the build.
func TestAssessGuide_FirstBuildAfterAFixPublishedLater(t *testing.T) {
	c := qt.New(t)
	entry := gdr2022("CVE-2026-50010", guideFix{KB: "5100001", Build: "16.0.1200.5"})
	entry.Month = "2026-Nov"
	articles := []kbArticle{kbArticles[0], kbArticles[2]}
	evidence := kbEvidence{
		Articles: articles,
		Rows:     []kbRow{{CVE: "CVE-2026-50010", KB: "5100003"}},
		Gaps:     []kbGap{{CVE: "CVE-2026-50010", Line: "16.0", Branch: "cu", Since: &kbFirstBuild{KB: "5100010", Build: "16.0.4250.1", Released: "2026-09-20"}}},
	}

	got, err := assessGuide(kbGuide(c, entry), []string{"16.0"}, evidence)

	c.Assert(err, qt.IsNil)
	c.Assert(got.unanswered, qt.HasLen, 0)
	c.Assert(got.unused, qt.HasLen, 0)
	c.Assert(got.ranges["CVE-2026-50010"], qt.DeepEquals, []vulnerability.VersionRange{{Line: "16.0", Branch: "gdr", From: "16.0", Fixed: "16.0.1200.5"}})
}

// TestAssessGuide_FirstBuildBeforeAListedFixIsRefused reads an advisory
// the guide names on the GDR branch in September and publishes in November,
// whose November GDR article lists it too. A CU branch whose first build
// shipped between the two cannot be said to carry the November fix.
func TestAssessGuide_FirstBuildBeforeAListedFixIsRefused(t *testing.T) {
	c := qt.New(t)
	entry := gdr2022("CVE-2026-50010", guideFix{KB: "5100001", Build: "16.0.1200.5"})
	entry.Month = "2026-Nov"
	evidence := kbEvidence{
		Articles: []kbArticle{kbArticles[0], kbArticles[2]},
		Rows:     []kbRow{{CVE: "CVE-2026-50010", KB: "5100003", Lists: true}},
		Gaps:     []kbGap{{CVE: "CVE-2026-50010", Line: "16.0", Branch: "cu", Since: &kbFirstBuild{KB: "5100010", Build: "16.0.4250.1", Released: "2026-09-20"}}},
	}

	got, err := assessGuide(kbGuide(c, entry), []string{"16.0"}, evidence)

	c.Assert(err, qt.ErrorMatches, `the reviewed KB gap CVE-2026-50010 on 16.0 cu says the branch's first build, 16.0.4250.1 of 2026-09-20, carries the fix, and it shipped no later than the 2026-Nov release that fixes the gdr branch`)
	c.Assert(got.ranges, qt.IsNil)
}

// TestAssessGuide_UnlistedLineKeepsTheGuidesAnswer answers the 2019
// questions of an advisory the guide names on 2022 only: nothing is pinned
// for 2019, which reads unaffected on a covered line.
func TestAssessGuide_UnlistedLineKeepsTheGuidesAnswer(t *testing.T) {
	c := qt.New(t)
	entries := []guideEntry{
		gdr2022("CVE-2026-50001", guideFix{KB: "5100003", Build: "16.0.1220.3"}),
		cu2022("CVE-2026-50001", guideFix{KB: "5100004", Build: "16.0.4315.2"}),
	}
	gaps := []kbGap{
		{CVE: "CVE-2026-50001", Line: "15.0", Branch: "cu", Unlisted: true},
		{CVE: "CVE-2026-50001", Line: "15.0", Branch: "gdr", Unlisted: true},
	}

	got, err := assessGuide(kbGuide(c, entries...), []string{"15.0", "16.0"}, kbEvidence{Articles: kbArticles, Gaps: gaps})

	c.Assert(err, qt.IsNil)
	c.Assert(got.unanswered, qt.HasLen, 0)
	c.Assert(got.unused, qt.HasLen, 0)
	c.Assert(got.ranges["CVE-2026-50001"], qt.DeepEquals, []vulnerability.VersionRange{
		{Line: "16.0", Branch: "cu", From: "16.0", Fixed: "16.0.4315.2"},
		{Line: "16.0", Branch: "gdr", From: "16.0", Fixed: "16.0.1220.3"},
	})
}

// TestAssessGuide_ListsUnusedAnswers records a row and a gap that no
// question reads: a September article for a November advisory, and a gap on
// a branch the guide names.
func TestAssessGuide_ListsUnusedAnswers(t *testing.T) {
	c := qt.New(t)
	entries := []guideEntry{
		gdr2022("CVE-2026-50001", guideFix{KB: "5100003", Build: "16.0.1220.3"}),
		cu2022("CVE-2026-50001", guideFix{KB: "5100004", Build: "16.0.4315.2"}),
	}
	evidence := kbEvidence{
		Articles: kbArticles,
		Rows:     []kbRow{{CVE: "CVE-2026-50001", KB: "5100001"}},
		Gaps:     []kbGap{{CVE: "CVE-2026-50001", Line: "16.0", Branch: "cu", Unlisted: true}},
	}

	got, err := assessGuide(kbGuide(c, entries...), []string{"16.0"}, evidence)

	c.Assert(err, qt.IsNil)
	c.Assert(got.unused, qt.DeepEquals, []string{"row CVE-2026-50001 in KB5100001", "gap CVE-2026-50001 on 16.0 cu"})
}

func TestAssessGuide_FailurePath(t *testing.T) {
	september := gdr2022("CVE-2026-50006", guideFix{KB: "5100001", Build: "16.0.1200.5"})
	november := []guideEntry{
		gdr2022("CVE-2026-50001", guideFix{KB: "5100003", Build: "16.0.1220.3"}),
		cu2022("CVE-2026-50001", guideFix{KB: "5100004", Build: "16.0.4315.2"}),
	}
	monthless := september
	monthless.Month = "September 2026"
	for _, test := range []struct {
		name    string
		entries []guideEntry
		covered []string
		gaps    []kbGap
		want    string
	}{
		{
			name: "first build shipped on the day of the fix's release", entries: []guideEntry{september}, covered: []string{"16.0"},
			gaps: []kbGap{{CVE: "CVE-2026-50006", Line: "16.0", Branch: "cu", Since: &kbFirstBuild{KB: "5100010", Build: "16.0.4250.1", Released: "2026-09-08"}}},
			want: `the reviewed KB gap CVE-2026-50006 on 16.0 cu says the branch's first build, 16.0.4250.1 of 2026-09-08, carries the fix, and it shipped no later than the 2026-Sep release that fixes the gdr branch`,
		},
		{
			name: "unlisted gap on a line the guide names", entries: []guideEntry{september}, covered: []string{"16.0"},
			gaps: []kbGap{{CVE: "CVE-2026-50006", Line: "16.0", Branch: "cu", Unlisted: true}},
			want: `the reviewed KB gap CVE-2026-50006 on 16.0 cu says the guide names no product of the line, and the guide names one`,
		},
		{
			name: "fixed only on the other branch of a line the guide does not name", entries: november, covered: []string{"15.0", "16.0"},
			gaps: []kbGap{{CVE: "CVE-2026-50001", Line: "15.0", Branch: "cu", Vulnerable: true}},
			want: `the reviewed KB gap CVE-2026-50001 on 15.0 cu answers for a line the guide names no product of, which only an unlisted gap can`,
		},
		{
			name: "release month the guide spells another way", entries: []guideEntry{monthless}, covered: []string{"16.0"},
			want: `the guide gives CVE-2026-50006 on Microsoft SQL Server 2022 for x64-based Systems \(GDR\) the release "September 2026", which names no monthly release`,
		},
		{
			name: "covered line with no release date", entries: november, covered: []string{"16.0", "18.0"},
			want: `the snapshot covers release line 18.0, and its release date is not recorded`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			articles := slices.DeleteFunc(slices.Clone(kbArticles), func(article kbArticle) bool { return article.KB == "5100002" })

			got, err := assessGuide(kbGuide(c, test.entries...), test.covered, kbEvidence{Articles: articles, Gaps: test.gaps})

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got.ranges, qt.IsNil)
		})
	}
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
			Product: "Microsoft SQL Server 2019 for x64-based Systems (GDR)", Month: "2026-Nov", Fixes: []guideFix{{KB: "5100003"}}}},
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
		{name: "gap on a branch the banner never names", data: `{"articles": [], "rows": [], "gaps": [{"cve": "CVE-2026-50001", "line": "16.0", "branch": "gdrr", "unlisted": true}]}`,
			want: `invalid or repeated reviewed KB gap CVE-2026-50001 on 16.0 gdrr`},
		{name: "repeated gap", data: `{"articles": [], "rows": [], "gaps": [{"cve": "CVE-2026-50001", "line": "16.0", "branch": "cu", "unlisted": true}, {"cve": "CVE-2026-50001", "line": "16.0", "branch": "cu", "vulnerable": true}]}`,
			want: `invalid or repeated reviewed KB gap CVE-2026-50001 on 16.0 cu`},
		{name: "gap with no answer", data: `{"articles": [], "rows": [], "gaps": [{"cve": "CVE-2026-50001", "line": "16.0", "branch": "cu"}]}`,
			want: `invalid or repeated reviewed KB gap CVE-2026-50001 on 16.0 cu`},
		{name: "gap with two answers", data: `{"articles": [], "rows": [], "gaps": [{"cve": "CVE-2026-50001", "line": "16.0", "branch": "cu", "unlisted": true, "vulnerable": true}]}`,
			want: `invalid or repeated reviewed KB gap CVE-2026-50001 on 16.0 cu`},
		{name: "first build off its line", data: `{"articles": [], "rows": [], "gaps": [{"cve": "CVE-2026-50001", "line": "16.0", "branch": "cu", "since": {"kb": "1", "build": "15.0.4003.1", "released": "2026-09-20"}}]}`,
			want: `invalid or repeated reviewed KB gap CVE-2026-50001 on 16.0 cu`},
		{name: "first build with no release date", data: `{"articles": [], "rows": [], "gaps": [{"cve": "CVE-2026-50001", "line": "16.0", "branch": "cu", "since": {"kb": "1", "build": "16.0.4003.1", "released": "2026-Sep"}}]}`,
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
