package main

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"ptah.run/internal/vulnerability"
)

// The Security Update Guide answers some rows by leaving them out: it lists
// an advisory for one servicing branch or release line and not another,
// although Microsoft released a security update for that branch in a month
// the advisory belongs to. The guide is sometimes wrong there, and the KB
// article of that update lists the advisory with a fix. So the SQL Server
// snapshot rests on a second witness: internal/vulnerability/sqlserver-kb.json
// records each KB article the guide names and a reviewed answer for every row
// the guide decides by absence. Where an article exists, the answer is read
// from it. Where the guide names one branch of a line and no article updates
// the other in the advisory's months, the answer is a gap, read from
// Microsoft's build list for the line: no security update shipped there. The
// check refuses a snapshot whose ranges are not what the guide and those
// answers give together, so a snapshot rebuilt from the guide alone cannot
// pass it.

// kbEvidencePath is the reviewed record of the KB articles.
var kbEvidencePath = filepath.Join("internal", "vulnerability", "sqlserver-kb.json")

// sqlServerEngine is a guide product the snapshot pins: a SQL Server database
// engine of one release year on one servicing branch.
var sqlServerEngine = regexp.MustCompile(`^Microsoft SQL Server ([0-9]{4}) for x64-based Systems \(?(GDR|CU ?[0-9]+)\)?$`)

// sqlServerLines is the release line of each SQL Server release year. The
// guide names a product by its year, and some rows name no build to read the
// line from.
var sqlServerLines = map[string]string{"2016": "13.0", "2017": "14.0", "2019": "15.0", "2022": "16.0", "2025": "17.0"}

var releaseMonth = regexp.MustCompile(`^[0-9]{4}-(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)$`)

var releaseLine = regexp.MustCompile(`^[0-9]+\.[0-9]+$`)

var buildNumber = regexp.MustCompile(`^([0-9]+\.[0-9]+)\.[0-9]+\.[0-9]+$`)

// kbEvidence is the reviewed record.
type kbEvidence struct {
	Articles []kbArticle `json:"articles"`
	Rows     []kbRow     `json:"rows"`
	Gaps     []kbGap     `json:"gaps"`
}

// kbArticle is a KB article as its page states it: the month Microsoft
// released it, the release line and servicing branch it updates, and the
// build it ships. GuideBuild is a different build the guide names for the
// article; where the guide names it, both builds are pinned, so a server
// between them reads uncertain.
type kbArticle struct {
	KB         string `json:"kb"`
	Month      string `json:"month"`
	Line       string `json:"line"`
	Branch     string `json:"branch"`
	Build      string `json:"build"`
	GuideBuild string `json:"guide_build,omitempty"`
}

// kbRow answers one row the guide decides by absence: whether the article
// lists the advisory among the vulnerabilities it fixes.
type kbRow struct {
	CVE   string `json:"cve"`
	KB    string `json:"kb"`
	Lists bool   `json:"lists"`
}

// kbGap answers a branch of a line the guide names an advisory for only on
// the other branch, where no recorded article updates the branch in the
// advisory's months: a reviewer read Microsoft's build list for the line and
// found no security update for the branch in those months, so the guide's
// absence stands.
type kbGap struct {
	CVE    string `json:"cve"`
	Line   string `json:"line"`
	Branch string `json:"branch"`
}

// kbQuestion is a row the guide decides by absence that the record does not
// answer yet, with the page a person reads to answer it: the article of that
// month, or for a gap, which names no article, the line's build list.
type kbQuestion struct {
	CVE    string   `json:"cve"`
	Line   string   `json:"line"`
	Branch string   `json:"branch"`
	Months []string `json:"months"`
	KB     string   `json:"kb,omitempty"`
	URL    string   `json:"url"`
}

func kbURL(kb string) string {
	return "https://support.microsoft.com/help/" + kb
}

// buildListURL is Microsoft's list of every build of a release line, with
// its date and KB article.
func buildListURL(line string) string {
	for year, known := range sqlServerLines {
		if known == line {
			return "https://learn.microsoft.com/troubleshoot/sql/releases/sqlserver-" + year + "/build-versions"
		}
	}
	return ""
}

// readKBEvidence reads and validates the reviewed record.
func readKBEvidence(path string) (kbEvidence, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return kbEvidence{}, fmt.Errorf("read the reviewed KB articles: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var evidence kbEvidence
	if err := decoder.Decode(&evidence); err != nil {
		return kbEvidence{}, fmt.Errorf("decode the reviewed KB articles: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return kbEvidence{}, errors.New("the reviewed KB articles carry trailing data")
	}
	if err := evidence.validate(); err != nil {
		return kbEvidence{}, err
	}
	return evidence, nil
}

func (e kbEvidence) validate() error {
	articles := make(map[string]bool, len(e.Articles))
	for _, article := range e.Articles {
		line := buildNumber.FindStringSubmatch(article.Build)
		guideLine := buildNumber.FindStringSubmatch(article.GuideBuild)
		if article.KB == "" || articles[article.KB] || !releaseMonth.MatchString(article.Month) ||
			(article.Branch != "gdr" && article.Branch != "cu") || line == nil || line[1] != article.Line ||
			(article.GuideBuild != "" && (guideLine == nil || guideLine[1] != article.Line || article.GuideBuild == article.Build)) {
			return fmt.Errorf("invalid or repeated reviewed KB article %q", article.KB)
		}
		articles[article.KB] = true
	}
	rows := make(map[kbRow]bool, len(e.Rows))
	for _, row := range e.Rows {
		key := kbRow{CVE: row.CVE, KB: row.KB}
		if !cveID.MatchString(row.CVE) || !articles[row.KB] || rows[key] {
			return fmt.Errorf("invalid or repeated reviewed KB row %s in KB%s", row.CVE, row.KB)
		}
		rows[key] = true
	}
	gaps := make(map[kbGap]bool, len(e.Gaps))
	for _, gap := range e.Gaps {
		if !cveID.MatchString(gap.CVE) || !releaseLine.MatchString(gap.Line) || (gap.Branch != "gdr" && gap.Branch != "cu") || gaps[gap] {
			return fmt.Errorf("invalid or repeated reviewed KB gap %s on %s %s", gap.CVE, gap.Line, gap.Branch)
		}
		gaps[gap] = true
	}
	return nil
}

// guideAssessment is what the guide and the reviewed record give together.
type guideAssessment struct {
	// unrecorded names each KB article the guide names on a covered line with
	// no reviewed record, or with one whose line, branch or build the guide
	// disagrees with.
	unrecorded []string
	// unanswered lists the rows the guide decides by absence that the record
	// does not answer.
	unanswered []kbQuestion
	// ranges is every advisory's fixed builds on the covered lines.
	ranges map[string][]vulnerability.VersionRange
}

// assessGuide reads the guide content against the reviewed record. An
// advisory's months are those of the articles the guide names for it on a
// covered line and the month that published it. A row is decided by absence
// where an article of one of those months on a covered line is not named for
// the advisory: the article may fix it too. It is a gap where the guide names
// one branch of a line and no recorded article of those months updates the
// other.
func assessGuide(content []byte, covered []string, evidence kbEvidence) (guideAssessment, error) {
	var entries []guideEntry
	if err := json.Unmarshal(content, &entries); err != nil {
		return guideAssessment{}, fmt.Errorf("decode the guide content: %w", err)
	}
	articles := make(map[string]kbArticle, len(evidence.Articles))
	for _, article := range evidence.Articles {
		articles[article.KB] = article
	}
	answers := make(map[kbRow]bool, len(evidence.Rows))
	for _, row := range evidence.Rows {
		answers[kbRow{CVE: row.CVE, KB: row.KB}] = row.Lists
	}
	result := guideAssessment{ranges: make(map[string][]vulnerability.VersionRange)}
	unrecorded := make(map[string]bool)
	named := make(map[string]map[string]bool)
	months := make(map[string]map[string]bool)
	for _, entry := range entries {
		product := sqlServerEngine.FindStringSubmatch(entry.Product)
		if product == nil {
			continue
		}
		branch := "cu"
		if product[2] == "GDR" {
			branch = "gdr"
		}
		for _, fix := range entry.Fixes {
			line := lineOf(product[1], fix.Build)
			if !slices.Contains(covered, line) {
				continue
			}
			article, recorded := articles[fix.KB]
			guideBuild := article.GuideBuild != "" && fix.Build == article.GuideBuild
			if !recorded || article.Line != line || article.Branch != branch || (fix.Build != "" && fix.Build != article.Build && !guideBuild) {
				unrecorded[fix.KB] = true
				continue
			}
			addKey(named, entry.CVE, fix.KB)
			addKey(months, entry.CVE, article.Month)
			if releaseMonth.MatchString(entry.Month) {
				addKey(months, entry.CVE, entry.Month)
			}
			result.ranges[entry.CVE] = append(result.ranges[entry.CVE], rangeOf(article))
			if guideBuild {
				result.ranges[entry.CVE] = append(result.ranges[entry.CVE],
					vulnerability.VersionRange{Line: line, Branch: branch, From: line, Fixed: article.GuideBuild})
			}
		}
	}
	gaps := make(map[kbGap]bool, len(evidence.Gaps))
	for _, gap := range evidence.Gaps {
		gaps[gap] = true
	}
	for _, cve := range sortedKeys(named) {
		// served holds each line and branch that a named article or an
		// article of the advisory's months updates.
		served := make(map[[2]string]bool)
		lines := make(map[string]bool)
		for kb := range named[cve] {
			served[[2]string{articles[kb].Line, articles[kb].Branch}] = true
			lines[articles[kb].Line] = true
		}
		for _, article := range evidence.Articles {
			if !months[cve][article.Month] || !slices.Contains(covered, article.Line) || named[cve][article.KB] {
				continue
			}
			served[[2]string{article.Line, article.Branch}] = true
			lists, answered := answers[kbRow{CVE: cve, KB: article.KB}]
			switch {
			case !answered:
				result.unanswered = append(result.unanswered, kbQuestion{CVE: cve, Line: article.Line, Branch: article.Branch,
					Months: []string{article.Month}, KB: article.KB, URL: kbURL(article.KB)})
			case lists:
				result.ranges[cve] = append(result.ranges[cve], rangeOf(article))
			}
		}
		for _, line := range sortedKeys(lines) {
			for _, branch := range []string{"cu", "gdr"} {
				gap := kbGap{CVE: cve, Line: line, Branch: branch}
				if !served[[2]string{line, branch}] && !gaps[gap] {
					result.unanswered = append(result.unanswered, kbQuestion{CVE: cve, Line: line, Branch: branch,
						Months: sortedKeys(months[cve]), URL: buildListURL(line)})
				}
			}
		}
	}
	for cve, ranges := range result.ranges {
		slices.SortFunc(ranges, compareRanges)
		result.ranges[cve] = slices.Compact(ranges)
	}
	result.unrecorded = sortedKeys(unrecorded)
	slices.SortFunc(result.unanswered, func(a, b kbQuestion) int {
		return cmp.Or(cmp.Compare(a.CVE, b.CVE), cmp.Compare(a.Line, b.Line), cmp.Compare(a.Branch, b.Branch), cmp.Compare(a.KB, b.KB))
	})
	return result, nil
}

func addKey(sets map[string]map[string]bool, key, value string) {
	if sets[key] == nil {
		sets[key] = make(map[string]bool)
	}
	sets[key][value] = true
}

// lineOf is a guide row's release line: its year's, or for a year the table
// does not know, its build's.
func lineOf(year, build string) string {
	if line, known := sqlServerLines[year]; known {
		return line
	}
	if parts := buildNumber.FindStringSubmatch(build); parts != nil {
		return parts[1]
	}
	return ""
}

func rangeOf(article kbArticle) vulnerability.VersionRange {
	return vulnerability.VersionRange{Line: article.Line, Branch: article.Branch, From: article.Line, Fixed: article.Build}
}

func compareRanges(a, b vulnerability.VersionRange) int {
	return cmp.Or(cmp.Compare(a.Line, b.Line), cmp.Compare(a.Branch, b.Branch), cmp.Compare(a.Fixed, b.Fixed),
		cmp.Compare(a.From, b.From), cmp.Compare(a.LastAffected, b.LastAffected))
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// verifySQLServerSnapshot refuses a snapshot the guide and the reviewed
// record do not give exactly: an unrecorded article, an unanswered row, or
// an advisory whose ranges differ from theirs.
func verifySQLServerSnapshot(current vulnerability.VersionedSnapshot, content []byte, evidence kbEvidence) error {
	assessment, err := assessGuide(content, current.CoveredLines, evidence)
	if err != nil {
		return err
	}
	if len(assessment.unrecorded) != 0 {
		return fmt.Errorf("the guide names KB articles %s has no record of, or one the guide disagrees with: KB%s",
			kbEvidencePath, strings.Join(assessment.unrecorded, ", KB"))
	}
	if len(assessment.unanswered) != 0 {
		first := assessment.unanswered[0]
		return fmt.Errorf("the guide decides %s on %s %s by absence, and %s does not answer it from %s; unanswered rows: %d",
			first.CVE, first.Line, first.Branch, kbEvidencePath, first.URL, len(assessment.unanswered))
	}
	pinned := make(map[string]bool, len(current.Advisories))
	for _, advisory := range current.Advisories {
		pinned[advisory.ID] = true
		ranges := slices.Clone(advisory.Ranges)
		slices.SortFunc(ranges, compareRanges)
		if want := assessment.ranges[advisory.ID]; !slices.Equal(ranges, want) {
			return fmt.Errorf("%s carries the fixed builds %s, and the guide with the reviewed KB articles gives %s",
				advisory.ID, rangeList(ranges), rangeList(want))
		}
	}
	for _, cve := range sortedKeys(assessment.ranges) {
		if !pinned[cve] {
			return fmt.Errorf("%s has fixed builds on a covered line, %s, and the snapshot does not pin it",
				cve, rangeList(assessment.ranges[cve]))
		}
	}
	return nil
}

func rangeList(ranges []vulnerability.VersionRange) string {
	if len(ranges) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(ranges))
	for _, interval := range ranges {
		parts = append(parts, interval.Line+" "+interval.Branch+" "+interval.Fixed)
	}
	return strings.Join(parts, ", ")
}

// withKBQuestions adds to a changed guide's review every article and row the
// reviewed record does not cover yet, so the person updating the snapshot
// reads those articles too.
func withKBQuestions(report review, current vulnerability.VersionedSnapshot, content []byte) (review, error) {
	evidence, err := readKBEvidence(kbEvidencePath)
	if err != nil {
		return review{}, err
	}
	assessment, err := assessGuide(content, current.CoveredLines, evidence)
	if err != nil {
		return review{}, err
	}
	for _, kb := range assessment.unrecorded {
		report.UnrecordedKBArticles = append(report.UnrecordedKBArticles, kbURL(kb))
	}
	report.UnansweredKBRows = assessment.unanswered
	return report, nil
}
