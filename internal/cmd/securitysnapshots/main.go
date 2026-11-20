// Command securitysnapshots checks the bundled server advisories against their
// upstream pages. It leaves changed version ranges for human review.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"ptah.run/internal/vulnerability"
)

const refreshInterval = 14 * 24 * time.Hour
const sourceLimit = 8 << 20

// sourceURLs names each product's fixed advisory page. MySQL has none: its
// pages are Oracle's updates, which oracleIndexURL lists.
var sourceURLs = map[string]string{
	"postgresql": vulnerability.SourceURL,
	"mariadb":    "https://mariadb.com/docs/server/security/cve/community-server.md",
	"clickhouse": "https://clickhouse.com/docs/resources/changelogs/security-changelog.md",
}

// oracleIndexURL lists Oracle's updates and schedules the next ones. Oracle
// publishes a Critical Patch Update each quarter and a Critical Security Patch
// Update in each month between them, each a page of its own, so the daily
// check reads the index to find an update newer than the snapshot's pages and
// the date the next one is due.
const oracleIndexURL = "https://www.oracle.com/security-alerts/"

var oracleUpdate = regexp.MustCompile(`^https://www\.oracle\.com/security-alerts/cs?pu(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)([0-9]{4})\.html$`)

var oracleUpdateLink = regexp.MustCompile(`href="(/security-alerts/cs?pu(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[0-9]{4}\.html)"`)

var updateMonths = []string{"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"}

// mysqlMatrixHeading opens the MySQL risk matrix on an Oracle update page. A
// monthly update with nothing for MySQL has none.
const mysqlMatrixHeading = `<h4 id="AppendixMSQL">`

const scheduleLead = "next four dates are:"

// scheduledDates reads one of the index's lists of upcoming release dates,
// one for each kind of update.
var scheduledDates = regexp.MustCompile(`next four dates are:</p>\s*<ul[^>]*>((?:\s*<li>[^<]*</li>)+)\s*</ul>`)

var scheduledDate = regexp.MustCompile(`<li>([^<]*)</li>`)

var cveID = regexp.MustCompile(`CVE-[0-9]{4}-[0-9]{4,}`)

// reviewSchemaVersion changes whenever a review field changes meaning, so a
// pending report in an older shape is written again rather than kept.
const reviewSchemaVersion = 3

// review is the report a changed versioned source leaves for a person. Added
// and removed IDs compare the observed page with the CVE IDs the reviewed
// snapshot records for that page, not with the pinned advisories: most IDs
// on a page are never pinned, and counting them as new hides what changed.
// ObservedIDs is the list the updated snapshot records for the page. Source
// is the observed page; ReviewedSource is set when that page is a newer one
// than the reviewed snapshot's, such as the next Oracle update.
type review struct {
	SchemaVersion  int       `json:"schema_version"`
	Product        string    `json:"product"`
	Source         string    `json:"source"`
	ReviewedSource string    `json:"reviewed_source,omitempty"`
	RetrievedAt    time.Time `json:"retrieved_at"`
	ReviewedSHA    string    `json:"reviewed_sha256"`
	ObservedSHA    string    `json:"observed_sha256"`
	AddedIDs       []string  `json:"added_cve_ids"`
	RemovedIDs     []string  `json:"removed_cve_ids"`
	ObservedIDs    []string  `json:"observed_cve_ids"`
}

var products = []string{"postgresql", "mariadb", "clickhouse", "mysql"}

// fetcher reads one page by its URL.
type fetcher func(url string) ([]byte, error)

func main() {
	inputDir := flag.String("input-dir", "", "Directory of saved pages for an offline check, each named by its URL, path-escaped")
	flag.Parse()
	client := &http.Client{Timeout: 30 * time.Second}
	now := time.Now().UTC()
	fetchPage := func(url string) ([]byte, error) { return fetch(client, url) }
	if *inputDir != "" {
		fetchPage = func(url string) ([]byte, error) {
			return os.ReadFile(filepath.Join(*inputDir, neturl.PathEscape(url)))
		}
	}
	err := refreshAll(products, func(product string) error { return check(product, fetchPage, now) })
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// refreshAll checks every product even when an earlier one fails, so one
// engine's broken source or snapshot does not hide the state of the others.
// The joined error names each failing product on its own line.
func refreshAll(products []string, checkProduct func(string) error) error {
	var failures []error
	for _, product := range products {
		if err := checkProduct(product); err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", product, err))
		}
	}
	return errors.Join(failures...)
}

// fetch reads one upstream page. Every URL it is given is a constant source,
// the Oracle index, or an Oracle update page that oracleUpdate has confined to
// www.oracle.com/security-alerts/: a link read from the index or a page a
// reviewed snapshot names, both checked before they are fetched.
func fetch(client *http.Client, url string) ([]byte, error) {
	response, err := client.Get(url) // #nosec G107 -- the URL is a constant source or an Oracle update page oracleUpdate confines.
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("advisory source returned HTTP %d", response.StatusCode)
	}
	source, err := io.ReadAll(io.LimitReader(response.Body, sourceLimit+1))
	if err != nil {
		return nil, err
	}
	if len(source) > sourceLimit {
		return nil, errors.New("advisory source exceeds 8 MiB")
	}
	return source, nil
}

// check reads one product's pages and checks its bundled snapshot against
// them.
func check(product string, fetchPage fetcher, now time.Time) error {
	if product == "postgresql" {
		source, err := fetchPage(sourceURLs[product])
		if err != nil {
			return err
		}
		return updatePostgreSQL(source, now)
	}
	current, err := vulnerability.ReadVersionedSnapshot(product, "")
	if err != nil {
		return err
	}
	return checkVersioned(current, fetchPage, now)
}

// checkVersioned leaves a review of a newer update when one carries
// advisories for the product, and otherwise reconciles every page the
// snapshot reviews. For MySQL it also reads when Oracle's next update is due,
// which the refreshed snapshot records as valid_until.
func checkVersioned(current vulnerability.VersionedSnapshot, fetchPage fetcher, now time.Time) error {
	sources := reviewedSources(current)
	for _, source := range sources {
		if !validSource(current.Product, source.url) {
			return fmt.Errorf("unexpected reviewed source %q", source.url)
		}
	}
	var next time.Time
	if current.Product == "mysql" {
		index, err := fetchPage(oracleIndexURL)
		if err != nil {
			return fmt.Errorf("read the security alerts index: %w", err)
		}
		successor, page, err := newerUpdate(current, index, fetchPage)
		if err != nil {
			return err
		}
		if successor != "" {
			return reviewSuccessor(current, successor, page, now)
		}
		if next, err = scheduledUpdate(index, now); err != nil {
			return err
		}
	}
	pages := make(map[string][]byte, len(sources))
	for _, source := range sources {
		page, err := fetchPage(source.url)
		if err != nil {
			return fmt.Errorf("read %s: %w", source.url, err)
		}
		pages[source.url] = page
	}
	return reconcileVersioned(current, pages, next, now)
}

// updateKey orders Oracle update pages by month. The second result is false
// for a URL that is not one.
func updateKey(url string) (int, bool) {
	parts := oracleUpdate.FindStringSubmatch(url)
	if parts == nil {
		return 0, false
	}
	year, err := strconv.Atoi(parts[2])
	if err != nil {
		return 0, false
	}
	return year*len(updateMonths) + slices.Index(updateMonths, parts[1]), true
}

// updatesAfter returns the update pages the index links that are newer than
// reviewed, oldest first. An index that links none has changed shape.
func updatesAfter(index []byte, reviewed string) ([]string, error) {
	reviewedKey, ok := updateKey(reviewed)
	if !ok {
		return nil, fmt.Errorf("reviewed source %q is not an Oracle update page", reviewed)
	}
	links := oracleUpdateLink.FindAllSubmatch(index, -1)
	if len(links) == 0 {
		return nil, errors.New("the security alerts index links no update")
	}
	newer := make([]string, 0)
	for _, match := range links {
		url := "https://www.oracle.com" + string(match[1])
		if key, _ := updateKey(url); key > reviewedKey && !slices.Contains(newer, url) {
			newer = append(newer, url)
		}
	}
	slices.SortFunc(newer, func(a, b string) int {
		left, _ := updateKey(a)
		right, _ := updateKey(b)
		return left - right
	})
	return newer, nil
}

// newerUpdate returns the oldest update newer than the snapshot's source that
// carries a MySQL risk matrix, with its page, or an empty URL when none does.
// An update with no matrix has nothing for MySQL, so it leaves the reviewed
// pages complete.
func newerUpdate(current vulnerability.VersionedSnapshot, index []byte, fetchPage fetcher) (string, []byte, error) {
	newer, err := updatesAfter(index, current.Source)
	if err != nil {
		return "", nil, err
	}
	for _, url := range newer {
		page, err := fetchPage(url)
		if err != nil {
			return "", nil, fmt.Errorf("read %s: %w", url, err)
		}
		if bytes.Contains(page, []byte(mysqlMatrixHeading)) {
			return url, page, nil
		}
		fmt.Printf("%s: %s carries no MySQL risk matrix\n", current.Product, url)
	}
	return "", nil, nil
}

// scheduledUpdate returns the earliest release date the index schedules for
// today or later, across every kind of update it schedules. A date that is
// today counts: Oracle publishes late in the day, so an update due today is
// one the snapshot cannot yet carry.
func scheduledUpdate(index []byte, now time.Time) (time.Time, error) {
	lists := scheduledDates.FindAllSubmatch(index, -1)
	if leads := bytes.Count(index, []byte(scheduleLead)); leads == 0 || len(lists) != leads {
		return time.Time{}, errors.New("the security alerts index has no update schedule in the expected shape")
	}
	year, month, day := now.UTC().Date()
	today := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	var next time.Time
	for _, list := range lists {
		for _, item := range scheduledDate.FindAllSubmatch(list[1], -1) {
			date, err := time.Parse("2 January 2006", strings.TrimSpace(string(item[1])))
			if err != nil {
				return time.Time{}, fmt.Errorf("unreadable scheduled update date %q", item[1])
			}
			if !date.Before(today) && (next.IsZero() || date.Before(next)) {
				next = date
			}
		}
	}
	if next.IsZero() {
		return time.Time{}, errors.New("the security alerts index schedules no update from today on")
	}
	return next, nil
}

// advisoryContent returns the part of a source page its advisories come from.
// An Oracle page carries every product's risk matrix and a script that
// changes on each request, so the MySQL snapshot reviews its own matrix.
func advisoryContent(product string, source []byte) ([]byte, error) {
	if product != "mysql" {
		return source, nil
	}
	start := bytes.Index(source, []byte(mysqlMatrixHeading))
	if start < 0 {
		return nil, errors.New("MySQL risk matrix heading missing from the Oracle update page")
	}
	end := bytes.Index(source[start+len(mysqlMatrixHeading):], []byte(`<h4 id="Appendix`))
	if end < 0 {
		return nil, errors.New("MySQL risk matrix has no following appendix")
	}
	return source[start : start+len(mysqlMatrixHeading)+end], nil
}

// validSource reports whether a reviewed snapshot may name a page as one of
// its product's sources.
func validSource(product, url string) bool {
	if product == "mysql" {
		return oracleUpdate.MatchString(url)
	}
	return url == sourceURLs[product]
}

// reviewedSource is one page a snapshot reviews: the hash a source check
// compares, which is the advisory content's when the snapshot pins one and
// the whole page's otherwise, and the CVE IDs recorded for the page.
type reviewedSource struct {
	url   string
	label string
	sha   string
	ids   []string
}

func reviewedSources(current vulnerability.VersionedSnapshot) []reviewedSource {
	sha := current.AdvisoryContentSHA256
	if sha == "" {
		sha = current.SourceSHA256
	}
	sources := []reviewedSource{{url: current.Source, label: "source_cve_ids", sha: sha, ids: current.SourceCVEIDs}}
	for _, additional := range current.AdditionalSources {
		sha := additional.AdvisoryContentSHA256
		if sha == "" {
			sha = additional.SHA256
		}
		sources = append(sources, reviewedSource{url: additional.URL, label: "the cve_ids of " + additional.URL, sha: sha, ids: additional.CVEIDs})
	}
	return sources
}

// reviewSuccessor leaves a review of a newer page than the reviewed one. Its
// IDs are compared with the snapshot's source, so the report shows what the
// new update adds.
func reviewSuccessor(current vulnerability.VersionedSnapshot, observedURL string, source []byte, now time.Time) error {
	product := current.Product
	if !validSource(product, current.Source) || !validSource(product, observedURL) {
		return fmt.Errorf("unexpected reviewed source %q or observed source %q", current.Source, observedURL)
	}
	content, err := advisoryContent(product, source)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(content)
	report, err := buildReview(product, reviewedSources(current)[0], content, hex.EncodeToString(digest[:]), now)
	if err != nil {
		return err
	}
	report.Source, report.ReviewedSource = observedURL, current.Source
	return writeReview(report)
}

func updatePostgreSQL(source []byte, now time.Time) error {
	current, err := vulnerability.ReadSnapshot("")
	if err != nil {
		return err
	}
	proposed, err := vulnerability.SnapshotFromHTML(source, now)
	if err != nil {
		return err
	}
	if proposed.SourceSHA256 == current.SourceSHA256 && recent(current.RetrievedAt, now) {
		fmt.Println("postgresql: source unchanged; snapshot remains current")
		return nil
	}
	if err := writeJSON("internal/vulnerability/postgresql.json", proposed); err != nil {
		return err
	}
	fmt.Printf("postgresql: refreshed %d advisories\n", len(proposed.Advisories))
	return nil
}

// reconcileVersioned compares the fetched pages with the reviewed snapshot
// and either confirms the snapshot, refreshes its verification time and
// valid_until, or leaves a review report for the first page that changed.
// next is the vendor's next scheduled update, zero when it publishes no
// schedule. Paths are relative to the repository root.
func reconcileVersioned(current vulnerability.VersionedSnapshot, pages map[string][]byte, next, now time.Time) error {
	product := current.Product
	if len(current.SourceCVEIDs) == 0 {
		return errors.New("reviewed snapshot records no source_cve_ids, so a changed source cannot show what changed")
	}
	for _, source := range reviewedSources(current) {
		if !validSource(product, source.url) {
			return fmt.Errorf("unexpected reviewed source %q", source.url)
		}
		content, err := advisoryContent(product, pages[source.url])
		if err != nil {
			return fmt.Errorf("%s: %w", source.url, err)
		}
		digest := sha256.Sum256(content)
		observedSHA := hex.EncodeToString(digest[:])
		if observedSHA != source.sha {
			report, err := buildReview(product, source, content, observedSHA, now)
			if err != nil {
				return err
			}
			return writeReview(report)
		}
		// The page is the reviewed one, so its IDs are the recorded list. A
		// snapshot whose hash moved without its list would make the next
		// review diff against the wrong page.
		if observed := sourceCVEIDs(content); !slices.Equal(observed, source.ids) {
			return fmt.Errorf("%s differ from the reviewed source: not recorded %s; recorded but absent %s", source.label,
				idList(difference(observed, source.ids)), idList(difference(source.ids, observed)))
		}
	}
	if err := os.Remove(reviewFile(product)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if recent(current.RetrievedAt, now) && current.ValidUntil.Equal(next) {
		fmt.Printf("%s: advisory content unchanged; snapshot remains current\n", product)
		return nil
	}
	current.RetrievedAt = now
	current.ValidUntil = next
	current.SourceSHA256 = digestOf(pages[current.Source])
	current.AdditionalSources = slices.Clone(current.AdditionalSources)
	for index := range current.AdditionalSources {
		current.AdditionalSources[index].SHA256 = digestOf(pages[current.AdditionalSources[index].URL])
	}
	if err := writeJSON(filepath.Join("internal", "vulnerability", product+".json"), current); err != nil {
		return err
	}
	fmt.Printf("%s: refreshed source verification time\n", product)
	return nil
}

func digestOf(page []byte) string {
	digest := sha256.Sum256(page)
	return hex.EncodeToString(digest[:])
}

func reviewFile(product string) string {
	return filepath.Join("internal", "vulnerability", "review", product+".json")
}

// writeReview leaves a review report unless the same one is already pending.
func writeReview(report review) error {
	reviewPath := reviewFile(report.Product)
	if data, readErr := os.ReadFile(reviewPath); readErr == nil {
		var pending review
		if err := json.Unmarshal(data, &pending); err != nil {
			return fmt.Errorf("decode pending source review: %w", err)
		}
		if pending.SchemaVersion == reviewSchemaVersion && pending.Source == report.Source &&
			pending.ReviewedSHA == report.ReviewedSHA && pending.ObservedSHA == report.ObservedSHA {
			fmt.Printf("%s: source review is still pending\n", report.Product)
			return nil
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	}
	if err := os.MkdirAll(filepath.Dir(reviewPath), 0o755); err != nil {
		return err
	}
	if err := writeJSON(reviewPath, report); err != nil {
		return err
	}
	fmt.Printf("%s: source changed; review %s before updating the snapshot\n", report.Product, reviewPath)
	return nil
}

func buildReview(product string, source reviewedSource, content []byte, observedSHA string, now time.Time) (review, error) {
	observed := sourceCVEIDs(content)
	if len(observed) == 0 {
		return review{}, errors.New("changed source contains no CVE IDs")
	}
	if len(source.ids) == 0 {
		return review{}, errors.New("reviewed snapshot records no source_cve_ids to compare with")
	}
	return review{
		SchemaVersion: reviewSchemaVersion, Product: product, Source: source.url, RetrievedAt: now,
		ReviewedSHA: source.sha, ObservedSHA: observedSHA,
		AddedIDs:    difference(observed, source.ids),
		RemovedIDs:  difference(source.ids, observed),
		ObservedIDs: observed,
	}, nil
}

// sourceCVEIDs is the one reading of a page's CVE IDs: the review report and
// the reviewed snapshot's source_cve_ids must agree on it, or every refresh
// reports IDs that did not change.
func sourceCVEIDs(source []byte) []string {
	ids := cveID.FindAllString(string(source), -1)
	slices.Sort(ids)
	return slices.Compact(ids)
}

// idListLimit is how many IDs a mismatch names before it counts the rest: a
// page whose every ID differs would otherwise put hundreds of IDs on one line.
const idListLimit = 10

func idList(ids []string) string {
	if len(ids) == 0 {
		return "none"
	}
	if len(ids) > idListLimit {
		return fmt.Sprintf("%s and %d more", strings.Join(ids[:idListLimit], ", "), len(ids)-idListLimit)
	}
	return strings.Join(ids, ", ")
}

func difference(left, right []string) []string {
	result := make([]string, 0)
	for _, id := range left {
		if !slices.Contains(right, id) {
			result = append(result, id)
		}
	}
	return result
}

func recent(retrievedAt, now time.Time) bool {
	age := now.Sub(retrievedAt)
	return age >= 0 && age < refreshInterval
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o600) // #nosec G703 -- paths are fixed in the contributor checkout.
}
