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
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"ptah.run/internal/vulnerability"
)

const refreshInterval = 14 * 24 * time.Hour
const sourceLimit = 8 << 20

// sourceURLs names each product's fixed advisory page. MySQL has none: its
// page is the newest Critical Patch Update, which oracleIndexURL lists.
var sourceURLs = map[string]string{
	"postgresql": vulnerability.SourceURL,
	"mariadb":    "https://mariadb.com/docs/server/security/cve/community-server.md",
	"clickhouse": "https://clickhouse.com/docs/resources/changelogs/security-changelog.md",
}

// oracleIndexURL lists Oracle's Critical Patch Updates. Each update is a page
// of its own, so the daily check reads the index to see whether a newer one
// than the reviewed snapshot's has been published.
const oracleIndexURL = "https://www.oracle.com/security-alerts/"

var criticalPatchUpdate = regexp.MustCompile(`^https://www\.oracle\.com/security-alerts/cpu(jan|apr|jul|oct)([0-9]{4})\.html$`)

var criticalPatchUpdateLink = regexp.MustCompile(`href="(/security-alerts/cpu(?:jan|apr|jul|oct)[0-9]{4}\.html)"`)

var cpuMonths = []string{"jan", "apr", "jul", "oct"}

var cveID = regexp.MustCompile(`CVE-[0-9]{4}-[0-9]{4,}`)

// reviewSchemaVersion changes whenever a review field changes meaning, so a
// pending report in an older shape is written again rather than kept.
const reviewSchemaVersion = 3

// review is the report a changed versioned source leaves for a person. Added
// and removed IDs compare the observed page with the CVE IDs the reviewed
// snapshot records for its own page, not with the pinned advisories: most IDs
// on a page are never pinned, and counting them as new hides what changed.
// ObservedIDs is the list the updated snapshot records as source_cve_ids.
// Source is the observed page; ReviewedSource is set when that page is a
// newer one than the reviewed snapshot's, such as the next Critical Patch
// Update.
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

func main() {
	inputDir := flag.String("input-dir", "", "Directory of saved source pages for offline checks")
	flag.Parse()
	client := &http.Client{Timeout: 30 * time.Second}
	now := time.Now().UTC()
	// observed records the page a product's source was read from when the
	// page is not fixed. Offline, MySQL's page is the reviewed one.
	observed := make(map[string]string)
	read := func(product string) ([]byte, error) {
		if *inputDir != "" {
			return os.ReadFile(filepath.Join(*inputDir, product+".source"))
		}
		if product != "mysql" {
			return fetch(client, sourceURLs[product])
		}
		index, err := fetch(client, oracleIndexURL)
		if err != nil {
			return nil, fmt.Errorf("read Critical Patch Update index: %w", err)
		}
		newest, err := newestCriticalPatchUpdate(index)
		if err != nil {
			return nil, err
		}
		observed[product] = newest
		return fetch(client, newest)
	}
	err := refreshAll(products, read, func(product string, source []byte) error {
		return update(product, observed[product], source, now)
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// refreshAll checks every product even when an earlier one fails, so one
// engine's broken source or snapshot does not hide the state of the others.
// The joined error names each failing product on its own line.
func refreshAll(products []string, read func(string) ([]byte, error), apply func(string, []byte) error) error {
	var failures []error
	for _, product := range products {
		source, err := read(product)
		if err == nil {
			err = apply(product, source)
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", product, err))
		}
	}
	return errors.Join(failures...)
}

func fetch(client *http.Client, url string) ([]byte, error) {
	response, err := client.Get(url) // #nosec G107 -- every URL is a constant upstream source.
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

// update checks one product. observedURL is the page the source was read
// from when the product's page is not fixed, and empty otherwise.
func update(product, observedURL string, source []byte, now time.Time) error {
	if len(source) == 0 || len(source) > sourceLimit {
		return errors.New("empty or oversized source")
	}
	if product == "postgresql" {
		return updatePostgreSQL(source, now)
	}
	current, err := vulnerability.ReadVersionedSnapshot(product, "")
	if err != nil {
		return err
	}
	if observedURL != "" && observedURL != current.Source {
		return reviewSuccessor(current, observedURL, source, now)
	}
	return reconcileVersioned(current, source, now)
}

// newestCriticalPatchUpdate returns the newest Critical Patch Update the
// Oracle index links. An index that links none has changed shape.
func newestCriticalPatchUpdate(index []byte) (string, error) {
	newest, newestKey := "", ""
	for _, match := range criticalPatchUpdateLink.FindAllSubmatch(index, -1) {
		url := "https://www.oracle.com" + string(match[1])
		parts := criticalPatchUpdate.FindStringSubmatch(url)
		key := parts[2] + fmt.Sprint(slices.Index(cpuMonths, parts[1]))
		if key > newestKey {
			newest, newestKey = url, key
		}
	}
	if newest == "" {
		return "", errors.New("the Critical Patch Update index links no update")
	}
	return newest, nil
}

// advisoryContent returns the part of a source page its advisories come from.
// An Oracle page carries every product's risk matrix and a script that
// changes on each request, so the MySQL snapshot reviews its own matrix.
func advisoryContent(product string, source []byte) ([]byte, error) {
	if product != "mysql" {
		return source, nil
	}
	const heading = `<h4 id="AppendixMSQL">`
	start := bytes.Index(source, []byte(heading))
	if start < 0 {
		return nil, errors.New("MySQL risk matrix heading missing from the Critical Patch Update")
	}
	end := bytes.Index(source[start+len(heading):], []byte(`<h4 id="Appendix`))
	if end < 0 {
		return nil, errors.New("MySQL risk matrix has no following appendix")
	}
	return source[start : start+len(heading)+end], nil
}

// validSource reports whether a reviewed snapshot names its product's page.
func validSource(product, url string) bool {
	if product == "mysql" {
		return criticalPatchUpdate.MatchString(url)
	}
	return url == sourceURLs[product]
}

// reviewedSHA is the hash a source check compares: the advisory content when
// the snapshot pins one, and the whole page otherwise.
func reviewedSHA(current vulnerability.VersionedSnapshot) string {
	if current.AdvisoryContentSHA256 != "" {
		return current.AdvisoryContentSHA256
	}
	return current.SourceSHA256
}

// reviewSuccessor leaves a review of a newer page than the reviewed one. Its
// IDs are compared with the reviewed page's, so the report shows what the new
// update adds.
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
	report, err := buildReview(current, content, hex.EncodeToString(digest[:]), now)
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

// reconcileVersioned compares a fetched source with the reviewed snapshot and
// either confirms the snapshot, refreshes its retrieval time, or leaves a
// review report. Paths are relative to the repository root.
func reconcileVersioned(current vulnerability.VersionedSnapshot, source []byte, now time.Time) error {
	product := current.Product
	if !validSource(product, current.Source) {
		return fmt.Errorf("unexpected reviewed source %q", current.Source)
	}
	if len(current.SourceCVEIDs) == 0 {
		return errors.New("reviewed snapshot records no source_cve_ids, so a changed source cannot show what changed")
	}
	content, err := advisoryContent(product, source)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(content)
	observedSHA := hex.EncodeToString(digest[:])
	reviewPath := reviewFile(product)
	if observedSHA == reviewedSHA(current) {
		// The page is the reviewed one, so its IDs are the recorded list. A
		// snapshot whose hash moved without its list would make the next
		// review diff against the wrong page.
		if observed := sourceCVEIDs(content); !slices.Equal(observed, current.SourceCVEIDs) {
			return fmt.Errorf("source_cve_ids differ from the reviewed source: not recorded %s; recorded but absent %s",
				idList(difference(observed, current.SourceCVEIDs)), idList(difference(current.SourceCVEIDs, observed)))
		}
		if err := os.Remove(reviewPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if recent(current.RetrievedAt, now) {
			fmt.Printf("%s: advisory content unchanged; snapshot remains current\n", product)
			return nil
		}
		pageDigest := sha256.Sum256(source)
		current.RetrievedAt = now
		current.SourceSHA256 = hex.EncodeToString(pageDigest[:])
		if err := writeJSON(filepath.Join("internal", "vulnerability", product+".json"), current); err != nil {
			return err
		}
		fmt.Printf("%s: refreshed source verification time\n", product)
		return nil
	}
	report, err := buildReview(current, content, observedSHA, now)
	if err != nil {
		return err
	}
	return writeReview(report)
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

func buildReview(current vulnerability.VersionedSnapshot, source []byte, observedSHA string, now time.Time) (review, error) {
	observed := sourceCVEIDs(source)
	if len(observed) == 0 {
		return review{}, errors.New("changed source contains no CVE IDs")
	}
	if len(current.SourceCVEIDs) == 0 {
		return review{}, errors.New("reviewed snapshot records no source_cve_ids to compare with")
	}
	return review{
		SchemaVersion: reviewSchemaVersion, Product: current.Product, Source: current.Source, RetrievedAt: now,
		ReviewedSHA: reviewedSHA(current), ObservedSHA: observedSHA,
		AddedIDs:    difference(observed, current.SourceCVEIDs),
		RemovedIDs:  difference(current.SourceCVEIDs, observed),
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
