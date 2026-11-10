// Command securitysnapshots checks the bundled server advisories against their
// upstream pages. It leaves changed version ranges for human review.
package main

import (
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
	"time"

	"ptah.run/internal/vulnerability"
)

const refreshInterval = 14 * 24 * time.Hour
const sourceLimit = 8 << 20

var sourceURLs = map[string]string{
	"postgresql": vulnerability.SourceURL,
	"ydb":        "https://ydb.tech/docs/en/security-changelog.md",
	"mariadb":    "https://mariadb.com/docs/server/security/cve/community-server.md",
	"clickhouse": "https://clickhouse.com/docs/resources/changelogs/security-changelog.md",
}

var cveID = regexp.MustCompile(`CVE-[0-9]{4}-[0-9]{4,}`)

type review struct {
	SchemaVersion int       `json:"schema_version"`
	Product       string    `json:"product"`
	Source        string    `json:"source"`
	RetrievedAt   time.Time `json:"retrieved_at"`
	ReviewedSHA   string    `json:"reviewed_sha256"`
	ObservedSHA   string    `json:"observed_sha256"`
	ObservedIDs   []string  `json:"observed_cve_ids"`
	AddedIDs      []string  `json:"new_cve_ids"`
	MissingIDs    []string  `json:"missing_cve_ids"`
}

func main() {
	inputDir := flag.String("input-dir", "", "Directory of saved source pages for offline checks")
	flag.Parse()
	client := &http.Client{Timeout: 30 * time.Second}
	now := time.Now().UTC()
	for _, product := range []string{"postgresql", "ydb", "mariadb", "clickhouse"} {
		var source []byte
		var err error
		if *inputDir != "" {
			source, err = os.ReadFile(filepath.Join(*inputDir, product+".source"))
		} else {
			source, err = fetch(client, sourceURLs[product])
		}
		if err == nil {
			err = update(product, source, now)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, product+":", err)
			os.Exit(1)
		}
	}
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

func update(product string, source []byte, now time.Time) error {
	if len(source) == 0 || len(source) > sourceLimit {
		return errors.New("empty or oversized source")
	}
	if product == "postgresql" {
		return updatePostgreSQL(source, now)
	}
	return updateVersioned(product, source, now)
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

func updateVersioned(product string, source []byte, now time.Time) error {
	current, err := vulnerability.ReadVersionedSnapshot(product, "")
	if err != nil {
		return err
	}
	if current.Source != sourceURLs[product] {
		return fmt.Errorf("unexpected reviewed source %q", current.Source)
	}
	digest := sha256.Sum256(source)
	observedSHA := hex.EncodeToString(digest[:])
	reviewPath := filepath.Join("internal", "vulnerability", "review", product+".json")
	if observedSHA == current.SourceSHA256 {
		if err := os.Remove(reviewPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if recent(current.RetrievedAt, now) {
			fmt.Printf("%s: source unchanged; snapshot remains current\n", product)
			return nil
		}
		current.RetrievedAt = now
		if err := writeJSON(filepath.Join("internal", "vulnerability", product+".json"), current); err != nil {
			return err
		}
		fmt.Printf("%s: refreshed source verification time\n", product)
		return nil
	}
	if data, readErr := os.ReadFile(reviewPath); readErr == nil {
		var pending review
		if err := json.Unmarshal(data, &pending); err != nil {
			return fmt.Errorf("decode pending source review: %w", err)
		}
		if pending.ReviewedSHA == current.SourceSHA256 && pending.ObservedSHA == observedSHA {
			fmt.Printf("%s: source review is still pending\n", product)
			return nil
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	}
	report, err := buildReview(current, source, observedSHA, now)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(reviewPath), 0o755); err != nil {
		return err
	}
	if err := writeJSON(reviewPath, report); err != nil {
		return err
	}
	fmt.Printf("%s: source changed; review %s before updating the snapshot\n", product, reviewPath)
	return nil
}

func buildReview(current vulnerability.VersionedSnapshot, source []byte, observedSHA string, now time.Time) (review, error) {
	observed := cveID.FindAllString(string(source), -1)
	slices.Sort(observed)
	observed = slices.Compact(observed)
	if len(observed) == 0 {
		return review{}, errors.New("changed source contains no CVE IDs")
	}
	known := make([]string, 0, len(current.Advisories))
	for _, advisory := range current.Advisories {
		known = append(known, advisory.ID)
	}
	slices.Sort(known)
	return review{
		SchemaVersion: 1, Product: current.Product, Source: current.Source, RetrievedAt: now,
		ReviewedSHA: current.SourceSHA256, ObservedSHA: observedSHA, ObservedIDs: observed,
		AddedIDs: difference(observed, known), MissingIDs: difference(known, observed),
	}, nil
}

func difference(left, right []string) []string {
	var result []string
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
