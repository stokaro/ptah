package main

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	neturl "net/url"
	"regexp"
	"slices"
	"strings"
	"time"
)

// guideURL is the Security Update Guide's list of every SQL Server product
// row: one row per advisory and product, each naming the KB articles that fix
// it and their builds. Microsoft serves it in pages of 500 that link the next
// one.
const guideURL = "https://api.msrc.microsoft.com/sug/v2.0/en-US/affectedProduct?$filter=productFamily%20eq%20%27SQL%20Server%27"

// guidePageLimit bounds how many pages the check follows. The list holds a
// few thousand rows, and a next link that never ends is a broken response.
const guidePageLimit = 64

// cvrfIndexURL lists Microsoft's monthly security releases, one CVRF
// document each, named by month such as "2026-Dec".
const cvrfIndexURL = "https://api.msrc.microsoft.com/cvrf/v3.0/updates"

// sqlServerProduct is a SQL Server database engine row of the guide, any
// version. Reporting Services, Big Data Clusters, the Linux container image,
// drivers and tools are products of their own, out of scope.
var sqlServerProduct = regexp.MustCompile(`^Microsoft SQL Server [0-9]{4}\b`)

// guidePage is the part of a guide response the check reads.
type guidePage struct {
	Count    *int        `json:"@odata.count"`
	NextLink string      `json:"@odata.nextLink"`
	Value    *[]guideRow `json:"value"`
}

type guideRow struct {
	ID         string `json:"id"`
	CVE        string `json:"cveNumber"`
	Product    string `json:"product"`
	BaseScore  string `json:"baseScore"`
	KBArticles []struct {
		ArticleName      string `json:"articleName"`
		FixedBuildNumber string `json:"fixedBuildNumber"`
	} `json:"kbArticles"`
}

// guideFix is one KB article and the build it ships.
type guideFix struct {
	KB    string `json:"kb"`
	Build string `json:"build"`
}

// guideEntry is the reviewed part of one engine row: what changes when
// Microsoft adds, revises or withdraws an advisory, its product or its fix,
// and nothing that changes on each request.
type guideEntry struct {
	CVE     string     `json:"cve"`
	Product string     `json:"product"`
	CVSS    string     `json:"cvss,omitempty"`
	Fixes   []guideFix `json:"fixes"`
}

// readGuide fetches every page of the guide's SQL Server list, following each
// page's next link, and returns them concatenated in order. A next link must
// stay on the guide's API and keep the first page's filter.
func readGuide(fetchPage fetcher, first string) ([]byte, error) {
	want, err := guideFilter(first)
	if err != nil {
		return nil, err
	}
	var pages bytes.Buffer
	url := first
	for range guidePageLimit {
		page, err := fetchPage(url)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", url, err)
		}
		var header struct {
			NextLink string `json:"@odata.nextLink"`
		}
		if err := json.Unmarshal(page, &header); err != nil {
			return nil, fmt.Errorf("decode %s: %w", url, err)
		}
		pages.Write(page)
		if header.NextLink == "" {
			return pages.Bytes(), nil
		}
		if filter, err := guideFilter(header.NextLink); err != nil || filter != want {
			return nil, fmt.Errorf("the guide links a next page %q that is not the same list on its API", header.NextLink)
		}
		url = header.NextLink
	}
	return nil, fmt.Errorf("the guide links more than %d pages", guidePageLimit)
}

// guideFilter returns the list filter of a guide URL, refusing any address
// that is not the guide's API.
func guideFilter(url string) (string, error) {
	parsed, err := neturl.Parse(url)
	if err != nil || parsed.Scheme != "https" || parsed.Host != "api.msrc.microsoft.com" ||
		!strings.HasPrefix(parsed.Path, "/sug/v2.0/") || !strings.HasSuffix(parsed.Path, "/affectedProduct") {
		return "", fmt.Errorf("%q is not a Security Update Guide list", url)
	}
	return parsed.Query().Get("$filter"), nil
}

// guideContent returns the engine rows of the concatenated guide pages,
// sorted and stripped to guideEntry, as JSON. The order of rows and of a
// row's KB articles is not content. It refuses a list whose pages
// do not add up to the row count the first page states, or that repeats a
// row: rows that moved between pages while they were read.
func guideContent(pages []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(pages))
	var count *int
	seen := make(map[string]bool)
	entries := make([]guideEntry, 0)
	for index := 0; ; index++ {
		var page guidePage
		err := decoder.Decode(&page)
		if errors.Is(err, io.EOF) {
			break
		}
		switch {
		case err != nil:
			return nil, fmt.Errorf("decode guide page %d: %w", index+1, err)
		case page.Value == nil:
			return nil, fmt.Errorf("guide page %d has no value list", index+1)
		case index == 0:
			count = page.Count
		}
		for _, row := range *page.Value {
			// An advisory is named by its CVE, or by Microsoft's own ID, such
			// as ADV180002, for guidance that has none.
			if row.ID == "" || seen[row.ID] || row.CVE == "" {
				return nil, fmt.Errorf("guide page %d repeats a row or holds one with no ID or advisory: %q", index+1, row.ID)
			}
			seen[row.ID] = true
			if sqlServerProduct.MatchString(row.Product) {
				entries = append(entries, entryOf(row))
			}
		}
	}
	if count == nil || *count != len(seen) {
		return nil, fmt.Errorf("the guide's pages hold %d rows, which is not the row count its first page states", len(seen))
	}
	if len(entries) == 0 {
		return nil, errors.New("the guide lists no SQL Server engine row")
	}
	slices.SortFunc(entries, func(a, b guideEntry) int {
		return cmp.Or(cmp.Compare(a.CVE, b.CVE), cmp.Compare(a.Product, b.Product), cmp.Compare(fixesKey(a), fixesKey(b)), cmp.Compare(a.CVSS, b.CVSS))
	})
	return json.MarshalIndent(entries, "", " ")
}

func entryOf(row guideRow) guideEntry {
	entry := guideEntry{CVE: row.CVE, Product: row.Product, CVSS: strings.TrimSpace(row.BaseScore), Fixes: make([]guideFix, 0, len(row.KBArticles))}
	for _, article := range row.KBArticles {
		entry.Fixes = append(entry.Fixes, guideFix{KB: strings.TrimSpace(article.ArticleName), Build: strings.TrimSpace(article.FixedBuildNumber)})
	}
	slices.SortFunc(entry.Fixes, func(a, b guideFix) int { return cmp.Or(cmp.Compare(a.KB, b.KB), cmp.Compare(a.Build, b.Build)) })
	return entry
}

func fixesKey(entry guideEntry) string {
	parts := make([]string, 0, len(entry.Fixes))
	for _, fix := range entry.Fixes {
		parts = append(parts, fix.KB+"="+fix.Build)
	}
	return strings.Join(parts, ",")
}

// patchTuesday returns Microsoft's next monthly security release from today
// on: the second Tuesday of this month, or of the next one when that has
// passed. A release due today counts, since it is published during the day.
func patchTuesday(now time.Time) time.Time {
	year, month, day := now.UTC().Date()
	today := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	second := func(offset int) time.Time {
		first := time.Date(year, month+time.Month(offset), 1, 0, 0, 0, 0, time.UTC)
		return first.AddDate(0, 0, (int(time.Tuesday)-int(first.Weekday())+7)%7+7)
	}
	if release := second(0); !release.Before(today) {
		return release
	}
	return second(1)
}

// confirmPublishedRelease refuses a next release later than the snapshot's
// valid_until unless the CVRF index lists the release of valid_until's month.
// The check reads the guide whole, but a guide read before Microsoft
// published that release would move valid_until past advisories nobody
// reviewed.
func confirmPublishedRelease(validUntil, next time.Time, index []byte) error {
	if validUntil.IsZero() || !next.After(validUntil) {
		return nil
	}
	var releases struct {
		Value []struct {
			ID string `json:"ID"`
		} `json:"value"`
	}
	if err := json.Unmarshal(index, &releases); err != nil {
		return fmt.Errorf("decode the security release index: %w", err)
	}
	month := validUntil.UTC().Format("2006-Jan")
	for _, release := range releases.Value {
		if release.ID == month {
			return nil
		}
	}
	return fmt.Errorf("the security release index lists no %s release, so the release due on %s is unread",
		month, validUntil.UTC().Format(time.DateOnly))
}
