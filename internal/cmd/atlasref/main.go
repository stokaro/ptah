// Command atlasref compares the committed Atlas analyzer reference with the
// published page, and reports what changed.
//
// The comparison the catalog carries is a reading of a public page, and a
// reading goes stale silently: consistency between Ptah's registry and its
// generated documentation cannot detect a check missing from both, which is
// how the catalog came to be nineteen checks behind (stokaro/ptah#2972).
//
// This command is the deliberate refresh. It is NOT part of `go test ./...`
// and no gate runs it, because a test that reaches the live Atlas website
// would fail on a network blip and pass on a rewritten page. The offline half
// -- comparing the catalog with the committed reference, code for code -- runs
// in `lintrules check` and needs no network.
//
//	go run ./internal/cmd/atlasref diff     what upstream has that the reference does not, and the reverse
//	go run ./internal/cmd/atlasref refresh  rewrite the reference from upstream, for review as a diff
//
// A fetch or parse failure exits non-zero and says so. It must never look like
// an unchanged catalog: reporting "no additions" for a page it failed to read
// is the same false green the offline check exists to refuse.
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"ptah.run/internal/lintcatalog"
)

const (
	analyzersURL = "https://atlasgo.io/lint/analyzers"
	// A page that yields fewer checks than this was not parsed. The reviewed
	// snapshot holds 77; a scrape that returns a handful has matched the wrong
	// markup, and answering "sixty removals" from it would be worse than
	// failing.
	minimumParsedChecks = 40
)

func main() {
	if len(os.Args) != 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "diff":
		if err := reportDrift(); err != nil {
			fmt.Fprintf(os.Stderr, "atlasref: %v\n", err)
			os.Exit(1)
		}
	case "refresh":
		if err := refreshReference(); err != nil {
			fmt.Fprintf(os.Stderr, "atlasref: %v\n", err)
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: atlasref diff|refresh

  diff     report what the published page has that the committed reference does
           not, and the reverse, without writing anything
  refresh  rewrite internal/lintcatalog/atlasreference.txt from the page, so the
           change arrives as a reviewable diff

A fetch or parse failure exits 1. It never reports an unchanged catalog.
`)
}

// reportDrift compares the code sets and writes nothing.
func reportDrift() error {
	_, err := compareCodeSets()
	return err
}

// refreshReference compares, then rewrites the reference when the sets differ.
func refreshReference() error {
	sets, err := compareCodeSets()
	if err != nil {
		return err
	}
	if len(sets.added) == 0 {
		fmt.Println("nothing to rewrite: the code set already matches")
		return nil
	}
	// Only the codes are rewritten from the page. A new row carries a
	// placeholder that says so, because a meaning nobody read is not a
	// reviewed meaning.
	if err := writeReference(mergeReference(sets.reference, sets.upstream, sets.added)); err != nil {
		return err
	}
	fmt.Print(`
rewrote internal/lintcatalog/atlasreference.txt. Every added row carries a
REVIEW-ME meaning: read the page, replace it, and update the reviewed-on date
in atlasreference.go. Then assess each added check and give it a catalog row --
` + "`lintrules check`" + ` fails until each one has an entry.
`)
	return nil
}

// compareCodeSets reports the reference, the page, and the codes the page has
// that the reference does not.
// compareCodeSets fetches the page, compares code sets with the committed
// reference, prints the report, and returns both sets plus the codes the page
// has that the reference does not.
// codeSets is what one comparison established.
type codeSets struct {
	reference []lintcatalog.AtlasReferenceEntry
	upstream  []lintcatalog.AtlasReferenceEntry
	added     []string
}

func compareCodeSets() (codeSets, error) {
	upstream, err := fetchChecks()
	if err != nil {
		return codeSets{}, err
	}
	reference := lintcatalog.AtlasReference()
	var added []string

	inReference := make(map[string]struct{}, len(reference))
	for _, entry := range reference {
		inReference[entry.Code] = struct{}{}
	}
	inUpstream := make(map[string]struct{}, len(upstream))
	for _, entry := range upstream {
		inUpstream[entry.Code] = struct{}{}
	}

	var removed []string
	for _, entry := range upstream {
		if _, found := inReference[entry.Code]; !found {
			added = append(added, entry.Code)
		}
	}
	for _, entry := range reference {
		if _, found := inUpstream[entry.Code]; !found {
			removed = append(removed, entry.Code)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)

	fmt.Printf("published page: %d check codes; committed reference: %d\n", len(upstream), len(reference))
	report("on the page and not in the reference", added)
	report("in the reference and not on the page", removed)
	if len(added) == 0 && len(removed) == 0 {
		fmt.Println("\nthe code set matches")
	}
	fmt.Print(`
This compares CODE SETS only. The meaning and the Atlas Pro marking beside each
code are a reading responsibility: the page is rendered prose, and a reading
that takes the text between two code mentions produced 59 false "definition
changed" reports against a reference that was in fact current. A refresh that
claimed to detect definition changes while inventing them would be worse than
one that says it does not, so this says it does not -- open the page and read
the rows for the codes named above.
`)
	return codeSets{reference: reference, upstream: upstream, added: added}, nil
}

// mergeReference keeps every reviewed row as it was and appends the added
// codes with a placeholder meaning.
//
// Rewriting the reviewed meanings from a scrape would replace text a person
// read with text nobody did, and the reviewed-on date would then be a claim
// about a review that never happened.
func mergeReference(reference, upstream []lintcatalog.AtlasReferenceEntry, added []string) []lintcatalog.AtlasReferenceEntry {
	isAdded := make(map[string]bool, len(added))
	for _, code := range added {
		isAdded[code] = true
	}
	onPage := make(map[string]bool, len(upstream))
	for _, entry := range upstream {
		onPage[entry.Code] = true
	}
	var merged []lintcatalog.AtlasReferenceEntry
	for _, entry := range reference {
		if onPage[entry.Code] {
			merged = append(merged, entry)
		}
	}
	for _, code := range added {
		merged = append(merged, lintcatalog.AtlasReferenceEntry{
			Code:    code,
			Pro:     false,
			Meaning: "REVIEW-ME read the page and replace this line",
		})
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Code < merged[j].Code })
	return merged
}

func report(title string, lines []string) {
	if len(lines) == 0 {
		return
	}
	fmt.Printf("\n%s (%d):\n", title, len(lines))
	for _, line := range lines {
		fmt.Printf("  %s\n", line)
	}
}

// checkRowPattern matches one row of the published checks table: a code cell
// and the description beside it.
var checkRowPattern = regexp.MustCompile(`(?i)\b([A-Z]{2})(\d{3})\b`)

// fetchChecks reads the published page and returns the checks it lists.
//
// It fails loudly on every path that could otherwise be mistaken for "nothing
// changed": a transport error, a non-200 status, an empty body, and a parse
// that finds implausibly few checks.
func fetchChecks() ([]lintcatalog.AtlasReferenceEntry, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, analyzersURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build the request for %s: %w", analyzersURL, err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", analyzersURL, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch %s: status %s", analyzersURL, response.Status)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", analyzersURL, err)
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("fetch %s: the body is empty", analyzersURL)
	}

	entries, err := parsePage(string(body))
	if err != nil {
		return nil, err
	}
	if len(entries) < minimumParsedChecks {
		return nil, fmt.Errorf(
			"parsed only %d checks from %s, fewer than the %d this parser requires; the page markup has "+
				"probably changed, and reporting removals from this would be worse than failing",
			len(entries), analyzersURL, minimumParsedChecks)
	}
	return entries, nil
}

// parsePage extracts the check codes and their descriptions from the page.
//
// The page is rendered HTML, so this reads the codes and the text that follows
// each one rather than modelling the table markup: the markup has changed
// before and the codes have not. The count floor above is what makes that
// tolerable -- a reading that stops working returns too few and is refused.
func parsePage(page string) ([]lintcatalog.AtlasReferenceEntry, error) {
	text := stripTags(page)
	matches := checkRowPattern.FindAllStringIndex(text, -1)
	if len(matches) == 0 {
		return nil, fmt.Errorf("found no check codes in %s", analyzersURL)
	}

	seen := make(map[string]bool)
	var entries []lintcatalog.AtlasReferenceEntry
	for i, match := range matches {
		code := text[match[0]:match[1]]
		if seen[code] {
			continue
		}
		seen[code] = true
		end := len(text)
		if i+1 < len(matches) {
			end = matches[i+1][0]
		}
		meaning := strings.TrimSpace(text[match[1]:end])
		meaning = strings.Join(strings.Fields(meaning), " ")
		if len(meaning) > 160 {
			meaning = meaning[:160]
		}
		// The meaning and the Pro marking are deliberately not derived here;
		// see run(). Only Code is trusted from this parse.
		entries = append(entries, lintcatalog.AtlasReferenceEntry{Code: code, Meaning: meaning})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Code < entries[j].Code })
	return entries, nil
}

// stripTags removes HTML tags so the codes and their descriptions read as one
// run of text.
func stripTags(page string) string {
	var out strings.Builder
	out.Grow(len(page))
	inTag := false
	for i := 0; i < len(page); i++ {
		switch {
		case page[i] == '<':
			inTag = true
			out.WriteByte(' ')
		case page[i] == '>':
			inTag = false
		case !inTag:
			out.WriteByte(page[i])
		}
	}
	return out.String()
}

// writeReference rewrites the committed snapshot, keeping its header.
func writeReference(entries []lintcatalog.AtlasReferenceEntry) error {
	const path = "internal/lintcatalog/atlasreference.txt"
	existing, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	var header strings.Builder
	for line := range strings.SplitSeq(string(existing), "\n") {
		if line != "" && !strings.HasPrefix(line, "#") {
			break
		}
		header.WriteString(line)
		header.WriteString("\n")
	}
	var body strings.Builder
	for _, entry := range entries {
		marking := "FREE"
		if entry.Pro {
			marking = "PRO"
		}
		fmt.Fprintf(&body, "%s %s %s\n", entry.Code, marking, entry.Meaning)
	}
	if err := os.WriteFile(path, []byte(header.String()+body.String()), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}
