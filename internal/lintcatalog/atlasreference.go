package lintcatalog

import (
	// embed populates atlasReferenceText from atlasreference.txt below; no
	// symbol names it, so removing this import compiles and leaves the
	// reviewed snapshot empty.
	_ "embed"
	"fmt"
	"sort"
	"strings"
)

// AtlasReferenceReviewedOn is the date the committed reference snapshot was
// reviewed against the published Atlas analyzer page.
//
// It is rendered into the generated lint-rules page beside the counts, so a
// reader can see how old the comparison is rather than reading "every check"
// and assuming it is current.
const AtlasReferenceReviewedOn = "2026-09-06"

// atlasReferenceText is the reviewed snapshot of the Atlas analyzer check
// table: one line per check, with the marking and meaning that page carried.
//
// It is a file rather than a Go literal because the refresh command rewrites
// it, and a diff of one line per check is what a reviewer reads to decide
// whether an upstream change is an addition, a removal, or a reworded meaning.
//
//go:embed atlasreference.txt
var atlasReferenceText string

// AtlasReferenceEntry is one line of the reviewed snapshot.
type AtlasReferenceEntry struct {
	// Code is the Atlas check code.
	Code string
	// Pro records that the reviewed page marked the check as an Atlas Pro
	// feature.
	Pro bool
	// Meaning is what that page said the check detects, in its own words,
	// trimmed to one line.
	Meaning string
}

// AtlasReference returns the reviewed snapshot, ordered by code.
//
// The order is this package's, not the page's: a diff between two refreshes
// should show what changed, and the page's family order moves a whole block
// when one family is reordered upstream.
func AtlasReference() []AtlasReferenceEntry {
	entries, err := parseAtlasReference(atlasReferenceText)
	if err != nil {
		// The file is embedded from this package's own directory and is
		// checked by TestAtlasReference_Parses, so a parse failure here is a
		// build-time mistake rather than a runtime condition a caller can
		// handle.
		panic(fmt.Sprintf("lintcatalog: the embedded Atlas reference does not parse: %v", err))
	}
	return entries
}

// parseAtlasReference reads the snapshot format: comments and blank lines are
// skipped, and every other line is CODE, PRO or FREE, then the meaning.
//
// A malformed line is an error rather than a skipped line. A parser that
// skipped what it did not understand would answer "no additions" for a file it
// had failed to read, which is the shape this whole mechanism exists to refuse.
func parseAtlasReference(text string) ([]AtlasReferenceEntry, error) {
	var entries []AtlasReferenceEntry
	seen := make(map[string]bool)
	for number, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		fields := strings.Fields(trimmed)
		if len(fields) < 3 {
			return nil, fmt.Errorf("line %d: want CODE PRO|FREE meaning, got %q", number+1, trimmed)
		}
		code, marking := fields[0], fields[1]
		if !isAtlasCheckCode(code) {
			return nil, fmt.Errorf("line %d: %q is not a check code", number+1, code)
		}
		if seen[code] {
			return nil, fmt.Errorf("line %d: %s appears twice", number+1, code)
		}
		seen[code] = true
		if marking != "PRO" && marking != "FREE" {
			return nil, fmt.Errorf("line %d: %s marking is %q, want PRO or FREE", number+1, code, marking)
		}
		entries = append(entries, AtlasReferenceEntry{
			Code:    code,
			Pro:     marking == "PRO",
			Meaning: strings.Join(fields[2:], " "),
		})
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("the reference holds no checks")
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Code < entries[j].Code })
	return entries, nil
}

// AtlasReferenceDrift is what the catalog and the reviewed snapshot disagree
// about.
type AtlasReferenceDrift struct {
	// MissingFromCatalog are codes the snapshot has and the catalog does not:
	// the state this whole mechanism exists to detect, and the state the
	// catalog was in for nineteen checks.
	MissingFromCatalog []string
	// MissingFromReference are codes the catalog has and the snapshot does
	// not: either an upstream removal that has not been reviewed, or a code
	// invented here.
	MissingFromReference []string
	// ProDisagreements are codes both hold where the Pro marking differs.
	ProDisagreements []string
}

// Empty reports whether the catalog agrees with the reviewed snapshot.
func (d AtlasReferenceDrift) Empty() bool {
	return len(d.MissingFromCatalog) == 0 &&
		len(d.MissingFromReference) == 0 &&
		len(d.ProDisagreements) == 0
}

// Error renders the drift as one message naming every disagreement.
func (d AtlasReferenceDrift) Error() string {
	var parts []string
	if len(d.MissingFromCatalog) > 0 {
		parts = append(parts, fmt.Sprintf("in the reviewed reference and not in the catalog: %s",
			strings.Join(d.MissingFromCatalog, ", ")))
	}
	if len(d.MissingFromReference) > 0 {
		parts = append(parts, fmt.Sprintf("in the catalog and not in the reviewed reference: %s",
			strings.Join(d.MissingFromReference, ", ")))
	}
	if len(d.ProDisagreements) > 0 {
		parts = append(parts, fmt.Sprintf("Pro marking differs: %s",
			strings.Join(d.ProDisagreements, ", ")))
	}
	return strings.Join(parts, "; ")
}

// CompareAtlasReference reports how the catalog differs from the reviewed
// snapshot.
//
// It compares the code SETS, not their sizes. A count agrees with itself while
// one check is added and another dropped, and a floor on the number of covered
// rows cannot see a check that is missing from the catalog and from the page
// the catalog was built from -- which is the state stokaro/ptah#2972 found.
func CompareAtlasReference() AtlasReferenceDrift {
	reference := AtlasReference()
	inReference := make(map[string]AtlasReferenceEntry, len(reference))
	for _, entry := range reference {
		inReference[entry.Code] = entry
	}

	checks := AtlasChecks()
	inCatalog := make(map[string]AtlasCheck, len(checks))
	for _, check := range checks {
		inCatalog[check.Code] = check
	}

	var drift AtlasReferenceDrift
	for _, entry := range reference {
		check, found := inCatalog[entry.Code]
		if !found {
			drift.MissingFromCatalog = append(drift.MissingFromCatalog, entry.Code)
			continue
		}
		if check.Pro != entry.Pro {
			drift.ProDisagreements = append(drift.ProDisagreements, entry.Code)
		}
	}
	for _, check := range checks {
		if _, found := inReference[check.Code]; !found {
			drift.MissingFromReference = append(drift.MissingFromReference, check.Code)
		}
	}
	sort.Strings(drift.MissingFromCatalog)
	sort.Strings(drift.MissingFromReference)
	sort.Strings(drift.ProDisagreements)
	return drift
}

// isAtlasCheckCode reports whether text has the shape of an Atlas check code:
// uppercase ASCII letters followed by digits, as every family on that page
// spells them.
//
// It is a local answer rather than a reach into the lint package, because this
// one is about a line of the reviewed snapshot rather than about a rule this
// repository registers, and the two would not stay the same question: a rule
// of ours may carry a trailing P, and a check code never does.
func isAtlasCheckCode(text string) bool {
	letters := 0
	for letters < len(text) && text[letters] >= 'A' && text[letters] <= 'Z' {
		letters++
	}
	if letters == 0 || letters == len(text) {
		return false
	}
	for i := letters; i < len(text); i++ {
		if text[i] < '0' || text[i] > '9' {
			return false
		}
	}
	return true
}
