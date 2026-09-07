package featureinventory

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Ledger is the classified package set docs/public_api.md declares.
//
// Two categories, because two different questions are asked of the same
// document. "Is this package allowed to be importable at all" is the boundary
// question, and it takes both fields. "Does this package carry a compatibility
// guarantee" is the API question, and it takes [Ledger.Stable] alone: the
// released-baseline check, the exported-doc-comment check and the site's
// stable-packages table are answers to that one.
//
// A sample package retained importable so that a published example stays
// copy-pasteable is not an embedder API and must not acquire one by sitting in
// the same list, which is what a single category would have done.
type Ledger struct {
	// Stable is the embedder API: importable, documented, and covered by the
	// compatibility guarantee the ledger's own text states.
	Stable []string
	// DocumentationOnly is the sample and fixture packages that stay
	// importable because documentation reaches them -- a godoc example an
	// embedder copies imports the fixture it runs on -- and that carry no
	// compatibility guarantee at all.
	DocumentationOnly []string
}

// Boundary is every package the ledger classifies, sorted and deduplicated.
//
// This is what the importability gate compares against. A package absent from
// it has to be behind an internal boundary, be a main package, or be a
// directory with no production source; anything else is an unclassified public
// import path, which is the condition the gate exists to report.
func (l Ledger) Boundary() []string {
	merged := make([]string, 0, len(l.Stable)+len(l.DocumentationOnly))
	merged = append(merged, l.Stable...)
	merged = append(merged, l.DocumentationOnly...)
	sort.Strings(merged)
	return merged
}

// The headings that classify. A module-path list item takes its category from
// the level-1 or level-2 heading above it, and there are exactly two headings
// that give it one.
const (
	stableHeading            = "Stable Embedder API"
	documentationOnlyHeading = "Documentation-Only Packages"
)

// atxHeading matches a level-1 or level-2 ATX heading and captures its text.
// Deeper headings do not reclassify: `### ptah.run/docs` is a subsection of the
// stable list explaining one of its entries, not a new category.
var atxHeading = regexp.MustCompile(`^#{1,2} +(.*?) *$`)

// codeFence opens or closes a fenced block. Inside one, `# something` is a
// shell comment rather than a heading, and reading it as a heading would move
// every package below the fence out of its section.
var codeFence = regexp.MustCompile("^(```|~~~)")

// ParseLedger classifies the import paths docs/public_api.md lists, and returns
// an error naming any module-path list item that no heading classifies.
//
// This is the one implementation of that recognition, and every gate that needs
// a package set reaches it through `featureinventory --list-ledger` or
// `--list-boundary`, directly or via scripts/list-public-api-packages.sh. Three
// grep pipelines used to answer this question separately, which is what
// AGENTS.md's "recognition that spans two functions belongs to one of them"
// forbids -- with the quiet failure mode that rule describes: a pattern that
// drifts by one character produces a SMALLER set, and a smaller set reports
// FEWER undocumented packages and FEWER incompatible-change findings rather than
// an error.
//
// The same failure mode is why an unclassified item is an error rather than a
// skip. Section awareness is what lets one document hold two categories, and it
// is also what lets a package fall out of both by being filed under the wrong
// heading. Skipping it would move a package off the compatibility surface and
// out of the boundary set at once, silently and in the permissive direction.
//
// List items only. A backticked package path in a prose paragraph or a heading
// is a mention, not a listing, and must not join a set -- stokaro/ptah#2246 is
// the fixture that says so.
//
// modulePath is a parameter rather than a constant so fixtures can prove the
// parser does not accept paths from a different module. An empty modulePath
// classifies nothing rather than matching every backticked list item, so the
// mistake fails closed through empty-kind instead of widening the set.
func ParseLedger(source []byte, modulePath string) (Ledger, error) {
	if modulePath == "" {
		return Ledger{}, nil
	}
	item := regexp.MustCompile("^- `(" + regexp.QuoteMeta(modulePath) + "[^`]+)`")

	var stable, documentationOnly, unclassified []string
	seen := make(map[string]bool)
	section := ""
	fenced := false
	for line := range strings.SplitSeq(strings.ReplaceAll(string(source), "\r\n", "\n"), "\n") {
		if codeFence.MatchString(line) {
			fenced = !fenced
			continue
		}
		if fenced {
			continue
		}
		if heading := atxHeading.FindStringSubmatch(line); heading != nil {
			section = heading[1]
			continue
		}
		match := item.FindStringSubmatch(line)
		if match == nil || seen[match[1]] {
			continue
		}
		seen[match[1]] = true
		switch section {
		case stableHeading:
			stable = append(stable, match[1])
		case documentationOnlyHeading:
			documentationOnly = append(documentationOnly, match[1])
		default:
			unclassified = append(unclassified, match[1])
		}
	}
	sort.Strings(stable)
	sort.Strings(documentationOnly)

	ledger := Ledger{Stable: stable, DocumentationOnly: documentationOnly}
	if len(unclassified) == 0 {
		return ledger, nil
	}
	sort.Strings(unclassified)
	return ledger, fmt.Errorf(
		"%s is listed under no classifying heading; move it under %q or %q",
		strings.Join(unclassified, ", "), "## "+stableHeading, "## "+documentationOnlyHeading)
}

// moduleDirective is the `module <path>` line of a go.mod file. The path may be
// quoted, and the directive may carry a trailing line comment.
var moduleDirective = regexp.MustCompile("^module[ \t]+(\"[^\"]+\"|[^ \t/]\\S*)")

// ModulePathOf returns the module path a go.mod file declares, or the empty
// string when it declares none.
//
// The module path is read from the manifest rather than written down here. A
// literal stops matching the day the module path moves, and because
// [ParseLedger] builds its pattern from it, the failure would be a ledger that
// lists nothing rather than an error naming the cause.
func ModulePathOf(goMod []byte) string {
	for line := range strings.SplitSeq(strings.ReplaceAll(string(goMod), "\r\n", "\n"), "\n") {
		match := moduleDirective.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		return strings.Trim(match[1], `"`)
	}
	return ""
}
