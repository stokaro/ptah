package featureinventory

import (
	"regexp"
	"sort"
	"strings"
)

// ledgerItem is the stable-embedder ledger's list-item shape.
//
// List items only. A backticked package path in a prose paragraph is a mention,
// not a listing, and must not join the set -- stokaro/ptah#2246 is the fixture
// that says so.
var ledgerItem = regexp.MustCompile("^- `(" + regexp.QuoteMeta(ModulePath) + "[^`]+)`")

// LedgerPackages returns the import paths docs/public_api.md lists as the
// stable embedder API, sorted and deduplicated.
//
// This is the ONE implementation of that recognition. scripts/check-public-api.sh
// scraped the ledger with a grep pipeline of its own and this generator needed
// the identical set; two implementations of one rule is what AGENTS.md's
// "recognition that spans two functions belongs to one of them" forbids, and the
// failure mode here is quiet -- a pattern that drifts by one character produces
// a SMALLER set, and a smaller inventory looks fine. The gate now calls
// `featureinventory --list-ledger` instead of scraping.
func LedgerPackages(source []byte) []string {
	seen := make(map[string]bool)
	var paths []string
	for line := range strings.SplitSeq(strings.ReplaceAll(string(source), "\r\n", "\n"), "\n") {
		match := ledgerItem.FindStringSubmatch(line)
		if match == nil || seen[match[1]] {
			continue
		}
		seen[match[1]] = true
		paths = append(paths, match[1])
	}
	sort.Strings(paths)
	return paths
}
