package featureinventory

import (
	"regexp"
	"sort"
	"strings"
)

// LedgerPackages returns the import paths docs/public_api.md lists as the
// stable embedder API, sorted and deduplicated.
//
// This is the one implementation of that recognition, and every gate that needs
// the set reaches it: scripts/check-public-api.sh calls
// `featureinventory --list-ledger`, scripts/check-public-api-snapshot.sh's
// `--list-packages` mode forwards to the same command, and
// scripts/check-public-api-docs-sync.sh, scripts/check-exported-docs.sh and
// scripts/check-public-api-released.sh read the ledger through that mode. Three
// grep pipelines used to answer this question separately, which is what
// AGENTS.md's "recognition that spans two functions belongs to one of them"
// forbids -- with the quiet failure mode that rule describes: a pattern that
// drifts by one character produces a SMALLER set, and a smaller set reports
// FEWER undocumented packages and FEWER incompatible-change findings rather than
// an error.
//
// List items only. A backticked package path in a prose paragraph or a heading
// is a mention, not a listing, and must not join the set -- stokaro/ptah#2246 is
// the fixture that says so.
//
// modulePath is a parameter rather than a constant because the callers do not
// share one module: internal/apiguard drives this through --list-packages
// against a throwaway fixture module, and a literal here would answer for the
// wrong one. An empty modulePath returns nothing rather than matching every
// backticked list item, so the mistake fails closed through empty-kind instead
// of widening the set.
func LedgerPackages(source []byte, modulePath string) []string {
	if modulePath == "" {
		return nil
	}
	item := regexp.MustCompile("^- `(" + regexp.QuoteMeta(modulePath) + "[^`]+)`")

	seen := make(map[string]bool)
	var paths []string
	for line := range strings.SplitSeq(strings.ReplaceAll(string(source), "\r\n", "\n"), "\n") {
		match := item.FindStringSubmatch(line)
		if match == nil || seen[match[1]] {
			continue
		}
		seen[match[1]] = true
		paths = append(paths, match[1])
	}
	sort.Strings(paths)
	return paths
}

// moduleDirective is the `module <path>` line of a go.mod file. The path may be
// quoted, and the directive may carry a trailing line comment.
var moduleDirective = regexp.MustCompile("^module[ \t]+(\"[^\"]+\"|[^ \t/]\\S*)")

// ModulePathOf returns the module path a go.mod file declares, or the empty
// string when it declares none.
//
// The module path is read from the manifest rather than written down here. A
// literal stops matching the day the module path moves, and because
// [LedgerPackages] builds its pattern from it, the failure would be a ledger
// that lists nothing rather than an error naming the cause.
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
