package compare

import (
	"regexp"
	"slices"
	"sort"
	"strings"
)

var (
	defaultAggregateAliasPattern       = regexp.MustCompile(`\b(count|sum|avg|min|max)\(([^)]*)\)\s+as\s+([a-z_][a-z0-9_]*)\b`)
	defaultColumnAliasPattern          = regexp.MustCompile(`\b([a-z_][a-z0-9_]*)\s+as\s+([a-z_][a-z0-9_]*)\b`)
	simpleComparisonParenthesesPattern = regexp.MustCompile(
		`\(([a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)*\s*` +
			`(?:=|<>|!=|<=|>=|<|>|like|is(?:\s+not)?)\s*` +
			`(?:[a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)*|[0-9]+(?:\.[0-9]+)?|'[^']*'|true|false|null))\)`,
	)
)

func nonEmptyNames(names []string) []string {
	filtered := make([]string, 0, len(names))
	for _, name := range names {
		if name := strings.TrimSpace(name); name != "" {
			filtered = append(filtered, name)
		}
	}
	return filtered
}

func stringSetsEqual(left, right []string) bool {
	left = uniqueStringsPreserveOrder(left)
	right = uniqueStringsPreserveOrder(right)
	sort.Strings(left)
	sort.Strings(right)
	return slices.Equal(left, right)
}

func boolPtrEqual(left, right *bool) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func cloneBoolPtr(value *bool) *bool {
	if value == nil {
		return nil
	}
	return new(*value)
}

func uniqueStringsPreserveOrder(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

// nullsDistinctEqual compares two NULLS [NOT] DISTINCT states, reading an
// unset value as NULLS DISTINCT rather than as a third state.
//
// It is not boolPtrEqual, and the difference is a convergence bug rather than
// a nicety. PostgreSQL prints the clause back only when it is NOT DISTINCT:
// pg_get_indexdef renders nothing for the default, so the reader answers nil
// for an index the author declared nulls_distinct="true" on. Compared as
// pointers, a declaration that spells its engine's own default therefore
// differs from the database forever. Measured on PostgreSQL 18.6: applying
// such a schema succeeds, the server stores exactly what was asked for, and
// every later compare reports the index as removed and added again --
// a DROP INDEX and CREATE INDEX pair that no number of applies settles
// (stokaro/ptah#2820).
//
// Reading nil as true is exact rather than lenient here. The field is only
// ever populated on targets that can spell the clause, and on those the plain
// UNIQUE they fall back to treats nulls as distinct; the renderer refuses the
// clause outright everywhere else, including the one engine whose default
// runs the other way. See internal/nullsdistinct.Validate.
func nullsDistinctEqual(left, right *bool) bool {
	return nullsDistinctOrDefault(left) == nullsDistinctOrDefault(right)
}

// nullsDistinctOrDefault answers the null-equality a NullsDistinct field
// stands for, resolving the unset value to the default the engines that can
// spell the clause apply.
func nullsDistinctOrDefault(nullsDistinct *bool) bool {
	if nullsDistinct == nil {
		return true
	}
	return *nullsDistinct
}
