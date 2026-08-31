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
