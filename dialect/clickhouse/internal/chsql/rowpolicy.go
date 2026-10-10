package chsql

import (
	"slices"

	"ptah.run/dialect/clickhouse/chschema"
)

// SameRowPolicy reports whether a declaration asks for the policy an
// observation describes: the same composition, ClickHouse's permissive default
// for an omitted one, the same users and roles, and the same filter. The
// filter is the server's spelling where a live comparison attached one, and
// the declaration's otherwise.
func SameRowPolicy(declared *chschema.DesiredRowPolicy, observed *chschema.ObservedRowPolicy) bool {
	composition := declared.Composition
	if composition == "" {
		composition = chschema.Permissive
	}
	if composition != observed.Composition || !declared.Roles.Equal(observed.Roles) {
		return false
	}
	filter := declared.Filter
	if declared.NormalizedFilter != nil {
		filter = declared.NormalizedFilter
	}
	if filter == nil || observed.Filter == nil {
		return filter == nil && observed.Filter == nil
	}
	return SameFilter(*filter, *observed.Filter)
}

// SameFilter compares two filters by tokens, ignoring parentheses that
// enclose the whole condition. The renderer writes `USING (<filter>)`, and
// measured on 26.9 the server keeps those parentheses where 24.10 drops them,
// while a policy created by hand is stored without them; removing a pair that
// encloses the whole condition never changes its meaning. Literals, case and
// order are kept, so two filters that mean the same thing written differently
// still compare different.
func SameFilter(a, b string) bool {
	return slices.Equal(unenclosed(expressionTokens(a)), unenclosed(expressionTokens(b)))
}

func unenclosed(tokens []string) []string {
	for enclosesKey(tokens) {
		tokens = tokens[1 : len(tokens)-1]
	}
	return tokens
}
