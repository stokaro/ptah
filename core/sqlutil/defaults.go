package sqlutil

import "strings"

// DefaultLooksLikeExpression reports whether a DEFAULT a catalog reported is an
// EXPRESSION rather than a literal value.
//
// A catalog hands back one string for both. The distinction matters because the
// two are declared differently -- `default` takes a value and `default_expr`
// takes SQL -- and a description that put an expression in the value slot would
// render it quoted, turning `now()` into the literal text "now()".
//
// The rule is the absence of quoting: a value the server wrapped in single or
// double quotes is a literal, and anything else is SQL it will evaluate. That
// is deliberately coarse, and it is the same rule a column default and a domain
// default are both routed by -- which is why it lives here rather than beside
// either of them (stokaro/ptah#2315).
func DefaultLooksLikeExpression(defaultSQL string) bool {
	value := strings.TrimSpace(defaultSQL)
	if value == "" {
		return false
	}
	if strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) {
		return false
	}
	if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
		return false
	}
	return true
}
