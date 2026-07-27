// Package atlaslint maps Ptah migration lint rules to Atlas-compatible
// analyzer names, diagnostic codes, and suppression selectors.
package atlaslint

import "strings"

const (
	AnalyzerConcurrentIndex = "concurrent_index"
	AnalyzerDataDependent   = "data_depend"
	AnalyzerDestructive     = "destructive"
	AnalyzerIncompatible    = "incompatible"
	AnalyzerNestedTX        = "nestedtx"
	AnalyzerPtah            = "ptah"
)

// Rule identifies one Atlas-compatible analyzer diagnostic.
type Rule struct {
	Analyzer string
	Code     string
}

// RuleForNativeCode returns the Atlas-compatible identity for a native Ptah
// rule. Rules without a proven Atlas identity remain Ptah diagnostics so
// overlapping code spaces cannot silently change their meaning.
func RuleForNativeCode(code string) Rule {
	switch code {
	case "DS101":
		return Rule{Analyzer: AnalyzerDestructive, Code: "DS102"}
	case "DS102":
		return Rule{Analyzer: AnalyzerDestructive, Code: "DS103"}
	case "DD101":
		return Rule{Analyzer: AnalyzerDataDependent, Code: "MF103"}
	default:
		return Rule{Analyzer: AnalyzerPtah, Code: code}
	}
}

// NativeSuppressionTargets translates one Atlas nolint selector into native
// Ptah rule codes or families. Unknown Atlas diagnostic codes intentionally do
// not fall through to same-named Ptah rules because the two code spaces overlap
// with different meanings.
func NativeSuppressionTargets(selector string) []string {
	switch strings.ToLower(strings.TrimSpace(selector)) {
	case AnalyzerDestructive:
		return []string{"DS", "CD"}
	case AnalyzerDataDependent:
		return []string{"DD"}
	case AnalyzerConcurrentIndex:
		return []string{"PG101", "PG103"}
	case AnalyzerIncompatible:
		return []string{"BC"}
	case AnalyzerNestedTX:
		return []string{"TX201"}
	case "ds102":
		return []string{"DS101"}
	case "ds103":
		return []string{"DS102"}
	case "mf103":
		return []string{"DD101"}
	default:
		return nil
	}
}
