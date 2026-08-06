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

// Target is one resolved nolint selector.
//
// A target with Family set suppresses every native rule code that starts with
// Value, which is how analyzer names work: "destructive" owns the whole DS and
// CD families. A target without it suppresses exactly the rule whose code
// equals Value, which is how code selectors work: the community binary silences
// nothing for `-- atlas:nolint DS` or `-- atlas:nolint D`, so a code selector
// must never widen into a family (measured against atlas community version
// v1.3.0; both leave the DS103 diagnostic reported and exit 1).
type Target struct {
	Value  string
	Family bool
}

// CodeTarget suppresses exactly one native rule code.
func CodeTarget(code string) Target { return Target{Value: code} }

// FamilyTarget suppresses every native rule code starting with prefix. The
// empty prefix is every rule, which is what a bare nolint directive means.
func FamilyTarget(prefix string) Target { return Target{Value: prefix, Family: true} }

// Matches reports whether this target suppresses the given native rule code.
func (t Target) Matches(code string) bool {
	if t.Family {
		return strings.HasPrefix(code, t.Value)
	}
	return code == t.Value
}

// MatchesEveryRule reports whether this target suppresses every rule, the
// meaning of a nolint directive written without a selector.
func (t Target) MatchesEveryRule() bool {
	return t.Family && t.Value == ""
}

// atlasIdentities lists every native rule whose printed code differs from its
// native code on the compatibility surface. It is the single source of truth
// for both directions: RuleForNativeCode reads it forward and
// NativeSuppressionTargets reads it backward, so a selector can never resolve
// to a rule that prints under a different code.
//
// Declared as an ordered slice rather than a map so resolution order is
// deterministic.
var atlasIdentities = []struct {
	NativeCode string
	Rule       Rule
}{
	{NativeCode: "DS101", Rule: Rule{Analyzer: AnalyzerDestructive, Code: "DS102"}},
	{NativeCode: "DS102", Rule: Rule{Analyzer: AnalyzerDestructive, Code: "DS103"}},
	{NativeCode: "DD101", Rule: Rule{Analyzer: AnalyzerDataDependent, Code: "MF103"}},
}

// RuleForNativeCode returns the Atlas-compatible identity for a native Ptah
// rule. Rules without a proven Atlas identity remain Ptah diagnostics so
// overlapping code spaces cannot silently change their meaning.
func RuleForNativeCode(code string) Rule {
	for _, identity := range atlasIdentities {
		if identity.NativeCode == code {
			return identity.Rule
		}
	}
	return Rule{Analyzer: AnalyzerPtah, Code: code}
}

// AnalyzerSuppressionTargets translates one Atlas analyzer name into the native
// Ptah rule families that analyzer owns. Analyzer names mean the same thing on
// both command surfaces because they name families, not printed codes.
func AnalyzerSuppressionTargets(selector string) ([]Target, bool) {
	switch strings.ToLower(strings.TrimSpace(selector)) {
	case AnalyzerDestructive:
		return []Target{FamilyTarget("DS"), FamilyTarget("CD")}, true
	case AnalyzerDataDependent:
		return []Target{FamilyTarget("DD")}, true
	case AnalyzerConcurrentIndex:
		return []Target{CodeTarget("PG101"), CodeTarget("PG103")}, true
	case AnalyzerIncompatible:
		return []Target{FamilyTarget("BC")}, true
	case AnalyzerNestedTX:
		return []Target{CodeTarget("TX201")}, true
	}
	return nil, false
}

// NativeSuppressionTargets translates one Atlas nolint selector into native
// Ptah suppression targets for the Atlas-compatible surface.
//
// A code selector names the code that surface prints, so it resolves to every
// native rule whose Atlas identity is that code — both producers of a shared
// printed code, never one of them. Selectors naming a code the surface cannot
// print (an Atlas code Ptah remaps away from, such as DS101) resolve to
// nothing rather than falling through to the same-named Ptah rule, because the
// two code spaces overlap with different meanings.
//
// Unknown selectors resolve to a target that matches no rule and are otherwise
// silent, matching atlas community version v1.3.0: `-- atlas:nolint
// totally_bogus_selector` above a DROP COLUMN leaves DS103 reported and exits 1
// there without printing anything about the selector.
func NativeSuppressionTargets(selector string) []Target {
	if targets, ok := AnalyzerSuppressionTargets(selector); ok {
		return targets
	}
	code := strings.ToUpper(strings.TrimSpace(selector))
	if code == "" {
		return nil
	}
	var targets []Target
	remapped := false
	for _, identity := range atlasIdentities {
		if identity.Rule.Code == code {
			targets = append(targets, CodeTarget(identity.NativeCode))
		}
		if identity.NativeCode == code {
			// The native rule with this code prints under another one, so the
			// compatibility surface never emits this code for it.
			remapped = true
		}
	}
	if !remapped {
		targets = append(targets, CodeTarget(code))
	}
	return targets
}
