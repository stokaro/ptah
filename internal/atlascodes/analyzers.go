// Package atlascodes is the Atlas compatibility layer of lint: the table
// from Ptah rule names to the Atlas check codes they report, the Atlas
// analyzer names and the rules each one covers, the Atlas check list with
// what Ptah does about each check, and the reviewed snapshot of Atlas's own
// list that the table is gated against.
//
// Ptah's lint core identifies a rule by its name and knows no Atlas code.
// This package is the only place the two vocabularies meet. Only the
// compatibility surface (ptah-compat and internal/atlasreport) and the
// documentation generator import it; native code and the code shared with it
// must not, so a code spelled as an Atlas check can never select or print a
// native rule by accident. The Atlas and Ptah vocabularies overlap with
// different meanings: Atlas DS101 is a dropped schema, and the Ptah rule that
// reports it is destructive/drop-object.
package atlascodes

import (
	"maps"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/lintnames"
)

// The Atlas analyzers whose names the compatibility surface prints and reads
// in `atlas:nolint` comments. AnalyzerPtah groups the rules that have no
// analyzer of their own.
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
	// Analyzer is the Atlas analyzer the diagnostic belongs to.
	Analyzer string
	// Code is the Atlas check code, or the Ptah rule name for a rule Atlas
	// has no check for.
	Code string
}

// Identity is the Atlas check one Ptah rule reports, on the engines it is
// reported for.
//
// A rule name can stand for one hazard on several engines, and Atlas gives
// each engine its own code: the name rewrite/alter-column-type is Atlas PG301
// on PostgreSQL and Atlas MY130 on MySQL and MariaDB.
type Identity struct {
	// Name is the Ptah rule name.
	Name string
	// Engine is the Atlas analyzer code prefix of the engines the code applies
	// to, such as "MY", which a dialect descriptor names in
	// Lint.AtlasAnalyzer. Empty means every dialect, and the identity then
	// answers where no earlier identity of the same name did.
	Engine string
	// Analyzer is the Atlas analyzer the surface groups the diagnostic under.
	Analyzer string
	// Code is the Atlas check code.
	Code string
}

// identities lists every Ptah rule that reports an Atlas check, in the order
// of the rule catalog. [RuleFor] reads it forward and the check rows read it
// backward through [NamesFor]; [Validate] holds the two to each other and
// both to the reviewed reference, so a selector can never resolve to a rule
// that prints under a different code. Declared as an ordered slice rather
// than a map so resolution order is deterministic.
var identities = []Identity{
	{Name: "destructive/drop-table", Engine: "", Analyzer: AnalyzerDestructive, Code: "DS102"},
	{Name: "destructive/drop-column", Engine: "", Analyzer: AnalyzerDestructive, Code: "DS103"},
	{Name: "integrity/drop-foreign-key", Engine: "", Analyzer: AnalyzerPtah, Code: "CD101"},
	{Name: "integrity/drop-check", Engine: "", Analyzer: AnalyzerPtah, Code: "CD102"},
	{Name: "integrity/drop-primary-key", Engine: "", Analyzer: AnalyzerPtah, Code: "CD103"},
	{Name: "data-dependent/add-not-null-column", Engine: "", Analyzer: AnalyzerDataDependent, Code: "MF103"},
	{Name: "rollout/rename", Engine: "", Analyzer: AnalyzerPtah, Code: "BC101"},
	{Name: "data-dependent/add-unique-index", Engine: "", Analyzer: AnalyzerPtah, Code: "MF101"},
	{Name: "data-dependent/rebuild-index-as-unique", Engine: "", Analyzer: AnalyzerPtah, Code: "MF102"},
	{Name: "naming/schema-name", Engine: "", Analyzer: AnalyzerPtah, Code: "NM101"},
	{Name: "naming/table-name", Engine: "", Analyzer: AnalyzerPtah, Code: "NM102"},
	{Name: "naming/column-name", Engine: "", Analyzer: AnalyzerPtah, Code: "NM103"},
	{Name: "naming/index-name", Engine: "", Analyzer: AnalyzerPtah, Code: "NM104"},
	{Name: "naming/foreign-key-name", Engine: "", Analyzer: AnalyzerPtah, Code: "NM105"},
	{Name: "naming/check-name", Engine: "", Analyzer: AnalyzerPtah, Code: "NM106"},
	{Name: "security/dynamic-sql-from-value", Engine: "", Analyzer: AnalyzerPtah, Code: "SA101"},
	{Name: "transaction/explicit-transaction", Engine: "", Analyzer: AnalyzerPtah, Code: "TX201"},
	{Name: "rollout/drop-table", Engine: "", Analyzer: AnalyzerPtah, Code: "BC103"},
	{Name: "rollout/drop-column", Engine: "", Analyzer: AnalyzerPtah, Code: "BC104"},
	{Name: "side-effect/inline-reference", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY102"},
	{Name: "rewrite/remove-enum-value", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY110"},
	{Name: "rewrite/reorder-enum-values", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY111"},
	{Name: "rewrite/insert-enum-value", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY112"},
	{Name: "rewrite/widen-enum", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY113"},
	{Name: "rewrite/remove-set-member", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY120"},
	{Name: "rewrite/reorder-set-members", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY121"},
	{Name: "rewrite/insert-set-member", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY122"},
	{Name: "rewrite/widen-set", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY123"},
	{Name: "rewrite/alter-column-type", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY130"},
	{Name: "locking/add-foreign-key", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY131"},
	{Name: "rewrite/add-primary-key", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY132"},
	{Name: "rewrite/drop-primary-key", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY133"},
	{Name: "locking/add-fulltext-index", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY134"},
	{Name: "locking/add-spatial-index", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY135"},
	{Name: "rewrite/convert-charset", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY136"},
	{Name: "rewrite/replace-primary-key", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY137"},
	{Name: "rewrite/change-engine", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY138"},
	{Name: "rewrite/change-partitioning", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY139"},
	{Name: "rewrite/add-stored-generated-column", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY140"},
	{Name: "locking/add-auto-increment-column", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY141"},
	{Name: "rewrite/add-column-before-existing", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY142"},
	{Name: "rewrite/modify-generated-column", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY143"},
	{Name: "locking/add-check", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY144"},
	{Name: "locking/enforce-check", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY145"},
	{Name: "destructive/drop-system-versioning", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY146"},
	{Name: "rewrite/set-not-null", Engine: "MY", Analyzer: AnalyzerPtah, Code: "MY147"},
	{Name: "rewrite/set-not-null", Engine: "", Analyzer: AnalyzerPtah, Code: "LT101"},
	{Name: "locking/create-index", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG101"},
	{Name: "transaction/concurrent-index", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG103"},
	{Name: "locking/add-primary-key", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG104"},
	{Name: "locking/add-unique-constraint", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG105"},
	{Name: "locking/drop-index", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG102"},
	{Name: "locking/partitioned-index", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG108"},
	{Name: "locking/add-exclusion-constraint", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG109"},
	{Name: "design/column-alignment", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG110"},
	{Name: "rewrite/alter-column-type", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG301"},
	{Name: "rewrite/volatile-default", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG302"},
	{Name: "locking/set-not-null", Engine: "", Analyzer: AnalyzerPtah, Code: "PG303"},
	{Name: "locking/add-primary-key-on-nullable", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG304"},
	{Name: "locking/add-check", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG305"},
	{Name: "locking/add-foreign-key", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG306"},
	{Name: "rewrite/set-persistence", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG307"},
	{Name: "locking/create-trigger", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG308"},
	{Name: "rewrite/add-stored-generated-column", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG309"},
	{Name: "rewrite/add-identity-column", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG310"},
	{Name: "rewrite/set-access-method", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG311"},
	{Name: "locking/replace-primary-key", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG312"},
	{Name: "rollout/weaken-replica-identity", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG314"},
	{Name: "design/disable-autovacuum", Engine: "PG", Analyzer: AnalyzerPtah, Code: "PG320"},
	{Name: "transaction/mixed-statements", Engine: "PG", Analyzer: AnalyzerPtah, Code: "TX101"},
}

// Identities returns every identity, in table order. The slice is a copy.
func Identities() []Identity {
	return slices.Clone(identities)
}

// appliesTo reports whether the identity is reported for dialect. An empty
// dialect, a run that names none, matches every identity. The engine is read
// from the dialect's descriptor, so the table names no dialect and a new
// engine takes a code by declaring its analyzer prefix.
func (i Identity) appliesTo(dialect string) bool {
	if i.Engine == "" || dialect == "" {
		return true
	}
	described, _ := platform.Lookup(dialect)
	return described.Lint.AtlasAnalyzer == i.Engine
}

// RuleFor returns the Atlas-compatible identity for a Ptah rule on dialect.
// A rule with no Atlas check stays a Ptah diagnostic under its own name, so
// the overlapping code spaces cannot silently change its meaning. A run that
// names no dialect takes the first identity of the name. That is a decision,
// not an oversight: a finding carries no dialect, so a run that names none
// cannot tell which engine's code a shared name stands for, and
// `atlas migrate lint` always has a dev database that names one.
func RuleFor(name, dialect string) Rule {
	for _, identity := range identities {
		if identity.Name == name && identity.appliesTo(dialect) {
			return Rule{Analyzer: identity.Analyzer, Code: identity.Code}
		}
	}
	return Rule{Analyzer: AnalyzerPtah, Code: name}
}

// Mapped reports whether the table maps the rule to an Atlas check on at least
// one dialect.
func Mapped(name string) bool {
	return slices.ContainsFunc(identities, func(identity Identity) bool { return identity.Name == name })
}

// NamesFor returns the Ptah rules that report the Atlas check code, as the
// check's row names them, and nothing for a code no Ptah rule reports. A
// code selector matches the code exactly: `DS` and `D` are not families.
func NamesFor(code string) []string {
	check, ok := checkFor(code)
	if !ok {
		return nil
	}
	return slices.Clone(check.PtahRules)
}

// analyzerRules are the Ptah rules each Atlas analyzer covers.
var analyzerRules = map[string][]string{
	AnalyzerDestructive: {"data-dependent/alter-column-type",
		"dependency/drop-column-in-use",
		"destructive/drop-column",
		"destructive/drop-object",
		"destructive/drop-table",
		"destructive/remove-enum-value",
		"destructive/truncate-table",
		"integrity/drop-check",
		"integrity/drop-foreign-key",
		"integrity/drop-not-null",
		"integrity/drop-primary-key",
		"integrity/drop-untyped-constraint",
		"security/disable-row-level-security",
		"security/no-force-row-level-security"},
	AnalyzerDataDependent: {"correctness/immutable-calls-volatile",
		"data-dependent/add-not-null-column",
		"data-dependent/set-not-null"},
	AnalyzerConcurrentIndex: {"locking/create-index",
		"transaction/concurrent-index"},
	AnalyzerIncompatible: {"rollout/drop-column",
		"rollout/drop-table",
		"rollout/rename"},
	AnalyzerNestedTX: {"transaction/explicit-transaction"},
}

// AnalyzerRules translates one Atlas analyzer name into the Ptah rules that
// analyzer covers. Analyzer names mean the same thing on both command surfaces
// because they name groups of hazards, not printed codes.
func AnalyzerRules(selector string) ([]string, bool) {
	rules, ok := analyzerRules[strings.ToLower(strings.TrimSpace(selector))]
	return slices.Clone(rules), ok
}

// SuppressedByOrName is [SuppressedBy] for the default compatibility surface,
// which also honors a Ptah rule name in an `atlas:nolint` comment. The strict
// policy uses [SuppressedBy] alone: the community binary reads a word that is
// no analyzer and no check code as a selector that matches nothing, and a rule
// name is such a word there, so honoring it would be looser than the binary.
func SuppressedByOrName(selector string) []string {
	if rules := SuppressedBy(selector); len(rules) > 0 {
		return rules
	}
	if trimmed := strings.TrimSpace(selector); lintnames.WellFormed(trimmed) {
		return []string{trimmed}
	}
	return nil
}

// SuppressedBy translates one selector of an `atlas:nolint` comment, an Atlas
// analyzer name or check code, into the Ptah rule names it silences. It is
// the [ptah.run/migration/lint.AtlasSelectors] the compatibility surface hands
// to lint.
//
// A code selector names one check exactly. A selector that is no analyzer and
// no code Ptah reports resolves to no rule and is otherwise silent, which
// matches the pinned community binary: `-- atlas:nolint totally_bogus_selector`
// above a DROP COLUMN leaves its diagnostic reported and exits 1 there without
// a word about the selector.
func SuppressedBy(selector string) []string {
	if rules, ok := AnalyzerRules(selector); ok {
		return rules
	}
	return NamesFor(strings.ToUpper(strings.TrimSpace(selector)))
}

// Selectors translates one selector written in the compatibility surface's
// vocabulary into the selectors the lint core reads. An analyzer name or an
// Atlas check code becomes the Ptah rule names it covers; anything else, which
// may already be a Ptah rule name or category, comes back as written.
func Selectors(selector string) []string {
	trimmed := strings.TrimSpace(selector)
	if rules, ok := AnalyzerRules(trimmed); ok {
		return rules
	}
	if names := NamesFor(trimmed); len(names) > 0 {
		return names
	}
	return []string{selector}
}

// SelectorList is [Selectors] over a list, without repeats.
func SelectorList(selectors []string) []string {
	var out []string
	for _, selector := range selectors {
		for _, translated := range Selectors(selector) {
			if !slices.Contains(out, translated) {
				out = append(out, translated)
			}
		}
	}
	return out
}

// RuleConfigKeys translates the keys of a per-rule policy map written in the
// compatibility surface's vocabulary, as an atlas.hcl `lint` block states it,
// into the rule names the lint core reads. An entry keyed by an analyzer name
// covers every rule the analyzer does, and an entry keyed by one check code or
// by a Ptah name covers that rule alone and wins over an analyzer's.
func RuleConfigKeys[V any](configs map[string]V) map[string]V {
	translated := make(map[string]V, len(configs))
	for _, key := range slices.Sorted(maps.Keys(configs)) {
		if _, ok := AnalyzerRules(key); ok {
			for _, name := range Selectors(key) {
				translated[name] = configs[key]
			}
		}
	}
	for _, key := range slices.Sorted(maps.Keys(configs)) {
		if _, ok := AnalyzerRules(key); ok {
			continue
		}
		for _, name := range Selectors(key) {
			translated[name] = configs[key]
		}
	}
	return translated
}
