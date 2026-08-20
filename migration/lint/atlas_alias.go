package lint

import (
	"maps"
	"slices"
	"strings"
)

// atlasCodeAliases maps an Atlas analyzer code onto the Ptah rules that report
// the same hazard, for the codes where the two spellings differ.
//
// A code is the unit of suppression, of severity policy, and of "we accept this
// risk" in a review. Fourteen Atlas codes report here under a differently
// spelled Ptah one, so a user who wrote `PG301` in a config -- the code the
// Atlas documentation tells them to write -- was refused it as unknown
// (stokaro/ptah#1631).
//
// Eight of those fourteen are aliased. The other six are listed in
// [atlasCodesPtahAlsoUses] and are deliberately left alone.
//
// This is an alias rather than five new rules on purpose. A dedicated PG301
// would have to make the distinction its Atlas description makes, and the note
// beside it in the Atlas check catalog says what is missing: rewrite and lock
// analysis for PG301, the nullable-column refinement for PG304, the table-copy
// cost for MY130 and MY133. Minting a code that fires on a broader condition
// than its name promises would be worse than mapping it.
//
// internal/lintcatalog holds the same pairs as data and TestAtlasAliases...
// there checks the two agree, so an alias cannot outlive the mapping it stands
// for.
type atlasAlias struct {
	// dialects are the engines the ATLAS code is defined for. Empty means the
	// code is not engine-specific and the alias applies everywhere.
	dialects []string
	// rules are the Ptah rules that report the same hazard.
	rules []string
}

var atlasCodeAliases = map[string]atlasAlias{
	"BC102": {rules: []string{"BC101"}},
	"MF104": {rules: []string{"PG303", "LT101"}},
	"MY110": {dialects: mysqlFamily, rules: []string{"DS103", "MY101"}},
	"MY130": {dialects: mysqlFamily, rules: []string{"MY101", "DS103"}},
	"MY133": {dialects: mysqlFamily, rules: []string{"CD103"}},
	"MY136": {dialects: mysqlFamily, rules: []string{"MY101"}},
	"PG301": {dialects: []string{"postgres"}, rules: []string{"DS103"}},
	"PG304": {dialects: []string{"postgres"}, rules: []string{"PG104"}},
}

// mysqlFamily is the pair every MY-coded alias is defined for; Ptah's own MySQL
// rules list the same two.
var mysqlFamily = []string{"mysql", "mariadb"}

// atlasCodesPtahAlsoUses are the Atlas codes that are ALSO Ptah rule codes,
// meaning something else.
//
// These are deliberately not aliased, and the reason is not tidiness. Atlas
// DS103 reports under Ptah DS102, and Ptah has its own DS103 for a different
// hazard. Aliasing would make `--disable DS103` silence two rules where the
// operator asked for one, and would change what an existing config does --
// which is a worse failure than the one this table fixes. The catalog rows say
// what each spelling means; a config selects the Ptah one.
var atlasCodesPtahAlsoUses = map[string]string{
	"DS101": "Ptah DS101 is its own rule; Atlas DS101 reports under Ptah DS107",
	"DS102": "Ptah DS102 is its own rule; Atlas DS102 reports under Ptah DS101",
	"DS103": "Ptah DS103 is its own rule; Atlas DS103 reports under Ptah DS102",
	"MF103": "Ptah MF103 is its own rule; Atlas MF103 reports under Ptah DD101",
	"MY101": "Ptah MY101 is its own rule; Atlas MY101 reports under Ptah DD101",
	"PG102": "Ptah PG102 is its own rule; Atlas PG102 reports under Ptah PG106",
}

// AtlasCodesPtahAlsoUses returns the codes an alias would collide with, so the
// catalog check can hold the exclusion against the same data rather than
// against a second list.
func AtlasCodesPtahAlsoUses() map[string]string {
	out := make(map[string]string, len(atlasCodesPtahAlsoUses))
	maps.Copy(out, atlasCodesPtahAlsoUses)
	return out
}

// AtlasCodeAliases returns the alias table, for the catalog check that holds it
// against the Atlas analyzer list.
func AtlasCodeAliases() map[string][]string {
	out := make(map[string][]string, len(atlasCodeAliases))
	for code, alias := range atlasCodeAliases {
		out[code] = append([]string(nil), alias.rules...)
	}
	return out
}

// expandAtlasCodeSelectors rewrites every Atlas-spelled selector into the Ptah
// rules that report it, leaving every other entry alone. It expands every
// alias regardless of engine, which is what VALIDATION wants: a policy shared
// across engines names PG301 legitimately even while linting MySQL.
//
// The original entry is kept as well. A selector is a PREFIX, so dropping it
// would change what an entry like `PG3` selects, and an operator who wrote a
// prefix meant the prefix.
//
// Use [expandAtlasCodeSelectorsForDialect] anywhere the expansion decides what
// RUNS. Expanding unconditionally there lets a policy for one engine weaken
// another engine's checks: `--dialect mysql --disable PG301` would expand to
// DS103 and silence MySQL column-type-change findings the operator never
// mentioned (stokaro/ptah#1631).
func expandAtlasCodeSelectors(selectors []string) []string {
	return expandAtlasCodeSelectorsForDialect(selectors, "")
}

// expandAtlasCodeSelectorsForDialect expands only the aliases whose Atlas code
// is defined for dialect. An empty dialect expands everything, matching the
// rule engine's own "no configured dialect runs everything" convention.
func expandAtlasCodeSelectorsForDialect(selectors []string, dialect string) []string {
	expanded := make([]string, 0, len(selectors))
	for _, selector := range selectors {
		expanded = append(expanded, selector)
		alias, found := atlasCodeAliases[strings.ToUpper(strings.TrimSpace(selector))]
		if !found || !aliasAppliesToDialect(alias, dialect) {
			continue
		}
		expanded = append(expanded, alias.rules...)
	}
	return expanded
}

// aliasAppliesToDialect mirrors ruleAppliesToDialect: an alias with no declared
// engine applies everywhere, and an empty configured dialect accepts every
// alias.
func aliasAppliesToDialect(alias atlasAlias, dialect string) bool {
	if len(alias.dialects) == 0 || dialect == "" {
		return true
	}
	return slices.Contains(alias.dialects, dialect)
}

// ExpandAtlasRuleSelectors expands the Atlas-spelled entries in selectors into
// the Ptah rules that report them, scoped to dialect.
//
// It is exported for the schema-apply path, which builds its own enabled set
// and would otherwise disable the very rule an aliased policy entry asked for.
func ExpandAtlasRuleSelectors(selectors []string, dialect string) []string {
	return expandAtlasCodeSelectorsForDialect(selectors, dialect)
}

// AtlasCodeFor returns the Atlas code a Ptah rule stands in for, so a report
// can name the code a reader greps for beside the one Ptah raised.
//
// A Ptah rule can stand in for more than one Atlas code -- DS103 covers both
// PG301 and part of MY130 -- so the answer is a list, ordered for stable output.
func AtlasCodeFor(ptahRule string) []string {
	var codes []string
	for code, alias := range atlasCodeAliases {
		if slices.Contains(alias.rules, ptahRule) {
			codes = append(codes, code)
		}
	}
	slices.Sort(codes)
	return codes
}
