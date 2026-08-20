package lint

import (
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
var atlasCodeAliases = map[string][]string{
	"BC102": {"BC101"},
	"MF104": {"PG303", "LT101"},
	"MY110": {"DS103", "MY101"},
	"MY130": {"MY101", "DS103"},
	"MY133": {"CD103"},
	"MY136": {"MY101"},
	"PG301": {"DS103"},
	"PG304": {"PG104"},
}

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
	for code, reason := range atlasCodesPtahAlsoUses {
		out[code] = reason
	}
	return out
}

// AtlasCodeAliases returns the alias table, for the catalog check that holds it
// against the Atlas analyzer list.
func AtlasCodeAliases() map[string][]string {
	out := make(map[string][]string, len(atlasCodeAliases))
	for code, rules := range atlasCodeAliases {
		out[code] = append([]string(nil), rules...)
	}
	return out
}

// expandAtlasCodeSelectors rewrites every Atlas-spelled selector into the Ptah
// rules that report it, leaving every other entry alone.
//
// The original entry is kept as well. A selector is a PREFIX, so dropping it
// would change what an entry like `PG3` selects, and an operator who wrote a
// prefix meant the prefix.
func expandAtlasCodeSelectors(selectors []string) []string {
	expanded := make([]string, 0, len(selectors))
	for _, selector := range selectors {
		expanded = append(expanded, selector)
		for _, rule := range atlasCodeAliases[strings.ToUpper(strings.TrimSpace(selector))] {
			expanded = append(expanded, rule)
		}
	}
	return expanded
}

// AtlasCodeFor returns the Atlas code a Ptah rule stands in for, so a report
// can name the code a reader greps for beside the one Ptah raised.
//
// A Ptah rule can stand in for more than one Atlas code -- DS103 covers both
// PG301 and part of MY130 -- so the answer is a list, ordered for stable output.
func AtlasCodeFor(ptahRule string) []string {
	var codes []string
	for code, rules := range atlasCodeAliases {
		for _, rule := range rules {
			if rule == ptahRule {
				codes = append(codes, code)
				break
			}
		}
	}
	slices.Sort(codes)
	return codes
}
