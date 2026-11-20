package atlascodes_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascodes"
)

// pinnedPair is one rule name, the dialects the Atlas code applies to (nil
// means every dialect), and the analyzer and code the compatibility surface
// prints for it.
type pinnedPair struct {
	rule     string
	dialects []string
	analyzer string
	code     string
}

// pinnedPairs is the Atlas identity of every Ptah rule that reports an Atlas
// check. It was built from the codes ptah-compat printed for each rule on
// origin/master (RuleForNativeCode and the rule catalog, joined by rule name
// and the dialects each master rule runs on), so a pair cannot move without
// this table moving with it.
//
// One pair differs from what master printed, on purpose. locking/drop-index
// is Atlas PG102, the check the Atlas analyzer list documents as an index
// dropped without CONCURRENTLY. Master printed Ptah's own PG106 for it, a code
// Atlas has never defined; Ptah's own codes are gone, and this table holds
// only Atlas codes.
var pinnedPairs = []pinnedPair{
	{rule: "destructive/drop-table", dialects: nil, analyzer: atlascodes.AnalyzerDestructive, code: "DS102"},
	{rule: "destructive/drop-column", dialects: nil, analyzer: atlascodes.AnalyzerDestructive, code: "DS103"},
	{rule: "integrity/drop-foreign-key", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "CD101"},
	{rule: "integrity/drop-check", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "CD102"},
	{rule: "integrity/drop-primary-key", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "CD103"},
	{rule: "data-dependent/add-not-null-column", dialects: nil, analyzer: atlascodes.AnalyzerDataDependent, code: "MF103"},
	{rule: "rollout/rename", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "BC101"},
	{rule: "data-dependent/add-unique-index", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "MF101"},
	{rule: "data-dependent/rebuild-index-as-unique", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "MF102"},
	{rule: "naming/schema-name", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "NM101"},
	{rule: "naming/table-name", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "NM102"},
	{rule: "naming/column-name", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "NM103"},
	{rule: "naming/index-name", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "NM104"},
	{rule: "naming/foreign-key-name", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "NM105"},
	{rule: "naming/check-name", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "NM106"},
	{rule: "security/dynamic-sql-from-value", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "SA101"},
	{rule: "rewrite/set-not-null", dialects: []string{"sqlite"}, analyzer: atlascodes.AnalyzerPtah, code: "LT101"},
	{rule: "transaction/explicit-transaction", dialects: []string{"postgres", "mysql", "mariadb", "clickhouse", "sqlite", "sqlserver", "cockroachdb", "yugabytedb"}, analyzer: atlascodes.AnalyzerPtah, code: "TX201"},
	{rule: "rollout/drop-table", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "BC103"},
	{rule: "rollout/drop-column", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "BC104"},
	{rule: "side-effect/inline-reference", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY102"},
	{rule: "rewrite/remove-enum-value", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY110"},
	{rule: "rewrite/reorder-enum-values", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY111"},
	{rule: "rewrite/insert-enum-value", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY112"},
	{rule: "rewrite/widen-enum", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY113"},
	{rule: "rewrite/remove-set-member", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY120"},
	{rule: "rewrite/reorder-set-members", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY121"},
	{rule: "rewrite/insert-set-member", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY122"},
	{rule: "rewrite/widen-set", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY123"},
	{rule: "rewrite/alter-column-type", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY130"},
	{rule: "locking/add-foreign-key", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY131"},
	{rule: "rewrite/add-primary-key", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY132"},
	{rule: "rewrite/drop-primary-key", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY133"},
	{rule: "locking/add-fulltext-index", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY134"},
	{rule: "locking/add-spatial-index", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY135"},
	{rule: "rewrite/convert-charset", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY136"},
	{rule: "rewrite/replace-primary-key", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY137"},
	{rule: "rewrite/change-engine", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY138"},
	{rule: "rewrite/change-partitioning", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY139"},
	{rule: "rewrite/add-stored-generated-column", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY140"},
	{rule: "locking/add-auto-increment-column", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY141"},
	{rule: "rewrite/add-column-before-existing", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY142"},
	{rule: "rewrite/modify-generated-column", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY143"},
	{rule: "locking/add-check", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY144"},
	{rule: "locking/enforce-check", dialects: []string{"mysql"}, analyzer: atlascodes.AnalyzerPtah, code: "MY145"},
	{rule: "destructive/drop-system-versioning", dialects: []string{"mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY146"},
	{rule: "rewrite/set-not-null", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY147"},
	{rule: "locking/create-index", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG101"},
	{rule: "transaction/concurrent-index", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG103"},
	{rule: "locking/add-primary-key", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG104"},
	{rule: "locking/add-unique-constraint", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG105"},
	{rule: "locking/drop-index", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG102"},
	{rule: "locking/partitioned-index", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG108"},
	{rule: "locking/add-exclusion-constraint", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG109"},
	{rule: "design/column-alignment", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG110"},
	{rule: "rewrite/alter-column-type", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG301"},
	{rule: "rewrite/volatile-default", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG302"},
	{rule: "locking/set-not-null", dialects: []string{"postgres", "cockroachdb", "yugabytedb"}, analyzer: atlascodes.AnalyzerPtah, code: "PG303"},
	{rule: "locking/add-primary-key-on-nullable", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG304"},
	{rule: "locking/add-check", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG305"},
	{rule: "locking/add-foreign-key", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG306"},
	{rule: "rewrite/set-persistence", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG307"},
	{rule: "locking/create-trigger", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG308"},
	{rule: "rewrite/add-stored-generated-column", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG309"},
	{rule: "rewrite/add-identity-column", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG310"},
	{rule: "rewrite/set-access-method", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG311"},
	{rule: "locking/replace-primary-key", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG312"},
	{rule: "rollout/weaken-replica-identity", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG314"},
	{rule: "design/disable-autovacuum", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG320"},
	{rule: "transaction/mixed-statements", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "TX101"},
}

// everyDialect is what a rule with no dialect restriction is resolved on: the
// empty dialect of a run that names none, and each dialect Ptah accepts.
var everyDialect = []string{
	"", "postgres", "mysql", "mariadb", "sqlite", "sqlserver", "clickhouse", "cockroachdb", "yugabytedb", "spanner", "ydb", "oracle",
}

// TestRuleFor_PinnedPairs resolves every pinned rule on every dialect it
// applies to and compares the Atlas analyzer and code with the pinned pair.
func TestRuleFor_PinnedPairs(t *testing.T) {
	for _, pair := range pinnedPairs {
		probes := map[bool][]string{true: everyDialect, false: pair.dialects}[len(pair.dialects) == 0]
		for _, dialect := range probes {
			t.Run(fmt.Sprintf("%s on %q", pair.rule, dialect), func(t *testing.T) {
				c := qt.New(t)
				got := atlascodes.RuleFor(pair.rule, dialect)
				c.Assert(got, qt.DeepEquals, atlascodes.Rule{Analyzer: pair.analyzer, Code: pair.code})
			})
		}
	}
}

// TestIdentities_EqualPinnedPairs holds the table to the pinned pairs in both
// directions, dialect lists included: an identity added, dropped or re-scoped
// without a matching edit here fails, where a resolution test alone would
// pass for the rows it happens to list.
func TestIdentities_EqualPinnedPairs(t *testing.T) {
	c := qt.New(t)

	var got, want []string
	for _, identity := range atlascodes.Identities() {
		got = append(got, renderPair(identity.Name, identity.Dialects, identity.Analyzer, identity.Code))
	}
	for _, pair := range pinnedPairs {
		want = append(want, renderPair(pair.rule, pair.dialects, pair.analyzer, pair.code))
	}

	c.Assert(slices.Sorted(slices.Values(got)), qt.DeepEquals, slices.Sorted(slices.Values(want)))
}

func renderPair(rule string, dialects []string, analyzer, code string) string {
	sorted := slices.Sorted(slices.Values(dialects))
	return fmt.Sprintf("%s|%s|%s|%s", rule, strings.Join(sorted, ","), analyzer, code)
}
