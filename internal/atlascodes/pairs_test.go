package atlascodes_test

import (
	"fmt"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascodes"
)

// pinnedPair is one rule name, the dialects master reported it on (nil means
// every dialect), the Atlas analyzer prefix of those engines that the table
// names instead of the dialects, and the analyzer and code the compatibility
// surface prints for it.
type pinnedPair struct {
	rule     string
	dialects []string
	engine   string
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
	{rule: "destructive/drop-table", dialects: nil, analyzer: atlascodes.AnalyzerDestructive, code: "DS102", engine: ""},
	{rule: "destructive/drop-column", dialects: nil, analyzer: atlascodes.AnalyzerDestructive, code: "DS103", engine: ""},
	{rule: "integrity/drop-foreign-key", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "CD101", engine: ""},
	{rule: "integrity/drop-check", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "CD102", engine: ""},
	{rule: "integrity/drop-primary-key", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "CD103", engine: ""},
	{rule: "data-dependent/add-not-null-column", dialects: nil, analyzer: atlascodes.AnalyzerDataDependent, code: "MF103", engine: ""},
	{rule: "rollout/rename", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "BC101", engine: ""},
	{rule: "data-dependent/add-unique-index", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "MF101", engine: ""},
	{rule: "data-dependent/rebuild-index-as-unique", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "MF102", engine: ""},
	{rule: "naming/schema-name", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "NM101", engine: ""},
	{rule: "naming/table-name", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "NM102", engine: ""},
	{rule: "naming/column-name", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "NM103", engine: ""},
	{rule: "naming/index-name", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "NM104", engine: ""},
	{rule: "naming/foreign-key-name", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "NM105", engine: ""},
	{rule: "naming/check-name", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "NM106", engine: ""},
	{rule: "security/dynamic-sql-from-value", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "SA101", engine: ""},
	{rule: "rewrite/set-not-null", dialects: []string{"sqlite"}, analyzer: atlascodes.AnalyzerPtah, code: "LT101", engine: ""},
	{rule: "transaction/explicit-transaction", dialects: []string{"postgres", "mysql", "mariadb", "clickhouse", "sqlite", "sqlserver", "cockroachdb", "yugabytedb"}, analyzer: atlascodes.AnalyzerPtah, code: "TX201", engine: ""},
	{rule: "rollout/drop-table", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "BC103", engine: ""},
	{rule: "rollout/drop-column", dialects: nil, analyzer: atlascodes.AnalyzerPtah, code: "BC104", engine: ""},
	{rule: "side-effect/inline-reference", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY102", engine: "MY"},
	{rule: "rewrite/remove-enum-value", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY110", engine: "MY"},
	{rule: "rewrite/reorder-enum-values", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY111", engine: "MY"},
	{rule: "rewrite/insert-enum-value", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY112", engine: "MY"},
	{rule: "rewrite/widen-enum", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY113", engine: "MY"},
	{rule: "rewrite/remove-set-member", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY120", engine: "MY"},
	{rule: "rewrite/reorder-set-members", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY121", engine: "MY"},
	{rule: "rewrite/insert-set-member", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY122", engine: "MY"},
	{rule: "rewrite/widen-set", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY123", engine: "MY"},
	{rule: "rewrite/alter-column-type", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY130", engine: "MY"},
	{rule: "locking/add-foreign-key", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY131", engine: "MY"},
	{rule: "rewrite/add-primary-key", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY132", engine: "MY"},
	{rule: "rewrite/drop-primary-key", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY133", engine: "MY"},
	{rule: "locking/add-fulltext-index", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY134", engine: "MY"},
	{rule: "locking/add-spatial-index", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY135", engine: "MY"},
	{rule: "rewrite/convert-charset", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY136", engine: "MY"},
	{rule: "rewrite/replace-primary-key", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY137", engine: "MY"},
	{rule: "rewrite/change-engine", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY138", engine: "MY"},
	{rule: "rewrite/change-partitioning", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY139", engine: "MY"},
	{rule: "rewrite/add-stored-generated-column", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY140", engine: "MY"},
	{rule: "locking/add-auto-increment-column", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY141", engine: "MY"},
	{rule: "rewrite/add-column-before-existing", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY142", engine: "MY"},
	{rule: "rewrite/modify-generated-column", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY143", engine: "MY"},
	{rule: "locking/add-check", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY144", engine: "MY"},
	{rule: "locking/enforce-check", dialects: []string{"mysql"}, analyzer: atlascodes.AnalyzerPtah, code: "MY145", engine: "MY"},
	{rule: "destructive/drop-system-versioning", dialects: []string{"mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY146", engine: "MY"},
	{rule: "rewrite/set-not-null", dialects: []string{"mysql", "mariadb"}, analyzer: atlascodes.AnalyzerPtah, code: "MY147", engine: "MY"},
	{rule: "locking/create-index", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG101", engine: "PG"},
	{rule: "transaction/concurrent-index", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG103", engine: "PG"},
	{rule: "locking/add-primary-key", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG104", engine: "PG"},
	{rule: "locking/add-unique-constraint", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG105", engine: "PG"},
	{rule: "locking/drop-index", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG102", engine: "PG"},
	{rule: "locking/partitioned-index", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG108", engine: "PG"},
	{rule: "locking/add-exclusion-constraint", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG109", engine: "PG"},
	{rule: "design/column-alignment", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG110", engine: "PG"},
	{rule: "rewrite/alter-column-type", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG301", engine: "PG"},
	{rule: "rewrite/volatile-default", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG302", engine: "PG"},
	{rule: "locking/set-not-null", dialects: []string{"postgres", "cockroachdb", "yugabytedb"}, analyzer: atlascodes.AnalyzerPtah, code: "PG303", engine: ""},
	{rule: "locking/add-primary-key-on-nullable", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG304", engine: "PG"},
	{rule: "locking/add-check", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG305", engine: "PG"},
	{rule: "locking/add-foreign-key", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG306", engine: "PG"},
	{rule: "rewrite/set-persistence", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG307", engine: "PG"},
	{rule: "locking/create-trigger", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG308", engine: "PG"},
	{rule: "rewrite/add-stored-generated-column", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG309", engine: "PG"},
	{rule: "rewrite/add-identity-column", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG310", engine: "PG"},
	{rule: "rewrite/set-access-method", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG311", engine: "PG"},
	{rule: "locking/replace-primary-key", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG312", engine: "PG"},
	{rule: "rollout/weaken-replica-identity", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG314", engine: "PG"},
	{rule: "design/disable-autovacuum", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "PG320", engine: "PG"},
	{rule: "transaction/mixed-statements", dialects: []string{"postgres"}, analyzer: atlascodes.AnalyzerPtah, code: "TX101", engine: "PG"},
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
// directions, engines included: an identity added, dropped or re-scoped
// without a matching edit here fails, where a resolution test alone would
// pass for the rows it happens to list.
func TestIdentities_EqualPinnedPairs(t *testing.T) {
	c := qt.New(t)

	var got, want []string
	for _, identity := range atlascodes.Identities() {
		got = append(got, renderPair(identity.Name, identity.Engine, identity.Analyzer, identity.Code))
	}
	for _, pair := range pinnedPairs {
		want = append(want, renderPair(pair.rule, pair.engine, pair.analyzer, pair.code))
	}

	c.Assert(slices.Sorted(slices.Values(got)), qt.DeepEquals, slices.Sorted(slices.Values(want)))
}

func renderPair(rule, engine, analyzer, code string) string {
	return fmt.Sprintf("%s|%s|%s|%s", rule, engine, analyzer, code)
}
