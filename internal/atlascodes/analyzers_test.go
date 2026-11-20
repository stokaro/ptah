package atlascodes_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascodes"
)

func TestRuleFor(t *testing.T) {
	rows := []struct {
		name    string
		rule    string
		dialect string
		want    atlascodes.Rule
	}{
		{name: "table drop", rule: "destructive/drop-table", want: atlascodes.Rule{Analyzer: atlascodes.AnalyzerDestructive, Code: "DS102"}},
		{name: "column drop", rule: "destructive/drop-column", want: atlascodes.Rule{Analyzer: atlascodes.AnalyzerDestructive, Code: "DS103"}},
		{name: "data dependent", rule: "data-dependent/add-not-null-column", want: atlascodes.Rule{Analyzer: atlascodes.AnalyzerDataDependent, Code: "MF103"}},
		{name: "an Atlas check under the ptah analyzer", rule: "locking/drop-index", want: atlascodes.Rule{Analyzer: atlascodes.AnalyzerPtah, Code: "PG102"}},
		{name: "one name, PostgreSQL's check", rule: "rewrite/alter-column-type", dialect: "postgres", want: atlascodes.Rule{Analyzer: atlascodes.AnalyzerPtah, Code: "PG301"}},
		{name: "one name, MySQL's check", rule: "rewrite/alter-column-type", dialect: "mysql", want: atlascodes.Rule{Analyzer: atlascodes.AnalyzerPtah, Code: "MY130"}},
		{name: "one name, MariaDB's check", rule: "rewrite/alter-column-type", dialect: "mariadb", want: atlascodes.Rule{Analyzer: atlascodes.AnalyzerPtah, Code: "MY130"}},
		{name: "a rule with no Atlas check keeps its name", rule: "data-dependent/alter-column-type", want: atlascodes.Rule{Analyzer: atlascodes.AnalyzerPtah, Code: "data-dependent/alter-column-type"}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlascodes.RuleFor(row.rule, row.dialect), qt.DeepEquals, row.want)
		})
	}
}

// TestSuppressedBy pins the resolution of one `atlas:nolint` selector into
// Ptah rule names.
//
// The "DS" and "D" rows pin exactness against the pinned community binary:
// with `-- atlas:nolint DS` or `-- atlas:nolint D` above `ALTER TABLE t DROP
// COLUMN legacy;` it reports DS103 and exits 1, so a code selector must never
// widen into a family. Atlas's DS101 is a dropped schema, so it selects
// destructive/drop-object and not the rule Ptah once spelled DS101.
func TestSuppressedBy(t *testing.T) {
	rows := []struct {
		name     string
		selector string
		want     []string
	}{
		{name: "destructive analyzer", selector: "destructive", want: []string{
			"data-dependent/alter-column-type", "dependency/drop-column-in-use", "destructive/drop-column", "destructive/drop-object",
			"destructive/drop-table", "destructive/remove-enum-value", "destructive/truncate-table", "integrity/drop-check",
			"integrity/drop-foreign-key", "integrity/drop-not-null", "integrity/drop-primary-key", "integrity/drop-untyped-constraint",
			"security/disable-row-level-security", "security/no-force-row-level-security",
		}},
		{name: "data dependent analyzer", selector: "data_depend", want: []string{
			"correctness/immutable-calls-volatile", "data-dependent/add-not-null-column", "data-dependent/set-not-null",
		}},
		{name: "concurrent index analyzer", selector: "concurrent_index", want: []string{"locking/create-index", "transaction/concurrent-index"}},
		{name: "incompatible analyzer", selector: "incompatible", want: []string{"rollout/drop-column", "rollout/drop-table", "rollout/rename"}},
		{name: "nested transaction analyzer", selector: "nestedtx", want: []string{"transaction/explicit-transaction"}},
		{name: "Atlas table drop", selector: "DS102", want: []string{"destructive/drop-table"}},
		{name: "Atlas schema drop", selector: "DS101", want: []string{"destructive/drop-object"}},
		{name: "Atlas column drop", selector: "DS103", want: []string{"destructive/drop-column"}},
		{name: "a lower-case code is read as the community binary reads it", selector: "ds102", want: []string{"destructive/drop-table"}},
		{name: "family prefix is not a code", selector: "DS", want: nil},
		{name: "single letter is not a code", selector: "D", want: nil},
		{name: "unknown selector resolves to nothing", selector: "totally_bogus_selector", want: nil},
		{name: "a Ptah rule name is not an Atlas selector", selector: "destructive/drop-table", want: nil},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlascodes.SuppressedBy(row.selector), qt.DeepEquals, row.want)
		})
	}
}

func TestSelectors(t *testing.T) {
	c := qt.New(t)

	c.Assert(atlascodes.Selectors("BC102"), qt.DeepEquals, []string{"rollout/rename"})
	c.Assert(atlascodes.Selectors("incompatible"), qt.DeepEquals, []string{"rollout/drop-column", "rollout/drop-table", "rollout/rename"})
	// Anything else comes back as written, so a Ptah name or category reaches
	// the lint core, which judges it.
	c.Assert(atlascodes.Selectors("locking"), qt.DeepEquals, []string{"locking"})
	c.Assert(atlascodes.Selectors("destructive/drop-table"), qt.DeepEquals, []string{"destructive/drop-table"})
	c.Assert(atlascodes.SelectorList([]string{"BC102", "rollout/rename", "locking"}), qt.DeepEquals, []string{"rollout/rename", "locking"})
}

// TestRuleConfigKeys pins the precedence of a translated policy: an entry for
// one Atlas check or one Ptah name wins over the entry of an analyzer that
// covers it, whatever order the keys sort in.
func TestRuleConfigKeys(t *testing.T) {
	c := qt.New(t)

	got := atlascodes.RuleConfigKeys(map[string]string{
		"incompatible":   "analyzer",
		"BC102":          "check",
		"rollout/rename": "name",
	})

	c.Assert(got, qt.DeepEquals, map[string]string{
		"rollout/rename":      "name",
		"rollout/drop-table":  "analyzer",
		"rollout/drop-column": "analyzer",
	})
}
