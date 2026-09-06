package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// selectorFS is one up file with no down, so it reports MF101P (missing
// down) and, on MySQL, MF101 (a unique index over existing rows): the pair
// a prefix selector cannot tell apart.
var selectorFS = map[string]string{
	"0000000001_index.up.sql": "CREATE UNIQUE INDEX u ON t (a);\n",
}

func lintSelectorFS(c *qt.C, opts lint.Options) []lint.Finding {
	c.Helper()
	opts.Dialect = "mysql"
	opts.DirFormat = migrationfile.DirFormatPtah
	findings, err := lint.LintFS(fixture(selectorFS), opts)
	c.Assert(err, qt.IsNil)
	return findings
}

// TestSelectors_ACodeSelectsItselfAlone pins the rule a selector follows: a
// registered code names one rule, a prefix names every rule under it.
func TestSelectors_ACodeSelectsItselfAlone(t *testing.T) {
	tests := []struct {
		name     string
		disabled []string
		want     []string
	}{
		{name: "nothing disabled", disabled: nil, want: []string{"MF101P", "MF101"}},
		{name: "the check alone", disabled: []string{"MF101"}, want: []string{"MF101P"}},
		{name: "the file-form rule alone", disabled: []string{"MF101P"}, want: []string{"MF101"}},
		{name: "the family", disabled: []string{"MF"}, want: make([]string, 0)},
		{name: "a range that is not a code", disabled: []string{"MF10"}, want: make([]string, 0)},
		{name: "both by code", disabled: []string{"MF101", "MF101P"}, want: make([]string, 0)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			findings := lintSelectorFS(c, lint.Options{Disabled: test.disabled})
			c.Assert(rulesOf(findings), qt.DeepEquals, test.want)
		})
	}
}

// TestSelectors_ARulesKeyGovernsItsCodeAlone: a `rules:` entry keyed by a
// code changes that rule's severity and nothing else's; a prefix key still
// governs every rule under it.
func TestSelectors_ARulesKeyGovernsItsCodeAlone(t *testing.T) {
	tests := []struct {
		name    string
		configs map[string]lint.RuleConfig
		want    map[string]lint.Severity
	}{
		{
			name:    "the check alone",
			configs: map[string]lint.RuleConfig{"MF101": {Severity: lint.SeverityError}},
			want:    map[string]lint.Severity{"MF101": lint.SeverityError, "MF101P": lint.SeverityWarning},
		},
		{
			name:    "the file-form rule alone",
			configs: map[string]lint.RuleConfig{"MF101P": {Severity: lint.SeverityInfo}},
			want:    map[string]lint.Severity{"MF101": lint.SeverityWarning, "MF101P": lint.SeverityInfo},
		},
		{
			name:    "the family",
			configs: map[string]lint.RuleConfig{"MF": {Severity: lint.SeverityError}},
			want:    map[string]lint.Severity{"MF101": lint.SeverityError, "MF101P": lint.SeverityError},
		},
		{
			name:    "a code beats the family it is in",
			configs: map[string]lint.RuleConfig{"MF": {Severity: lint.SeverityError}, "MF101P": {Severity: lint.SeverityInfo}},
			want:    map[string]lint.Severity{"MF101": lint.SeverityError, "MF101P": lint.SeverityInfo},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			findings := lintSelectorFS(c, lint.Options{RuleConfigs: test.configs})
			got := make(map[string]lint.Severity)
			for _, finding := range findings {
				got[finding.Rule] = finding.Severity
			}
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestSelectors_ANoLintDirectiveFollowsTheSameRule: `ptah:nolint MF101`
// silences the check on that statement and leaves the file-form rule, which
// has no statement to sit under, reported.
func TestSelectors_ANoLintDirectiveFollowsTheSameRule(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "the code silences the check alone",
			sql:  "-- ptah:nolint MF101\nCREATE UNIQUE INDEX u ON t (a);\n",
			want: []string{"MF101P"},
		},
		{
			name: "the family silences the check too",
			sql:  "-- ptah:nolint MF\nCREATE UNIQUE INDEX u ON t (a);\n",
			want: []string{"MF101P"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			findings, err := lint.LintFS(fixture(map[string]string{"0000000001_index.up.sql": test.sql}), lint.Options{
				Dialect:   "mysql",
				DirFormat: migrationfile.DirFormatPtah,
			})
			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(findings), qt.DeepEquals, test.want)
		})
	}
}

// TestSelectors_ANoLintCodeLeavesADeclaredNeighborReported: a declared rule
// whose code extends a built-in one (DS102X beside DS102) is a statement rule
// too, so this is the pair that tells a code target from a family target on
// one statement: `ptah:nolint DS102` leaves DS102X reported and the family
// prefix `DS` silences both. The second declared rule, DS102XY, is there to
// show the directive is judged against the run's own rule set: DS102X is a
// code in this run, so `ptah:nolint DS102X` leaves DS102XY reported.
func TestSelectors_ANoLintCodeLeavesADeclaredNeighborReported(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "the built-in code alone",
			sql:  "-- ptah:nolint DS102\nALTER TABLE users DROP COLUMN legacy;\n",
			want: []string{"DS102X", "DS102XY"},
		},
		{
			name: "the declared code alone",
			sql:  "-- ptah:nolint DS102X\nALTER TABLE users DROP COLUMN legacy;\n",
			want: []string{"DS102", "DS102XY"},
		},
		{
			name: "the family",
			sql:  "-- ptah:nolint DS\nALTER TABLE users DROP COLUMN legacy;\n",
			want: make([]string, 0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			findings, err := lint.LintFS(fixture(map[string]string{
				"0000000001_drop.up.sql":   test.sql,
				"0000000001_drop.down.sql": "-- restore\n",
			}), lint.Options{
				Dialect:   "postgres",
				DirFormat: migrationfile.DirFormatPtah,
				RuleConfigs: map[string]lint.RuleConfig{
					"DS102X": {
						Match:   `strcontains(upper(statement.sql), "DROP COLUMN")`,
						Message: "a dropped column is gone for good",
					},
					"DS102XY": {
						Match:   `strcontains(upper(statement.sql), "DROP COLUMN")`,
						Message: "a dropped column is gone for good",
					},
				},
			})
			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(findings), qt.DeepEquals, test.want)
		})
	}
}

func TestSelectorSelects_TellsACodeFromAPrefix(t *testing.T) {
	tests := []struct {
		selector string
		code     string
		want     bool
	}{
		{selector: "MF101", code: "MF101", want: true},
		{selector: "MF101", code: "MF101P", want: false},
		{selector: "MF101P", code: "MF101P", want: true},
		{selector: "MF", code: "MF101P", want: true},
		{selector: "MF10", code: "MF102", want: true},
		{selector: "PG3", code: "PG301", want: true},
		{selector: "PG3", code: "PG101", want: false},
		{selector: "", code: "DS101", want: false},
		{selector: " DS101 ", code: "DS101", want: true},
	}

	for _, test := range tests {
		t.Run(test.selector+" on "+test.code, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lint.SelectorSelects(test.selector, test.code), qt.Equals, test.want)
		})
	}
}
