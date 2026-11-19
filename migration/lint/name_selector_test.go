package lint_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// The fixture of these tests is selectorFS: on MySQL it reports MF101, named
// data-dependent/add-unique-index, and MF101P, named file-form/missing-down.
// The codes differ by one letter, and a code prefix cannot reach MF101
// without MF101P; the names sit in different categories.

// TestNameSelectors_DisableSelectsByNameAndCategory disables by Ptah name and
// by category, alone and beside a code.
func TestNameSelectors_DisableSelectsByNameAndCategory(t *testing.T) {
	rows := []struct {
		name     string
		disabled []string
		want     []string
	}{
		{name: "the check's name", disabled: []string{"data-dependent/add-unique-index"}, want: []string{"MF101P"}},
		{name: "the file-form rule's name", disabled: []string{"file-form/missing-down"}, want: []string{"MF101"}},
		{name: "the check's category", disabled: []string{"data-dependent"}, want: []string{"MF101P"}},
		{name: "the file-form category with a slash", disabled: []string{"file-form/"}, want: []string{"MF101"}},
		{name: "a name beside a code", disabled: []string{"file-form/missing-down", "MF101"}, want: make([]string, 0)},
		{name: "a category no finding is in", disabled: []string{"locking"}, want: []string{"MF101P", "MF101"}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			findings := lintSelectorFS(c, lint.Options{Disabled: row.disabled})
			c.Assert(rulesOf(findings), qt.DeepEquals, row.want)
		})
	}
}

// TestNameSelectors_DisableRefusesAnUnknownName refuses a name or category
// migration lint does not have. The first two rows begin a real name and a
// real category, which a prefix match would accept.
func TestNameSelectors_DisableRefusesAnUnknownName(t *testing.T) {
	rows := []struct {
		name     string
		disabled []string
		wantErr  string
	}{
		{
			name:     "the beginning of a name",
			disabled: []string{"file-form/missing"},
			wantErr: `rule selector "file-form/missing" is not the name of a migration lint rule; ` +
				`a name is a category and a rule joined by a slash, such as locking/set-not-null`,
		},
		{
			name:     "the beginning of a category",
			disabled: []string{"file/"},
			wantErr:  `rule selector "file/" is not a rule category; the categories are capability, correctness, .*`,
		},
		{
			name:     "a name only SQL lint has",
			disabled: []string{"design/no-primary-key"},
			wantErr:  `rule selector "design/no-primary-key" is not the name of a migration lint rule; .*`,
		},
		{
			name:     "a lower-case word that is not a category",
			disabled: []string{"lockin"},
			wantErr:  `rule selector "lockin" must start with an uppercase ASCII letter .*, or be a rule name or category, such as locking/set-not-null or locking`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(selectorFS), lint.Options{
				Dialect:   "mysql",
				DirFormat: migrationfile.DirFormatPtah,
				Disabled:  row.disabled,
			})
			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(findings, qt.IsNil)
		})
	}
}

// TestNameSelectors_RulesKeyGovernsByNameAndCategory sets severities by name
// and by category. A key naming one rule beats a key naming a group, and a
// category beats a code prefix.
func TestNameSelectors_RulesKeyGovernsByNameAndCategory(t *testing.T) {
	rows := []struct {
		name    string
		configs map[string]lint.RuleConfig
		want    map[string]lint.Severity
	}{
		{
			name:    "the check's name",
			configs: map[string]lint.RuleConfig{"data-dependent/add-unique-index": {Severity: lint.SeverityError}},
			want:    map[string]lint.Severity{"MF101": lint.SeverityError, "MF101P": lint.SeverityWarning},
		},
		{
			name:    "the file-form category",
			configs: map[string]lint.RuleConfig{"file-form": {Severity: lint.SeverityInfo}},
			want:    map[string]lint.Severity{"MF101": lint.SeverityWarning, "MF101P": lint.SeverityInfo},
		},
		{
			name:    "the file-form category with a slash",
			configs: map[string]lint.RuleConfig{"file-form/": {Severity: lint.SeverityInfo}},
			want:    map[string]lint.Severity{"MF101": lint.SeverityWarning, "MF101P": lint.SeverityInfo},
		},
		{
			name: "a code beats the rule's name",
			configs: map[string]lint.RuleConfig{
				"MF101": {Severity: lint.SeverityInfo}, "data-dependent/add-unique-index": {Severity: lint.SeverityError},
			},
			want: map[string]lint.Severity{"MF101": lint.SeverityInfo, "MF101P": lint.SeverityWarning},
		},
		{
			name: "a name beats its category",
			configs: map[string]lint.RuleConfig{
				"data-dependent/add-unique-index": {Severity: lint.SeverityError}, "data-dependent": {Severity: lint.SeverityInfo},
			},
			want: map[string]lint.Severity{"MF101": lint.SeverityError, "MF101P": lint.SeverityWarning},
		},
		{
			name: "a category beats a code prefix",
			configs: map[string]lint.RuleConfig{
				"data-dependent": {Severity: lint.SeverityError}, "MF": {Severity: lint.SeverityInfo},
			},
			want: map[string]lint.Severity{"MF101": lint.SeverityError, "MF101P": lint.SeverityInfo},
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			findings := lintSelectorFS(c, lint.Options{RuleConfigs: row.configs})
			got := make(map[string]lint.Severity)
			for _, finding := range findings {
				got[finding.Rule] = finding.Severity
			}
			c.Assert(got, qt.DeepEquals, row.want)
		})
	}
}

// TestNameSelectors_PolicyRefusesAnUnknownName loads policies that name a
// rule wrongly. A `rules:` key may carry a name either linter has, since
// `ptah sql lint` reads the section too; `disabled-rules` is migration
// lint's alone.
func TestNameSelectors_PolicyRefusesAnUnknownName(t *testing.T) {
	rows := []struct {
		name    string
		config  string
		wantErr string
	}{
		{
			name:    "a disabled name",
			config:  "disabled-rules: [locking/set-not-nul]\n",
			wantErr: `.*rule selector "locking/set-not-nul" is not the name of a migration lint rule; .*`,
		},
		{
			name:    "a disabled name only SQL lint has",
			config:  "disabled-rules: [design/no-primary-key]\n",
			wantErr: `.*rule selector "design/no-primary-key" is not the name of a migration lint rule; .*`,
		},
		{
			name:    "a rules key naming no rule",
			config:  "rules:\n  locking/set-not-nul:\n    severity: warning\n",
			wantErr: `.*rule selector "locking/set-not-nul" is not the name of a lint rule; .*`,
		},
		{
			name:    "a rules key naming a category twice",
			config:  "rules:\n  locking:\n    severity: warning\n  locking/:\n    severity: error\n",
			wantErr: `.*lint rules "locking" and "locking/" name one category; keep one of them`,
		},
		{
			name: "a declared rule under a name",
			config: "rules:\n  locking/no-renames:\n    match: 'strcontains(upper(statement.sql), \"RENAME\")'\n" +
				"    message: renames break readers\n",
			wantErr: `.*lint rule "locking/no-renames": a declared rule needs a code in the same form as every other rule .*`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(c.TempDir(), ".ptah-lint.yaml")
			c.Assert(os.WriteFile(path, []byte(row.config), 0o600), qt.IsNil)

			cfg, err := lint.LoadConfig(builtintest.Runtime(), path)

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(cfg, qt.IsNil)
		})
	}
}

// TestNameSelectors_PolicyAcceptsAKnownName_HappyPath loads names either
// linter has as `rules:` keys, and a migration lint name in
// `disabled-rules`.
func TestNameSelectors_PolicyAcceptsAKnownName_HappyPath(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), ".ptah-lint.yaml")
	c.Assert(os.WriteFile(path, []byte("disabled-rules: [locking/set-not-null, rewrite]\n"+
		"rules:\n  design/no-primary-key:\n    severity: error\n  file-form/:\n    severity: info\n"), 0o600), qt.IsNil)

	cfg, err := lint.LoadConfig(builtintest.Runtime(), path)

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DisabledRules, qt.DeepEquals, []string{"locking/set-not-null", "rewrite"})
	c.Assert(cfg.Rules["design/no-primary-key"].Severity, qt.Equals, lint.SeverityError)
}

// TestNameSelectors_NoLintSelectsByNameAndCategory silences the statement's
// check by its name and by its category. MF101P has no statement to sit
// under, so a directive never silences it.
func TestNameSelectors_NoLintSelectsByNameAndCategory(t *testing.T) {
	rows := []struct {
		name string
		sql  string
		want []string
	}{
		{name: "the check's name", sql: "-- ptah:nolint data-dependent/add-unique-index\n", want: []string{"MF101P"}},
		{name: "the check's category", sql: "-- ptah:nolint data-dependent\n", want: []string{"MF101P"}},
		{name: "the check's category with a slash", sql: "-- ptah:nolint data-dependent/\n", want: []string{"MF101P"}},
		{name: "a name and a code in one directive", sql: "-- ptah:nolint file-form/missing-down, MF101\n", want: []string{"MF101P"}},
		{name: "another category", sql: "-- ptah:nolint rollout\n", want: []string{"MF101P", "MF101"}},
		{name: "codes joined by a slash", sql: "-- ptah:nolint DS101/MF101\n", want: []string{"MF101P"}},
		{name: "a slash in the reason", sql: "-- ptah:nolint MF101 / reviewed\n", want: []string{"MF101P"}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			findings, err := lint.LintFS(c.Context(), builtintest.Runtime(),
				fixture(map[string]string{"0000000001_index.up.sql": row.sql + "CREATE UNIQUE INDEX u ON t (a);\n"}),
				lint.Options{Dialect: "mysql", DirFormat: migrationfile.DirFormatPtah})
			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(findings), qt.DeepEquals, row.want)
		})
	}
}

// TestNameSelectors_NoLintReasonNeverSelects pins the directive grammar: the
// selectors come first and the reason starts at the first word that is not
// one, or after `--`. A category, a name or a code in the reason, and a word
// holding a slash, select nothing, and refuse nothing.
func TestNameSelectors_NoLintReasonNeverSelects(t *testing.T) {
	rows := []struct {
		name      string
		directive string
		want      []string
	}{
		{name: "no reason", directive: "ptah:nolint BC103", want: []string{"MF101P", "DS101"}},
		{name: "a category in the reason", directive: "ptah:nolint BC103 destructive on purpose", want: []string{"MF101P", "DS101"}},
		{name: "a category after a separator", directive: "ptah:nolint BC103 -- destructive/", want: []string{"MF101P", "DS101"}},
		{name: "a name in the reason", directive: "ptah:nolint BC103 because destructive/drop-table", want: []string{"MF101P", "DS101"}},
		{name: "a code in the reason", directive: "ptah:nolint BC103 because DS101 is wrong", want: []string{"MF101P", "DS101"}},
		{name: "a code after a separator", directive: "ptah:nolint BC103 -- DS101", want: []string{"MF101P", "DS101"}},
		{name: "a URL", directive: "ptah:nolint BC103 see https://example.com/ticket/ABC-1", want: []string{"MF101P", "DS101"}},
		{name: "and/or", directive: "ptah:nolint BC103 this and/or that", want: []string{"MF101P", "DS101"}},
		{name: "a slash word right after the code", directive: "ptah:nolint BC103 dba/ops", want: []string{"MF101P", "DS101"}},
		{name: "a URL right after the code", directive: "ptah:nolint BC103 https://example.com/ticket/ABC-1", want: []string{"MF101P", "DS101"}},
		{name: "w/o", directive: "ptah:nolint BC103 w/o review", want: []string{"MF101P", "DS101"}},
		{name: "slashes before the text", directive: "ptah:nolint BC103 // dba/ops", want: []string{"MF101P", "DS101"}},
		{name: "a diff header", directive: "ptah:nolint BC103 --- a/b", want: []string{"MF101P", "DS101"}},
		{name: "a text and no selector", directive: "ptah:nolint reviewed by dba/ops", want: []string{"MF101P", "BC103", "DS101"}},
		{name: "a bare category as the first word", directive: "ptah:nolint destructive", want: []string{"MF101P", "BC103"}},
		{name: "a name beside a code", directive: "ptah:nolint BC103 destructive/drop-table on purpose", want: []string{"MF101P"}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			findings, err := lint.LintFS(c.Context(), builtintest.Runtime(),
				fixture(map[string]string{"0000000001_drop.up.sql": "-- " + row.directive + "\nDROP TABLE users;\n"}),
				lint.Options{Dialect: "postgres", DirFormat: migrationfile.DirFormatPtah})
			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(findings), qt.DeepEquals, row.want)
		})
	}
}

// TestNameSelectors_NoLintRefusesAWrongCategoryName refuses a word that is
// meant as a name of a declared category and is wrong, even beside a reason.
func TestNameSelectors_NoLintRefusesAWrongCategoryName(t *testing.T) {
	c := qt.New(t)
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(),
		fixture(map[string]string{"0000000001_drop.up.sql": "-- ptah:nolint BC103 locking/typo\nDROP TABLE users;\n"}),
		lint.Options{Dialect: "postgres", DirFormat: migrationfile.DirFormatPtah})
	c.Assert(err, qt.ErrorMatches, `.*ptah:nolint: "locking/typo" is not the name of a migration lint rule; .*`)
	c.Assert(findings, qt.IsNil)
}

// TestNameSelectors_NoLintRefusesAnUnknownName refuses a directive naming a
// rule migration lint does not have, at the statement it sits above. Read as
// a family, the misspelling would silence nothing and say nothing.
func TestNameSelectors_NoLintRefusesAnUnknownName(t *testing.T) {
	rows := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name: "the beginning of a name",
			sql:  "-- ptah:nolint data-dependent/add-unique\n",
			wantErr: `0000000001_index.up.sql: the comment above line 2: ptah:nolint: "data-dependent/add-unique" ` +
				`is not the name of a migration lint rule; a name is a category and a rule joined by a slash, such as locking/set-not-null`,
		},
		{
			name:    "a category that is not declared",
			sql:     "-- ptah:nolint data/\n",
			wantErr: `0000000001_index.up.sql: the comment above line 2: ptah:nolint: "data/" is not a rule category; .*`,
		},
		{
			name:    "an unknown name beside a known one",
			sql:     "-- ptah:nolint data-dependent, locking/set-not-nul\n",
			wantErr: `.*ptah:nolint: "locking/set-not-nul" is not the name of a migration lint rule; .*`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			findings, err := lint.LintFS(c.Context(), builtintest.Runtime(),
				fixture(map[string]string{"0000000001_index.up.sql": row.sql + "CREATE UNIQUE INDEX u ON t (a);\n"}),
				lint.Options{Dialect: "mysql", DirFormat: migrationfile.DirFormatPtah})
			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(findings, qt.IsNil)
		})
	}
}

// TestNameSelectors_FindingsCarryTheRuleName reads the Ptah name on each
// finding, and the empty name of a rule a configuration declares.
func TestNameSelectors_FindingsCarryTheRuleName(t *testing.T) {
	c := qt.New(t)
	findings := lintSelectorFS(c, lint.Options{RuleConfigs: map[string]lint.RuleConfig{
		"XX101": {Match: `strcontains(upper(statement.sql), "UNIQUE")`, Message: "a unique index"},
	}})
	names := make(map[string]string)
	for _, finding := range findings {
		names[finding.Rule] = finding.Name
	}

	c.Assert(names, qt.DeepEquals, map[string]string{
		"MF101":  "data-dependent/add-unique-index",
		"MF101P": "file-form/missing-down",
		"XX101":  "",
	})
}

// TestSelectorSelects_MatchesANameExactly drives the exported predicate with
// names. The rows that answer false begin the rule's name or category.
func TestSelectorSelects_MatchesANameExactly(t *testing.T) {
	rows := []struct {
		selector string
		code     string
		want     bool
	}{
		{selector: "file-form/missing-down", code: "MF101P", want: true},
		{selector: "file-form", code: "MF101P", want: true},
		{selector: "file-form/", code: "MF101P", want: true},
		{selector: "file-form/missing-down", code: "MF101", want: false},
		{selector: "file-form/missing", code: "MF101P", want: false},
		{selector: "file/", code: "MF101P", want: false},
		{selector: "data-dependent", code: "MF101", want: true},
		{selector: "data-dependent", code: "MF101P", want: false},
	}
	for _, row := range rows {
		t.Run(row.selector+" on "+row.code, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lint.SelectorSelects(must.Must(lint.Rules(builtintest.Runtime())), row.selector, row.code), qt.Equals, row.want)
		})
	}
}
