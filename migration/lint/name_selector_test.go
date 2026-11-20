package lint_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// selectorFS is one up file with no down, so it reports file-form/missing-down
// and, on MySQL, data-dependent/add-unique-index (a unique index over
// existing rows).
var selectorFS = map[string]string{
	"0000000001_index.up.sql": "CREATE UNIQUE INDEX u ON t (a);\n",
}

func lintSelectorFS(c *qt.C, opts lint.Options) []lint.Finding {
	c.Helper()
	opts.Dialect = "mysql"
	opts.DirFormat = migrationfile.DirFormatPtah
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(selectorFS), opts)
	c.Assert(err, qt.IsNil)
	return findings
}

// The fixture of these tests is selectorFS: on MySQL it reports
// data-dependent/add-unique-index and file-form/missing-down. They sit in
// different categories, and neither name begins the other, so a prefix match
// of one cannot reach the other.

// TestNameSelectors_DisableSelectsByNameAndCategory disables by Ptah name and
// by category, alone and beside a code.
func TestNameSelectors_DisableSelectsByNameAndCategory(t *testing.T) {
	rows := []struct {
		name     string
		disabled []string
		want     []string
	}{
		{name: "the check's name", disabled: []string{"data-dependent/add-unique-index"}, want: []string{"file-form/missing-down"}},
		{name: "the file-form rule's name", disabled: []string{"file-form/missing-down"}, want: []string{"data-dependent/add-unique-index"}},
		{name: "the check's category", disabled: []string{"data-dependent"}, want: []string{"file-form/missing-down"}},
		{name: "the file-form category with a slash", disabled: []string{"file-form/"}, want: []string{"data-dependent/add-unique-index"}},
		{name: "a name beside a code", disabled: []string{"file-form/missing-down", "data-dependent/add-unique-index"}, want: make([]string, 0)},
		{name: "a category no finding is in", disabled: []string{"locking"}, want: []string{"file-form/missing-down", "data-dependent/add-unique-index"}},
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
				`a name is a category and a rule joined by a slash, such as locking/add-foreign-key`,
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
			wantErr:  `rule selector "lockin" is not a rule name or category.*`,
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
			want:    map[string]lint.Severity{"data-dependent/add-unique-index": lint.SeverityError, "file-form/missing-down": lint.SeverityWarning},
		},
		{
			name:    "the file-form category",
			configs: map[string]lint.RuleConfig{"file-form": {Severity: lint.SeverityInfo}},
			want:    map[string]lint.Severity{"data-dependent/add-unique-index": lint.SeverityWarning, "file-form/missing-down": lint.SeverityInfo},
		},
		{
			name:    "the file-form category with a slash",
			configs: map[string]lint.RuleConfig{"file-form/": {Severity: lint.SeverityInfo}},
			want:    map[string]lint.Severity{"data-dependent/add-unique-index": lint.SeverityWarning, "file-form/missing-down": lint.SeverityInfo},
		},
		{
			name: "a name beats its category",
			configs: map[string]lint.RuleConfig{
				"data-dependent/add-unique-index": {Severity: lint.SeverityError}, "data-dependent": {Severity: lint.SeverityInfo},
			},
			want: map[string]lint.Severity{"data-dependent/add-unique-index": lint.SeverityError, "file-form/missing-down": lint.SeverityWarning},
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
			wantErr: `.*lint rules key "locking/set-not-nul" is not the name of a lint rule; .*`,
		},
		{
			name:    "a rules key naming a category twice",
			config:  "rules:\n  locking:\n    severity: warning\n  locking/:\n    severity: error\n",
			wantErr: `.*lint rules "locking" and "locking/" name one category; keep one of them`,
		},
		{
			name: "a declared rule under a category nobody declared",
			config: "rules:\n  nosuch/no-renames:\n    match: 'strcontains(upper(statement.sql), \"RENAME\")'\n" +
				"    message: renames break readers\n",
			wantErr: `.*lint rule "nosuch/no-renames": "nosuch" is not a rule category; the categories are .*`,
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
// check by its name and by its category. file-form/missing-down has no statement to sit
// under, so a directive never silences it.
func TestNameSelectors_NoLintSelectsByNameAndCategory(t *testing.T) {
	rows := []struct {
		name string
		sql  string
		want []string
	}{
		{name: "the check's name", sql: "-- ptah:nolint data-dependent/add-unique-index\n", want: []string{"file-form/missing-down"}},
		{name: "the check's category", sql: "-- ptah:nolint data-dependent\n", want: []string{"file-form/missing-down"}},
		{name: "the check's category with a slash", sql: "-- ptah:nolint data-dependent/\n", want: []string{"file-form/missing-down"}},
		{name: "a name and a code in one directive", sql: "-- ptah:nolint file-form/missing-down, data-dependent/add-unique-index\n", want: []string{"file-form/missing-down"}},
		{name: "another category", sql: "-- ptah:nolint rollout\n", want: []string{"file-form/missing-down", "data-dependent/add-unique-index"}},
		{name: "a slash in the reason", sql: "-- ptah:nolint data-dependent/add-unique-index / reviewed\n", want: []string{"file-form/missing-down"}},
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
		{name: "no reason", directive: "ptah:nolint rollout/drop-table", want: []string{"file-form/missing-down", "destructive/drop-table"}},
		{name: "a category in the reason", directive: "ptah:nolint rollout/drop-table destructive on purpose", want: []string{"file-form/missing-down", "destructive/drop-table"}},
		{name: "a category after a separator", directive: "ptah:nolint rollout/drop-table -- destructive/", want: []string{"file-form/missing-down", "destructive/drop-table"}},
		{name: "a name in the reason", directive: "ptah:nolint rollout/drop-table because destructive/drop-table", want: []string{"file-form/missing-down", "destructive/drop-table"}},
		{name: "a code in the reason", directive: "ptah:nolint rollout/drop-table because destructive/drop-table is wrong", want: []string{"file-form/missing-down", "destructive/drop-table"}},
		{name: "a code after a separator", directive: "ptah:nolint rollout/drop-table -- destructive/drop-table", want: []string{"file-form/missing-down", "destructive/drop-table"}},
		{name: "a URL", directive: "ptah:nolint rollout/drop-table see https://example.com/ticket/ABC-1", want: []string{"file-form/missing-down", "destructive/drop-table"}},
		{name: "and/or", directive: "ptah:nolint rollout/drop-table this and/or that", want: []string{"file-form/missing-down", "destructive/drop-table"}},
		{name: "a slash word right after the code", directive: "ptah:nolint rollout/drop-table dba/ops", want: []string{"file-form/missing-down", "destructive/drop-table"}},
		{name: "a URL right after the code", directive: "ptah:nolint rollout/drop-table https://example.com/ticket/ABC-1", want: []string{"file-form/missing-down", "destructive/drop-table"}},
		{name: "w/o", directive: "ptah:nolint rollout/drop-table w/o review", want: []string{"file-form/missing-down", "destructive/drop-table"}},
		{name: "slashes before the text", directive: "ptah:nolint rollout/drop-table // dba/ops", want: []string{"file-form/missing-down", "destructive/drop-table"}},
		{name: "a diff header", directive: "ptah:nolint rollout/drop-table --- a/b", want: []string{"file-form/missing-down", "destructive/drop-table"}},
		{name: "a text and no selector", directive: "ptah:nolint reviewed by dba/ops", want: []string{"file-form/missing-down", "destructive/drop-table", "rollout/drop-table"}},
		{name: "a bare category as the first word", directive: "ptah:nolint destructive", want: []string{"file-form/missing-down", "rollout/drop-table"}},
		{name: "a name beside a code", directive: "ptah:nolint rollout/drop-table destructive/drop-table on purpose", want: []string{"file-form/missing-down"}},
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
		fixture(map[string]string{"0000000001_drop.up.sql": "-- ptah:nolint rollout/drop-table locking/typo\nDROP TABLE users;\n"}),
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
				`is not the name of a migration lint rule; a name is a category and a rule joined by a slash, such as locking/add-foreign-key`,
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

// TestNameSelectors_FindingsCarryTheRuleName reads the rule name on each
// finding, a declared rule's included.
func TestNameSelectors_FindingsCarryTheRuleName(t *testing.T) {
	c := qt.New(t)
	findings := lintSelectorFS(c, lint.Options{RuleConfigs: map[string]lint.RuleConfig{
		"design/xx101": {Match: `strcontains(upper(statement.sql), "UNIQUE")`, Message: "a unique index"},
	}})

	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"file-form/missing-down", "data-dependent/add-unique-index", "design/xx101"})
}

// TestSelectorSelects_MatchesANameExactly drives the exported predicate with
// names. The rows that answer false begin the rule's name or category, and a
// prefix match would turn each into a hit.
func TestSelectorSelects_MatchesANameExactly(t *testing.T) {
	rows := []struct {
		selector string
		rule     string
		want     bool
	}{
		{selector: "file-form/missing-down", rule: "file-form/missing-down", want: true},
		{selector: "file-form", rule: "file-form/missing-down", want: true},
		{selector: "file-form/", rule: "file-form/missing-down", want: true},
		{selector: "file-form/missing-down", rule: "data-dependent/add-unique-index", want: false},
		{selector: "file-form/missing", rule: "file-form/missing-down", want: false},
		{selector: "file/", rule: "file-form/missing-down", want: false},
		{selector: "data-dependent", rule: "data-dependent/add-unique-index", want: true},
		{selector: "data-dependent", rule: "file-form/missing-down", want: false},
		{selector: "MF", rule: "file-form/missing-down", want: false},
		{selector: "MF103", rule: "file-form/file-name", want: false},
	}
	for _, row := range rows {
		t.Run(row.selector+" on "+row.rule, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lint.SelectorSelects(row.selector, row.rule), qt.Equals, row.want)
		})
	}
}

// TestNameSelectors_AnyCodeShapedWordIsRefused pins the native refusal: a word
// spelled as a rule code is refused wherever a selector is read, with one
// message that names no rule the code might stand for.
func TestNameSelectors_AnyCodeShapedWordIsRefused(t *testing.T) {
	const refusal = `lint rules are named category/name; %s is not a rule name`
	rows := []struct {
		name    string
		options lint.Options
		wantErr string
	}{
		{name: "--disable a code", options: lint.Options{Disabled: []string{"DS102"}}, wantErr: `rule selector ` + refusal},
		{name: "--disable a prefix", options: lint.Options{Disabled: []string{"MF"}}, wantErr: `rule selector ` + refusal},
		{name: "a rules key", options: lint.Options{RuleConfigs: map[string]lint.RuleConfig{"PG301": {Severity: lint.SeverityError}}}, wantErr: refusal},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			err := lint.ValidateOptions(builtintest.Runtime(), row.options)
			c.Assert(err, qt.ErrorMatches, `.*lint rules are named category/name; [A-Z0-9]+ is not a rule name`)
		})
	}
}

// TestNameSelectors_NoLintRefusesACode refuses a code in a ptah:nolint
// comment at its line, and the code a reason merely resembles is read as
// text.
func TestNameSelectors_NoLintRefusesACode(t *testing.T) {
	c := qt.New(t)
	_, err := lint.LintFS(c.Context(), builtintest.Runtime(),
		fixture(map[string]string{"0000000001_drop.up.sql": "-- ptah:nolint DS101\nDROP TABLE users;\n"}),
		lint.Options{Dialect: "postgres", DirFormat: migrationfile.DirFormatPtah})

	c.Assert(err, qt.ErrorMatches,
		`0000000001_drop.up.sql: the comment above line 2: ptah:nolint: lint rules are named category/name; DS101 is not a rule name`)
}

// TestNameSelectors_NoLintReadsAnUppercaseWordWithoutDigitsAsText keeps a
// shouted reason from being refused as a code.
func TestNameSelectors_NoLintReadsAnUppercaseWordWithoutDigitsAsText(t *testing.T) {
	c := qt.New(t)
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(),
		fixture(map[string]string{"0000000001_drop.up.sql": "-- ptah:nolint rollout/drop-table ASAP\nDROP TABLE users;\n"}),
		lint.Options{Dialect: "postgres", DirFormat: migrationfile.DirFormatPtah})

	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.Not(qt.Contains), "rollout/drop-table")
}
