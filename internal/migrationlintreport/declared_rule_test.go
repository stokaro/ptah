package migrationlintreport_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/migrationlintreport"
	migrationlint "go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
)

// buildWithProjectRules runs a lint report over one migration with the project
// config a `lint { rule ... }` block would have produced.
//
// This is the seam the declaration has to survive: the project file and the
// `.ptah-lint.yaml` policy are merged here, and it was this merge that dropped
// `match` and left a rule that parsed, validated, and never ran
// (stokaro/ptah#1706).
func buildWithProjectRules(
	c *qt.C, sql string, rules map[string]projectconfig.LintRuleConfig,
) (migrationlintreport.Report, error) {
	c.Helper()
	return migrationlintreport.Build(c.TB.Context(), migrationlintreport.Options{
		Dir:       "unused",
		FS:        fstest.MapFS{"1_init.sql": {Data: []byte(sql)}},
		DirFormat: string(migrator.MigrationDirFormatAtlas),
		Dialect:   "postgres",
		FailOn:    migrationlintreport.FailOnNone,
		Changed:   migrationlintreport.ChangedOptions{Dialect: true},
	}, projectconfig.Config{Lint: projectconfig.LintConfig{RuleConfigs: rules}})
}

// severitiesOf returns the severities a given rule reported, so a test can
// assert on one rule without a conditional of its own.
func severitiesOf(report migrationlintreport.Report, code string) []migrationlint.Severity {
	var severities []migrationlint.Severity
	for _, finding := range report.Analysis.Findings() {
		if finding.Rule == code {
			severities = append(severities, finding.Severity)
		}
	}
	return severities
}

// messagesOf is the same for finding text.
func messagesOf(report migrationlintreport.Report, code string) []string {
	var messages []string
	for _, finding := range report.Analysis.Findings() {
		if finding.Rule == code {
			messages = append(messages, finding.Message)
		}
	}
	return messages
}

// TestBuild_ProjectDeclaredRuleReportsAFinding is the end-to-end assertion for
// the project-file spelling: a rule declared in `atlas.hcl` reaches the
// analyzer and reports.
func TestBuild_ProjectDeclaredRuleReportsAFinding(t *testing.T) {
	c := qt.New(t)

	report, err := buildWithProjectRules(c, "CREATE TABLE users (nick VARCHAR(20));\n",
		map[string]projectconfig.LintRuleConfig{
			"NOVARCHAR": {
				Match:    `strcontains(lower(statement.sql), "varchar(")`,
				Message:  "use text, not varchar(n)",
				Severity: string(migrationlint.SeverityError),
			},
		})

	c.Assert(err, qt.IsNil)
	c.Assert(messagesOf(report, "NOVARCHAR"), qt.DeepEquals, []string{"use text, not varchar(n)"})
	c.Assert(severitiesOf(report, "NOVARCHAR"), qt.DeepEquals,
		[]migrationlint.Severity{migrationlint.SeverityError})
}

// TestBuild_ProjectRuleWithoutMatchStillConfigures is the paired case.
//
// An entry without `match` has always meant "configure the rule with this
// code". Carrying the declaration through the merge must not change what those
// entries do.
//
// DS101 is used rather than a file-name rule because the fixture has to be a
// statement the chosen rule really fires on: a rule that never fires would let
// this pass whatever the severity did.
func TestBuild_ProjectRuleWithoutMatchStillConfigures(t *testing.T) {
	c := qt.New(t)

	report, err := buildWithProjectRules(c, "DROP TABLE users;\n",
		map[string]projectconfig.LintRuleConfig{
			"DS101": {Severity: string(migrationlint.SeverityInfo)},
		})

	c.Assert(err, qt.IsNil)
	c.Assert(severitiesOf(report, "DS101"), qt.DeepEquals,
		[]migrationlint.Severity{migrationlint.SeverityInfo})
}

// TestBuild_ProjectRuleSeverityOverrideIsObservable is the control for the case
// above: without it, a run where DS101 defaulted to error would pass whether or
// not the override was applied.
func TestBuild_ProjectRuleSeverityOverrideIsObservable(t *testing.T) {
	c := qt.New(t)

	report, err := buildWithProjectRules(c, "DROP TABLE users;\n", nil)

	c.Assert(err, qt.IsNil)
	// DS101 defaults to error, which is why the override above asks for info:
	// overriding a rule to the severity it already had would pass whether or
	// not the override was applied at all.
	c.Assert(severitiesOf(report, "DS101"), qt.DeepEquals,
		[]migrationlint.Severity{migrationlint.SeverityError})
}

// TestBuild_ProjectDeclaredRuleIsRefusedWhenMalformed keeps a broken
// declaration failing the run rather than being dropped on the way through.
func TestBuild_ProjectDeclaredRuleIsRefusedWhenMalformed(t *testing.T) {
	c := qt.New(t)

	_, err := buildWithProjectRules(c, "CREATE TABLE users (nick VARCHAR(20));\n",
		map[string]projectconfig.LintRuleConfig{
			"NOVARCHAR": {Match: `stmt.sql != ""`, Message: "x"},
		})

	c.Assert(err, qt.ErrorMatches, `.*unknown name "stmt".*`)
}
