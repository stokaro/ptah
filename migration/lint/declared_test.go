package lint_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
)

// declaredRuleOptions runs a directory with one declared rule.
func declaredRuleOptions(code string, config lint.RuleConfig, dialect string) lint.Options {
	return lint.Options{
		Dialect:     dialect,
		DirFormat:   migrator.MigrationDirFormatAtlas,
		RuleConfigs: map[string]lint.RuleConfig{code: config},
	}
}

// findingsFor counts only the findings a given rule reported, so a fixture that
// also trips a built-in rule (a non-conventional file name, say) still measures
// the declared rule rather than the total.
func findingsFor(findings []lint.Finding, code string) []lint.Finding {
	var matched []lint.Finding
	for _, finding := range findings {
		if finding.Rule == code {
			matched = append(matched, finding)
		}
	}
	return matched
}

// varcharRule is the declaration used across these tests: the most ordinary
// rule anybody writes -- does this statement mention something we forbid.
func varcharRule() lint.RuleConfig {
	return lint.RuleConfig{
		Match:   `strcontains(lower(statement.sql), "varchar(")`,
		Message: "use text, not varchar(n)",
	}
}

func varcharFS() fstest.MapFS {
	return fstest.MapFS{
		"1_init.sql": {Data: []byte("CREATE TABLE users (id integer, nick VARCHAR(20));")},
	}
}

// TestDeclaredRule_RunsFromConfiguration is the acceptance case of
// stokaro/ptah#1706: a rule that exists only in a configuration file reports a
// finding, with no Go build anywhere.
func TestDeclaredRule_RunsFromConfiguration(t *testing.T) {
	c := qt.New(t)

	analysis, err := lint.AnalyzeFS(varcharFS(), declaredRuleOptions("NOVARCHAR", varcharRule(), "postgres"))

	c.Assert(err, qt.IsNil)
	findings := analysis.Findings()
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, "NOVARCHAR")
	c.Assert(findings[0].Message, qt.Equals, "use text, not varchar(n)")
	c.Assert(findings[0].Line, qt.Equals, 1)
	// Warning is the default: a project's own rule is advisory until its author
	// says otherwise, and a rule that failed the build the moment it was
	// written would be discovered by breaking CI.
	c.Assert(findings[0].Severity, qt.Equals, lint.SeverityWarning)
	// The code stands in for the title when none is given, so a report never
	// prints an empty column.
	c.Assert(findings[0].Title, qt.Equals, "NOVARCHAR")
}

// TestDeclaredRule_SilentOnAStatementItDoesNotMatch is the control. Without it
// a rule that fired on everything would pass the case above.
func TestDeclaredRule_SilentOnAStatementItDoesNotMatch(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_init.sql": {Data: []byte("CREATE TABLE users (id integer, nick text);")},
	}

	analysis, err := lint.AnalyzeFS(fsys, declaredRuleOptions("NOVARCHAR", varcharRule(), "postgres"))

	c.Assert(err, qt.IsNil)
	c.Assert(analysis.Findings(), qt.HasLen, 0)
}

// TestDeclaredRule_SeverityAndTitleAreHonored covers the fields a report prints.
func TestDeclaredRule_SeverityAndTitleAreHonored(t *testing.T) {
	c := qt.New(t)
	config := varcharRule()
	config.Severity = lint.SeverityError
	config.Title = "varchar(n) instead of text"

	analysis, err := lint.AnalyzeFS(varcharFS(), declaredRuleOptions("NOVARCHAR", config, "postgres"))

	c.Assert(err, qt.IsNil)
	findings := analysis.Findings()
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Severity, qt.Equals, lint.SeverityError)
	c.Assert(findings[0].Title, qt.Equals, "varchar(n) instead of text")
}

// TestDeclaredRule_DialectScope covers `dialects`, in both directions.
//
// A rule scoped to a dialect it is not running under must stay silent, or the
// scope is decoration.
func TestDeclaredRule_DialectScope(t *testing.T) {
	tests := []struct {
		name    string
		running string
		want    int
	}{
		{name: "matching dialect fires", running: "postgres", want: 1},
		{name: "other dialect stays silent", running: "mysql", want: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			config := varcharRule()
			config.Dialects = []string{"postgres"}

			analysis, err := lint.AnalyzeFS(
				varcharFS(), declaredRuleOptions("NOVARCHAR", config, test.running))

			c.Assert(err, qt.IsNil)
			c.Assert(analysis.Findings(), qt.HasLen, test.want)
		})
	}
}

// TestDeclaredRule_ReadsTheDialectItRunsUnder covers `dialect` as a NAME in the
// expression, which is a different mechanism from the Dialects scope above: it
// lets one rule say different things per engine instead of being switched off.
func TestDeclaredRule_ReadsTheDialectItRunsUnder(t *testing.T) {
	c := qt.New(t)
	config := lint.RuleConfig{Match: `dialect == "postgres"`, Message: "postgres only"}

	postgres, err := lint.AnalyzeFS(varcharFS(), declaredRuleOptions("PGONLY", config, "postgres"))
	c.Assert(err, qt.IsNil)
	mysql, err := lint.AnalyzeFS(varcharFS(), declaredRuleOptions("PGONLY", config, "mysql"))
	c.Assert(err, qt.IsNil)

	c.Assert(postgres.Findings(), qt.HasLen, 1)
	c.Assert(mysql.Findings(), qt.HasLen, 0)
}

// TestDeclaredRule_DownDirection covers applies-to-down in both directions.
//
// Off by default for the same reason it is off on a built-in rule: most
// statement rules describe a forward change, where the rollback is the remedy
// rather than the hazard.
func TestDeclaredRule_DownDirection(t *testing.T) {
	tests := []struct {
		name          string
		appliesToDown bool
		want          int
	}{
		{name: "down is skipped by default", appliesToDown: false, want: 0},
		{name: "down runs when declared", appliesToDown: true, want: 1},
	}

	fsys := fstest.MapFS{
		"0000000001_init.up.sql":   {Data: []byte("CREATE TABLE users (id integer);")},
		"0000000001_init.down.sql": {Data: []byte("CREATE TABLE audit (nick VARCHAR(20));")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			config := varcharRule()
			config.AppliesToDown = test.appliesToDown
			opts := declaredRuleOptions("NOVARCHAR", config, "postgres")
			opts.DirFormat = migrator.MigrationDirFormatPtah

			analysis, err := lint.AnalyzeFS(fsys, opts)

			c.Assert(err, qt.IsNil)
			c.Assert(findingsFor(analysis.Findings(), "NOVARCHAR"), qt.HasLen, test.want)
		})
	}
}

// TestDeclaredRule_HonorsNolint proves a declared rule is suppressed the same
// way a built-in one is.
//
// A declared rule is implemented as a file-level checker, which the analyzer
// does NOT run per-statement suppression for -- so this is the assertion that
// the implementation reproduces it rather than skipping it.
func TestDeclaredRule_HonorsNolint(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_init.sql": {Data: []byte(
			"-- ptah:nolint NOVARCHAR\nCREATE TABLE users (id integer, nick VARCHAR(20));")},
	}

	analysis, err := lint.AnalyzeFS(fsys, declaredRuleOptions("NOVARCHAR", varcharRule(), "postgres"))

	c.Assert(err, qt.IsNil)
	c.Assert(analysis.Findings(), qt.HasLen, 0)
}

// TestDeclaredRule_HonorsDisable covers --disable selecting a declared rule by
// its code, which only works because a declared code is an ordinary rule code.
func TestDeclaredRule_HonorsDisable(t *testing.T) {
	c := qt.New(t)
	opts := declaredRuleOptions("NOVARCHAR", varcharRule(), "postgres")
	opts.Disabled = []string{"NOVARCHAR"}

	analysis, err := lint.AnalyzeFS(varcharFS(), opts)

	c.Assert(err, qt.IsNil)
	c.Assert(analysis.Findings(), qt.HasLen, 0)
}

// TestDeclaredRule_EvaluationFailureIsReported keeps a broken rule visible.
//
// There is no error channel out of a rule. Returning false would make a broken
// rule indistinguishable from a clean file, which is the worst of the available
// outcomes: the directory would read as checked by a rule that never ran.
func TestDeclaredRule_EvaluationFailureIsReported(t *testing.T) {
	c := qt.New(t)
	config := lint.RuleConfig{Match: `upper(statement.sql)`, Message: "unused"}

	analysis, err := lint.AnalyzeFS(varcharFS(), declaredRuleOptions("BROKEN", config, "postgres"))

	c.Assert(err, qt.IsNil)
	findings := analysis.Findings()
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Message, qt.Contains, "must evaluate to a boolean")
}

// TestDeclaredRule_RefusedDeclarations covers what a malformed declaration
// reports, at LOAD time rather than as findings.
//
// A configuration error has to fail before findings are reported, or a run
// reports some of them and then dies -- which reads as a partial result nobody
// can act on.
func TestDeclaredRule_RefusedDeclarations(t *testing.T) {
	tests := []struct {
		name    string
		code    string
		config  lint.RuleConfig
		message string
	}{
		{
			name:    "no message",
			code:    "NOVARCHAR",
			config:  lint.RuleConfig{Match: `file.is_up`},
			message: "declares `match` but no `message`",
		},
		{
			name:    "built-in code",
			code:    "DS101",
			config:  lint.RuleConfig{Match: `file.is_up`, Message: "x"},
			message: "is already defined",
		},
		{
			name:    "unknown name in expression",
			code:    "NOVARCHAR",
			config:  lint.RuleConfig{Match: `stmt.sql != ""`, Message: "x"},
			message: `unknown name "stmt"`,
		},
		{
			name:    "unparseable expression",
			code:    "NOVARCHAR",
			config:  lint.RuleConfig{Match: `contains(`, Message: "x"},
			message: "parse match expression",
		},
		{
			name: "unsupported dialect scope",
			code: "NOVARCHAR",
			config: lint.RuleConfig{
				Match: `file.is_up`, Message: "x", Dialects: []string{"postgrez"},
			},
			message: `scoped to unsupported dialect "postgrez"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := declaredRuleOptions(test.code, test.config, "postgres")

			err := lint.ValidateOptions(opts)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.message)
			// The same refusal reaches an actual run, rather than only the gate.
			_, analyzeErr := lint.AnalyzeFS(varcharFS(), opts)
			c.Assert(analyzeErr, qt.IsNotNil)
		})
	}
}

// TestDeclaredRule_ConfiguringAnExistingRuleStillWorks is the paired case for
// the built-in-code refusal above.
//
// An entry WITHOUT `match` configures a rule that already exists, which is what
// this map has always meant. Refusing a built-in code outright would have
// broken every severity override in every existing project.
func TestDeclaredRule_ConfiguringAnExistingRuleStillWorks(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{"1_drop.sql": {Data: []byte("DROP TABLE users;")}}

	analysis, err := lint.AnalyzeFS(fsys, lint.Options{
		DirFormat:   migrator.MigrationDirFormatAtlas,
		RuleConfigs: map[string]lint.RuleConfig{"DS101": {Severity: lint.SeverityError}},
	})

	c.Assert(err, qt.IsNil)
	findings := analysis.Findings()
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, "DS101")
	c.Assert(findings[0].Severity, qt.Equals, lint.SeverityError)
}

// TestRuleConfig_DeclaresSeparatesTheTwoUses states the discriminator directly.
func TestRuleConfig_DeclaresSeparatesTheTwoUses(t *testing.T) {
	tests := []struct {
		name   string
		config lint.RuleConfig
		want   bool
	}{
		{name: "match declares", config: lint.RuleConfig{Match: `file.is_up`}, want: true},
		{name: "blank match does not", config: lint.RuleConfig{Match: "   "}, want: false},
		{name: "severity only configures", config: lint.RuleConfig{Severity: lint.SeverityError}, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.config.Declares(), qt.Equals, test.want)
		})
	}
}

// TestDeclaredRule_SuppressionIsPerStatementNotPerLine is what makes the
// declared rule's own suppression check load-bearing.
//
// A declared rule is a file-level checker, and the analyzer suppresses those by
// mapping a finding back to a statement BY LINE NUMBER. When two statements
// share a line that mapping is ambiguous and the analyzer declines to suppress
// either -- so a directive on the second statement would be ignored, and the
// first statement's finding would be suppressed by a directive that was never
// about it.
//
// The rule resolves suppression against the statement it is actually looking
// at, which is exact. Removing that check leaves both findings, which is how
// this case tells the two implementations apart.
func TestDeclaredRule_SuppressionIsPerStatementNotPerLine(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_init.sql": {Data: []byte(
			"-- ptah:nolint NOVARCHAR\n" +
				"CREATE TABLE a (x VARCHAR(1)); CREATE TABLE b (y VARCHAR(1));\n")},
	}

	analysis, err := lint.AnalyzeFS(fsys, declaredRuleOptions("NOVARCHAR", varcharRule(), "postgres"))

	c.Assert(err, qt.IsNil)
	findings := findingsFor(analysis.Findings(), "NOVARCHAR")
	// Exactly one: the suppressed statement is silent and the other is not.
	c.Assert(findings, qt.HasLen, 1)
}
