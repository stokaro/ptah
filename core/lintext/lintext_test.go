package lintext_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
)

func validRule() lintext.Rule {
	return lintext.Rule{Code: "XY101", Title: "Drops a table", Severity: lintext.Error, Targets: []string{"postgres"}}
}

func TestRule_Validate_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		rule lintext.Rule
	}{
		{"statement text", validRule()},
		{"a routine body, two targets, down and a subsumption", lintext.Rule{Code: "XY102P", Title: "t", Severity: lintext.Info,
			Targets: []string{"mysql", "mariadb"}, AppliesToDown: true, Input: lintext.InputRoutineBody, Subsumes: []string{"XY101"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.rule.Validate(), qt.IsNil)
		})
	}
}

func TestRule_Validate_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		rule    lintext.Rule
		wantErr string
	}{
		{"a code outside the convention", lintext.Rule{Code: "xy101", Title: "t", Severity: lintext.Error, Targets: []string{"postgres"}},
			`.*rule code "xy101" must start with an uppercase ASCII letter.*`},
		{"a code starting with a digit", lintext.Rule{Code: "1XY", Title: "t", Severity: lintext.Error, Targets: []string{"postgres"}},
			`.*rule code "1XY" must start.*`},
		{"no title", lintext.Rule{Code: "XY101", Title: " ", Severity: lintext.Error, Targets: []string{"postgres"}}, `.*rule XY101 has no title`},
		{"an unknown severity", lintext.Rule{Code: "XY101", Title: "t", Severity: "fatal", Targets: []string{"postgres"}},
			`.*rule XY101 has unknown severity "fatal"`},
		{"an unknown input", lintext.Rule{Code: "XY101", Title: "t", Severity: lintext.Error, Targets: []string{"postgres"}, Input: "history"},
			`.*rule XY101 has unknown input "history"`},
		{"no target", lintext.Rule{Code: "XY101", Title: "t", Severity: lintext.Error}, `.*rule XY101 names no target`},
		{"a repeated target", lintext.Rule{Code: "XY101", Title: "t", Severity: lintext.Error, Targets: []string{"postgres", "postgres"}},
			`.*rule XY101 names a blank or repeated target`},
		{"a blank target", lintext.Rule{Code: "XY101", Title: "t", Severity: lintext.Error, Targets: []string{" "}},
			`.*rule XY101 names a blank or repeated target`},
		{"itself subsumed", lintext.Rule{Code: "XY101", Title: "t", Severity: lintext.Error, Targets: []string{"postgres"}, Subsumes: []string{"XY101"}},
			`.*rule XY101 subsumes itself`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := test.rule.Validate()
			c.Assert(err, qt.ErrorIs, lintext.ErrInvalid)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// A rule judges the targets it names, and every rule judges a run that
// names none; a clone shares no slice with the rule.
func TestRule_JudgesAndClone(t *testing.T) {
	c := qt.New(t)
	rule := validRule()
	clone := rule.Clone()
	clone.Targets[0] = "mysql"

	c.Assert(rule.Judges("postgres"), qt.IsTrue)
	c.Assert(rule.Judges(""), qt.IsTrue)
	c.Assert(rule.Judges("mysql"), qt.IsFalse)
	c.Assert(rule.Targets, qt.DeepEquals, []string{"postgres"})
}

// splitOf is a request over one source and a result splitting it as given.
func splitOf(sql string, statements ...lintext.Statement) (lintext.SplitRequest, lintext.SplitResult) {
	return lintext.SplitRequest{Target: "postgres", Sources: []lintext.Source{{Name: "1.up.sql", SQL: sql}}},
		lintext.SplitResult{Files: [][]lintext.Statement{statements}}
}

func TestValidateSplit_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := splitOf("DROP TABLE a;\nDROP TABLE b;",
		lintext.Statement{Start: 0, End: 12, Line: 1, SQL: "DROP TABLE a"}, lintext.Statement{Start: 14, End: 26, Line: 2, SQL: "DROP TABLE b"})

	c.Assert(lintext.ValidateSplit(request, result), qt.IsNil)
}

func TestValidateSplit_FailurePath(t *testing.T) {
	const sql = "DROP TABLE a;\nDROP TABLE b;"
	for _, test := range []struct {
		name       string
		statements []lintext.Statement
		files      int
	}{
		{"text other than the span", []lintext.Statement{{Start: 0, End: 12, Line: 1, SQL: "DROP TABLE b"}}, 1},
		{"a span past the source", []lintext.Statement{{Start: 14, End: 40, Line: 2, SQL: "DROP TABLE b;"}}, 1},
		{"overlapping spans", []lintext.Statement{{Start: 0, End: 12, Line: 1, SQL: "DROP TABLE a"}, {Start: 5, End: 12, Line: 1, SQL: "TABLE a"}}, 1},
		{"no line", []lintext.Statement{{Start: 0, End: 12, SQL: "DROP TABLE a"}}, 1},
		{"a list per source missing", nil, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := splitOf(sql, test.statements...)
			result.Files = result.Files[:test.files]
			c.Assert(lintext.ValidateSplit(request, result), qt.ErrorIs, lintext.ErrInvalid)
		})
	}
}

// checkRequest enables XY101 over one up file of two statements.
func checkRequest() lintext.CheckRequest {
	return lintext.CheckRequest{Target: lintext.Target{Name: "postgres"}, Codes: []string{"XY101"}, Files: []lintext.File{{
		Name: "1.up.sql", Up: true, Statements: []lintext.Statement{
			{Start: 0, End: 12, Line: 1, SQL: "DROP TABLE a", Words: []string{"DROP", "TABLE", "A"}},
			{Start: 14, End: 30, Line: 2, SQL: "CREATE TABLE b()", Words: []string{"CREATE", "TABLE", "B", "(", ")"}},
		},
	}}}
}

func TestValidateCheck_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		result lintext.CheckResult
	}{
		{"incomplete", lintext.CheckResult{}},
		{"a code not enabled", lintext.CheckResult{Complete: true, Findings: []lintext.Finding{{Code: "XY102", Statement: 0, Message: "m"}}}},
		{"a file not held", lintext.CheckResult{Complete: true, Findings: []lintext.Finding{{Code: "XY101", File: 1, Message: "m"}}}},
		{"a statement not held", lintext.CheckResult{Complete: true, Findings: []lintext.Finding{{Code: "XY101", Statement: 2, Message: "m"}}}},
		{"below the file", lintext.CheckResult{Complete: true, Findings: []lintext.Finding{{Code: "XY101", Statement: -2, Message: "m"}}}},
		{"no message", lintext.CheckResult{Complete: true, Findings: []lintext.Finding{{Code: "XY101"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lintext.ValidateCheck(checkRequest(), test.result), qt.ErrorIs, lintext.ErrInvalid)
		})
	}
}

// dropCheck finds DROP TABLE statements.
func dropCheck(appliesToDown bool) lintext.Check {
	rule := validRule()
	rule.AppliesToDown = appliesToDown
	return lintext.Check{Rule: rule, Statement: func(_ lintext.Target, _ *lintext.File, statement *lintext.Statement) (bool, string) {
		return len(statement.Words) > 1 && statement.Words[0] == "DROP" && statement.Words[1] == "TABLE", "drops a table"
	}}
}

// Checks judge the statements of an up file, of a down file only where the
// rule applies to down, and a file check every file; the result is complete
// and answers its request.
func TestChecks_CheckMigrations_HappyPath(t *testing.T) {
	down := checkRequest()
	down.Files[0].Up, down.Files[0].Down = false, true
	fileCheck := lintext.Check{Rule: lintext.Rule{Code: "XY103", Title: "t", Severity: lintext.Warning, Targets: []string{"postgres"}},
		File: func(_ lintext.Target, files []lintext.File, index int) []lintext.Violation {
			return []lintext.Violation{{Statement: -1, Message: files[index].Name + " has " + strings.Repeat("s", len(files[index].Statements))}}
		}}
	for _, test := range []struct {
		name    string
		checks  lintext.Checks
		request lintext.CheckRequest
		want    []lintext.Finding
	}{
		{"an up file", lintext.Checks{dropCheck(false)}, checkRequest(),
			[]lintext.Finding{{Code: "XY101", File: 0, Statement: 0, Message: "drops a table"}}},
		{"a down file of an up-only rule", lintext.Checks{dropCheck(false)}, down, nil},
		{"a down file of a rule that applies to down", lintext.Checks{dropCheck(true)}, down,
			[]lintext.Finding{{Code: "XY101", File: 0, Statement: 0, Message: "drops a table"}}},
		{"a file check", lintext.Checks{dropCheck(false), fileCheck}, lintext.CheckRequest{Codes: []string{"XY103"}, Files: checkRequest().Files},
			[]lintext.Finding{{Code: "XY103", File: 0, Statement: -1, Message: "1.up.sql has ss"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := test.checks.CheckMigrations(t.Context(), test.request)

			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, lintext.CheckResult{Complete: true, Findings: test.want})
			c.Assert(lintext.ValidateCheck(test.request, result), qt.IsNil)
		})
	}
}

func TestChecks_CheckMigrations_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	both := dropCheck(false)
	both.File = func(lintext.Target, []lintext.File, int) []lintext.Violation { return nil }
	for _, test := range []struct {
		name    string
		ctx     context.Context
		checks  lintext.Checks
		wantErr string
	}{
		{"a code no check carries", context.Background(), lintext.Checks{}, `.*no check of rule XY101`},
		{"a check with both functions", context.Background(), lintext.Checks{both}, `.*check XY101 must set exactly one of Statement and File`},
		{"two checks of one code", context.Background(), lintext.Checks{dropCheck(false), dropCheck(true)}, `.*two checks of rule XY101`},
		{"cancellation", canceled, lintext.Checks{dropCheck(false)}, `context canceled`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := test.checks.CheckMigrations(test.ctx, checkRequest())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, lintext.CheckResult{})
		})
	}
}

func TestChecks_Rules(t *testing.T) {
	c := qt.New(t)
	checks := lintext.Checks{dropCheck(true)}

	rules := checks.Rules()
	rules[0].Targets[0] = "mysql"

	c.Assert(checks.Rules(), qt.DeepEquals, []lintext.Rule{{Code: "XY101", Title: "Drops a table", Severity: lintext.Error,
		Targets: []string{"postgres"}, AppliesToDown: true}})
}

func TestCanonicalCode(t *testing.T) {
	for _, test := range []struct {
		code string
		want bool
	}{{"DS101", true}, {"MF101P", true}, {"X", true}, {"", false}, {"ds101", false}, {"1DS", false}, {"DS-1", false}} {
		t.Run(test.code, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lintext.CanonicalCode(test.code), qt.Equals, test.want)
		})
	}
}
