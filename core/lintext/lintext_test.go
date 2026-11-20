package lintext_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/core/schemaext"
)

func validRule() lintext.Rule {
	return lintext.Rule{Name: "test/drop-table", Title: "Drops a table", Severity: lintext.Error, Targets: []string{"postgres"}}
}

func TestRule_Validate_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		rule lintext.Rule
	}{
		{"statement text", validRule()},
		{"a routine body, two targets, down and a subsumption", lintext.Rule{Name: "test/info-rule", Title: "t", Severity: lintext.Info,
			Targets: []string{"mysql", "mariadb"}, AppliesToDown: true, Input: lintext.InputRoutineBody, Subsumes: []string{"test/drop-table"}}},
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
		{"a code-shaped name", lintext.Rule{Name: "XY101", Title: "t", Severity: lintext.Error, Targets: []string{"postgres"}},
			`.*rule name "XY101" must be a category and a rule joined by a slash.*`},
		{"a name without a category", lintext.Rule{Name: "drop-table", Title: "t", Severity: lintext.Error, Targets: []string{"postgres"}},
			`.*rule name "drop-table" must be a category.*`},
		{"no title", lintext.Rule{Name: "test/drop-table", Title: " ", Severity: lintext.Error, Targets: []string{"postgres"}}, `.*rule test/drop-table has no title`},
		{"an unknown severity", lintext.Rule{Name: "test/drop-table", Title: "t", Severity: "fatal", Targets: []string{"postgres"}},
			`.*rule test/drop-table has unknown severity "fatal"`},
		{"an unknown input", lintext.Rule{Name: "test/drop-table", Title: "t", Severity: lintext.Error, Targets: []string{"postgres"}, Input: "history"},
			`.*rule test/drop-table has unknown input "history"`},
		{"no target", lintext.Rule{Name: "test/drop-table", Title: "t", Severity: lintext.Error}, `.*rule test/drop-table names no target`},
		{"a repeated target", lintext.Rule{Name: "test/drop-table", Title: "t", Severity: lintext.Error, Targets: []string{"postgres", "postgres"}},
			`.*rule test/drop-table names a blank or repeated target`},
		{"a blank target", lintext.Rule{Name: "test/drop-table", Title: "t", Severity: lintext.Error, Targets: []string{" "}},
			`.*rule test/drop-table names a blank or repeated target`},
		{"itself subsumed", lintext.Rule{Name: "test/drop-table", Title: "t", Severity: lintext.Error, Targets: []string{"postgres"}, Subsumes: []string{"test/drop-table"}},
			`.*rule test/drop-table subsumes itself`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := test.rule.Validate()
			c.Assert(err, qt.ErrorIs, lintext.ErrInvalid)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// The two baseline inputs read the state a version starts from; the text and
// a routine body do not.
func TestInput_ReadsBaseline(t *testing.T) {
	for _, test := range []struct {
		input lintext.Input
		want  bool
	}{
		{lintext.InputStatementText, false},
		{lintext.InputRoutineBody, false},
		{lintext.InputBaselineSchema, true},
		{lintext.InputBaselineRefinement, true},
	} {
		t.Run(string(test.input), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.input.ReadsBaseline(), qt.Equals, test.want)
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

// A split may judge facts and report effects of them, and an owner's safety
// judgment of either impact it may make.
func TestValidateSplit_Effects_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := splitOf("DROP TABLE a;\nDROP TABLE b;",
		lintext.Statement{Start: 0, End: 12, Line: 1, SQL: "DROP TABLE a", Effects: []lintext.Effect{
			{Fact: lintext.FactRebuildDrop},
			{Fact: lintext.FactSafetyEffect, Impact: schemaext.Destructive, Reason: "drops a table"},
		}},
		lintext.Statement{Start: 14, End: 26, Line: 2, SQL: "DROP TABLE b", Effects: []lintext.Effect{
			{Fact: lintext.FactSafetyEffect, Impact: schemaext.Behavioral, Reason: "moves a table aside"},
		}})
	result.Judged = []lintext.Fact{lintext.FactRebuildDrop, lintext.FactRebuildRename, lintext.FactStatefulDrop, lintext.FactSafetyEffect}

	c.Assert(lintext.ValidateSplit(request, result), qt.IsNil)
}

func TestValidateSplit_FailurePath(t *testing.T) {
	const sql = "DROP TABLE a;\nDROP TABLE b;"
	dropA := lintext.Statement{Start: 0, End: 12, Line: 1, SQL: "DROP TABLE a"}
	for _, test := range []struct {
		name       string
		statements []lintext.Statement
		files      int
		judged     []lintext.Fact
		wantErr    string
	}{
		{"text other than the span", []lintext.Statement{{Start: 0, End: 12, Line: 1, SQL: "DROP TABLE b"}}, 1, nil,
			`.*statement 0 of 1.up.sql does not match its source`},
		{"a span past the source", []lintext.Statement{{Start: 14, End: 40, Line: 2, SQL: "DROP TABLE b;"}}, 1, nil,
			`.*statement 0 of 1.up.sql does not match its source`},
		{"overlapping spans", []lintext.Statement{dropA, {Start: 5, End: 12, Line: 1, SQL: "TABLE a"}}, 1, nil,
			`.*statement 1 of 1.up.sql does not match its source`},
		{"no line", []lintext.Statement{{Start: 0, End: 12, SQL: "DROP TABLE a"}}, 1, nil,
			`.*statement 0 of 1.up.sql does not match its source`},
		{"a list per source missing", nil, 0, nil, `.*a split of 1 sources returned 0`},
		{"an unknown fact judged", []lintext.Statement{dropA}, 1, []lintext.Fact{"loses-sleep"},
			`.*a split judges unknown fact "loses-sleep"`},
		{"one rebuild fact judged without the other", []lintext.Statement{dropA}, 1, []lintext.Fact{lintext.FactRebuildDrop},
			`.*a split judges "rebuild-drop" and "rebuild-rename" together or neither`},
		{"an effect of a fact not judged", []lintext.Statement{{Start: 0, End: 12, Line: 1, SQL: "DROP TABLE a",
			Effects: []lintext.Effect{{Fact: lintext.FactUniqueIndex}}}}, 1, []lintext.Fact{lintext.FactStatefulDrop},
			`.*statement 0 of 1.up.sql reports "unique-index", which the split does not judge`},
		{"an additive judgment", judging(lintext.Effect{Fact: lintext.FactSafetyEffect, Impact: schemaext.Additive, Reason: "adds"}),
			1, judgedFacts, `.*statement 0 of 1.up.sql judges impact "additive", which is not "behavioral" or "destructive"`},
		{"a judgment without an impact", judging(lintext.Effect{Fact: lintext.FactSafetyEffect, Reason: "drops"}),
			1, judgedFacts, `.*statement 0 of 1.up.sql judges impact "", which is not "behavioral" or "destructive"`},
		{"a judgment without a reason", judging(lintext.Effect{Fact: lintext.FactSafetyEffect, Impact: schemaext.Destructive, Reason: " "}),
			1, judgedFacts, `.*statement 0 of 1.up.sql judges impact "destructive" without a reason`},
		{"two judgments of one statement", judging(
			lintext.Effect{Fact: lintext.FactSafetyEffect, Impact: schemaext.Destructive, Reason: "drops"},
			lintext.Effect{Fact: lintext.FactSafetyEffect, Impact: schemaext.Behavioral, Reason: "moves"}),
			1, judgedFacts, `.*statement 0 of 1.up.sql reports "safety-effect" twice`},
		{"an impact on another fact", judging(lintext.Effect{Fact: lintext.FactStatefulDrop, Impact: schemaext.Destructive}),
			1, judgedFacts, `.*statement 0 of 1.up.sql carries an impact or a reason on "stateful-drop", which only "safety-effect" carries`},
		{"a reason on another fact", judging(lintext.Effect{Fact: lintext.FactStatefulDrop, Reason: "drops"}),
			1, judgedFacts, `.*statement 0 of 1.up.sql carries an impact or a reason on "stateful-drop", which only "safety-effect" carries`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := splitOf(sql, test.statements...)
			result.Files = result.Files[:test.files]
			result.Judged = test.judged
			err := lintext.ValidateSplit(request, result)
			c.Assert(err, qt.ErrorIs, lintext.ErrInvalid)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// judgedFacts are the facts the failing splits of one statement judge.
var judgedFacts = []lintext.Fact{lintext.FactStatefulDrop, lintext.FactSafetyEffect}

// judging is the statement DROP TABLE a of a split, with effects.
func judging(effects ...lintext.Effect) []lintext.Statement {
	return []lintext.Statement{{Start: 0, End: 12, Line: 1, SQL: "DROP TABLE a", Effects: effects}}
}

// annotateRequest asks about stateful drops and safety effects in one file of
// two statements.
func annotateRequest() lintext.AnnotateRequest {
	const sql = "DROP TOPIC t;\nSELECT 1;"
	return lintext.AnnotateRequest{Facts: []lintext.Fact{lintext.FactStatefulDrop, lintext.FactSafetyEffect},
		Sources: []lintext.Source{{Name: "1.up.sql", SQL: sql}},
		Files: [][]lintext.Statement{{
			{Start: 0, End: 12, Line: 1, SQL: "DROP TOPIC t"},
			{Start: 14, End: 22, Line: 2, SQL: "SELECT 1"},
		}}}
}

// An answer judges each statement: an effect the request asked about, or a
// statement declined with none.
func TestValidateAnnotate_HappyPath(t *testing.T) {
	c := qt.New(t)
	request := annotateRequest()

	c.Assert(lintext.ValidateAnnotateRequest(request), qt.IsNil)
	c.Assert(lintext.ValidateAnnotate(request, lintext.AnnotateResult{Files: [][]lintext.Annotation{{
		{Effects: []lintext.Effect{
			{Fact: lintext.FactStatefulDrop, Object: "t", Kind: "TOPIC"},
			{Fact: lintext.FactSafetyEffect, Impact: schemaext.Destructive, Reason: "drops a topic"},
		}},
		{Declined: true},
	}}}), qt.IsNil)
}

func TestValidateAnnotate_FailurePath(t *testing.T) {
	topic := []lintext.Effect{{Fact: lintext.FactStatefulDrop}}
	for _, test := range []struct {
		name    string
		files   [][]lintext.Annotation
		wantErr string
	}{
		{"a file missing", nil, `.*an annotation of 1 files returned 0`},
		{"a statement missing", [][]lintext.Annotation{{{}}}, `.*file 0 holds 2 statements and 1 annotations`},
		{"a declined statement with an effect", [][]lintext.Annotation{{{Effects: topic, Declined: true}, {}}},
			`.*statement 0 of file 0 is declined and carries effects`},
		{"an effect the request did not ask about", [][]lintext.Annotation{{{}, {Effects: []lintext.Effect{{Fact: lintext.FactUniqueIndex}}}}},
			`.*statement 1 of file 0 reports "unique-index", which the request did not ask about`},
		{"an additive judgment", [][]lintext.Annotation{{{Effects: []lintext.Effect{
			{Fact: lintext.FactSafetyEffect, Impact: schemaext.Additive, Reason: "adds"}}}, {}}},
			`.*statement 0 of file 0 judges impact "additive", which is not "behavioral" or "destructive"`},
		{"a reason on another fact", [][]lintext.Annotation{{{}, {Effects: []lintext.Effect{
			{Fact: lintext.FactStatefulDrop, Reason: "drops"}}}}},
			`.*statement 1 of file 0 carries an impact or a reason on "stateful-drop", which only "safety-effect" carries`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := lintext.ValidateAnnotate(annotateRequest(), lintext.AnnotateResult{Files: test.files})
			c.Assert(err, qt.ErrorIs, lintext.ErrInvalid)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// Each statement list comes with the source it was split from.
func TestValidateAnnotateRequest_FailurePath(t *testing.T) {
	c := qt.New(t)
	request := annotateRequest()
	request.Sources = nil

	err := lintext.ValidateAnnotateRequest(request)

	c.Assert(err, qt.ErrorIs, lintext.ErrInvalid)
	c.Assert(err, qt.ErrorMatches, `.*an annotation request holds 0 sources and 1 statement lists`)
}

// checkRequest enables test/drop-table over one up file of two statements.
func checkRequest() lintext.CheckRequest {
	return lintext.CheckRequest{Target: lintext.Target{Name: "postgres"}, Names: []string{"test/drop-table"}, Files: []lintext.File{{
		Name: "1.up.sql", Up: true, Statements: []lintext.Statement{
			{Start: 0, End: 12, Line: 1, SQL: "DROP TABLE a", Words: []string{"DROP", "TABLE", "A"}},
			{Start: 14, End: 30, Line: 2, SQL: "CREATE TABLE b()", Words: []string{"CREATE", "TABLE", "B", "(", ")"}},
		},
	}}}
}

// A finding about a statement may name its subjects, and a baseline rule may
// name the statements it wants the state for.
func TestValidateCheck_HappyPath(t *testing.T) {
	c := qt.New(t)
	result := lintext.CheckResult{Complete: true,
		Findings: []lintext.Finding{
			{Name: "test/drop-table", Statement: 0, Message: "m", Subjects: []lintext.Subject{
				{Kind: lintext.SubjectTable, Name: "a"}, {Kind: lintext.SubjectColumn, Name: "id", Parent: "a", DataType: "int"}}},
			{Name: "test/drop-table", Statement: -1, Message: "m"},
		},
		BaselineSubjects: []lintext.BaselineSubject{{Name: "test/drop-table", File: 0, Statements: []int{0, 1}}},
	}

	c.Assert(lintext.ValidateCheck(checkRequest(), result), qt.IsNil)
}

func TestValidateCheck_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		result lintext.CheckResult
	}{
		{"incomplete", lintext.CheckResult{}},
		{"a name not enabled", lintext.CheckResult{Complete: true, Findings: []lintext.Finding{{Name: "test/other-rule", Statement: 0, Message: "m"}}}},
		{"a file not held", lintext.CheckResult{Complete: true, Findings: []lintext.Finding{{Name: "test/drop-table", File: 1, Message: "m"}}}},
		{"a statement not held", lintext.CheckResult{Complete: true, Findings: []lintext.Finding{{Name: "test/drop-table", Statement: 2, Message: "m"}}}},
		{"below the file", lintext.CheckResult{Complete: true, Findings: []lintext.Finding{{Name: "test/drop-table", Statement: -2, Message: "m"}}}},
		{"no message", lintext.CheckResult{Complete: true, Findings: []lintext.Finding{{Name: "test/drop-table"}}}},
		{"a subject of a whole file", lintext.CheckResult{Complete: true, Findings: []lintext.Finding{{Name: "test/drop-table", Statement: -1, Message: "m",
			Subjects: []lintext.Subject{{Kind: lintext.SubjectTable, Name: "a"}}}}}},
		{"a subject of no known kind", lintext.CheckResult{Complete: true, Findings: []lintext.Finding{{Name: "test/drop-table", Message: "m",
			Subjects: []lintext.Subject{{Kind: "index", Name: "a"}}}}}},
		{"a subject without a name", lintext.CheckResult{Complete: true, Findings: []lintext.Finding{{Name: "test/drop-table", Message: "m",
			Subjects: []lintext.Subject{{Kind: lintext.SubjectColumn}}}}}},
		{"a baseline subject of a name not enabled", lintext.CheckResult{Complete: true,
			BaselineSubjects: []lintext.BaselineSubject{{Name: "test/other-rule", Statements: []int{0}}}}},
		{"a baseline subject of a file not held", lintext.CheckResult{Complete: true,
			BaselineSubjects: []lintext.BaselineSubject{{Name: "test/drop-table", File: 1, Statements: []int{0}}}}},
		{"a baseline subject of a statement not held", lintext.CheckResult{Complete: true,
			BaselineSubjects: []lintext.BaselineSubject{{Name: "test/drop-table", Statements: []int{0, 2}}}}},
		{"a baseline subject below the statements", lintext.CheckResult{Complete: true,
			BaselineSubjects: []lintext.BaselineSubject{{Name: "test/drop-table", Statements: []int{-1}}}}},
		{"a baseline subject naming no statement", lintext.CheckResult{Complete: true,
			BaselineSubjects: []lintext.BaselineSubject{{Name: "test/drop-table"}}}},
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
	fileCheck := lintext.Check{Rule: lintext.Rule{Name: "test/file-rule", Title: "t", Severity: lintext.Warning, Targets: []string{"postgres"}},
		File: func(_ lintext.Target, files []lintext.File, index int) []lintext.Violation {
			return []lintext.Violation{{Statement: -1, Message: files[index].Name + " has " + strings.Repeat("s", len(files[index].Statements))}}
		}}
	subjectCheck := lintext.Check{Rule: lintext.Rule{Name: "test/subject-rule", Title: "t", Severity: lintext.Warning, Targets: []string{"postgres"}},
		File: func(lintext.Target, []lintext.File, int) []lintext.Violation {
			return []lintext.Violation{{Statement: 1, Message: "creates b", Subjects: []lintext.Subject{{Kind: lintext.SubjectTable, Name: "b"}}}}
		}}
	for _, test := range []struct {
		name    string
		checks  lintext.Checks
		request lintext.CheckRequest
		want    []lintext.Finding
	}{
		{"an up file", lintext.Checks{dropCheck(false)}, checkRequest(),
			[]lintext.Finding{{Name: "test/drop-table", File: 0, Statement: 0, Message: "drops a table"}}},
		{"a down file of an up-only rule", lintext.Checks{dropCheck(false)}, down, nil},
		{"a down file of a rule that applies to down", lintext.Checks{dropCheck(true)}, down,
			[]lintext.Finding{{Name: "test/drop-table", File: 0, Statement: 0, Message: "drops a table"}}},
		{"a file check", lintext.Checks{dropCheck(false), fileCheck}, lintext.CheckRequest{Names: []string{"test/file-rule"}, Files: checkRequest().Files},
			[]lintext.Finding{{Name: "test/file-rule", File: 0, Statement: -1, Message: "1.up.sql has ss"}}},
		{"a file check naming subjects", lintext.Checks{subjectCheck}, lintext.CheckRequest{Names: []string{"test/subject-rule"}, Files: checkRequest().Files},
			[]lintext.Finding{{Name: "test/subject-rule", File: 0, Statement: 1, Message: "creates b",
				Subjects: []lintext.Subject{{Kind: lintext.SubjectTable, Name: "b"}}}}},
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
	baselineCheck := dropCheck(false)
	baselineCheck.Rule.Input = lintext.InputBaselineRefinement
	for _, test := range []struct {
		name    string
		ctx     context.Context
		checks  lintext.Checks
		wantErr string
	}{
		{"a name no check carries", context.Background(), lintext.Checks{}, `.*no check of rule test/drop-table`},
		{"a check with both functions", context.Background(), lintext.Checks{both}, `.*check test/drop-table must set exactly one of Statement and File`},
		{"two checks of one name", context.Background(), lintext.Checks{dropCheck(false), dropCheck(true)}, `.*two checks of rule test/drop-table`},
		{"a check whose rule reads a baseline", context.Background(), lintext.Checks{baselineCheck},
			`.*check test/drop-table reads a baseline, which Checks cannot ask for; implement Service`},
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

	c.Assert(checks.Rules(), qt.DeepEquals, []lintext.Rule{{Name: "test/drop-table", Title: "Drops a table", Severity: lintext.Error,
		Targets: []string{"postgres"}, AppliesToDown: true}})
}

func TestCanonicalName(t *testing.T) {
	for _, test := range []struct {
		name string
		want bool
	}{
		{"locking/add-foreign-key", true}, {"ydb/add-column-form", true}, {"naming/identifier-too-long2", true},
		{"", false}, {"DS101", false}, {"locking", false}, {"locking/", false}, {"/add", false},
		{"Locking/add", false}, {"locking/Add", false}, {"locking/add-", false}, {"locking/-add", false},
		{"locking/add--check", false}, {"locking/add/check", false}, {"1locking/add", false}, {"locking/1add", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lintext.CanonicalName(test.name), qt.Equals, test.want)
		})
	}
}
