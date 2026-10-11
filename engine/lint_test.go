package engine_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/lintext"
	"ptah.run/core/ptaherr"
	"ptah.run/engine"
)

// countingChecks is a lint service that counts its calls and answers with
// the checks behind it, or fails.
type countingChecks struct {
	checks  lintext.Checks
	calls   *int
	failure error
	reply   *lintext.CheckResult
}

func (s countingChecks) CheckMigrations(ctx context.Context, request lintext.CheckRequest) (lintext.CheckResult, error) {
	*s.calls++
	result, err := s.checks.CheckMigrations(ctx, request)
	return *cmpOr(s.reply, &result), errors.Join(err, s.failure)
}

// cmpOr is the first non-nil pointer.
func cmpOr[T any](values ...*T) *T {
	return values[slices.IndexFunc(values, func(value *T) bool { return value != nil })]
}

// wordCheck finds statements holding word, as rule code on targets.
func wordCheck(code, word string, targets ...string) lintext.Check {
	return lintext.Check{Rule: lintext.Rule{Code: code, Title: code, Severity: lintext.Warning, Targets: targets},
		Statement: func(_ lintext.Target, _ *lintext.File, statement *lintext.Statement) (bool, string) {
			return slices.Contains(statement.Words, word), "holds " + word
		}}
}

// upperSplitter splits on semicolons and uppercases words, recording the
// target it was asked for.
type upperSplitter struct{ asked *[]string }

func (s upperSplitter) SplitStatements(_ context.Context, request lintext.SplitRequest) (lintext.SplitResult, error) {
	*s.asked = append(*s.asked, request.Target)
	result := lintext.SplitResult{}
	for _, source := range request.Sources {
		var statements []lintext.Statement
		start := 0
		for part := range strings.SplitSeq(source.SQL, ";") {
			text := strings.TrimSpace(part)
			offset := start + strings.Index(part, text)
			start += len(part) + 1
			statements = append(statements, lintext.Statement{Start: offset, End: offset + len(text), Line: 1, SQL: text,
				Words: strings.Fields(strings.ToUpper(text))})
		}
		result.Files = append(result.Files, statements)
	}
	return result, nil
}

// lintRuntime holds targets alpha (alias a) and beta, alpha splitting with
// splitter, and two owners: XY101 on alpha and XY102 on both, then XY201 on
// alpha.
func lintRuntime(c *qt.C, splitter lintext.Splitter, first, second countingChecks) *engine.Runtime {
	c.Helper()
	return must.Must(engine.New(
		engine.Provider{ID: "example.org/one", Targets: []engine.Target{{Name: "alpha", Aliases: []string{"a"}, Splitter: splitter}, {Name: "beta"}},
			Lint: []engine.Lint{{Rules: first.checks.Rules(), Service: first}}},
		engine.Provider{ID: "example.org/two", Lint: []engine.Lint{{Rules: second.checks.Rules(), Service: second}}},
	))
}

func defaultOwners() (first, second countingChecks) {
	return countingChecks{checks: lintext.Checks{wordCheck("XY101", "DROP", "alpha"), wordCheck("XY102", "TRUNCATE", "a", "beta")}, calls: new(0)},
		countingChecks{checks: lintext.Checks{wordCheck("XY201", "GRANT", "alpha")}, calls: new(0)}
}

// LintRules reads the published rules without calling an owner: those that
// judge a target, under canonical target names, and every rule for a run
// that names none.
func TestRuntime_LintRules_HappyPath(t *testing.T) {
	first, second := defaultOwners()
	for _, test := range []struct {
		target string
		want   []string
	}{
		{"alpha", []string{"XY101", "XY102", "XY201"}},
		{"a", []string{"XY101", "XY102", "XY201"}},
		{"beta", []string{"XY102"}},
		{"", []string{"XY101", "XY102", "XY201"}},
	} {
		t.Run(test.target, func(t *testing.T) {
			c := qt.New(t)
			runtime := lintRuntime(c, nil, first, second)

			rules, err := runtime.LintRules(test.target)

			c.Assert(err, qt.IsNil)
			codes := make([]string, 0, len(rules))
			for _, rule := range rules {
				codes = append(codes, rule.Code)
			}
			c.Assert(codes, qt.DeepEquals, test.want)
			c.Assert(rules[slices.IndexFunc(rules, func(rule lintext.Rule) bool { return rule.Code == "XY102" })].Targets,
				qt.DeepEquals, []string{"alpha", "beta"})
			c.Assert(*first.calls+*second.calls, qt.Equals, 0)
		})
	}
}

func TestRuntime_LintRules_FailurePath(t *testing.T) {
	c := qt.New(t)
	first, second := defaultOwners()

	rules, err := lintRuntime(c, nil, first, second).LintRules("gamma")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(rules, qt.IsNil)
}

func TestNew_LintFailurePath(t *testing.T) {
	checks := lintext.Checks{wordCheck("XY101", "DROP", "alpha")}
	service := countingChecks{checks: checks, calls: new(0)}
	target := []engine.Target{{Name: "alpha"}}
	for _, test := range []struct {
		name      string
		providers []engine.Provider
		wantErr   string
	}{
		{"rules without a service", []engine.Provider{{ID: "example.org/one", Targets: target, Lint: []engine.Lint{{Rules: checks.Rules()}}}},
			`.*publishes lint rules without a service, or a service without rules`},
		{"a service without rules", []engine.Provider{{ID: "example.org/one", Targets: target, Lint: []engine.Lint{{Service: service}}}},
			`.*publishes lint rules without a service, or a service without rules`},
		{"an invalid rule", []engine.Provider{{ID: "example.org/one", Targets: target, Lint: []engine.Lint{{
			Rules: []lintext.Rule{{Code: "XY101", Title: "t", Severity: "fatal", Targets: []string{"alpha"}}}, Service: service}}}},
			`.*rule XY101 has unknown severity "fatal"`},
		{"an unregistered target", []engine.Provider{{ID: "example.org/one", Targets: []engine.Target{{Name: "beta"}},
			Lint: []engine.Lint{{Rules: checks.Rules(), Service: service}}}}, `.*lint rule XY101 judges unregistered target "alpha"`},
		{"one code from two owners", []engine.Provider{
			{ID: "example.org/one", Targets: target, Lint: []engine.Lint{{Rules: checks.Rules(), Service: service}}},
			{ID: "example.org/two", Lint: []engine.Lint{{Rules: checks.Rules(), Service: service}}},
		}, `.*lint rule XY101 is published by "example.org/one" and "example.org/two"`},
		{"a typed-nil splitter", []engine.Provider{{ID: "example.org/one", Targets: []engine.Target{{Name: "alpha", Splitter: (*upperSplitter)(nil)}}}},
			`.*target "alpha" has a typed-nil statement splitter service`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			runtime, err := engine.New(test.providers...)

			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(runtime, qt.IsNil)
		})
	}
}

// runRequest enables codes over one up file holding a DROP, a TRUNCATE and
// a GRANT.
func runRequest(target string, codes ...string) lintext.CheckRequest {
	return lintext.CheckRequest{Target: lintext.Target{Name: target}, Codes: codes, Files: []lintext.File{{Name: "1.up.sql", Up: true,
		Statements: []lintext.Statement{
			{Start: 0, End: 1, Line: 1, SQL: "x", Words: []string{"GRANT"}},
			{Start: 2, End: 3, Line: 2, SQL: "y", Words: []string{"TRUNCATE", "DROP"}},
		}}}}
}

// CheckMigrations sends each owner its codes in one call and joins the
// findings in file, statement and code order.
func TestRuntime_CheckMigrations_HappyPath(t *testing.T) {
	c := qt.New(t)
	first, second := defaultOwners()
	runtime := lintRuntime(c, nil, first, second)

	result, err := runtime.CheckMigrations(t.Context(), runRequest("a", "XY201", "XY101", "XY102"))

	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.DeepEquals, lintext.CheckResult{Complete: true, Findings: []lintext.Finding{
		{Code: "XY201", File: 0, Statement: 0, Message: "holds GRANT"},
		{Code: "XY101", File: 0, Statement: 1, Message: "holds DROP"},
		{Code: "XY102", File: 0, Statement: 1, Message: "holds TRUNCATE"},
	}})
	c.Assert([]int{*first.calls, *second.calls}, qt.DeepEquals, []int{1, 1})
}

func TestRuntime_CheckMigrations_FailurePath(t *testing.T) {
	failure := errors.New("plugin closed")
	for _, test := range []struct {
		name    string
		request lintext.CheckRequest
		second  countingChecks
		wantIs  error
		wantErr string
	}{
		{"a code no owner publishes", runRequest("alpha", "XY999"), countingChecks{}, lintext.ErrInvalid, `.*no published lint rule XY999 judges target "alpha".*`},
		{"a rule that does not judge the target", runRequest("beta", "XY101"), countingChecks{}, lintext.ErrInvalid, `.*no published lint rule XY101 judges target "beta".*`},
		{"a code asked for twice", runRequest("alpha", "XY101", "XY101"), countingChecks{}, lintext.ErrInvalid, `.*or it is asked for twice`},
		{"an unregistered target", runRequest("gamma", "XY101"), countingChecks{}, ptaherr.ErrUnsupportedDialect, `.*lint target "gamma" is not registered`},
		{"an incomplete reply", runRequest("alpha", "XY201"), countingChecks{reply: &lintext.CheckResult{}}, lintext.ErrInvalid,
			`lint rules of "example.org/two": .*a lint check did not complete`},
		{"a reply naming another rule", runRequest("alpha", "XY201"), countingChecks{reply: &lintext.CheckResult{Complete: true,
			Findings: []lintext.Finding{{Code: "XY101", Message: "m"}}}}, lintext.ErrInvalid, `lint rules of "example.org/two": .*finding XY101.*`},
		{"an owner's failure", runRequest("alpha", "XY201"), countingChecks{failure: failure}, failure, `plugin closed`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			first, second := defaultOwners()
			second.reply, second.failure = test.second.reply, test.second.failure

			result, err := lintRuntime(c, nil, first, second).CheckMigrations(t.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, lintext.CheckResult{})
		})
	}
}

// SplitStatements asks the target's splitter under the canonical name and
// returns its answer.
func TestRuntime_SplitStatements_HappyPath(t *testing.T) {
	c := qt.New(t)
	var asked []string
	first, second := defaultOwners()
	runtime := lintRuntime(c, upperSplitter{asked: &asked}, first, second)

	result, err := runtime.SplitStatements(t.Context(), lintext.SplitRequest{Target: "a", Sources: []lintext.Source{{Name: "1.up.sql", SQL: "drop x; grant y"}}})

	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.DeepEquals, lintext.SplitResult{Files: [][]lintext.Statement{{
		{Start: 0, End: 6, Line: 1, SQL: "drop x", Words: []string{"DROP", "X"}},
		{Start: 8, End: 15, Line: 1, SQL: "grant y", Words: []string{"GRANT", "Y"}},
	}}})
	c.Assert(asked, qt.DeepEquals, []string{"alpha"})
}

// badSplitter answers with a statement that is not its source's.
type badSplitter struct{}

func (badSplitter) SplitStatements(context.Context, lintext.SplitRequest) (lintext.SplitResult, error) {
	return lintext.SplitResult{Files: [][]lintext.Statement{{{Start: 0, End: 2, Line: 1, SQL: "no"}}}}, nil
}

func TestRuntime_SplitStatements_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name     string
		splitter lintext.Splitter
		target   string
		wantIs   error
		wantErr  string
	}{
		{"a target without a splitter", nil, "beta", ptaherr.ErrUnsupportedDialect, `.*no statement splitter for target "beta"`},
		{"an unregistered target", nil, "gamma", ptaherr.ErrUnsupportedDialect, `.*no statement splitter for target "gamma"`},
		{"a reply that is not the source's", badSplitter{}, "alpha", lintext.ErrInvalid, `statement splitter of "example.org/one": .*statement 0 of 1.up.sql.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			first, second := defaultOwners()

			result, err := lintRuntime(c, test.splitter, first, second).SplitStatements(t.Context(),
				lintext.SplitRequest{Target: test.target, Sources: []lintext.Source{{Name: "1.up.sql", SQL: "drop x"}}})

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, lintext.SplitResult{})
		})
	}
}
