package lint_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/lintext"
	"ptah.run/engine"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/lintscan"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// scriptedOwner answers every check with the answer for whether the request
// carries a baseline, or fails, and records each request.
type scriptedOwner struct {
	requests *[]lintext.CheckRequest
	answers  map[bool]lintext.CheckResult
	failure  error
}

func (o scriptedOwner) CheckMigrations(_ context.Context, request lintext.CheckRequest) (lintext.CheckResult, error) {
	*o.requests = append(*o.requests, request)
	return o.answers[request.Baseline != nil], o.failure
}

// ownedRuntime registers postgres and mysql, each splitting with the bundled
// scanner, and one owner publishing rules and answering through owner.
func ownedRuntime(c *qt.C, owner scriptedOwner, rules ...lintext.Rule) *engine.Runtime {
	c.Helper()
	return must.Must(engine.New(engine.Provider{
		ID: "example.org/owner",
		Targets: []engine.Target{
			{Name: "postgres", Splitter: lintscan.Splitter{Dialect: "postgres"}},
			{Name: "mysql", Splitter: lintscan.Splitter{Dialect: "mysql"}},
		},
		Lint: []engine.Lint{{Rules: rules, Service: owner}},
	}))
}

// ownedRule is a text rule judging targets.
func ownedRule(code string, targets ...string) lintext.Rule {
	return lintext.Rule{Code: code, Title: code + " title", Severity: lintext.Warning, Targets: targets}
}

// selectMigration is a directory of one version whose up file holds two
// statements, the second on line three, and whose down file holds one.
func selectMigration() map[string]string {
	return map[string]string{
		"0000000001_x.down.sql": "SELECT 0;\n",
		"0000000001_x.up.sql":   "SELECT 1;\n-- the statement an owner reports\nSELECT 2;\n",
	}
}

// Rules holds this package's rules and then every rule an owner publishes,
// carrying the targets it judges as its dialects and neither checker, and a
// call's rules share nothing with the next call's.
func TestRules_HoldsTheOwnersRules(t *testing.T) {
	c := qt.New(t)
	runtime := ownedRuntime(c, scriptedOwner{}, ownedRule("XY101", "postgres", "mysql"))
	own := must.Must(lint.Rules(must.Must(engine.New())))

	rules := must.Must(lint.Rules(runtime))
	rules[len(rules)-1].Dialects[0] = "sqlite"

	again := must.Must(lint.Rules(runtime))
	c.Assert(codesOf(again[:len(own)]), qt.DeepEquals, codesOf(own))
	owned := again[len(own):]
	c.Assert(owned, qt.HasLen, 1)
	c.Assert([]any{owned[0].Code, owned[0].Title, owned[0].Severity, owned[0].Dialects, owned[0].Input},
		qt.DeepEquals, []any{"XY101", "XY101 title", lint.SeverityWarning, []string{"postgres", "mysql"}, lint.InputStatementText})
	c.Assert(owned[0].CheckStatement == nil && owned[0].CheckFile == nil, qt.IsTrue)
}

// codesOf lists the codes of rules, in order.
func codesOf(rules []lint.Rule) []string {
	codes := make([]string, 0, len(rules))
	for _, rule := range rules {
		codes = append(codes, rule.Code)
	}
	return codes
}

// The bundled runtime's owners publish the transaction family, which this
// package no longer checks itself.
func TestRules_TheBundledOwnersPublishTheTransactionFamily(t *testing.T) {
	c := qt.New(t)
	codes := codesOf(must.Must(lint.Rules(builtintest.Runtime())))
	own := codesOf(must.Must(lint.Rules(must.Must(engine.New()))))

	c.Assert(codes, qt.Contains, "TX101")
	c.Assert(codes, qt.Contains, "TX201")
	c.Assert(own, qt.Not(qt.Contains), "TX101")
	c.Assert(own, qt.Not(qt.Contains), "TX201")
}

func TestRules_FailurePath(t *testing.T) {
	c := qt.New(t)

	rules, err := lint.Rules(nil)

	c.Assert(err, qt.ErrorMatches, `lint rules require an engine runtime`)
	c.Assert(rules, qt.IsNil)
}

// An owner's finding reaches the report as one of this package's: under the
// rule's title and default severity, at its statement's line, with its
// subjects; the run asks the owner once, for the rules it enables on the
// target, over every file.
func TestAnalyzeFS_ReportsTheOwnersFindings(t *testing.T) {
	c := qt.New(t)
	var requests []lintext.CheckRequest
	owner := scriptedOwner{requests: &requests, answers: map[bool]lintext.CheckResult{false: {Complete: true,
		Findings: []lintext.Finding{{Code: "XY101", File: 1, Statement: 1, Message: "reads b",
			Subjects: []lintext.Subject{{Kind: lintext.SubjectColumn, Name: "id", Parent: "b", DataType: "int"}}}}}}}
	runtime := ownedRuntime(c, owner, ownedRule("XY101", "postgres"), ownedRule("XY102", "postgres"), ownedRule("XY103", "mysql"))

	findings, err := lint.LintFS(c.Context(), runtime, fixture(selectMigration()),
		lint.Options{Dialect: "postgres", Disabled: []string{"XY102"}, Compatibility: lint.CompatibilityProfileAtlas})

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.DeepEquals, []lint.Finding{{Rule: "XY101", Title: "XY101 title", Severity: lint.SeverityWarning,
		File: "0000000001_x.up.sql", Line: 3, Message: "reads b", Context: &lint.FindingContext{StatementIndex: 1,
			Subjects: []lint.Subject{{Kind: lint.SubjectColumn, Name: "id", Parent: "b", DataType: "int"}}}}})
	c.Assert(requests, qt.HasLen, 1)
	c.Assert([]any{requests[0].Target.Name, requests[0].Codes, requests[0].Compatibility, requests[0].Baseline},
		qt.DeepEquals, []any{"postgres", []string{"XY101"}, "atlas", (*lintext.Baseline)(nil)})
	c.Assert(requests[0].Files, qt.HasLen, 2)
	c.Assert([]any{requests[0].Files[0].Name, requests[0].Files[0].Down, requests[0].Files[1].Name, requests[0].Files[1].Up},
		qt.DeepEquals, []any{"0000000001_x.down.sql", true, "0000000001_x.up.sql", true})
	c.Assert(requests[0].Files[1].Statements[1].Line, qt.Equals, 3)
}

// The policy decides over an owner's finding as over any other: a nolint
// directive silences it, a configured severity replaces its default, and an
// excluded path drops it.
func TestAnalyzeFS_AppliesThePolicyToTheOwnersFindings(t *testing.T) {
	for _, test := range []struct {
		name string
		up   string
		opts lint.Options
		want []string
	}{
		{"reported", "SELECT 1;\n", lint.Options{Dialect: "postgres"}, []string{"XY101 warning"}},
		{"silenced by a directive", "-- ptah:nolint XY101\nSELECT 1;\n", lint.Options{Dialect: "postgres"}, make([]string, 0)},
		{"a configured severity", "SELECT 1;\n",
			lint.Options{Dialect: "postgres", RuleConfigs: map[string]lint.RuleConfig{"XY101": {Severity: lint.SeverityError}}},
			[]string{"XY101 error"}},
		{"an excluded path", "SELECT 1;\n",
			lint.Options{Dialect: "postgres", RuleConfigs: map[string]lint.RuleConfig{"XY101": {Exclude: []string{"*.up.sql"}}}},
			make([]string, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner := scriptedOwner{requests: new([]lintext.CheckRequest), answers: map[bool]lintext.CheckResult{false: {Complete: true,
				Findings: []lintext.Finding{{Code: "XY101", File: 1, Statement: 0, Message: "m"}}}}}

			findings, err := lint.LintFS(c.Context(), ownedRuntime(c, owner, ownedRule("XY101", "postgres")), fixture(map[string]string{
				"0000000001_x.down.sql": "SELECT 0;\n",
				"0000000001_x.up.sql":   test.up,
			}), test.opts)

			c.Assert(err, qt.IsNil)
			got := make([]string, 0, len(findings))
			for _, finding := range findings {
				got = append(got, finding.Rule+" "+string(finding.Severity))
			}
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// A run asks no owner when it enables none of the owners' rules or lints no
// file.
func TestAnalyzeFS_AsksNoOwnerWithNothingToJudge(t *testing.T) {
	for _, test := range []struct {
		name string
		opts lint.Options
	}{
		{"every owned rule disabled", lint.Options{Dialect: "postgres", Disabled: []string{"XY"}}},
		{"a target no owned rule judges", lint.Options{Dialect: "mysql"}},
		{"no file selected", lint.Options{Dialect: "postgres", Selection: lint.VersionSelection{Versions: []int64{9}, Restricted: true}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []lintext.CheckRequest
			owner := scriptedOwner{requests: &requests, answers: map[bool]lintext.CheckResult{false: {Complete: true}}}

			_, err := lint.LintFS(c.Context(), ownedRuntime(c, owner, ownedRule("XY101", "postgres")), fixture(selectMigration()), test.opts)

			c.Assert(err, qt.IsNil)
			c.Assert(requests, qt.HasLen, 0)
		})
	}
}

// An owned rule of the online family is sent only to a run in the online
// mode, as the family's rules of this package run only there.
func TestAnalyzeFS_SendsAnOnlineRuleOnlyInTheOnlineMode(t *testing.T) {
	for _, test := range []struct {
		name string
		opts lint.Options
		want [][]string
	}{
		{"an ordinary run", lint.Options{Dialect: "postgres"}, [][]string{{"XY101"}}},
		{"the online mode", lint.Options{Dialect: "postgres", RequireOnline: true}, [][]string{{"ON901", "XY101"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []lintext.CheckRequest
			owner := scriptedOwner{requests: &requests, answers: map[bool]lintext.CheckResult{false: {Complete: true}}}

			_, err := lint.LintFS(c.Context(), ownedRuntime(c, owner, ownedRule("ON901", "postgres"), ownedRule("XY101", "postgres")),
				fixture(selectMigration()), test.opts)

			c.Assert(err, qt.IsNil)
			got := make([][]string, 0, len(requests))
			for _, request := range requests {
				got = append(got, request.Codes)
			}
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// An owner reads a statement's line in the file as written, the line every
// finding reports, also where the analyzed SQL starts further down: in an
// Atlas txtar file the migration section begins on line four.
func TestAnalyzeFS_OwnersReadTheLineOfTheFileAsWritten(t *testing.T) {
	c := qt.New(t)
	var requests []lintext.CheckRequest
	owner := scriptedOwner{requests: &requests, answers: map[bool]lintext.CheckResult{false: {Complete: true,
		Findings: []lintext.Finding{{Code: "XY101", File: 0, Statement: 0, Message: "m"}}}}}

	findings, err := lint.LintFS(c.Context(), ownedRuntime(c, owner, ownedRule("XY101", "postgres")), fixture(map[string]string{
		"1_select.sql": "-- atlas:txtar\n\n-- migration.sql --\nSELECT 1;\n\n-- down.sql --\nSELECT 0;\n",
	}), lint.Options{Dialect: "postgres", DirFormat: migrationfile.DirFormatAtlas})

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.HasLen, 1)
	c.Assert(requests[0].Files[0].Statements[0].Line, qt.Equals, 4)
	c.Assert(ruleCodes(findings), qt.DeepEquals, []string{"XY101"})
	c.Assert(findings[0].Line, qt.Equals, 4)
}

// A run that names no target sends every owned rule, whatever it judges,
// over the dialect-blind split.
func TestAnalyzeFS_AsksEveryOwnerWithoutATarget(t *testing.T) {
	c := qt.New(t)
	var requests []lintext.CheckRequest
	owner := scriptedOwner{requests: &requests, answers: map[bool]lintext.CheckResult{false: {Complete: true}}}

	_, err := lint.LintFS(c.Context(), ownedRuntime(c, owner, ownedRule("XY101", "postgres"), ownedRule("XY103", "mysql")),
		fixture(selectMigration()), lint.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.HasLen, 1)
	c.Assert([]any{requests[0].Target.Name, requests[0].Codes}, qt.DeepEquals, []any{"", []string{"XY101", "XY103"}})
	c.Assert(requests[0].Files[1].Statements, qt.DeepEquals, lintscan.Split(selectMigration()["0000000001_x.up.sql"], lintscan.ForDialect("")))
}

// A baseline rule is served in two calls. The first names the statement the
// rule wants the state for, which makes its version one to read and, with
// nothing read, an unmet input. The second carries the state the caller read,
// and the owner judges with it.
func TestAnalyzeFS_ServesABaselineRuleInTwoCalls(t *testing.T) {
	c := qt.New(t)
	var requests []lintext.CheckRequest
	subjects := []lintext.BaselineSubject{{Code: "XY301", File: 1, Statements: []int{0}}}
	owner := scriptedOwner{requests: &requests, answers: map[bool]lintext.CheckResult{
		false: {Complete: true, BaselineSubjects: subjects},
		true: {Complete: true, BaselineSubjects: subjects,
			Findings: []lintext.Finding{{Code: "XY301", File: 1, Statement: 0, Message: "a was int"}}},
	}}
	rule := lintext.Rule{Code: "XY301", Title: "t", Severity: lintext.Warning, Targets: []string{"postgres"}, Input: lintext.InputBaselineSchema}
	runtime := ownedRuntime(c, owner, rule)
	opts := lint.Options{Dialect: "postgres"}

	first, err := lint.AnalyzeFS(c.Context(), runtime, fixture(selectMigration()), opts)
	c.Assert(err, qt.IsNil)
	c.Assert(first.Findings(), qt.HasLen, 0)
	c.Assert(first.BaselineVersions(), qt.DeepEquals, []int64{1})
	c.Assert(first.UnmetInputs(), qt.DeepEquals, []lint.UnmetInput{
		{Rule: "XY301", Input: lint.InputBaselineSchema, File: "0000000001_x.up.sql", Version: 1}})

	opts.Baseline = []lintext.BaselineColumn{{Version: 1, Table: "t", Name: "a", DataType: "int"}}
	second, err := lint.AnalyzeFS(c.Context(), runtime, fixture(selectMigration()), opts)
	c.Assert(err, qt.IsNil)
	c.Assert(ruleCodes(second.Findings()), qt.DeepEquals, []string{"XY301"})
	c.Assert(second.UnmetInputs(), qt.HasLen, 0)

	c.Assert(requests, qt.HasLen, 2)
	c.Assert(requests[0].Baseline, qt.IsNil)
	c.Assert(requests[1].Baseline, qt.DeepEquals, &lintext.Baseline{Columns: opts.Baseline})
}

// A subject on a statement the reviewed schema leaves out asks for nothing: no
// version to read, and no unmet input to report.
func TestAnalyzeFS_BaselineSubjectsOutsideTheReviewedSchema(t *testing.T) {
	c := qt.New(t)
	owner := scriptedOwner{requests: new([]lintext.CheckRequest), answers: map[bool]lintext.CheckResult{false: {Complete: true,
		BaselineSubjects: []lintext.BaselineSubject{{Code: "XY301", File: 1, Statements: []int{0}}}}}}
	rule := lintext.Rule{Code: "XY301", Title: "t", Severity: lintext.Warning, Targets: []string{"postgres"}, Input: lintext.InputBaselineSchema}

	analysis, err := lint.AnalyzeFS(c.Context(), ownedRuntime(c, owner, rule), fixture(map[string]string{
		"0000000001_x.down.sql": "SELECT 0;\n",
		"0000000001_x.up.sql":   "ALTER TABLE other.t RENAME COLUMN a TO b;\n",
	}), lint.Options{Dialect: "postgres", SchemaScope: "public"})

	c.Assert(err, qt.IsNil)
	c.Assert(analysis.BaselineVersions(), qt.HasLen, 0)
	c.Assert(analysis.UnmetInputs(), qt.HasLen, 0)
}

// An owner that fails fails the run: an error is never a clean report.
func TestAnalyzeFS_OwnerFailure_FailurePath(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("plugin closed")
	owner := scriptedOwner{requests: new([]lintext.CheckRequest), failure: failure}

	findings, err := lint.LintFS(c.Context(), ownedRuntime(c, owner, ownedRule("XY101", "postgres")), fixture(selectMigration()),
		lint.Options{Dialect: "postgres"})

	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(err, qt.ErrorMatches, `check the lint rules the engine owners publish: plugin closed`)
	c.Assert(findings, qt.IsNil)
}

// A gate family is checked against the rules of the runtime: a family only an
// owner publishes is one, and a family nothing publishes is refused.
func TestLoadConfigFS_GateFamiliesOfTheRuntime(t *testing.T) {
	c := qt.New(t)
	runtime := ownedRuntime(c, scriptedOwner{}, ownedRule("XY101", "postgres"))
	gate := func(family string) map[string]string {
		return map[string]string{lint.ConfigFileName: "gate:\n  families: [" + family + "]\n"}
	}

	config, err := lint.LoadConfigFS(runtime, fixture(gate("XY")), lint.ConfigFileName)
	c.Assert(err, qt.IsNil)
	c.Assert(config.GateFamilies(), qt.DeepEquals, []string{"DS", "XY"})

	_, err = lint.LoadConfigFS(must.Must(engine.New()), fixture(gate("XY")), lint.ConfigFileName)
	c.Assert(err, qt.ErrorMatches, `failed to parse lint config \.ptah-lint\.yaml: gate: family "XY" matches no registered rule`)
}

// Selection compares against the rules it is given: a code one of them holds
// selects that rule alone, and any other selector is a prefix.
func TestSelectorSelects_ReadsTheGivenRules(t *testing.T) {
	c := qt.New(t)
	rules := must.Must(lint.Rules(ownedRuntime(c, scriptedOwner{}, ownedRule("XY1", "postgres"), ownedRule("XY10", "postgres"))))

	c.Assert(lint.SelectorSelects(rules, "XY1", "XY10"), qt.IsFalse)
	c.Assert(lint.SelectorSelects(slices.DeleteFunc(rules, func(rule lint.Rule) bool { return rule.Code == "XY1" }), "XY1", "XY10"), qt.IsTrue)
}
