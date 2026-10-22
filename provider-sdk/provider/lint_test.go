package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func lintEngineID() string { return lintDescription().Engines[0].Id }

const lintSQL = "DROP TABLE a;\nDROP TABLE b;"

func lintDescription() *protocol.Description {
	description, _, _ := creationExchange()
	description.Features = append(description.Features, provider.LintFeature)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, provider.LintFeature)
	engine.LintRules = []*protocol.LintRule{{Code: "XY101", Title: "Drop loses data", Severity: protocol.LintRule_ERROR}}
	return description
}

func lintStatements() []*protocol.LintStatement {
	return []*protocol.LintStatement{
		{Start: 0, End: 13, Line: 1, Sql: "DROP TABLE a;"},
		{Start: 14, End: 27, Line: 2, Sql: "DROP TABLE b;"},
	}
}

func lintSplit() (*protocol.SplitLintRequest, *protocol.SplitLint) {
	request := &protocol.SplitLintRequest{Engine: lintEngineID(), Sources: []*protocol.LintSource{{Name: "1.sql", Sql: lintSQL, Up: true}}}
	result := &protocol.SplitLint{Files: []*protocol.LintFile{{Name: "1.sql", Statements: lintStatements()}}}
	return request, result
}

func TestLintDescriptionDeclaresRules_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(provider.ValidateDescription(lintDescription(), nil), qt.IsNil)
}

func TestLintDescriptionDeclaresRules_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.Description)
	}{
		{"rules without the description service", func(d *protocol.Description) { d.Features = d.Features[:len(d.Features)-1] }},
		{"rules without the engine service", func(d *protocol.Description) {
			d.Engines[0].Features = d.Engines[0].Features[:len(d.Engines[0].Features)-1]
		}},
		{"service without rules", func(d *protocol.Description) { d.Engines[0].LintRules = nil }},
		{"lowercase code", func(d *protocol.Description) { d.Engines[0].LintRules[0].Code = "xy101" }},
		{"empty code", func(d *protocol.Description) { d.Engines[0].LintRules[0].Code = "" }},
		{"digit first", func(d *protocol.Description) { d.Engines[0].LintRules[0].Code = "1XY" }},
		{"blank title", func(d *protocol.Description) { d.Engines[0].LintRules[0].Title = " " }},
		{"no severity", func(d *protocol.Description) { d.Engines[0].LintRules[0].Severity = 0 }},
		{"subsumes itself", func(d *protocol.Description) { d.Engines[0].LintRules[0].Subsumes = []string{"XY101"} }},
		{"duplicate code", func(d *protocol.Description) {
			d.Engines[0].LintRules = append(d.Engines[0].LintRules, proto.CloneOf(d.Engines[0].LintRules[0]))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description := lintDescription()
			test.mutate(description)
			c.Assert(provider.ValidateDescription(description, nil), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestLintSplit_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := lintSplit()
	c.Assert(provider.ValidateSplitLint(lintDescription(), request, result), qt.IsNil)
	result.Judged = []protocol.LintFact{protocol.LintFact_LINT_FACT_STATEFUL_DROP}
	result.Files[0].Statements[0].Effects = []*protocol.LintEffect{{Fact: protocol.LintFact_LINT_FACT_STATEFUL_DROP, Object: "a"}}
	c.Assert(provider.ValidateSplitLint(lintDescription(), request, result), qt.IsNil)
}

func TestLintSplit_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.SplitLintRequest, *protocol.SplitLint)
	}{
		{"missing file", func(_ *protocol.SplitLintRequest, r *protocol.SplitLint) { r.Files = nil }},
		{"text differs from source", func(_ *protocol.SplitLintRequest, r *protocol.SplitLint) {
			r.Files[0].Statements[0].Sql = "DROP TABLE x;"
		}},
		{"offset beyond source", func(_ *protocol.SplitLintRequest, r *protocol.SplitLint) { r.Files[0].Statements[1].End = 99 }},
		{"statements overlap", func(_ *protocol.SplitLintRequest, r *protocol.SplitLint) { r.Files[0].Statements[1].Start = 5 }},
		{"line zero", func(_ *protocol.SplitLintRequest, r *protocol.SplitLint) { r.Files[0].Statements[0].Line = 0 }},
		{"effect of an unjudged fact", func(_ *protocol.SplitLintRequest, r *protocol.SplitLint) {
			r.Files[0].Statements[0].Effects = []*protocol.LintEffect{{Fact: protocol.LintFact_LINT_FACT_UNIQUE_INDEX}}
		}},
		{"unknown judged fact", func(_ *protocol.SplitLintRequest, r *protocol.SplitLint) { r.Judged = []protocol.LintFact{99} }},
		{"rebuild drop without rename", func(_ *protocol.SplitLintRequest, r *protocol.SplitLint) {
			r.Judged = []protocol.LintFact{protocol.LintFact_LINT_FACT_REBUILD_DROP}
		}},
		{"impact on a plain fact", func(_ *protocol.SplitLintRequest, r *protocol.SplitLint) {
			r.Judged = []protocol.LintFact{protocol.LintFact_LINT_FACT_STATEFUL_DROP}
			r.Files[0].Statements[0].Effects = []*protocol.LintEffect{{Fact: protocol.LintFact_LINT_FACT_STATEFUL_DROP, Reason: "x"}}
		}},
		{"safety effect without a reason", func(_ *protocol.SplitLintRequest, r *protocol.SplitLint) {
			r.Judged = []protocol.LintFact{protocol.LintFact_LINT_FACT_SAFETY_EFFECT}
			r.Files[0].Statements[0].Effects = []*protocol.LintEffect{{Fact: protocol.LintFact_LINT_FACT_SAFETY_EFFECT, Impact: protocol.LintImpact_LINT_IMPACT_DESTRUCTIVE}}
		}},
		{"two safety effects", func(_ *protocol.SplitLintRequest, r *protocol.SplitLint) {
			r.Judged = []protocol.LintFact{protocol.LintFact_LINT_FACT_SAFETY_EFFECT}
			effect := &protocol.LintEffect{Fact: protocol.LintFact_LINT_FACT_SAFETY_EFFECT, Impact: protocol.LintImpact_LINT_IMPACT_BEHAVIORAL, Reason: "x"}
			r.Files[0].Statements[0].Effects = []*protocol.LintEffect{effect, proto.CloneOf(effect)}
		}},
		{"nameless source", func(q *protocol.SplitLintRequest, _ *protocol.SplitLint) { q.Sources[0].Name = "" }},
		{"foreign engine", func(q *protocol.SplitLintRequest, _ *protocol.SplitLint) { q.Engine = "other" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := lintSplit()
			test.mutate(request, result)
			c.Assert(provider.ValidateSplitLint(lintDescription(), request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	t.Run("nil result", func(t *testing.T) {
		c := qt.New(t)
		request, _ := lintSplit()
		c.Assert(provider.ValidateSplitLint(lintDescription(), request, nil), qt.ErrorIs, provider.ErrProtocol)
	})
	t.Run("service not negotiated", func(t *testing.T) {
		c := qt.New(t)
		request, result := lintSplit()
		description := lintDescription()
		description.Features = nil
		c.Assert(provider.ValidateSplitLint(description, request, result), qt.ErrorIs, provider.ErrProtocol)
	})
}

func lintAnnotation() (*protocol.AnnotateLintRequest, *protocol.AnnotatedLint) {
	request := &protocol.AnnotateLintRequest{Engine: lintEngineID(),
		Facts:   []protocol.LintFact{protocol.LintFact_LINT_FACT_STATEFUL_DROP},
		Sources: []*protocol.LintSource{{Name: "1.sql", Sql: lintSQL, Up: true}},
		Files:   []*protocol.LintFile{{Name: "1.sql", Statements: lintStatements()}}}
	result := &protocol.AnnotatedLint{Files: []*protocol.LintAnnotations{{Statements: []*protocol.LintAnnotation{
		{Effects: []*protocol.LintEffect{{Fact: protocol.LintFact_LINT_FACT_STATEFUL_DROP, Object: "a"}}},
		{Declined: true},
	}}}}
	return request, result
}

func TestLintAnnotation_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := lintAnnotation()
	c.Assert(provider.ValidateAnnotatedLint(lintDescription(), request, result), qt.IsNil)
}

func TestLintAnnotation_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.AnnotateLintRequest, *protocol.AnnotatedLint)
	}{
		{"missing file", func(_ *protocol.AnnotateLintRequest, r *protocol.AnnotatedLint) { r.Files = nil }},
		{"missing statement", func(_ *protocol.AnnotateLintRequest, r *protocol.AnnotatedLint) {
			r.Files[0].Statements = r.Files[0].Statements[:1]
		}},
		{"declined with an effect", func(_ *protocol.AnnotateLintRequest, r *protocol.AnnotatedLint) {
			r.Files[0].Statements[1].Effects = r.Files[0].Statements[0].Effects
		}},
		{"effect nobody asked for", func(q *protocol.AnnotateLintRequest, _ *protocol.AnnotatedLint) {
			q.Facts = []protocol.LintFact{protocol.LintFact_LINT_FACT_UNIQUE_INDEX}
		}},
		{"no facts", func(q *protocol.AnnotateLintRequest, _ *protocol.AnnotatedLint) { q.Facts = nil }},
		{"repeated fact", func(q *protocol.AnnotateLintRequest, _ *protocol.AnnotatedLint) {
			q.Facts = append(q.Facts, q.Facts[0])
		}},
		{"sources and files differ", func(q *protocol.AnnotateLintRequest, _ *protocol.AnnotatedLint) { q.Files = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := lintAnnotation()
			test.mutate(request, result)
			c.Assert(provider.ValidateAnnotatedLint(lintDescription(), request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func lintCheck() (*protocol.CheckLintRequest, *protocol.CheckedLint) {
	request := &protocol.CheckLintRequest{Engine: lintEngineID(), Codes: []string{"XY101"},
		Files: []*protocol.LintFile{{Name: "1.sql", Up: true, Statements: lintStatements()}}}
	result := &protocol.CheckedLint{Complete: true, Findings: []*protocol.LintFinding{{
		Code: "XY101", File: 0, Statement: 1, Message: "drop loses data",
		Subjects: []*protocol.LintSubject{{Kind: protocol.LintSubject_TABLE, Name: "b"}},
	}}}
	return request, result
}

func TestLintCheck_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := lintCheck()
	c.Assert(provider.ValidateCheckedLint(lintDescription(), request, result), qt.IsNil)
	result.Findings = []*protocol.LintFinding{{Code: "XY101", File: 0, Statement: -1, Message: "file level"}}
	c.Assert(provider.ValidateCheckedLint(lintDescription(), request, result), qt.IsNil)
	result.Findings = nil
	c.Assert(provider.ValidateCheckedLint(lintDescription(), request, result), qt.IsNil)
}

func TestLintCheck_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.CheckLintRequest, *protocol.CheckedLint)
	}{
		{"incomplete", func(_ *protocol.CheckLintRequest, r *protocol.CheckedLint) { r.Complete = false }},
		{"rule not enabled", func(q *protocol.CheckLintRequest, _ *protocol.CheckedLint) { q.Codes = nil }},
		{"undeclared rule enabled", func(q *protocol.CheckLintRequest, _ *protocol.CheckedLint) { q.Codes = []string{"ZZ999"} }},
		{"repeated rule enabled", func(q *protocol.CheckLintRequest, _ *protocol.CheckedLint) { q.Codes = []string{"XY101", "XY101"} }},
		{"file beyond request", func(_ *protocol.CheckLintRequest, r *protocol.CheckedLint) { r.Findings[0].File = 1 }},
		{"negative file", func(_ *protocol.CheckLintRequest, r *protocol.CheckedLint) { r.Findings[0].File = -1 }},
		{"statement beyond file", func(_ *protocol.CheckLintRequest, r *protocol.CheckedLint) { r.Findings[0].Statement = 2 }},
		{"statement below whole-file marker", func(_ *protocol.CheckLintRequest, r *protocol.CheckedLint) { r.Findings[0].Statement = -2 }},
		{"no message", func(_ *protocol.CheckLintRequest, r *protocol.CheckedLint) { r.Findings[0].Message = "" }},
		{"whole-file finding with subjects", func(_ *protocol.CheckLintRequest, r *protocol.CheckedLint) { r.Findings[0].Statement = -1 }},
		{"subject without a kind", func(_ *protocol.CheckLintRequest, r *protocol.CheckedLint) { r.Findings[0].Subjects[0].Kind = 0 }},
		{"subject without a name", func(_ *protocol.CheckLintRequest, r *protocol.CheckedLint) { r.Findings[0].Subjects[0].Name = "" }},
		{"nameless file", func(q *protocol.CheckLintRequest, _ *protocol.CheckedLint) { q.Files[0].Name = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := lintCheck()
			test.mutate(request, result)
			c.Assert(provider.ValidateCheckedLint(lintDescription(), request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	t.Run("nil result", func(t *testing.T) {
		c := qt.New(t)
		request, _ := lintCheck()
		c.Assert(provider.ValidateCheckedLint(lintDescription(), request, nil), qt.ErrorIs, provider.ErrProtocol)
	})
}
