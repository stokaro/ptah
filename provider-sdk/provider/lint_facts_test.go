package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// The facts `sql lint` needs from an owner: what each statement is, a table
// created without a primary key, and the capabilities a statement needs.
func statementFacts() []protocol.LintFact {
	return []protocol.LintFact{
		protocol.LintFact_LINT_FACT_STATEMENT_KIND, protocol.LintFact_LINT_FACT_MISSING_PRIMARY_KEY, protocol.LintFact_LINT_FACT_REQUIRED_CAPABILITY,
	}
}

// statementFactsDescription is lintDescription negotiated with a host that
// lists the statement facts.
func statementFactsDescription() *protocol.Description {
	description := lintDescription()
	description.Engines[0].Features = append(description.Engines[0].Features, provider.LintStatementFactsFeature)
	return description
}

const statementSQL = "CREATE TABLE a (id Int32);\nUPSERT INTO a (id) VALUES (1);"

func kindEffect(class protocol.LintStatementClass, kind, object string) *protocol.LintEffect {
	return &protocol.LintEffect{Fact: protocol.LintFact_LINT_FACT_STATEMENT_KIND, StatementClass: class, Kind: kind, Object: object}
}

func statementSplit() (*protocol.SplitLintRequest, *protocol.SplitLint) {
	request := &protocol.SplitLintRequest{Engine: lintEngineID(), Sources: []*protocol.LintSource{{Name: "1.sql", Sql: statementSQL, Up: true}}}
	result := &protocol.SplitLint{Judged: statementFacts(), Files: []*protocol.LintFile{{Name: "1.sql", Statements: []*protocol.LintStatement{
		{Start: 0, End: 26, Line: 1, Sql: "CREATE TABLE a (id Int32);", Effects: []*protocol.LintEffect{
			kindEffect(protocol.LintStatementClass_LINT_STATEMENT_CLASS_CREATE_TABLE, "CREATE TABLE", "a"),
			{Fact: protocol.LintFact_LINT_FACT_MISSING_PRIMARY_KEY, Table: "a"},
			{Fact: protocol.LintFact_LINT_FACT_REQUIRED_CAPABILITY, Capability: "vector_indexes", Kind: "INDEX", Object: "emb", Table: "a"},
		}},
		{Start: 27, End: 57, Line: 2, Sql: "UPSERT INTO a (id) VALUES (1);", Effects: []*protocol.LintEffect{
			kindEffect(protocol.LintStatementClass_LINT_STATEMENT_CLASS_OTHER, "UPSERT", "a"),
		}},
	}}}}
	return request, result
}

func TestLintStatementFacts_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := statementSplit()
	c.Assert(provider.ValidateSplitLint(statementFactsDescription(), request, result), qt.IsNil)
	// A split that judges no statement kind may leave a statement without one.
	result.Judged = result.Judged[1:]
	result.Files[0].Statements[0].Effects = result.Files[0].Statements[0].Effects[1:]
	result.Files[0].Statements[1].Effects = nil
	c.Assert(provider.ValidateSplitLint(statementFactsDescription(), request, result), qt.IsNil)
}

func TestLintStatementFacts_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.SplitLint)
	}{
		{"a judged statement with no kind", func(r *protocol.SplitLint) { r.Files[0].Statements[1].Effects = nil }},
		{"a statement with two kinds", func(r *protocol.SplitLint) {
			r.Files[0].Statements[1].Effects = append(r.Files[0].Statements[1].Effects, kindEffect(protocol.LintStatementClass_LINT_STATEMENT_CLASS_OTHER, "UPSERT", "a"))
		}},
		{"a kind without a class", func(r *protocol.SplitLint) {
			r.Files[0].Statements[1].Effects[0].StatementClass = protocol.LintStatementClass_LINT_STATEMENT_CLASS_UNSPECIFIED
		}},
		{"a kind with an unknown class", func(r *protocol.SplitLint) { r.Files[0].Statements[1].Effects[0].StatementClass = 99 }},
		{"a kind without a name", func(r *protocol.SplitLint) { r.Files[0].Statements[1].Effects[0].Kind = "" }},
		{"a kind named on two lines", func(r *protocol.SplitLint) { r.Files[0].Statements[1].Effects[0].Kind = "UPSERT\nINTO" }},
		{"a class on another fact", func(r *protocol.SplitLint) {
			r.Files[0].Statements[0].Effects[1].StatementClass = protocol.LintStatementClass_LINT_STATEMENT_CLASS_CREATE_TABLE
		}},
		{"a missing primary key naming no table", func(r *protocol.SplitLint) { r.Files[0].Statements[0].Effects[1].Table = " " }},
		{"a missing primary key on a statement that creates no table", func(r *protocol.SplitLint) {
			r.Files[0].Statements[1].Effects = append(r.Files[0].Statements[1].Effects,
				&protocol.LintEffect{Fact: protocol.LintFact_LINT_FACT_MISSING_PRIMARY_KEY, Table: "a"})
		}},
		{"a required capability naming none", func(r *protocol.SplitLint) { r.Files[0].Statements[0].Effects[2].Capability = "" }},
		{"a required capability name with a space at its end", func(r *protocol.SplitLint) {
			r.Files[0].Statements[0].Effects[2].Capability = "vector_indexes "
		}},
		{"a capability on another fact", func(r *protocol.SplitLint) { r.Files[0].Statements[0].Effects[1].Capability = "vector_indexes" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := statementSplit()
			test.mutate(result)
			c.Assert(provider.ValidateSplitLint(statementFactsDescription(), request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func statementAnnotation() (*protocol.AnnotateLintRequest, *protocol.AnnotatedLint) {
	request, split := statementSplit()
	annotate := &protocol.AnnotateLintRequest{Engine: request.Engine, Facts: statementFacts(), Sources: request.Sources, Files: split.Files}
	result := &protocol.AnnotatedLint{Files: []*protocol.LintAnnotations{{Statements: []*protocol.LintAnnotation{
		{Effects: split.Files[0].Statements[0].Effects}, {Declined: true},
	}}}}
	return annotate, result
}

// A declined statement carries no effect, a kind included; a judged one
// carries its kind.
func TestLintStatementFactsAnnotation_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := statementAnnotation()
	c.Assert(provider.ValidateAnnotatedLint(statementFactsDescription(), request, result), qt.IsNil)
}

func TestLintStatementFactsAnnotation_FailurePath(t *testing.T) {
	c := qt.New(t)
	request, result := statementAnnotation()
	result.Files[0].Statements[1].Declined = false
	c.Assert(provider.ValidateAnnotatedLint(statementFactsDescription(), request, result), qt.ErrorIs, provider.ErrProtocol)
}

// A split or an annotation uses the statement facts only where the
// negotiated engine lists them: a provider does not report them to a host of
// contract 25.0, and a host does not ask a provider that does not offer them.
func TestLintStatementFactsNeedNegotiation_FailurePath(t *testing.T) {
	c := qt.New(t)
	splitRequest, split := statementSplit()
	c.Assert(provider.ValidateSplitLint(lintDescription(), splitRequest, split), qt.ErrorMatches, `.*unnegotiated.*`)
	annotateRequest, annotated := statementAnnotation()
	c.Assert(provider.ValidateAnnotatedLint(lintDescription(), annotateRequest, annotated), qt.ErrorMatches, `.*unnegotiated.*`)
	c.Assert(provider.ValidateAnnotateLintRequest(lintDescription(), annotateRequest), qt.ErrorIs, provider.ErrProtocol)
	description := statementFactsDescription()
	description.Engines[0].Features = slices.DeleteFunc(description.Engines[0].Features, func(feature string) bool { return feature == provider.LintFeature })
	description.Engines[0].LintRules = nil
	c.Assert(provider.ValidateDescription(description, nil), qt.ErrorMatches, `.*lint statement facts require engine lint`)
	c.Assert(provider.ValidateDescription(statementFactsDescription(), nil), qt.IsNil)
}
