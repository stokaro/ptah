//go:build integration

package providercli_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/core/platform/capability"
)

const lintMigration = "CREATE TABLE a (id INTEGER);\nDROP TABLE a;"

func TestProviderPublishesAndChecksLintRulesIndependently(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "lint")

	rules, err := runtime.LintRules("fixture")
	c.Assert(err, qt.IsNil)
	c.Assert(rules, qt.HasLen, 2)
	c.Assert(rules[0].Code, qt.Equals, "FX101")
	c.Assert(rules[0].Severity, qt.Equals, lintext.Error)
	c.Assert(rules[0].Targets, qt.DeepEquals, []string{"fixture"})
	c.Assert(rules[1].Code, qt.Equals, "FX102")

	split, err := runtime.SplitStatements(c.Context(), lintext.SplitRequest{Target: "fixture",
		Sources: []lintext.Source{{Name: "1.sql", SQL: lintMigration, Up: true}}})
	c.Assert(err, qt.IsNil)
	c.Assert(split.Judged, qt.DeepEquals, []lintext.Fact{lintext.FactStatefulDrop})
	c.Assert(split.Files, qt.HasLen, 1)
	c.Assert(split.Files[0], qt.HasLen, 2)
	c.Assert(split.Files[0][1].Line, qt.Equals, 2)
	c.Assert(split.Files[0][1].Effects, qt.HasLen, 1)
	c.Assert(split.Files[0][1].Effects[0].Fact, qt.Equals, lintext.FactStatefulDrop)
	c.Assert(split.Files[0][0].Effects, qt.HasLen, 0)

	result, err := runtime.CheckMigrations(c.Context(), lintext.CheckRequest{
		Target: lintext.Target{Name: "fixture", Capabilities: capability.Of(make(map[capability.Capability]bool))},
		Codes:  []string{"FX101"},
		Files:  []lintext.File{{Name: "1.sql", Up: true, Statements: split.Files[0]}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Findings, qt.DeepEquals, []lintext.Finding{{Code: "FX101", File: 0, Statement: 1, Message: "DROP TABLE removes every row"}})
}

func TestProviderLintDiscardsCancellation(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "lint")
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	result, err := runtime.CheckMigrations(ctx, lintext.CheckRequest{
		Target: lintext.Target{Name: "fixture", Capabilities: capability.Of(make(map[capability.Capability]bool))},
		Codes:  []string{"FX101"},
	})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result.Findings, qt.HasLen, 0)
}

func TestProviderAnnotatesStatementsASplitWithoutADialect(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "lint")
	sources := []lintext.Source{{Name: "1.sql", SQL: lintMigration, Up: true}}
	statements := []lintext.Statement{{Start: 0, End: 28, Line: 1, SQL: "CREATE TABLE a (id INTEGER);"}, {Start: 29, End: 42, Line: 2, SQL: "DROP TABLE a;"}}
	result, err := runtime.AnnotateStatements(c.Context(), lintext.AnnotateRequest{Facts: []lintext.Fact{lintext.FactStatefulDrop}, Sources: sources, Files: [][]lintext.Statement{statements}})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Files, qt.HasLen, 1)
	c.Assert(result.Files[0], qt.HasLen, 2)
	c.Assert(result.Files[0][0].Effects, qt.HasLen, 0)
	c.Assert(result.Files[0][1].Effects, qt.HasLen, 1)
	c.Assert(result.Files[0][1].Effects[0].Fact, qt.Equals, lintext.FactStatefulDrop)
	c.Assert(result.Declined, qt.HasLen, 0)
}

// TestProviderLintReceivesTheTargetTheHostResolved reads back, through FX102,
// the server version and the capabilities the host sent. The host resolves
// both before it asks; the provider judges with what it is given.
func TestProviderLintReceivesTheTargetTheHostResolved(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "lint")
	result, err := runtime.CheckMigrations(c.Context(), lintext.CheckRequest{
		Target: lintext.Target{Name: "fixture", Version: "fixture-1", Capabilities: capability.Of(map[capability.Capability]bool{
			capability.ParameterizedDecimal: true, capability.UniqueIndexOnExistingTable: false,
		})},
		Codes: []string{"FX102"},
		Files: []lintext.File{{Name: "1.sql", Up: true}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Findings, qt.DeepEquals, []lintext.Finding{{Code: "FX102", File: 0, Statement: -1,
		Message: `checked against fixture server "fixture-1" with 2 capabilities: parameterized_decimal=true unique_index_on_existing_table=false`}})
}

// TestProviderLintOfARunThatNamesNoTarget is the run the host answers for the
// provider. Sent on, the fixture would report FX101 on the DROP TABLE and
// FX102 on the file; refused, the run would fail.
func TestProviderLintOfARunThatNamesNoTarget(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "lint")
	statements := []lintext.Statement{{Start: 0, End: 28, Line: 1, SQL: "CREATE TABLE a (id INTEGER);"}, {Start: 29, End: 42, Line: 2, SQL: "DROP TABLE a;"}}
	result, err := runtime.CheckMigrations(c.Context(), lintext.CheckRequest{
		Codes: []string{"FX101", "FX102"},
		Files: []lintext.File{{Name: "1.sql", Up: true, Statements: statements}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.DeepEquals, lintext.CheckResult{Complete: true})
}
