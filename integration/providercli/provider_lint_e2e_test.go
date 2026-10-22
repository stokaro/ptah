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
	c.Assert(rules, qt.HasLen, 1)
	c.Assert(rules[0].Code, qt.Equals, "FX101")
	c.Assert(rules[0].Severity, qt.Equals, lintext.Error)
	c.Assert(rules[0].Targets, qt.DeepEquals, []string{"fixture"})

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
		Target: lintext.Target{Name: "fixture", Capabilities: capability.Of(map[capability.Capability]bool{})},
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
		Target: lintext.Target{Name: "fixture", Capabilities: capability.Of(map[capability.Capability]bool{})},
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
