package providerhost

// White-box testing required: the lint adapter is reached only through a
// started provider process, and the answers under test are the ones it gives
// without calling that process, so they are checked on the adapter itself;
// transport is covered by the process tests under integration/providercli.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/lintext"
	"ptah.run/dialect/ydb/ydblint"
)

// lintTarget is an adapter whose host has no client: a call that reaches the
// provider panics, so a test that passes made none.
func lintTarget() *target {
	return &target{host: &Host{}, engine: &protocol.Engine{Id: "fixture", Features: []string{provider.LintFeature},
		LintRules: []*protocol.LintRule{{Code: "FX101", Title: "DROP TABLE loses rows", Severity: protocol.LintRule_ERROR}}}}
}

// untargetedRun is a run that names no target over a file whose statement the
// fixture's FX101 would report.
func untargetedRun(codes ...string) lintext.CheckRequest {
	return lintext.CheckRequest{Codes: codes, Files: []lintext.File{{Name: "1.sql", Up: true,
		Statements: []lintext.Statement{{Start: 0, End: 13, Line: 1, SQL: "DROP TABLE a;"}}}}}
}

func TestUntargetedLintRunIsAnsweredWithoutTheProvider_HappyPath(t *testing.T) {
	c := qt.New(t)
	result, err := lintService{target: lintTarget()}.CheckMigrations(c.Context(), untargetedRun("FX101"))
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.DeepEquals, lintext.CheckResult{Complete: true})
}

func TestUntargetedLintRunIsAnsweredWithoutTheProvider_FailurePath(t *testing.T) {
	t.Run("a code the engine does not publish", func(t *testing.T) {
		c := qt.New(t)
		result, err := lintService{target: lintTarget()}.CheckMigrations(c.Context(), untargetedRun("FX101", "ZZ999"))
		c.Assert(err, qt.ErrorIs, lintext.ErrInvalid)
		c.Assert(err, qt.ErrorMatches, `.*engine "fixture" publishes no lint rule ZZ999`)
		c.Assert(result, qt.DeepEquals, lintext.CheckResult{})
	})
	t.Run("a canceled run", func(t *testing.T) {
		c := qt.New(t)
		ctx, cancel := context.WithCancel(c.Context())
		cancel()
		result, err := lintService{target: lintTarget()}.CheckMigrations(ctx, untargetedRun("FX101"))
		c.Assert(err, qt.ErrorIs, context.Canceled)
		c.Assert(result, qt.DeepEquals, lintext.CheckResult{})
	})
	t.Run("another engine's target", func(t *testing.T) {
		c := qt.New(t)
		request := untargetedRun("FX101")
		request.Target.Name = "other"
		result, err := lintService{target: lintTarget()}.CheckMigrations(c.Context(), request)
		c.Assert(err, qt.ErrorIs, lintext.ErrInvalid)
		c.Assert(err, qt.ErrorMatches, `.*lint target changed`)
		c.Assert(result, qt.DeepEquals, lintext.CheckResult{})
	})
}

// TestPublishedRuleCatalogCarriesTheYDBFamily sends the YD family through the
// wire's rule shape and reads it back as the host publishes a provider's
// rules. The YDB provider serves the family this way, so a field the wire
// cannot carry -- a rule input, say -- would change what lint and the rule
// catalog read once the bundled owner is gone.
func TestPublishedRuleCatalogCarriesTheYDBFamily(t *testing.T) {
	c := qt.New(t)
	severities := map[lintext.Severity]protocol.LintRule_Severity{
		lintext.Info: protocol.LintRule_INFO, lintext.Warning: protocol.LintRule_WARNING, lintext.Error: protocol.LintRule_ERROR,
	}
	bundled := ydblint.Rules()
	wire := make([]*protocol.LintRule, 0, len(bundled))
	for _, rule := range bundled {
		wire = append(wire, &protocol.LintRule{Code: rule.Code, Title: rule.Title, Severity: severities[rule.Severity],
			AppliesToDown: rule.AppliesToDown, Subsumes: rule.Subsumes})
	}
	published := &target{host: &Host{}, engine: &protocol.Engine{Id: "ydb", Features: []string{provider.LintFeature}, LintRules: wire}}
	c.Assert(len(bundled) > 20, qt.IsTrue, qt.Commentf("the YD family shrank to %d rules", len(bundled)))
	c.Assert(published.lint().Rules, qt.DeepEquals, bundled)
}
