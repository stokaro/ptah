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
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
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

// An effect crosses the wire and comes back as it was, the statement facts'
// class and capability key included: the host sends a split's effects back
// to the owner with a check.
func TestLintEffectsRoundTrip(t *testing.T) {
	for _, effect := range []lintext.Effect{
		{Fact: lintext.FactStatefulDrop, Object: "t", Kind: "TOPIC"},
		{Fact: lintext.FactSafetyEffect, Impact: schemaext.Destructive, Reason: "drops a topic"},
		{Fact: lintext.FactStatementKind, Class: lintext.ClassCreateTable, Kind: "CREATE TABLE", Object: "a"},
		{Fact: lintext.FactStatementKind, Class: lintext.ClassAlterTable, Kind: "ALTER TABLE", Object: "a"},
		{Fact: lintext.FactStatementKind, Class: lintext.ClassSchema, Kind: "DROP TABLE", Object: "a"},
		{Fact: lintext.FactStatementKind, Class: lintext.ClassOther, Kind: "UPSERT"},
		{Fact: lintext.FactMissingPrimaryKey, Table: "a"},
		{Fact: lintext.FactRequiredCapability, Capability: capability.ParameterizedDecimal, Kind: "COLUMN", Object: "price", Table: "a", Clause: "ADD COLUMN"},
	} {
		t.Run(string(effect.Fact)+" "+string(effect.Class), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lintEffects([]*protocol.LintEffect{lintEffect(effect)}), qt.DeepEquals, []lintext.Effect{effect})
		})
	}
}

// The owner a run names reaches the wire as the contract spells it. An engine
// that does not list the feature is sent the zero value, since both transport
// boundaries refuse the other for it.
func TestWireTransactionOwner_HappyPath(t *testing.T) {
	listed := []string{provider.LintFeature, provider.LintTransactionOwnerFeature}
	for _, test := range []struct {
		name     string
		features []string
		owner    lintext.TransactionOwner
		want     protocol.LintTransactionOwner
	}{
		{"the migrator wraps each file", listed, lintext.TransactionOwnerMigrator, protocol.LintTransactionOwner_LINT_TRANSACTION_OWNER_MIGRATOR},
		{"a tool opens no transaction", listed, lintext.TransactionOwnerFile, protocol.LintTransactionOwner_LINT_TRANSACTION_OWNER_FILE},
		{"an engine that does not list the feature", []string{provider.LintFeature}, lintext.TransactionOwnerFile, protocol.LintTransactionOwner_LINT_TRANSACTION_OWNER_MIGRATOR},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service := lintService{target: &target{engine: &protocol.Engine{Id: "fixture", Features: test.features}}}

			got, err := service.wireTransactionOwner(test.owner)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestWireTransactionOwner_FailurePath(t *testing.T) {
	c := qt.New(t)
	service := lintService{target: &target{engine: &protocol.Engine{Id: "fixture"}}}

	got, err := service.wireTransactionOwner(lintext.TransactionOwner("savepoint"))

	c.Assert(err, qt.ErrorIs, lintext.ErrInvalid)
	c.Assert(err, qt.ErrorMatches, `.*lint transaction owner "savepoint"`)
	c.Assert(got, qt.Equals, protocol.LintTransactionOwner(0))
}
