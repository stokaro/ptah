package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// transactionOwnerDescription is lintDescription with the provider and its
// engine listing the feature, as a host that lists it negotiates them.
func transactionOwnerDescription() *protocol.Description {
	description := lintDescription()
	description.Features = append(description.Features, provider.LintTransactionOwnerFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.LintTransactionOwnerFeature)
	return description
}

func TestLintTransactionOwner_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		description *protocol.Description
		owner       protocol.LintTransactionOwner
	}{
		{name: "the migrator without the feature", description: lintDescription(), owner: protocol.LintTransactionOwner_LINT_TRANSACTION_OWNER_MIGRATOR},
		{name: "the migrator with the feature", description: transactionOwnerDescription(), owner: protocol.LintTransactionOwner_LINT_TRANSACTION_OWNER_MIGRATOR},
		{name: "the file with the feature", description: transactionOwnerDescription(), owner: protocol.LintTransactionOwner_LINT_TRANSACTION_OWNER_FILE},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := &protocol.CheckLintRequest{Engine: lintEngineID(), TransactionOwner: test.owner}

			c.Assert(provider.ValidateCheckLintRequest(test.description, request), qt.IsNil)
			c.Assert(provider.ValidateCheckedLint(test.description, request, &protocol.CheckedLint{Complete: true}), qt.IsNil)
		})
	}
}

func TestLintTransactionOwner_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		description *protocol.Description
		owner       protocol.LintTransactionOwner
		want        string
	}{
		{name: "the file without the feature", description: lintDescription(), owner: protocol.LintTransactionOwner_LINT_TRANSACTION_OWNER_FILE,
			want: `.*lint check names a transaction owner, which needs lint\.transaction-owner`},
		{name: "an owner the contract does not name", description: transactionOwnerDescription(), owner: protocol.LintTransactionOwner(2),
			want: `.*lint check names an unknown transaction owner 2`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := &protocol.CheckLintRequest{Engine: lintEngineID(), TransactionOwner: test.owner}

			c.Assert(provider.ValidateCheckLintRequest(test.description, request), qt.ErrorMatches, test.want)
			c.Assert(provider.ValidateCheckedLint(test.description, request, &protocol.CheckedLint{Complete: true}), qt.ErrorMatches, test.want)
		})
	}
}

// An engine lists the feature only beside its own lint service and the
// provider's feature, so a host never negotiates an owner no rule can read.
func TestLintTransactionOwnerDescription_FailurePath(t *testing.T) {
	c := qt.New(t)
	withoutProvider := transactionOwnerDescription()
	withoutProvider.Features = lintDescription().Features
	c.Assert(provider.ValidateDescription(withoutProvider, nil), qt.ErrorMatches, `.*engine lint\.transaction-owner requires the provider's`)

	withoutLint := transactionOwnerDescription()
	withoutLint.Engines[0].Features = []string{provider.LintTransactionOwnerFeature}
	withoutLint.Engines[0].LintRules = nil
	c.Assert(provider.ValidateDescription(withoutLint, nil), qt.ErrorMatches, `.*lint\.transaction-owner requires engine lint`)
}

// A host that does not list the feature negotiates a description without it,
// which is what makes the provider refuse the owner it did not ask for.
func TestLintTransactionOwnerNegotiation_HappyPath(t *testing.T) {
	c := qt.New(t)
	offered := transactionOwnerDescription()

	listed := provider.NegotiatedDescription(offered, []string{provider.LintTransactionOwnerFeature})
	c.Assert(listed.Features, qt.Contains, provider.LintTransactionOwnerFeature)
	c.Assert(listed.Engines[0].Features, qt.Contains, provider.LintTransactionOwnerFeature)

	unlisted := provider.NegotiatedDescription(offered, nil)
	c.Assert(unlisted.Features, qt.Not(qt.Contains), provider.LintTransactionOwnerFeature)
	c.Assert(unlisted.Engines[0].Features, qt.Not(qt.Contains), provider.LintTransactionOwnerFeature)
	request := &protocol.CheckLintRequest{Engine: lintEngineID(), TransactionOwner: protocol.LintTransactionOwner_LINT_TRANSACTION_OWNER_FILE}
	c.Assert(provider.ValidateCheckLintRequest(unlisted, request), qt.ErrorIs, provider.ErrProtocol)
}
