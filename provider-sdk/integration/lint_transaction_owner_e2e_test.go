//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
	transport "ptah.run/provider-sdk/runtime"
)

// transactionOwnerClient starts the proof provider for a host that lists
// features.
func transactionOwnerClient(c *qt.C, features []string) *transport.Client {
	c.Helper()
	binary := buildSDKFixture(c, "sdk-lint-transaction-owner")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	return start(c, transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture",
		RequiredFeatures: []string{provider.LintFeature}, HostFeatures: features})
}

// transactionOwnerCheck checks one file that opens its own transaction, under
// owner.
func transactionOwnerCheck(owner protocol.LintTransactionOwner) *protocol.CheckLintRequest {
	return &protocol.CheckLintRequest{Engine: "fixture", Codes: []string{"FX301"}, TransactionOwner: owner,
		Files: []*protocol.LintFile{{Name: "1.sql", Up: true, Statements: []*protocol.LintStatement{
			{Start: 0, End: 6, Line: 1, Sql: "BEGIN;"},
			{Start: 7, End: 21, Line: 2, Sql: "DROP TABLE a;"},
		}}}}
}

// The owner a check names reaches a rule in another process, which judges the
// file's BEGIN by it. A host that does not list the feature gets the judgment
// of a migrator that wraps each file, which is all an earlier minor means.
func TestIndependentProviderJudgesAFileByItsTransactionOwner_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		features []string
		owner    protocol.LintTransactionOwner
		findings int
	}{
		{name: "a migrator that wraps the file", features: []string{provider.LintTransactionOwnerFeature},
			owner: protocol.LintTransactionOwner_LINT_TRANSACTION_OWNER_MIGRATOR, findings: 1},
		{name: "a tool that opens no transaction", features: []string{provider.LintTransactionOwnerFeature},
			owner: protocol.LintTransactionOwner_LINT_TRANSACTION_OWNER_FILE, findings: 0},
		{name: "a host that does not list the feature",
			owner: protocol.LintTransactionOwner_LINT_TRANSACTION_OWNER_MIGRATOR, findings: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			client := transactionOwnerClient(c, test.features)

			checked, err := client.CheckLintMigrations(c.Context(), transactionOwnerCheck(test.owner))

			c.Assert(err, qt.IsNil)
			c.Assert(checked.GetComplete(), qt.IsTrue)
			c.Assert(checked.GetFindings(), qt.HasLen, test.findings)
		})
	}
}

// A host that does not list the feature cannot name an owner: the negotiation
// removed the feature from the engine it reads.
func TestIndependentProviderJudgesAFileByItsTransactionOwner_FailurePath(t *testing.T) {
	c := qt.New(t)
	client := transactionOwnerClient(c, nil)
	c.Assert(client.Description().GetEngines()[0].GetFeatures(), qt.Not(qt.Contains), provider.LintTransactionOwnerFeature)

	checked, err := client.CheckLintMigrations(c.Context(), transactionOwnerCheck(protocol.LintTransactionOwner_LINT_TRANSACTION_OWNER_FILE))

	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(checked, qt.IsNil)
}
