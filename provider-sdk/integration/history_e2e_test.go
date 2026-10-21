//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	transport "github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

func historyOptions(c *qt.C) transport.Options {
	c.Helper()
	binary := buildFixture(c, "transactions")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	return transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture", RequiredFeatures: []string{provider.HistoryFeature, provider.TransactionsFeature}}
}
func processHistoryScope(session *protocol.Session) *protocol.HistoryScope {
	return &protocol.HistoryScope{SessionId: session.GetId(), Schema: "main", Table: "_revisions", Format: protocol.HistoryScope_NATIVE}
}
func processHistoryRow() *protocol.HistoryRevision {
	return &protocol.HistoryRevision{Version: "1", Description: "first migration", State: protocol.HistoryRevision_APPLIED, Direction: protocol.HistoryRevision_UP, Applied: 2, Total: 2, Checksum: "sha256:source", AppliedAtSeconds: 1792571646, AppliedAtNanos: 123456789, ExecutionNanoseconds: 1234567}
}
func TestIndependentProviderHistorySharesExecutionTransaction(t *testing.T) {
	c := qt.New(t)
	options := historyOptions(c)
	for _, test := range []struct {
		name   string
		action protocol.FinishTransactionRequest_Action
		count  int
	}{
		{"commit", protocol.FinishTransactionRequest_COMMIT, 1},
		{"rollback", protocol.FinishTransactionRequest_ROLLBACK, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			client := start(c, options)
			session := open(c, client, "")
			scope := processHistoryScope(session)
			_, err := client.PrepareHistory(c.Context(), &protocol.PrepareHistoryRequest{Scope: scope, RequestId: "prepare"})
			c.Assert(err, qt.IsNil)
			tx, err := client.BeginTransaction(c.Context(), &protocol.BeginTransactionRequest{Isolation: protocol.TransactionIsolation_DEFAULT, SessionId: session.GetId(), TransactionId: "transaction"})
			c.Assert(err, qt.IsNil)
			execute := execution(session.GetId())
			execute.TransactionId = tx.GetId()
			executed, err := client.Execute(c.Context(), execute)
			c.Assert(err, qt.IsNil)
			c.Assert(provider.CheckExecution(len(execute.GetUnits()), executed), qt.IsNil)
			scope.TransactionId = tx.GetId()
			_, err = client.MutateHistory(c.Context(), &protocol.MutateHistoryRequest{Scope: scope, RequestId: "record", Replacement: processHistoryRow()})
			c.Assert(err, qt.IsNil)
			snapshot, err := client.ReadHistory(c.Context(), &protocol.ReadHistoryRequest{Scope: scope, MaxRevisions: 10})
			c.Assert(err, qt.IsNil)
			c.Assert(snapshot.GetRevisions(), qt.HasLen, 1)
			c.Assert(proto.Equal(snapshot.GetRevisions()[0], processHistoryRow()), qt.IsTrue)
			_, err = client.FinishTransaction(c.Context(), &protocol.FinishTransactionRequest{SessionId: session.GetId(), TransactionId: tx.GetId(), Action: test.action})
			c.Assert(err, qt.IsNil)
			scope.TransactionId = ""
			snapshot, err = client.ReadHistory(c.Context(), &protocol.ReadHistoryRequest{Scope: scope, MaxRevisions: 10})
			c.Assert(err, qt.IsNil)
			c.Assert(snapshot.GetRevisions(), qt.HasLen, test.count)
			schema, err := client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: session.GetId()})
			c.Assert(err, qt.IsNil)
			c.Assert(slices.ContainsFunc(schema.GetTables(), func(table *protocol.Table) bool { return table.GetIdentity().GetName() == "first" }), qt.Equals, test.count == 1)
		})
	}
}
func TestIndependentProviderHistoryLostReplyRequiresReconciliation(t *testing.T) {
	c := qt.New(t)
	client := start(c, historyOptions(c))
	session := open(c, client, "lost-history")
	scope := processHistoryScope(session)
	_, err := client.PrepareHistory(c.Context(), &protocol.PrepareHistoryRequest{Scope: scope, RequestId: "prepare"})
	c.Assert(err, qt.IsNil)
	result, err := client.MutateHistory(c.Context(), &protocol.MutateHistoryRequest{Scope: scope, RequestId: "lost", Replacement: processHistoryRow()})
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	c.Assert(result, qt.IsNil)
	snapshot, err := client.ReadHistory(c.Context(), &protocol.ReadHistoryRequest{Scope: scope, MaxRevisions: 10})
	c.Assert(err, qt.IsNil)
	c.Assert(snapshot.GetRevisions(), qt.HasLen, 1)
	c.Assert(proto.Equal(snapshot.GetRevisions()[0], processHistoryRow()), qt.IsTrue)
}
