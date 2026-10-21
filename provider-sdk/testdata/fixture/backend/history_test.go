package backend_test

import (
	"context"
	"testing"

	"example.com/ptah-fixture-provider/backend"
	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func nativeHistoryScope(id string) *protocol.HistoryScope {
	return &protocol.HistoryScope{SessionId: id, Schema: "main", Table: "_revisions", Format: protocol.HistoryScope_NATIVE}
}
func nativeHistoryRow() *protocol.HistoryRevision {
	return &protocol.HistoryRevision{Version: "1", Description: "create records", State: protocol.HistoryRevision_APPLIED, Direction: protocol.HistoryRevision_UP, Applied: 1, Total: 1, Checksum: "sha256:fixture"}
}
func prepareHistory(c *qt.C, owner *backend.TransactionalProvider, scope *protocol.HistoryScope) {
	c.Helper()
	result, err := owner.PrepareHistory(c.Context(), &protocol.PrepareHistoryRequest{Scope: scope, RequestId: "prepare"})
	c.Assert(err, qt.IsNil)
	c.Assert(provider.CheckHistoryResult(scope, "prepare", result), qt.IsNil)
}
func readHistory(c *qt.C, owner *backend.TransactionalProvider, scope *protocol.HistoryScope) *protocol.HistorySnapshot {
	c.Helper()
	result, err := owner.ReadHistory(c.Context(), &protocol.ReadHistoryRequest{Scope: scope, MaxRevisions: 100})
	c.Assert(err, qt.IsNil)
	return result
}
func TestHistoryAndSchemaShareCommitOrRollback(t *testing.T) {
	for _, test := range []struct {
		name   string
		action protocol.FinishTransactionRequest_Action
		rows   int
		table  bool
	}{
		{"commit", protocol.FinishTransactionRequest_COMMIT, 1, true},
		{"rollback", protocol.FinishTransactionRequest_ROLLBACK, 0, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner := &backend.TransactionalProvider{}
			session := openTransactionSession(c, owner, transactionAddress(c))
			scope := nativeHistoryScope(session.GetId())
			c.Assert(readHistory(c, owner, scope).GetExists(), qt.IsFalse)
			prepareHistory(c, owner, scope)
			c.Assert(readHistory(c, owner, scope).GetExists(), qt.IsTrue)
			tx, err := owner.BeginTransaction(c.Context(), &protocol.BeginTransactionRequest{Isolation: protocol.TransactionIsolation_DEFAULT, SessionId: session.GetId(), TransactionId: "tx"})
			c.Assert(err, qt.IsNil)
			scope.TransactionId = tx.GetId()
			c.Assert(executeTransactionSQL(c, owner, session.GetId(), tx.GetId(), `CREATE TABLE "records" ("id" INTEGER NOT NULL PRIMARY KEY);`).GetOutcome(), qt.Equals, protocol.Execution_APPLIED)
			result, err := owner.MutateHistory(c.Context(), &protocol.MutateHistoryRequest{Scope: scope, RequestId: "record", Replacement: nativeHistoryRow()})
			c.Assert(err, qt.IsNil)
			c.Assert(provider.CheckHistoryResult(scope, "record", result), qt.IsNil)
			c.Assert(readHistory(c, owner, scope).GetRevisions(), qt.HasLen, 1)
			_, err = owner.FinishTransaction(c.Context(), &protocol.FinishTransactionRequest{SessionId: session.GetId(), TransactionId: tx.GetId(), Action: test.action})
			c.Assert(err, qt.IsNil)
			scope.TransactionId = ""
			c.Assert(readHistory(c, owner, scope).GetRevisions(), qt.HasLen, test.rows)
			c.Assert(tablePresent(c, owner, session.GetId(), "records"), qt.Equals, test.table)
		})
	}
}
func TestHistoryCompareAndSwapDetectsAnotherSessionWrite(t *testing.T) {
	c := qt.New(t)
	owner := &backend.TransactionalProvider{}
	address := transactionAddress(c)
	first := openTransactionSession(c, owner, address)
	second := openTransactionSession(c, owner, address)
	scope := nativeHistoryScope(first.GetId())
	prepareHistory(c, owner, scope)
	row := nativeHistoryRow()
	result, err := owner.MutateHistory(c.Context(), &protocol.MutateHistoryRequest{Scope: scope, RequestId: "insert", Replacement: row})
	c.Assert(err, qt.IsNil)
	c.Assert(result.GetOutcome(), qt.Equals, protocol.HistoryResult_APPLIED)
	next := proto.CloneOf(row)
	next.Description = "changed by another runner"
	other := nativeHistoryScope(second.GetId())
	result, err = owner.MutateHistory(c.Context(), &protocol.MutateHistoryRequest{Scope: other, RequestId: "update", Expected: row, Replacement: next})
	c.Assert(err, qt.IsNil)
	c.Assert(result.GetOutcome(), qt.Equals, protocol.HistoryResult_APPLIED)
	result, err = owner.MutateHistory(c.Context(), &protocol.MutateHistoryRequest{Scope: scope, RequestId: "stale-delete", Expected: row})
	c.Assert(err, qt.IsNil)
	c.Assert(result.GetOutcome(), qt.Equals, protocol.HistoryResult_CONFLICT)
	got := readHistory(c, owner, scope)
	c.Assert(got.GetRevisions(), qt.HasLen, 1)
	c.Assert(proto.Equal(got.GetRevisions()[0], next), qt.IsTrue)
	result, err = owner.MutateHistory(c.Context(), &protocol.MutateHistoryRequest{Scope: scope, RequestId: "delete", Expected: next})
	c.Assert(err, qt.IsNil)
	c.Assert(result.GetOutcome(), qt.Equals, protocol.HistoryResult_APPLIED)
	c.Assert(readHistory(c, owner, scope).GetRevisions(), qt.HasLen, 0)
}
func TestHistoryRefusesForeignAndClosedTransactionHandles(t *testing.T) {
	c := qt.New(t)
	owner := &backend.TransactionalProvider{}
	address := transactionAddress(c)
	first := openTransactionSession(c, owner, address)
	second := openTransactionSession(c, owner, address)
	scope := nativeHistoryScope(first.GetId())
	prepareHistory(c, owner, scope)
	_, err := owner.BeginTransaction(c.Context(), &protocol.BeginTransactionRequest{Isolation: protocol.TransactionIsolation_DEFAULT, SessionId: first.GetId(), TransactionId: "tx"})
	c.Assert(err, qt.IsNil)
	foreign := nativeHistoryScope(second.GetId())
	foreign.TransactionId = "tx"
	result, err := owner.MutateHistory(c.Context(), &protocol.MutateHistoryRequest{Scope: foreign, RequestId: "foreign", Replacement: nativeHistoryRow()})
	c.Assert(err, qt.IsNotNil)
	c.Assert(result, qt.IsNil)
	scope.TransactionId = "tx"
	c.Assert(owner.Close(context.Background(), &protocol.CloseRequest{SessionId: first.GetId()}), qt.IsNil)
	result, err = owner.MutateHistory(c.Context(), &protocol.MutateHistoryRequest{Scope: scope, RequestId: "closed", Replacement: nativeHistoryRow()})
	c.Assert(err, qt.IsNotNil)
	c.Assert(result, qt.IsNil)
	foreign.TransactionId = ""
	c.Assert(readHistory(c, owner, foreign).GetRevisions(), qt.HasLen, 0)
}
func TestHistoryRefusesOversizedReadWithoutPartialRows(t *testing.T) {
	c := qt.New(t)
	owner := &backend.TransactionalProvider{}
	session := openTransactionSession(c, owner, transactionAddress(c))
	scope := nativeHistoryScope(session.GetId())
	prepareHistory(c, owner, scope)
	for _, version := range []string{"1", "2"} {
		row := nativeHistoryRow()
		row.Version = version
		result, err := owner.MutateHistory(c.Context(), &protocol.MutateHistoryRequest{Scope: scope, RequestId: version, Replacement: row})
		c.Assert(err, qt.IsNil)
		c.Assert(result.GetOutcome(), qt.Equals, protocol.HistoryResult_APPLIED)
	}
	result, err := owner.ReadHistory(c.Context(), &protocol.ReadHistoryRequest{Scope: scope, MaxRevisions: 1})
	c.Assert(err, qt.ErrorIs, provider.ErrIncomplete)
	c.Assert(result, qt.IsNil)
}
func TestHistoryRefusesExistingForeignLayout(t *testing.T) {
	c := qt.New(t)
	owner := &backend.TransactionalProvider{}
	session := openTransactionSession(c, owner, transactionAddress(c))
	c.Assert(executeTransactionSQL(c, owner, session.GetId(), "", `CREATE TABLE "_revisions" (id INTEGER);`).GetOutcome(), qt.Equals, protocol.Execution_APPLIED)
	result, err := owner.PrepareHistory(c.Context(), &protocol.PrepareHistoryRequest{Scope: nativeHistoryScope(session.GetId()), RequestId: "prepare"})
	c.Assert(err, qt.ErrorMatches, "fixture history storage has an incompatible layout")
	c.Assert(result, qt.IsNil)
}

func TestHistoryRefusesTriggersBeforeWriting(t *testing.T) {
	c := qt.New(t)
	owner := &backend.TransactionalProvider{}
	session := openTransactionSession(c, owner, transactionAddress(c))
	scope := nativeHistoryScope(session.GetId())
	prepareHistory(c, owner, scope)
	result := executeTransactionSQL(c, owner, session.GetId(), "", `CREATE TEMP TRIGGER erase_history AFTER INSERT ON main."_revisions" BEGIN DELETE FROM "_revisions"; END;`)
	c.Assert(result.GetOutcome(), qt.Equals, protocol.Execution_APPLIED)
	receipt, err := owner.MutateHistory(c.Context(), &protocol.MutateHistoryRequest{Scope: scope, RequestId: "record", Replacement: nativeHistoryRow()})
	c.Assert(err, qt.ErrorMatches, "fixture history storage has triggers")
	c.Assert(receipt, qt.IsNil)
}
func TestHistoryRefusesEarlierEncodingWithoutReinterpretingRows(t *testing.T) {
	c := qt.New(t)
	owner := &backend.TransactionalProvider{}
	session := openTransactionSession(c, owner, transactionAddress(c))
	scope := nativeHistoryScope(session.GetId())
	prepareHistory(c, owner, scope)
	receipt, err := owner.MutateHistory(c.Context(), &protocol.MutateHistoryRequest{Scope: scope, RequestId: "record", Replacement: nativeHistoryRow()})
	c.Assert(err, qt.IsNil)
	c.Assert(receipt.GetOutcome(), qt.Equals, protocol.HistoryResult_APPLIED)
	c.Assert(executeTransactionSQL(c, owner, session.GetId(), "", `UPDATE "_revisions" SET revision = X'0a0131';`).GetOutcome(), qt.Equals, protocol.Execution_APPLIED)
	snapshot, err := owner.ReadHistory(c.Context(), &protocol.ReadHistoryRequest{Scope: scope, MaxRevisions: 10})
	c.Assert(err, qt.ErrorMatches, "fixture history has an incompatible encoding")
	c.Assert(snapshot, qt.IsNil)
}
