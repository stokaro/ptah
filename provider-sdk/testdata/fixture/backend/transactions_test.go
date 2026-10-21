package backend_test

import (
	"context"
	"net/url"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"example.com/ptah-fixture-provider/backend"
	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

func openTransactionSession(c *qt.C, owner *backend.TransactionalProvider, address string) *protocol.Session {
	c.Helper()
	session, err := owner.Open(c.Context(), &protocol.OpenRequest{Engine: "fixture", Address: address})
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(owner.Close(context.Background(), &protocol.CloseRequest{SessionId: session.GetId()}), qt.IsNil)
	})
	return session
}
func transactionAddress(c *qt.C) string {
	c.Helper()
	return (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(filepath.Join(c.TempDir(), "transactions.db")), "/")}).String()
}
func executeTransactionSQL(c *qt.C, owner *backend.TransactionalProvider, sessionID, transactionID, sql string) *protocol.Execution {
	c.Helper()
	result, err := owner.Execute(c.Context(), &protocol.ExecuteRequest{SessionId: sessionID, TransactionId: transactionID, RequestId: "request", Units: []*protocol.Unit{{ChangeId: "statement", Body: &protocol.Unit_Sql{Sql: sql}}}})
	c.Assert(err, qt.IsNil)
	return result
}
func tablePresent(c *qt.C, owner *backend.TransactionalProvider, sessionID, name string) bool {
	c.Helper()
	schema, err := owner.Inspect(c.Context(), &protocol.InspectRequest{SessionId: sessionID})
	c.Assert(err, qt.IsNil)
	return slices.ContainsFunc(schema.GetTables(), func(table *protocol.Table) bool { return table.GetIdentity().GetName() == name })
}
func TestTransactionLifetimeSurvivesBeginCallAndRecordsFinalState(t *testing.T) {
	for _, test := range []struct {
		name    string
		action  protocol.FinishTransactionRequest_Action
		outcome protocol.TransactionResult_Outcome
		present bool
	}{
		{"commit", protocol.FinishTransactionRequest_COMMIT, protocol.TransactionResult_COMMITTED, true},
		{"rollback", protocol.FinishTransactionRequest_ROLLBACK, protocol.TransactionResult_ROLLED_BACK, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner := &backend.TransactionalProvider{}
			session := openTransactionSession(c, owner, transactionAddress(c))
			request := &protocol.BeginTransactionRequest{Isolation: protocol.TransactionIsolation_DEFAULT, SessionId: session.GetId(), TransactionId: "transaction"}
			call, cancel := context.WithCancel(c.Context())
			handle, err := owner.BeginTransaction(call, request)
			cancel()
			c.Assert(err, qt.IsNil)
			c.Assert(handle.GetId(), qt.Equals, "transaction")
			result := executeTransactionSQL(c, owner, session.GetId(), handle.GetId(), `CREATE TABLE "records" ("id" INTEGER NOT NULL PRIMARY KEY);`)
			c.Assert(result.GetOutcome(), qt.Equals, protocol.Execution_APPLIED)
			finish := &protocol.FinishTransactionRequest{SessionId: session.GetId(), TransactionId: handle.GetId(), Action: test.action}
			terminal, err := owner.FinishTransaction(c.Context(), finish)
			c.Assert(err, qt.IsNil)
			c.Assert(terminal.GetOutcome(), qt.Equals, test.outcome)
			finish.Action = protocol.FinishTransactionRequest_ROLLBACK
			repeated, err := owner.FinishTransaction(c.Context(), finish)
			c.Assert(err, qt.IsNil)
			c.Assert(repeated.GetOutcome(), qt.Equals, test.outcome)
			c.Assert(tablePresent(c, owner, session.GetId(), "records"), qt.Equals, test.present)
			_, err = owner.BeginTransaction(c.Context(), request)
			c.Assert(err, qt.IsNotNil)
			c.Assert(executeTransactionSQL(c, owner, session.GetId(), handle.GetId(), `CREATE TABLE "late" ("id" INTEGER);`).GetOutcome(), qt.Equals, protocol.Execution_NOT_STARTED)
		})
	}
}
func TestTransactionCloseRollsBackAndSessionIdentityCannotBeSubstituted(t *testing.T) {
	c := qt.New(t)
	owner := &backend.TransactionalProvider{}
	address := transactionAddress(c)
	session := openTransactionSession(c, owner, address)
	other := openTransactionSession(c, owner, transactionAddress(c))
	handle, err := owner.BeginTransaction(c.Context(), &protocol.BeginTransactionRequest{Isolation: protocol.TransactionIsolation_DEFAULT, SessionId: session.GetId(), TransactionId: "transaction"})
	c.Assert(err, qt.IsNil)
	c.Assert(executeTransactionSQL(c, owner, other.GetId(), handle.GetId(), `CREATE TABLE "wrong" ("id" INTEGER);`).GetOutcome(), qt.Equals, protocol.Execution_NOT_STARTED)
	c.Assert(executeTransactionSQL(c, owner, session.GetId(), handle.GetId(), `CREATE TABLE "abandoned" ("id" INTEGER);`).GetOutcome(), qt.Equals, protocol.Execution_APPLIED)
	c.Assert(owner.Close(c.Context(), &protocol.CloseRequest{SessionId: session.GetId()}), qt.IsNil)
	_, err = owner.BeginTransaction(c.Context(), &protocol.BeginTransactionRequest{Isolation: protocol.TransactionIsolation_DEFAULT, SessionId: session.GetId(), TransactionId: "late"})
	c.Assert(err, qt.IsNotNil)
	fresh := openTransactionSession(c, owner, address)
	c.Assert(tablePresent(c, owner, fresh.GetId(), "abandoned"), qt.IsFalse)
	c.Assert(tablePresent(c, owner, other.GetId(), "wrong"), qt.IsFalse)
}
func TestRollbackFencesConcurrentAndLateBegins(t *testing.T) {
	c := qt.New(t)
	owner := &backend.TransactionalProvider{}
	session := openTransactionSession(c, owner, transactionAddress(c))
	for i := range 32 {
		id := strconv.Itoa(i)
		beginDone := make(chan struct{})
		go func() {
			defer close(beginDone)
			_, _ = owner.BeginTransaction(c.Context(), &protocol.BeginTransactionRequest{Isolation: protocol.TransactionIsolation_DEFAULT, SessionId: session.GetId(), TransactionId: id})
		}()
		result, err := owner.FinishTransaction(c.Context(), &protocol.FinishTransactionRequest{SessionId: session.GetId(), TransactionId: id, Action: protocol.FinishTransactionRequest_ROLLBACK})
		<-beginDone
		c.Assert(err, qt.IsNil)
		c.Assert([]protocol.TransactionResult_Outcome{protocol.TransactionResult_NOT_STARTED, protocol.TransactionResult_ROLLED_BACK}, qt.Contains, result.GetOutcome())
		_, err = owner.BeginTransaction(c.Context(), &protocol.BeginTransactionRequest{Isolation: protocol.TransactionIsolation_DEFAULT, SessionId: session.GetId(), TransactionId: id})
		c.Assert(err, qt.IsNotNil)
		c.Assert(executeTransactionSQL(c, owner, session.GetId(), id, `CREATE TABLE "late" ("id" INTEGER);`).GetOutcome(), qt.Equals, protocol.Execution_NOT_STARTED)
	}
	c.Assert(tablePresent(c, owner, session.GetId(), "late"), qt.IsFalse)
}
