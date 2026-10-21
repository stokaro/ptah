//go:build integration

package integration_test

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	transport "github.com/stokaro/ptah/provider-sdk/runtime"
)

func lockOptions(c *qt.C) transport.Options {
	c.Helper()
	binary := buildFixture(c, "locks")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	return transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture", RequiredFeatures: []string{provider.LocksFeature, provider.TransactionsFeature, provider.HistoryFeature}}
}

func reopenLockedDatabase(c *qt.C, client *transport.Client, database, fault string) *protocol.Session {
	c.Helper()
	address := &url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(database), "/"), RawQuery: url.Values{"fault": {fault}}.Encode()}
	c.Cleanup(client.Close)
	session, err := client.Open(c.Context(), &protocol.OpenRequest{Engine: "fixture", Address: address.String()})
	c.Assert(err, qt.IsNil)
	return session
}

func lockRequest(session *protocol.Session, id, name string) *protocol.LockRequest {
	return &protocol.LockRequest{Scope: &protocol.LockScope{SessionId: session.GetId(), LockId: id, Name: name}, RequestId: rand.Text()}
}

func TestIndependentProviderLockExclusion(t *testing.T) {
	c := qt.New(t)
	options := lockOptions(c)
	owner, contender := start(c, options), start(c, options)
	session := open(c, owner, "")
	other := reopenLockedDatabase(c, contender, session.GetDatabase(), "")
	result, err := owner.TryLock(c.Context(), lockRequest(session, "owner", "migration"))
	c.Assert(err, qt.IsNil)
	c.Assert(result.GetOutcome(), qt.Equals, protocol.LockResult_ACQUIRED)
	_, err = contender.TryLock(c.Context(), lockRequest(other, "busy", "migration"))
	c.Assert(err, qt.ErrorIs, provider.ErrLockBusy)
	_, err = contender.TryLock(c.Context(), lockRequest(other, "other-name", "unrelated"))
	c.Assert(err, qt.IsNil)
	separate := open(c, contender, "")
	_, err = contender.TryLock(c.Context(), lockRequest(separate, "separate-database", "migration"))
	c.Assert(err, qt.IsNil)
	_, err = contender.ReleaseLock(c.Context(), lockRequest(other, "owner", "migration"))
	c.Assert(err, qt.IsNil)
	_, err = contender.TryLock(c.Context(), lockRequest(other, "still-busy", "migration"))
	c.Assert(err, qt.ErrorIs, provider.ErrLockBusy)
	_, err = owner.CheckLock(c.Context(), lockRequest(session, "owner", "changed-name"))
	c.Assert(err, qt.IsNotNil)
	_, err = owner.CheckLock(c.Context(), lockRequest(session, "owner", "migration"))
	c.Assert(err, qt.IsNil)
	_, err = owner.ReleaseLock(c.Context(), lockRequest(session, "owner", "migration"))
	c.Assert(err, qt.IsNil)
	_, err = contender.TryLock(c.Context(), lockRequest(other, "busy", "migration"))
	c.Assert(err, qt.ErrorIs, provider.ErrLockBusy)
	_, err = contender.TryLock(c.Context(), lockRequest(other, "owner", "migration"))
	c.Assert(err, qt.ErrorIs, provider.ErrExecution)
	_, err = owner.TryLock(c.Context(), lockRequest(session, "owner", "migration"))
	c.Assert(err, qt.ErrorIs, provider.ErrExecution)
	_, err = contender.TryLock(c.Context(), lockRequest(other, "fresh", "migration"))
	c.Assert(err, qt.IsNil)
	c.Assert(contender.CloseSession(c.Context(), other.GetId()), qt.IsNil)
	_, err = owner.TryLock(c.Context(), lockRequest(session, "after-close", "migration"))
	c.Assert(err, qt.IsNil)
	_, err = contender.TryLock(c.Context(), lockRequest(other, "late", "migration"))
	c.Assert(err, qt.IsNotNil)
}

func TestIndependentProviderLockDeathAndLostReceipt(t *testing.T) {
	c := qt.New(t)
	options := lockOptions(c)
	t.Run("crash after committed write", func(t *testing.T) {
		c := qt.New(t)
		owner, contender := start(c, options), start(c, options)
		session := open(c, owner, "crash-after")
		other := reopenLockedDatabase(c, contender, session.GetDatabase(), "")
		crashLockedOwner(c, owner, contender, session, other)
		assertLockRecovery(c, contender, other, []string{"first"})
	})
	t.Run("lost acquisition reply", func(t *testing.T) {
		c := qt.New(t)
		owner, contender := start(c, options), start(c, options)
		session := open(c, owner, "lost-lock-reply")
		other := reopenLockedDatabase(c, contender, session.GetDatabase(), "")
		loseAcquisitionReply(c, owner, session)
		assertLockRecovery(c, contender, other, make([]string, 0))
	})
}

func assertLockRecovery(c *qt.C, contender *transport.Client, other *protocol.Session, tables []string) {
	c.Helper()
	_, err := contender.TryLock(c.Context(), lockRequest(other, "fresh", "migration"))
	c.Assert(err, qt.IsNil)
	schema, err := contender.Inspect(c.Context(), &protocol.InspectRequest{SessionId: other.GetId()})
	c.Assert(err, qt.IsNil)
	names := make([]string, len(schema.GetTables()))
	for index, table := range schema.GetTables() {
		names[index] = table.GetIdentity().GetName()
	}
	c.Assert(names, qt.DeepEquals, tables)
}

func crashLockedOwner(c *qt.C, owner, contender *transport.Client, session, other *protocol.Session) {
	c.Helper()
	_, err := owner.TryLock(c.Context(), lockRequest(session, "owner", "migration"))
	c.Assert(err, qt.IsNil)
	_, err = contender.TryLock(c.Context(), lockRequest(other, "busy", "migration"))
	c.Assert(err, qt.ErrorIs, provider.ErrLockBusy)
	_, err = owner.Execute(c.Context(), execution(session.GetId()))
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
}

func loseAcquisitionReply(c *qt.C, owner *transport.Client, session *protocol.Session) {
	c.Helper()
	_, err := owner.TryLock(c.Context(), lockRequest(session, "owner", "migration"))
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
}

func TestIndependentProviderLostLockFencesWrites(t *testing.T) {
	c := qt.New(t)
	options := lockOptions(c)
	t.Run("ownership check detects rollback", func(t *testing.T) {
		c := qt.New(t)
		scenario := newLockScenario(c, options, "lose-lock-on-check")
		loseCheckedLock(c, scenario.owner, scenario.session)
		scenario.assertFenced(c)
	})
	t.Run("execution detects rollback without an earlier check", func(t *testing.T) {
		c := qt.New(t)
		scenario := newLockScenario(c, options, "lose-lock-before-write")
		loseLockBeforeWrite(c, scenario.owner, scenario.session)
		scenario.assertFenced(c)
	})
	t.Run("commit refuses and explicit rollback remains available", func(t *testing.T) {
		c := qt.New(t)
		scenario := newLockScenario(c, options, "lose-lock-before-commit")
		loseLockBeforeCommit(c, scenario.owner, scenario.session, scenario.scope)
		scenario.assertFenced(c)
	})
}

type lockScenario struct {
	owner, contender *transport.Client
	session, other   *protocol.Session
	scope            *protocol.HistoryScope
}

func newLockScenario(c *qt.C, options transport.Options, fault string) lockScenario {
	c.Helper()
	owner, contender := start(c, options), start(c, options)
	session := open(c, owner, fault)
	other := reopenLockedDatabase(c, contender, session.GetDatabase(), "")
	scope := processHistoryScope(session)
	_, err := owner.PrepareHistory(c.Context(), &protocol.PrepareHistoryRequest{Scope: scope, RequestId: "prepare"})
	c.Assert(err, qt.IsNil)
	_, err = owner.TryLock(c.Context(), lockRequest(session, "owner", "migration"))
	c.Assert(err, qt.IsNil)
	return lockScenario{owner: owner, contender: contender, session: session, other: other, scope: scope}
}

func (s lockScenario) assertFenced(c *qt.C) {
	c.Helper()
	_, err := s.owner.ReleaseLock(c.Context(), lockRequest(s.session, "owner", "migration"))
	c.Assert(err, qt.ErrorIs, provider.ErrLockLost)
	_, err = s.owner.TryLock(c.Context(), lockRequest(s.session, "replacement", "migration"))
	c.Assert(err, qt.ErrorIs, provider.ErrLockLost)
	_, err = s.owner.Execute(c.Context(), execution(s.session.GetId()))
	c.Assert(err, qt.IsNotNil)
	_, err = s.owner.PrepareHistory(c.Context(), &protocol.PrepareHistoryRequest{Scope: s.scope, RequestId: "prepare-again"})
	c.Assert(err, qt.IsNotNil)
	_, err = s.owner.MutateHistory(c.Context(), &protocol.MutateHistoryRequest{Scope: s.scope, RequestId: "late-record", Replacement: processHistoryRow()})
	c.Assert(err, qt.IsNotNil)
	_, err = s.owner.BeginTransaction(c.Context(), &protocol.BeginTransactionRequest{SessionId: s.session.GetId(), TransactionId: "late-tx", Isolation: protocol.TransactionIsolation_DEFAULT})
	c.Assert(err, qt.IsNotNil)
	assertLockRecovery(c, s.contender, s.other, []string{"_revisions"})
	snapshot, err := s.contender.ReadHistory(c.Context(), &protocol.ReadHistoryRequest{Scope: processHistoryScope(s.other), MaxRevisions: 10})
	c.Assert(err, qt.IsNil)
	c.Assert(snapshot.GetRevisions(), qt.HasLen, 0)
}

func loseCheckedLock(c *qt.C, owner *transport.Client, session *protocol.Session) {
	c.Helper()
	_, err := owner.CheckLock(c.Context(), lockRequest(session, "owner", "migration"))
	c.Assert(err, qt.ErrorIs, provider.ErrLockLost)
}

func loseLockBeforeWrite(c *qt.C, owner *transport.Client, session *protocol.Session) {
	c.Helper()
	_, err := owner.Execute(c.Context(), execution(session.GetId()))
	c.Assert(err, qt.IsNotNil)
}

func loseLockBeforeCommit(c *qt.C, owner *transport.Client, session *protocol.Session, scope *protocol.HistoryScope) {
	c.Helper()
	tx, err := owner.BeginTransaction(c.Context(), &protocol.BeginTransactionRequest{SessionId: session.GetId(), TransactionId: "tx", Isolation: protocol.TransactionIsolation_DEFAULT})
	c.Assert(err, qt.IsNil)
	request := execution(session.GetId())
	request.TransactionId = tx.GetId()
	_, err = owner.Execute(c.Context(), request)
	c.Assert(err, qt.IsNil)
	scope.TransactionId = tx.GetId()
	_, err = owner.MutateHistory(c.Context(), &protocol.MutateHistoryRequest{Scope: scope, RequestId: "record", Replacement: processHistoryRow()})
	c.Assert(err, qt.IsNil)
	_, err = owner.FinishTransaction(c.Context(), &protocol.FinishTransactionRequest{SessionId: session.GetId(), TransactionId: tx.GetId(), Action: protocol.FinishTransactionRequest_COMMIT})
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	_, err = owner.FinishTransaction(c.Context(), &protocol.FinishTransactionRequest{SessionId: session.GetId(), TransactionId: tx.GetId(), Action: protocol.FinishTransactionRequest_ROLLBACK})
	c.Assert(err, qt.IsNil)
	scope.TransactionId = ""
}
