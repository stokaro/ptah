package providerhost

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"sync"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

func (t *target) supportsTransactions() bool {
	return slices.Contains(t.engine.GetFeatures(), provider.TransactionsFeature) && slices.Contains(t.host.description.GetFeatures(), provider.TransactionsFeature)
}

func (s *session) BeginTransaction(ctx context.Context) (catalog.SchemaTransaction, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed.Load() {
		return nil, fmt.Errorf("provider session is closed")
	}
	if s.IsDryRun() {
		return dryTransaction{session: s}, nil
	}
	if err := s.CheckTransaction(); err != nil {
		return nil, err
	}
	return s.beginProviderTransaction(ctx, protocol.TransactionIsolation_DEFAULT)
}

// CheckTransaction says why BeginTransaction would refuse this session, and
// asks the provider nothing: what it reads was settled when the session opened.
// A dry run begins no transaction, so its BeginTransaction refuses nothing, and
// this answers for the run that follows it.
func (s *session) CheckTransaction() error {
	if !s.target.supportsTransactions() {
		return unsupported("transactions for the selected engine")
	}
	if !s.info.Capabilities.Has(capability.TransactionalDDL) {
		return unsupported("transactions without established transactional DDL")
	}
	return nil
}

// beginProviderTransaction opens the shared remote handle. Callers choose the
// restricted history surface or establish transactional DDL before exposing SQL.
func (s *session) beginProviderTransaction(ctx context.Context, isolation protocol.TransactionIsolation) (*transaction, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed.Load() {
		return nil, fmt.Errorf("provider session is closed")
	}
	if err := s.lockError(); err != nil {
		return nil, err
	}
	if s.IsDryRun() {
		return nil, unsupported("history transactions during a dry run")
	}
	if !s.target.supportsTransactions() {
		return nil, unsupported("transactions for the selected engine")
	}
	handle, err := s.target.host.client.BeginTransaction(ctx, &protocol.BeginTransactionRequest{SessionId: s.remote.GetId(), TransactionId: rand.Text(), Isolation: isolation})
	if err != nil {
		return nil, nativeOutcomeError(err)
	}
	return &transaction{session: s, id: handle.GetId(), ctx: ctx}, nil
}

// A failed execution prevents commit even when its reply is uncertain. Rollback
// remains available for cleanup. A finish is sent once per action; confirmed
// terminal replies are reused and never cause another database write.
type transaction struct {
	mu            sync.Mutex
	session       *session
	id            string
	ctx           context.Context
	executionErr  error
	commitTried   bool
	rollbackTried bool
	result        *protocol.TransactionResult
	// commitOutcome is the outcome the provider answered a commit with, and
	// UNKNOWN when no commit was answered.
	commitOutcome protocol.TransactionResult_Outcome
	finishErr     error
}

func (*transaction) IsDryRun() bool { return false }
func (t *transaction) ExecuteSQL(ctx context.Context, sql string, args ...any) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.executionReady(ctx); err != nil {
		return err
	}
	if len(args) != 0 {
		return unsupported("bound SQL parameters")
	}
	t.executionErr = t.session.execute(ctx, t.id, sql)
	return t.executionErr
}
func (t *transaction) executionReady(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if t.session.closed.Load() {
		return fmt.Errorf("provider session is closed")
	}
	if t.commitTried || t.rollbackTried {
		return fmt.Errorf("provider transaction has finished execution")
	}
	if t.executionErr != nil {
		return t.executionErr
	}
	return nil
}

func (t *transaction) ValidateUnits(ctx context.Context, units []renderer.Fragment) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.executionReady(ctx); err != nil {
		return err
	}
	_, err := t.session.prepareUnits(ctx, units)
	return err
}

func (t *transaction) ExecuteUnits(ctx context.Context, units []renderer.Fragment) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.executionReady(ctx); err != nil {
		return err
	}
	prepared, err := t.session.prepareUnits(ctx, units)
	if err != nil {
		return err
	}
	if len(prepared.units) == 0 {
		return nil
	}
	t.executionErr = t.session.dispatchUnits(ctx, t.id, prepared)
	return t.executionErr
}

func (t *transaction) Commit() error   { return t.finish(protocol.FinishTransactionRequest_COMMIT) }
func (t *transaction) Rollback() error { return t.finish(protocol.FinishTransactionRequest_ROLLBACK) }

// RolledBackAtCommit reports that the provider answered the commit with
// ROLLED_BACK, so nothing the transaction executed took effect. A commit that
// was refused before it was sent, or whose reply was lost, answers false, and
// so does a rollback the host asked for.
func (t *transaction) RolledBackAtCommit() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.commitOutcome == protocol.TransactionResult_ROLLED_BACK
}

func (t *transaction) finish(action protocol.FinishTransactionRequest_Action) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.result != nil && t.result.GetOutcome() != protocol.TransactionResult_UNKNOWN {
		return nativeOutcomeError(provider.CheckTransactionResult(action, t.result))
	}
	if action == protocol.FinishTransactionRequest_COMMIT {
		if t.commitTried || t.rollbackTried {
			return nativeOutcomeError(errors.Join(t.finishErr, &provider.TransactionError{}))
		}
		if t.executionErr != nil {
			return fmt.Errorf("provider transaction cannot commit after failed execution: %w", t.executionErr)
		}
		if err := t.session.lockError(); err != nil {
			return err
		}
		t.commitTried = true
	} else {
		if t.rollbackTried {
			return t.finishErr
		}
		t.rollbackTried = true
	}
	if t.session.closed.Load() {
		t.finishErr = nativeOutcomeError(errors.Join(fmt.Errorf("provider session is closed"), &provider.TransactionError{}))
		return t.finishErr
	}
	ctx := t.ctx
	if action == protocol.FinishTransactionRequest_ROLLBACK {
		ctx = context.WithoutCancel(ctx)
	}
	t.result, t.finishErr = t.session.target.host.client.FinishTransaction(ctx, &protocol.FinishTransactionRequest{
		SessionId: t.session.remote.GetId(), TransactionId: t.id, Action: action,
	})
	if action == protocol.FinishTransactionRequest_COMMIT {
		t.commitOutcome = t.result.GetOutcome()
	}
	t.finishErr = nativeOutcomeError(t.finishErr)
	if t.result.GetAborted() {
		t.finishErr = abortedError(t.finishErr)
	}
	return t.finishErr
}
