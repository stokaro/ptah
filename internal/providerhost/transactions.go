package providerhost

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"sync"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
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
		return dryTransaction{}, nil
	}
	if !s.target.supportsTransactions() {
		return nil, unsupported("transactions for the selected engine")
	}
	if !s.info.Capabilities.Has(capability.TransactionalDDL) {
		return nil, unsupported("transactions without established transactional DDL")
	}
	return s.beginProviderTransaction(ctx)
}

// beginProviderTransaction opens the shared remote handle. Callers choose the
// restricted history surface or establish transactional DDL before exposing SQL.
func (s *session) beginProviderTransaction(ctx context.Context) (*transaction, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed.Load() {
		return nil, fmt.Errorf("provider session is closed")
	}
	if s.IsDryRun() {
		return nil, unsupported("history transactions during a dry run")
	}
	if !s.target.supportsTransactions() {
		return nil, unsupported("transactions for the selected engine")
	}
	handle, err := s.target.host.client.BeginTransaction(ctx, &protocol.BeginTransactionRequest{SessionId: s.remote.GetId(), TransactionId: rand.Text()})
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
	finishErr     error
}

func (*transaction) IsDryRun() bool { return false }
func (t *transaction) ExecuteSQL(ctx context.Context, sql string, args ...any) error {
	t.mu.Lock()
	defer t.mu.Unlock()
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
	if len(args) != 0 {
		return unsupported("bound SQL parameters")
	}
	t.executionErr = t.session.execute(ctx, t.id, sql)
	return t.executionErr
}
func (t *transaction) Commit() error   { return t.finish(protocol.FinishTransactionRequest_COMMIT) }
func (t *transaction) Rollback() error { return t.finish(protocol.FinishTransactionRequest_ROLLBACK) }

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
	t.finishErr = nativeOutcomeError(t.finishErr)
	return t.finishErr
}
