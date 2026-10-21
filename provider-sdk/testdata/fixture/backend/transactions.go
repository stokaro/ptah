package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// TransactionalProvider establishes transactional DDL for the SQLite fixture.
// Service advertisement is selected by the executable, not by this wrapper.
type TransactionalProvider struct{ Provider }

// Open establishes the session's transactional DDL capability.
func (p *TransactionalProvider) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	session, err := p.Provider.Open(ctx, request)
	if err != nil {
		return nil, err
	}
	session.Capabilities = map[string]bool{"transactional_ddl": true}
	return session, nil
}

type transaction struct {
	tx      *sql.Tx
	outcome protocol.TransactionResult_Outcome
}
type executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (s *session) executor(id string) (executor, error) {
	if id == "" {
		if s.active != "" {
			return nil, fmt.Errorf("fixture session has an active transaction")
		}
		return s.db, nil
	}
	tx := s.transactions[id]
	if tx == nil || tx.tx == nil || s.active != id {
		return nil, fmt.Errorf("transaction is not active in this session")
	}
	return tx.tx, nil
}

// BeginTransaction reserves each caller handle once, including failed begins.
// The session lock serializes reservation with finish, execution, and close.
func (p *Provider) BeginTransaction(ctx context.Context, request *protocol.BeginTransactionRequest) (*protocol.Transaction, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := provider.ValidateBeginTransactionRequest(request); err != nil {
		return nil, err
	}
	s, err := p.lookup(request.GetSessionId())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed {
		return nil, fmt.Errorf("fixture session is closed")
	}
	if err := s.guardLocks(ctx); err != nil {
		return nil, err
	}
	if s.transactions == nil {
		s.transactions = make(map[string]*transaction)
	}
	id := request.GetTransactionId()
	if s.transactions[id] != nil {
		return nil, fmt.Errorf("fixture transaction handle was already reserved")
	}
	record := &transaction{outcome: protocol.TransactionResult_NOT_STARTED}
	s.transactions[id] = record
	if s.active != "" {
		return nil, fmt.Errorf("fixture session has an active transaction")
	}
	// database/sql ties a transaction to its begin context. The RPC context ends
	// when its reply is sent, so the session owns the lifetime instead. The pool
	// has one connection and the active check prevents waiting behind another tx.
	tx, err := s.beginDatabaseTransaction(ctx, request.GetIsolation())
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		rollbackErr := tx.Rollback()
		record.outcome = protocol.TransactionResult_ROLLED_BACK
		if rollbackErr != nil {
			record.outcome = protocol.TransactionResult_UNKNOWN
		}
		return nil, errors.Join(err, rollbackErr)
	}
	record.tx, record.outcome, s.active = tx, protocol.TransactionResult_UNKNOWN, id
	return &protocol.Transaction{SessionId: request.GetSessionId(), Id: id, Isolation: request.GetIsolation()}, nil
}

// The fixture opens private SQLite connections. Its driver ignores the isolation
// option, so inspect the actual connection setting before claiming serializable
// isolation. WAL snapshots refuse a write after a concurrent commit.
func (s *session) beginDatabaseTransaction(ctx context.Context, isolation protocol.TransactionIsolation) (*sql.Tx, error) {
	opts := &sql.TxOptions{}
	if isolation == protocol.TransactionIsolation_SERIALIZABLE {
		opts.Isolation = sql.LevelSerializable
	}
	tx, err := s.db.BeginTx(context.WithoutCancel(ctx), opts)
	if err != nil {
		return nil, err
	}
	if isolation == protocol.TransactionIsolation_SERIALIZABLE {
		var uncommitted int
		if err := tx.QueryRowContext(ctx, "PRAGMA read_uncommitted").Scan(&uncommitted); err != nil {
			return nil, errors.Join(err, tx.Rollback())
		}
		if uncommitted != 0 {
			return nil, errors.Join(fmt.Errorf("fixture cannot establish serializable isolation with read_uncommitted enabled"), tx.Rollback())
		}
	}
	return tx, nil
}

// FinishTransaction retains terminal results and fences handles that never
// began. It never issues another database operation for a terminal handle.
func (p *Provider) FinishTransaction(ctx context.Context, request *protocol.FinishTransactionRequest) (*protocol.TransactionResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := provider.ValidateTransactionRequest(request.GetSessionId(), request.GetTransactionId()); err != nil {
		return nil, err
	}
	if err := provider.ValidateTransactionAction(request.GetAction()); err != nil {
		return nil, err
	}
	s, err := p.lookup(request.GetSessionId())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed {
		return nil, fmt.Errorf("fixture session is closed")
	}
	if s.transactions == nil {
		s.transactions = make(map[string]*transaction)
	}
	record := s.transactions[request.GetTransactionId()]
	if record == nil {
		record = &transaction{outcome: protocol.TransactionResult_NOT_STARTED}
		s.transactions[request.GetTransactionId()] = record
	}
	if record.tx == nil {
		return &protocol.TransactionResult{Outcome: record.outcome}, nil
	}
	if request.GetAction() == protocol.FinishTransactionRequest_COMMIT {
		if s.fault == "lose-lock-before-commit" {
			s.interruptLocks()
		}
		if err := s.guardLocks(ctx); err != nil {
			return nil, err
		}
	}
	if s.fault == "lost-before-commit" && request.GetAction() == protocol.FinishTransactionRequest_COMMIT {
		return nil, fmt.Errorf("fixture lost commit request before execution")
	}
	record.outcome = finishDatabaseTransaction(record.tx, request.GetAction())
	record.tx, s.active = nil, ""
	if s.fault == "lost-commit" && request.GetAction() == protocol.FinishTransactionRequest_COMMIT {
		return nil, fmt.Errorf("fixture lost commit reply")
	}
	if s.fault == "reject-rollback" && request.GetAction() == protocol.FinishTransactionRequest_ROLLBACK {
		return nil, fmt.Errorf("fixture lost rollback reply")
	}
	return &protocol.TransactionResult{Outcome: record.outcome}, nil
}

func finishDatabaseTransaction(tx *sql.Tx, action protocol.FinishTransactionRequest_Action) protocol.TransactionResult_Outcome {
	if action == protocol.FinishTransactionRequest_COMMIT {
		if err := tx.Commit(); err != nil {
			return protocol.TransactionResult_UNKNOWN
		}
		return protocol.TransactionResult_COMMITTED
	}
	if err := tx.Rollback(); err != nil {
		return protocol.TransactionResult_UNKNOWN
	}
	return protocol.TransactionResult_ROLLED_BACK
}

func (s *session) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	var failures []error
	for _, record := range s.transactions {
		if record.tx != nil {
			failures = append(failures, record.tx.Rollback())
			record.tx = nil
		}
	}
	for _, record := range s.locks {
		failures = append(failures, record.release())
	}
	failures = append(failures, s.db.Close())
	return errors.Join(failures...)
}
