package backend

import (
	"context"
	"database/sql"
	"fmt"
)

// Querier runs the statements of a service that a later contract minor adds:
// the session's database, or the transaction a request names. *sql.DB and
// *sql.Tx both satisfy it.
type Querier interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// Access is what such a service reaches of an open session.
type Access struct {
	// Querier is the session's database, or the named transaction.
	Querier Querier
	// DB is the session's database. It is nil when a transaction was named,
	// so a service cannot begin a second transaction beside an active one.
	DB *sql.DB
	// Path is the database file.
	Path string
	// Fault is the fault mode the address selected.
	Fault string
}

// WithSession runs use with the session sessionID names, holding the session
// for the call, so it is serialized with execution, transactions and close.
// An empty transactionID selects the session's database, which an active
// transaction makes unavailable; otherwise the transaction must be the
// session's active one.
func (p *Provider) WithSession(ctx context.Context, sessionID, transactionID string, use func(Access) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s, err := p.lookup(sessionID)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("fixture session is closed")
	}
	if err := s.guardLocks(ctx); err != nil {
		return err
	}
	queries, err := s.executor(transactionID)
	if err != nil {
		return err
	}
	access := Access{Path: s.path, Fault: s.fault}
	switch value := queries.(type) {
	case *sql.DB:
		access.Querier, access.DB = value, value
	case *sql.Tx:
		access.Querier = value
	default:
		return fmt.Errorf("fixture session has no database")
	}
	return use(access)
}
