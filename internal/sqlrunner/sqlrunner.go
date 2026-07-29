// Package sqlrunner defines the SQL operations shared by pooled and pinned
// database sessions.
package sqlrunner

import (
	"context"
	"database/sql"
)

// Runner is the database/sql surface used by schema readers and writers.
// Both *sql.DB and Conn satisfy it.
type Runner interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	Exec(string, ...any) (sql.Result, error)
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	Query(string, ...any) (*sql.Rows, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRow(string, ...any) *sql.Row
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// Connector acquires a dedicated physical database session.
type Connector interface {
	Conn(context.Context) (*sql.Conn, error)
}

// Conn adapts *sql.Conn to Runner. Context-free calls use the context that was
// active when the session was pinned.
type Conn struct {
	ctx  context.Context
	conn *sql.Conn
}

// NewConn binds conn to ctx without taking ownership of conn.
func NewConn(ctx context.Context, conn *sql.Conn) *Conn {
	return &Conn{ctx: ctx, conn: conn}
}

// BeginTx starts a transaction on the pinned session.
func (c *Conn) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return c.conn.BeginTx(ctx, opts)
}

// Exec executes a statement on the pinned session.
func (c *Conn) Exec(query string, args ...any) (sql.Result, error) {
	return c.conn.ExecContext(c.ctx, query, args...)
}

// ExecContext executes a statement on the pinned session.
func (c *Conn) ExecContext(
	ctx context.Context,
	query string,
	args ...any,
) (sql.Result, error) {
	return c.conn.ExecContext(ctx, query, args...)
}

// Query executes a query on the pinned session.
func (c *Conn) Query(query string, args ...any) (*sql.Rows, error) {
	return c.conn.QueryContext(c.ctx, query, args...)
}

// QueryContext executes a query on the pinned session.
func (c *Conn) QueryContext(
	ctx context.Context,
	query string,
	args ...any,
) (*sql.Rows, error) {
	return c.conn.QueryContext(ctx, query, args...)
}

// QueryRow executes a single-row query on the pinned session.
func (c *Conn) QueryRow(query string, args ...any) *sql.Row {
	return c.conn.QueryRowContext(c.ctx, query, args...)
}

// QueryRowContext executes a single-row query on the pinned session.
func (c *Conn) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return c.conn.QueryRowContext(ctx, query, args...)
}
