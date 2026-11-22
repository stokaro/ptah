// Package dbtest provides an in-memory database/sql driver whose query and
// exec behavior is scripted per test, so dialect readers and writers can be
// tested without a live database.
package dbtest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
)

type QueryResult struct {
	Columns []string
	Rows    [][]driver.Value

	// TerminalErr is delivered INSTEAD of the end of the result set, after
	// every row in Rows has been handed over. It is what a connection dropping
	// mid-stream looks like to a reader: database/sql reports it through
	// [database/sql.Rows.Err] and not through Scan, so a reader that only
	// checks Scan sees the rows that did arrive and no error at all.
	//
	// Leave it nil for an ordinary result. It must not be [driver.ErrBadConn],
	// which database/sql intercepts and retries the whole query on rather than
	// surfacing.
	TerminalErr error
}

type QueryHandler func(query string, args []driver.NamedValue) (QueryResult, error)

type ExecHandler func(query string, args []driver.NamedValue) (driver.Result, error)

// ExecContextHandler is an [ExecHandler] that also receives the context the
// caller passed, so a test can block until that context ends and see which
// context a statement ran under.
type ExecContextHandler func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error)

type DB struct {
	SQL        *sql.DB
	prepared   *preparedLog
	queryCount *atomic.Int64
	execCount  *atomic.Int64
	beginCount *atomic.Int64
	commits    *atomic.Int64
	rollbacks  *atomic.Int64
}

func Open(t interface{ Cleanup(func()) }, handler QueryHandler) *DB {
	return OpenWithExec(t, handler, nil)
}

func OpenWithExec(t interface{ Cleanup(func()) }, queryHandler QueryHandler, execHandler ExecHandler) *DB {
	if execHandler == nil {
		return OpenWithExecContext(t, queryHandler, nil)
	}
	return OpenWithExecContext(t, queryHandler,
		func(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
			return execHandler(query, args)
		},
	)
}

// OpenWithExecContext is [OpenWithExec] with an exec handler that receives the
// statement's context.
func OpenWithExecContext(
	t interface{ Cleanup(func()) },
	queryHandler QueryHandler,
	execHandler ExecContextHandler,
) *DB {
	name := "ptah_dbtest_" + strconv.FormatInt(driverID.Add(1), 10)
	queryCount := new(atomic.Int64)
	execCount := new(atomic.Int64)
	beginCount := new(atomic.Int64)
	commits := new(atomic.Int64)
	rollbacks := new(atomic.Int64)
	prepared := new(preparedLog)
	sql.Register(name, &countingDriver{
		prepared:     prepared,
		queryHandler: queryHandler,
		execHandler:  execHandler,
		queryCount:   queryCount,
		execCount:    execCount,
		beginCount:   beginCount,
		commits:      commits,
		rollbacks:    rollbacks,
	})
	db, err := sql.Open(name, "")
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return &DB{
		SQL:        db,
		prepared:   prepared,
		queryCount: queryCount,
		execCount:  execCount,
		beginCount: beginCount,
		commits:    commits,
		rollbacks:  rollbacks,
	}
}

// Prepared is every statement a caller prepared, in the order it prepared
// them. A statement run without a prepare is in the exec or query handler's
// log and not here.
func (db *DB) Prepared() []string {
	return db.prepared.all()
}

// preparedLog is the statements a [DB] prepared, shared by its connections.
type preparedLog struct {
	mu         sync.Mutex
	statements []string
}

func (l *preparedLog) add(statement string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.statements = append(l.statements, statement)
}

func (l *preparedLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.statements...)
}

func (db *DB) QueryCount() int {
	return int(db.queryCount.Load())
}

func (db *DB) ExecCount() int {
	return int(db.execCount.Load())
}

func (db *DB) BeginCount() int {
	return int(db.beginCount.Load())
}

func (db *DB) CommitCount() int {
	return int(db.commits.Load())
}

func (db *DB) RollbackCount() int {
	return int(db.rollbacks.Load())
}

var driverID atomic.Int64

type countingDriver struct {
	prepared     *preparedLog
	queryHandler QueryHandler
	execHandler  ExecContextHandler
	queryCount   *atomic.Int64
	execCount    *atomic.Int64
	beginCount   *atomic.Int64
	commits      *atomic.Int64
	rollbacks    *atomic.Int64
}

func (d *countingDriver) Open(_ string) (driver.Conn, error) {
	return &countingConn{
		prepared:     d.prepared,
		queryHandler: d.queryHandler,
		execHandler:  d.execHandler,
		queryCount:   d.queryCount,
		execCount:    d.execCount,
		beginCount:   d.beginCount,
		commits:      d.commits,
		rollbacks:    d.rollbacks,
	}, nil
}

type countingConn struct {
	prepared     *preparedLog
	queryHandler QueryHandler
	execHandler  ExecContextHandler
	queryCount   *atomic.Int64
	execCount    *atomic.Int64
	beginCount   *atomic.Int64
	commits      *atomic.Int64
	rollbacks    *atomic.Int64
}

// Prepare answers a statement that runs through the scripted handlers, as
// the query or exec it would be without the prepare, so a caller that
// prepares each statement before it runs it meets the same script.
func (c *countingConn) Prepare(query string) (driver.Stmt, error) {
	c.prepared.add(query)
	return &stmt{conn: c, query: query}, nil
}

func (c *countingConn) Close() error {
	return nil
}

func (c *countingConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *countingConn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	c.beginCount.Add(1)
	return &tx{commits: c.commits, rollbacks: c.rollbacks}, nil
}

func (c *countingConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.queryCount.Add(1)
	if c.queryHandler == nil {
		return nil, fmt.Errorf("queries are not supported")
	}
	result, err := c.queryHandler(query, args)
	if err != nil {
		return nil, err
	}
	return &rows{columns: result.Columns, rows: result.Rows, terminalErr: result.TerminalErr}, nil
}

func (c *countingConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.execCount.Add(1)
	if c.execHandler == nil {
		return driver.RowsAffected(0), nil
	}
	return c.execHandler(ctx, query, args)
}

type tx struct {
	commits   *atomic.Int64
	rollbacks *atomic.Int64
}

func (tx *tx) Commit() error {
	tx.commits.Add(1)
	return nil
}

func (tx *tx) Rollback() error {
	tx.rollbacks.Add(1)
	return nil
}

// stmt is a prepared statement of [countingConn]: running it runs its query
// through the connection.
type stmt struct {
	conn  *countingConn
	query string
}

func (s *stmt) Close() error { return nil }

// NumInput is -1: the driver does not count placeholders, and database/sql
// then passes every argument through.
func (s *stmt) NumInput() int { return -1 }

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), namedValues(args))
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), namedValues(args))
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.query, args)
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.QueryContext(ctx, s.query, args)
}

// namedValues is args in the shape the context methods take.
func namedValues(args []driver.Value) []driver.NamedValue {
	named := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: arg}
	}
	return named
}

var _ driver.StmtExecContext = (*stmt)(nil)
var _ driver.StmtQueryContext = (*stmt)(nil)

var _ driver.ConnBeginTx = (*countingConn)(nil)
var _ driver.ExecerContext = (*countingConn)(nil)
var _ driver.QueryerContext = (*countingConn)(nil)

type rows struct {
	columns     []string
	rows        [][]driver.Value
	index       int
	terminalErr error
}

func (r *rows) Columns() []string {
	return r.columns
}

func (r *rows) Close() error {
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		if r.terminalErr != nil {
			return r.terminalErr
		}
		return io.EOF
	}
	copy(dest, r.rows[r.index])
	r.index++
	return nil
}
