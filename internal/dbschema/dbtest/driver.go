package dbtest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
)

type QueryResult struct {
	Columns []string
	Rows    [][]driver.Value
}

type QueryHandler func(query string, args []driver.NamedValue) (QueryResult, error)

type ExecHandler func(query string, args []driver.NamedValue) (driver.Result, error)

type DB struct {
	SQL        *sql.DB
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
	name := "ptah_dbtest_" + strconv.FormatInt(driverID.Add(1), 10)
	queryCount := new(atomic.Int64)
	execCount := new(atomic.Int64)
	beginCount := new(atomic.Int64)
	commits := new(atomic.Int64)
	rollbacks := new(atomic.Int64)
	sql.Register(name, &countingDriver{
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
		queryCount: queryCount,
		execCount:  execCount,
		beginCount: beginCount,
		commits:    commits,
		rollbacks:  rollbacks,
	}
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
	queryHandler QueryHandler
	execHandler  ExecHandler
	queryCount   *atomic.Int64
	execCount    *atomic.Int64
	beginCount   *atomic.Int64
	commits      *atomic.Int64
	rollbacks    *atomic.Int64
}

func (d *countingDriver) Open(_ string) (driver.Conn, error) {
	return &countingConn{
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
	queryHandler QueryHandler
	execHandler  ExecHandler
	queryCount   *atomic.Int64
	execCount    *atomic.Int64
	beginCount   *atomic.Int64
	commits      *atomic.Int64
	rollbacks    *atomic.Int64
}

func (c *countingConn) Prepare(_ string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare is not supported")
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
	return &rows{columns: result.Columns, rows: result.Rows}, nil
}

func (c *countingConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.execCount.Add(1)
	if c.execHandler == nil {
		return driver.RowsAffected(0), nil
	}
	return c.execHandler(query, args)
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

var _ driver.ConnBeginTx = (*countingConn)(nil)
var _ driver.ExecerContext = (*countingConn)(nil)
var _ driver.QueryerContext = (*countingConn)(nil)

type rows struct {
	columns []string
	rows    [][]driver.Value
	index   int
}

func (r *rows) Columns() []string {
	return r.columns
}

func (r *rows) Close() error {
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.index])
	r.index++
	return nil
}
