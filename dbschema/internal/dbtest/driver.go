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

type DB struct {
	SQL        *sql.DB
	queryCount *atomic.Int64
}

func Open(t interface{ Cleanup(func()) }, handler QueryHandler) *DB {
	name := "ptah_dbtest_" + strconv.FormatInt(driverID.Add(1), 10)
	queryCount := new(atomic.Int64)
	sql.Register(name, &countingDriver{
		handler:    handler,
		queryCount: queryCount,
	})
	db, err := sql.Open(name, "")
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return &DB{SQL: db, queryCount: queryCount}
}

func (db *DB) QueryCount() int {
	return int(db.queryCount.Load())
}

var driverID atomic.Int64

type countingDriver struct {
	handler    QueryHandler
	queryCount *atomic.Int64
}

func (d *countingDriver) Open(_ string) (driver.Conn, error) {
	return &countingConn{
		handler:    d.handler,
		queryCount: d.queryCount,
	}, nil
}

type countingConn struct {
	handler    QueryHandler
	queryCount *atomic.Int64
}

func (c *countingConn) Prepare(_ string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare is not supported")
}

func (c *countingConn) Close() error {
	return nil
}

func (c *countingConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("transactions are not supported")
}

func (c *countingConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.queryCount.Add(1)
	result, err := c.handler(query, args)
	if err != nil {
		return nil, err
	}
	return &rows{columns: result.Columns, rows: result.Rows}, nil
}

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
