package sessiontx_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sessiontx"
)

// Canceling the caller's context leaves the transaction to the caller: the
// driver began it under a context that is not done, the caller's rollback is
// the one that reaches the driver, and the session is discarded afterwards.
// Bound to the caller's context instead, the transaction is ended by
// database/sql from a goroutine of its own, which is the discard a later
// Conn.Raw races.
func TestBegin_HappyPath(t *testing.T) {
	c := qt.New(t)
	backend := &recordingBackend{}
	db := sql.OpenDB(backend)
	defer db.Close()
	session, err := db.Conn(t.Context())
	c.Assert(err, qt.IsNil)

	ctx, cancel := context.WithCancel(t.Context())
	tx, err := sessiontx.Begin(ctx, session, nil)
	c.Assert(err, qt.IsNil)
	cancel()

	c.Assert(backend.beginContextErr(), qt.IsNil)
	c.Assert(tx.Rollback(), qt.IsNil)
	c.Assert(backend.counts(), qt.Equals, counts{begins: 1, rollbacks: 1})
	c.Assert(session.Raw(func(any) error { return driver.ErrBadConn }), qt.ErrorIs, driver.ErrBadConn)
	c.Assert(backend.counts(), qt.Equals, counts{begins: 1, rollbacks: 1, closes: 1})
}

func TestBegin_FailurePath(t *testing.T) {
	c := qt.New(t)
	backend := &recordingBackend{}
	db := sql.OpenDB(backend)
	defer db.Close()
	session, err := db.Conn(t.Context())
	c.Assert(err, qt.IsNil)
	defer session.Close()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	tx, err := sessiontx.Begin(ctx, session, nil)

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(tx, qt.IsNil)
	c.Assert(backend.counts(), qt.Equals, counts{})
}

type counts struct {
	begins, rollbacks, closes int
}

// recordingBackend is a database/sql driver that records the transactions it
// begins and ends and the connections it closes.
type recordingBackend struct {
	mu           sync.Mutex
	beginContext context.Context
	recorded     counts
}

func (b *recordingBackend) Connect(context.Context) (driver.Conn, error) {
	return &recordingConn{b}, nil
}
func (b *recordingBackend) Driver() driver.Driver { return recordingDriver{} }

func (b *recordingBackend) beginContextErr() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.beginContext.Err()
}

func (b *recordingBackend) counts() counts {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.recorded
}

type recordingDriver struct{}

func (recordingDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("open through the connector")
}

type recordingConn struct{ backend *recordingBackend }

func (c *recordingConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("no statements") }
func (c *recordingConn) Begin() (driver.Tx, error)           { return nil, errors.New("begin through BeginTx") }

func (c *recordingConn) Close() error {
	c.backend.mu.Lock()
	defer c.backend.mu.Unlock()
	c.backend.recorded.closes++
	return nil
}

func (c *recordingConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	c.backend.mu.Lock()
	defer c.backend.mu.Unlock()
	c.backend.beginContext = ctx
	c.backend.recorded.begins++
	return recordingTx{c.backend}, nil
}

type recordingTx struct{ backend *recordingBackend }

func (recordingTx) Commit() error { return errors.New("no commits") }

func (t recordingTx) Rollback() error {
	t.backend.mu.Lock()
	defer t.backend.mu.Unlock()
	t.backend.recorded.rollbacks++
	return nil
}
