package sqlite_test

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"log/slog"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	qt "github.com/frankban/quicktest"
	moderncsqlite "modernc.org/sqlite"

	"github.com/stokaro/ptah/internal/dbschema/sqlite"
)

func TestWriterDropAllTables_DoesNotLog(t *testing.T) {
	c := qt.New(t)
	db := openFileSQLiteDB(t)
	execSQL(t, db, `CREATE TABLE users (id INTEGER PRIMARY KEY)`)

	var output bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
	})

	writer := sqlite.NewSQLiteWriter(db, "main")
	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(output.String(), qt.Equals, "")
}

func TestWriterDropAllTables_PinsCleanupToOneConnection(t *testing.T) {
	c := qt.New(t)
	trace := new(sqliteCleanupTrace)
	db := openTrackedSQLiteDB(t, trace)
	execSQL(t, db, `CREATE TABLE parents (id INTEGER PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE children (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER NOT NULL REFERENCES parents(id)
	)`)
	trace.reset()

	writer := sqlite.NewSQLiteWriter(db, "main")
	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(trace.connectionIDs(), qt.HasLen, 1)
	c.Assert(trace.statements(), qt.Contains, "PRAGMA foreign_keys = OFF")
	c.Assert(trace.statements(), qt.Contains, `DROP TABLE IF EXISTS "children"`)
	c.Assert(trace.statements(), qt.Contains, `DROP TABLE IF EXISTS "parents"`)
	c.Assert(trace.statements(), qt.Contains, "PRAGMA foreign_keys = ON")
}

func TestWriterDropAllTables_RestoresForeignKeysAfterCancellation(t *testing.T) {
	c := qt.New(t)
	trace := new(sqliteCleanupTrace)
	db := openTrackedSQLiteDB(t, trace)
	execSQL(t, db, `CREATE TABLE users (id INTEGER PRIMARY KEY)`)
	trace.reset()

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	trace.cancelOnDrop = cancel
	trace.dropErr = context.Canceled

	writer := sqlite.NewSQLiteWriter(db, "main")
	err := writer.DropAllTables(ctx)

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(trace.connectionIDs(), qt.HasLen, 1)
	c.Assert(trace.statements(), qt.Contains, "PRAGMA foreign_keys = ON")
}

func TestWriterDropAllTables_JoinsPrimaryAndRestoreErrors(t *testing.T) {
	c := qt.New(t)
	trace := new(sqliteCleanupTrace)
	db := openTrackedSQLiteDB(t, trace)
	execSQL(t, db, `CREATE TABLE users (id INTEGER PRIMARY KEY)`)
	trace.reset()

	dropErr := errors.New("drop failed")
	restoreErr := errors.New("restore failed")
	trace.dropErr = dropErr
	trace.restoreErr = restoreErr

	writer := sqlite.NewSQLiteWriter(db, "main")
	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorIs, dropErr)
	c.Assert(err, qt.ErrorIs, restoreErr)
	c.Assert(trace.statements(), qt.Contains, "PRAGMA foreign_keys = ON")
}

func openFileSQLiteDB(t *testing.T) *sql.DB {
	t.Helper()

	path := filepath.Join(t.TempDir(), "cleanup.sqlite")
	db, err := sql.Open("sqlite", path+"?_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func openTrackedSQLiteDB(t *testing.T, trace *sqliteCleanupTrace) *sql.DB {
	t.Helper()

	driverName := "ptah_sqlite_cleanup_" + strconv.FormatInt(sqliteCleanupDriverID.Add(1), 10)
	sql.Register(driverName, &sqliteCleanupDriver{trace: trace})

	path := filepath.Join(t.TempDir(), "cleanup.sqlite")
	db, err := sql.Open(driverName, path+"?_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open tracked sqlite: %v", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(0)
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

var sqliteCleanupDriverID atomic.Int64

type sqliteCleanupEvent struct {
	connectionID int
	statement    string
}

type sqliteCleanupTrace struct {
	mu             sync.Mutex
	nextConnection atomic.Int64
	events         []sqliteCleanupEvent
	cancelOnDrop   context.CancelFunc
	dropErr        error
	restoreErr     error
}

func (tr *sqliteCleanupTrace) record(connectionID int, query string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.events = append(tr.events, sqliteCleanupEvent{
		connectionID: connectionID,
		statement:    normalizeSQL(query),
	})
}

func (tr *sqliteCleanupTrace) executionError(query string) error {
	statement := normalizeSQL(query)

	tr.mu.Lock()
	cancelOnDrop := tr.cancelOnDrop
	dropErr := tr.dropErr
	restoreErr := tr.restoreErr
	tr.mu.Unlock()

	switch {
	case strings.HasPrefix(statement, "DROP TABLE") && dropErr != nil:
		if cancelOnDrop != nil {
			cancelOnDrop()
		}
		return dropErr
	case statement == "PRAGMA foreign_keys = ON" && restoreErr != nil:
		return restoreErr
	default:
		return nil
	}
}

func (tr *sqliteCleanupTrace) reset() {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.events = nil
}

func (tr *sqliteCleanupTrace) statements() []string {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	statements := make([]string, len(tr.events))
	for index, event := range tr.events {
		statements[index] = event.statement
	}
	return statements
}

func (tr *sqliteCleanupTrace) connectionIDs() []int {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	unique := make(map[int]struct{})
	for _, event := range tr.events {
		unique[event.connectionID] = struct{}{}
	}
	ids := make([]int, 0, len(unique))
	for id := range unique {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

type sqliteCleanupDriver struct {
	trace *sqliteCleanupTrace
}

func (drv *sqliteCleanupDriver) Open(name string) (driver.Conn, error) {
	conn, err := new(moderncsqlite.Driver).Open(name)
	if err != nil {
		return nil, err
	}
	return &sqliteCleanupConn{
		Conn:         conn,
		connectionID: int(drv.trace.nextConnection.Add(1)),
		trace:        drv.trace,
	}, nil
}

type sqliteCleanupConn struct {
	driver.Conn
	connectionID int
	trace        *sqliteCleanupTrace
}

func (conn *sqliteCleanupConn) QueryContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Rows, error) {
	conn.trace.record(conn.connectionID, query)
	queryer, ok := conn.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return queryer.QueryContext(ctx, query, args)
}

func (conn *sqliteCleanupConn) ExecContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Result, error) {
	conn.trace.record(conn.connectionID, query)
	if err := conn.trace.executionError(query); err != nil {
		return nil, err
	}
	execer, ok := conn.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return execer.ExecContext(ctx, query, args)
}

func (conn *sqliteCleanupConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	beginner, ok := conn.Conn.(driver.ConnBeginTx)
	if !ok {
		return nil, driver.ErrSkip
	}
	return beginner.BeginTx(ctx, opts)
}

func (conn *sqliteCleanupConn) ResetSession(ctx context.Context) error {
	resetter, ok := conn.Conn.(driver.SessionResetter)
	if !ok {
		return nil
	}
	return resetter.ResetSession(ctx)
}

func (conn *sqliteCleanupConn) IsValid() bool {
	validator, ok := conn.Conn.(driver.Validator)
	return !ok || validator.IsValid()
}

func normalizeSQL(query string) string {
	return strings.Join(strings.Fields(query), " ")
}

var (
	_ driver.ConnBeginTx     = (*sqliteCleanupConn)(nil)
	_ driver.ExecerContext   = (*sqliteCleanupConn)(nil)
	_ driver.QueryerContext  = (*sqliteCleanupConn)(nil)
	_ driver.SessionResetter = (*sqliteCleanupConn)(nil)
	_ driver.Validator       = (*sqliteCleanupConn)(nil)
)
