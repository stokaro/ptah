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

	"go.5x5.cz/ptah/internal/dbschema/sqlite"
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
	c.Assert(trace.statements(), qt.Contains, `DROP TABLE IF EXISTS "main"."children"`)
	c.Assert(trace.statements(), qt.Contains, `DROP TABLE IF EXISTS "main"."parents"`)
	c.Assert(trace.statements(), qt.Contains, "PRAGMA foreign_keys = 1")
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
	c.Assert(trace.statements(), qt.Contains, "PRAGMA foreign_keys = 1")
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
	c.Assert(trace.statements(), qt.Contains, "PRAGMA foreign_keys = 1")
}

func TestWriterDropAllTables_RestoresDisabledForeignKeys(t *testing.T) {
	c := qt.New(t)
	trace := new(sqliteCleanupTrace)
	db := openTrackedSQLiteDBWithDSN(t, trace, filepath.Join(t.TempDir(), "cleanup.sqlite"))
	execSQL(t, db, `CREATE TABLE users (id INTEGER PRIMARY KEY)`)
	trace.reset()

	writer := sqlite.NewSQLiteWriter(db, "main")
	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(trace.statements(), qt.Contains, "PRAGMA foreign_keys = 0")
}

func TestWriterDropAllTables_RestoresAfterDisableFailure(t *testing.T) {
	c := qt.New(t)
	trace := new(sqliteCleanupTrace)
	db := openTrackedSQLiteDB(t, trace)
	execSQL(t, db, `CREATE TABLE users (id INTEGER PRIMARY KEY)`)
	trace.reset()
	disableErr := errors.New("disable outcome unknown")
	trace.disableErr = disableErr

	writer := sqlite.NewSQLiteWriter(db, "main")
	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorIs, disableErr)
	c.Assert(trace.statements(), qt.Contains, "PRAGMA foreign_keys = OFF")
	c.Assert(trace.statements(), qt.Contains, "PRAGMA foreign_keys = 1")
}

func TestWriterDropAllTables_CleansOnlyConfiguredAttachedSchema(t *testing.T) {
	c := qt.New(t)
	db := openFileSQLiteDB(t)
	conn, err := db.Conn(t.Context())
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})
	auxPath := filepath.Join(t.TempDir(), "aux'quoted.sqlite")
	_, err = conn.ExecContext(t.Context(), `ATTACH DATABASE ? AS aux`, auxPath)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), `CREATE TABLE main.users (id INTEGER PRIMARY KEY)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), `CREATE TABLE aux.users (id INTEGER PRIMARY KEY)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), `CREATE VIRTUAL TABLE aux.search USING fts5(body)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), `CREATE TEMP TABLE users (id INTEGER PRIMARY KEY)`)
	c.Assert(err, qt.IsNil)

	writer := sqlite.NewSQLiteWriterForConnection(conn, "aux")
	err = writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(sqliteConnSchemaObjectCount(c, conn, "main", "users"), qt.Equals, 1)
	c.Assert(sqliteConnSchemaObjectCount(c, conn, "aux", "users"), qt.Equals, 0)
	c.Assert(sqliteConnSchemaObjectCount(c, conn, "aux", "search"), qt.Equals, 0)
	c.Assert(sqliteConnSchemaObjectCount(c, conn, "temp", "users"), qt.Equals, 1)
}

func TestWriterDropAllTables_PreservesPaddedCatalogIdentifierBytes(t *testing.T) {
	c := qt.New(t)
	db := openFileSQLiteDB(t)
	conn, err := db.Conn(t.Context())
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})
	paddedPath := filepath.Join(t.TempDir(), "padded.sqlite")
	unpaddedPath := filepath.Join(t.TempDir(), "unpadded.sqlite")
	_, err = conn.ExecContext(t.Context(), `ATTACH DATABASE ? AS " aux "`, paddedPath)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), `ATTACH DATABASE ? AS aux`, unpaddedPath)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), `CREATE TABLE " aux ".users (id INTEGER PRIMARY KEY)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), `CREATE TABLE aux.users (id INTEGER PRIMARY KEY)`)
	c.Assert(err, qt.IsNil)

	writer := sqlite.NewSQLiteWriterForConnection(conn, " aux ")
	err = writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(sqliteConnSchemaObjectCount(c, conn, " aux ", "users"), qt.Equals, 0)
	c.Assert(sqliteConnSchemaObjectCount(c, conn, "aux", "users"), qt.Equals, 1)
}

func TestWriterDropAllTables_RefusesCleanupWhenTempViewExists(t *testing.T) {
	c := qt.New(t)
	trace := new(sqliteCleanupTrace)
	db := openTrackedSQLiteDB(t, trace)
	conn, err := db.Conn(t.Context())
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})
	_, err = conn.ExecContext(t.Context(), `CREATE TABLE main.users (id INTEGER PRIMARY KEY)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), `INSERT INTO main.users (id) VALUES (7)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), `CREATE TEMP VIEW temp_users AS SELECT id FROM main.users`)
	c.Assert(err, qt.IsNil)
	trace.reset()

	writer := sqlite.NewSQLiteWriterForConnection(conn, "main")
	err = writer.DropAllTables(t.Context())
	statements := trace.statements()

	c.Assert(
		err,
		qt.ErrorMatches,
		`sqlite: refusing to clean schema "main": TEMP views exist on the cleanup connection and may depend on it`,
	)
	c.Assert(statements, qt.Not(qt.Contains), "PRAGMA foreign_keys = OFF")
	c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "DROP ")
	c.Assert(sqliteConnSchemaObjectCount(c, conn, "main", "users"), qt.Equals, 1)
	c.Assert(sqliteConnSchemaObjectCount(c, conn, "temp", "temp_users"), qt.Equals, 1)

	var id int
	err = conn.QueryRowContext(t.Context(), `SELECT id FROM temp_users`).Scan(&id)
	c.Assert(err, qt.IsNil)
	c.Assert(id, qt.Equals, 7)
}

func TestWriterDropDatabaseRealm_CleansRevisionAndUserObjects(t *testing.T) {
	c := qt.New(t)
	db := openFileSQLiteDB(t)
	conn, err := db.Conn(t.Context())
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})
	_, err = conn.ExecContext(t.Context(), `CREATE TABLE users (id INTEGER PRIMARY KEY)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), `CREATE INDEX users_id_idx ON users (id)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), `CREATE VIEW user_ids AS SELECT id FROM users`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), `CREATE VIRTUAL TABLE search USING fts5(body)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), `CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY)`)
	c.Assert(err, qt.IsNil)

	writer := sqlite.NewSQLiteWriterForConnection(conn, "main")
	err = writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.IsNil)
	var count int
	err = conn.QueryRowContext(t.Context(), `
		SELECT COUNT(*)
		FROM main.sqlite_schema
		WHERE name NOT LIKE 'sqlite_%'
	`).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}

func TestWriterDropDatabaseRealm_RequiresPinnedConnection(t *testing.T) {
	c := qt.New(t)
	db := openFileSQLiteDB(t)
	writer := sqlite.NewSQLiteWriter(db, "main")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorMatches, `sqlite: database-realm cleanup requires a pinned connection`)
}

func TestWriterDropDatabaseRealm_RejectsAttachedDatabase(t *testing.T) {
	c := qt.New(t)
	db := openFileSQLiteDB(t)
	conn, err := db.Conn(t.Context())
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})
	// The file is not named for the schema it is attached as. `aux` is a
	// reserved DOS device name, and SQLite cannot open a database file called
	// that on Windows -- measured in a windowsservercore-ltsc2022 container on
	// Go 1.26.6, where this test failed with `unable to open database:
	// …\aux.sqlite (14)`, SQLITE_CANTOPEN. The schema alias below stays `aux`
	// because the refusal this test asserts names it.
	attachedPath := filepath.Join(t.TempDir(), "attached.sqlite")
	_, err = conn.ExecContext(t.Context(), `ATTACH DATABASE ? AS aux`, attachedPath)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), `CREATE TABLE main.users (id INTEGER PRIMARY KEY)`)
	c.Assert(err, qt.IsNil)

	writer := sqlite.NewSQLiteWriterForConnection(conn, "main")
	err = writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorMatches, `sqlite: refusing database-realm cleanup with attached databases: aux`)
	c.Assert(sqliteConnSchemaObjectCount(c, conn, "main", "users"), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_RejectsTempObject(t *testing.T) {
	c := qt.New(t)
	db := openFileSQLiteDB(t)
	conn, err := db.Conn(t.Context())
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})
	_, err = conn.ExecContext(t.Context(), `CREATE TEMP TABLE scratch (id INTEGER PRIMARY KEY)`)
	c.Assert(err, qt.IsNil)

	writer := sqlite.NewSQLiteWriterForConnection(conn, "main")
	err = writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorMatches, `sqlite: refusing database-realm cleanup with TEMP objects: table:scratch`)
	c.Assert(sqliteConnSchemaObjectCount(c, conn, "temp", "scratch"), qt.Equals, 1)
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

	return openTrackedSQLiteDBWithDSN(t, trace, filepath.Join(t.TempDir(), "cleanup.sqlite")+"?_pragma=foreign_keys(1)")
}

func openTrackedSQLiteDBWithDSN(t *testing.T, trace *sqliteCleanupTrace, dsn string) *sql.DB {
	t.Helper()

	driverName := "ptah_sqlite_cleanup_" + strconv.FormatInt(sqliteCleanupDriverID.Add(1), 10)
	sql.Register(driverName, &sqliteCleanupDriver{trace: trace})

	db, err := sql.Open(driverName, dsn)
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

func sqliteConnSchemaObjectCount(c *qt.C, conn *sql.Conn, schema, name string) int {
	c.Helper()
	var count int
	const query = `SELECT COUNT(*) FROM pragma_table_list WHERE schema = ? AND name = ?`
	err := conn.QueryRowContext(c.Context(), query, schema, name).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
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
	disableErr     error
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
	disableErr := tr.disableErr
	dropErr := tr.dropErr
	restoreErr := tr.restoreErr
	tr.mu.Unlock()

	switch {
	case statement == "PRAGMA foreign_keys = OFF" && disableErr != nil:
		return disableErr
	case strings.HasPrefix(statement, "DROP TABLE") && dropErr != nil:
		if cancelOnDrop != nil {
			cancelOnDrop()
		}
		return dropErr
	case strings.HasPrefix(statement, "PRAGMA foreign_keys = ") &&
		statement != "PRAGMA foreign_keys = OFF" &&
		restoreErr != nil:
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
