package ydb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Coordination_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Scheme_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"

	"ptah.run/internal/ydbcomment"
	"ptah.run/internal/ydbcoordination"
	"ptah.run/internal/ydbsecret"
)

// connector hands database/sql the SDK's connections with Ptah's argument
// binding in front of them, and runs onClose after the SDK connector closes.
// sdk is the driver the connections belong to, which [DriverOf] hands back;
// it is nil for a connector [NewBindingConnector] built. prefix is written in
// front of every query the connections run, and is empty unless the URL named
// a dev realm. root is the directory a relative name in those queries means:
// the realm's, or the database's.
type connector struct {
	inner  driver.Connector
	sdk    *ydbsdk.Driver
	prefix string
	// root is the absolute path the connections treat as their database: the
	// dev realm's directory when the URL named one, empty otherwise.
	root    string
	onClose func() error
}

// NewBindingConnector returns inner with Ptah's argument binding in front of
// every connection it makes; see [conn.CheckNamedValue]. Closing the result
// closes inner when inner is an io.Closer, and then calls onClose, which may
// be nil. A connection inner makes must offer what database/sql uses of a
// ydb-go-sdk connection, or Connect refuses it.
func NewBindingConnector(inner driver.Connector, onClose func() error) driver.Connector {
	return &connector{inner: inner, onClose: onClose}
}

// sdkConn is what database/sql uses of a ydb-go-sdk connection. The SDK's
// connection type is internal, so the set is checked when a connection is
// made: a release that drops one of them fails there, loudly, instead of
// database/sql quietly taking a slower or different path.
type sdkConn interface {
	driver.Conn
	driver.ConnBeginTx
	driver.ConnPrepareContext
	driver.QueryerContext
	driver.ExecerContext
	driver.Pinger
	driver.NamedValueChecker
	driver.SessionResetter
	driver.Validator
}

// Connect opens one SDK connection and wraps it.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	opened, err := c.inner.Connect(ctx)
	if err != nil {
		return nil, WithoutStackFrames(err)
	}
	sdk, ok := opened.(sdkConn)
	if !ok {
		_ = opened.Close()
		return nil, fmt.Errorf("the YDB driver's connection %T no longer offers what Ptah binds through", opened)
	}
	return conn{sdkConn: sdk, driver: c.sdk, prefix: c.prefix, root: c.root, state: &connState{}}, nil
}

// Driver returns the SDK's driver.
func (c *connector) Driver() driver.Driver {
	return c.inner.Driver()
}

// Close closes the SDK connector and then runs onClose, which closes the SDK
// driver that database/sql's Close does not reach on its own.
func (c *connector) Close() error {
	var errs []error
	if closer, ok := c.inner.(io.Closer); ok {
		errs = append(errs, closer.Close())
	}
	if c.onClose != nil {
		errs = append(errs, c.onClose())
	}
	return errors.Join(errs...)
}

// conn is an SDK connection whose arguments are named and widened before the
// SDK binds them, whose queries start with prefix, and whose errors reach
// database/sql without the SDK's stack frames; see [WithoutStackFrames]. The
// transactions, statements and result sets it returns are wrapped for the same
// reason.
type conn struct {
	sdkConn
	driver *ydbsdk.Driver
	prefix string
	// root is the absolute path the connection treats as its database, empty
	// when it is the driver's own database.
	root string
	// state is what the connection knows about itself between calls;
	// database/sql uses a connection from one goroutine at a time.
	state *connState
}

// connState is the state of one connection.
type connState struct {
	// inTransaction is set while a transaction the connection began is open.
	inTransaction bool
}

// ExecContext runs a statement after the connection's prefix and returns its
// error without stack frames. A secret value the statement refers to is
// defined from the environment first; see [ydbsecret.Expand].
//
// A query that runs one of Ptah's coordination node statements (see
// [ydbcoordination.Recognize]) is not sent to YDB, which has no such
// statement: it is run through the coordination service, outside any
// transaction, as YDB runs every scheme statement. The connection's prefix is
// read with it, so a relative path names a node inside a dev realm as it names
// a table there. It is refused inside a transaction, where it could not be
// rolled back, and with arguments, which it takes none of.
// A query that runs one of Ptah's comment statements (see
// [ydbcomment.Recognize]) is not sent to YDB, which has no COMMENT statement:
// it is run through the table service as a change of the object's user
// attributes, outside any transaction, as YDB runs every scheme statement
// (see [RunCommentStatement]). The connection's prefix is read with it, so a
// relative path names an object inside a dev realm as it names a table
// there. It is refused inside a transaction, where it could not be rolled
// back, and with arguments, which it takes none of.
func (c conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	comment, recognized, err := ydbcomment.Recognize(c.prefix + query)
	switch {
	case err != nil:
		return nil, err
	case recognized:
		return c.runComment(ctx, comment, args)
	}
	coordination, recognized, err := ydbcoordination.Recognize(c.prefix + query)
	switch {
	case err != nil:
		return nil, err
	case recognized:
		return c.runCoordination(ctx, coordination, args)
	}
	expanded, err := ydbsecret.Expand(query, os.LookupEnv)
	if err != nil {
		return nil, fmt.Errorf("ydb: %w", err)
	}
	result, err := c.sdkConn.ExecContext(ctx, c.prefix+expanded.Text, args)
	return result, redacted(expanded, WithoutStackFrames(err))
}

// runCoordination runs one of Ptah's coordination node statements through the
// coordination service of the driver the connection belongs to.
func (c conn) runCoordination(ctx context.Context, query ydbcoordination.Query, args []driver.NamedValue) (driver.Result, error) {
	switch {
	case len(args) > 0:
		return nil, fmt.Errorf("%w: a coordination node statement takes no arguments, and %d were given",
			ydbcoordination.ErrStatement, len(args))
	case c.state != nil && c.state.inTransaction:
		return nil, fmt.Errorf("%w: a coordination node statement runs outside a transaction, as YDB runs every "+
			"scheme statement, and this connection has one open", ydbcoordination.ErrStatement)
	case c.driver == nil:
		return nil, errNoCoordinationService
	}
	root := c.root
	if root == "" {
		root = c.driver.Name()
	}
	service := grpcCoordination{client: Ydb_Coordination_V1.NewCoordinationServiceClient(ydbsdk.GRPCConn(c.driver))}
	if err := RunCoordinationStatement(ctx, service, c.driver.Name(), root, query); err != nil {
		return nil, err
	}
	return driver.ResultNoRows, nil
}

// runComment runs one of Ptah's comment statements through the scheme and
// table services of the driver the connection belongs to.
func (c conn) runComment(ctx context.Context, query ydbcomment.Query, args []driver.NamedValue) (driver.Result, error) {
	switch {
	case len(args) > 0:
		return nil, fmt.Errorf("%w: a comment statement takes no arguments, and %d were given",
			ydbcomment.ErrStatement, len(args))
	case c.state != nil && c.state.inTransaction:
		return nil, fmt.Errorf("%w: a comment statement runs outside a transaction, as YDB runs every "+
			"scheme statement, and this connection has one open", ydbcomment.ErrStatement)
	case c.driver == nil:
		return nil, errNoTableService
	}
	root := c.root
	if root == "" {
		root = c.driver.Name()
	}
	connection := ydbsdk.GRPCConn(c.driver)
	service := newGRPCAttributes(Ydb_Scheme_V1.NewSchemeServiceClient(connection), Ydb_Table_V1.NewTableServiceClient(connection))
	if err := RunCommentStatement(ctx, service, c.driver.Name(), root, query); err != nil {
		return nil, err
	}
	return driver.ResultNoRows, nil
}

// QueryContext runs a query after the connection's prefix and wraps the result
// set it returns, expanding secret references as ExecContext does. A
// coordination node or comment statement returns no rows and is refused here.
func (c conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if _, recognized, err := ydbcomment.Recognize(query); err != nil || recognized {
		return nil, errors.Join(err, fmt.Errorf("%w: a comment statement returns no rows; execute it",
			ydbcomment.ErrStatement))
	}
	if _, recognized, err := ydbcoordination.Recognize(query); err != nil || recognized {
		return nil, errors.Join(err, fmt.Errorf("%w: a coordination node statement returns no rows; execute it",
			ydbcoordination.ErrStatement))
	}
	expanded, err := ydbsecret.Expand(query, os.LookupEnv)
	if err != nil {
		return nil, fmt.Errorf("ydb: %w", err)
	}
	opened, err := c.sdkConn.QueryContext(ctx, c.prefix+expanded.Text, args)
	return wrapRows(opened, redacted(expanded, err))
}

// redacted returns err with every secret value expanded defined written out of
// its text, so a refusal the server answers a CREATE SECRET with cannot carry
// the value into a log or an error. errors.Is and errors.As still reach err.
func redacted(expanded ydbsecret.Expansion, err error) error {
	if err == nil || !expanded.Defines() {
		return err
	}
	return redactedError{err: err, text: expanded.Redact(err.Error())}
}

// redactedError is an error whose text had a secret value taken out of it.
type redactedError struct {
	err  error
	text string
}

func (e redactedError) Error() string { return e.text }
func (e redactedError) Unwrap() error { return e.err }

// wrapRows wraps a result set the SDK returned, or returns its error without
// stack frames.
func wrapRows(opened driver.Rows, err error) (driver.Rows, error) {
	if err != nil {
		return nil, WithoutStackFrames(err)
	}
	sdk, ok := opened.(sdkRows)
	if !ok {
		_ = opened.Close()
		return nil, fmt.Errorf("the YDB driver's result set %T no longer offers what Ptah reads through", opened)
	}
	return rows{sdkRows: sdk}, nil
}

// PrepareContext prepares a statement after the connection's prefix and wraps
// it. A statement that refers to a secret value is refused: its value is
// defined only in a query run at once, so a prepared statement would keep it
// in a text the connection holds on to.
func (c conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := refuseSecretPreparation(query); err != nil {
		return nil, err
	}
	return wrapStmt(c.sdkConn.PrepareContext(ctx, c.prefix+query))
}

// Prepare prepares a statement after the connection's prefix, without a
// context, and wraps it, refusing a secret value as PrepareContext does.
func (c conn) Prepare(query string) (driver.Stmt, error) {
	if err := refuseSecretPreparation(query); err != nil {
		return nil, err
	}
	return wrapStmt(c.sdkConn.Prepare(c.prefix + query))
}

// refuseSecretPreparation refuses a statement that refers to a secret value.
func refuseSecretPreparation(query string) error {
	if variables := ydbsecret.References(query); len(variables) > 0 {
		return fmt.Errorf("ydb: a prepared statement cannot take a secret's value; run the statement that "+
			"refers to %s directly", ydbsecret.Reference(variables[0]))
	}
	return nil
}

// wrapStmt wraps a statement the SDK prepared, or returns its error without
// stack frames.
func wrapStmt(prepared driver.Stmt, err error) (driver.Stmt, error) {
	if err != nil {
		return nil, WithoutStackFrames(err)
	}
	sdk, ok := prepared.(sdkStmt)
	if !ok {
		_ = prepared.Close()
		return nil, fmt.Errorf("the YDB driver's statement %T no longer offers what Ptah runs through", prepared)
	}
	return stmt{sdkStmt: sdk}, nil
}

// BeginTx starts a transaction and wraps it.
func (c conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.wrapTx(c.sdkConn.BeginTx(ctx, opts))
}

// Begin starts a transaction without a context or options and wraps it.
func (c conn) Begin() (driver.Tx, error) {
	return c.wrapTx(c.sdkConn.Begin())
}

// wrapTx wraps a transaction the SDK began, and marks the connection as in
// it until it ends, or returns its error without stack frames.
func (c conn) wrapTx(begun driver.Tx, err error) (driver.Tx, error) {
	if err != nil {
		return nil, WithoutStackFrames(err)
	}
	if c.state != nil {
		c.state.inTransaction = true
	}
	return tx{inner: begun, state: c.state}, nil
}

// Ping checks the connection and returns its error without stack frames.
func (c conn) Ping(ctx context.Context) error {
	return WithoutStackFrames(c.sdkConn.Ping(ctx))
}

// tx is an SDK transaction whose commit and rollback errors reach
// database/sql without stack frames. A commit is where YDB reports a
// transaction its optimistic locks lost, so it is the error a retry names.
type tx struct {
	inner driver.Tx
	state *connState
}

func (t tx) Commit() error {
	t.end()
	return WithoutStackFrames(t.inner.Commit())
}

func (t tx) Rollback() error {
	t.end()
	return WithoutStackFrames(t.inner.Rollback())
}

// end marks the connection as out of the transaction.
func (t tx) end() {
	if t.state != nil {
		t.state.inTransaction = false
	}
}

// sdkRows is what database/sql uses of a ydb-go-sdk result set, checked when
// a query returns one, as [sdkConn] is when a connection is made.
type sdkRows interface {
	driver.Rows
	driver.RowsNextResultSet
	driver.RowsColumnTypeDatabaseTypeName
	driver.RowsColumnTypeNullable
}

// rows is an SDK result set whose errors reach database/sql without stack
// frames. The end of a result set is io.EOF, which carries none and so reaches
// database/sql as the very value it compares against.
type rows struct {
	sdkRows
}

func (r rows) Next(dest []driver.Value) error { return WithoutStackFrames(r.sdkRows.Next(dest)) }
func (r rows) NextResultSet() error           { return WithoutStackFrames(r.sdkRows.NextResultSet()) }
func (r rows) Close() error                   { return WithoutStackFrames(r.sdkRows.Close()) }

// sdkStmt is what database/sql uses of a ydb-go-sdk prepared statement.
type sdkStmt interface {
	driver.Stmt
	driver.StmtExecContext
	driver.StmtQueryContext
}

// stmt is an SDK prepared statement whose errors reach database/sql without
// stack frames.
type stmt struct {
	sdkStmt
}

func (s stmt) Close() error { return WithoutStackFrames(s.sdkStmt.Close()) }

func (s stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	result, err := s.sdkStmt.ExecContext(ctx, args)
	return result, WithoutStackFrames(err)
}

func (s stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return wrapRows(s.sdkStmt.QueryContext(ctx, args))
}

// DriverOf returns the SDK driver behind session, a connection from a pool
// [Open] made. The scheme, table and coordination services are reached
// through the driver, not through SQL, and a caller that holds only a pooled
// connection reaches them this way. A connection from any other pool is an
// error.
func DriverOf(session *sql.Conn) (*ydbsdk.Driver, error) {
	var sdk *ydbsdk.Driver
	err := session.Raw(func(raw any) error {
		bound, ok := raw.(conn)
		if !ok || bound.driver == nil {
			return fmt.Errorf("%T is not a connection to a YDB database Ptah opened", raw)
		}
		sdk = bound.driver
		return nil
	})
	if err != nil {
		return nil, err
	}
	return sdk, nil
}

// CheckNamedValue names a positional argument after its position, and widens a
// Go int or uint to 64 bits, before the SDK sees it.
//
// YQL has no positional parameter, and sqlutil.Rebind writes `$p1`, `$p2`, ...
// for Ptah's `?` placeholders, so the first positional argument is bound to
// `$p1`. Without a name the SDK answers `Unknown name: $p1`.
//
// The SDK binds a Go int as Int32 and truncates it: measured on YDB 26.2.1.14
// with ydb-go-sdk v3.153.2, 5000000000 bound to an Int64 key was stored as
// 705032704 and reported success. Widened to Int64, a value bound to an Int32
// or a smaller column is refused instead (`Failed to convert 'i32': Int64 to
// Optional<Int32>`), and one bound to an Int64 column is stored whole.
func (c conn) CheckNamedValue(value *driver.NamedValue) error {
	if value.Name == "" {
		value.Name = "p" + strconv.Itoa(value.Ordinal)
	}
	value.Value = widenInteger(value.Value)
	return c.sdkConn.CheckNamedValue(value)
}

// widenInteger returns a Go int or uint, or a value built on one, as its 64-bit
// counterpart, and any other value unchanged. A pointer keeps its nil, which
// the SDK binds as a typed NULL. A value with a Value method keeps it, since
// the method decides what is bound.
func widenInteger(value any) any {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case uint:
		return uint64(typed)
	case *int:
		if typed == nil {
			return (*int64)(nil)
		}
		return new(int64(*typed))
	case *uint:
		if typed == nil {
			return (*uint64)(nil)
		}
		return new(uint64(*typed))
	case []int:
		widened := make([]int64, len(typed))
		for i, item := range typed {
			widened[i] = int64(item)
		}
		return widened
	case []uint:
		widened := make([]uint64, len(typed))
		for i, item := range typed {
			widened[i] = uint64(item)
		}
		return widened
	case sql.Null[int]:
		return sql.Null[int64]{V: int64(typed.V), Valid: typed.Valid}
	case sql.Null[uint]:
		return sql.Null[uint64]{V: uint64(typed.V), Valid: typed.Valid}
	case driver.Valuer:
		return value
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Int:
		return reflected.Int()
	case reflect.Uint:
		return reflected.Uint()
	default:
		return value
	}
}
