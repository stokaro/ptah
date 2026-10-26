package providerhost

import (
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"time"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/dbschema"
)

// A provider runs the statements the operator wrote, such as a check, a test
// assertion or a script step, through sessions.statements (ADR 0038). The host
// reaches them through database/sql, so the callers keep reading *sql.Rows,
// *sql.Tx and sql.Result. Only those callers take this handle: the
// connection's general QueryContext and ExecContext keep refusing a provider's
// engine, so SQL the host composes for its own engines cannot reach one.

// The session serves the statement handle the dbschema connection asks for.
var _ dbschema.OperatorStatementProvider = (*session)(nil)

// statementPageRows bounds the rows of each page the host asks for.
const statementPageRows = 1000

// StatementFailure is a statement the engine refused, with the server's
// message for it and its code. The message is the server's words, which a
// test assertion may match.
type StatementFailure struct {
	Message string
	Code    string
}

// Error returns the server's message.
func (e *StatementFailure) Error() string { return e.Message }

// OperatorStatements returns a database/sql handle over the session's
// statement service. The handle has one connection, bound to this session,
// and closing it leaves the session open. A read-only handle runs no
// transaction.
func (s *session) OperatorStatements(readOnly bool) (*sql.DB, error) {
	if s.closed.Load() {
		return nil, fmt.Errorf("provider session is closed")
	}
	if !s.target.offers(provider.StatementsFeature) {
		return nil, unsupported("statements the operator wrote")
	}
	db := sql.OpenDB(statementConnector{session: s, readOnly: readOnly})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}

type statementConnector struct {
	session  *session
	readOnly bool
}

func (c statementConnector) Connect(context.Context) (driver.Conn, error) {
	return &statementConn{session: c.session, readOnly: c.readOnly}, nil
}

func (statementConnector) Driver() driver.Driver { return statementDriver{} }

// statementDriver opens nothing by name: a handle exists only through the
// session it runs on.
type statementDriver struct{}

func (statementDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("a provider statement handle opens only through its session")
}

// statementConn runs each statement on the session, in the transaction it
// began when there is one.
type statementConn struct {
	session  *session
	readOnly bool
	tx       *transaction
}

func (c *statementConn) Prepare(query string) (driver.Stmt, error) {
	return &statementStmt{conn: c, query: query}, nil
}

func (c *statementConn) Close() error { return nil }

func (c *statementConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx opens a provider transaction. The provider establishes the requested
// isolation or refuses it; a read-only transaction has no counterpart, and a
// read-only handle opens none.
func (c *statementConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.readOnly || opts.ReadOnly {
		return nil, unsupported("a read-only transaction for statements the operator wrote")
	}
	if c.tx != nil {
		return nil, fmt.Errorf("a provider statement transaction is already open")
	}
	isolation := protocol.TransactionIsolation_DEFAULT
	switch sql.IsolationLevel(opts.Isolation) {
	case sql.LevelDefault:
	case sql.LevelSerializable:
		isolation = protocol.TransactionIsolation_SERIALIZABLE
	default:
		return nil, unsupported(fmt.Sprintf("the %s isolation level", sql.IsolationLevel(opts.Isolation)))
	}
	tx, err := c.session.beginProviderTransaction(ctx, isolation)
	if err != nil {
		return nil, err
	}
	c.tx = tx
	return statementTx{conn: c}, nil
}

type statementTx struct{ conn *statementConn }

type statementForm uint8

const (
	statementRowsForm statementForm = iota
	statementEffectForm
)

func (t statementTx) Commit() error {
	tx := t.conn.tx
	t.conn.tx = nil
	return tx.Commit()
}

func (t statementTx) Rollback() error {
	tx := t.conn.tx
	t.conn.tx = nil
	return tx.Rollback()
}

func (c *statementConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	result, request, err := c.run(ctx, query, args, statementRowsForm)
	if err != nil {
		return nil, err
	}
	return &statementRows{ctx: ctx, conn: c, result: result, maxRows: request.GetMaxRows(), next: result.GetNextPageToken()}, nil
}

// ExecContext runs a statement for its effect. A statement that returned rows
// is released before the reply is handed back.
func (c *statementConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, _, err := c.run(ctx, query, args, statementEffectForm)
	if err != nil {
		return nil, err
	}
	if token := result.GetNextPageToken(); token != "" {
		if err := c.closeResult(ctx, token); err != nil {
			return nil, err
		}
	}
	return statementResult{rowsAffected: result.RowsAffected}, nil
}

// run sends one statement and turns any outcome but SUCCEEDED into an error:
// the server's message for FAILED, the provider's refusal for REFUSED, and an
// unknown outcome otherwise, which a transaction then cannot commit over.
func (c *statementConn) run(ctx context.Context, query string, args []driver.NamedValue, form statementForm) (*protocol.StatementResult, *protocol.RunStatementRequest, error) {
	if err := c.session.lockError(); err != nil {
		return nil, nil, err
	}
	inputs, err := c.session.target.executionInputs(ctx, []*protocol.Unit{{ChangeId: "statement", Body: &protocol.Unit_Sql{Sql: query}}})
	if err != nil {
		return nil, nil, err
	}
	if len(inputs) != 0 {
		if form != statementEffectForm || c.readOnly || len(args) != 0 {
			return nil, nil, unsupported("sensitive inputs for result-bearing, read-only, or parameterized statements")
		}
		return c.executeInputStatement(ctx, query)
	}
	values, err := statementArguments(args)
	if err != nil {
		return nil, nil, err
	}
	request := &protocol.RunStatementRequest{SessionId: c.session.remote.GetId(), RequestId: rand.Text(), Sql: query, Arguments: values,
		Access: protocol.RunStatementRequest_READ_WRITE, MaxRows: statementPageRows}
	if c.readOnly {
		request.Access = protocol.RunStatementRequest_READ_ONLY
	}
	if c.tx != nil {
		request.TransactionId = c.tx.id
	}
	result, err := c.session.target.host.client.RunStatement(ctx, request)
	if err == nil {
		err = statementOutcome(result)
	}
	if err != nil && c.tx != nil && errors.Is(err, provider.ErrOutcomeUnknown) {
		c.tx.mu.Lock()
		c.tx.executionErr = err
		c.tx.mu.Unlock()
	}
	if err != nil {
		return nil, nil, err
	}
	return result, request, nil
}

// A write-only statement uses the exact approvals retained by the caller's
// preparation phase. Execute checks those references before binding values;
// discovering inputs here does not authorize any environment variable.
func (c *statementConn) executeInputStatement(ctx context.Context, query string) (*protocol.StatementResult, *protocol.RunStatementRequest, error) {
	var err error
	if c.tx != nil {
		err = c.tx.ExecuteSQL(ctx, query)
	} else {
		err = c.session.ExecuteSQL(ctx, query)
	}
	if err != nil {
		return nil, nil, err
	}
	return &protocol.StatementResult{Outcome: protocol.StatementResult_SUCCEEDED}, &protocol.RunStatementRequest{}, nil
}

func statementOutcome(result *protocol.StatementResult) error {
	switch result.GetOutcome() {
	case protocol.StatementResult_SUCCEEDED:
		return nil
	case protocol.StatementResult_FAILED:
		return &StatementFailure{Message: result.GetError(), Code: result.GetErrorCode()}
	case protocol.StatementResult_REFUSED:
		return fmt.Errorf("the provider refused the statement: %s: %w", result.GetRefusal(), provider.ErrExecution)
	default:
		return fmt.Errorf("the provider cannot say whether the statement ran: %w", &provider.StatementError{Outcome: result.GetOutcome()})
	}
}

func (c *statementConn) closeResult(ctx context.Context, token string) error {
	_, err := c.session.target.host.client.CloseStatement(context.WithoutCancel(ctx), &protocol.CloseStatementRequest{
		SessionId: c.session.remote.GetId(), PageToken: token})
	return err
}

// statementArguments binds positional arguments. A named one has no place on
// the wire: the provider binds arguments in the order the statement's
// placeholders take them.
func statementArguments(args []driver.NamedValue) ([]*protocol.DataValue, error) {
	values := make([]*protocol.DataValue, 0, len(args))
	for _, arg := range args {
		if arg.Name != "" {
			return nil, unsupported(fmt.Sprintf("the named argument %q", arg.Name))
		}
		value, err := dataValueOf(arg.Value)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

// dataValueOf is the wire form of a driver value, after database/sql has
// converted it to one of the driver.Value types.
func dataValueOf(value driver.Value) (*protocol.DataValue, error) {
	switch v := value.(type) {
	case nil:
		return &protocol.DataValue{Value: &protocol.DataValue_Null{Null: &protocol.Empty{}}}, nil
	case int64:
		return &protocol.DataValue{Value: &protocol.DataValue_Int64Value{Int64Value: v}}, nil
	case float64:
		return &protocol.DataValue{Value: &protocol.DataValue_DoubleValue{DoubleValue: v}}, nil
	case bool:
		return &protocol.DataValue{Value: &protocol.DataValue_BoolValue{BoolValue: v}}, nil
	case string:
		return &protocol.DataValue{Value: &protocol.DataValue_StringValue{StringValue: v}}, nil
	case []byte:
		return &protocol.DataValue{Value: &protocol.DataValue_BytesValue{BytesValue: append([]byte(nil), v...)}}, nil
	case time.Time:
		utc := v.UTC()
		return &protocol.DataValue{Value: &protocol.DataValue_TimestampValue{TimestampValue: &protocol.TimestampValue{
			Seconds: utc.Unix(), Nanos: int32(utc.Nanosecond())}}}, nil // #nosec G115 -- Nanosecond is in [0, 1e9)
	default:
		return nil, unsupported(fmt.Sprintf("a %T statement argument", value))
	}
}

// statementResult answers the row count only when the engine measured it, so
// a caller that asserts a count cannot read an unmeasured one as zero.
type statementResult struct{ rowsAffected *uint64 }

func (statementResult) LastInsertId() (int64, error) {
	return 0, unsupported("the ID a statement inserted")
}

func (r statementResult) RowsAffected() (int64, error) {
	if r.rowsAffected == nil {
		return 0, unsupported("a row count the engine did not measure")
	}
	if *r.rowsAffected > math.MaxInt64 {
		return 0, fmt.Errorf("the row count %d exceeds what database/sql reports", *r.rowsAffected)
	}
	return int64(*r.rowsAffected), nil
}

// statementRows reads a result page by page. Closing it before the last page
// releases the result on the provider's side.
type statementRows struct {
	ctx     context.Context
	conn    *statementConn
	result  *protocol.StatementResult
	rows    []*protocol.Row
	index   int
	maxRows uint32
	next    string
	started bool
	once    sync.Once
	err     error
}

func (r *statementRows) Columns() []string {
	names := make([]string, len(r.result.GetColumns()))
	for i, column := range r.result.GetColumns() {
		names[i] = column.GetName()
	}
	return names
}

// ColumnTypeDatabaseTypeName is the engine type the provider reported.
func (r *statementRows) ColumnTypeDatabaseTypeName(index int) string {
	return strings.ToUpper(r.result.GetColumns()[index].GetType())
}

func (r *statementRows) Close() error {
	r.once.Do(func() {
		if r.next != "" {
			r.err = r.conn.closeResult(r.ctx, r.next)
			r.next = ""
		}
	})
	return r.err
}

func (r *statementRows) Next(dest []driver.Value) error {
	if !r.started {
		r.started, r.rows = true, r.result.GetRows()
	}
	for r.index >= len(r.rows) {
		if r.next == "" {
			return io.EOF
		}
		page, err := r.conn.session.target.host.client.ReadStatementRows(r.ctx, &protocol.ReadStatementRowsRequest{
			SessionId: r.conn.session.remote.GetId(), PageToken: r.next, MaxRows: r.maxRows, ColumnCount: uint32(len(r.result.GetColumns()))}) // #nosec G115 -- a result's columns fit one reply
		if err != nil {
			return err
		}
		r.rows, r.index, r.next = page.GetRows(), 0, page.GetNextPageToken()
	}
	for i, value := range r.rows[r.index].GetValues() {
		converted, err := driverValueOf(value)
		if err != nil {
			return err
		}
		dest[i] = converted
	}
	r.index++
	return nil
}

// driverValueOf is the database/sql value of a wire value: the Go value the
// built-in drivers scan for the same kind. A decimal is its digits with the
// point placed, an unsigned value above int64 its decimal text, a date its
// midnight in UTC, an interval its text, and a native value its payload.
func driverValueOf(value *protocol.DataValue) (driver.Value, error) {
	switch v := value.GetValue().(type) {
	case *protocol.DataValue_Null:
		return nil, nil
	case *protocol.DataValue_BoolValue:
		return v.BoolValue, nil
	case *protocol.DataValue_Int64Value:
		return v.Int64Value, nil
	case *protocol.DataValue_Uint64Value:
		if v.Uint64Value > math.MaxInt64 {
			return fmt.Sprint(v.Uint64Value), nil
		}
		return int64(v.Uint64Value), nil
	case *protocol.DataValue_DoubleValue:
		return v.DoubleValue, nil
	case *protocol.DataValue_StringValue:
		return v.StringValue, nil
	case *protocol.DataValue_BytesValue:
		return v.BytesValue, nil
	case *protocol.DataValue_DecimalValue:
		return decimalText(v.DecimalValue), nil
	case *protocol.DataValue_TimestampValue:
		return time.Unix(v.TimestampValue.GetSeconds(), int64(v.TimestampValue.GetNanos())).UTC(), nil
	case *protocol.DataValue_DateValue:
		return time.Unix(v.DateValue*86400, 0).UTC(), nil
	case *protocol.DataValue_IntervalValue:
		interval := v.IntervalValue
		return fmt.Sprintf("%d months %d days %d nanoseconds", interval.GetMonths(), interval.GetDays(), interval.GetNanoseconds()), nil
	case *protocol.DataValue_JsonValue:
		return v.JsonValue, nil
	case *protocol.DataValue_NativeValue:
		return v.NativeValue.GetPayload(), nil
	default:
		return nil, fmt.Errorf("a statement result holds an unset value")
	}
}

type statementStmt struct {
	conn  *statementConn
	query string
}

func (s *statementStmt) Close() error  { return nil }
func (s *statementStmt) NumInput() int { return -1 }

func (s *statementStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.conn.ExecContext(context.Background(), s.query, namedValues(args))
}

func (s *statementStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.conn.QueryContext(context.Background(), s.query, namedValues(args))
}

func (s *statementStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.query, args)
}

func (s *statementStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.QueryContext(ctx, s.query, args)
}

func namedValues(args []driver.Value) []driver.NamedValue {
	named := make([]driver.NamedValue, len(args))
	for i, value := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: value}
	}
	return named
}
