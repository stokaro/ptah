// Package statements serves the operator statements of contract 25.4 on the
// fixture's SQLite: a statement the operator wrote runs on the session's
// database or in its active transaction, and its rows come back in pages. A
// read-only statement must be one SELECT or WITH query, and runs on a
// connection SQLite holds query-only. The address fault uncounted-rows reports
// no row count, as an engine that does not measure one would, and the fault
// read-only-statements refuses every other statement, so a test can tell which
// access a host asked for.
package statements

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// Sessions reaches an open fixture session, as *backend.Provider does.
type Sessions interface {
	WithSession(ctx context.Context, sessionID, transactionID string, use func(backend.Access) error) error
}

// Service is the fixture's statement service.
type Service struct {
	Sessions Sessions
	mu       sync.Mutex
	open     map[string]*result
}

// result is what is left of an open result, bound to the session and the
// transaction it was read in.
type result struct {
	session, transaction string
	width                int
	rows                 []*protocol.Row
}

// Features are what a provider and its engine list to serve the service.
func Features() []string { return []string{provider.StatementsFeature} }

// RunStatement runs one statement.
func (s *Service) RunStatement(ctx context.Context, request *protocol.RunStatementRequest) (*protocol.StatementResult, error) {
	reply := &protocol.StatementResult{RequestId: request.GetRequestId()}
	readOnly := request.GetAccess() == protocol.RunStatementRequest_READ_ONLY
	if readOnly {
		if reason := unprovable(request.GetSql()); reason != "" {
			reply.Outcome, reply.Refusal = protocol.StatementResult_REFUSED, reason
			return reply, nil
		}
	}
	args := make([]any, len(request.GetArguments()))
	for i, argument := range request.GetArguments() {
		args[i] = goValue(argument)
	}
	err := s.Sessions.WithSession(ctx, request.GetSessionId(), request.GetTransactionId(), func(access backend.Access) error {
		switch {
		case !readOnly && access.Fault == "read-only-statements":
			reply.Outcome, reply.Refusal = protocol.StatementResult_REFUSED, "the fixture runs read-only statements alone at this address"
			return nil
		case readOnly:
			return s.readOnly(ctx, access.DB, request, args, reply)
		case query(request.GetSql()):
			return s.query(ctx, access.Querier, request, args, reply)
		default:
			return exec(ctx, access, request, args, reply)
		}
	})
	if err != nil {
		return nil, err
	}
	return reply, nil
}

// ReadStatementRows returns the next page of an open result.
func (s *Service) ReadStatementRows(ctx context.Context, request *protocol.ReadStatementRowsRequest) (*protocol.StatementRows, error) {
	s.mu.Lock()
	open := s.open[request.GetPageToken()]
	delete(s.open, request.GetPageToken())
	s.mu.Unlock()
	if open == nil || open.session != request.GetSessionId() || open.width != int(request.GetColumnCount()) {
		return nil, fmt.Errorf("no open result for the page token")
	}
	// The result lives as long as what it was read in: a finished
	// transaction or a closed session has released it.
	if err := s.Sessions.WithSession(ctx, open.session, open.transaction, func(backend.Access) error { return nil }); err != nil {
		return nil, fmt.Errorf("the result was released with its transaction or session")
	}
	page := &protocol.StatementRows{}
	page.Rows, page.NextPageToken = s.keep(open, int(request.GetMaxRows()))
	return page, nil
}

// CloseStatement releases an open result. A released one succeeds.
func (s *Service) CloseStatement(_ context.Context, request *protocol.CloseStatementRequest) (*protocol.Empty, error) {
	s.mu.Lock()
	if open := s.open[request.GetPageToken()]; open != nil && open.session == request.GetSessionId() {
		delete(s.open, request.GetPageToken())
	}
	s.mu.Unlock()
	return &protocol.Empty{}, nil
}

// unprovable names why a read-only statement cannot be proved to change
// nothing: anything but one SELECT or WITH query.
func unprovable(text string) string {
	trimmed := strings.TrimSuffix(strings.TrimSpace(text), ";")
	words := strings.Fields(strings.ToUpper(trimmed))
	if len(words) == 0 || words[0] != "SELECT" && words[0] != "WITH" || strings.Contains(trimmed, ";") {
		return "the fixture proves read-only only one SELECT or WITH query"
	}
	return ""
}

// query reports whether a read-write statement returns rows.
func query(text string) bool {
	words := strings.Fields(strings.ToUpper(text))
	return len(words) > 0 && (words[0] == "SELECT" || words[0] == "WITH" || words[0] == "VALUES" || words[0] == "PRAGMA")
}

// readOnly runs a query on a connection SQLite holds query-only.
func (s *Service) readOnly(ctx context.Context, db *sql.DB, request *protocol.RunStatementRequest, args []any, reply *protocol.StatementResult) (resultErr error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_, offErr := conn.ExecContext(context.WithoutCancel(ctx), "PRAGMA query_only = OFF")
		if closeErr := conn.Close(); resultErr == nil {
			resultErr = cmpError(offErr, closeErr)
		}
	}()
	if _, err := conn.ExecContext(ctx, "PRAGMA query_only = ON"); err != nil {
		return err
	}
	return s.query(ctx, conn, request, args, reply)
}

func cmpError(first, second error) error {
	if first != nil {
		return first
	}
	return second
}

type querier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// query runs a statement that returns rows, keeping what the first page does
// not hold. A statement SQLite refuses is FAILED with its message.
func (s *Service) query(ctx context.Context, db querier, request *protocol.RunStatementRequest, args []any, reply *protocol.StatementResult) error {
	rows, err := db.QueryContext(ctx, request.GetSql(), args...)
	if err != nil {
		failed(reply, err)
		return nil
	}
	defer rows.Close()
	names, err := rows.Columns()
	if err != nil {
		return err
	}
	var all []*protocol.Row
	for rows.Next() {
		values := make([]any, len(names))
		pointers := make([]any, len(names))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return err
		}
		row := &protocol.Row{}
		for _, value := range values {
			row.Values = append(row.Values, dataValue(value))
		}
		all = append(all, row)
	}
	if err := rows.Err(); err != nil {
		failed(reply, err)
		return nil
	}
	reply.Outcome = protocol.StatementResult_SUCCEEDED
	for _, name := range names {
		reply.Columns = append(reply.Columns, &protocol.DataColumn{Name: name})
	}
	open := &result{session: request.GetSessionId(), transaction: request.GetTransactionId(), width: len(names), rows: all}
	reply.Rows, reply.NextPageToken = s.keep(open, int(request.GetMaxRows()))
	return nil
}

// keep returns the next page of open and its token, keeping the rest under
// that token.
func (s *Service) keep(open *result, limit int) ([]*protocol.Row, string) {
	if len(open.rows) <= limit {
		return open.rows, ""
	}
	page := open.rows[:limit]
	open.rows = open.rows[limit:]
	token := rand.Text()
	s.mu.Lock()
	if s.open == nil {
		s.open = make(map[string]*result)
	}
	s.open[token] = open
	s.mu.Unlock()
	return page, token
}

// exec runs a statement for its effect and reports the rows it changed.
func exec(ctx context.Context, access backend.Access, request *protocol.RunStatementRequest, args []any, reply *protocol.StatementResult) error {
	done, err := access.Querier.ExecContext(ctx, request.GetSql(), args...)
	if err != nil {
		failed(reply, err)
		return nil
	}
	reply.Outcome = protocol.StatementResult_SUCCEEDED
	if access.Fault == "uncounted-rows" {
		return nil
	}
	affected, err := done.RowsAffected()
	if err != nil || affected < 0 {
		return nil
	}
	reply.RowsAffected = new(uint64(affected))
	return nil
}

// failed records SQLite's refusal of a statement in the server's words,
// within the contract's bound.
func failed(reply *protocol.StatementResult, err error) {
	message := strings.ReplaceAll(err.Error(), "\x00", "")
	if len(message) > provider.MaxStatementErrorBytes {
		message = message[:provider.MaxStatementErrorBytes]
	}
	message = strings.ToValidUTF8(message, "")
	if message == "" {
		message = "the statement failed"
	}
	reply.Outcome, reply.Error = protocol.StatementResult_FAILED, message
}

// goValue is the argument SQLite binds for a wire value.
func goValue(value *protocol.DataValue) any {
	switch v := value.GetValue().(type) {
	case *protocol.DataValue_BoolValue:
		return v.BoolValue
	case *protocol.DataValue_Int64Value:
		return v.Int64Value
	case *protocol.DataValue_Uint64Value:
		return v.Uint64Value
	case *protocol.DataValue_DoubleValue:
		return v.DoubleValue
	case *protocol.DataValue_StringValue:
		return v.StringValue
	case *protocol.DataValue_BytesValue:
		return v.BytesValue
	case *protocol.DataValue_TimestampValue:
		return time.Unix(v.TimestampValue.GetSeconds(), int64(v.TimestampValue.GetNanos())).UTC()
	case *protocol.DataValue_JsonValue:
		return v.JsonValue
	default:
		return nil
	}
}

// dataValue is the wire value of what SQLite scanned.
func dataValue(value any) *protocol.DataValue {
	switch v := value.(type) {
	case nil:
		return &protocol.DataValue{Value: &protocol.DataValue_Null{Null: &protocol.Empty{}}}
	case int64:
		return &protocol.DataValue{Value: &protocol.DataValue_Int64Value{Int64Value: v}}
	case float64:
		return &protocol.DataValue{Value: &protocol.DataValue_DoubleValue{DoubleValue: v}}
	case bool:
		return &protocol.DataValue{Value: &protocol.DataValue_BoolValue{BoolValue: v}}
	case []byte:
		return &protocol.DataValue{Value: &protocol.DataValue_BytesValue{BytesValue: v}}
	case string:
		return &protocol.DataValue{Value: &protocol.DataValue_StringValue{StringValue: v}}
	case time.Time:
		utc := v.UTC()
		return &protocol.DataValue{Value: &protocol.DataValue_TimestampValue{TimestampValue: &protocol.TimestampValue{
			Seconds: utc.Unix(), Nanos: int32(utc.Nanosecond())}}} // #nosec G115 -- Nanosecond is in [0, 1e9)
	default:
		return &protocol.DataValue{Value: &protocol.DataValue_StringValue{StringValue: fmt.Sprint(v)}}
	}
}
