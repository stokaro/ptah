package execoutcome_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"ptah.run/internal/execoutcome"
)

// Each row is a failure after which the statement may have been applied: the
// connection broke while the server had it.
func TestUnknown_ConnectionBroke(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "the stream ended", err: fmt.Errorf("failed to receive message: %w", io.ErrUnexpectedEOF)},
		{name: "the server closed the connection", err: fmt.Errorf("read: %w", io.EOF)},
		{name: "a reset", err: &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}},
		{name: "a broken pipe", err: fmt.Errorf("write: %w", syscall.EPIPE)},
		{name: "a read timeout", err: &net.OpError{Op: "read", Net: "tcp", Err: timeoutError{}}},
		{name: "go-sql-driver/mysql's broken connection", err: fmt.Errorf("query: %w", mysql.ErrInvalidConn)},
		{name: "a YDB transport error", err: status.Error(codes.Unavailable, "transport is closing")},
		{
			// ydb-go-sdk's database/sql driver reports a transport error as a
			// bad connection, which must not read as a statement never sent.
			name: "a YDB transport error the driver marked a bad connection",
			err:  errors.Join(driver.ErrBadConn, status.Error(codes.Unavailable, "error reading from server: EOF")),
		},
		{name: "a YDB stream reset", err: status.Error(codes.Internal, "stream terminated by RST_STREAM")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(execoutcome.Unknown(test.err), qt.IsTrue)
		})
	}
}

// Each row is a failure whose outcome is known: the server answered, or the
// statement was never sent.
func TestUnknown_OutcomeKnown(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "no failure"},
		{name: "a PostgreSQL refusal", err: &pgconn.PgError{Code: "42P07", Message: `relation "t" already exists`}},
		{
			name: "a PostgreSQL session ended by the server mid-statement",
			err:  &pgconn.PgError{Severity: "FATAL", Code: "57P01", Message: "terminating connection due to administrator command"},
		},
		{name: "a MySQL refusal", err: &mysql.MySQLError{Number: 1050, Message: "Table 't' already exists"}},
		{name: "a statement never sent", err: fmt.Errorf("exec: %w", driver.ErrBadConn)},
		{name: "a failed dial", err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}},
		{name: "an error nothing recognizes", err: errors.New("syntax error at or near \"CREAT\"")},
		{name: "a canceled context, which the caller judges", err: context.Canceled},
		{name: "a YDB message too large for the server", err: status.Error(codes.ResourceExhausted, "message larger than max")},
		{name: "a YDB refusal by the gRPC layer", err: status.Error(codes.PermissionDenied, "access denied")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(execoutcome.Unknown(test.err), qt.IsFalse)
		})
	}
}

// timeoutError is a network error that says it timed out.
type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }
