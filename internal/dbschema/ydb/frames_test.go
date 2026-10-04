package ydb_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	ydbschema "ptah.run/internal/dbschema/ydb"
)

// errServerAnswer is the text ydb-go-sdk v3.153.2 gave for a DROP INDEX of an
// index that does not exist, on YDB 26.2.1.14: the status, its code, the
// server's address, and the issues with their codes and positions.
var errServerAnswer = errors.New(`operation/GENERIC_ERROR (code = 400080, address = 100.101.64.121:25136, ` +
	`issues = [{#1030 'Type annotation' [{2:31 => 'At function: KiAlterTable!' [{2:31 => ` +
	`'AlterTable : db.[/local/ptah_capprobe_28fef122709e7a16/sc_cix] Index: "sc_cix_n" does not exist'}]}]}])`)

// sdkFrames are the frames the SDK wrote after errServerAnswer for that statement,
// innermost first.
var sdkFrames = []string{
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*grpcClientStream).RecvMsg(grpc_client_stream.go:180)",
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query.nextPart(result.go:305)",
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*streamResult).nextPart(result.go:280)",
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query.newResult(result.go:217)",
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query.execute(execute_query.go:158)",
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).execute(session.go:184)",
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).Exec(session.go:255)",
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/xquery.(*Conn).Exec(conn.go:70)",
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql.(*Conn).ExecContext(conn.go:244)",
}

// withFrames wraps err once per frame, the way the SDK's stack-trace wrapper
// does: the wrapped error's text, then " at `frame`".
func withFrames(err error, frames ...string) error {
	for _, frame := range frames {
		err = fmt.Errorf("%w at `%s`", err, frame)
	}
	return err
}

// The frames go and everything the server said stays, and the error still
// answers as the one it wraps.
func TestWithoutStackFrames_StripsTheSDKsFrames(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "an SDK error with its frames",
			err:  withFrames(errServerAnswer, sdkFrames...),
			want: errServerAnswer.Error(),
		},
		{
			// Ptah's own context around an SDK error stays where it was.
			name: "an SDK error inside Ptah's context",
			err:  fmt.Errorf("open YDB driver: %w", withFrames(errServerAnswer, sdkFrames[:2]...)),
			want: "open YDB driver: " + errServerAnswer.Error(),
		},
		{
			// Backticked text in a message is the server's, not a frame.
			name: "a message quoting a name in backticks",
			err:  withFrames(errors.New("Unknown name: `t` at `other/place`"), sdkFrames[0]),
			want: "Unknown name: `t` at `other/place`",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := ydbschema.WithoutStackFrames(test.err)
			c.Assert(got, qt.ErrorMatches, regexp.QuoteMeta(test.want))
			c.Assert(got, qt.ErrorIs, test.err)
		})
	}
}

// An error with no frame comes back as the same value, which is what keeps
// the sentinels database/sql compares by identity working, and nil stays nil.
func TestWithoutStackFrames_LeavesAnErrorWithoutFramesAlone(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbschema.WithoutStackFrames(io.EOF), qt.Equals, io.EOF)
	c.Assert(ydbschema.WithoutStackFrames(errServerAnswer), qt.Equals, errServerAnswer)
	c.Assert(ydbschema.WithoutStackFrames(nil), qt.IsNil)
}

// framedConn stands in for a ydb-go-sdk connection whose every call fails
// with an SDK error, except a query, whose result set fails on its second row.
type framedConn struct{ recordingConn }

func (framedConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, withFrames(errServerAnswer, sdkFrames...)
}
func (framedConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &framedRows{}, nil
}
func (framedConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return framedTx{}, nil
}
func (framedConn) Ping(context.Context) error { return withFrames(errServerAnswer, sdkFrames...) }

// framedRows yields one row and then an SDK error, as a result set does that
// the server fails while it is being read.
type framedRows struct{ read int }

func (*framedRows) Columns() []string { return []string{"n"} }
func (*framedRows) Close() error      { return nil }
func (r *framedRows) Next(dest []driver.Value) error {
	r.read++
	if r.read > 1 {
		return withFrames(errServerAnswer, sdkFrames...)
	}
	dest[0] = int64(1)
	return nil
}
func (*framedRows) HasNextResultSet() bool                     { return false }
func (*framedRows) NextResultSet() error                       { return io.EOF }
func (*framedRows) ColumnTypeDatabaseTypeName(int) string      { return "Int64" }
func (*framedRows) ColumnTypeNullable(int) (nullable, ok bool) { return false, true }

// framedTx is a transaction whose commit fails with an SDK error, the way a
// commit that lost its optimistic locks does.
type framedTx struct{}

func (framedTx) Commit() error   { return withFrames(errServerAnswer, sdkFrames...) }
func (framedTx) Rollback() error { return nil }

// framedConnector hands out framedConn.
type framedConnector struct{}

func (framedConnector) Connect(context.Context) (driver.Conn, error) {
	return framedConn{recordingConn{got: new([]driver.NamedValue)}}, nil
}
func (framedConnector) Driver() driver.Driver { return nil }

// What database/sql reports from a YDB connection carries the server's answer
// without the SDK's frames, wherever the SDK raised it, and still wraps the
// error the SDK returned.
func TestBindingConnector_ReportsErrorsWithoutStackFrames(t *testing.T) {
	c := qt.New(t)
	db := sql.OpenDB(ydbschema.NewBindingConnector(framedConnector{}, nil))
	c.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()

	t.Run("a statement", func(t *testing.T) {
		c := qt.New(t)
		_, err := db.ExecContext(ctx, "ALTER TABLE sc_cix DROP INDEX sc_cix_n")
		c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(errServerAnswer.Error()))
		c.Assert(err, qt.ErrorIs, errServerAnswer)
	})

	t.Run("a ping", func(t *testing.T) {
		c := qt.New(t)
		err := db.PingContext(ctx)
		c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(errServerAnswer.Error()))
		c.Assert(err, qt.ErrorIs, errServerAnswer)
	})

	t.Run("a result set read", func(t *testing.T) {
		c := qt.New(t)
		rows, err := db.QueryContext(ctx, "SELECT n FROM t")
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() { _ = rows.Close() })
		c.Assert(rows.Next(), qt.IsTrue)
		c.Assert(rows.Next(), qt.IsFalse)
		c.Assert(rows.Err(), qt.ErrorMatches, regexp.QuoteMeta(errServerAnswer.Error()))
		c.Assert(rows.Err(), qt.ErrorIs, errServerAnswer)
	})

	t.Run("a commit", func(t *testing.T) {
		c := qt.New(t)
		tx, err := db.BeginTx(ctx, nil)
		c.Assert(err, qt.IsNil)
		err = tx.Commit()
		c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(errServerAnswer.Error()))
		c.Assert(err, qt.ErrorIs, errServerAnswer)
	})
}
