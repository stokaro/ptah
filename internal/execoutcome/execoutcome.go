// Package execoutcome says whether a statement that failed may have been
// applied all the same.
//
// A statement that runs outside a transaction has an outcome the client knows
// only from the server's answer. A refusal is an answer: the statement was not
// applied. A connection that breaks after the statement was sent is not: the
// server may have applied it and lost the way to say so, as a migration's
// CREATE TABLE does when the network fails while the server runs it. The
// migrator keeps such a statement marked as one whose outcome is unknown, so a
// resume does not run it a second time on a guess.
package execoutcome

import (
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"syscall"

	"github.com/go-sql-driver/mysql"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

// Unknown reports whether a statement that failed with err may have been
// applied: the failure is the connection's, so the server's answer, if it sent
// one, never reached the client. It answers false for a server's answer, for
// an error that says the statement was never sent, and for any error it does
// not recognize as a broken connection.
//
// What it recognizes:
//
//   - a YDB error, as ydb-go-sdk classifies it: a transport error the SDK
//     would retry only for an idempotent operation (Unavailable, Internal,
//     Canceled, DeadlineExceeded), an operation status of the same kind
//     (UNDETERMINED, TIMEOUT, SESSION_EXPIRED), and a stream the connection
//     ended under may have been applied. YDB is asked first, because its
//     database/sql driver reports a transport error as a bad connection;
//   - driver.ErrBadConn otherwise means the statement was not sent: drivers
//     return it only then, and database/sql retries on it;
//   - a connection that ended mid-statement (io.EOF, io.ErrUnexpectedEOF,
//     a reset or broken pipe), a network error other than a failed dial, and
//     go-sql-driver/mysql's ErrInvalidConn may have been applied.
//
// Context cancellation is the caller's to judge: it is not a failure of the
// connection, and it is not reported here.
func Unknown(err error) bool {
	if err == nil {
		return false
	}
	if unknown, ok := ydbUnknown(err); ok {
		return unknown
	}
	if errors.Is(err, driver.ErrBadConn) {
		return false
	}
	return brokenConnection(err)
}

// ydbUnknown answers for an error ydb-go-sdk returned, and reports false for
// ok when err is not one.
func ydbUnknown(err error) (unknown, ok bool) {
	checked := err
	switch {
	case ydbsdk.IsTransportError(err):
		// The SDK's own transport error, also for a gRPC status it did not
		// wrap, which its classification does not read otherwise.
		checked = ydbsdk.TransportError(err)
	case !ydbsdk.IsOperationError(err):
		return false, false
	}
	mode := retry.Check(checked)
	// A failure the SDK would retry for any operation left nothing applied;
	// one it retries only for an idempotent operation may have, and so may a
	// stream the connection ended under, whatever code the SDK gave it.
	return (mode.MustRetry(true) && !mode.MustRetry(false)) || brokenConnection(err), true
}

// brokenConnection reports whether err says the connection ended after the
// request could have reached the server.
func brokenConnection(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, mysql.ErrInvalidConn) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNABORTED) {
		return true
	}
	if opErr, ok := errors.AsType[*net.OpError](err); ok {
		// A failed dial sent nothing.
		return opErr.Op != "dial"
	}
	_, isNetErr := errors.AsType[net.Error](err)
	return isNetErr
}
