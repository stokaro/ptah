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

	"ptah.run/core/ptaherr"
)

// Unknown reports whether a statement that failed with err may have been
// applied: a native adapter reported uncertainty, or the connection failed
// before the server's answer reached the client. It answers false for a server's
// answer, an error that says the statement was never sent, and an error it does
// not recognize as uncertainty.
//
// What it recognizes:
//
//   - an explicit [ptaherr.ErrOutcomeUnknown] from a native execution
//     adapter, including an uncertain result wrapping a transport error, as
//     an executable provider's session reports one;
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
	if errors.Is(err, ptaherr.ErrOutcomeUnknown) {
		return true
	}
	if errors.Is(err, driver.ErrBadConn) {
		return false
	}
	return brokenConnection(err)
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
