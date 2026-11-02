// Package errorkind names why a command could not open its connection to the
// target database, from a closed vocabulary a program can branch on.
//
// The machine-readable documents `schema drift`, `schema plan`, `schema
// apply`, `migrations status` and `migrations up` print carry an error object
// whose kind comes from here. A caller that must not repeat what a database
// said -- the Kubernetes operator puts nothing a child process wrote into a
// resource's status -- maps the kind to a sentence of its own and never reads
// the message.
//
// # What is classified
//
// Only a failure to open the target connection. A command marks that one
// failure with [TargetConnection] where it connects, and [Classify] answers
// nothing for an error without the mark. The mark is what keeps the answer
// honest: a registry, a Git remote, a webhook and a dev database all dial
// too, and a refused dial to any of them is not the target being unreachable.
// A failure the command did not mark has no kind, which is a missing answer
// rather than a wrong one.
//
// The kind is read from typed errors and never from message text: the driver's
// error type and code for an answer from the server, the crypto/tls and
// crypto/x509 error types for a handshake, and the net package's error types
// for a server that never answered. An error that carries none of them has no
// kind.
package errorkind

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"slices"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
)

// Kind is one member of the closed vocabulary. The empty Kind means the
// failure has none.
//
// The set is closed on purpose: a caller writing a branch per kind needs the
// list to be finite. A kind is never renamed or given another meaning. A new
// kind may be added, and a caller treats a kind it does not know as no kind.
type Kind string

// The vocabulary. Each kind names a condition that sends the person reading it
// to a different place to fix it.
const (
	// DatabaseUnreachable means the server never answered: the host name did
	// not resolve, the address refused or could not route the connection, or
	// the connection timed out before the server completed it.
	DatabaseUnreachable Kind = "database_unreachable"
	// AuthenticationFailed means the server answered and did not accept the
	// connection's credentials: a wrong password, an unknown or locked role,
	// an expired password, or no host rule that admits the connection.
	AuthenticationFailed Kind = "authentication_failed"
	// DatabaseNotFound means the server answered that the database the
	// connection names does not exist.
	DatabaseNotFound Kind = "database_not_found"
	// PermissionDenied means the server accepted the credentials and refused
	// the role access to the database it names.
	PermissionDenied Kind = "permission_denied"
	// TLSFailed means the TLS handshake failed: the server's certificate did
	// not verify, the server does not offer TLS the connection requires, or
	// the server requires TLS the connection did not use.
	TLSFailed Kind = "tls_failed"
)

// Kinds returns the vocabulary in declaration order.
func Kinds() []Kind {
	return []Kind{DatabaseUnreachable, AuthenticationFailed, DatabaseNotFound, PermissionDenied, TLSFailed}
}

// Report is the error object a machine-readable document carries for a run
// that failed.
type Report struct {
	// Kind is from the vocabulary, and absent when the failure has none.
	Kind Kind `json:"kind,omitempty"`
	// Message is the error written for a person. It can quote what the
	// database said, so a caller that must not publish that reads Kind.
	Message string `json:"message"`
}

// NewReport describes err, and returns nil for a nil err.
func NewReport(err error) *Report {
	if err == nil {
		return nil
	}
	return &Report{Kind: Classify(err), Message: err.Error()}
}

// Failure is the document a command prints when it fails and its own document
// has no place for the error: the version of the command's contract, the
// command, and the error. It carries none of the fields of the document a
// successful run prints, so a reader cannot take it for one.
type Failure struct {
	ContractVersion int     `json:"contract_version"`
	Command         string  `json:"command"`
	Error           *Report `json:"error"`
}

// NewFailure describes a run of command that failed with err, in the version
// of the command's contract.
func NewFailure(contractVersion int, command string, err error) Failure {
	return Failure{ContractVersion: contractVersion, Command: command, Error: NewReport(err)}
}

// TargetConnection marks err as the failure to open the connection to the
// command's target database, which is the only failure [Classify] gives a
// kind. The mark changes neither the message nor what errors.Is and errors.As
// find. It returns nil for a nil err.
func TargetConnection(err error) error {
	if err == nil {
		return nil
	}
	return &targetConnectionError{err: err}
}

// targetConnectionError is the mark [TargetConnection] puts on an error.
type targetConnectionError struct {
	err error
}

func (e *targetConnectionError) Error() string { return e.err.Error() }

func (e *targetConnectionError) Unwrap() error { return e.err }

// Classify returns the kind of err, and the empty Kind when it has none.
//
// Only the part of err under a [TargetConnection] mark is read. Within it an
// answer from the server decides first, because a server that answered was
// reached: a code outside the vocabulary leaves the failure with no kind
// rather than letting a network error elsewhere in the chain speak for it.
// Then a handshake failure, then a server that never answered. A cancellation
// has no kind, since the command was stopped rather than refused.
func Classify(err error) Kind {
	target, ok := errors.AsType[*targetConnectionError](err)
	if !ok {
		return ""
	}
	err = target.err
	if errors.Is(err, context.Canceled) {
		return ""
	}
	if kind, answered := serverAnswer(err); answered {
		return kind
	}
	if handshakeFailed(err) {
		return TLSFailed
	}
	if neverAnswered(err) {
		return DatabaseUnreachable
	}
	return ""
}

// PostgreSQL answers by SQLSTATE.
var (
	postgresAuthenticationFailed = []string{
		"28000", // invalid_authorization_specification: unknown role, no pg_hba.conf entry
		"28P01", // invalid_password
	}
	postgresDatabaseNotFound = []string{
		"3D000", // invalid_catalog_name
	}
	postgresPermissionDenied = []string{
		"42501", // insufficient_privilege: no CONNECT privilege on the database
	}
)

// MySQL and MariaDB answer by error number.
var (
	mysqlAuthenticationFailed = []uint16{
		1045, // ER_ACCESS_DENIED_ERROR
		1698, // ER_ACCESS_DENIED_NO_PASSWORD_ERROR
		1862, // ER_MUST_CHANGE_PASSWORD_LOGIN: the password expired
		3118, // ER_ACCOUNT_HAS_BEEN_LOCKED
		3955, // ER_USER_ACCESS_DENIED_FOR_USER_ACCOUNT_BLOCKED_BY_PASSWORD_LOCK
	}
	mysqlDatabaseNotFound = []uint16{
		1049, // ER_BAD_DB_ERROR
	}
	mysqlPermissionDenied = []uint16{
		1044, // ER_DBACCESS_DENIED_ERROR
		1142, // ER_TABLEACCESS_DENIED_ERROR
		1227, // ER_SPECIFIC_ACCESS_DENIED_ERROR
	}
	mysqlTLSFailed = []uint16{
		3159, // ER_SECURE_TRANSPORT_REQUIRED: the server requires TLS
	}
)

// serverAnswer reports the kind of the server's own answer in err, and whether
// err carries one at all.
func serverAnswer(err error) (Kind, bool) {
	if answer, ok := errors.AsType[*pgconn.PgError](err); ok {
		switch {
		case slices.Contains(postgresAuthenticationFailed, answer.Code):
			return AuthenticationFailed, true
		case slices.Contains(postgresDatabaseNotFound, answer.Code):
			return DatabaseNotFound, true
		case slices.Contains(postgresPermissionDenied, answer.Code):
			return PermissionDenied, true
		default:
			return "", true
		}
	}
	if answer, ok := errors.AsType[*mysqldriver.MySQLError](err); ok {
		switch {
		case slices.Contains(mysqlAuthenticationFailed, answer.Number):
			return AuthenticationFailed, true
		case slices.Contains(mysqlDatabaseNotFound, answer.Number):
			return DatabaseNotFound, true
		case slices.Contains(mysqlPermissionDenied, answer.Number):
			return PermissionDenied, true
		case slices.Contains(mysqlTLSFailed, answer.Number):
			return TLSFailed, true
		default:
			return "", true
		}
	}
	return "", false
}

// handshakeFailed reports a TLS handshake that did not complete: a
// certificate that did not verify, a server that answered TLS with something
// else, a TLS alert from the server, or a server without the TLS the
// connection requires.
//
// The x509 types are listed beside the crypto/tls one because a driver that
// verifies the certificate in its own callback, as pgx does for
// sslmode=verify-ca, returns the x509 error without the wrapper crypto/tls
// puts around its own verification. crypto/tls reports an alert the server
// sent as a *net.OpError whose Op is "remote error".
func handshakeFailed(err error) bool {
	if errors.Is(err, mysqldriver.ErrNoTLS) {
		return true
	}
	if _, ok := errors.AsType[*tls.CertificateVerificationError](err); ok {
		return true
	}
	if _, ok := errors.AsType[x509.UnknownAuthorityError](err); ok {
		return true
	}
	if _, ok := errors.AsType[x509.CertificateInvalidError](err); ok {
		return true
	}
	if _, ok := errors.AsType[tls.RecordHeaderError](err); ok {
		return true
	}
	alert, ok := errors.AsType[*net.OpError](err)
	return ok && alert.Op == "remote error"
}

// neverAnswered reports a connection the server never completed: a name that
// did not resolve, a dial that failed, or a deadline that passed while the
// connection was being opened.
//
// A timeout counts here only because the caller marked err as the failure to
// open the connection. The same timeout from a statement on an open
// connection says nothing about reachability, and it never reaches this
// function.
func neverAnswered(err error) bool {
	if _, ok := errors.AsType[*net.DNSError](err); ok {
		return true
	}
	if dial, ok := errors.AsType[*net.OpError](err); ok && dial.Op == "dial" {
		return true
	}
	return errors.Is(err, context.DeadlineExceeded)
}
