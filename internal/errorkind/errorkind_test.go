package errorkind_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/errorkind"
)

// connectFailure opens dbURL through Ptah's own connection path, the one
// every command opens its target with, and returns the error it fails with.
//
// A zero wait gives the attempt longer than any refusal takes, Windows
// included, which retries a refused loopback dial for about two seconds. A
// row whose server never answers names its own, shorter wait.
func connectFailure(c *qt.C, dbURL string, wait time.Duration) error {
	c.Helper()
	if wait == 0 {
		wait = 20 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(conn, qt.IsNil, qt.Commentf("the connection to %s opened", dbURL))
	c.Assert(err, qt.IsNotNil)
	return err
}

// unresolvableHost is a name that never resolves (RFC 6761 reserves .invalid).
const unresolvableHost = "ptah-errorkind.invalid"

// TestClassifyPostgreSQL holds each kind to the error pgx returns for it,
// through Ptah's PostgreSQL connector.
func TestClassifyPostgreSQL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		behavior serverBehavior
		sqlstate string
		host     string
		query    string
		// wait bounds the attempt for a server that never answers.
		wait time.Duration
		want errorkind.Kind
	}{
		{name: "nothing listens on the port", host: "refused", query: "sslmode=disable", want: errorkind.DatabaseUnreachable},
		{name: "the host name does not resolve", host: unresolvableHost + ":5432", query: "sslmode=disable", want: errorkind.DatabaseUnreachable},
		{name: "the server never answers", behavior: stayQuiet, query: "sslmode=disable", wait: 2 * time.Second, want: errorkind.DatabaseUnreachable},
		{name: "the password is wrong", behavior: answerError, sqlstate: "28P01", query: "sslmode=disable", want: errorkind.AuthenticationFailed},
		{name: "the role is unknown or no host rule admits it", behavior: answerError, sqlstate: "28000", query: "sslmode=disable", want: errorkind.AuthenticationFailed},
		{name: "the database does not exist", behavior: answerError, sqlstate: "3D000", query: "sslmode=disable", want: errorkind.DatabaseNotFound},
		{name: "the role may not connect to the database", behavior: answerError, sqlstate: "42501", query: "sslmode=disable", want: errorkind.PermissionDenied},
		// pgx tries each host of a multi-host URL and joins the errors. The
		// server that answered was reached, so its answer decides over the
		// host that refused the dial.
		{name: "one host refuses and the other refuses the password", host: "refused,", behavior: answerError, sqlstate: "28P01", query: "sslmode=disable", want: errorkind.AuthenticationFailed},
		{name: "the certificate does not verify", behavior: presentUntrustedCertificate, query: "sslmode=verify-full", want: errorkind.TLSFailed},
		{name: "the certificate authority does not verify", behavior: presentUntrustedCertificate, query: "sslmode=verify-ca", want: errorkind.TLSFailed},
		{name: "a trusted certificate expired", behavior: presentExpiredCertificate, query: "sslmode=verify-ca&sslrootcert={root}", want: errorkind.TLSFailed},
		// The certificate is trusted and names the loopback address, not the
		// name the URL dials, so only crypto/tls's own wrapper says why.
		{name: "a trusted certificate names another host", host: "localhost", behavior: presentUntrustedCertificate, query: "sslmode=verify-full&sslrootcert={root}", want: errorkind.TLSFailed},
		{name: "the server requires a client certificate", behavior: requireClientCertificate, query: "sslmode=require", want: errorkind.TLSFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			host, rootFile := postgresHost(c, test.host, test.behavior, test.sqlstate)
			query := strings.ReplaceAll(test.query, "{root}", url.QueryEscape(rootFile))
			err := connectFailure(c, fmt.Sprintf("postgres://ptah:secret@%s/app?%s", host, query), test.wait)
			c.Assert(errorkind.Classify(errorkind.TargetConnection(err)), qt.Equals, test.want, qt.Commentf("%v", err))
		})
	}
}

// TestClassifyMySQL holds each kind to the error go-sql-driver/mysql returns
// for it, through Ptah's MySQL connector.
func TestClassifyMySQL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		behavior serverBehavior
		number   uint16
		host     string
		query    string
		// wait bounds the attempt for a server that never answers.
		wait time.Duration
		want errorkind.Kind
	}{
		{name: "nothing listens on the port", host: "refused", want: errorkind.DatabaseUnreachable},
		{name: "the host name does not resolve", host: unresolvableHost + ":3306", want: errorkind.DatabaseUnreachable},
		{name: "the server never answers", behavior: stayQuiet, wait: 2 * time.Second, want: errorkind.DatabaseUnreachable},
		{name: "the password is wrong", behavior: answerError, number: 1045, want: errorkind.AuthenticationFailed},
		{name: "the account has no password and the server wants one", behavior: answerError, number: 1698, want: errorkind.AuthenticationFailed},
		{name: "the account is locked", behavior: answerError, number: 3118, want: errorkind.AuthenticationFailed},
		{name: "the account is blocked after failed logins", behavior: answerError, number: 3955, want: errorkind.AuthenticationFailed},
		{name: "the password expired", behavior: answerError, number: 1862, want: errorkind.AuthenticationFailed},
		{name: "the database does not exist", behavior: answerError, number: 1049, want: errorkind.DatabaseNotFound},
		{name: "the user may not use the database", behavior: answerError, number: 1044, want: errorkind.PermissionDenied},
		{name: "the user may not read a table the connection reads", behavior: answerError, number: 1142, want: errorkind.PermissionDenied},
		{name: "the user lacks a privilege the connection needs", behavior: answerError, number: 1227, want: errorkind.PermissionDenied},
		{name: "the server requires TLS", behavior: answerError, number: 3159, want: errorkind.TLSFailed},
		{name: "the server does not offer TLS", behavior: refuseTLS, query: "?tls=true", want: errorkind.TLSFailed},
		{name: "the certificate does not verify", behavior: presentUntrustedCertificate, query: "?tls=true", want: errorkind.TLSFailed},
		{name: "the server answers TLS in plaintext", behavior: answerTLSInPlaintext, query: "?tls=skip-verify", want: errorkind.TLSFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			host := mysqlHost(c, test.host, test.behavior, test.number)
			err := connectFailure(c, fmt.Sprintf("mysql://ptah:secret@%s/app%s", host, test.query), test.wait)
			c.Assert(errorkind.Classify(errorkind.TargetConnection(err)), qt.Equals, test.want, qt.Commentf("%v", err))
		})
	}
}

// TestClassifyLeavesOtherFailuresWithoutAKind holds the failures that have no
// kind to having none, each beside a failure of the same shape that has one.
func TestClassifyLeavesOtherFailuresWithoutAKind(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		url  string
	}{
		// The server answered, so it was reached; the answer is not one the
		// vocabulary names.
		{name: "a PostgreSQL server answers with too many connections", url: "postgres-53300"},
		{name: "a MySQL server answers with too many connections", url: "mysql-1040"},
		// The answer decides even beside a host that refused the dial, which
		// alone would be an unreachable server.
		{name: "one host refuses and the other answers with too many connections", url: "postgres-refused-53300"},
		// pgx returns this refusal as text with no type or sentinel of its
		// own, and a kind is never read from text. The MySQL driver's
		// sentinel for the same refusal is classified above.
		{name: "a PostgreSQL server refuses the TLS a connection requires", url: "postgres-refuse-tls"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			err := connectFailure(c, unclassifiedURL(c, test.url), 0)
			c.Assert(errorkind.Classify(errorkind.TargetConnection(err)), qt.Equals, errorkind.Kind(""), qt.Commentf("%v", err))
		})
	}
}

// TestClassifyLetsAMySQLAnswerDecide holds the ordering rule for MySQL, whose
// driver makes one attempt and so never joins a server's answer with a network
// error itself. Joined here from the errors the driver returned, an answer the
// vocabulary does not name still leaves the failure with no kind, where the
// refused dial alone is an unreachable server.
func TestClassifyLetsAMySQLAnswerDecide(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	answer := connectFailure(c, "mysql://ptah:secret@"+startMySQL(c, answerError, 1040)+"/app", 0)
	refused := connectFailure(c, "mysql://ptah:secret@"+refusedAddress(c)+"/app", 0)

	c.Assert(errorkind.Classify(errorkind.TargetConnection(errors.Join(refused, answer))), qt.Equals, errorkind.Kind(""))
	c.Assert(errorkind.Classify(errorkind.TargetConnection(refused)), qt.Equals, errorkind.DatabaseUnreachable)
}

// TestClassifyReadsOnlyTheTargetConnection is the reason for the mark: the
// same refused dial is the target being unreachable when the command marks it,
// and has no kind when it does not, because a registry, a webhook and a dev
// database dial too.
func TestClassifyReadsOnlyTheTargetConnection(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	err := connectFailure(c, "postgres://ptah:secret@"+refusedAddress(c)+"/app?sslmode=disable", 0)

	c.Assert(errorkind.Classify(err), qt.Equals, errorkind.Kind(""))
	c.Assert(errorkind.Classify(fmt.Errorf("fetch the schema: %w", err)), qt.Equals, errorkind.Kind(""))
	c.Assert(errorkind.Classify(fmt.Errorf("connect to --db-url: %w", errorkind.TargetConnection(err))), qt.Equals, errorkind.DatabaseUnreachable)
}

// TestClassifyGivesACancellationNoKind: an interrupted connection was stopped,
// not refused, even when the dial it interrupted is a dial error.
func TestClassifyGivesACancellationNoKind(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	address, _ := startPostgres(c, stayQuiet, "")
	stop := time.AfterFunc(100*time.Millisecond, cancel)
	c.Cleanup(func() { stop.Stop() })
	conn, err := dbschema.ConnectToDatabase(ctx, "postgres://ptah:secret@"+address+"/app?sslmode=disable")

	c.Assert(conn, qt.IsNil)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(errorkind.Classify(errorkind.TargetConnection(err)), qt.Equals, errorkind.Kind(""))
}

// TestClassifyGivesACanceledDialNoKind holds the cancellation check to the
// error the net package returns when the command's context ends during a dial:
// a dial error, which would otherwise read as an unreachable server.
func TestClassifyGivesACanceledDialNoKind(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	address := refusedAddress(c)
	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()

	_, canceled := (&net.Dialer{}).DialContext(canceledContext, "tcp", address)
	_, refused := (&net.Dialer{}).DialContext(context.Background(), "tcp", address)

	c.Assert(canceled, qt.ErrorAs, new(*net.OpError))
	c.Assert(canceled, qt.ErrorIs, context.Canceled)
	c.Assert(errorkind.Classify(errorkind.TargetConnection(canceled)), qt.Equals, errorkind.Kind(""))
	c.Assert(errorkind.Classify(errorkind.TargetConnection(refused)), qt.Equals, errorkind.DatabaseUnreachable)
}

func TestTargetConnectionKeepsTheError(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	cause := errors.New("dial tcp 127.0.0.1:1: connect: connection refused")

	marked := errorkind.TargetConnection(cause)

	c.Assert(marked, qt.ErrorIs, cause)
	c.Assert(marked.Error(), qt.Equals, cause.Error())
	c.Assert(errorkind.TargetConnection(nil), qt.IsNil)
}

func TestNewReport(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	err := connectFailure(c, "mysql://ptah:secret@"+refusedAddress(c)+"/app", 0)
	marked := fmt.Errorf("connect to --db-url: %w", errorkind.TargetConnection(err))

	c.Assert(errorkind.NewReport(marked), qt.DeepEquals, &errorkind.Report{
		Kind:    errorkind.DatabaseUnreachable,
		Message: marked.Error(),
	})
	c.Assert(errorkind.NewReport(err), qt.DeepEquals, &errorkind.Report{Message: err.Error()})
	c.Assert(errorkind.NewReport(nil), qt.IsNil)
}

func TestKindsListsTheVocabulary(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	c.Assert(errorkind.Kinds(), qt.DeepEquals, []errorkind.Kind{
		"database_unreachable",
		"authentication_failed",
		"database_not_found",
		"permission_denied",
		"tls_failed",
	})
}

// postgresHost starts what a row describes and returns the host:port it
// listens on, or the row's own host, and the file holding the certificate
// the server presents.
func postgresHost(c *qt.C, host string, behavior serverBehavior, sqlstate string) (address, rootFile string) {
	c.Helper()
	switch host {
	case "":
		return startPostgres(c, behavior, sqlstate)
	case "refused":
		return refusedAddress(c), ""
	case "refused,":
		address, rootFile := startPostgres(c, behavior, sqlstate)
		return refusedAddress(c) + "," + address, rootFile
	case "localhost":
		address, rootFile := startPostgres(c, behavior, sqlstate)
		_, port, err := net.SplitHostPort(address)
		c.Assert(err, qt.IsNil)
		return net.JoinHostPort("localhost", port), rootFile
	default:
		return host, ""
	}
}

// mysqlHost is postgresHost for MySQL.
func mysqlHost(c *qt.C, host string, behavior serverBehavior, number uint16) string {
	c.Helper()
	switch host {
	case "":
		return startMySQL(c, behavior, number)
	case "refused":
		return refusedAddress(c)
	default:
		return host
	}
}

// unclassifiedURL starts the server an unclassified row names.
func unclassifiedURL(c *qt.C, row string) string {
	c.Helper()
	switch row {
	case "postgres-53300":
		address, _ := startPostgres(c, answerError, "53300")
		return "postgres://ptah:secret@" + address + "/app?sslmode=disable"
	case "mysql-1040":
		return "mysql://ptah:secret@" + startMySQL(c, answerError, 1040) + "/app"
	case "postgres-refused-53300":
		address, _ := startPostgres(c, answerError, "53300")
		return "postgres://ptah:secret@" + refusedAddress(c) + "," + address + "/app?sslmode=disable"
	case "postgres-refuse-tls":
		address, _ := startPostgres(c, refuseTLS, "")
		return "postgres://ptah:secret@" + address + "/app?sslmode=require"
	default:
		c.Fatalf("no server for row %q", row)
		return ""
	}
}
