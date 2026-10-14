// Package oracleconnect connects to Oracle Database through go-ora.
package oracleconnect

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/connector"
	"ptah.run/internal/dbschema/oracle"
	"ptah.run/internal/sqlrunner"
)

// Registrations registers the service under Oracle.
func Registrations() []connector.Registration {
	return []connector.Registration{{Dialect: platform.Oracle, Service: Service{}}}
}

// Service is Oracle's connector.
type Service struct {
	connector.Base
}

// Open opens the pool through go-ora, with its fast-login path off; see
// oracleDataSourceName.
func (Service) Open(ctx context.Context, request connector.Request) (*connector.Pool, error) {
	db, err := connector.OpenDB(dataSource(request))
	if err != nil {
		return nil, err
	}
	if err := connector.Ping(ctx, db); err != nil {
		return nil, err
	}
	return &connector.Pool{DB: db}, nil
}

// dataSource is the driver and the data source a connection to request
// opens.
func dataSource(request connector.Request) (driverName, dataSourceName string) {
	return request.Dialect.Driver, oracleDataSourceName(request.URL)
}

// oracleDataSourceName turns off go-ora's fast-login path, and with it the
// connection cookie that path caches.
//
// go-ora v3 caches the server's negotiation in a PACKAGE-LEVEL store, keyed by
// the server's UUID and service name, and replays it on the next connection to
// the same server to skip a round trip. Oracle 23.26.2.0.0 closes the socket on
// the replayed cookie, so the second connection a process opens answers EOF:
//
//	first handle ok, second handle -> EOF
//
// The store outlives the failure, and the driver's own recovery does not clear
// it: it deletes a cookie only when the error is driver.ErrBadConn, and this
// one is io.EOF. So one refused connection leaves every later one in the
// process refused too, whether or not the first is still open.
//
// Two connections is not an exotic thing to want. The capability probe waits
// for a server on one and measures it on the next; a comparison against a dev
// database holds the target and the dev server together; a command resolving a
// desired state from one URL and applying it to another does the same. All of
// it reported a connection error against a server that was up.
//
// Oracle 21.3.0.0.0 does not advertise the fast-auth capability the path needs,
// so it never cached a cookie and never failed -- which is why a check that ran
// only there would agree with a build that cannot read a 23 catalog
// (stokaro/ptah#1875).
// A caller who spells the option themselves keeps their answer, including
// switching it back on: the default is Ptah's, not a prohibition.
func oracleDataSourceName(dbURL string) string {
	parsed, err := url.Parse(dbURL)
	if err != nil {
		// The connection parsed this URL before reaching here, so a failure
		// is unreachable rather than tolerated. Returning the input unchanged
		// leaves the driver to report on it, which is a better message than
		// anything this function could invent about a URL it cannot read.
		return dbURL
	}
	query := parsed.Query()
	for key := range query {
		// Matched on the decoded key, so the caller may spell the space as a
		// space, a plus or %20 and still be heard.
		if strings.EqualFold(strings.TrimSpace(key), "FAST LOGIN") {
			return dbURL
		}
	}
	query.Set("FAST LOGIN", "FALSE")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// Describe reads the version and the schema an Oracle connection works in.
//
// The version comes from product_component_version.version_full rather than
// from the banner, and the difference is measured: on 21.3 the banner says
// "Release 21.0.0.0.0" while that column says "21.3.0.0.0", and only the second
// is a release line capability.ResolveServerVersion has measured. The banner
// carries a second trap on the newer line -- "Oracle AI Database 26ai Free
// Release 23.26.2.0.0" names 26 before it names 23.26 -- which the resolver
// handles, and which this column avoids entirely.
//
// The schema is the connected user, because in Oracle a schema IS a user:
// objects live in the namespace of the account that owns them, and there is no
// separate namespace to select. An explicit ?schema= still wins, for the reader
// that has been granted access to somebody else's objects.
func (Service) Describe(
	ctx context.Context,
	opened *connector.Pool,
	request connector.Request,
	info catalog.ServerInfo,
) (catalog.ServerInfo, error) {
	db := opened.DB
	var version string
	const versionQuery = `SELECT version_full FROM product_component_version WHERE ROWNUM = 1`
	if err := db.QueryRowContext(ctx, versionQuery).Scan(&version); err != nil {
		return info, fmt.Errorf("failed to get Oracle version: %w", err)
	}
	info.Version = version

	var currentSchema string
	const schemaQuery = `SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM dual`
	if err := db.QueryRowContext(ctx, schemaQuery).Scan(&currentSchema); err != nil {
		return info, fmt.Errorf("failed to get Oracle schema: %w", err)
	}
	info.Schema = currentSchema
	if schema := request.Parsed.Query().Get("schema"); schema != "" {
		info.Schema = schema
	}
	info.IdentifierSemantics = identifier.ForDialect(platform.Oracle)
	info.IdentifierSemantics.DefaultSchema = info.Schema
	return info, nil
}

// NewReader returns the Oracle reader.
func (Service) NewReader(_ *connector.Pool, info catalog.ServerInfo, runner sqlrunner.Runner) catalog.SchemaReader {
	return oracle.NewOracleReaderWithCapabilities(runner, info.Schema, info.Capabilities)
}

// NewWriter returns the Oracle writer.
func (Service) NewWriter(_ *connector.Pool, info catalog.ServerInfo, runner sqlrunner.Runner, _ *sql.Conn) catalog.SchemaWriter {
	return oracle.NewOracleWriterForRunner(runner, info.Schema)
}

// ReadOnlyTransaction moves a read-only request from the driver, which cannot
// carry one, to the server, which can.
//
// go-ora answers "readonly transaction is not supported" to a BeginTx that asks
// for a read-only transaction, so passing the request through drops it in
// silence. The Oracle server accepts SET TRANSACTION READ ONLY as the first
// statement of a transaction and then answers ORA-01456 to an insert, update or
// delete inside it, measured on Oracle Free 23.
//
// What that binds is DML. Oracle commits DDL on its own, and a routine declared
// PRAGMA AUTONOMOUS_TRANSACTION runs in a transaction of its own, so a caller
// that needs a statement to change nothing still has to say so about the
// statement itself.
func (Service) ReadOnlyTransaction(opts *sql.TxOptions) (*sql.TxOptions, string) {
	if opts == nil || !opts.ReadOnly {
		return opts, ""
	}
	carried := *opts
	carried.ReadOnly = false
	return &carried, "SET TRANSACTION READ ONLY"
}
