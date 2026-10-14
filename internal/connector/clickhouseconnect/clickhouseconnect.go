// Package clickhouseconnect connects to ClickHouse through clickhouse-go's
// database/sql driver.
package clickhouseconnect

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/connector"
	"ptah.run/internal/dbschema/clickhouse"
	"ptah.run/internal/sqlrunner"
)

// Registrations registers the service under ClickHouse.
func Registrations() []connector.Registration {
	return []connector.Registration{{Dialect: platform.ClickHouse, Service: Service{}}}
}

// Service is ClickHouse's connector.
type Service struct {
	connector.Base
}

// Open opens the pool through clickhouse-go.
func (Service) Open(ctx context.Context, request connector.Request) (*connector.Pool, error) {
	db, err := connector.OpenDB(request.Dialect.Driver, convertClickHouseURL(request.URL))
	if err != nil {
		return nil, err
	}
	if err := connector.Ping(ctx, db); err != nil {
		return nil, err
	}
	return &connector.Pool{DB: db}, nil
}

// convertClickHouseURL normalizes a ClickHouse connection URL into the form
// expected by the clickhouse-go/v2 driver's database/sql registration.
//
// The driver accepts either the canonical `clickhouse://user:pass@host:port/db`
// URL form or the legacy DSN form. We canonicalize to the URL form and pass
// the query parameters through unchanged. If the input cannot be parsed as a
// URL we return it as-is so the driver can produce its own (likely better)
// error message.
func convertClickHouseURL(dbURL string) string {
	parsed, err := url.Parse(dbURL)
	if err != nil {
		return dbURL
	}
	if parsed.Scheme == "" {
		return dbURL
	}
	// The driver registers as the lowercase scheme "clickhouse". Normalize.
	parsed.Scheme = "clickhouse"
	return parsed.String()
}

// Describe reads the version and the database the URL selects, or the
// session's current one when the URL names none.
func (Service) Describe(
	ctx context.Context,
	opened *connector.Pool,
	request connector.Request,
	info catalog.ServerInfo,
) (catalog.ServerInfo, error) {
	db := opened.DB
	var version string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		return info, fmt.Errorf("failed to get ClickHouse version: %w", err)
	}
	info.Version = version

	parsedURL := request.Parsed
	if parsedURL.Path != "" && len(parsedURL.Path) > 1 {
		info.Schema = parsedURL.Path[1:]
	} else {
		var dbName string
		if err := db.QueryRowContext(ctx, "SELECT currentDatabase()").Scan(&dbName); err != nil {
			return info, fmt.Errorf("failed to get current ClickHouse database name: %w", err)
		}
		info.Schema = dbName
	}
	// A ClickHouse database is a schema, and no static rule names the
	// connected one -- the same shape as the MySQL family.
	info.IdentifierSemantics.DefaultSchema = info.Schema
	return info, nil
}

// NewReader returns the ClickHouse reader. Capabilities and version travel
// with it so that roles and grants are read only where RoleManagement says
// they exist, and so the diagnostic can name the server it read. Without them
// a live ClickHouse would describe no RBAC at all and every declared role
// would be planned again on every run (stokaro/ptah#1025).
func (Service) NewReader(_ *connector.Pool, info catalog.ServerInfo, runner sqlrunner.Runner) catalog.SchemaReader {
	return clickhouse.NewClickHouseReaderWithCapabilities(runner, info.Schema, info.Version, info.Capabilities)
}

// NewWriter returns the ClickHouse writer. The capabilities travel with it for
// the reason they travel with the reader: the realm-cleanup gate is a
// capability question, and resolving it here means the banner is read once
// (stokaro/ptah#916).
func (Service) NewWriter(_ *connector.Pool, info catalog.ServerInfo, runner sqlrunner.Runner, _ *sql.Conn) catalog.SchemaWriter {
	return clickhouse.NewClickHouseWriterForRunnerWithCapabilities(runner, info.Schema, info.Capabilities)
}

// Transactional answers false: clickhouse-go's database/sql driver does not
// implement transactions, so an isolated query runs directly on its
// disposable session.
func (Service) Transactional() bool { return false }
