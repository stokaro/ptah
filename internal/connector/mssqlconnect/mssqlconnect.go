// Package mssqlconnect connects to SQL Server through go-mssqldb.
package mssqlconnect

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	_ "github.com/microsoft/go-mssqldb" // registers the sqlserver driver

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/connector"
	"ptah.run/internal/dbschema/mssql"
	"ptah.run/internal/sqlrunner"
)

// Registrations registers the service under SQL Server.
func Registrations() []connector.Registration {
	return []connector.Registration{{Dialect: platform.SQLServer, Service: Service{}}}
}

// Service is SQL Server's connector.
type Service struct {
	connector.Base
}

// Open opens the pool through go-mssqldb.
func (Service) Open(ctx context.Context, request connector.Request) (*connector.Pool, error) {
	db, err := connector.OpenDB(request.Dialect.Driver, convertSQLServerURL(request.URL))
	if err != nil {
		return nil, err
	}
	if err := connector.Ping(ctx, db); err != nil {
		return nil, err
	}
	return &connector.Pool{DB: db}, nil
}

// convertSQLServerURL spells the scheme as the one go-mssqldb reads and takes
// out the `schema` parameter, which is Ptah's rather than the driver's.
func convertSQLServerURL(dbURL string) string {
	parsed, err := url.Parse(dbURL)
	if err != nil || parsed.Scheme == "" {
		return dbURL
	}
	parsed.Scheme = platform.SQLServer
	query := parsed.Query()
	query.Del("schema")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// Describe reads the version, the collation CATALOG_DEFAULT applies, which
// decides how the catalog compares identifiers, and the schema: `dbo` unless
// the URL's `schema` parameter names another.
func (Service) Describe(
	ctx context.Context,
	opened *connector.Pool,
	request connector.Request,
	info catalog.ServerInfo,
) (catalog.ServerInfo, error) {
	db := opened.DB
	var version string
	if err := db.QueryRowContext(ctx, "SELECT @@VERSION").Scan(&version); err != nil {
		return info, fmt.Errorf("failed to get SQL Server version: %w", err)
	}
	info.Version = version
	var catalogCollation string
	// Inspect the collation applied by CATALOG_DEFAULT directly. The
	// sys.databases catalog_collation columns are Azure SQL Database-only.
	const catalogSemanticsQuery = `
SELECT
    COALESCE(CONVERT(
        nvarchar(128),
        SQL_VARIANT_PROPERTY(
            CONVERT(nvarchar(1), N'x') COLLATE CATALOG_DEFAULT,
            'Collation'
        )
    ), N'')`
	if err := db.QueryRowContext(ctx, catalogSemanticsQuery).Scan(&catalogCollation); err != nil {
		return info, fmt.Errorf("failed to get SQL Server catalog identifier semantics: %w", err)
	}
	info.Schema = "dbo"
	if schema := request.Parsed.Query().Get("schema"); schema != "" {
		info.Schema = schema
	}
	info.IdentifierSemantics = identifier.ForSQLServerCatalog(catalogCollation)
	info.IdentifierSemantics.DefaultSchema = info.Schema
	return info, nil
}

// NewReader returns the SQL Server reader.
func (Service) NewReader(_ *connector.Pool, info catalog.ServerInfo, runner sqlrunner.Runner) catalog.SchemaReader {
	return mssql.NewSQLServerReader(runner, info.Schema)
}

// NewWriter returns the SQL Server writer.
func (Service) NewWriter(_ *connector.Pool, info catalog.ServerInfo, runner sqlrunner.Runner, _ *sql.Conn) catalog.SchemaWriter {
	return mssql.NewSQLServerWriterForRunner(runner, info.Schema)
}

// OutsideTransaction asks the server: a session with @@TRANCOUNT zero is
// outside any transaction, whoever opened the last one.
func (Service) OutsideTransaction(ctx context.Context, session *sql.Conn) bool {
	var open int
	if err := session.QueryRowContext(ctx, "SELECT @@TRANCOUNT").Scan(&open); err != nil {
		return false
	}
	return open == 0
}
