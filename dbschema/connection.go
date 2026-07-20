package dbschema

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"regexp"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"  // PostgreSQL driver
	_ "github.com/microsoft/go-mssqldb" // SQL Server driver

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/dbschema/clickhouse"
	"github.com/stokaro/ptah/internal/dbschema/mssql"
	"github.com/stokaro/ptah/internal/dbschema/mysql"
	"github.com/stokaro/ptah/internal/dbschema/postgres"
	"github.com/stokaro/ptah/internal/dbschema/sqlite"
)

// ConnectToDatabase creates a database connection from a URL.
//
// The provided context governs the initial Ping used to verify the connection
// and the metadata queries issued to populate [DBInfo]. Cancelling the context
// before or during the call causes ConnectToDatabase to return promptly with
// the context error wrapped in a descriptive message. The context does not
// affect the lifetime of the returned *DatabaseConnection; callers are
// responsible for closing it.
func ConnectToDatabase(ctx context.Context, dbURL string) (*DatabaseConnection, error) {
	// Handle MySQL URLs specially since they have a different format
	var parsedURL *url.URL
	var err error

	if (strings.HasPrefix(dbURL, "mysql://") || strings.HasPrefix(dbURL, "mariadb://")) && strings.Contains(dbURL, "@tcp(") {
		// For MySQL/MariaDB URLs, create a fake parseable URL for scheme detection
		fakeURL := strings.Replace(dbURL, "@tcp(", "@", 1)
		fakeURL = strings.Replace(fakeURL, ")", "", 1)
		parsedURL, err = url.Parse(fakeURL)
	} else {
		parsedURL, err = url.Parse(dbURL)
	}

	if err != nil {
		return nil, fmt.Errorf("invalid database URL: %w", err)
	}

	// Check for empty or invalid scheme
	if parsedURL.Scheme == "" {
		return nil, fmt.Errorf("invalid database URL: missing scheme")
	}

	// Determine the dialect
	rawDialect := strings.ToLower(parsedURL.Scheme)
	dialect := platform.NormalizeDialect(rawDialect)
	if dialect == "" {
		return nil, fmt.Errorf("unsupported database dialect: %s", rawDialect)
	}

	dialectProtocol, connectionString := databaseDriverConfig(dialect, dbURL)

	db, err := sql.Open(dialectProtocol, connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}
	if dialectProtocol == "sqlite" {
		db.SetMaxOpenConns(1)
	}

	// Test the connection — honour the caller-supplied context so a stuck or
	// slow host cannot block ConnectToDatabase indefinitely.
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Get database info
	info, err := getDatabaseInfo(ctx, db, dialect, parsedURL, dbURL)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to get database info: %w", err)
	}
	caps, versionSpecific := capability.ForServerVersionResult(info.Dialect, info.Version)
	info.Capabilities = caps
	if !versionSpecific {
		slog.Debug(
			"falling back to dialect default capabilities",
			"dialect", info.Dialect,
			"version", info.Version,
		)
	}

	// Create appropriate schema reader and writer
	var reader types.SchemaReader
	var writer types.SchemaWriter
	switch dialectProtocol {
	case "pgx":
		reader = postgres.NewPostgreSQLReaderWithCapabilities(db, info.Schema, info.Capabilities)
		writer = postgres.NewPostgreSQLWriter(db, info.Schema)
	case "mysql":
		reader = mysql.NewMySQLReader(db, info.Schema)
		writer = mysql.NewMySQLWriter(db, info.Schema)
	case "clickhouse":
		reader = clickhouse.NewClickHouseReader(db, info.Schema)
		writer = clickhouse.NewClickHouseWriter(db, info.Schema)
	case "sqlite":
		reader = sqlite.NewSQLiteReader(db, info.Schema)
		writer = sqlite.NewSQLiteWriter(db, info.Schema)
	case "sqlserver":
		reader = mssql.NewSQLServerReader(db, info.Schema)
		writer = mssql.NewSQLServerWriter(db, info.Schema)
	default:
		_ = db.Close()
		return nil, fmt.Errorf("no schema reader available for dialect: %s", dialect)
	}

	return &DatabaseConnection{
		db:     db,
		info:   info,
		reader: reader,
		writer: writer,
	}, nil
}

func databaseDriverConfig(dialect, dbURL string) (driverName, dataSourceName string) {
	switch dialect {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		return "pgx", convertPostgresWireURL(dbURL)
	case platform.MySQL, platform.MariaDB:
		return "mysql", convertMySQLURL(dbURL)
	case platform.ClickHouse:
		return "clickhouse", convertClickHouseURL(dbURL)
	case platform.SQLite:
		return "sqlite", convertSQLiteURL(dbURL)
	case platform.SQLServer:
		return "sqlserver", convertSQLServerURL(dbURL)
	default:
		return "", ""
	}
}

// DatabaseConnection represents a database connection with metadata
type DatabaseConnection struct {
	db       *sql.DB
	info     types.DBInfo
	reader   types.SchemaReader
	writer   types.SchemaWriter
	executor types.SchemaExecutor
}

type schemaScopedReader interface {
	SetSchemas([]string)
}

// ReadSchemaWithSchemas reads a database schema, applying a schema allow-list
// when the underlying dialect reader supports schema scoping.
func ReadSchemaWithSchemas(conn *DatabaseConnection, schemas []string) (*types.DBSchema, error) {
	reader := conn.Reader()
	scoped, ok := reader.(schemaScopedReader)
	if ok {
		scoped.SetSchemas(schemas)
		defer scoped.SetSchemas(nil)
	}
	return reader.ReadSchema()
}

// Info returns the database connection information
func (dc *DatabaseConnection) Info() types.DBInfo {
	info := dc.info
	info.Capabilities = info.Capabilities.Clone()
	return info
}

// Reader returns the schema reader
func (dc *DatabaseConnection) Reader() types.SchemaReader {
	return dc.reader
}

// Writer returns the active schema SQL executor. Transaction-scoped connection
// copies return their transaction executor here; root connections return the
// root schema writer.
func (dc *DatabaseConnection) Writer() types.SchemaExecutor {
	if dc.executor != nil {
		return dc.executor
	}
	return dc.writer
}

// SchemaWriter returns the root schema writer for administrative operations
// such as starting transactions, toggling dry-run mode, or dropping all tables.
func (dc *DatabaseConnection) SchemaWriter() types.SchemaWriter {
	return dc.writer
}

// WithExecutor returns a shallow connection copy that uses executor as the active
// SQL executor while keeping the same database handle, reader, and metadata.
//
// This is used to pass transaction-scoped writers into migration callbacks
// without storing the active transaction on the root writer.
func (dc *DatabaseConnection) WithExecutor(executor types.SchemaExecutor) *DatabaseConnection {
	cloned := *dc
	cloned.executor = executor
	return &cloned
}

// Query executes a query and returns the result rows
func (dc *DatabaseConnection) Query(query string, args ...any) (*sql.Rows, error) {
	return dc.db.Query(query, args...)
}

// QueryRow executes a query that returns a single row
func (dc *DatabaseConnection) QueryRow(query string, args ...any) *sql.Row {
	return dc.db.QueryRow(query, args...)
}

// QueryRowContext executes a query that returns a single row using a context
func (dc *DatabaseConnection) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return dc.db.QueryRowContext(ctx, query, args...)
}

// Exec executes a query without returning any rows
func (dc *DatabaseConnection) Exec(query string, args ...any) (sql.Result, error) {
	return dc.db.Exec(query, args...)
}

// ExecContext executes a query without returning any rows using a context
func (dc *DatabaseConnection) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return dc.db.ExecContext(ctx, query, args...)
}

// Conn returns a dedicated database session. Callers that use session-scoped
// database features must close the returned connection when finished.
func (dc *DatabaseConnection) Conn(ctx context.Context) (*sql.Conn, error) {
	return dc.db.Conn(ctx)
}

// Close closes the database connection
func (dc *DatabaseConnection) Close() error {
	if dc.db != nil {
		return dc.db.Close()
	}
	return nil
}

// CloseAndWarn closes the connection and logs a warning at slog.LevelWarn if
// Close returns an error. It is intended for `defer` use in CLI handlers and
// library code that does not have a natural error channel for cleanup
// failures, so that close errors are surfaced rather than silently dropped.
//
// Calling CloseAndWarn on a nil *DatabaseConnection is a no-op, allowing the
// idiom:
//
//	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
//	if err != nil {
//	    return err
//	}
//	defer dbschema.CloseAndWarn(conn)
func CloseAndWarn(conn *DatabaseConnection) {
	if conn == nil {
		return
	}
	if err := conn.Close(); err != nil {
		slog.Warn("failed to close database connection", "error", err)
	}
}

const redactedQueryValue = "redacted"

var mySQLTCPPasswordPattern = regexp.MustCompile(`^((?:mysql|mariadb)://[^:@/?#]+):([^@/?#]+)@`)

// FormatDatabaseURL formats a database URL for display (hiding secrets).
func FormatDatabaseURL(dbURL string) string {
	// Handle MySQL/MariaDB URLs specially since they have a different format
	if (strings.HasPrefix(dbURL, "mysql://") || strings.HasPrefix(dbURL, "mariadb://")) && strings.Contains(dbURL, "@tcp(") {
		// For MySQL/MariaDB URLs like mysql://user:pass@tcp(host:port)/db?params
		// Redact only the leading authority credentials, not DSN-like values in query params.
		return redactURLQuery(mySQLTCPPasswordPattern.ReplaceAllString(dbURL, "$1:***@"))
	}

	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		return dbURL
	}
	parsedURL.RawQuery = redactRawQuery(parsedURL.RawQuery)

	// Hide password
	if parsedURL.User != nil {
		if _, hasPassword := parsedURL.User.Password(); hasPassword {
			return formatURLWithRedactedUserPassword(parsedURL)
		}
	}

	return parsedURL.String()
}

func formatURLWithRedactedUserPassword(parsedURL *url.URL) string {
	displayURL := *parsedURL
	username := displayURL.User.Username()
	displayURL.User = nil

	prefix := displayURL.Scheme + "://"
	base := strings.TrimPrefix(displayURL.String(), prefix)
	return prefix + username + ":***@" + base
}

func redactURLQuery(displayURL string) string {
	prefix, rawQuery, ok := strings.Cut(displayURL, "?")
	if !ok {
		return displayURL
	}

	query, fragment, hasFragment := strings.Cut(rawQuery, "#")
	redactedQuery := redactRawQuery(query)
	if redactedQuery == "" {
		if hasFragment {
			return prefix + "#" + fragment
		}
		return prefix
	}

	result := prefix + "?" + redactedQuery
	if hasFragment {
		result += "#" + fragment
	}
	return result
}

func redactRawQuery(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}

	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return ""
	}
	for key, values := range query {
		if isSecretQueryParam(key) {
			for idx := range values {
				values[idx] = redactedQueryValue
			}
			query[key] = values
		}
	}
	return query.Encode()
}

func isSecretQueryParam(key string) bool {
	switch strings.ToLower(key) {
	case "access_token",
		"api_key",
		"apikey",
		"aws_secret_access_key",
		"aws_session_token",
		"client_secret",
		"id_token",
		"password",
		"passwd",
		"private_key",
		"pwd",
		"refresh_token",
		"secret",
		"sslcert",
		"sslkey",
		"sslpassword",
		"token":
		return true
	default:
		return false
	}
}

// getDatabaseInfo retrieves database metadata
func getDatabaseInfo(ctx context.Context, db *sql.DB, dialect string, parsedURL *url.URL, originalURL string) (types.DBInfo, error) {
	info := types.DBInfo{
		Dialect: dialect,
		URL:     originalURL,
	}

	switch dialect {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		// Get PostgreSQL version
		var version string
		err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version)
		if err != nil {
			return info, fmt.Errorf("failed to get PostgreSQL version: %w", err)
		}
		info.Version = version
		info.Dialect = detectPostgresWireDialect(dialect, version)

		// Get schema name (default to 'public' if not specified in URL)
		schema := "public"
		if parsedURL.Path != "" && len(parsedURL.Path) > 1 {
			// Extract database name from path, schema is typically 'public'
			// For PostgreSQL, schema is usually specified via search_path or defaults to 'public'
			schema = "public"
		}
		info.Schema = schema

	case platform.MySQL, platform.MariaDB:
		// Get MySQL/MariaDB version
		var version string
		err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
		if err != nil {
			return info, fmt.Errorf("failed to get MySQL/MariaDB version: %w", err)
		}
		info.Version = version

		// Get database name from URL path
		if parsedURL.Path != "" && len(parsedURL.Path) > 1 {
			info.Schema = parsedURL.Path[1:] // Remove leading '/'
		} else {
			// Get current database
			var dbName string
			err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&dbName)
			if err != nil {
				return info, fmt.Errorf("failed to get current database name: %w", err)
			}
			info.Schema = dbName
		}
	case platform.ClickHouse:
		var version string
		if err := db.QueryRow("SELECT version()").Scan(&version); err != nil {
			return info, fmt.Errorf("failed to get ClickHouse version: %w", err)
		}
		info.Version = version

		if parsedURL.Path != "" && len(parsedURL.Path) > 1 {
			info.Schema = parsedURL.Path[1:]
		} else {
			var dbName string
			if err := db.QueryRow("SELECT currentDatabase()").Scan(&dbName); err != nil {
				return info, fmt.Errorf("failed to get current ClickHouse database name: %w", err)
			}
			info.Schema = dbName
		}
	case platform.SQLite:
		var version string
		if err := db.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version); err != nil {
			return info, fmt.Errorf("failed to get SQLite version: %w", err)
		}
		info.Version = version
		info.Schema = "main"
	case platform.SQLServer:
		var version string
		if err := db.QueryRowContext(ctx, "SELECT @@VERSION").Scan(&version); err != nil {
			return info, fmt.Errorf("failed to get SQL Server version: %w", err)
		}
		info.Version = version
		info.Schema = "dbo"
		if schema := parsedURL.Query().Get("schema"); schema != "" {
			info.Schema = schema
		}
	}

	return info, nil
}

func detectPostgresWireDialect(declaredDialect, version string) string {
	versionLower := strings.ToLower(version)
	switch {
	case strings.Contains(versionLower, "cockroachdb"):
		return platform.CockroachDB
	case strings.Contains(versionLower, "yugabytedb") || strings.Contains(versionLower, "yugabyte") || strings.Contains(versionLower, "-yb-"):
		return platform.YugabyteDB
	case strings.Contains(versionLower, "spanner"):
		return platform.Spanner
	default:
		return platform.NormalizeDialect(declaredDialect)
	}
}

// convertMySQLURL converts a MySQL/MariaDB URL from standard format to Go driver format
func convertMySQLURL(dbURL string) string {
	// If the URL is already in the correct format (contains @tcp), return as-is
	if strings.Contains(dbURL, "@tcp(") {
		// Remove the mysql:// or mariadb:// prefix if present
		if after, ok := strings.CutPrefix(dbURL, "mysql://"); ok {
			return after
		}
		if after, ok := strings.CutPrefix(dbURL, "mariadb://"); ok {
			return after
		}
		return dbURL
	}

	// Parse the URL
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		return dbURL // Return as-is if parsing fails
	}

	// Extract components
	user := parsedURL.User.Username()
	password, _ := parsedURL.User.Password()
	host := parsedURL.Host
	dbName := strings.TrimPrefix(parsedURL.Path, "/")
	query := parsedURL.RawQuery

	// Build MySQL connection string: user:password@tcp(host)/database?params
	connectionString := fmt.Sprintf("%s:%s@tcp(%s)/%s", user, password, host, dbName)
	if query != "" {
		connectionString += "?" + query
	}

	return connectionString
}

func convertPostgresWireURL(dbURL string) string {
	cleaned := removePostgresPoolParams(dbURL)
	parsedURL, err := url.Parse(cleaned)
	if err != nil {
		return cleaned
	}

	switch platform.NormalizeDialect(parsedURL.Scheme) {
	case platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		parsedURL.Scheme = platform.Postgres
		return parsedURL.String()
	default:
		return cleaned
	}
}

// convertClickHouseURL normalises a ClickHouse connection URL into the form
// expected by the clickhouse-go/v2 driver's database/sql registration.
//
// The driver accepts either the canonical `clickhouse://user:pass@host:port/db`
// URL form or the legacy DSN form. We canonicalise to the URL form and pass
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
	// The driver registers as the lowercase scheme "clickhouse". Normalise.
	parsed.Scheme = "clickhouse"
	return parsed.String()
}

func convertSQLiteURL(dbURL string) string {
	parsed, err := url.Parse(dbURL)
	if err != nil {
		return dbURL
	}
	if parsed.Scheme == "" {
		return dbURL
	}

	var dsn string
	switch {
	case parsed.Opaque != "":
		dsn = parsed.Opaque
	case parsed.Host != "" && parsed.Path != "":
		dsn = parsed.Host + parsed.Path
	case parsed.Host != "":
		dsn = parsed.Host
	case parsed.Path == "/:memory:":
		dsn = ":memory:"
	case parsed.Path != "":
		dsn = parsed.Path
	default:
		dsn = ":memory:"
	}

	query := parsed.Query()
	if !hasSQLiteForeignKeysPragma(query) {
		query.Add("_pragma", "foreign_keys(1)")
	}
	if encoded := query.Encode(); encoded != "" {
		dsn += "?" + encoded
	}
	return dsn
}

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

func hasSQLiteForeignKeysPragma(query url.Values) bool {
	for _, pragma := range query["_pragma"] {
		if strings.Contains(strings.ToLower(pragma), "foreign_keys") {
			return true
		}
	}
	return false
}

// removePostgresPoolParams removes PostgreSQL connection pool parameters from a database URL.
// These parameters (pool_max_conns and pool_min_conns) are specific to pgx driver configuration
// and may interfere with standard database connections. This function ensures compatibility
// by removing them while preserving all other query parameters.
// If the URL cannot be parsed, it returns the original URL unchanged.
func removePostgresPoolParams(dbURL string) string {
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		return dbURL
	}
	q := parsedURL.Query()
	q.Del("pool_max_conns")
	q.Del("pool_min_conns")
	parsedURL.RawQuery = q.Encode()
	return parsedURL.String()
}
