package dbschema

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"regexp"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"  // PostgreSQL driver
	_ "github.com/microsoft/go-mssqldb" // SQL Server driver

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/core/platform/identifier"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/dbschema/clickhouse"
	"github.com/stokaro/ptah/internal/dbschema/mssql"
	"github.com/stokaro/ptah/internal/dbschema/mysql"
	"github.com/stokaro/ptah/internal/dbschema/postgres"
	"github.com/stokaro/ptah/internal/dbschema/sqlite"
	"github.com/stokaro/ptah/internal/sqlrunner"
)

// ConnectToDatabase creates a database connection from a URL.
//
// The provided context governs the initial Ping used to verify the connection
// and the metadata queries issued to populate [DBInfo]. Canceling the context
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
	inMemorySQLite := dialectProtocol == "sqlite" && isSQLiteMemoryDSN(connectionString)
	if inMemorySQLite {
		db.SetMaxOpenConns(1)
	}

	// Test the connection — honor the caller-supplied context so a stuck or
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

	var newReader schemaReaderFactory
	var newWriter schemaWriterFactory
	switch dialectProtocol {
	case "pgx":
		newReader = func(runner sqlrunner.Runner) types.SchemaReader {
			return postgres.NewPostgreSQLReaderWithCapabilities(runner, info.Schema, info.Capabilities)
		}
		newWriter = func(runner sqlrunner.Runner, _ *sql.Conn) types.SchemaWriter {
			return postgres.NewPostgreSQLWriterForRunner(runner, info.Schema)
		}
	case "mysql":
		newReader = func(runner sqlrunner.Runner) types.SchemaReader {
			return mysql.NewMySQLReader(runner, info.Schema)
		}
		newWriter = func(runner sqlrunner.Runner, session *sql.Conn) types.SchemaWriter {
			if session != nil {
				return mysql.NewMySQLWriterForPinnedRunner(
					runner,
					db,
					session,
					info.Schema,
					info.Dialect,
					info.Version,
				)
			}
			return mysql.NewMySQLWriterForRunner(runner, db, info.Schema, info.Dialect, info.Version)
		}
	case "clickhouse":
		newReader = func(runner sqlrunner.Runner) types.SchemaReader {
			return clickhouse.NewClickHouseReader(runner, info.Schema)
		}
		newWriter = func(runner sqlrunner.Runner, _ *sql.Conn) types.SchemaWriter {
			return clickhouse.NewClickHouseWriterForRunner(runner, info.Schema)
		}
	case "sqlite":
		newReader = func(runner sqlrunner.Runner) types.SchemaReader {
			return sqlite.NewSQLiteReader(runner, info.Schema)
		}
		newWriter = func(runner sqlrunner.Runner, session *sql.Conn) types.SchemaWriter {
			if session != nil {
				return sqlite.NewSQLiteWriterForPinnedRunner(runner, session, info.Schema)
			}
			return sqlite.NewSQLiteWriterForRunner(runner, info.Schema)
		}
	case "sqlserver":
		newReader = func(runner sqlrunner.Runner) types.SchemaReader {
			return mssql.NewSQLServerReader(runner, info.Schema)
		}
		newWriter = func(runner sqlrunner.Runner, _ *sql.Conn) types.SchemaWriter {
			return mssql.NewSQLServerWriterForRunner(runner, info.Schema)
		}
	default:
		_ = db.Close()
		return nil, fmt.Errorf("no schema reader available for dialect: %s", dialect)
	}

	runner := sqlrunner.Runner(db)
	return &DatabaseConnection{
		db:             db,
		runner:         runner,
		info:           info,
		reader:         newReader(runner),
		writer:         newWriter(runner, nil),
		newReader:      newReader,
		newWriter:      newWriter,
		inMemorySQLite: inMemorySQLite,
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
	db             *sql.DB
	runner         sqlrunner.Runner
	info           types.DBInfo
	reader         types.SchemaReader
	writer         types.SchemaWriter
	executor       types.SchemaExecutor
	newReader      schemaReaderFactory
	newWriter      schemaWriterFactory
	pinned         bool
	inMemorySQLite bool
	// session is the pinned physical session set by WithSession. Connection
	// state that only applies per physical connection — see
	// WithUntrustedSQLSession — needs it.
	session *sql.Conn
}

type schemaReaderFactory func(sqlrunner.Runner) types.SchemaReader
type schemaWriterFactory func(sqlrunner.Runner, *sql.Conn) types.SchemaWriter

// IsolatedQueryer is the query-only surface exposed inside an isolated physical
// database session. It deliberately omits transaction control and schema
// writers so the callback cannot commit or acquire an auxiliary connection
// through Ptah.
type IsolatedQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type queryContextRunner interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type isolatedQueryer struct {
	runner queryContextRunner
}

func (q isolatedQueryer) QueryContext(
	ctx context.Context,
	query string,
	args ...any,
) (*sql.Rows, error) {
	return q.runner.QueryContext(ctx, query, args...)
}

type schemaScopedReader interface {
	SetSchemas([]string)
}

type contextExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
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
	info.IdentifierSemantics = info.IdentifierSemantics.Clone()
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

// SchemaWriter returns the schema writer bound to this connection's SQL
// session. Transaction-scoped executors do not replace the administrative
// writer.
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

// WithIsolatedQuerySession runs use with a query-only handle on one physical
// database session. On transaction-capable drivers the queries run in a
// transaction that is always rolled back; on ClickHouse, whose database/sql
// driver does not implement transactions, they run directly on the disposable
// session. The physical session is discarded afterward so session-level state
// cannot leak back into the pool. In-memory SQLite is the sole exception: its
// only connection owns the database lifetime, so Ptah rolls back and returns it
// to the pool. The callback cannot control a transaction or reach Ptah schema
// writers. opts is passed to database/sql unchanged when a transaction is
// available. This method provides lifecycle isolation, not SQL read-only
// validation: callers must restrict statements themselves.
func (dc *DatabaseConnection) WithIsolatedQuerySession(
	ctx context.Context,
	opts *sql.TxOptions,
	use func(IsolatedQueryer) error,
) (resultErr error) {
	if dc == nil || dc.db == nil {
		return fmt.Errorf("isolated query session requires an open database connection")
	}
	if use == nil {
		return fmt.Errorf("isolated query session callback is nil")
	}
	if dc.pinned {
		return fmt.Errorf("database connection is already pinned to a session")
	}

	session, err := dc.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("pin isolated query session: %w", err)
	}
	defer func() {
		if dc.inMemorySQLite {
			resultErr = errors.Join(resultErr, closeSQLConnection(session, "in-memory SQLite query session"))
			return
		}
		resultErr = errors.Join(resultErr, discardSQLConnection(session, "isolated query session"))
	}()
	if platform.NormalizeDialect(dc.info.Dialect) == platform.ClickHouse {
		return use(isolatedQueryer{runner: session})
	}

	tx, err := session.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("begin isolated query transaction: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, fmt.Errorf("roll back transaction: %w", rollbackErr))
		}
	}()

	return use(isolatedQueryer{runner: tx})
}

// WithSession pins one physical database session for the callback and rebuilds
// the dialect reader and writer on that same session. The scoped connection
// does not own the pool and must not escape the callback. The physical
// connection is discarded afterward so callback-created session state cannot
// leak to another pool user.
func (dc *DatabaseConnection) WithSession(
	ctx context.Context,
	use func(*DatabaseConnection) error,
) (resultErr error) {
	if dc == nil || dc.db == nil {
		return fmt.Errorf("database session requires an open database connection")
	}
	if use == nil {
		return fmt.Errorf("database session callback is nil")
	}
	if dc.pinned {
		return fmt.Errorf("database connection is already pinned to a session")
	}
	if dc.newReader == nil || dc.newWriter == nil {
		return fmt.Errorf("database connection does not support session rebinding")
	}

	session, err := dc.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("pin database session: %w", err)
	}
	defer func() {
		resultErr = errors.Join(resultErr, discardSQLConnection(session, "pinned database session"))
	}()

	runner := sqlrunner.NewConn(ctx, session)
	scoped := *dc
	scoped.runner = runner
	scoped.reader = dc.newReader(runner)
	scoped.writer = dc.newWriter(runner, session)
	scoped.executor = nil
	scoped.pinned = true
	scoped.session = session
	if dc.writer != nil && dc.writer.IsDryRun() {
		scoped.writer.SetDryRun(true)
	}
	return use(&scoped)
}

func discardSQLConnection(session *sql.Conn, label string) error {
	var resultErr error
	discardErr := session.Raw(func(any) error {
		return driver.ErrBadConn
	})
	if discardErr != nil && !errors.Is(discardErr, driver.ErrBadConn) {
		resultErr = errors.Join(resultErr, fmt.Errorf("discard %s: %w", label, discardErr))
	}
	resultErr = errors.Join(resultErr, closeSQLConnection(session, label))
	return resultErr
}

func closeSQLConnection(session *sql.Conn, label string) error {
	if err := session.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
		return fmt.Errorf("close %s: %w", label, err)
	}
	return nil
}

// Query executes a query and returns the result rows
func (dc *DatabaseConnection) Query(query string, args ...any) (*sql.Rows, error) {
	return dc.sqlRunner().Query(query, args...)
}

// QueryContext executes a query using a context and returns the result rows.
func (dc *DatabaseConnection) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return dc.sqlRunner().QueryContext(ctx, query, args...)
}

// QueryRow executes a query that returns a single row
func (dc *DatabaseConnection) QueryRow(query string, args ...any) *sql.Row {
	return dc.sqlRunner().QueryRow(query, args...)
}

// QueryRowContext executes a query that returns a single row using a context
func (dc *DatabaseConnection) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return dc.sqlRunner().QueryRowContext(ctx, query, args...)
}

// Exec executes a query without returning any rows
func (dc *DatabaseConnection) Exec(query string, args ...any) (sql.Result, error) {
	if executor, ok := dc.executor.(contextExecutor); ok {
		return executor.ExecContext(context.Background(), query, args...)
	}
	return dc.sqlRunner().Exec(query, args...)
}

// ExecContext executes a query without returning any rows using a context
func (dc *DatabaseConnection) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if executor, ok := dc.executor.(contextExecutor); ok {
		return executor.ExecContext(ctx, query, args...)
	}
	return dc.sqlRunner().ExecContext(ctx, query, args...)
}

// WithUntrustedSQLSession pins a session that will execute SQL the caller does
// not trust, applying and verifying every engine-level restriction the dialect
// supports before the callback runs.
//
// Use it instead of [DatabaseConnection.WithSession] whenever the statements
// come from somewhere outside the operator's own project — a plan file
// received from another tool, for example. Because the restrictions are
// properties of the physical connection, applying them anywhere other than the
// pinned session would silently protect nothing; taking the session and the
// restrictions in one step is what makes that mistake unrepresentable.
//
// What the restrictions buy depends on the dialect. On SQLite the engine
// refuses ATTACH, DETACH, and VACUUM INTO, so the callback's SQL cannot reach
// another database file or write a database copy to an arbitrary path, and
// native extensions cannot be loaded; the restriction is verified to be in
// force before the callback runs. Storage-directory pragmas are not covered.
// Other dialects have no equivalent session-level control, so the callback
// runs unrestricted — on those, only the caller's own review of the SQL and
// the disposability of the database stand between it and the statements.
func (dc *DatabaseConnection) WithUntrustedSQLSession(
	ctx context.Context,
	use func(*DatabaseConnection) error,
) error {
	if dc == nil {
		return fmt.Errorf("untrusted SQL session requires an open database connection")
	}
	// WithSession's own nil check never sees the caller's callback, because
	// the one handed to it below is this method's closure.
	if use == nil {
		return fmt.Errorf("database session callback is nil")
	}
	return dc.WithSession(ctx, func(session *DatabaseConnection) error {
		if err := session.restrictUntrustedSQL(ctx); err != nil {
			return err
		}
		return use(session)
	})
}

// restrictUntrustedSQL applies the engine-level restrictions available for the
// pinned session's dialect. Dialects without such controls are a no-op, which
// WithUntrustedSQLSession documents.
func (dc *DatabaseConnection) restrictUntrustedSQL(ctx context.Context) error {
	if !dc.pinned || dc.session == nil {
		return fmt.Errorf("untrusted SQL restrictions require a pinned session")
	}
	if platform.NormalizeDialect(dc.info.Dialect) != platform.SQLite {
		return nil
	}
	return sqlite.RestrictSession(ctx, dc.session)
}

// Conn returns a dedicated database session. Callers that use session-scoped
// database features must close the returned connection when finished.
func (dc *DatabaseConnection) Conn(ctx context.Context) (*sql.Conn, error) {
	if dc.pinned {
		return nil, fmt.Errorf("database connection is already pinned to a session")
	}
	return dc.db.Conn(ctx)
}

// Close closes the database connection
func (dc *DatabaseConnection) Close() error {
	if dc.pinned {
		return nil
	}
	if dc.db != nil {
		return dc.db.Close()
	}
	return nil
}

func (dc *DatabaseConnection) sqlRunner() sqlrunner.Runner {
	if dc.runner != nil {
		return dc.runner
	}
	return dc.db
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
		Dialect:             dialect,
		URL:                 originalURL,
		IdentifierSemantics: identifier.ForDialect(dialect),
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
		info.IdentifierSemantics = identifier.ForDialect(info.Dialect)

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
		info.Dialect = detectMySQLWireDialect(dialect, version)
		info.IdentifierSemantics = identifier.ForDialect(info.Dialect)

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
		return getSQLServerDatabaseInfo(ctx, db, parsedURL, info)
	}

	return info, nil
}

func getSQLServerDatabaseInfo(
	ctx context.Context,
	db *sql.DB,
	parsedURL *url.URL,
	info types.DBInfo,
) (types.DBInfo, error) {
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
	if schema := parsedURL.Query().Get("schema"); schema != "" {
		info.Schema = schema
	}
	info.IdentifierSemantics = identifier.ForSQLServerCatalog(catalogCollation)
	info.IdentifierSemantics.DefaultSchema = info.Schema
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

func detectMySQLWireDialect(declaredDialect, version string) string {
	if strings.Contains(strings.ToLower(version), "mariadb") {
		return platform.MariaDB
	}
	return platform.NormalizeDialect(declaredDialect)
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

func isSQLiteMemoryDSN(dsn string) bool {
	path, rawQuery, _ := strings.Cut(dsn, "?")
	if path == "" || path == ":memory:" {
		return true
	}
	if strings.EqualFold(path, "file::memory:") {
		return true
	}
	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return false
	}
	return strings.EqualFold(query.Get("mode"), "memory")
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
