package dbschema

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"regexp"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"                   // PostgreSQL driver
	_ "github.com/microsoft/go-mssqldb"                  // SQL Server driver
	_ "github.com/tursodatabase/libsql-client-go/libsql" // libsql (Turso) driver

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/dbschema/clickhouse"
	"go.5x5.cz/ptah/internal/dbschema/mssql"
	"go.5x5.cz/ptah/internal/dbschema/mysql"
	"go.5x5.cz/ptah/internal/dbschema/postgres"
	"go.5x5.cz/ptah/internal/dbschema/sqlite"
	"go.5x5.cz/ptah/internal/schemaselection"
	"go.5x5.cz/ptah/internal/sqlrunner"
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
	parsedURL, err := parseDatabaseURL(dbURL)
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
	info, resolution, err := getDatabaseInfoWithCapabilities(
		ctx,
		db,
		dialect,
		parsedURL,
		dbURL,
	)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to get database info: %w", err)
	}
	reportCapabilityResolution(info, resolution)

	var newReader schemaReaderFactory
	var newWriter schemaWriterFactory
	switch dialectProtocol {
	case "pgx":
		newReader = func(runner sqlrunner.Runner) types.SchemaReader {
			return postgres.NewPostgreSQLReaderWithCapabilities(runner, info.Schema, info.Capabilities)
		}
		newWriter = func(runner sqlrunner.Runner, _ *sql.Conn) types.SchemaWriter {
			// The set decides whether cleanup may take a transaction: Spanner
			// refuses DDL inside one, and the writer cannot ask the server
			// without opening the transaction it is trying to avoid.
			return postgres.NewPostgreSQLWriterForRunnerWithCapabilities(
				runner, info.Schema, info.Capabilities)
		}
	case "mysql":
		newReader = func(runner sqlrunner.Runner) types.SchemaReader {
			// The set decides which spelling of the CHECK_CONSTRAINTS read is
			// asked first. MySQL's view has no TABLE_NAME -- that column is a
			// MariaDB extension -- so without this every MySQL schema read
			// spent a round trip being told error 1054 (stokaro/ptah#916).
			return mysql.NewMySQLReaderWithCapabilities(runner, info.Schema, info.Capabilities)
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
			// Capabilities and version travel with the reader so that roles
			// and grants are read only where RoleManagement says they exist,
			// and so the diagnostic can name the server it read. Without them
			// a live ClickHouse would describe no RBAC at all and every
			// declared role would be planned again on every run
			// (stokaro/ptah#1025).
			return clickhouse.NewClickHouseReaderWithCapabilities(
				runner, info.Schema, info.Version, info.Capabilities,
			)
		}
		newWriter = func(runner sqlrunner.Runner, _ *sql.Conn) types.SchemaWriter {
			// The capabilities travel with the writer for the same reason
			// they travel with the reader above: the realm-cleanup gate is a
			// capability question, and resolving it here means the banner is
			// read once (stokaro/ptah#916).
			return clickhouse.NewClickHouseWriterForRunnerWithCapabilities(
				runner, info.Schema, info.Capabilities,
			)
		}
	// libsql reaches a remote server, but the schema it serves is SQLite's, so
	// it takes SQLite's reader, writer and executor unchanged.
	case "sqlite", "libsql":
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
		remoteLibSQL:   dialectProtocol == "libsql",
	}, nil
}

// parseDatabaseURL parses a database URL into the form the rest of the
// connection path reads.
//
// Two shapes are not URLs and have to be rewritten before net/url sees them.
//
// The MySQL family carries a NETWORK where a URL carries a host:
// user:pass@tcp(127.0.0.1:3306)/db and user:pass@unix(/tmp/mysql.sock)/db are
// one grammar with two networks, and neither is an authority. The network and
// its address are dropped so what remains parses, and the parts the caller
// reads survive it. It is not only the scheme that is read from the result --
// getDatabaseInfo takes the MySQL database name from the path -- so recognizing
// only tcp( left a socket address parsed as a host called "unix(" with the
// socket path folded into the database name.
//
// A Windows absolute path is not an authority either. sqlite://C:\dir\app.db
// makes net/url read the drive letter's colon as a port separator and refuse
// the whole URL, which is why every command that provisions a local database
// failed on Windows: 1014 tests across more than thirty packages, 1382 of them
// reporting "invalid database URL". The path is carried as opaque instead,
// which is the shape convertSQLiteURL already reads first.
func parseDatabaseURL(dbURL string) (*url.URL, error) {
	if strings.HasPrefix(dbURL, "mysql://") || strings.HasPrefix(dbURL, "mariadb://") {
		if rewritten, ok := withoutMySQLNetwork(dbURL); ok {
			return url.Parse(rewritten)
		}
	}

	// The Windows rule lives in atlasurl, which is where the other database-URL
	// parser already knew it. What each parser does about a MySQL address is
	// its own and stays so: that one keeps the host because it compares
	// endpoints, this one drops it because getDatabaseInfo reads the database
	// name from the path.
	return atlasurl.Parse(dbURL)
}

// mysqlNetworkMarkers are the spellings by which a MySQL-family address
// introduces go-sql-driver's own network wrapper.
//
// Credentials are optional in that grammar, so tcp(host:port)/db and
// unix(/path)/db are valid targets on their own and follow the scheme directly
// rather than an "@". tcp and unix are two spellings of one thing to the
// driver -- the network and the address to reach it on -- so recognizing one
// without the other reads a socket address as a host called "unix(" with the
// path folded into the database name.
//
// There is one list because the parser and the converter have to recognize
// exactly the same set, and [ConnectToDatabase] calls them in sequence.
// stokaro/ptah#1540 is what a second list costs: the parser accepted both
// credential-free targets while the converter recognized neither, so a valid
// URL reached the driver either with its scheme still attached or rebuilt
// around a host that was never one.
var mysqlNetworkMarkers = []string{"@tcp(", "@unix(", "://tcp(", "://unix("}

// mysqlNetworkMarker returns the marker a MySQL-family address carries and the
// index it starts at, reporting false when the address names no network
// wrapper at all.
func mysqlNetworkMarker(dbURL string) (marker string, start int, ok bool) {
	for _, candidate := range mysqlNetworkMarkers {
		if index := strings.Index(dbURL, candidate); index >= 0 {
			return candidate, index, true
		}
	}
	return "", 0, false
}

// withoutMySQLNetwork removes the network wrapper from a MySQL-family address,
// reporting whether one was there to remove.
func withoutMySQLNetwork(dbURL string) (string, bool) {
	marker, start, ok := mysqlNetworkMarker(dbURL)
	if !ok {
		return dbURL, false
	}
	end := strings.Index(dbURL[start:], ")")
	if end < 0 {
		return dbURL, false
	}
	// Keep everything up to and including the separator the network followed
	// -- "@" or "://" -- and drop the network with its address.
	keep := start + strings.Index(marker, "(")
	return dbURL[:keep] + dbURL[start+end+1:], true
}

func getDatabaseInfoWithCapabilities(
	ctx context.Context,
	db *sql.DB,
	dialect string,
	parsedURL *url.URL,
	dbURL string,
) (types.DBInfo, capability.VersionResolution, error) {
	info, err := getDatabaseInfo(ctx, db, dialect, parsedURL, dbURL)
	if err != nil {
		return types.DBInfo{}, capability.VersionResolution{}, err
	}
	resolution := resolveDatabaseCapabilities(info)
	info.Capabilities = resolution.Capabilities
	return info, resolution, nil
}

// reportCapabilityResolution records how the live server version was mapped
// onto a capability preset, in the two cases where the mapping is not a plain
// match: the version could not be parsed at all, or it parsed and ran off the
// top of its dialect's ladder.
//
// All stay at DEBUG, and that level is the point. The default logger
// cmd/internal/cliobs installs keeps WARN and above precisely because a clean
// run against a supported server emits nothing there; anything it does emit is
// a diagnostic that exists nowhere else. Connecting to a server Ptah supports
// and runs in CI is not such an event. A saturated resolution fires on every
// connection to an unmeasured newer major, so reporting it at WARN writes a
// line to stderr on every command and breaks the clean error-stream contract.
//
// The fact itself is not lost. capability.ResolveServerVersion returns
// Saturated and NewestMeasured to any caller that wants to act on them, and
// `--log-level debug` prints these lines. Surfacing an unrefined version to
// the user on a channel of its own is criterion 6 of issue #916 and belongs
// with the CLI work that owns that channel.
func reportCapabilityResolution(info types.DBInfo, resolution capability.VersionResolution) {
	if resolution.Saturated {
		slog.Debug(
			"server is newer than the newest measured capability line; planning with that line's preset",
			"dialect", info.Dialect,
			"version", info.Version,
			"newest_measured", resolution.NewestMeasured,
		)
		return
	}
	if !resolution.VersionSpecific {
		slog.Debug(
			"falling back from an unmeasured server version",
			"dialect", info.Dialect,
			"version", info.Version,
		)
	}
}

func resolveDatabaseCapabilities(info types.DBInfo) capability.VersionResolution {
	// Root metadata must describe the conservative server-version baseline.
	// Session variables can differ between pooled physical connections, so
	// session-specific relaxations are detected only after WithSession pins the
	// connection that will plan and execute the statements.
	//
	// The resolution carries more than the preset: a server newer than the
	// newest measured version line resolves saturated and not
	// version-specific, which is what ConnectToDatabase records.
	return capability.ResolveServerVersion(info.Dialect, info.Version)
}

// libSQLDriverConfig maps the two libsql spellings onto the transport the
// pinned community binary v1.3.0 uses for each, measured against a live
// libsql-server v0.24.32:
//
//	libsql://host     -> POST https://host/v2/pipeline
//	libsql+ws://host  -> a WebSocket dial
//
// The client library spells the second `ws://`, and takes `libsql://` as it is.
// Anything else is not a libsql URL and is left to the local SQLite driver.
func libSQLDriverConfig(dbURL string) (driverName, dataSourceName string, remote bool) {
	switch {
	case strings.HasPrefix(strings.ToLower(dbURL), "libsql+ws://"):
		return "libsql", "ws://" + dbURL[len("libsql+ws://"):], true
	case strings.HasPrefix(strings.ToLower(dbURL), "libsql://"):
		return "libsql", dbURL, true
	}
	return "", "", false
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
		// libsql shares SQLite's dialect but not its connection: it is a remote
		// protocol, so the dialect alone cannot pick the driver.
		if libSQLDriver, libSQLDSN, remote := libSQLDriverConfig(dbURL); remote {
			return libSQLDriver, libSQLDSN
		}
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
	// remoteLibSQL marks a connection reached over the libsql WebSocket
	// transport, whose teardown reports an error that is not one.
	remoteLibSQL bool
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

// WithExecutor returns a shallow connection copy that uses executor as the
// active SQL executor. When executor exposes a live transaction runner, the
// copy's reader and direct SQL use that same execution session.
//
// This is used to pass transaction-scoped writers into migration callbacks
// without storing the active transaction on the root writer.
func (dc *DatabaseConnection) WithExecutor(executor types.SchemaExecutor) *DatabaseConnection {
	cloned := *dc
	cloned.executor = executor
	provider, ok := executor.(schemaTransactionRunnerProvider)
	if !ok {
		return &cloned
	}
	runner := provider.SchemaQueryRunner()
	if runner == nil {
		return &cloned
	}
	cloned.runner = runner
	if dc.newReader != nil {
		cloned.reader = dc.newReader(runner)
	}
	return &cloned
}

type schemaTransactionRunnerProvider interface {
	SchemaQueryRunner() sqlrunner.Runner
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
// leak to another pool user. In-memory SQLite is the sole exception: its only
// connection owns the database lifetime, so it is returned to the pool.
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
		if dc.inMemorySQLite {
			resultErr = errors.Join(resultErr, closeSQLConnection(session, "in-memory SQLite database session"))
			return
		}
		resultErr = errors.Join(resultErr, discardSQLConnection(session, "pinned database session"))
	}()

	runner := sqlrunner.NewConn(ctx, session)
	baselineCapabilities := capability.ForServerVersion(dc.info.Dialect, dc.info.Version)
	sessionCapabilities, err := refineMySQLForeignKeyCapabilities(
		dc.info.Dialect,
		baselineCapabilities,
		baselineCapabilities,
		func(destination ...any) error {
			return session.QueryRowContext(
				ctx,
				"SELECT @@SESSION.restrict_fk_on_non_standard_key",
			).Scan(destination...)
		},
	)
	if err != nil {
		return fmt.Errorf("detect pinned-session foreign-key reference policy: %w", err)
	}
	scoped := *dc
	scoped.info.Capabilities = sessionCapabilities
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

// WithSessionOrCurrent calls use with one physical database session. Pool-backed
// connections pin a new session and discard it afterward; already pinned
// connections reuse the current session and leave its lifecycle with the caller.
func (dc *DatabaseConnection) WithSessionOrCurrent(
	ctx context.Context,
	use func(*DatabaseConnection) error,
) error {
	if dc == nil || dc.db == nil {
		return fmt.Errorf("database session requires an open database connection")
	}
	if use == nil {
		return fmt.Errorf("database session callback is nil")
	}
	if dc.pinned {
		return use(dc)
	}
	return dc.WithSession(ctx, use)
}

func refineMySQLForeignKeyCapabilities(
	dialect string,
	caps,
	baseline capability.Capabilities,
	scan func(...any) error,
) (capability.Capabilities, error) {
	if platform.NormalizeDialect(dialect) != platform.MySQL ||
		!baseline.Has(capability.ForeignKeysRequireUniqueReference) {
		return caps, nil
	}

	var restrictNonstandardKey int64
	if err := scan(&restrictNonstandardKey); err != nil {
		return nil, fmt.Errorf("query restrict_fk_on_non_standard_key: %w", err)
	}
	if restrictNonstandardKey != 0 {
		return caps.
			With(capability.ForeignKeysRequireIndexedReference, false).
			With(capability.ForeignKeysRequireUniqueReference, true), nil
	}

	return caps.
		With(capability.ForeignKeysRequireUniqueReference, false).
		With(capability.ForeignKeysRequireIndexedReference, true), nil
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
	if dc.executor != nil {
		if err := dc.executor.ExecuteSQL(context.Background(), query, args...); err != nil {
			return nil, err
		}
		return driver.RowsAffected(0), nil
	}
	return dc.sqlRunner().Exec(query, args...)
}

// ExecContext executes a query without returning any rows using a context
func (dc *DatabaseConnection) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if executor, ok := dc.executor.(contextExecutor); ok {
		return executor.ExecContext(ctx, query, args...)
	}
	if dc.executor != nil {
		if err := dc.executor.ExecuteSQL(ctx, query, args...); err != nil {
			return nil, err
		}
		return driver.RowsAffected(0), nil
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
	if dc.db == nil {
		return nil
	}
	err := dc.db.Close()
	if libSQLTeardownIsBenign(dc.remoteLibSQL, err) {
		// Measured against libsql-server v0.24.32: EVERY libsql WebSocket
		// connection that ran a statement reports `failed to close WebSocket:
		// failed to read frame header: EOF` on Close, and one that ran none
		// closes cleanly. The server hangs up first, so the client's read of
		// the close frame returns io.EOF. Reporting it would put a warning on
		// stderr after every successful command, which the pinned community
		// binary does not, and which would be the only difference between the
		// two on this transport (stokaro/ptah#1615).
		//
		// Only io.EOF is swallowed, and only here: a genuine close failure on
		// this transport still reaches the caller.
		return nil
	}
	return err
}

// libSQLTeardownIsBenign reports the one close error this transport always
// produces and which is not a failure.
func libSQLTeardownIsBenign(remoteLibSQL bool, err error) bool {
	return remoteLibSQL && errors.Is(err, io.EOF)
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

		// The schema is whatever the server resolves this session's search_path
		// to, which is what a `?search_path=` on the URL selects. It used to be
		// the constant "public", with a branch on the URL path that assigned
		// "public" again, so a dev URL naming another schema was not merely
		// ignored -- the writer treated that schema as a stranger's and DROPPED
		// it while cleaning the database realm, then replayed with a search_path
		// resolving to nothing (stokaro/ptah#1198).
		//
		// Why a selection that resolves to nothing is refused rather than folded
		// back to "public" is documented on [schemaselection.Selection.Resolve],
		// next to the code that decides it.
		schema, err := schemaselection.FromParsedURL(parsedURL).Resolve(ctx, db)
		if err != nil {
			return info, err
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
		// A MySQL-family schema is a database, so no static dialect rule can
		// name the one that owns an unqualified table the way "public" and
		// "main" do; only the connection knows it. Leaving the field empty is
		// what made the two comparison sides key differently: the catalog
		// reports every table with no schema, while a desired state written as
		// Atlas HCL carries `schema = schema.<database>`. Measured on live
		// MySQL 9.7.1, `schema apply` fed the database's own inspected HCL
		// planned CREATE TABLE and DROP TABLE for every table where the pinned
		// Atlas community v1.3.0 binary answered "Schema is synced, no changes
		// to be made" (stokaro/ptah#1244). SQL Server pins the same field from
		// the same place, in getSQLServerDatabaseInfo.
		info.IdentifierSemantics.DefaultSchema = info.Schema
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

// detectPostgresWireDialect names the product behind a PostgreSQL-wire
// connection from its own SELECT version() banner.
//
// The product tokens come from capability.BannerPlatform rather than a second
// copy of the same substrings, because this function and
// capability.ResolveServerVersion must not disagree about which server a
// banner describes: this one picks the dialect, that one picks the preset the
// dialect is planned with, and they are handed the same string.
// TestWireDialectDetectionAgreesWithTheCapabilityResolver holds them together.
//
// A banner naming only PostgreSQL is not a product answer here. CockroachDB,
// YugabyteDB and Spanner all speak this protocol and a deployment of any of
// them may report a banner carrying no token of its own, so the dialect the
// operator connected with survives — it is the only evidence left.
func detectPostgresWireDialect(declaredDialect, version string) string {
	switch banner := capability.BannerPlatform(version); banner {
	case platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		return banner
	default:
		return platform.NormalizeDialect(declaredDialect)
	}
}

// detectMySQLWireDialect is detectPostgresWireDialect's MySQL-wire twin, and
// reads the same table for the same reason. MariaDB is the one product that
// announces itself over this protocol.
func detectMySQLWireDialect(declaredDialect, version string) string {
	if capability.BannerPlatform(version) == platform.MariaDB {
		return platform.MariaDB
	}
	return platform.NormalizeDialect(declaredDialect)
}

// convertMySQLURL converts a MySQL/MariaDB URL from standard format to Go driver format
func convertMySQLURL(dbURL string) string {
	// Already in the driver's own form, so it is returned with only the scheme
	// removed. The recognition is [mysqlNetworkMarker]'s, which is the same one
	// parseDatabaseURL uses -- see the note there for why it cannot be a second
	// list that happens to agree.
	if _, _, ok := mysqlNetworkMarker(dbURL); ok {
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
	// Through parseDatabaseURL rather than url.Parse: a Windows absolute path
	// is refused by the latter, and returning the URL whole then handed the
	// driver a string beginning with sqlite:// as though it were a filename.
	parsed, err := parseDatabaseURL(dbURL)
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
	dsn = sqliteURIForMemory(dsn, query)
	if encoded := query.Encode(); encoded != "" {
		dsn += "?" + encoded
	}
	return dsn
}

// sqliteURIForMemory prefixes a named in-memory database with `file:`, which is
// what makes SQLite read the query as URI parameters at all.
//
// Without it `mode=memory` is not ignored so much as never seen: the whole
// string is taken as a filename, so `sqlite://dev?mode=memory` -- the form this
// repository's own tests, help text and docs use -- opens a FILE named
// `dev?mode=memory` in the working directory and leaves it there.
//
// Measured against modernc.org/sqlite v1.57.0:
//
//	dev?mode=memory       -> a file appears
//	file:dev?mode=memory  -> no file
//
// The second problem is the worse one. A dev database exists to be empty, and a
// persistent one carries the previous run's state into the next plan -- which
// produces a wrong answer that looks like a right one (stokaro/ptah#1819).
//
// Only the memory case is rewritten. A plain path stays a plain path, because
// `file:` there would change how a name containing `?` or `#` is read.
func sqliteURIForMemory(dsn string, query url.Values) string {
	if !strings.EqualFold(query.Get("mode"), "memory") {
		return dsn
	}
	if strings.HasPrefix(dsn, "file:") || dsn == ":memory:" {
		return dsn
	}
	return "file:" + dsn
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
