package dbschema

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/connectgate"
	"ptah.run/internal/connector"
	"ptah.run/internal/dburldisplay"
	"ptah.run/internal/servertarget"
	"ptah.run/internal/sqlrunner"
)

// ConnectToDatabase creates a database connection from a URL.
//
// The URL scheme selects the dialect: every spelling platform.NormalizeDialect
// resolves that URL syntax can carry as a scheme is accepted -- the canonical
// names and the aliases that fold onto them, barring the underscore spellings,
// which a scheme cannot hold -- and a scheme it does not know is refused with
// an error rather than guessed at. Within SQLite's dialect the URL also
// selects the transport: libsql:// and libsql+ws:// reach a remote libsql
// (Turso) server, while any other sqlite URL opens a local file or an
// in-memory database. An in-memory SQLite database has no existence apart from
// its connection: every caller of the returned *DatabaseConnection shares one
// session, and closing it discards the database.
//
// The provided context governs the initial Ping used to verify the connection
// and the metadata queries issued to populate [catalog.ServerInfo]. Canceling
// the context before or during the call causes ConnectToDatabase to return
// promptly with the context error wrapped in a descriptive message. The
// context does not affect the lifetime of the returned *DatabaseConnection;
// callers are responsible for closing it.
//
// A MySQL or MariaDB URL that names no database is refused: a caller written
// for one database would otherwise be handed a whole server. [ConnectToServer]
// is the connector that accepts one.
func ConnectToDatabase(ctx context.Context, dbURL string) (*DatabaseConnection, error) {
	return connect(ctx, dbURL, false)
}

// ConnectToServer is [ConnectToDatabase] for a caller that also handles a
// whole MySQL or MariaDB server. A URL of either engine that names no database
// -- `mysql://user@host:3306`, `mysql://user@host:3306/`, or a `+unix` socket
// URL without `database=` -- opens a session that selects no database. Its
// [catalog.ServerInfo.Schema] is empty, and a read of it describes every user
// database, each object under the database it is in; see
// [ReadSchemaWithSchemasContext] for reading some of them. Every other URL
// connects exactly as ConnectToDatabase connects it.
//
// The pinned Atlas community binary v1.3.0 reads such a URL the same way
// (stokaro/ptah#3789).
func ConnectToServer(ctx context.Context, dbURL string) (*DatabaseConnection, error) {
	return connect(ctx, dbURL, true)
}

// admitted is the request a URL connects with, and the service of its
// dialect, once the caller's context and the dialect's scope both accept it.
// A surface that refuses a dialect refuses it through the context, for every
// path a URL can take to a connection; see internal/connectgate. wholeServer
// is true for a caller that also handles a whole server.
func admitted(
	ctx context.Context,
	dbURL string,
	parsedURL *url.URL,
	wholeServer bool,
) (connector.Request, connector.Service, error) {
	dialect, err := connectionDialect(parsedURL)
	if err != nil {
		return connector.Request{}, nil, err
	}
	if err := connectgate.Check(ctx, dialect); err != nil {
		return connector.Request{}, nil, err
	}
	described, _ := platform.Lookup(dialect)
	service, connects := services().Lookup(described.Name)
	if !connects || described.Driver == "" {
		// A dialect Ptah renders and plans for, and does not connect to yet.
		// Without this the empty driver name reaches sql.Open, which answers
		// `sql: unknown driver "" (forgotten import?)` -- a message about this
		// package's imports rather than about what the operator asked for.
		// No dialect reaches this today; it is the arm the next one lands in.
		return connector.Request{}, nil, fmt.Errorf(
			"connecting to %s is not supported yet: Ptah renders and plans %s schemas, "+
				"and reading a live %s catalog is not implemented", dialect, dialect, dialect)
	}
	if err := service.CheckScope(parsedURL, wholeServer); err != nil {
		return connector.Request{}, nil, err
	}
	return connector.Request{Dialect: described, URL: dbURL, Parsed: parsedURL}, service, nil
}

// connect opens dbURL once its dialect's scope accepts it for a caller that
// does, or does not, handle a whole server.
func connect(ctx context.Context, dbURL string, wholeServer bool) (*DatabaseConnection, error) {
	parsedURL, err := parseDatabaseURL(dbURL)
	if err != nil {
		return nil, fmt.Errorf("invalid database URL: %w", err)
	}

	request, service, err := admitted(ctx, dbURL, parsedURL, wholeServer)
	if err != nil {
		return nil, err
	}

	// The context governs the ping that verifies the pool, so a stuck or slow
	// host cannot block ConnectToDatabase indefinitely.
	pool, err := service.Open(ctx, request)
	if err != nil {
		return nil, err
	}

	info, resolution, err := getDatabaseInfoWithCapabilities(ctx, service, pool, request)
	if err != nil {
		_ = pool.DB.Close()
		return nil, fmt.Errorf("failed to get database info: %w", err)
	}
	reportCapabilityResolution(info, resolution)

	newReader := func(runner sqlrunner.Runner) catalog.SchemaReader {
		return service.NewReader(pool, info, runner)
	}
	newWriter := func(runner sqlrunner.Runner, session *sql.Conn) catalog.SchemaWriter {
		return service.NewWriter(pool, info, runner, session)
	}
	runner := sqlrunner.Runner(pool.DB)
	return &DatabaseConnection{
		db:            pool.DB,
		runner:        runner,
		info:          info,
		reader:        newReader(runner),
		writer:        newWriter(runner, nil),
		newReader:     newReader,
		newWriter:     newWriter,
		service:       service,
		singleSession: pool.SingleSession,
		benignClose:   pool.BenignClose,
	}, nil
}

// parseDatabaseURL parses a database URL into the form the rest of the
// connection path reads; see [connector.ParseURL].
func parseDatabaseURL(dbURL string) (*url.URL, error) {
	return connector.ParseURL(dbURL)
}

func getDatabaseInfoWithCapabilities(
	ctx context.Context,
	service connector.Service,
	pool *connector.Pool,
	request connector.Request,
) (catalog.ServerInfo, capability.VersionResolution, error) {
	info, err := getDatabaseInfo(ctx, service, pool, request)
	if err != nil {
		return catalog.ServerInfo{}, capability.VersionResolution{}, err
	}
	resolution := resolveDatabaseCapabilities(info)
	info.Capabilities, err = service.Refine(ctx, pool, request, info, resolution.Capabilities)
	if err != nil {
		return catalog.ServerInfo{}, capability.VersionResolution{}, err
	}
	// The same sentence the typed --server-version path renders, from the same
	// function. A second wording here would be a second answer to "what was
	// actually planned", and the two would drift on the first edit --
	// servertarget.VersionNote is exported saying exactly that.
	info.CapabilityNote = servertarget.VersionNote(info.Dialect, info.Version, resolution)
	return info, resolution, nil
}

// getDatabaseInfo describes the server a pool reaches: the dialect, the URL in
// both its forms and the dialect's offline identifier semantics, completed by
// the dialect's connector with what the server reports.
func getDatabaseInfo(
	ctx context.Context,
	service connector.Service,
	pool *connector.Pool,
	request connector.Request,
) (catalog.ServerInfo, error) {
	info := catalog.ServerInfo{
		Dialect:             request.Dialect.Name,
		URL:                 request.URL,
		RedactedURL:         dburldisplay.Format(request.URL),
		IdentifierSemantics: identifier.ForDialect(request.Dialect.Name),
	}
	return service.Describe(ctx, pool, request, info)
}

// reportCapabilityResolution says what a non-version-specific resolution
// actually planned. One of its three cases reaches a person; the rest stay a
// debug record.
//
// All three said it at DEBUG, and the reasoning was recorded here: the default
// logger keeps WARN and above precisely so a clean run against a supported
// server emits nothing.
//
// That reasoning holds for two of the cases and not for the third. A dialect
// with no version ladder resolves the same way on every connection forever --
// every SQL Server command would carry a line saying its version refined
// nothing, which is true, permanent and not actionable. A version that fell in
// a gap between measured lines is the same kind of fact.
//
// A server NEWER than the newest line this repository has measured is not that.
// Its capabilities are unmodeled: anything the new release gained or lost is
// planned as the old line, and the operator is the only one who can decide
// whether that matters. It is also rare and it ends -- it fires until the line
// is measured, not forever. That is the case the level is reserved for rather
// than an exception to it, and the same fact already reaches the same stream on
// the typed path, where a --server-version selecting no measured line prints
// `ptah: <note>` on every command. Reporting it only when the operator typed
// the version is how planning against an unmodeled server reads as planning
// against a modeled one (stokaro/ptah#916, criterion 6).
//
// The sentence is [catalog.ServerInfo.CapabilityNote] in every case, rendered
// by servertarget.VersionNote, so the live path and the typed one cannot
// describe one outcome two ways. Empty means a measured release line was
// selected and nothing is written at any level.
func reportCapabilityResolution(info catalog.ServerInfo, resolution capability.VersionResolution) {
	if info.CapabilityNote == "" {
		return
	}
	if resolution.Saturated {
		slog.Warn(info.CapabilityNote,
			"dialect", info.Dialect,
			"version", info.Version,
			"newest_measured", resolution.NewestMeasured,
		)
		return
	}
	slog.Debug(info.CapabilityNote,
		"dialect", info.Dialect,
		"version", info.Version,
	)
}

func resolveDatabaseCapabilities(info catalog.ServerInfo) capability.VersionResolution {
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

// DatabaseConnection represents a database connection with metadata
type DatabaseConnection struct {
	db        *sql.DB
	runner    sqlrunner.Runner
	info      catalog.ServerInfo
	reader    catalog.SchemaReader
	writer    catalog.SchemaWriter
	executor  catalog.SchemaExecutor
	newReader schemaReaderFactory
	newWriter schemaWriterFactory
	pinned    bool
	// service is the connector of the connection's dialect, which answers
	// every question whose answer differs between engines. A connection
	// assembled without one asks the service registered for its dialect;
	// see engine.
	service connector.Service
	// singleSession marks a database that has no existence apart from its
	// one connection, such as an in-memory SQLite database: a session taken
	// from it is returned to the pool rather than discarded.
	singleSession bool
	// benignClose reports an error closing the pool returns that is not a
	// failure, such as the libsql WebSocket teardown. Nil means none is.
	benignClose func(error) bool
	// session is the pinned physical session set by WithSession. Connection
	// state that only applies per physical connection — see
	// WithUntrustedSQLSession — needs it.
	session *sql.Conn
}

type schemaReaderFactory func(sqlrunner.Runner) catalog.SchemaReader
type schemaWriterFactory func(sqlrunner.Runner, *sql.Conn) catalog.SchemaWriter

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

// ReadSchemaWithSchemasContext reads a database schema, applying a schema
// allow-list when the underlying dialect reader supports schema scoping.
//
// The context governs every catalog query the read issues: canceling it, or
// letting its deadline pass, makes the read return promptly with an error
// rather than running the remaining queries.
//
// The read runs on a reader of its own rather than on the connection's shared
// one, so two concurrent calls carrying different allow-lists cannot see each
// other's scope. The scope is reader state -- the PostgreSQL-family and SQL
// Server readers each write two unsynchronized fields in SetSchemas -- so
// scoping a shared reader makes every concurrent pair a data race whose
// visible effect is one read answering for the other's schemas. Measured with
// `go test -race` against two goroutines calling this function with different
// allow-lists, the detector reports the write in SetSchemas from both
// goroutines; see
// TestReadSchemaWithSchemas_ConcurrentScopesDoNotShareAReader.
//
// A reader per call costs whatever the reader caches across reads, which today
// is the PostgreSQL pg_relation_size probe: one extra round trip per scoped
// read, against a correctness defect that has no bound.
func ReadSchemaWithSchemasContext(
	ctx context.Context,
	conn *DatabaseConnection,
	schemas []string,
) (*catalog.Database, error) {
	reader, restore := conn.readerScopedTo(schemas)
	defer restore()
	schema, err := reader.ReadSchemaContext(ctx)
	if err != nil {
		return nil, err
	}
	return conn.engine().MarkRead(schema, conn.Info()), nil
}

// ReadSchemaWithSchemas is [ReadSchemaWithSchemasContext] under
// context.Background(), for a caller with no context to hand.
//
// The pair is the one this package offers for every other database call --
// [DatabaseConnection.Exec] beside [DatabaseConnection.ExecContext],
// [DatabaseConnection.Query] beside [DatabaseConnection.QueryContext] -- and it
// is database/sql's own. Prefer ReadSchemaWithSchemasContext: a schema read is
// dozens of round trips against a server that may be slow or unreachable, and
// only the Context form can be told to stop.
func ReadSchemaWithSchemas(conn *DatabaseConnection, schemas []string) (*catalog.Database, error) {
	return ReadSchemaWithSchemasContext(context.Background(), conn, schemas)
}

// readerScopedTo returns a reader restricted to schemas, plus the cleanup the
// caller must run when the read is done.
//
// A connection that knows how to build readers builds one, and the returned
// cleanup does nothing: nothing else can observe a private reader, so nothing
// has to be put back. A connection without a factory -- only a hand-assembled
// one inside this package, since the struct's fields are unexported -- falls
// back to scoping the shared reader and restoring it afterwards, which is the
// behavior that cannot be made concurrency-safe.
func (dc *DatabaseConnection) readerScopedTo(schemas []string) (catalog.SchemaReader, func()) {
	if dc.newReader != nil {
		reader := dc.newReader(dc.sqlRunner())
		if scoped, ok := reader.(schemaScopedReader); ok {
			scoped.SetSchemas(schemas)
		}
		return reader, func() {}
	}

	reader := dc.Reader()
	scoped, ok := reader.(schemaScopedReader)
	if !ok {
		return reader, func() {}
	}
	scoped.SetSchemas(schemas)
	return reader, func() { scoped.SetSchemas(nil) }
}

// engine returns the connector of the connection's dialect.
func (dc *DatabaseConnection) engine() connector.Service {
	if dc.service != nil {
		return dc.service
	}
	return serviceFor(dc.info.Dialect)
}

// serviceFor returns the connector registered for dialect, or [connector.Base]
// for a dialect Ptah does not connect to, which answers every session
// question the way an engine with nothing to add does.
func serviceFor(dialect string) connector.Service {
	if service, ok := services().Lookup(platform.NormalizeDialect(dialect)); ok {
		return service
	}
	return connector.Base{}
}

// Info returns the connection's server metadata as a defensive copy: the
// mutable metadata it carries is cloned rather than shared, so mutating the
// result never reaches the connection and concurrent calls are safe. Anything
// serialized or displayed from it should read [catalog.ServerInfo]'s
// RedactedURL rather than URL, which retains the credentials the connection
// was opened with.
func (dc *DatabaseConnection) Info() catalog.ServerInfo {
	info := dc.info
	info.IdentifierSemantics = info.IdentifierSemantics.Clone()
	return info
}

// Reader returns the connection's schema reader -- one shared instance, the
// same for every caller. Use it for plain unscoped reads. When a schema
// allow-list is needed, use [ReadSchemaWithSchemasContext] instead, which
// builds a private reader per call: the scope is reader state, so scoping this
// shared instance from two goroutines is a data race (stokaro/ptah#2246).
func (dc *DatabaseConnection) Reader() catalog.SchemaReader {
	return dc.reader
}

// Writer returns the active schema SQL executor. Transaction-scoped connection
// copies return their transaction executor here; root connections return the
// root schema writer.
func (dc *DatabaseConnection) Writer() catalog.SchemaExecutor {
	if dc.executor != nil {
		return dc.executor
	}
	return dc.writer
}

// SchemaWriter returns the schema writer bound to this connection's SQL
// session. Transaction-scoped executors do not replace the administrative
// writer.
func (dc *DatabaseConnection) SchemaWriter() catalog.SchemaWriter {
	return dc.writer
}

// WithExecutor returns a shallow connection copy that uses executor as the
// active SQL executor. When executor exposes a live transaction runner, the
// copy's reader and direct SQL use that same execution session.
//
// This is used to pass transaction-scoped writers into migration callbacks
// without storing the active transaction on the root writer.
func (dc *DatabaseConnection) WithExecutor(executor catalog.SchemaExecutor) *DatabaseConnection {
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
// writers. opts reaches database/sql unchanged on every driver that can carry
// it; where one cannot, the request is moved to the server rather than dropped
// (see isolatedSessionReadOnly). This method provides lifecycle isolation, not
// SQL read-only validation: callers must restrict statements themselves.
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
		if dc.singleSession {
			resultErr = errors.Join(resultErr, closeSQLConnection(session, "in-memory SQLite query session"))
			return
		}
		resultErr = errors.Join(resultErr, discardSQLConnection(session, "isolated query session"))
	}()
	service := dc.engine()
	if !service.Transactional() {
		return use(isolatedQueryer{runner: session})
	}

	txOpts, readOnlyStatement := isolatedSessionReadOnly(service, opts)
	tx, err := session.BeginTx(ctx, txOpts)
	if err != nil {
		return fmt.Errorf("begin isolated query transaction: %w", err)
	}
	defer func() {
		rollbackErr := tx.Rollback()
		if rollbackErr == nil || errors.Is(rollbackErr, sql.ErrTxDone) || service.TransactionEnded(rollbackErr) {
			return
		}
		resultErr = errors.Join(resultErr, fmt.Errorf("roll back transaction: %w", rollbackErr))
	}()
	if readOnlyStatement != "" {
		if _, err := tx.ExecContext(ctx, readOnlyStatement); err != nil {
			return fmt.Errorf("make isolated query transaction read-only: %w", err)
		}
	}

	return use(isolatedQueryer{runner: tx})
}

// isolatedSessionReadOnly returns the options an isolated session begins its
// transaction with, and a statement to run first in it, for the read-only
// request opts carries. A driver that cannot carry the request has its engine
// move it to the server: Oracle sets the transaction read-only with a
// statement, and YDB asks for the snapshot isolation a read-only transaction
// needs there.
func isolatedSessionReadOnly(service connector.Service, opts *sql.TxOptions) (*sql.TxOptions, string) {
	if opts == nil || !opts.ReadOnly {
		return opts, ""
	}
	return service.ReadOnlyTransaction(opts)
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
		if dc.singleSession {
			resultErr = errors.Join(resultErr, closeSQLConnection(session, "in-memory SQLite database session"))
			return
		}
		resultErr = errors.Join(resultErr, discardSQLConnection(session, "pinned database session"))
	}()

	runner := sqlrunner.NewConn(ctx, session)
	baselineCapabilities := capability.ForServerVersion(dc.info.Dialect, dc.info.Version)
	sessionCapabilities, err := dc.engine().RefineSession(
		ctx,
		session,
		dc.info,
		connectionCapabilities(dc.info.Capabilities, baselineCapabilities),
		baselineCapabilities,
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

// connectionCapabilities is the set a pinned session starts from: what the
// connection established for every session -- the release line, an extension
// it found, a cluster's feature flags -- or the release line's preset where a
// connection assembled without one carries none. A session refines it further
// only with what the session itself can change.
func connectionCapabilities(connection, baseline capability.Capabilities) capability.Capabilities {
	if connection.Len() == 0 {
		return baseline
	}
	return connection
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

// BeginTx starts a transaction on the connection's pool.
//
// It sits beside Query and Exec above and delegates the same way. The caller
// that needs it is `ptah-compat script`, whose exec and loop kinds run an
// operator's statements as one unit: everything or nothing, which is a promise
// only a transaction can keep (stokaro/ptah#1017).
//
// A schema operation should not reach for this. The writer and the executor
// carry dialect handling, dry-run and the safety gates, and a caller driving a
// transaction directly opts out of all three.
func (dc *DatabaseConnection) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return dc.db.BeginTx(ctx, opts)
}

// Exec is the context-free twin of [DatabaseConnection.ExecContext], for a
// caller with no context to hand. It carries the same executor-routing
// contract, including the result a routed statement returns.
//
// Prefer ExecContext: only it can be told to stop. An Exec issued on a
// session-scoped copy is bound to whatever lifetime that session was pinned
// with rather than to the caller's own.
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

// ExecContext executes a statement without returning rows.
//
// On a transaction-scoped connection copy (see
// [DatabaseConnection.WithExecutor]) the statement routes through the
// installed executor so it joins that executor's session. An executor that
// reports no count of its own leaves the returned [sql.Result] synthetic: it
// answers zero rows affected and has no insert id, so a caller that needs a
// real count must not read one off a routed statement. On a root connection
// the statement goes straight to the pool and the result is the driver's.
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
	return dc.engine().RestrictUntrustedSQL(ctx, dc.session)
}

// Conn returns a dedicated database session. Callers that use session-scoped
// database features must close the returned connection when finished.
func (dc *DatabaseConnection) Conn(ctx context.Context) (*sql.Conn, error) {
	if dc.pinned {
		return nil, fmt.Errorf("database connection is already pinned to a session")
	}
	return dc.db.Conn(ctx)
}

// Close closes the connection pool a root connection owns.
//
// A session-scoped connection copy -- the *DatabaseConnection a session
// method such as [DatabaseConnection.WithSession] hands its callback -- does
// not own the pool, so Close on it is a no-op returning nil: the session's
// lifecycle belongs to the method that pinned it. A connection holding no
// database is a no-op too, so Close composes with defer on every path.
func (dc *DatabaseConnection) Close() error {
	if dc.pinned {
		return nil
	}
	if dc.db == nil {
		return nil
	}
	err := dc.db.Close()
	if err != nil && dc.benignClose != nil && dc.benignClose(err) {
		// The engine's transport reports an error on a teardown that is not
		// a failure, such as the libsql WebSocket's io.EOF after every
		// connection that ran a statement; see internal/connector/sqliteconnect.
		return nil
	}
	return err
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

// connectionDialect is the dialect a parsed database URL connects with. It
// refuses a missing or unknown scheme before anything is dialed.
func connectionDialect(parsedURL *url.URL) (string, error) {
	if parsedURL.Scheme == "" {
		return "", errors.New("invalid database URL: missing scheme")
	}
	rawDialect := strings.ToLower(parsedURL.Scheme)
	dialect := platform.NormalizeDialect(rawDialect)
	if dialect == "" {
		return "", fmt.Errorf("unsupported database dialect: %s", rawDialect)
	}
	return dialect, nil
}
