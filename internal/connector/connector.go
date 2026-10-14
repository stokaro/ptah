// Package connector declares the service an engine provides to connect to its
// servers, and the registry dbschema chooses one from.
//
// A connection asks its engine's [Service] everything that differs between
// engines: how a database URL becomes a driver and a data source, how the pool
// is opened and verified, what the server is, which capabilities the
// connection has beyond its release line, which reader and writer serve it,
// and how a pinned session behaves. dbschema owns what every engine shares --
// pinning sessions, routing statements, closing -- and asks this package for
// the rest, so it never branches on a dialect's name.
//
// A service holds no state of its own: what it keeps for one connection, such
// as YDB's SDK connection, travels in the [Pool] its Open returns. Engines that
// share a wire protocol may share one implementation and register it under
// each of their dialects; the service then reads catalog.ServerInfo.Dialect
// where the dialects differ.
package connector

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/sqlrunner"
)

// Request is what a connection is opened from.
type Request struct {
	// Dialect is the descriptor of the dialect the URL's scheme selects.
	Dialect descriptor.Dialect
	// URL is the database URL as the caller wrote it.
	URL string
	// Parsed is the URL in the form dbschema reads it: the scheme names the
	// dialect, and the path names the database. A MySQL-family DSN that is not
	// a URL arrives here as one.
	Parsed *url.URL
}

// Pool is an open, verified connection pool and what its engine keeps beside
// it for the connection's lifetime.
type Pool struct {
	// DB is the pool.
	DB *sql.DB
	// SingleSession reports a database that has no existence apart from its
	// one connection, such as an in-memory SQLite database: every caller
	// shares that session, and discarding it discards the database.
	SingleSession bool
	// Native is what the engine that opened the pool keeps for its own
	// services, such as YDB's SDK connection. Only that engine reads it.
	Native any
	// BenignClose reports an error closing DB returns that is not a failure.
	// Nil means every such error is one.
	BenignClose func(error) bool
}

// Executor is the part of a connection an engine runs its own statements on,
// such as an identifier probe. *dbschema.DatabaseConnection satisfies it and
// routes the statements the way it routes every other one.
type Executor interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Service is how one engine connects. [Base] answers every method the way an
// engine with nothing to add answers it, so an implementation embeds Base and
// writes only what its engine does differently.
type Service interface {
	// RequireDatabase refuses, before anything is dialed, a URL that names a
	// whole server rather than one database. It is asked for a caller that
	// reads and changes one database, and not for one that handles a server.
	RequireDatabase(parsed *url.URL) error
	// Open opens the pool for request and verifies it with a ping. The pool
	// is closed again on every error.
	Open(ctx context.Context, request Request) (*Pool, error)
	// Describe completes info, which carries the dialect, the URLs and the
	// dialect's offline identifier semantics, with what the server reports:
	// its version, the dialect its banner names, the schema the connection
	// works in, and the identifier semantics that follow from it.
	Describe(ctx context.Context, pool *Pool, request Request, info catalog.ServerInfo) (catalog.ServerInfo, error)
	// Refine adjusts the capabilities a release line resolved to with what
	// the connection itself establishes, such as an installed extension or a
	// cluster's feature flags.
	Refine(ctx context.Context, pool *Pool, request Request, info catalog.ServerInfo, caps capability.Capabilities) (capability.Capabilities, error)
	// NewReader returns the schema reader that reads through runner.
	NewReader(pool *Pool, info catalog.ServerInfo, runner sqlrunner.Runner) catalog.SchemaReader
	// NewWriter returns the schema writer that writes through runner. session
	// is the pinned physical session runner runs on, or nil for the pool.
	NewWriter(pool *Pool, info catalog.ServerInfo, runner sqlrunner.Runner, session *sql.Conn) catalog.SchemaWriter

	// RefineSession adjusts a pinned session's capabilities with what the
	// session's own settings change. caps is what the session starts from,
	// baseline the release line's preset.
	RefineSession(ctx context.Context, session *sql.Conn, info catalog.ServerInfo, caps, baseline capability.Capabilities) (capability.Capabilities, error)
	// Transactional reports whether the driver can run a transaction. A
	// session that cannot runs an isolated query directly on itself.
	Transactional() bool
	// ReadOnlyTransaction returns the options that start a read-only
	// transaction the caller asked for with opts, and a statement to run
	// first in it when the driver cannot carry the request itself.
	ReadOnlyTransaction(opts *sql.TxOptions) (*sql.TxOptions, string)
	// TransactionEnded reports a rollback error that says the server had
	// already ended the transaction, which leaves nothing to undo.
	TransactionEnded(rollbackErr error) bool
	// OutsideTransaction reports whether the pinned session is provably
	// outside any transaction, as its driver or its server says. Any failure
	// to ask answers false.
	OutsideTransaction(ctx context.Context, session *sql.Conn) bool
	// KeepDDLInTransaction prepares tx, a transaction that will be rolled
	// back, so that DDL run inside it stays inside it.
	KeepDDLInTransaction(ctx context.Context, tx *sql.Tx, info catalog.ServerInfo) error
	// RestrictUntrustedSQL applies, and verifies, every restriction the
	// engine offers on a session that will run SQL the caller does not
	// trust.
	RestrictUntrustedSQL(ctx context.Context, session *sql.Conn) error

	// MarkRead records on a schema the reader read what the read cannot
	// describe on this target.
	MarkRead(schema *catalog.Database, info catalog.ServerInfo) *catalog.Database
	// ResolveIdentifierSemantics answers how the connected catalog compares
	// names, asking the server where it is the only authority.
	ResolveIdentifierSemantics(ctx context.Context, executor Executor, info catalog.ServerInfo, names []string) (identifier.Semantics, error)
	// HoldsBytes reports whether a column the driver describes as
	// databaseType holds binary values.
	HoldsBytes(databaseType string) bool
	// RowValue converts a value the driver scanned into the value a row read
	// returns.
	RowValue(scanned any) (any, error)
}

// Base is the answer of an engine with nothing to add. Its Open opens
// Request.Dialect.Driver with the URL as its data source; NewReader and
// NewWriter return nil, so an engine that connects implements both.
type Base struct{}

// RequireDatabase accepts every URL.
func (Base) RequireDatabase(*url.URL) error { return nil }

// Open opens the dialect's driver with the URL as its data source.
func (Base) Open(ctx context.Context, request Request) (*Pool, error) {
	db, err := OpenDB(request.Dialect.Driver, request.URL)
	if err != nil {
		return nil, err
	}
	if err := Ping(ctx, db); err != nil {
		return nil, err
	}
	return &Pool{DB: db}, nil
}

// Describe adds nothing to info.
func (Base) Describe(_ context.Context, _ *Pool, _ Request, info catalog.ServerInfo) (catalog.ServerInfo, error) {
	return info, nil
}

// Refine keeps caps.
func (Base) Refine(_ context.Context, _ *Pool, _ Request, _ catalog.ServerInfo, caps capability.Capabilities) (capability.Capabilities, error) {
	return caps, nil
}

// NewReader returns nil: an engine that connects provides its reader.
func (Base) NewReader(*Pool, catalog.ServerInfo, sqlrunner.Runner) catalog.SchemaReader { return nil }

// NewWriter returns nil: an engine that connects provides its writer.
func (Base) NewWriter(*Pool, catalog.ServerInfo, sqlrunner.Runner, *sql.Conn) catalog.SchemaWriter {
	return nil
}

// RefineSession keeps caps.
func (Base) RefineSession(
	_ context.Context, _ *sql.Conn, _ catalog.ServerInfo, caps, _ capability.Capabilities,
) (capability.Capabilities, error) {
	return caps, nil
}

// Transactional answers true.
func (Base) Transactional() bool { return true }

// ReadOnlyTransaction passes opts through, with no statement.
func (Base) ReadOnlyTransaction(opts *sql.TxOptions) (*sql.TxOptions, string) { return opts, "" }

// TransactionEnded answers false.
func (Base) TransactionEnded(error) bool { return false }

// OutsideTransaction answers false: nothing says the session is outside one.
func (Base) OutsideTransaction(context.Context, *sql.Conn) bool { return false }

// KeepDDLInTransaction does nothing.
func (Base) KeepDDLInTransaction(context.Context, *sql.Tx, catalog.ServerInfo) error { return nil }

// RestrictUntrustedSQL does nothing: the engine offers no session-level
// restriction.
func (Base) RestrictUntrustedSQL(context.Context, *sql.Conn) error { return nil }

// MarkRead keeps schema as it is.
func (Base) MarkRead(schema *catalog.Database, _ catalog.ServerInfo) *catalog.Database { return schema }

// ResolveIdentifierSemantics answers from the connection's own semantics,
// normalized for its dialect.
func (Base) ResolveIdentifierSemantics(
	_ context.Context, _ Executor, info catalog.ServerInfo, _ []string,
) (identifier.Semantics, error) {
	return info.IdentifierSemantics.Normalize(info.Dialect), nil
}

// HoldsBytes reports whether the driver names a binary column type.
//
// The name is what decides, because the Go type cannot. go-sql-driver/mysql
// scans character, numeric, date and JSON columns as []byte, go-mssqldb does the
// same for DECIMAL and MONEY, and pgx for JSON, JSONB and XML; each of them names
// the column type. go-ora and modernc SQLite return []byte only for a binary
// value, and go-ora names no column type at all, as SQLite does not for a column
// declared without one, so a column with no name keeps its bytes. A length
// suffix such as VARBINARY(16) is ignored.
func (Base) HoldsBytes(databaseType string) bool {
	name, _, _ := strings.Cut(strings.ToUpper(strings.TrimSpace(databaseType)), "(")
	switch strings.TrimSpace(name) {
	case "", "BYTEA", "BINARY", "VARBINARY", "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "IMAGE":
		return true
	default:
		return false
	}
}

// RowValue returns scanned unchanged.
func (Base) RowValue(scanned any) (any, error) { return scanned, nil }

// OpenDB opens a pool with database/sql. sql.Open dials nothing, so the pool
// is verified by [Ping].
func OpenDB(driverName, dataSourceName string) (*sql.DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}
	return db, nil
}

// Ping verifies db under ctx, so a stuck or slow host cannot block the caller
// indefinitely, and closes it when the server does not answer.
func Ping(ctx context.Context, db *sql.DB) error {
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("failed to ping database: %w", err)
	}
	return nil
}

// NormalizedNames returns names sorted, without repeats and without the empty
// name: the set an identifier resolution asks the server about.
func NormalizedNames(names []string) []string {
	names = slices.Clone(names)
	names = slices.DeleteFunc(names, func(name string) bool {
		return name == ""
	})
	slices.Sort(names)
	return slices.Compact(names)
}
