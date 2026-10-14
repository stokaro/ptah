// Package sqliteconnect connects to SQLite: a local file or an in-memory
// database through modernc's driver, or a remote libSQL (Turso) server, which
// keeps SQLite's schema semantics over its own transport. The URL chooses the
// transport; the dialect is SQLite's either way.
package sqliteconnect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	_ "github.com/tursodatabase/libsql-client-go/libsql" // registers the libsql driver a libsql:// URL opens

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/connector"
	"ptah.run/internal/dbschema/sqlite"
	"ptah.run/internal/sqlrunner"
)

// Registrations registers the service under SQLite.
func Registrations() []connector.Registration {
	return []connector.Registration{{Dialect: platform.SQLite, Service: Service{}}}
}

// Service is SQLite's connector.
type Service struct {
	connector.Base
}

// Open opens a libsql:// or libsql+ws:// URL on the libsql driver and every
// other SQLite URL on the dialect's own. An in-memory database has no
// existence apart from its connection, so its pool keeps one: every caller
// shares that session, and closing it discards the database.
func (Service) Open(ctx context.Context, request connector.Request) (*connector.Pool, error) {
	driverName, dataSourceName, remote := libSQLDriverConfig(request.URL)
	if !remote {
		driverName, dataSourceName = request.Dialect.Driver, convertSQLiteURL(request.URL)
	}
	db, err := connector.OpenDB(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	singleSession := !remote && isSQLiteMemoryDSN(dataSourceName)
	if singleSession {
		db.SetMaxOpenConns(1)
	}
	if err := connector.Ping(ctx, db); err != nil {
		return nil, err
	}
	pool := &connector.Pool{DB: db, SingleSession: singleSession}
	if remote {
		pool.BenignClose = libSQLTeardownIsBenign
	}
	return pool, nil
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

// libSQLTeardownIsBenign reports the one close error the libsql WebSocket
// transport always produces and which is not a failure.
//
// Measured against libsql-server v0.24.32: EVERY libsql WebSocket connection
// that ran a statement reports `failed to close WebSocket: failed to read frame
// header: EOF` on Close, and one that ran none closes cleanly. The server hangs
// up first, so the client's read of the close frame returns io.EOF. Reporting
// it would put a warning on stderr after every successful command, which the
// pinned community binary does not, and which would be the only difference
// between the two on this transport (stokaro/ptah#1615).
//
// Only io.EOF is swallowed, and only on this transport: a genuine close
// failure still reaches the caller.
func libSQLTeardownIsBenign(err error) bool {
	return errors.Is(err, io.EOF)
}

func convertSQLiteURL(dbURL string) string {
	// Through connector.ParseURL rather than url.Parse: a Windows absolute
	// path is refused by the latter, and returning the URL whole then handed
	// the driver a string beginning with sqlite:// as though it were a
	// filename.
	parsed, err := connector.ParseURL(dbURL)
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

func hasSQLiteForeignKeysPragma(query url.Values) bool {
	for _, pragma := range query["_pragma"] {
		if strings.Contains(strings.ToLower(pragma), "foreign_keys") {
			return true
		}
	}
	return false
}

// Describe reads the SQLite version. A SQLite connection has one schema,
// main.
func (Service) Describe(
	ctx context.Context,
	opened *connector.Pool,
	_ connector.Request,
	info catalog.ServerInfo,
) (catalog.ServerInfo, error) {
	var version string
	if err := opened.DB.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version); err != nil {
		return info, fmt.Errorf("failed to get SQLite version: %w", err)
	}
	info.Version = version
	info.Schema = "main"
	// Already what the dialect rule answers. Assigned anyway so the rule is
	// one line in every engine: four of the six pinned it and two did not,
	// and the two that did not were the defect.
	info.IdentifierSemantics.DefaultSchema = info.Schema
	return info, nil
}

// NewReader returns the SQLite reader. libsql reaches a remote server, but the
// schema it serves is SQLite's, so it takes SQLite's reader, writer and
// executor unchanged.
func (Service) NewReader(_ *connector.Pool, info catalog.ServerInfo, runner sqlrunner.Runner) catalog.SchemaReader {
	return sqlite.NewSQLiteReader(runner, info.Schema)
}

// NewWriter returns the SQLite writer, bound to the pinned session when there
// is one.
func (Service) NewWriter(_ *connector.Pool, info catalog.ServerInfo, runner sqlrunner.Runner, session *sql.Conn) catalog.SchemaWriter {
	if session != nil {
		return sqlite.NewSQLiteWriterForPinnedRunner(runner, session, info.Schema)
	}
	return sqlite.NewSQLiteWriterForRunner(runner, info.Schema)
}

// RestrictUntrustedSQL makes the engine refuse ATTACH, DETACH and VACUUM INTO
// and the loading of native extensions on the session, and verifies the
// restriction is in force.
func (Service) RestrictUntrustedSQL(ctx context.Context, session *sql.Conn) error {
	return sqlite.RestrictSession(ctx, session)
}
