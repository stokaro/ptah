// Package mysqlconnect connects to the engines that speak the MySQL wire
// protocol, MySQL and MariaDB, through go-sql-driver/mysql. One service serves
// both and reads catalog.ServerInfo.Dialect where they differ.
package mysqlconnect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/connector"
	"ptah.run/internal/dbschema/mysql"
	"ptah.run/internal/sqlrunner"
)

// Registrations registers the service under MySQL and MariaDB.
func Registrations() []connector.Registration {
	return []connector.Registration{
		{Dialect: platform.MySQL, Service: Service{}},
		{Dialect: platform.MariaDB, Service: Service{}},
	}
}

// Service is the MySQL wire family's connector.
type Service struct {
	connector.Base
}

// errURLNamesNoDatabase refuses a MySQL-family URL that selects no database,
// and says what to write instead.
//
// The pinned community binary v1.3.0 reads such a URL as the whole server: it
// inspects every database, plans CREATE DATABASE and DROP DATABASE, keeps its
// revisions in a database of their own, and `schema clean` drops every
// database on the server. dbschema.ConnectToServer opens one for the callers
// that handle a server; every other caller reads and changes one database, so
// it is refused rather than handed a server (stokaro/ptah#3761,
// stokaro/ptah#3789).
var errURLNamesNoDatabase = errors.New(
	"the database URL names no database, and this command reads and changes one MySQL or MariaDB database, " +
		"not a whole server. Name the database in the URL path (mysql://user@host:3306/app) or, " +
		"for a socket URL, in the database parameter (mysql+unix://user@/run/mysqld/mysqld.sock?database=app), " +
		"and run the command once for each database")

// RequireDatabase refuses a URL that names no database: its session would
// start with no default database, so what a caller written for one database
// reads or changes would be decided by nothing the operator wrote.
func (Service) RequireDatabase(parsed *url.URL) error {
	if strings.TrimPrefix(parsed.Path, "/") == "" {
		return errURLNamesNoDatabase
	}
	return nil
}

// Open opens the pool through go-sql-driver/mysql.
func (Service) Open(ctx context.Context, request connector.Request) (*connector.Pool, error) {
	db, err := connector.OpenDB(request.Dialect.Driver, convertMySQLURL(request.URL))
	if err != nil {
		return nil, err
	}
	if err := connector.Ping(ctx, db); err != nil {
		return nil, err
	}
	return &connector.Pool{DB: db}, nil
}

// convertMySQLURL converts a MySQL-family URL into go-sql-driver's DSN.
//
// The DSN is atlasurl.ParseMySQLURL's, which is also where dbschema reads the
// database. One parser is what keeps the two in agreement: with a recognizer
// in each, a URL can reach the driver with its scheme attached, or be rebuilt
// around a host that was never one (stokaro/ptah#1540).
func convertMySQLURL(dbURL string) string {
	mysqlURL, err := atlasurl.ParseMySQLURL(dbURL)
	if err != nil {
		// The connection parsed this URL before reaching here, so a failure
		// is unreachable rather than tolerated. Returning the input leaves the
		// driver to report on it.
		return dbURL
	}
	return mysqlURL.DSN()
}

// Describe reads the version banner, the product it names and the database
// the URL selects.
func (Service) Describe(
	ctx context.Context,
	opened *connector.Pool,
	request connector.Request,
	info catalog.ServerInfo,
) (catalog.ServerInfo, error) {
	var version string
	err := opened.DB.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
	if err != nil {
		return info, fmt.Errorf("failed to get MySQL/MariaDB version: %w", err)
	}
	info.Version = version
	info.Dialect = detectMySQLWireDialect(version)
	info.IdentifierSemantics = identifier.ForDialect(info.Dialect)

	// The database the URL names. None is a whole-server connection, which
	// only dbschema.ConnectToServer opens: the session selects no database,
	// and the empty schema is what every realm question reads it by.
	info.Schema = strings.TrimPrefix(request.Parsed.Path, "/")
	info.WholeServer = info.Schema == ""
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
	// the same place.
	info.IdentifierSemantics.DefaultSchema = info.Schema
	return info, nil
}

// detectMySQLWireDialect names the product behind a MySQL-wire connection from its
// VERSION() banner, reading the table the PostgreSQL wire's detection reads.
//
// Unlike the PostgreSQL wire, the server is the whole answer here and the
// scheme the operator connected with is not consulted. MariaDB announces itself
// in every banner, the replication prefix included, and a banner without it is
// a MySQL server. `mysql://`, `mariadb://` and `maria://` open one driver, so
// the scheme cannot say which of the two a server is; the pinned community
// binary v1.3.0 decides by the server the same way, and printed byte-identical
// `schema inspect` output through all three against MariaDB 11.8.9 and against
// MySQL 8.4.11. Read from the scheme, a `mariadb://` URL naming a MySQL server
// plans MariaDB statements for it, and a dev URL spelled differently from its
// target is refused as another dialect on the same server (stokaro/ptah#3756).
func detectMySQLWireDialect(version string) string {
	if capability.BannerPlatform(version) == platform.MariaDB {
		return platform.MariaDB
	}
	return platform.MySQL
}

// NewReader returns the MySQL-family reader. The set decides which spelling of
// the CHECK_CONSTRAINTS read is asked first. MySQL's view has no TABLE_NAME --
// that column is a MariaDB extension -- so without this every MySQL schema
// read spent a round trip being told error 1054 (stokaro/ptah#916).
func (Service) NewReader(_ *connector.Pool, info catalog.ServerInfo, runner sqlrunner.Runner) catalog.SchemaReader {
	return mysql.NewMySQLReaderWithCapabilities(runner, info.Schema, info.Capabilities)
}

// NewWriter returns the MySQL-family writer, bound to the pinned session when
// there is one.
func (Service) NewWriter(
	opened *connector.Pool,
	info catalog.ServerInfo,
	runner sqlrunner.Runner,
	session *sql.Conn,
) catalog.SchemaWriter {
	if session != nil {
		return mysql.NewMySQLWriterForPinnedRunner(runner, opened.DB, session, info.Schema, info.Dialect, info.Version)
	}
	return mysql.NewMySQLWriterForRunner(runner, opened.DB, info.Schema, info.Dialect, info.Version)
}

// RefineSession reads the foreign-key reference policy of the pinned session,
// which MySQL lets a session change.
func (Service) RefineSession(
	ctx context.Context,
	session *sql.Conn,
	info catalog.ServerInfo,
	caps, baseline capability.Capabilities,
) (capability.Capabilities, error) {
	return refineMySQLForeignKeyCapabilities(info.Dialect, caps, baseline, func(destination ...any) error {
		return session.QueryRowContext(ctx, "SELECT @@SESSION.restrict_fk_on_non_standard_key").Scan(destination...)
	})
}

// refineMySQLForeignKeyCapabilities decides, on a MySQL release line that requires
// a unique foreign-key reference, which reference policy the session runs:
// restrict_fk_on_non_standard_key off relaxes it to an indexed reference.
// scan reads the variable; it is not asked on MariaDB or on a line without the
// requirement.
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
		return capability.Capabilities{}, fmt.Errorf("query restrict_fk_on_non_standard_key: %w", err)
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

// ResolveIdentifierSemantics asks the server which index names collide. The
// MySQL family is the second dialect whose index-name equivalence the server is
// the only authority on, and it is asked a different question: SQL Server can
// partition a set under a collation, while neither MySQL nor MariaDB exposes
// the fold its identifier comparison uses, so what is asked is the collision
// itself (stokaro/ptah#2768).
func (Service) ResolveIdentifierSemantics(
	ctx context.Context,
	executor connector.Executor,
	info catalog.ServerInfo,
	names []string,
) (identifier.Semantics, error) {
	return resolveIndexNames(ctx, executor, info, names)
}
