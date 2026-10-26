// Package pgconnect connects to the engines that speak the PostgreSQL wire
// protocol: PostgreSQL, CockroachDB, YugabyteDB and the Spanner PostgreSQL
// interface. One service serves all four through pgx and reads
// catalog.ServerInfo.Dialect where the four differ.
package pgconnect

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/stdlib"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/internal/connector"
	"ptah.run/internal/dbschema/postgres"
	"ptah.run/internal/schemaselection"
	"ptah.run/internal/sqlrunner"
)

// Registrations registers the service under each dialect of the PostgreSQL
// wire family.
func Registrations() []connector.Registration {
	return []connector.Registration{
		{Dialect: platform.Postgres, Service: Service{}},
		{Dialect: platform.CockroachDB, Service: Service{}},
		{Dialect: platform.YugabyteDB, Service: Service{}},
		{Dialect: platform.Spanner, Service: Service{}},
	}
}

// Service is the PostgreSQL wire family's connector.
type Service struct {
	connector.Base
}

// pool is what a connection keeps beside its pool: whether the schema
// selection could be sent, which decides how the schema is resolved.
type pool struct {
	// carried marks a connection reopened without its `search_path`
	// startup parameter, whose session reports the connection default for a
	// URL that named something else.
	carried bool
}

// Open opens the pool through pgx. A server, or a proxy in front of it, that
// refuses the URL's `search_path` startup parameter is asked again without it;
// see reconnectCarryingSearchPath.
func (Service) Open(ctx context.Context, request connector.Request) (*connector.Pool, error) {
	db, err := connector.OpenDB(request.Dialect.Driver, convertPostgresWireURL(request.URL))
	if err != nil {
		return nil, err
	}
	pingErr := db.PingContext(ctx)
	if pingErr == nil {
		return &connector.Pool{DB: db, Native: pool{}}, nil
	}
	_ = db.Close()
	retried, err := reconnectCarryingSearchPath(ctx, request.URL, request.Dialect.Driver, request.Dialect.Name, pingErr)
	if err != nil {
		return nil, err
	}
	return &connector.Pool{DB: retried, Native: pool{carried: true}}, nil
}

// convertPostgresWireURL turns a PostgreSQL-wire URL into the one pgx reads: the pool
// parameters pgx's own pool takes are removed, and a scheme pgx does not know,
// such as cockroachdb://, is spelled as the scheme the dialect's descriptor
// names for the wire.
func convertPostgresWireURL(dbURL string) string {
	cleaned := removePostgresPoolParams(dbURL)
	parsedURL, err := url.Parse(cleaned)
	if err != nil {
		return cleaned
	}
	described, known := platform.Lookup(parsedURL.Scheme)
	if !known || described.WireScheme == "" {
		return cleaned
	}
	parsedURL.Scheme = described.WireScheme
	return parsedURL.String()
}

// Describe reads the version banner, the product it names, the schema the
// session resolved to and, on CockroachDB, the width of a bare INT.
func (Service) Describe(
	ctx context.Context,
	opened *connector.Pool,
	request connector.Request,
	info catalog.ServerInfo,
) (catalog.ServerInfo, error) {
	db := opened.DB
	var version string
	err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return info, fmt.Errorf("failed to get PostgreSQL version: %w", err)
	}
	info.Version = version
	info.Dialect = detectPostgresWireDialect(info.Dialect, version)
	info.IdentifierSemantics = identifier.ForDialect(info.Dialect)

	// The schema is whatever the server resolves this session's search_path
	// to, which is what a `?search_path=` on the URL selects. A constant
	// "public" here, with a branch on the URL path that assigns "public"
	// again, does not merely ignore a dev URL naming another schema -- the
	// writer treats that schema as a stranger's and DROPS it while cleaning
	// the database realm, then replays with a search_path resolving to
	// nothing (stokaro/ptah#1198).
	//
	// Why a selection that resolves to nothing is refused rather than folded
	// back to "public" is documented on [schemaselection.Selection.Resolve],
	// next to the code that decides it.
	schema, err := resolveSchema(ctx, opened, request.Parsed)
	if err != nil {
		return info, err
	}
	info.Schema = schema
	// The schema a connection resolved to IS the schema its unqualified
	// names mean, and the two were different values here: the dialect rule
	// answers the constant "public", so a URL selecting another schema left
	// every implicit name pointing at the wrong one.
	//
	// The catalog reports an object with no schema when it is in the schema
	// the read was scoped to, and a desired state written as HCL or Go
	// annotations carries `app` explicitly. Keyed through a "public"
	// default, `widget` and `app.widget` are two tables. Measured on
	// PostgreSQL 17 against a database holding `app.widget`, with
	// `?search_path=app` on the URL and a document declaring the same
	// table, `schema apply` planned CREATE TABLE "app"."widget" and
	// DROP TABLE "widget" CASCADE, and failed on 42P07 (stokaro/ptah#1991).
	//
	// It costs nothing in the ordinary case: both values are "public"
	// there. MySQL, MariaDB, Oracle and SQL Server pin the same field from
	// the same place, for the reason stokaro/ptah#1244 records.
	info.IdentifierSemantics.DefaultSchema = info.Schema
	// Every schema an unqualified name resolves in, which on a search_path
	// naming several schemas is more than the default one; see
	// [schemaselection.Selection.ResolveSearchPath]. A server that cannot
	// answer leaves the path unknown, and a reader of unqualified names then
	// takes one to be in any schema, which errs toward keeping what it reads.
	if path, err := resolveSearchPath(ctx, opened, request.Parsed); err == nil {
		info.IdentifierSemantics.SearchPath = path
	}
	info.DefaultIntSize = cockroachDefaultIntSize(ctx, db, info.Dialect)
	return info, nil
}

// resolveSchema answers which schema a connection works in. The two answers
// come from two different places, not from two branches of one: the session
// knows for a connection that could send the selection, and the URL knows for
// one that could not.
func resolveSchema(ctx context.Context, opened *connector.Pool, parsedURL *url.URL) (string, error) {
	selection := schemaselection.FromParsedURL(parsedURL)
	if state, ok := opened.Native.(pool); ok && state.carried {
		// The session reports the connection default for a URL that named
		// something else, so the server is asked to resolve the selection
		// the URL carries.
		return selection.ResolveCarried(ctx, opened.DB)
	}
	// The session resolved the selection itself, which accounts for a
	// `search_path` naming several schemas, a schema the URL did not name,
	// and the connection default.
	return selection.Resolve(ctx, opened.DB)
}

// resolveSearchPath is [resolveSchema] for the whole search path.
func resolveSearchPath(ctx context.Context, opened *connector.Pool, parsedURL *url.URL) ([]string, error) {
	selection := schemaselection.FromParsedURL(parsedURL)
	if state, ok := opened.Native.(pool); ok && state.carried {
		return selection.ResolveCarriedSearchPath(ctx, opened.DB)
	}
	return selection.ResolveSearchPath(ctx, opened.DB)
}

// detectPostgresWireDialect names the product behind a PostgreSQL-wire connection from
// its own SELECT version() banner.
//
// The product tokens come from capability.BannerPlatform rather than a second
// copy of the same substrings, because this function and
// capability.ResolveServerVersion must not disagree about which server a
// banner describes: this one picks the dialect, that one picks the preset the
// dialect is planned with, and they are handed the same string.
//
// A banner naming only PostgreSQL is not a product answer here. CockroachDB,
// YugabyteDB and Spanner all speak this protocol and a deployment of any of
// them may report a banner carrying no token of its own, so the dialect the
// operator connected with survives -- it is the only evidence left.
func detectPostgresWireDialect(declaredDialect, version string) string {
	switch banner := capability.BannerPlatform(version); banner {
	case platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		return banner
	default:
		return platform.NormalizeDialect(declaredDialect)
	}
}

// cockroachDefaultIntSize reads the width CockroachDB gives a column declared
// INT or INTEGER in this session, or 0 on any other dialect.
//
// CockroachDB resolves those spellings through the session variable
// default_int_size, 8 unless the session sets it: measured on v26.3.2,
// `CREATE TABLE t (n integer, k int)` builds both columns as INT8, and the
// same statement in a session with default_int_size = 4 builds INT4. Taken at
// PostgreSQL's width, the declaration differs from the table the same SQL
// built, and a comparison plans an ALTER COLUMN ... TYPE on every run
// (stokaro/ptah#3922).
//
// A failed read leaves 0, which the comparison takes as the server's default,
// rather than failing the connection over a setting it can do without.
func cockroachDefaultIntSize(ctx context.Context, db *sql.DB, dialect string) int {
	if platform.NormalizeDialect(dialect) != platform.CockroachDB {
		return 0
	}
	var size string
	if err := db.QueryRowContext(ctx, "SHOW default_int_size").Scan(&size); err != nil {
		slog.Debug("could not read default_int_size", "error", err)
		return 0
	}
	width, err := strconv.Atoi(strings.TrimSpace(size))
	if err != nil {
		slog.Debug("could not parse default_int_size", "value", size, "error", err)
		return 0
	}
	return width
}

// Refine decides the keys an EXTENSION carries rather than a version line.
//
// TimescaleDB is the case: it is PostgreSQL with an extension installed, not a
// dialect or a version of its own, and it puts no token in `version()` -- so
// nothing the resolver reads can see it. What decides [capability.Hypertables]
// and [capability.ContinuousAggregates] is `pg_extension`, on the connection
// (stokaro/ptah#1026).
//
// It is asked only of PostgreSQL itself. CockroachDB, YugabyteDB and the
// Spanner PostgreSQL interface share the wire and not the extension mechanism,
// and asking a target that has no `pg_extension` would spend a round trip to
// learn nothing.
//
// A failed query leaves the key alone rather than failing the connection. This
// runs before any transaction the caller opens, so a refusal here cannot poison
// one, and a server that will not answer has told us nothing about the
// extension either way.
func (Service) Refine(
	ctx context.Context,
	opened *connector.Pool,
	_ connector.Request,
	info catalog.ServerInfo,
	caps capability.Capabilities,
) (capability.Capabilities, error) {
	return refineExtensionCapabilities(ctx, opened.DB, info.Dialect, caps), nil
}

// refineExtensionCapabilities is [Service.Refine] over db for dialect: on
// PostgreSQL it sets the keys TimescaleDB carries when `pg_extension` lists
// it, and on every other dialect it returns caps unchanged.
func refineExtensionCapabilities(
	ctx context.Context,
	db *sql.DB,
	dialect string,
	caps capability.Capabilities,
) capability.Capabilities {
	if platform.NormalizeDialect(dialect) != platform.Postgres {
		return caps
	}
	var installed bool
	if err := db.QueryRowContext(
		ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')",
	).Scan(&installed); err != nil {
		slog.Debug("could not read pg_extension for capability refinement", "error", err)
		return caps
	}
	if !installed {
		return caps
	}
	return caps.With(capability.Hypertables, true).With(capability.ContinuousAggregates, true)
}

// NewReader returns the PostgreSQL-wire reader, configured with the
// connection's capabilities rather than with a dialect name.
func (Service) NewReader(_ *connector.Pool, info catalog.ServerInfo, runner sqlrunner.Runner) catalog.SchemaReader {
	return postgres.NewPostgreSQLWireReaderWithCapabilities(runner, info.Schema, info.Dialect, info.Capabilities)
}

// NewWriter returns the PostgreSQL-wire writer. The set decides whether
// cleanup may take a transaction: Spanner refuses DDL inside one, and the
// writer cannot ask the server without opening the transaction it is trying
// to avoid. The dialect decides which schemas a cleanup leaves to the server.
func (Service) NewWriter(_ *connector.Pool, info catalog.ServerInfo, runner sqlrunner.Runner, _ *sql.Conn) catalog.SchemaWriter {
	return postgres.NewPostgreSQLWriterForRunnerWithCapabilities(runner, info.Schema, info.Dialect, info.Capabilities)
}

// OutsideTransaction reads the status byte of the server's last
// ReadyForQuery message, which pgx keeps for the session. The answer comes
// from the server, never from Ptah's own bookkeeping, because an owner can
// open a transaction with a plain BEGIN that no wrapper sees. A session pgx
// does not hold answers false.
func (Service) OutsideTransaction(_ context.Context, session *sql.Conn) bool {
	idle := false
	err := session.Raw(func(driverConn any) error {
		conn, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return nil
		}
		idle = conn.Conn().PgConn().TxStatus() == txStatusIdle
		return nil
	})
	return err == nil && idle
}

// txStatusIdle is the ReadyForQuery status byte of a session outside any
// transaction block; 'T' is inside one and 'E' inside a failed one.
const txStatusIdle = 'I'

// KeepDDLInTransaction asks a server that would not to keep the caller's DDL
// where the caller put it.
//
// CockroachDB defaults autocommit_before_ddl to on: a DDL statement issued
// inside an explicit transaction makes the server COMMIT that transaction
// first and run the DDL in one of its own. Everything a rolled-back probe
// guarantees rests on the transaction still being there, and it is not.
//
// The symptom is not the commit, which is silent. It is what happens next --
// measured on cockroachdb/cockroach:v25.4.0, `ptah schema apply --dry-run`
// against any schema holding a CHECK constraint:
//
//	error: compare database schema: compare schemas: resolve check expressions:
//	roll back to savepoint after "customers.customers_amount_ck":
//	ERROR: savepoint "ptah_check_probe" does not exist (SQLSTATE 3B001)
//
// The probe's CREATE TEMPORARY TABLE is refused there anyway -- temp tables are
// experimental and off -- and the probe treats a refusal as "unresolved",
// which is the honest answer. It never gets to: the auto-commit ahead of the
// refused DDL has already taken the savepoint with it, so the recovery fails
// and the whole comparison fails with it. The reported error names the
// rollback, which is the one thing that was not wrong.
//
// With the setting off the same sequence recovers and the comparison continues.
// SET LOCAL rather than SET, so it lasts exactly as long as the transaction and
// cannot reach the next borrower of a pooled connection (stokaro/ptah#2140).
//
// Asked of a CockroachDB banner alone. PostgreSQL has no such variable and
// answers `unrecognized configuration parameter`, and a failed statement
// inside a PostgreSQL transaction poisons every later one -- so a version this
// does not recognize is left alone rather than probed.
func (Service) KeepDDLInTransaction(ctx context.Context, tx *sql.Tx, info catalog.ServerInfo) error {
	if capability.BannerPlatform(info.Version) != platform.CockroachDB {
		return nil
	}
	if _, err := tx.ExecContext(ctx, "SET LOCAL autocommit_before_ddl = off"); err != nil {
		return fmt.Errorf("keep DDL inside the transaction: %w", err)
	}
	return nil
}

// MarkRead marks the object kinds a Spanner target has and Ptah does not
// model, so a description's silence about them is not read as their absence.
//
// Spanner change streams are the case that made it necessary. A change stream
// is a database object with its own lifecycle -- measured against the emulator
// behind PGAdapter 0.55.2, `CREATE CHANGE STREAM ttl_a_stream FOR ttl_a` is
// accepted and listed in information_schema.change_streams -- and nothing in
// Ptah models one. Before this the reader met them and said nothing, which is
// the failure stokaro/ptah#2236 is about: an unmodeled object dropped without a
// diagnostic is one nobody learns about until it stops existing.
//
// It is recorded for the TARGET rather than for what the read found. Recording
// only the streams a particular database happens to have would assert that the
// absence of every other one is authoritative, which is the under-claim that
// turns a presentation decision into a removal.
//
// It sits here rather than in the reader because this is where the dialect is
// known: the PostgreSQL-family reader is shared by every engine of the wire and
// is configured with capabilities rather than with a dialect name.
func (Service) MarkRead(schema *catalog.Database, info catalog.ServerInfo) *catalog.Database {
	return recordUnmodeledObjectKinds(schema, info.Dialect)
}

// recordUnmodeledObjectKinds is [Service.MarkRead] for dialect.
func recordUnmodeledObjectKinds(schema *catalog.Database, dialect string) *catalog.Database {
	if schema == nil || platform.NormalizeDialect(dialect) != platform.Spanner {
		return schema
	}
	schema.NotDescribed = schema.NotDescribed.With(coverage.Object{
		Kind:       spannerschema.CoverageChangeStream,
		Reason:     coverage.Unsupported,
		Provenance: coverage.DerivedFromTarget,
	})
	return schema
}
