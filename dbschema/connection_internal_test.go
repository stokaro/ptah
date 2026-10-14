package dbschema

// White-box testing required: this file exercises unexported DSN normalization
// helpers and connection option paths that are not directly observable through
// the public connection API alone.

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/connector"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/servertarget"
	"ptah.run/internal/sqlrunner"
)

type connectionTestWriter struct {
	executed string
}

func (w *connectionTestWriter) ExecuteSQL(_ context.Context, statement string, _ ...any) error {
	w.executed = statement
	return nil
}

func (w *connectionTestWriter) ExecContext(
	_ context.Context,
	statement string,
	_ ...any,
) (sql.Result, error) {
	w.executed = statement
	return driver.RowsAffected(1), nil
}

func (w *connectionTestWriter) DropAllTables(context.Context) error {
	return nil
}

func (w *connectionTestWriter) BeginTransaction(context.Context) (catalog.SchemaTransaction, error) {
	return nil, nil
}

func (w *connectionTestWriter) SetDryRun(bool) {}

func (w *connectionTestWriter) IsDryRun() bool {
	return false
}

type connectionTestExecutor struct {
	executed string
}

func (e *connectionTestExecutor) ExecuteSQL(_ context.Context, statement string, _ ...any) error {
	e.executed = statement
	return nil
}

func (*connectionTestExecutor) IsDryRun() bool {
	return false
}

type connectionQueryExecutor struct {
	runner   sqlrunner.Runner
	executed string
}

func (e *connectionQueryExecutor) ExecuteSQL(_ context.Context, statement string, _ ...any) error {
	e.executed = statement
	return nil
}

func (*connectionQueryExecutor) IsDryRun() bool { return false }

func (e *connectionQueryExecutor) SchemaQueryRunner() sqlrunner.Runner {
	return e.runner
}

func TestDatabaseConnectionWithExecutor_PreservesRootWriterForNarrowExecutor(t *testing.T) {
	c := qt.New(t)
	root := new(connectionTestWriter)
	executor := new(connectionTestExecutor)
	conn := &DatabaseConnection{writer: root}

	scoped := conn.WithExecutor(executor)
	_, err := scoped.ExecContext(t.Context(), "DIRECT")

	c.Assert(err, qt.IsNil)
	c.Assert(executor.executed, qt.Equals, "DIRECT")
	c.Assert(scoped.SchemaWriter(), qt.Equals, catalog.SchemaWriter(root))
	c.Assert(scoped.Writer(), qt.Equals, catalog.SchemaExecutor(executor))
}

func TestDatabaseConnectionWithExecutor_RebindsReaderToQueryableExecutor(t *testing.T) {
	c := qt.New(t)
	queried := false
	db := dbtest.OpenWithExec(
		t,
		func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
			queried = true
			return dbtest.QueryResult{
				Columns: []string{"value"},
				Rows:    [][]driver.Value{{int64(1)}},
			}, nil
		},
		func(string, []driver.NamedValue) (driver.Result, error) {
			return driver.RowsAffected(0), nil
		},
	)
	runner := sqlrunner.Runner(db.SQL)
	newReader := func(readerRunner sqlrunner.Runner) catalog.SchemaReader {
		return &connectionSessionReader{runner: readerRunner}
	}
	conn := &DatabaseConnection{newReader: newReader}
	executor := &connectionQueryExecutor{runner: runner}

	scoped := conn.WithExecutor(executor)
	_, err := scoped.Reader().ReadSchemaContext(t.Context())
	_, execErr := scoped.ExecContext(t.Context(), "SET search_path = app")

	c.Assert(err, qt.IsNil)
	c.Assert(execErr, qt.IsNil)
	c.Assert(queried, qt.IsTrue)
	c.Assert(executor.executed, qt.Equals, "SET search_path = app")
	c.Assert(scoped.Writer(), qt.Equals, catalog.SchemaExecutor(executor))
}

func TestDatabaseConnectionWithExecutor_DoesNotRebindUnavailableRunner(t *testing.T) {
	c := qt.New(t)
	root := &connectionSessionReader{}
	conn := &DatabaseConnection{
		reader: root,
		newReader: func(runner sqlrunner.Runner) catalog.SchemaReader {
			return &connectionSessionReader{runner: runner}
		},
	}
	executor := new(connectionQueryExecutor)

	scoped := conn.WithExecutor(executor)

	c.Assert(scoped.Reader(), qt.Equals, catalog.SchemaReader(root))
	c.Assert(scoped.Writer(), qt.Equals, catalog.SchemaExecutor(executor))
}

type connectionSessionReader struct {
	runner sqlrunner.Runner
}

func (r *connectionSessionReader) ReadSchema() (*catalog.Database, error) {
	return r.ReadSchemaContext(context.Background())
}

func (r *connectionSessionReader) ReadSchemaContext(ctx context.Context) (*catalog.Database, error) {
	var value int
	err := r.runner.QueryRowContext(ctx, "SELECT 1").Scan(&value)
	return &catalog.Database{}, err
}

type connectionSessionWriter struct {
	runner sqlrunner.Runner
}

func (w *connectionSessionWriter) ExecuteSQL(
	ctx context.Context,
	statement string,
	args ...any,
) error {
	_, err := w.runner.ExecContext(ctx, statement, args...)
	return err
}

func (w *connectionSessionWriter) DropAllTables(context.Context) error {
	return nil
}

func (w *connectionSessionWriter) BeginTransaction(context.Context) (catalog.SchemaTransaction, error) {
	return nil, nil
}

func (w *connectionSessionWriter) SetDryRun(bool) {}

func (w *connectionSessionWriter) IsDryRun() bool {
	return false
}

func TestDatabaseConnectionWithSession_RebindsAllDatabaseOperations(t *testing.T) {
	c := qt.New(t)
	db := dbtest.OpenWithExec(
		t,
		func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
			return dbtest.QueryResult{
				Columns: []string{"value"},
				Rows:    [][]driver.Value{{int64(1)}},
			}, nil
		},
		func(string, []driver.NamedValue) (driver.Result, error) {
			return driver.RowsAffected(1), nil
		},
	)
	db.SQL.SetMaxOpenConns(1)
	newReader := func(runner sqlrunner.Runner) catalog.SchemaReader {
		return &connectionSessionReader{runner: runner}
	}
	newWriter := func(runner sqlrunner.Runner, _ *sql.Conn) catalog.SchemaWriter {
		return &connectionSessionWriter{runner: runner}
	}
	rootRunner := sqlrunner.Runner(db.SQL)
	conn := &DatabaseConnection{
		db:        db.SQL,
		runner:    rootRunner,
		reader:    newReader(rootRunner),
		writer:    newWriter(rootRunner, nil),
		newReader: newReader,
		newWriter: newWriter,
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	t.Cleanup(cancel)

	err := conn.WithSession(ctx, func(scoped *DatabaseConnection) error {
		_, execErr := scoped.ExecContext(ctx, "DIRECT")
		c.Assert(execErr, qt.IsNil)
		_, readErr := scoped.Reader().ReadSchemaContext(ctx)
		c.Assert(readErr, qt.IsNil)
		writerErr := scoped.Writer().ExecuteSQL(ctx, "WRITER")
		c.Assert(writerErr, qt.IsNil)
		nested, nestedErr := scoped.Conn(ctx)
		c.Assert(nestedErr, qt.ErrorMatches, `database connection is already pinned to a session`)
		c.Assert(nested, qt.IsNil)
		currentErr := scoped.WithSessionOrCurrent(ctx, func(current *DatabaseConnection) error {
			c.Assert(current, qt.Equals, scoped)
			_, execErr := current.ExecContext(ctx, "CURRENT")
			return execErr
		})
		c.Assert(currentErr, qt.IsNil)
		c.Assert(scoped.Close(), qt.IsNil)
		return nil
	})

	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, "ROOT")
	c.Assert(err, qt.IsNil)
	c.Assert(db.QueryCount(), qt.Equals, 1)
	c.Assert(db.ExecCount(), qt.Equals, 4)
}

func TestResolveDatabaseCapabilities_MySQLKeepsVersionBaseline(t *testing.T) {
	c := qt.New(t)

	got := resolveDatabaseCapabilities(catalog.ServerInfo{
		Dialect: "mysql",
		Version: "8.4.0",
	})

	c.Assert(got.VersionSpecific, qt.IsTrue)
	c.Assert(got.Saturated, qt.IsFalse)
	c.Assert(got.Capabilities, qt.DeepEquals, capability.MySQL84())
	c.Assert(got.Capabilities.Has(capability.ForeignKeysRequireUniqueReference), qt.IsTrue)
	c.Assert(got.Capabilities.Has(capability.ForeignKeysRequireIndexedReference), qt.IsFalse)
}

// defaultCLILogLevel is the threshold internal/cli/cliobs.QuietDefaultLogger
// installs before any command runs, and therefore the level at which library
// slog calls reach a user's stderr on a default invocation. It is duplicated
// rather than imported because cliobs lives under internal/cli, which is the
// command tree, and a library package does not reach into it.
const defaultCLILogLevel = slog.LevelWarn

// captureResolutionReport runs the reporter with a default logger writing to a
// buffer at the given threshold, and returns everything it wrote.
func captureResolutionReport(t *testing.T, level slog.Level, info catalog.ServerInfo) string {
	t.Helper()
	var output bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: level})))
	defer slog.SetDefault(previousLogger)
	resolution := resolveDatabaseCapabilities(info)
	// Assembled the way getDatabaseInfoWithCapabilities assembles it. A test
	// that hand-wrote the note would report on a sentence production never
	// produces, and the reporter's own early return would go unmeasured.
	info.CapabilityNote = servertarget.VersionNote(info.Dialect, info.Version, resolution)
	reportCapabilityResolution(info, resolution)
	return output.String()
}

// TestReportCapabilityResolution covers the one production caller of the
// version-aware selector, and pins WHICH of its cases a person sees.
//
// One does. A server newer than the newest measured line plans with that
// line's preset, so anything the newer release gained or lost is unmodeled, and
// only the operator can decide whether that matters. It is rare and it ends
// when the line is measured (stokaro/ptah#916).
//
// The others do not, and the rows below are what keeps that from drifting into
// a blanket WARN. A dialect with no version ladder resolves the same way on
// every connection forever -- every SQL Server command would carry a permanent,
// unactionable line. cliobs.QuietDefaultLogger's contract is that a clean run
// emits nothing at WARN or above, and those runs are clean.
//
// The debug column is the other half: every case is recorded, not dropped, and
// `--log-level debug` shows it. The quiet rows are the non-interference control
// — without them, a reporter that said nothing at any level would pass.
func TestReportCapabilityResolution(t *testing.T) {
	tests := []struct {
		name           string
		info           catalog.ServerInfo
		wantDebug      []string
		wantDebugQuiet bool
		wantWarn       []string
	}{
		{
			name: "mysql inside the measured line says nothing at all",
			info: catalog.ServerInfo{Dialect: "mysql", Version: "9.7.1"},
			// The integration matrix runs mysql:9.7.
			wantDebugQuiet: true,
		},
		{
			name:           "mysql current measured line says nothing at all",
			info:           catalog.ServerInfo{Dialect: "mysql", Version: "26.7.0"},
			wantDebugQuiet: true,
		},
		{
			name:           "mariadb current measured line says nothing at all",
			info:           catalog.ServerInfo{Dialect: "mariadb", Version: "12.3.0-MariaDB"},
			wantDebugQuiet: true,
		},
		{
			name:           "postgres current measured line says nothing at all",
			info:           catalog.ServerInfo{Dialect: "postgres", Version: "PostgreSQL 18.4 (Debian)"},
			wantDebugQuiet: true,
		},
		{
			name:      "mysql past the newest measured line reaches a person",
			info:      catalog.ServerInfo{Dialect: "mysql", Version: "99.0"},
			wantDebug: []string{"level=WARN", "newer than the newest measured release line", "dialect=mysql", "version=99.0", "newest_measured=26.7"},
			wantWarn:  []string{"level=WARN", "newer than the newest measured release line", "newest_measured=26.7"},
		},
		{
			name:      "mariadb past the newest measured line reaches a person",
			info:      catalog.ServerInfo{Dialect: "mariadb", Version: "99.0-MariaDB"},
			wantDebug: []string{"level=WARN", "dialect=mariadb", "version=99.0-MariaDB", "newest_measured=12.3"},
			wantWarn:  []string{"level=WARN", "newest_measured=12.3"},
		},
		{
			name:      "postgres past the newest measured line reaches a person",
			info:      catalog.ServerInfo{Dialect: "postgres", Version: "PostgreSQL 99.0"},
			wantDebug: []string{"level=WARN", "dialect=postgres", "newest_measured=18.x"},
			wantWarn:  []string{"level=WARN", "newest_measured=18.x"},
		},
		{
			name: "an unparseable version stays a debug-level fallback",
			info: catalog.ServerInfo{Dialect: "mysql", Version: "who knows"},
			// A gap or an unreadable banner stays a debug record: it is not
			// rare, it does not end, and there is nothing to act on.
			wantDebug: []string{"level=DEBUG", "does not name a mysql server version Ptah can read"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			atDefault := captureResolutionReport(t, defaultCLILogLevel, tt.info)
			c.Assert(atDefault == "", qt.Equals, len(tt.wantWarn) == 0,
				qt.Commentf("emitted on default stderr: %q", atDefault))
			for _, want := range tt.wantWarn {
				c.Assert(atDefault, qt.Contains, want)
			}

			atDebug := captureResolutionReport(t, slog.LevelDebug, tt.info)
			c.Assert(atDebug == "", qt.Equals, tt.wantDebugQuiet, qt.Commentf("logged: %q", atDebug))
			for _, want := range tt.wantDebug {
				c.Assert(atDebug, qt.Contains, want)
			}
		})
	}
}

func TestDatabaseConnectionWithSession_MySQLRelaxationIsCallbackScoped(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{
			Columns: []string{"restrict_fk_on_non_standard_key"},
			Rows:    [][]driver.Value{{int64(0)}},
		}, nil
	})
	db.SQL.SetMaxOpenConns(1)
	newReader := func(runner sqlrunner.Runner) catalog.SchemaReader {
		return &connectionSessionReader{runner: runner}
	}
	newWriter := func(runner sqlrunner.Runner, _ *sql.Conn) catalog.SchemaWriter {
		return &connectionSessionWriter{runner: runner}
	}
	baseline := capability.MySQL84()
	rootRunner := sqlrunner.Runner(db.SQL)
	conn := &DatabaseConnection{
		db:     db.SQL,
		runner: rootRunner,
		info: catalog.ServerInfo{
			Dialect:      "mysql",
			Version:      "8.4.0",
			Capabilities: baseline,
		},
		reader:    newReader(rootRunner),
		writer:    newWriter(rootRunner, nil),
		newReader: newReader,
		newWriter: newWriter,
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	t.Cleanup(cancel)

	c.Assert(conn.Info().Capabilities, qt.DeepEquals, baseline)
	err := conn.WithSession(ctx, func(scoped *DatabaseConnection) error {
		c.Assert(scoped.Info().Capabilities.Has(capability.ForeignKeysRequireUniqueReference), qt.IsFalse)
		c.Assert(scoped.Info().Capabilities.Has(capability.ForeignKeysRequireIndexedReference), qt.IsTrue)
		c.Assert(conn.Info().Capabilities, qt.DeepEquals, baseline)
		return nil
	})

	c.Assert(err, qt.IsNil)
	c.Assert(db.QueryCount(), qt.Equals, 1)
	c.Assert(conn.Info().Capabilities, qt.DeepEquals, baseline)
}

// A connection's refinement belongs to every session it pins. A YDB cluster's
// feature flags are read once, at connect time, and a session that started
// from the release line's preset again would plan a unique index on an
// existing table as refused on a cluster that takes it.
func TestDatabaseConnectionWithSession_KeepsTheConnectionRefinement(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{}, fmt.Errorf("no query is expected, got %q", query)
	})
	db.SQL.SetMaxOpenConns(1)
	newReader := func(runner sqlrunner.Runner) catalog.SchemaReader {
		return &connectionSessionReader{runner: runner}
	}
	newWriter := func(runner sqlrunner.Runner, _ *sql.Conn) catalog.SchemaWriter {
		return &connectionSessionWriter{runner: runner}
	}
	refined := capability.YDB262().With(capability.UniqueIndexOnExistingTable, true)
	rootRunner := sqlrunner.Runner(db.SQL)
	conn := &DatabaseConnection{
		db:     db.SQL,
		runner: rootRunner,
		info: catalog.ServerInfo{
			Dialect:      platform.YDB,
			Version:      "26.2.1.14",
			Capabilities: refined,
		},
		reader:    newReader(rootRunner),
		writer:    newWriter(rootRunner, nil),
		newReader: newReader,
		newWriter: newWriter,
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	t.Cleanup(cancel)

	var scopedCapabilities capability.Capabilities
	err := conn.WithSession(ctx, func(scoped *DatabaseConnection) error {
		scopedCapabilities = scoped.Info().Capabilities
		return nil
	})

	c.Assert(err, qt.IsNil)
	c.Assert(scopedCapabilities, qt.DeepEquals, refined)
	c.Assert(db.QueryCount(), qt.Equals, 0)
}

// TestWireDialectDetectionAgreesWithTheCapabilityResolver holds the two readers
// of a SELECT version() banner to one answer.
//
// getDatabaseInfo asks detect*WireDialect which product it is connected to, and
// then hands that dialect plus the same banner to
// capability.ResolveServerVersion, which asks the same question again to pick
// the preset the connection will plan with. When the two disagree, the second
// answer silently wins and the connection is planned as a server it is not:
// measured, "cockroachdb" plus "PostgreSQL-compatible server 25.4" resolved to
// PostgreSQL capabilities, and "spanner" plus "PostgreSQL 14.1" replaced
// SpannerPostgres with Postgres14 across 19 keys — materialized_views,
// functions and triggers among them. Nothing in the suite noticed, because each
// reader had its own table and each table was tested alone.
//
// This asserts the fixed point rather than either table: whatever dialect the
// detection settles on, the resolver handed that dialect must answer from it.
func TestWireDialectDetectionAgreesWithTheCapabilityResolver(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		version  string
	}{
		{
			name:     "postgres wire, plain PostgreSQL banner",
			declared: "postgres",
			version:  "PostgreSQL 16.3 (Debian 16.3-1.pgdg120+1)",
		},
		{
			name:     "postgres wire, CockroachDB banner on a postgres URL",
			declared: "postgres",
			version:  "CockroachDB CCL v25.4.5 (x86_64-pc-linux-gnu)",
		},
		{
			name:     "postgres wire, YugabyteDB banner on a postgres URL",
			declared: "postgres",
			version:  "PostgreSQL 11.2-YB-2.25.1.0-b0 on x86_64-pc-linux-gnu, compiled by clang, YugabyteDB",
		},
		{
			name:     "postgres wire, Spanner banner on a postgres URL",
			declared: "postgres",
			version:  "Cloud Spanner PostgreSQL interface",
		},
		{
			name:     "postgres wire, explicit cockroachdb with a generic banner",
			declared: "cockroachdb",
			version:  "PostgreSQL-compatible server 25.4",
		},
		{
			name:     "postgres wire, explicit cockroachdb with an unversioned generic banner",
			declared: "cockroachdb",
			version:  "PostgreSQL-compatible server",
		},
		{
			name:     "postgres wire, explicit cockroachdb with its own banner",
			declared: "cockroachdb",
			version:  "CockroachDB CCL v25.4.5 (x86_64-pc-linux-gnu)",
		},
		{
			name:     "postgres wire, explicit yugabytedb with a PostgreSQL engine banner",
			declared: "yugabytedb",
			version:  "PostgreSQL 11.2 on x86_64-pc-linux-gnu",
		},
		{
			name:     "postgres wire, explicit yugabytedb with its own banner",
			declared: "yugabytedb",
			version:  "PostgreSQL 15.2-YB-2026.1.0.0-b0",
		},
		{
			name:     "postgres wire, explicit spanner with a plain PostgreSQL banner",
			declared: "spanner",
			version:  "PostgreSQL 14.1",
		},
		{
			name:     "postgres wire, explicit spanner with its own banner",
			declared: "spanner",
			version:  "Cloud Spanner PostgreSQL interface",
		},
		{
			name:     "mysql wire, MySQL banner",
			declared: "mysql",
			version:  "9.7.1",
		},
		{
			name:     "mysql wire, MariaDB banner on a mysql URL",
			declared: "mysql",
			version:  "10.11.15-MariaDB-ubu2204",
		},
		{
			name:     "mysql wire, the replication prefix on a mysql URL",
			declared: "mysql",
			version:  "5.5.5-10.11.15-MariaDB-ubu2204",
		},
		{
			name:     "mysql wire, a mariadb URL naming a server that does not announce MariaDB",
			declared: "mariadb",
			version:  "MySQL-compatible server 8.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			db := bannerDB(c, tt.version)

			info, err := getDatabaseInfo(
				c.Context(), serviceFor(tt.declared), &connector.Pool{DB: db.SQL},
				requestFor(c, tt.declared+"://localhost/app"),
			)

			c.Assert(err, qt.IsNil)
			detected := info.Dialect
			resolution := capability.ResolveServerVersion(detected, tt.version)

			c.Assert(detected, qt.Not(qt.Equals), "")
			c.Assert(resolution.ResolvedDialect, qt.Equals, detected)
			c.Assert(resolution.Capabilities, qt.DeepEquals, capability.ForServerVersion(detected, tt.version))
		})
	}
}

// bannerDB is a server that answers the questions a PostgreSQL-wire or
// MySQL-wire connection asks when it is described: the version banner, the
// session's schema and CockroachDB's integer width.
func bannerDB(c *qt.C, version string) *dbtest.DB {
	answers := map[string]dbtest.QueryResult{
		"SELECT version()":        {Columns: []string{"version"}, Rows: [][]driver.Value{{version}}},
		"SELECT VERSION()":        {Columns: []string{"VERSION()"}, Rows: [][]driver.Value{{version}}},
		"SELECT current_schema()": {Columns: []string{"current_schema"}, Rows: [][]driver.Value{{"public"}}},
		"SHOW default_int_size":   {Columns: []string{"default_int_size"}, Rows: [][]driver.Value{{"8"}}},
	}
	return dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		answer, known := answers[query]
		c.Assert(known, qt.IsTrue, qt.Commentf("unexpected query %q", query))
		return answer, nil
	})
}

// requestFor is the request a connection to dbURL is opened from.
func requestFor(c *qt.C, dbURL string) connector.Request {
	parsed, err := parseDatabaseURL(dbURL)
	c.Assert(err, qt.IsNil)
	dialect, err := connectionDialect(parsed)
	c.Assert(err, qt.IsNil)
	described, known := platform.Lookup(dialect)
	c.Assert(known, qt.IsTrue)
	return connector.Request{Dialect: described, URL: dbURL, Parsed: parsed}
}

func TestDatabaseConnectionInfoClonesCapabilities(t *testing.T) {
	c := qt.New(t)

	conn := &DatabaseConnection{
		info: catalog.ServerInfo{
			Dialect:      "cockroachdb",
			Capabilities: capability.CockroachDB23(),
		},
	}

	info := conn.Info()
	info.Capabilities = info.Capabilities.With(capability.XMLType, true)

	c.Assert(conn.Info().Capabilities.Has(capability.XMLType), qt.IsFalse)
}

// TestConnectToDatabase_ReadsTheDialectOfASocketURL is the other half: the
// scheme detection has to recognize the same shape, or the URL never reaches
// the converter as MySQL at all.
//
// The connection itself is expected to fail — no socket is listening — but the
// error has to be about reaching the database rather than about parsing what
// was asked for.
func TestConnectToDatabase_ReadsTheDialectOfASocketURL(t *testing.T) {
	c := qt.New(t)

	_, err := ConnectToDatabase(context.Background(), "mysql://user:pass@unix(/tmp/ptah-absent.sock)/db")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "invalid database URL")
	c.Assert(err.Error(), qt.Not(qt.Contains), "unsupported database dialect")
}

// TestParseDatabaseURL_StillRefusesAnAddressThatIsNotOne is the control that
// keeps the Windows accommodation from swallowing real errors. A colon that is
// a malformed port stays a malformed port.
func TestParseDatabaseURL_StillRefusesAnAddressThatIsNotOne(t *testing.T) {
	c := qt.New(t)

	_, err := parseDatabaseURL("postgres://host:notaport/db")

	c.Assert(err, qt.IsNotNil)
}

// TestParseDatabaseURL_ReadsTheDatabaseOfASocketURL checks what the parse is
// used for rather than only that it succeeded.
//
// The parsed URL does not only carry the scheme: its path is where the MySQL
// branch of getDatabaseInfo reads the database name, so recognizing only @tcp(
// left a socket address parsed as a host called "unix(" with the socket path
// folded into the name, and readers and writers targeted a database called
// tmp/mysql.sock)/db.
func TestParseDatabaseURL_ReadsTheDatabaseOfASocketURL(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{name: "a socket target names its database", url: "mysql://user:pass@unix(/tmp/mysql.sock)/shop", want: "shop"},
		{name: "a mariadb socket target too", url: "mariadb://u:p@unix(/var/run/mysqld/mysqld.sock)/shop", want: "shop"},
		{name: "a maria socket target too", url: "maria://u:p@unix(/var/run/mysqld/mysqld.sock)/shop", want: "shop"},
		{name: "a maria TCP target", url: "maria://u:p@tcp(127.0.0.1:3306)/shop", want: "shop"},
		{name: "a socket URL names its database in the query", url: "mysql+unix://u:p@/tmp/mysql.sock?database=shop", want: "shop"},
		{name: "a socket URL in upper case", url: "MARIADB+UNIX://u@/tmp/mysql.sock?database=shop", want: "shop"},
		{name: "the TCP spelling is unchanged", url: "mysql://user:pass@tcp(127.0.0.1:3306)/shop", want: "shop"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parsed, err := parseDatabaseURL(test.url)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.TrimPrefix(parsed.Path, "/"), qt.Equals, test.want)
		})
	}
}

// TestParseDatabaseURL_ReadsANetworkDSNWithoutCredentials covers the half of
// the grammar the first recognizer missed.
//
// Credentials are optional in go-sql-driver's DSN, so tcp(localhost:3306)/db
// and unix(/tmp/mysql.sock)/db are valid targets. Matching only "@tcp(" left
// the first refused as an invalid URL and the second parsed with the socket
// path folded into the database name.
func TestParseDatabaseURL_ReadsANetworkDSNWithoutCredentials(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{name: "a TCP target with no credentials", url: "mysql://tcp(localhost:3306)/shop", want: "shop"},
		{name: "a socket target with no credentials", url: "mysql://unix(/tmp/mysql.sock)/shop", want: "shop"},
		{name: "credentials are still read", url: "mysql://user:pass@tcp(localhost:3306)/shop", want: "shop"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parsed, err := parseDatabaseURL(test.url)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.TrimPrefix(parsed.Path, "/"), qt.Equals, test.want)
		})
	}
}

// mysqlSessionAnswers is a MariaDB server, as far as getDatabaseInfo asks.
var mysqlSessionAnswers = map[string]dbtest.QueryResult{
	"SELECT VERSION()": {Columns: []string{"VERSION()"}, Rows: [][]driver.Value{{"11.8.9-MariaDB-ubu2404"}}},
}

// TestGetDatabaseInfo_ReadsTheDatabaseOfASocketURL checks that the database a
// socket URL names is the one the connection reports. The path of that URL is
// the socket, so a reader of the path reports a database called
// run/mysqld/mysqld.sock while the driver connects to shop.
func TestGetDatabaseInfo_ReadsTheDatabaseOfASocketURL(t *testing.T) {
	c := qt.New(t)
	const socketURL = "mariadb+unix://root@/run/mysqld/mysqld.sock?database=shop"
	db := dbtest.Open(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		return mysqlSessionAnswers[query], nil
	})
	info, err := getDatabaseInfo(
		t.Context(), serviceFor(platform.MariaDB), &connector.Pool{DB: db.SQL}, requestFor(qt.New(t), socketURL),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(info.Schema, qt.Equals, "shop")
	c.Assert(info.Dialect, qt.Equals, platform.MariaDB)
}

// TestConnectToDatabase_ReachesTheDriverForACredentialFreeSocketTarget is the
// public-path control stokaro/ptah#1540 asked for: the two halves are wired
// together by ConnectToDatabase, and a unit test of either half alone cannot
// see them disagree.
//
// The socket is chosen because it needs no network and no database. A target
// the driver understood fails at dial, naming the path as a file that is not
// there. A target it did not understand fails earlier and differently -- with
// the converter's old recognition the path became a hostname and the failure
// was a DNS lookup of "unix()/tmp/...", which is both wrong and a name no
// caller wrote.
func TestConnectToDatabase_ReachesTheDriverForACredentialFreeSocketTarget(t *testing.T) {
	c := qt.New(t)
	socket := filepath.Join(c.TempDir(), "absent.sock")

	_, err := ConnectToDatabase(t.Context(), "mysql://unix("+socket+")/shop")

	c.Assert(err, qt.ErrorMatches, `.*dial unix `+regexp.QuoteMeta(socket)+`.*`)
}

// TestParseDatabaseURL_RefusesAMalformedQueryOnAWindowsPath keeps the Windows
// accommodation from turning every parse error into success.
//
// The fallback exists for one reason: a drive letter's colon is not a port
// separator. It is not a licence to accept anything else net/url refused. A
// malformed escape in the query is a real error, and admitting it means
// Query() silently drops the pair -- so an attempted mode=ro restriction
// disappears and the database opens writable, which is the failure this whole
// area keeps producing when something is dropped instead of refused.
func TestParseDatabaseURL_RefusesAMalformedQueryOnAWindowsPath(t *testing.T) {
	c := qt.New(t)

	_, err := parseDatabaseURL(`sqlite://C:\tmp\app.db?mode=%zz`)

	c.Assert(err, qt.IsNotNil)
	// The refusal has to name the query. Passing on the parse error that got
	// here would tell an operator whose address has no port that the port is
	// invalid, which is the diagnostic this whole path exists to remove.
	c.Assert(err.Error(), qt.Contains, "query")
	c.Assert(err.Error(), qt.Not(qt.Contains), "port")
}

// A read-only request a driver cannot carry has to go somewhere, and where it
// goes is not observable through WithIsolatedQuerySession without an Oracle
// server: every other dialect passes the options through and answers the same
// either way.
func TestIsolatedSessionReadOnly_HappyPath(t *testing.T) {
	t.Run("Oracle carries the request to the server", func(t *testing.T) {
		c := qt.New(t)
		asked := &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelReadCommitted}

		carried, statement := isolatedSessionReadOnly(serviceFor(platform.Oracle), asked)

		c.Assert(statement, qt.Equals, "SET TRANSACTION READ ONLY")
		c.Assert(carried.ReadOnly, qt.IsFalse)
		c.Assert(carried.Isolation, qt.Equals, sql.LevelReadCommitted)
		// The caller's options are not the ones handed to the driver: a caller
		// that reuses them would otherwise find its own request erased.
		c.Assert(asked.ReadOnly, qt.IsTrue)
	})

	t.Run("a dialect whose driver carries it is untouched", func(t *testing.T) {
		c := qt.New(t)
		asked := &sql.TxOptions{ReadOnly: true}

		carried, statement := isolatedSessionReadOnly(serviceFor(platform.Postgres), asked)

		c.Assert(statement, qt.Equals, "")
		c.Assert(carried, qt.Equals, asked)
	})

	t.Run("Oracle without the request is untouched", func(t *testing.T) {
		c := qt.New(t)
		asked := &sql.TxOptions{}

		carried, statement := isolatedSessionReadOnly(serviceFor(platform.Oracle), asked)

		c.Assert(statement, qt.Equals, "")
		c.Assert(carried, qt.Equals, asked)
	})

	t.Run("no options at all", func(t *testing.T) {
		c := qt.New(t)

		carried, statement := isolatedSessionReadOnly(serviceFor(platform.Oracle), nil)

		c.Assert(statement, qt.Equals, "")
		c.Assert(carried, qt.IsNil)
	})
}

// TestServices_ServeEveryDialectWithADriver pins the registry against the
// descriptors: a dialect that names a driver is one Ptah connects to, so a
// connection to it must find its engine's connector, and one that names none
// must not. The registry is unexported, which is why this is white-box: the
// exported path to it opens a connection.
func TestServices_ServeEveryDialectWithADriver(t *testing.T) {
	for _, described := range platform.Dialects().Dialects() {
		t.Run(described.Name, func(t *testing.T) {
			c := qt.New(t)

			_, found := services().Lookup(described.Name)

			c.Assert(found, qt.Equals, described.Driver != "")
		})
	}
}

