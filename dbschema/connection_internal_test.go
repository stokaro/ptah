package dbschema

// White-box testing required: this file exercises unexported DSN normalization
// helpers and connection option paths that are not directly observable through
// the public connection API alone.

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"log/slog"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
	"go.5x5.cz/ptah/internal/sqlrunner"
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

func (w *connectionTestWriter) BeginTransaction(context.Context) (types.SchemaTransaction, error) {
	return nil, nil
}

func (w *connectionTestWriter) SetDryRun(bool) {}

func (w *connectionTestWriter) IsDryRun() bool {
	return false
}

type connectionTestExecutor struct{}

func (*connectionTestExecutor) ExecuteSQL(context.Context, string, ...any) error {
	return nil
}

func (*connectionTestExecutor) IsDryRun() bool {
	return false
}

func TestDatabaseConnectionWithExecutor_PreservesRootWriterForNarrowExecutor(t *testing.T) {
	c := qt.New(t)
	root := new(connectionTestWriter)
	executor := new(connectionTestExecutor)
	conn := &DatabaseConnection{writer: root}

	scoped := conn.WithExecutor(executor)

	c.Assert(scoped.SchemaWriter(), qt.Equals, types.SchemaWriter(root))
	c.Assert(scoped.Writer(), qt.Equals, types.SchemaExecutor(executor))
}

type connectionSessionReader struct {
	runner sqlrunner.Runner
}

func (r *connectionSessionReader) ReadSchema() (*types.DBSchema, error) {
	var value int
	err := r.runner.QueryRow("SELECT 1").Scan(&value)
	return &types.DBSchema{}, err
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

func (w *connectionSessionWriter) BeginTransaction(context.Context) (types.SchemaTransaction, error) {
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
	newReader := func(runner sqlrunner.Runner) types.SchemaReader {
		return &connectionSessionReader{runner: runner}
	}
	newWriter := func(runner sqlrunner.Runner, _ *sql.Conn) types.SchemaWriter {
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
		_, readErr := scoped.Reader().ReadSchema()
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

	got := resolveDatabaseCapabilities(types.DBInfo{
		Dialect: "mysql",
		Version: "8.4.0",
	})

	c.Assert(got.VersionSpecific, qt.IsTrue)
	c.Assert(got.Saturated, qt.IsFalse)
	c.Assert(got.Capabilities, qt.DeepEquals, capability.MySQL84())
	c.Assert(got.Capabilities.Has(capability.ForeignKeysRequireUniqueReference), qt.IsTrue)
	c.Assert(got.Capabilities.Has(capability.ForeignKeysRequireIndexedReference), qt.IsFalse)
}

// defaultCLILogLevel is the threshold cmd/internal/cliobs.QuietDefaultLogger
// installs before any command runs, and therefore the level at which library
// slog calls reach a user's stderr on a default invocation. It is duplicated
// rather than imported because cliobs lives under cmd/internal and this
// package cannot reach it.
const defaultCLILogLevel = slog.LevelWarn

// captureResolutionReport runs the reporter with a default logger writing to a
// buffer at the given threshold, and returns everything it wrote.
func captureResolutionReport(t *testing.T, level slog.Level, info types.DBInfo) string {
	t.Helper()
	var output bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: level})))
	defer slog.SetDefault(previousLogger)
	reportCapabilityResolution(info, resolveDatabaseCapabilities(info))
	return output.String()
}

// TestReportCapabilityResolution covers the one production caller of the
// version-aware selector.
//
// Every row asserts the same thing first: nothing is written at the level a
// default command runs at. A saturated resolution is not an incident — the
// integration matrix runs postgres:18, which is saturated against the
// PostgreSQL 17 line, so a WARN there fires on every connection to a server
// Ptah supports. It did, and it broke 25 subtests that assert a clean error
// stream. cliobs.QuietDefaultLogger's contract is that a clean run emits
// nothing at WARN or above; a supported server is a clean run.
//
// The debug column is the other half: the fact is recorded, not dropped, and
// `--log-level debug` shows it. The quiet row is the non-interference control
// — without it, a reporter that said nothing at any level would pass.
func TestReportCapabilityResolution(t *testing.T) {
	tests := []struct {
		name           string
		info           types.DBInfo
		wantDebug      []string
		wantDebugQuiet bool
	}{
		{
			name: "mysql inside the measured line says nothing at all",
			info: types.DBInfo{Dialect: "mysql", Version: "9.7.1"},
			// The integration matrix runs mysql:9.7.
			wantDebugQuiet: true,
		},
		{
			name:      "mysql past the newest measured line is recorded at debug",
			info:      types.DBInfo{Dialect: "mysql", Version: "26.7.0"},
			wantDebug: []string{"level=DEBUG", "newest measured capability line", "dialect=mysql", "version=26.7.0", "newest_measured=9.x"},
		},
		{
			name:      "mariadb past the newest measured line is recorded at debug",
			info:      types.DBInfo{Dialect: "mariadb", Version: "12.3.0-MariaDB"},
			wantDebug: []string{"level=DEBUG", "dialect=mariadb", "version=12.3.0-MariaDB", "newest_measured=11.x"},
		},
		{
			name:      "postgres past the newest measured line is recorded at debug",
			info:      types.DBInfo{Dialect: "postgres", Version: "PostgreSQL 18.4 (Debian)"},
			wantDebug: []string{"level=DEBUG", "dialect=postgres", "newest_measured=17.x"},
		},
		{
			name:      "an unparseable version stays a debug-level fallback",
			info:      types.DBInfo{Dialect: "mysql", Version: "who knows"},
			wantDebug: []string{"level=DEBUG", "falling back to dialect default capabilities"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			atDefault := captureResolutionReport(t, defaultCLILogLevel, tt.info)
			c.Assert(atDefault, qt.Equals, "", qt.Commentf("emitted on default stderr: %q", atDefault))

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
	newReader := func(runner sqlrunner.Runner) types.SchemaReader {
		return &connectionSessionReader{runner: runner}
	}
	newWriter := func(runner sqlrunner.Runner, _ *sql.Conn) types.SchemaWriter {
		return &connectionSessionWriter{runner: runner}
	}
	baseline := capability.MySQL84()
	rootRunner := sqlrunner.Runner(db.SQL)
	conn := &DatabaseConnection{
		db:     db.SQL,
		runner: rootRunner,
		info: types.DBInfo{
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

func TestRefineMySQLForeignKeyCapabilities_RestrictionEnabled(t *testing.T) {
	c := qt.New(t)
	probeCalls := 0

	got, err := refineMySQLForeignKeyCapabilities(
		"mysql",
		capability.MySQL84(),
		capability.MySQL84(),
		func(destination ...any) error {
			probeCalls++
			value := destination[0].(*int64)
			*value = 1
			return nil
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(probeCalls, qt.Equals, 1)
	c.Assert(got.Has(capability.ForeignKeysRequireUniqueReference), qt.IsTrue)
	c.Assert(got.Has(capability.ForeignKeysRequireIndexedReference), qt.IsFalse)
	c.Assert(got.Validate(), qt.IsNil)
}

func TestRefineMySQLForeignKeyCapabilities_RestrictionDisabled(t *testing.T) {
	c := qt.New(t)

	got, err := refineMySQLForeignKeyCapabilities(
		"mysql",
		capability.MySQL84(),
		capability.MySQL84(),
		func(destination ...any) error {
			value := destination[0].(*int64)
			*value = 0
			return nil
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(got.Has(capability.ForeignKeysRequireUniqueReference), qt.IsFalse)
	c.Assert(got.Has(capability.ForeignKeysRequireIndexedReference), qt.IsTrue)
	c.Assert(got.Validate(), qt.IsNil)
}

func TestRefineMySQLForeignKeyCapabilities_LegacyPresetSkipsProbe(t *testing.T) {
	c := qt.New(t)
	probeCalls := 0

	got, err := refineMySQLForeignKeyCapabilities(
		"mysql",
		capability.MySQL8019(),
		capability.MySQL8019(),
		func(...any) error {
			probeCalls++
			return nil
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(probeCalls, qt.Equals, 0)
	c.Assert(got, qt.DeepEquals, capability.MySQL8019())
}

func TestRefineMySQLForeignKeyCapabilities_RestrictionEnabledRestoresStrictPolicy(t *testing.T) {
	c := qt.New(t)

	got, err := refineMySQLForeignKeyCapabilities(
		"mysql",
		capability.MySQL84().
			With(capability.ForeignKeysRequireUniqueReference, false).
			With(capability.ForeignKeysRequireIndexedReference, true),
		capability.MySQL84(),
		func(destination ...any) error {
			value := destination[0].(*int64)
			*value = 1
			return nil
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(got.Has(capability.ForeignKeysRequireUniqueReference), qt.IsTrue)
	c.Assert(got.Has(capability.ForeignKeysRequireIndexedReference), qt.IsFalse)
	c.Assert(got.Validate(), qt.IsNil)
}

func TestRefineMySQLForeignKeyCapabilities_ProbeFailure(t *testing.T) {
	c := qt.New(t)

	got, err := refineMySQLForeignKeyCapabilities(
		"mysql",
		capability.MySQL84(),
		capability.MySQL84(),
		func(...any) error { return driver.ErrBadConn },
	)

	c.Assert(err, qt.ErrorMatches, `query restrict_fk_on_non_standard_key: driver: bad connection`)
	c.Assert(got, qt.IsNil)
}

func TestConvertClickHouseURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "passthrough canonical URL",
			input:    "clickhouse://default:secret@localhost:9000/analytics",
			expected: "clickhouse://default:secret@localhost:9000/analytics",
		},
		{
			name:     "rewrites uppercase scheme",
			input:    "CLICKHOUSE://default@localhost:9000/db",
			expected: "clickhouse://default@localhost:9000/db",
		},
		{
			name:     "returns input on malformed URL",
			input:    "::not-a-url::",
			expected: "::not-a-url::",
		},
		{
			name:     "native TLS port 9440 round-trips",
			input:    "clickhouse://default@localhost:9440/analytics",
			expected: "clickhouse://default@localhost:9440/analytics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(convertClickHouseURL(tt.input), qt.Equals, tt.expected)
		})
	}
}

func TestConvertClickHouseURL_PreservesQueryParameters(t *testing.T) {
	c := qt.New(t)

	got := convertClickHouseURL("clickhouse://default:secret@localhost:9000/analytics?secure=true&dial_timeout=10s")

	c.Assert(got, qt.Contains, "clickhouse://default:secret@localhost:9000/analytics?")
	for kv := range strings.SplitSeq("secure=true&dial_timeout=10s", "&") {
		c.Assert(got, qt.Contains, kv)
	}
}

func TestConvertClickHouseURL_ExactQueryRoundTrips(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "preserves secure=true on native port",
			input:    "clickhouse://default@localhost:9000/analytics?secure=true",
			expected: "clickhouse://default@localhost:9000/analytics?secure=true",
		},
		{
			name:     "HTTP-SSL port 8443 with secure flag round-trips",
			input:    "clickhouse://default@localhost:8443/db?secure=true",
			expected: "clickhouse://default@localhost:8443/db?secure=true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(convertClickHouseURL(tt.input), qt.Equals, tt.expected)
		})
	}
}

func TestConvertSQLiteURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "relative file host",
			input:    "sqlite://test.db",
			expected: "test.db?_pragma=foreign_keys%281%29",
		},
		{
			name:     "relative nested file",
			input:    "sqlite://data/app.db",
			expected: "data/app.db?_pragma=foreign_keys%281%29",
		},
		{
			name:     "absolute file path",
			input:    "sqlite:///tmp/app.db",
			expected: "/tmp/app.db?_pragma=foreign_keys%281%29",
		},
		{
			name:     "memory database",
			input:    "sqlite:///:memory:",
			expected: ":memory:?_pragma=foreign_keys%281%29",
		},
		{
			name:     "uri memory database",
			input:    "sqlite:file:memdb1?mode=memory&cache=shared",
			expected: "file:memdb1?_pragma=foreign_keys%281%29&cache=shared&mode=memory",
		},
		{
			name:     "preserves explicit foreign keys pragma",
			input:    "sqlite:///:memory:?_pragma=foreign_keys(0)",
			expected: ":memory:?_pragma=foreign_keys%280%29",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(convertSQLiteURL(tt.input), qt.Equals, tt.expected)
		})
	}
}

func TestIsSQLiteMemoryDSN(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
		want bool
	}{
		{name: "default empty dsn", dsn: "", want: true},
		{name: "memory path", dsn: ":memory:?_pragma=foreign_keys%281%29", want: true},
		{name: "anonymous uri memory path", dsn: "file::memory:?_pragma=foreign_keys%281%29&cache=shared", want: true},
		{name: "uri memory mode", dsn: "file:memdb1?_pragma=foreign_keys%281%29&cache=shared&mode=memory", want: true},
		{name: "file path", dsn: "test.db?_pragma=foreign_keys%281%29", want: false},
		{name: "absolute file path", dsn: "/tmp/app.db?_pragma=foreign_keys%281%29", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(isSQLiteMemoryDSN(tt.dsn), qt.Equals, tt.want)
		})
	}
}

func TestConvertSQLServerURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "canonical sqlserver URL passes through",
			input:    "sqlserver://sa:pass@localhost:1433?database=ptah&encrypt=disable",
			expected: "sqlserver://sa:pass@localhost:1433?database=ptah&encrypt=disable",
		},
		{
			name:     "mssql alias rewrites to preferred driver scheme",
			input:    "mssql://sa:pass@localhost:1433?database=ptah&encrypt=disable",
			expected: "sqlserver://sa:pass@localhost:1433?database=ptah&encrypt=disable",
		},
		{
			name:     "drops ptah-only schema parameter before driver sees URL",
			input:    "mssql://sa:pass@localhost:1433?database=ptah&schema=custom&encrypt=disable",
			expected: "sqlserver://sa:pass@localhost:1433?database=ptah&encrypt=disable",
		},
		{
			name:     "malformed URL falls back",
			input:    "::not-a-url::",
			expected: "::not-a-url::",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(convertSQLServerURL(tt.input), qt.Equals, tt.expected)
		})
	}
}

func TestConvertPostgresWireURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "postgres passthrough with pool params removed",
			input:    "postgres://user:pass@localhost:5432/app?pool_max_conns=10&sslmode=disable",
			expected: "postgres://user:pass@localhost:5432/app?sslmode=disable",
		},
		{
			name:     "cockroachdb scheme rewrites to postgres for pgx",
			input:    "cockroachdb://root@localhost:26257/defaultdb?sslmode=disable",
			expected: "postgres://root@localhost:26257/defaultdb?sslmode=disable",
		},
		{
			name:     "yugabytedb scheme rewrites to postgres for pgx",
			input:    "yugabytedb://yugabyte@localhost:5433/yugabyte",
			expected: "postgres://yugabyte@localhost:5433/yugabyte",
		},
		{
			name:     "spanner scheme rewrites to postgres for pgx",
			input:    "spanner://user@localhost:5432/db",
			expected: "postgres://user@localhost:5432/db",
		},
		{
			name:     "malformed URL falls back to cleaned input",
			input:    "::not-a-url::",
			expected: "::not-a-url::",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(convertPostgresWireURL(tt.input), qt.Equals, tt.expected)
		})
	}
}

func TestDetectPostgresWireDialect(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		version  string
		expected string
	}{
		{
			name:     "plain postgres",
			declared: "postgres",
			version:  "PostgreSQL 16.3 (Debian 16.3-1.pgdg120+1)",
			expected: "postgres",
		},
		{
			name:     "cockroach detected from postgres URL",
			declared: "postgres",
			version:  "CockroachDB CCL v23.2.5 (x86_64-pc-linux-gnu)",
			expected: "cockroachdb",
		},
		{
			name:     "yugabyte detected from postgres URL",
			declared: "postgres",
			version:  "PostgreSQL 11.2-YB-2.25.1.0-b0 on x86_64-pc-linux-gnu, compiled by clang, YugabyteDB",
			expected: "yugabytedb",
		},
		{
			name:     "spanner detected from postgres URL",
			declared: "postgres",
			version:  "Cloud Spanner PostgreSQL interface",
			expected: "spanner",
		},
		{
			name:     "explicit cockroach survives generic banner",
			declared: "cockroachdb",
			version:  "PostgreSQL-compatible server",
			expected: "cockroachdb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(detectPostgresWireDialect(tt.declared, tt.version), qt.Equals, tt.expected)
		})
	}
}

func TestDetectMySQLWireDialect(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		version  string
		expected string
	}{
		{
			name:     "mysql server",
			declared: "mysql",
			version:  "9.7.0",
			expected: "mysql",
		},
		{
			name:     "mariadb detected from mysql URL",
			declared: "mysql",
			version:  "10.11.15-MariaDB-ubu2204",
			expected: "mariadb",
		},
		{
			name:     "mariadb replication prefix",
			declared: "mysql",
			version:  "5.5.5-10.11.15-MariaDB-ubu2204",
			expected: "mariadb",
		},
		{
			name:     "explicit mariadb survives generic banner",
			declared: "mariadb",
			version:  "MySQL-compatible server",
			expected: "mariadb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(detectMySQLWireDialect(tt.declared, tt.version), qt.Equals, tt.expected)
		})
	}
}

func TestDatabaseConnectionInfoClonesCapabilities(t *testing.T) {
	c := qt.New(t)

	conn := &DatabaseConnection{
		info: types.DBInfo{
			Dialect:      "cockroachdb",
			Capabilities: capability.CockroachDB23(),
		},
	}

	info := conn.Info()
	info.Capabilities[capability.RowLevelSecurity] = true

	c.Assert(conn.Info().Capabilities.Has(capability.RowLevelSecurity), qt.IsFalse)
}

func TestRemovePostgresPoolParams(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "URL with both pool params",
			input:    "postgres://user:pass@localhost:5432/db?pool_max_conns=10&pool_min_conns=2&other=value",
			expected: "postgres://user:pass@localhost:5432/db?other=value",
		},
		{
			name:     "URL with only max_conns",
			input:    "postgres://user:pass@localhost:5432/db?pool_max_conns=10&other=value",
			expected: "postgres://user:pass@localhost:5432/db?other=value",
		},
		{
			name:     "URL with only min_conns",
			input:    "postgres://user:pass@localhost:5432/db?pool_min_conns=2&other=value",
			expected: "postgres://user:pass@localhost:5432/db?other=value",
		},
		{
			name:     "URL without pool params",
			input:    "postgres://user:pass@localhost:5432/db?other=value",
			expected: "postgres://user:pass@localhost:5432/db?other=value",
		},
		{
			name:     "URL with no query params",
			input:    "postgres://user:pass@localhost:5432/db",
			expected: "postgres://user:pass@localhost:5432/db",
		},
		{
			name:     "URL with pool params and multiple other params",
			input:    "postgres://user:pass@localhost:5432/db?sslmode=disable&pool_max_conns=20&timeout=30&pool_min_conns=5&application_name=myapp",
			expected: "postgres://user:pass@localhost:5432/db?application_name=myapp&sslmode=disable&timeout=30",
		},
		{
			name:     "URL with pool params at different positions",
			input:    "postgres://user:pass@localhost:5432/db?first=1&pool_max_conns=10&middle=2&pool_min_conns=3&last=4",
			expected: "postgres://user:pass@localhost:5432/db?first=1&last=4&middle=2",
		},
		{
			name:     "URL with only pool params (should result in no query string)",
			input:    "postgres://user:pass@localhost:5432/db?pool_max_conns=10&pool_min_conns=2",
			expected: "postgres://user:pass@localhost:5432/db",
		},
		{
			name:     "Invalid URL fallback",
			input:    "not-a-url",
			expected: "not-a-url",
		},
		{
			name:     "URL with special characters in pool params",
			input:    "postgres://user:pass@localhost:5432/db?pool_max_conns=10&other=special%20value&pool_min_conns=2",
			expected: "postgres://user:pass@localhost:5432/db?other=special+value",
		},
		{
			name:     "Empty URL",
			input:    "",
			expected: "",
		},
		{
			name:     "URL with case variations (should not match)",
			input:    "postgres://user:pass@localhost:5432/db?POOL_MAX_CONNS=10&Pool_Min_Conns=2&other=value",
			expected: "postgres://user:pass@localhost:5432/db?POOL_MAX_CONNS=10&Pool_Min_Conns=2&other=value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			result := removePostgresPoolParams(tt.input)
			c.Assert(result, qt.Equals, tt.expected, qt.Commentf("removePostgresPoolParams(%q) = %q, want %q", tt.input, result, tt.expected))
		})
	}
}

func TestRemovePostgresPoolParams_ParameterOrdering(t *testing.T) {
	c := qt.New(t)

	// Test that the function produces consistent results regardless of input parameter order
	input1 := "postgres://user:pass@localhost:5432/db?pool_max_conns=10&other=value&pool_min_conns=2"
	input2 := "postgres://user:pass@localhost:5432/db?pool_min_conns=2&pool_max_conns=10&other=value"
	input3 := "postgres://user:pass@localhost:5432/db?other=value&pool_max_conns=10&pool_min_conns=2"

	result1 := removePostgresPoolParams(input1)
	result2 := removePostgresPoolParams(input2)
	result3 := removePostgresPoolParams(input3)

	// All should result in the same cleaned URL
	expected := "postgres://user:pass@localhost:5432/db?other=value"
	c.Assert(result1, qt.Equals, expected)
	c.Assert(result2, qt.Equals, expected)
	c.Assert(result3, qt.Equals, expected)

	// All results should be identical
	c.Assert(result1, qt.Equals, result2)
	c.Assert(result2, qt.Equals, result3)
}

func TestRemovePostgresPoolParams_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "URL with fragment",
			input:    "postgres://user:pass@localhost:5432/db?pool_max_conns=10#fragment",
			expected: "postgres://user:pass@localhost:5432/db#fragment",
		},
		{
			name:     "URL with port and path",
			input:    "postgres://user:pass@localhost:5432/path/to/db?pool_max_conns=10&pool_min_conns=2",
			expected: "postgres://user:pass@localhost:5432/path/to/db",
		},
		{
			name:     "URL with encoded characters",
			input:    "postgres://user:pass%40word@localhost:5432/db?pool_max_conns=10&other=value%20with%20spaces",
			expected: "postgres://user:pass%40word@localhost:5432/db?other=value+with+spaces",
		},
		{
			name:     "URL with duplicate non-pool params (should preserve all)",
			input:    "postgres://user:pass@localhost:5432/db?other=value1&pool_max_conns=10&other=value2",
			expected: "postgres://user:pass@localhost:5432/db?other=value1&other=value2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			result := removePostgresPoolParams(tt.input)
			c.Assert(result, qt.Equals, tt.expected, qt.Commentf("removePostgresPoolParams(%q) = %q, want %q", tt.input, result, tt.expected))
		})
	}
}
