package testkit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/dbschema"
	dbtypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/migrator"
)

const (
	defaultStartupTimeout  = 2 * time.Minute
	defaultPostgresImage   = "postgres:16-alpine"
	defaultMySQLImage      = "mysql:8.4"
	defaultMariaDBImage    = "mariadb:11.4"
	defaultDatabaseName    = "ptah_test"
	defaultRootUsername    = "root"
	defaultPostgresUser    = "ptah"
	defaultDatabasePass    = "ptah"
	defaultReusableBaseDB  = "ptah_testkit"
	defaultMySQLParameters = "parseTime=true&multiStatements=true"
)

// Option configures a testkit database helper.
type Option func(*databaseConfig)

type databaseConfig struct {
	image          string
	database       string
	username       string
	password       string
	sqlitePath     string
	reuseName      string
	startupTimeout time.Duration
	ctx            context.Context
}

func defaultConfig(image, username string) databaseConfig {
	return databaseConfig{
		image:          image,
		database:       defaultDatabaseName,
		username:       username,
		password:       defaultDatabasePass,
		startupTimeout: defaultStartupTimeout,
		ctx:            context.Background(),
	}
}

// WithImage overrides the container image used by StartPostgres, StartMySQL,
// or StartMariaDB.
func WithImage(image string) Option {
	return func(cfg *databaseConfig) {
		cfg.image = strings.TrimSpace(image)
	}
}

// WithDatabase overrides the database name used by container-backed helpers.
func WithDatabase(database string) Option {
	return func(cfg *databaseConfig) {
		cfg.database = strings.TrimSpace(database)
	}
}

// WithUsername overrides the database username used by container-backed
// helpers.
func WithUsername(username string) Option {
	return func(cfg *databaseConfig) {
		cfg.username = strings.TrimSpace(username)
	}
}

// WithPassword overrides the database password used by container-backed
// helpers.
func WithPassword(password string) Option {
	return func(cfg *databaseConfig) {
		cfg.password = password
	}
}

// WithStartupTimeout overrides the container startup timeout.
func WithStartupTimeout(timeout time.Duration) Option {
	return func(cfg *databaseConfig) {
		if timeout > 0 {
			cfg.startupTimeout = timeout
		}
	}
}

// WithContext sets the context used while starting containers and opening
// database connections.
func WithContext(ctx context.Context) Option {
	return func(cfg *databaseConfig) {
		if ctx != nil {
			cfg.ctx = ctx
		}
	}
}

// WithReuseByName reuses a container with the given name and creates a fresh
// random database inside it for each test.
func WithReuseByName(name string) Option {
	return func(cfg *databaseConfig) {
		cfg.reuseName = strings.TrimSpace(name)
	}
}

// WithSQLitePath stores the SQLite database at path instead of using a
// t.TempDir-backed file.
func WithSQLitePath(path string) Option {
	return func(cfg *databaseConfig) {
		cfg.sqlitePath = strings.TrimSpace(path)
	}
}

// StartSQLite opens a SQLite database and registers cleanup with t.
func StartSQLite(t testing.TB, opts ...Option) *dbschema.DatabaseConnection {
	t.Helper()

	cfg := defaultConfig("", "")
	applyOptions(&cfg, opts)

	dbPath := cfg.sqlitePath
	if dbPath == "" {
		dbPath = filepath.Join(t.TempDir(), "ptah-test.sqlite")
	}

	dbURL := (&url.URL{Scheme: platform.SQLite, Path: dbPath}).String()
	conn := connectForTest(t, cfg.ctx, dbURL)
	t.Cleanup(func() {
		requireNoError(t, conn.Close(), "close SQLite connection")
	})
	return conn
}

// StartPostgres starts a PostgreSQL container, opens a Ptah database
// connection, and registers cleanup with t.
func StartPostgres(t testing.TB, opts ...Option) *dbschema.DatabaseConnection {
	t.Helper()

	cfg := defaultConfig(defaultPostgresImage, defaultPostgresUser)
	applyOptions(&cfg, opts)
	ctx, cancel := context.WithTimeout(cfg.ctx, cfg.startupTimeout)
	t.Cleanup(cancel)

	baseDB := cfg.database
	if cfg.reuseName != "" {
		baseDB = defaultReusableBaseDB
	}
	container, err := tcpostgres.Run(ctx, cfg.image,
		tcpostgres.WithUsername(cfg.username),
		tcpostgres.WithPassword(cfg.password),
		tcpostgres.WithDatabase(baseDB),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		),
		reuseOption(cfg.reuseName),
	)
	requireNoError(t, err, "start PostgreSQL container")
	if cfg.reuseName == "" {
		t.Cleanup(func() {
			requireNoError(t, testcontainers.TerminateContainer(container), "terminate PostgreSQL container")
		})
	}

	dbName := cfg.database
	if cfg.reuseName != "" {
		dbName = uniqueDatabaseName(t)
		adminURL, err := container.ConnectionString(ctx, "sslmode=disable")
		requireNoError(t, err, "build PostgreSQL admin connection string")
		createPostgresDatabase(t, ctx, adminURL, dbName)
		t.Cleanup(func() {
			dropPostgresDatabase(t, context.Background(), adminURL, dbName)
		})
	}

	dbURL, err := container.ConnectionString(ctx, "sslmode=disable")
	requireNoError(t, err, "build PostgreSQL connection string")
	if cfg.reuseName != "" {
		dbURL = postgresDatabaseURL(dbURL, dbName)
	}
	conn := connectForTest(t, ctx, dbURL)
	t.Cleanup(func() {
		requireNoError(t, conn.Close(), "close PostgreSQL connection")
	})
	return conn
}

// StartMySQL starts a MySQL container, opens a Ptah database connection, and
// registers cleanup with t.
func StartMySQL(t testing.TB, opts ...Option) *dbschema.DatabaseConnection {
	t.Helper()
	return startMySQLLike(t, platform.MySQL, defaultMySQLImage, opts...)
}

// StartMariaDB starts a MariaDB container, opens a Ptah database connection,
// and registers cleanup with t.
func StartMariaDB(t testing.TB, opts ...Option) *dbschema.DatabaseConnection {
	t.Helper()
	return startMySQLLike(t, platform.MariaDB, defaultMariaDBImage, opts...)
}

// ApplyMigrationsFromFS applies all migrations from fsys to conn.
func ApplyMigrationsFromFS(
	t testing.TB,
	conn *dbschema.DatabaseConnection,
	fsys fs.FS,
	opts ...migrator.FSProviderOption,
) {
	t.Helper()

	m, err := migrator.NewFSMigrator(conn, fsys, opts...)
	requireNoError(t, err, "create filesystem migrator")
	requireNoError(t, m.MigrateUp(context.Background()), "apply migrations")
}

// Seed executes SQL seed statements against conn.
func Seed(t testing.TB, conn *dbschema.DatabaseConnection, sqlBytes []byte) {
	t.Helper()

	dialect := ""
	if conn != nil {
		dialect = conn.Info().Dialect
	}
	statements := sqlutil.SplitSQLStatementsForDialect(string(sqlBytes), dialect)
	for _, stmt := range statements {
		stmt = strings.TrimSpace(sqlutil.StripComments(stmt))
		if stmt == "" {
			continue
		}
		_, err := conn.ExecContext(context.Background(), stmt)
		requireNoError(t, err, "execute seed statement")
	}
}

// Snapshot returns a deterministic JSON schema snapshot for golden tests.
func Snapshot(t testing.TB, conn *dbschema.DatabaseConnection) string {
	t.Helper()

	schema, err := conn.Reader().ReadSchema()
	requireNoError(t, err, "read schema snapshot")
	normalizeSchema(schema)

	data, err := json.MarshalIndent(schema, "", "  ")
	requireNoError(t, err, "encode schema snapshot")
	return string(data) + "\n"
}

func startMySQLLike(t testing.TB, dialect, defaultImage string, opts ...Option) *dbschema.DatabaseConnection {
	t.Helper()

	cfg := defaultConfig(defaultImage, defaultRootUsername)
	applyOptions(&cfg, opts)
	ctx, cancel := context.WithTimeout(cfg.ctx, cfg.startupTimeout)
	t.Cleanup(cancel)

	baseDB := cfg.database
	if cfg.reuseName != "" {
		baseDB = defaultReusableBaseDB
	}
	container, err := tcmysql.Run(ctx, cfg.image,
		tcmysql.WithUsername(cfg.username),
		tcmysql.WithPassword(cfg.password),
		tcmysql.WithDatabase(baseDB),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("3306/tcp")),
		reuseOption(cfg.reuseName),
	)
	requireNoError(t, err, "start "+dialect+" container")
	if cfg.reuseName == "" {
		t.Cleanup(func() {
			requireNoError(t, testcontainers.TerminateContainer(container), "terminate "+dialect+" container")
		})
	}

	dbName := cfg.database
	adminDSN, err := container.ConnectionString(ctx, defaultMySQLParameters)
	requireNoError(t, err, "build "+dialect+" admin connection string")
	adminURL := mysqlURL(dialect, adminDSN)
	if cfg.reuseName != "" {
		dbName = uniqueDatabaseName(t)
		createMySQLDatabase(t, ctx, adminURL, dbName)
		t.Cleanup(func() {
			dropMySQLDatabase(t, context.Background(), adminURL, dbName)
		})
	}

	dbURL := mysqlURL(dialect, mysqlDSNDatabase(adminDSN, dbName))
	conn := connectForTest(t, ctx, dbURL)
	t.Cleanup(func() {
		requireNoError(t, conn.Close(), "close "+dialect+" connection")
	})
	return conn
}

func applyOptions(cfg *databaseConfig, opts []Option) {
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}
}

func reuseOption(name string) testcontainers.ContainerCustomizer {
	if name == "" {
		return noopCustomizer{}
	}
	return testcontainers.WithReuseByName(name)
}

type noopCustomizer struct{}

func (noopCustomizer) Customize(*testcontainers.GenericContainerRequest) error {
	return nil
}

func connectForTest(t testing.TB, ctx context.Context, dbURL string) *dbschema.DatabaseConnection {
	t.Helper()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	requireNoError(t, err, "connect to database")
	return conn
}

func postgresDatabaseURL(dbURL, database string) string {
	u, err := url.Parse(dbURL)
	if err != nil {
		return dbURL
	}
	u.Path = "/" + database
	return u.String()
}

func mysqlURL(dialect, dsn string) string {
	return dialect + "://" + dsn
}

func mysqlDSNDatabase(dsn, database string) string {
	prefix, rest, ok := strings.Cut(dsn, ")/")
	if !ok {
		return dsn
	}
	_, query, hasQuery := strings.Cut(rest, "?")
	if hasQuery {
		return prefix + ")/" + database + "?" + query
	}
	return prefix + ")/" + database
}

func createPostgresDatabase(t testing.TB, ctx context.Context, adminURL string, database string) {
	t.Helper()

	admin := connectForTest(t, ctx, adminURL)
	defer closeForTest(t, admin)
	_, err := admin.ExecContext(ctx, "CREATE DATABASE "+quoteIdentifier(database, '"'))
	requireNoError(t, err, "create PostgreSQL database")
}

func dropPostgresDatabase(t testing.TB, ctx context.Context, adminURL string, database string) {
	t.Helper()

	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	if err != nil {
		t.Logf("connect to PostgreSQL admin database for cleanup: %v", err)
		return
	}
	defer closeForTest(t, admin)

	_, err = admin.ExecContext(
		ctx,
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
		database,
	)
	if err != nil {
		t.Logf("terminate PostgreSQL sessions for cleanup: %v", err)
	}
	_, err = admin.ExecContext(ctx, "DROP DATABASE IF EXISTS "+quoteIdentifier(database, '"'))
	if err != nil {
		t.Logf("drop PostgreSQL database for cleanup: %v", err)
	}
}

func createMySQLDatabase(t testing.TB, ctx context.Context, adminURL string, database string) {
	t.Helper()

	admin := connectForTest(t, ctx, adminURL)
	defer closeForTest(t, admin)
	_, err := admin.ExecContext(ctx, "CREATE DATABASE "+quoteIdentifier(database, '`'))
	requireNoError(t, err, "create MySQL-compatible database")
}

func dropMySQLDatabase(t testing.TB, ctx context.Context, adminURL string, database string) {
	t.Helper()

	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	if err != nil {
		t.Logf("connect to MySQL-compatible admin database for cleanup: %v", err)
		return
	}
	defer closeForTest(t, admin)

	_, err = admin.ExecContext(ctx, "DROP DATABASE IF EXISTS "+quoteIdentifier(database, '`'))
	if err != nil {
		t.Logf("drop MySQL-compatible database for cleanup: %v", err)
	}
}

func closeForTest(t testing.TB, conn *dbschema.DatabaseConnection) {
	t.Helper()
	requireNoError(t, conn.Close(), "close database connection")
}

func quoteIdentifier(identifier string, quote rune) string {
	return string(quote) + strings.ReplaceAll(identifier, string(quote), string(quote)+string(quote)) + string(quote)
}

func uniqueDatabaseName(t testing.TB) string {
	t.Helper()

	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		t.Fatalf("generate database suffix: %v", err)
	}
	return "ptah_" + sanitizeIdentifier(t.Name()) + "_" + hex.EncodeToString(buf[:])
}

func sanitizeIdentifier(input string) string {
	var builder strings.Builder
	for _, r := range strings.ToLower(input) {
		switch {
		case r == '_' || unicode.IsDigit(r) || ('a' <= r && r <= 'z'):
			builder.WriteRune(r)
		default:
			builder.WriteByte('_')
		}
	}
	result := strings.Trim(builder.String(), "_")
	if result == "" {
		return "test"
	}
	if len(result) > 32 {
		return result[:32]
	}
	return result
}

func requireNoError(t testing.TB, err error, action string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", action, err)
	}
}

func normalizeSchema(schema *dbtypes.DBSchema) {
	schema.Tables = slices.DeleteFunc(schema.Tables, func(table dbtypes.DBTable) bool {
		return isMigrationMetadataTable(table.Name)
	})
	schema.Indexes = slices.DeleteFunc(schema.Indexes, func(index dbtypes.DBIndex) bool {
		return isMigrationMetadataTable(index.TableName)
	})
	schema.Constraints = slices.DeleteFunc(schema.Constraints, func(constraint dbtypes.DBConstraint) bool {
		return isMigrationMetadataTable(constraint.TableName)
	})

	for idx := range schema.Tables {
		schema.Tables[idx].EstimatedRows = 0
		slices.SortFunc(schema.Tables[idx].Columns, compareColumns)
	}

	slices.SortFunc(schema.Tables, func(a, b dbtypes.DBTable) int {
		return strings.Compare(a.QualifiedName(), b.QualifiedName())
	})
	slices.SortFunc(schema.Enums, func(a, b dbtypes.DBEnum) int {
		return strings.Compare(a.Name, b.Name)
	})
	slices.SortFunc(schema.Indexes, func(a, b dbtypes.DBIndex) int {
		return strings.Compare(a.QualifiedTableName()+"."+a.Name, b.QualifiedTableName()+"."+b.Name)
	})
	slices.SortFunc(schema.Constraints, func(a, b dbtypes.DBConstraint) int {
		return strings.Compare(a.QualifiedTableName()+"."+a.Name, b.QualifiedTableName()+"."+b.Name)
	})
	slices.SortFunc(schema.Extensions, func(a, b dbtypes.DBExtension) int {
		return strings.Compare(a.Schema+"."+a.Name, b.Schema+"."+b.Name)
	})
	slices.SortFunc(schema.Functions, func(a, b dbtypes.DBFunction) int {
		return strings.Compare(a.Name+"("+a.Parameters+")", b.Name+"("+b.Parameters+")")
	})
	slices.SortFunc(schema.Views, func(a, b dbtypes.DBView) int {
		return strings.Compare(a.QualifiedName(), b.QualifiedName())
	})
	slices.SortFunc(schema.MatViews, func(a, b dbtypes.DBMatView) int {
		return strings.Compare(a.QualifiedName(), b.QualifiedName())
	})
	slices.SortFunc(schema.Triggers, func(a, b dbtypes.DBTrigger) int {
		return strings.Compare(a.QualifiedTable()+"."+a.Name, b.QualifiedTable()+"."+b.Name)
	})
	slices.SortFunc(schema.RLSPolicies, func(a, b dbtypes.DBRLSPolicy) int {
		return strings.Compare(a.Table+"."+a.Name, b.Table+"."+b.Name)
	})
	slices.SortFunc(schema.Roles, func(a, b dbtypes.DBRole) int {
		return strings.Compare(a.Name, b.Name)
	})
	slices.SortFunc(schema.Grants, func(a, b dbtypes.DBGrant) int {
		return strings.Compare(a.Role+"."+a.ObjectType+"."+a.QualifiedTarget()+"."+a.Privilege, b.Role+"."+b.ObjectType+"."+b.QualifiedTarget()+"."+b.Privilege)
	})
}

func isMigrationMetadataTable(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "schema_migrations", "atlas_schema_revisions":
		return true
	default:
		return false
	}
}

func compareColumns(a, b dbtypes.DBColumn) int {
	if a.OrdinalPosition != b.OrdinalPosition {
		return a.OrdinalPosition - b.OrdinalPosition
	}
	return strings.Compare(a.Name, b.Name)
}
