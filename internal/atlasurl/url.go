// Package atlasurl interprets Atlas-style database URLs: mapping a URL to a
// Ptah dialect, validating that a URL matches a target dialect, and deciding
// whether two URLs address the same database.
package atlasurl

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/microsoft/go-mssqldb/msdsn"

	"go.5x5.cz/ptah/core/platform"
)

var defaultPorts = map[string]string{
	platform.Postgres:    "5432",
	platform.CockroachDB: "26257",
	platform.YugabyteDB:  "5433",
	platform.MySQL:       "3306",
	platform.MariaDB:     "3306",
	platform.SQLServer:   "1433",
	platform.ClickHouse:  "9000",
}

// SQLiteURLFromPath returns a SQLite URL whose path remains unambiguous on the
// current operating system. Windows drive paths use the URL's opaque form
// instead of being misparsed as a host and port.
func SQLiteURLFromPath(path string) string {
	cleaned := filepath.ToSlash(filepath.Clean(path))
	escaped := (&url.URL{Path: cleaned}).EscapedPath()
	return "sqlite:file:" + escaped
}

// Parse parses a database URL, accepting the one shape net/url refuses.
//
// A Windows absolute path is not an authority: sqlite://C:\dir\app.db makes
// net/url read the drive letter's colon as a port separator and refuse the
// whole address. The path is carried as opaque instead.
//
// This is the shared half. What each caller does about a MySQL address is its
// own, and deliberately so: this package keeps the host because it compares
// endpoints, while dbschema drops it because it reads the database name from
// the path. Only the Windows rule is one rule.
func Parse(rawURL string) (*url.URL, error) {
	parsed, err := url.Parse(rawURL)
	if err == nil {
		return parsed, nil
	}
	if scheme, rest, found := strings.Cut(rawURL, "://"); found && IsWindowsPath(rest) {
		return &url.URL{Scheme: scheme, Opaque: rest}, nil
	}
	return nil, err
}

// IsWindowsPath reports whether a URL's remainder is a Windows absolute path,
// which is the one shape whose colon is not a port separator.
func IsWindowsPath(rest string) bool {
	if len(rest) < 3 || rest[1] != ':' {
		return false
	}
	drive := rest[0]
	if (drive < 'A' || drive > 'Z') && (drive < 'a' || drive > 'z') {
		return false
	}
	return rest[2] == '\\' || rest[2] == '/'
}

func DialectFromURL(rawURL string) (string, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return "", nil
	}
	parsed, err := Parse(normalizeMySQLTCPURL(rawURL))
	if err != nil {
		return "", fmt.Errorf("parse --dev-url: %w", err)
	}
	switch parsed.Scheme {
	case "docker":
		return dialectFromDockerURL(parsed)
	case "sqlite", "sqlite3", "mysql", "mariadb", "postgres", "postgresql", "sqlserver", "mssql", "clickhouse", "cockroach", "cockroachdb", "yugabyte", "yugabytedb":
		dialect := platform.NormalizeDialect(parsed.Scheme)
		if dialect != "" {
			return dialect, nil
		}
	}
	return "", fmt.Errorf("unsupported --dev-url dialect %q", rawURL)
}

// ValidateDialectMatch verifies that rawURL resolves to the same dialect as the
// already-open target database.
func ValidateDialectMatch(rawURL, targetDialect string) error {
	dialect, err := DialectFromURL(rawURL)
	if err != nil {
		return err
	}
	if dialect == "" {
		return nil
	}
	normalizedTarget := platform.NormalizeDialect(targetDialect)
	if normalizedTarget == "" {
		normalizedTarget = targetDialect
	}
	if dialect != normalizedTarget {
		return fmt.Errorf("--dev-url dialect %q does not match --url dialect %q", dialect, normalizedTarget)
	}
	return nil
}

// SameDatabaseEndpoint reports whether two directly connectable URLs prove
// that they select the same database endpoint. Credentials and non-identity
// connection options are intentionally ignored. Driver-specific endpoint and
// database overrides participate in the comparison.
//
// This exact relation is suitable for validating alternate credentials for
// one known endpoint. Destructive dev and shadow workflows must instead use
// MayAddressSameDatabase and a live realm comparison after connecting.
func SameDatabaseEndpoint(left, right string) (bool, error) {
	leftURL, leftDialect, err := parseDatabaseURL(left)
	if err != nil {
		return false, err
	}
	rightURL, rightDialect, err := parseDatabaseURL(right)
	if err != nil {
		return false, err
	}
	if leftDialect != rightDialect {
		return false, nil
	}
	if leftDialect == platform.SQLite {
		return sameSQLiteDatabase(leftURL, rightURL)
	}
	leftIdentity, err := networkDatabaseIdentity(leftURL, leftDialect)
	if err != nil {
		return false, err
	}
	rightIdentity, err := networkDatabaseIdentity(rightURL, rightDialect)
	if err != nil {
		return false, err
	}
	if leftIdentity.database == "" || rightIdentity.database == "" {
		return false, nil
	}
	return leftIdentity == rightIdentity, nil
}

// MayAddressSameDatabase reports whether two URLs cannot be proven to select
// distinct database realms. Network hosts are intentionally excluded: DNS
// aliases and replicated members with the same database name must fail closed
// before destructive dev or shadow cleanup. Callers that connect both URLs
// must also compare their live realm identity before cleanup.
func MayAddressSameDatabase(left, right string) (bool, error) {
	leftURL, leftDialect, err := parseDatabaseURL(left)
	if err != nil {
		return false, err
	}
	rightURL, rightDialect, err := parseDatabaseURL(right)
	if err != nil {
		return false, err
	}
	if leftDialect != rightDialect {
		return false, nil
	}
	if leftDialect == platform.SQLite {
		return sameSQLiteDatabase(leftURL, rightURL)
	}
	leftIdentity, err := networkDatabaseIdentity(leftURL, leftDialect)
	if err != nil {
		return false, err
	}
	rightIdentity, err := networkDatabaseIdentity(rightURL, rightDialect)
	if err != nil {
		return false, err
	}
	if leftIdentity.database == "" || rightIdentity.database == "" {
		return true, nil
	}
	return strings.EqualFold(leftIdentity.database, rightIdentity.database), nil
}

type databaseIdentity struct {
	dialect  string
	endpoint string
	database string
}

func networkDatabaseIdentity(parsed *url.URL, dialect string) (databaseIdentity, error) {
	endpoint := networkEndpoint(parsed.Hostname(), parsed.Port(), dialect)
	database := strings.Trim(parsed.Path, "/")
	switch dialect {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		connectionURL := *parsed
		connectionURL.Scheme = platform.Postgres
		config, err := pgconn.ParseConfig(connectionURL.String())
		if err != nil {
			return databaseIdentity{}, errors.New("invalid PostgreSQL database URL")
		}
		endpoints := []string{networkEndpoint(config.Host, strconv.Itoa(int(config.Port)), dialect)}
		for _, fallback := range config.Fallbacks {
			endpoints = append(endpoints, networkEndpoint(
				fallback.Host,
				strconv.Itoa(int(fallback.Port)),
				dialect,
			))
		}
		endpoint = networkEndpointRoute(endpoints)
		database = config.Database
	case platform.SQLServer:
		connectionURL := *parsed
		connectionURL.Scheme = platform.SQLServer
		config, err := msdsn.Parse(connectionURL.String())
		if err != nil {
			return databaseIdentity{}, errors.New("invalid SQL Server database URL")
		}
		port := ""
		if config.Port != 0 {
			port = strconv.FormatUint(config.Port, 10)
		}
		endpoint = networkEndpoint(config.Host, port, dialect)
		if config.Instance != "" {
			endpoint += "\x00" + strings.ToLower(config.Instance)
		}
		if config.FailOverPartner != "" {
			failoverPort := ""
			if config.FailOverPort != 0 {
				failoverPort = strconv.FormatUint(config.FailOverPort, 10)
			}
			endpoint = networkEndpointRoute([]string{
				endpoint,
				networkEndpoint(config.FailOverPartner, failoverPort, dialect),
			})
		}
		database = config.Database
	case platform.ClickHouse:
		connectionURL := *parsed
		connectionURL.Scheme = platform.ClickHouse
		options, err := clickhouse.ParseDSN(connectionURL.String())
		if err != nil {
			return databaseIdentity{}, errors.New("invalid ClickHouse database URL")
		}
		if len(options.Addr) != 0 {
			endpoints := make([]string, 0, len(options.Addr))
			for _, address := range options.Addr {
				endpoints = append(endpoints, normalizedNetworkAddress(address, dialect))
			}
			endpoint = networkEndpointRoute(endpoints)
		}
		database = options.Auth.Database
	}
	return databaseIdentity{
		dialect:  dialect,
		endpoint: endpoint,
		database: database,
	}, nil
}

func networkEndpointRoute(endpoints []string) string {
	unique := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if !slices.Contains(unique, endpoint) {
			unique = append(unique, endpoint)
		}
	}
	return strings.Join(unique, "\x01")
}

func normalizedNetworkAddress(address, dialect string) string {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return networkEndpoint(address, "", dialect)
	}
	return networkEndpoint(host, port, dialect)
}

func networkEndpoint(host, port, dialect string) string {
	if port == "" {
		port = defaultPorts[dialect]
	}
	return normalizedDatabaseHost(host) + "\x00" + port
}

func normalizedDatabaseHost(host string) string {
	if filepath.IsAbs(host) {
		return filepath.Clean(host)
	}
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if host == "localhost" {
		return "loopback"
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return host
	}
	if ip.IsLoopback() {
		return "loopback"
	}
	return ip.String()
}

func parseDatabaseURL(rawURL string) (*url.URL, string, error) {
	normalized := normalizeMySQLTCPURL(strings.TrimSpace(rawURL))
	parsed, err := url.Parse(normalized)
	if err != nil {
		return nil, "", errors.New("invalid database URL")
	}
	dialect := platform.NormalizeDialect(parsed.Scheme)
	if dialect == "" {
		return nil, "", errors.New("unsupported database URL dialect")
	}
	return parsed, dialect, nil
}

func normalizeMySQLTCPURL(rawURL string) string {
	if !strings.HasPrefix(rawURL, "mysql://") && !strings.HasPrefix(rawURL, "mariadb://") {
		return rawURL
	}
	prefix, address, found := strings.Cut(rawURL, "@tcp(")
	if !found {
		return rawURL
	}
	host, suffix, found := strings.Cut(address, ")")
	if !found {
		return rawURL
	}
	return prefix + "@" + host + suffix
}

func sqliteIdentity(parsed *url.URL) (string, error) {
	path, memory, err := sqliteDatabasePath(parsed)
	if err != nil {
		return "", err
	}
	if memory {
		return "sqlite\x00:memory:", nil
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", errors.New("resolve SQLite database path")
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err == nil {
		absolute = resolved
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", errors.New("resolve SQLite database path")
	}
	return "sqlite\x00" + filepath.Clean(absolute), nil
}

func sameSQLiteDatabase(left, right *url.URL) (bool, error) {
	leftPath, leftMemory, err := sqliteDatabasePath(left)
	if err != nil {
		return false, err
	}
	rightPath, rightMemory, err := sqliteDatabasePath(right)
	if err != nil {
		return false, err
	}
	if leftMemory || rightMemory {
		return leftMemory && rightMemory, nil
	}
	leftInfo, leftErr := os.Stat(leftPath)
	rightInfo, rightErr := os.Stat(rightPath)
	if leftErr == nil && rightErr == nil {
		return os.SameFile(leftInfo, rightInfo), nil
	}
	leftIdentity, err := sqliteIdentity(left)
	if err != nil {
		return false, err
	}
	rightIdentity, err := sqliteIdentity(right)
	if err != nil {
		return false, err
	}
	return leftIdentity == rightIdentity, nil
}

func sqliteDatabasePath(parsed *url.URL) (string, bool, error) {
	path := parsed.Opaque
	fileURI := strings.HasPrefix(path, "file:")
	switch {
	case path != "":
		// Opaque SQLite URLs already contain the driver path verbatim.
	case parsed.Host != "" && parsed.Path != "":
		path = parsed.Host + parsed.Path
	case parsed.Host != "":
		path = parsed.Host
	default:
		path = parsed.Path
	}
	path = strings.TrimPrefix(path, "file:")
	if fileURI {
		decoded, err := url.PathUnescape(path)
		if err != nil {
			return "", false, errors.New("invalid SQLite database URL")
		}
		path = decoded
	}
	if path == "" || path == "/:memory:" || path == ":memory:" ||
		parsed.Query().Get("mode") == "memory" {
		return "", true, nil
	}
	return filepath.Clean(path), false, nil
}

// dockerEngineAliases are docker image names the pinned community binary
// provisions that are not also dialect spellings.
//
// `maria` is the whole set today. It is a measured community spelling --
// `docker://maria/11/dev` and `docker://mariadb/11/dev` both resolve to that
// binary's MariaDB image -- and [go.5x5.cz/ptah/internal/devdocker] starts a
// container for either. [platform.NormalizeDialect] knows only `mariadb`,
// because `maria` is not a dialect anyone writes as a URL scheme, so without
// this the dialect preflight refused `docker://maria/11/dev` with `unsupported
// docker --dev-url engine "maria"` and no container was ever started: a
// capability the pinned binary has, removed.
//
// This duplicates one row of devdocker's engine table, which is the wrong shape
// and is deliberate for now: devdocker imports dbschema (for the readiness
// probe) and dbschema depends on this package, so atlasurl cannot ask devdocker
// what a docker engine name means without an import cycle. The single-source
// fix is to lift the engine table below both, and it is a refactor rather than
// a defect repair.
var dockerEngineAliases = map[string]string{
	"maria": platform.MariaDB,
}

func dialectFromDockerURL(parsed *url.URL) (string, error) {
	engine := parsed.Host
	if engine == "" {
		return "", errors.New("docker --dev-url is missing database engine")
	}
	if before, _, found := strings.Cut(engine, "/"); found {
		engine = before
	}
	if before, _, found := strings.Cut(engine, ":"); found {
		engine = before
	}
	if dialect, ok := dockerEngineAliases[strings.ToLower(engine)]; ok {
		return dialect, nil
	}
	dialect := platform.NormalizeDialect(engine)
	if dialect == "" {
		return "", fmt.Errorf("unsupported docker --dev-url engine %q", parsed.Host)
	}
	return dialect, nil
}
