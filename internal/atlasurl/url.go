package atlasurl

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/stokaro/ptah/core/platform"
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

func DialectFromURL(rawURL string) (string, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return "", nil
	}
	parsed, err := url.Parse(normalizeMySQLTCPURL(rawURL))
	if err != nil {
		return "", fmt.Errorf("parse --dev-url: %w", err)
	}
	switch parsed.Scheme {
	case "docker":
		return dialectFromDockerURL(parsed)
	case "sqlite", "mysql", "mariadb", "postgres", "postgresql", "sqlserver", "mssql", "clickhouse", "cockroach", "cockroachdb", "yugabyte", "yugabytedb":
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

// SameDatabase reports whether two directly connectable URLs identify the same
// database. Credentials and non-identity connection options are intentionally
// ignored: using different users, TLS settings, or pool settings does not make
// a destructive dev operation safe against the same database.
func SameDatabase(left, right string) (bool, error) {
	leftIdentity, err := databaseIdentity(left)
	if err != nil {
		return false, err
	}
	rightIdentity, err := databaseIdentity(right)
	if err != nil {
		return false, err
	}
	return leftIdentity == rightIdentity, nil
}

func databaseIdentity(rawURL string) (string, error) {
	parsed, dialect, err := parseDatabaseURL(rawURL)
	if err != nil {
		return "", err
	}
	if dialect == platform.SQLite {
		return sqliteIdentity(parsed)
	}

	host := strings.ToLower(parsed.Hostname())
	port := parsed.Port()
	if port == "" {
		port = defaultPorts[dialect]
	}
	database := strings.Trim(parsed.Path, "/")
	if dialect == platform.SQLServer && database == "" {
		database = parsed.Query().Get("database")
	}
	return strings.Join([]string{dialect, host, port, database}, "\x00"), nil
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
	path := parsed.Opaque
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
	if path == "" || path == "/:memory:" || path == ":memory:" {
		return "sqlite\x00:memory:", nil
	}
	if strings.HasPrefix(path, "file:") {
		return "sqlite\x00" + path, nil
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", errors.New("resolve SQLite database path")
	}
	return "sqlite\x00" + filepath.Clean(absolute), nil
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
	dialect := platform.NormalizeDialect(engine)
	if dialect == "" {
		return "", fmt.Errorf("unsupported docker --dev-url engine %q", parsed.Host)
	}
	return dialect, nil
}
