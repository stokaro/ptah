// Package atlasurl interprets Atlas-style database URLs: mapping a URL to a
// Ptah dialect, validating that a URL matches a target dialect, and deciding
// whether two URLs address the same database.
package atlasurl

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/microsoft/go-mssqldb/msdsn"

	"go.5x5.cz/ptah/core/platform"
)

// SQLiteURLFromPath returns a SQLite URL whose path remains unambiguous on the
// current operating system. Windows drive paths use the URL's opaque form
// instead of being misparsed as a host and port.
func SQLiteURLFromPath(path string) string {
	return "sqlite:" + filepath.ToSlash(filepath.Clean(path))
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
// a destructive dev operation safe against the same database. Network URLs
// with the same dialect and selected database name fail closed even when their
// endpoints differ, because DNS aliases and replicated members cannot be
// proven to be independent before destructive cleanup.
func SameDatabase(left, right string) (bool, error) {
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
	return networkIdentitiesMayOverlap(leftIdentity, rightIdentity), nil
}

type databaseIdentity struct {
	dialect  string
	database string
}

func networkIdentitiesMayOverlap(left, right databaseIdentity) bool {
	return left.dialect == right.dialect &&
		(left.database == "" || right.database == "" || left.database == right.database)
}

func networkDatabaseIdentity(parsed *url.URL, dialect string) (databaseIdentity, error) {
	var database string
	switch dialect {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		connectionURL := *parsed
		connectionURL.Scheme = platform.Postgres
		config, err := pgconn.ParseConfig(connectionURL.String())
		if err != nil {
			return databaseIdentity{}, errors.New("invalid PostgreSQL database URL")
		}
		database = config.Database
	case platform.SQLServer:
		connectionURL := *parsed
		connectionURL.Scheme = platform.SQLServer
		config, err := msdsn.Parse(connectionURL.String())
		if err != nil {
			return databaseIdentity{}, errors.New("invalid SQL Server database URL")
		}
		database = config.Database
	case platform.ClickHouse:
		connectionURL := *parsed
		connectionURL.Scheme = platform.ClickHouse
		options, err := clickhouse.ParseDSN(connectionURL.String())
		if err != nil {
			return databaseIdentity{}, errors.New("invalid ClickHouse database URL")
		}
		database = options.Auth.Database
	default:
		database = strings.Trim(parsed.Path, "/")
	}
	return databaseIdentity{
		dialect:  dialect,
		database: strings.ToLower(database),
	}, nil
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
