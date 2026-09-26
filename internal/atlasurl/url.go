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

	"ptah.run/core/platform"
)

// dockerScheme is the URL scheme that asks Ptah to start a dev database.
const dockerScheme = "docker"

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
		// The query is split off rather than folded into the path. Carrying the
		// whole remainder as opaque left the options inside the filename, so a
		// caller appending its own parameter wrote a second "?" and the
		// requested ones were silently dropped.
		path, query, _ := strings.Cut(rest, "?")
		// The query is still validated. This fallback exists because a drive
		// letter's colon is not a port separator, not because a Windows path
		// makes every other error acceptable: a malformed escape admitted here
		// is one url.Values silently drops, so an attempted mode=ro
		// restriction would disappear and the database open writable.
		//
		// The refusal names the query rather than passing on the parse error
		// that got us here. That one says the port is invalid, because it read
		// the drive letter's colon as one -- handing it to an operator whose
		// address has no port at all is the misleading diagnostic this whole
		// path exists to remove.
		if _, queryErr := url.ParseQuery(query); queryErr != nil {
			return nil, fmt.Errorf("parse the query of %s: %w", rawURL, queryErr)
		}
		return &url.URL{Scheme: scheme, Opaque: path, RawQuery: query}, nil
	}
	return nil, err
}

// CutMySQLScheme reports whether rawURL begins with a scheme that selects the
// MySQL driver, and returns what follows that scheme's "://".
//
// Which schemes those are is [platform.NormalizeDialect]'s answer: every
// spelling that folds onto MySQL or MariaDB, in any letter case, since net/url
// lowercases a scheme too. The scheme is read as text because net/url refuses
// the go-sql-driver form a MySQL-family URL may carry,
// user:pass@tcp(host:port)/db, so a caller that rewrites, redacts or splits
// that form has to recognize the scheme before it can parse anything.
//
// It is one predicate because the dialect check and each of those callers must
// recognize the same set. With a list of `mysql://` and `mariadb://` at each
// caller instead, `maria://user:pass@tcp(host)/db` reaches the MySQL driver
// while the display prints its password and the online-DDL parser reads
// `maria` as the user name (stokaro/ptah#3744).
//
// A prefix that is not a URL scheme, such as one with leading space, is not
// recognized: net/url refuses it as well.
func CutMySQLScheme(rawURL string) (rest string, ok bool) {
	_, rest, ok = cutMySQLScheme(rawURL)
	return rest, ok
}

// isURLScheme reports whether s is spelled as RFC 3986 section 3.1 and net/url
// spell a scheme: a letter, then letters, digits, "+", "-" or ".".
func isURLScheme(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case 'a' <= r && r <= 'z', 'A' <= r && r <= 'Z':
		case i > 0 && ('0' <= r && r <= '9' || r == '+' || r == '-' || r == '.'):
		default:
			return false
		}
	}
	return true
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

// IsDockerURL reports whether rawURL is a `docker://` URL, the spelling that
// asks Ptah to start a dev database rather than naming one that exists.
//
// [DialectFromURL] answers such a URL with the engine it would start, which is
// the right answer for a dev URL and the wrong one for a target: no database is
// there, and `--db-url` has no arm for the scheme. A caller deciding something
// about a target asks this first, so the recognition of the scheme stays in one
// place rather than in each caller's prefix test.
func IsDockerURL(rawURL string) bool {
	parsed, err := Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return false
	}
	return parsed.Scheme == dockerScheme
}

func DialectFromURL(rawURL string) (string, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return "", nil
	}
	// The scheme is all a dialect needs, and a MySQL-family URL may be in a
	// form net/url refuses, so its scheme is read as text.
	if scheme, _, ok := cutMySQLScheme(rawURL); ok {
		return platform.NormalizeDialect(scheme), nil
	}
	parsed, err := Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse --dev-url: %w", err)
	}
	if parsed.Scheme == dockerScheme {
		return dialectFromDockerURL(parsed)
	}
	// Every spelling platform.NormalizeDialect accepts, rather than a list
	// copied from it.
	//
	// The copy had drifted by fifteen spellings, and two of them were canonical
	// dialect names rather than aliases: `oracle://` and `spanner://` were
	// refused as a dev URL while `ptah schema render --dialect oracle` and
	// `--dialect spanner` both worked. The other thirteen were documented
	// aliases -- `crdb`, `ch`, `pgx`, `tsql`, `ysql`, `sql-server`,
	// `cloudspanner` and the rest -- refused here alone while every other
	// boundary took them. That is the defect stokaro/ptah#270 fixed for lint
	// dialects, in a second hand-maintained list (stokaro/ptah#1875).
	//
	// A scheme that names no dialect still fails: NormalizeDialect answers the
	// empty string for it, which is what the refusal below reads.
	if dialect := platform.NormalizeDialect(parsed.Scheme); dialect != "" {
		return dialect, nil
	}
	return "", fmt.Errorf("unsupported --dev-url dialect %q", rawURL)
}

// ValidateDialectMatch verifies that rawURL can name a server of the dialect
// the already-open target database has. The comparison is
// [SchemeDialectMatches]'s, because the dialect of rawURL is read from its
// scheme, before anything is connected.
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
	if !SchemeDialectMatches(dialect, normalizedTarget) {
		return fmt.Errorf("--dev-url dialect %q does not match --url dialect %q", dialect, normalizedTarget)
	}
	return nil
}

// SchemeDialectMatches reports whether a dialect read from a URL scheme can
// name a server of the other dialect: the two are the same, or both are in the
// MySQL family.
//
// A MySQL-family scheme selects a driver, not a server. `mysql://`,
// `mariadb://` and `maria://` open the same MySQL driver, and whether the
// server behind them is MySQL or MariaDB only the server says, in its version
// banner. The pinned community binary v1.3.0 reads them that way: its `schema
// inspect` output was byte-identical through all three schemes against MariaDB
// 11.8.9 and against MySQL 8.4.11, and it accepts any pair of them for a
// target and its dev database (stokaro/ptah#3756). Held to the spelling, a
// comparison refuses a dev URL naming the target's own server.
//
// Which server each URL reaches is compared once both are connected. A check
// made before connecting can compare only what the schemes can say.
func SchemeDialectMatches(schemeDialect, other string) bool {
	return identityDialect(schemeDialect) == identityDialect(other)
}

// identityDialect is the dialect a URL is compared under: the MySQL family
// is one, since its schemes cannot tell MySQL from MariaDB, and every other
// dialect is itself.
func identityDialect(dialect string) string {
	switch normalized := platform.NormalizeDialect(dialect); normalized {
	case platform.MySQL, platform.MariaDB:
		return platform.MySQL
	case "":
		return dialect
	default:
		return normalized
	}
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
	leftURL, err := parseDatabaseURL(left)
	if err != nil {
		return false, err
	}
	rightURL, err := parseDatabaseURL(right)
	if err != nil {
		return false, err
	}
	if identityDialect(leftURL.dialect) != identityDialect(rightURL.dialect) {
		return false, nil
	}
	if leftURL.dialect == platform.SQLite {
		return sameSQLiteDatabase(leftURL.parsed, rightURL.parsed)
	}
	leftIdentity, err := leftURL.identity()
	if err != nil {
		return false, err
	}
	rightIdentity, err := rightURL.identity()
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
	leftURL, err := parseDatabaseURL(left)
	if err != nil {
		return false, err
	}
	rightURL, err := parseDatabaseURL(right)
	if err != nil {
		return false, err
	}
	if identityDialect(leftURL.dialect) != identityDialect(rightURL.dialect) {
		return false, nil
	}
	if leftURL.dialect == platform.SQLite {
		return sameSQLiteDatabase(leftURL.parsed, rightURL.parsed)
	}
	leftIdentity, err := leftURL.identity()
	if err != nil {
		return false, err
	}
	rightIdentity, err := rightURL.identity()
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
		dialect:  identityDialect(dialect),
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

// databaseURL is a database URL read far enough to compare it with another.
// A MySQL-family URL is read by [ParseMySQLURL] and every other one by [Parse].
type databaseURL struct {
	dialect string
	parsed  *url.URL
	mysql   *MySQLURL
}

func parseDatabaseURL(rawURL string) (databaseURL, error) {
	rawURL = strings.TrimSpace(rawURL)
	mysqlURL, err := ParseMySQLURL(rawURL)
	switch {
	case err == nil:
		return databaseURL{dialect: mysqlURL.Dialect(), mysql: &mysqlURL}, nil
	case !errors.Is(err, ErrNotMySQLURL):
		return databaseURL{}, errors.New("invalid database URL")
	}
	// [Parse], not url.Parse. This package exports the Windows rule and then
	// has to obey it: reaching for the standard parser here refused every
	// Windows SQLite address the endpoint comparison was given, so `schema
	// apply`, `migrate diff` and the rollback verification all answered
	// "invalid database URL" for a path that is perfectly valid on the
	// operating system they were running on.
	parsed, err := Parse(rawURL)
	if err != nil {
		return databaseURL{}, errors.New("invalid database URL")
	}
	dialect := platform.NormalizeDialect(parsed.Scheme)
	if dialect == "" {
		return databaseURL{}, errors.New("unsupported database URL dialect")
	}
	return databaseURL{dialect: dialect, parsed: parsed}, nil
}

// identity is the server and database the URL selects.
func (d databaseURL) identity() (databaseIdentity, error) {
	if d.mysql != nil {
		return mysqlDatabaseIdentity(*d.mysql), nil
	}
	return networkDatabaseIdentity(d.parsed, d.dialect)
}

// mysqlDatabaseIdentity reads the server and the database from what the driver
// connects to, so a socket URL is compared by its socket and its `database`
// parameter rather than by a path that names the socket.
func mysqlDatabaseIdentity(u MySQLURL) databaseIdentity {
	endpoint := normalizedDatabaseHost(u.Address()) + "\x00unix"
	if host, port, ok := u.HostPort(); ok {
		endpoint = networkEndpoint(host, port, u.Dialect())
	}
	return databaseIdentity{dialect: identityDialect(u.Dialect()), endpoint: endpoint, database: u.Database()}
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

// WithDatabaseName returns rawURL addressing the database name instead of the
// one it names now.
//
// It is for a server URL -- one whose path is the database -- and it replaces
// that path rather than appending to it, so a base URL already naming a
// database yields a sibling rather than a nested path. Everything else the URL
// carries, credentials, host, port and query, is preserved: dropping a
// `sslmode` or a `parseTime` while renaming the database would change how the
// connection behaves for a reason the caller never asked about.
//
// A MySQL-family URL keeps the form it was written in, and the database goes
// where that form puts it: [MySQLURL.WithDatabase] says where. In a +unix URL
// the path is the socket, so replacing the path would point the connection at
// a socket named after the database.
//
// SQLite is refused. Its URL names a file rather than a server, so swapping the
// path would name a database in another directory rather than another database
// on the same server, and the caller that wants a disposable SQLite database
// wants a new file instead.
func WithDatabaseName(rawURL, name string) (string, error) {
	mysqlURL, err := ParseMySQLURL(rawURL)
	switch {
	case err == nil:
		return mysqlURL.WithDatabase(name), nil
	case !errors.Is(err, ErrNotMySQLURL):
		return "", err
	}
	parsed, err := Parse(rawURL)
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "sqlite" || parsed.Scheme == "sqlite3" || parsed.Scheme == "file" {
		return "", fmt.Errorf("a %s URL names a file rather than a server, so it has no database name to replace", parsed.Scheme)
	}
	parsed.Path = "/" + name
	parsed.Opaque = ""
	return parsed.String(), nil
}
