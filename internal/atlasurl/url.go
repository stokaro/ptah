// Package atlasurl interprets Atlas-style database URLs: mapping a URL to a
// Ptah dialect, validating that a URL matches a target dialect, and deciding
// whether two URLs address the same database.
//
// It names no engine. What an engine states about its URLs is read from its
// descriptor (descriptor.URLs and descriptor.Atlas), and what an engine does
// differently with one, such as reading it with its driver's own parser, is
// asked of its services in internal/dburlext.
package atlasurl

import (
	"cmp"
	"errors"
	"fmt"
	"net"
	"net/url"
	posixpath "path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/dburlext"
	"ptah.run/internal/dburlext/builtin"
	"ptah.run/internal/descriptortable"
)

// dockerScheme is the URL scheme that asks Ptah to start a dev database from
// an engine's own image: `docker://postgres/16/dev`.
const dockerScheme = "docker"

// dockerImage is the engine a `docker://<engine>/...` URL names.
type dockerImage struct {
	// dialect is the dialect of the server it starts.
	dialect string
	// extension reports that no Atlas edition has a driver for its engine.
	extension bool
}

// dockerImages are the engines a `docker://<engine>/...` URL may name: the
// images every descriptor states (descriptor.Atlas.DockerImages), each with
// the dialect that claims it. They are matched as written: the pinned
// community binary v1.3.0 refuses `docker://POSTGRES/...` and
// `docker://Postgres/...` with `unsupported docker image`. An engine that
// binary answers `unknown driver` for, such as ClickHouse, claims no image.
//
// internal/devdocker keeps what it needs to start each engine; these are the
// one answer to which engines there are and what each speaks.
var dockerImages = descriptortable.NewIndex(func(dialects []descriptor.Dialect) map[string]dockerImage {
	images := make(map[string]dockerImage)
	for _, described := range dialects {
		for _, image := range described.Atlas.DockerImages {
			images[image] = dockerImage{dialect: described.Name, extension: described.Atlas.Extension}
		}
	}
	return images
})

// DockerEngineDialect returns the dialect of the server a `docker://` URL
// naming engine starts, and whether engine is one Ptah starts. engine is
// compared as written.
func DockerEngineDialect(engine string) (string, bool) {
	image, ok := dockerImages.Get()[engine]
	return image.dialect, ok
}

// DockerEngineIsExtension reports whether engine is one Ptah starts from a
// `docker://` URL and the pinned community binary refuses as an unsupported
// image: an image of an engine no Atlas edition has a driver for
// (descriptor.Atlas.Extension). Measured on v1.3.0, `docker://ydb/...`
// answers `unsupported docker image "ydb"`. Strict compatibility refuses each
// in those words.
func DockerEngineIsExtension(engine string) bool {
	return dockerImages.Get()[engine].extension
}

// dockerImageSchemePrefix begins the scheme that asks Ptah to start a dev
// database from the image the URL names: `docker+postgres://_/postgres:17/dev`.
const dockerImageSchemePrefix = "docker+"

// dockerImageDrivers are the drivers the pinned community binary v1.3.0 accepts
// after [dockerImageSchemePrefix], each with the dialect that claims it
// (descriptor.Atlas.DockerDrivers). It registers exactly these, so any other
// `docker+` scheme is a driver it does not know: measured, `docker+sqlite`,
// `docker+postgis` and `docker+nosuch` all answer `sql/sqlclient: unknown
// driver` at exit 1.
var dockerImageDrivers = descriptortable.NewIndex(func(dialects []descriptor.Dialect) map[string]string {
	drivers := make(map[string]string)
	for _, described := range dialects {
		for _, driver := range described.Atlas.DockerDrivers {
			drivers[driver] = described.Name
		}
	}
	return drivers
})

// DockerImageDriver returns the driver a `docker+<driver>` scheme names, and
// whether scheme is one of the `docker+` schemes the pinned community binary
// registers. scheme is compared as written; [url.Parse] already lowercases it.
func DockerImageDriver(scheme string) (string, bool) {
	driver, found := strings.CutPrefix(scheme, dockerImageSchemePrefix)
	if _, known := dockerImageDrivers.Get()[driver]; !found || !known {
		return "", false
	}
	return driver, true
}

// IsDockerScheme reports whether scheme asks Ptah to start a dev database:
// `docker`, or one of the `docker+<driver>` schemes [DockerImageDriver]
// recognizes. Every boundary that treats a docker dev URL differently from a
// database URL asks this, so the two spellings cannot be recognized in one
// place and missed in another.
func IsDockerScheme(scheme string) bool {
	if scheme == dockerScheme {
		return true
	}
	_, ok := DockerImageDriver(scheme)
	return ok
}

// defaultPorts is the port a URL of each dialect reaches when it names none
// (descriptor.URLs.DefaultPort), spelled as a URL spells it, or empty where
// the descriptor states none.
var defaultPorts = descriptortable.New(func(described descriptor.Dialect) string {
	if described.URLs.DefaultPort == 0 {
		return ""
	}
	return strconv.Itoa(int(described.URLs.DefaultPort))
})

// comparedAs is the dialect a URL of each dialect is compared as
// (descriptor.URLs.ComparedAs): itself unless its descriptor names another.
// An unknown dialect answers empty.
var comparedAs = descriptortable.New(func(described descriptor.Dialect) string {
	return cmp.Or(described.URLs.ComparedAs, described.Name)
})

// mysqlGrammar reports for each dialect whether its URLs take go-sql-driver's
// forms (descriptor.URLs.Grammar), which [ParseMySQLURL] reads.
var mysqlGrammar = descriptortable.New(func(described descriptor.Dialect) bool {
	return described.URLs.Grammar == descriptor.URLGrammarGoSQLDriver
})

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
// Which schemes those are is the descriptors' answer: every spelling of a
// dialect whose URLs take go-sql-driver's forms (descriptor.URLs.Grammar),
// MySQL's and MariaDB's, in any letter case, since net/url lowercases a scheme
// too. The scheme is read as text because net/url refuses
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

// IsDockerURL reports whether rawURL is a `docker://` or `docker+<driver>://`
// URL, a spelling that asks Ptah to start a dev database rather than naming one
// that exists.
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
	return IsDockerScheme(parsed.Scheme)
}

func DialectFromURL(rawURL string) (string, error) {
	asWritten := rawURL
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
	// A docker URL is read as written, as internal/devdocker reads it, because
	// whether it starts a container depends on the bytes. Measured on the
	// pinned community binary v1.3.0, ` docker://sqlite/dev` with its leading
	// space answers `parse open url: first path segment in URL cannot contain
	// colon` on `migrate diff`, `migrate lint` and `schema diff`, where the
	// value without the space answers `unsupported docker image "sqlite"`.
	// Trimmed, the value would be refused as an image, a sentence about a
	// container that value cannot start.
	if IsDockerScheme(parsed.Scheme) {
		if _, err := url.Parse(asWritten); err != nil {
			return "", fmt.Errorf("parse --dev-url: %w", err)
		}
	}
	if parsed.Scheme == dockerScheme {
		return dialectFromDockerURL(parsed)
	}
	if driver, ok := DockerImageDriver(parsed.Scheme); ok {
		return dockerImageDrivers.Get()[driver], nil
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
// name a server of the other dialect: the two are compared as one dialect
// (descriptor.URLs.ComparedAs).
//
// A scheme selects a driver, not a server. `mysql://`, `mariadb://` and
// `maria://` open the same MySQL driver, and whether the server behind them
// is MySQL or MariaDB only the server says, in its version banner. The pinned
// community binary v1.3.0 reads them that way: its `schema inspect` output was
// byte-identical through all three schemes against MariaDB 11.8.9 and against
// MySQL 8.4.11, and it accepts any pair of them for a target and its dev
// database (stokaro/ptah#3756). Held to the spelling, a comparison refuses a
// dev URL naming the target's own server.
//
// Which server each URL reaches is compared once both are connected. A check
// made before connecting can compare only what the schemes can say.
func SchemeDialectMatches(schemeDialect, other string) bool {
	return identityDialect(schemeDialect) == identityDialect(other)
}

// identityDialect is the dialect a URL is compared under: the one its
// descriptor names, or itself. A dialect no descriptor states is compared as
// written.
func identityDialect(dialect string) string {
	if compared := comparedAs.Get(dialect); compared != "" {
		return compared
	}
	return dialect
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
	if databases := builtin.Lookup(leftURL.dialect).Databases; databases != nil {
		return databases.SameDatabase(leftURL.parsed, rightURL.parsed)
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
	if databases := builtin.Lookup(leftURL.dialect).Databases; databases != nil {
		return databases.SameDatabase(leftURL.parsed, rightURL.parsed)
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

// networkDatabaseIdentity is the server and database a URL of a dialect that
// names a server selects: what the dialect's services read
// (dburlext.Services.Endpoints), or the host, port and path the URL names.
func networkDatabaseIdentity(parsed *url.URL, dialect string) (databaseIdentity, error) {
	endpoint := networkEndpoint(parsed.Hostname(), parsed.Port(), dialect)
	database := strings.Trim(parsed.Path, "/")
	if reader := builtin.Lookup(dialect).Endpoints; reader != nil {
		described, _ := platform.Lookup(dialect)
		selected, err := reader.Select(parsed, described)
		if err != nil {
			return databaseIdentity{}, err
		}
		if len(selected.Endpoints) != 0 {
			endpoints := make([]string, 0, len(selected.Endpoints))
			for _, server := range selected.Endpoints {
				endpoints = append(endpoints, serverEndpoint(server, dialect))
			}
			endpoint = networkEndpointRoute(endpoints)
		}
		database = selected.Database
	}
	return databaseIdentity{
		dialect:  identityDialect(dialect),
		endpoint: endpoint,
		database: database,
	}, nil
}

// serverEndpoint is the endpoint of one server a service read, with the
// instance it names on its host and port.
func serverEndpoint(server dburlext.Endpoint, dialect string) string {
	endpoint := networkEndpoint(server.Host, server.Port, dialect)
	if server.Instance != "" {
		endpoint += "\x00" + server.Instance
	}
	return endpoint
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

func networkEndpoint(host, port, dialect string) string {
	if port == "" {
		port = defaultPorts.Get(dialect)
	}
	return normalizedDatabaseHost(host) + "\x00" + port
}

// normalizedDatabaseHost folds a host to the one spelling of the server it
// names. A socket path in a URL is a POSIX path whatever machine reads it, so a
// leading slash is cleaned with slash semantics on every platform:
// filepath.IsAbs answers false on Windows for /run/mysqld/mysqld.sock, and the
// same socket spelled with a `..` segment would then read as another server. A
// drive-letter path is still cleaned the Windows way.
func normalizedDatabaseHost(host string) string {
	if posixpath.IsAbs(host) {
		return posixpath.Clean(host)
	}
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

// dialectFromDockerURL answers the dialect of the engine a `docker://` URL
// names. The host is the engine, matched whole and as written, and a value that
// names none is refused in the pinned community binary's own words, quoting the
// host: `unsupported docker image "postgres:16"`, `unsupported docker image ""`.
// internal/devdocker refuses the same values with the same sentence.
func dialectFromDockerURL(parsed *url.URL) (string, error) {
	dialect, ok := DockerEngineDialect(parsed.Host)
	if !ok {
		return "", fmt.Errorf("unsupported docker image %q", parsed.Host)
	}
	return dialect, nil
}

// fileSchemes are the schemes of a URL that names a database file rather
// than a server (descriptor.URLs.FileSchemes), such as SQLite's sqlite and
// file. A libsql URL names a server.
var fileSchemes = descriptortable.NewIndex(func(dialects []descriptor.Dialect) map[string]bool {
	schemes := make(map[string]bool)
	for _, described := range dialects {
		for _, scheme := range described.URLs.FileSchemes {
			schemes[scheme] = true
		}
	}
	return schemes
})

// NamesAFile reports whether rawURL names a database file rather than a
// server. Opening one creates the file when it does not exist, and the engine
// behind it is the one compiled into Ptah.
func NamesAFile(rawURL string) bool {
	parsed, err := Parse(rawURL)
	return err == nil && fileSchemes.Get()[parsed.Scheme]
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
// A URL that names a file is refused: swapping its path would name a database
// in another directory rather than another database on the same server, and
// the caller that wants a disposable SQLite database wants a new file
// instead. So is a URL whose engine's services refuse a rename
// (dburlext.Services.Rename), such as YDB's.
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
	if fileSchemes.Get()[parsed.Scheme] {
		return "", fmt.Errorf("a %s URL names a file rather than a server, so it has no database name to replace", parsed.Scheme)
	}
	if renamer := builtin.Lookup(parsed.Scheme).Rename; renamer != nil {
		return renamer.Rename(parsed, name)
	}
	parsed.Path = "/" + name
	parsed.Opaque = ""
	return parsed.String(), nil
}
