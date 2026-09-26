package atlasurl

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/core/platform"
)

// This file is the one reader of a MySQL-family database URL. Every part of
// Ptah that needs the database, the server address or the credentials of such
// a URL asks [ParseMySQLURL], because the URL has three shapes and each puts
// the database somewhere else:
//
//	mysql://user:pass@host:3306/app                  the URL form
//	mysql://user:pass@tcp(host:3306)/app             go-sql-driver's own form, unix(...) too
//	mysql+unix://user:pass@/run/mysqld/mysqld.sock?database=app
//
// In the socket form the path is the socket, and the database is the
// `database` query parameter. The pinned community binary v1.3.0 opens that
// form for `mysql+unix`, `maria+unix` and `mariadb+unix` on every verb that
// takes a database URL (stokaro/ptah#3755). A reader that took the database
// from the path would read the socket path instead, and the endpoint check that
// keeps dev-database cleanup off the target would compare the wrong names.

// mysqlSocketTransport is the scheme suffix that selects the socket form.
const mysqlSocketTransport = "+unix"

// mysqlSocketDatabaseParam is the query parameter that names the database in
// the socket form. It is matched case-sensitively, as the pinned community
// binary matches it: `Database=app` reaches the server as a system variable.
const mysqlSocketDatabaseParam = "database"

// ErrNotMySQLURL reports a URL whose scheme selects no MySQL-family driver.
var ErrNotMySQLURL = errors.New("not a MySQL or MariaDB URL")

// mysqlURLForm is the shape a MySQL-family URL was written in.
type mysqlURLForm int

const (
	// mysqlURLFormURL is scheme://user:pass@host:port/database.
	mysqlURLFormURL mysqlURLForm = iota
	// mysqlURLFormDriver is go-sql-driver's grammar behind the scheme:
	// user:pass@tcp(host:port)/database or user:pass@unix(/path)/database.
	mysqlURLFormDriver
	// mysqlURLFormSocket is scheme+unix://user:pass@/path?database=name.
	mysqlURLFormSocket
)

// MySQLURL is a MySQL-family database URL, read once.
//
// Its accessors answer from the DSN the connection is opened with, read by the
// driver's own parser, so what a caller learns about the database and the
// server is what the driver connects to.
type MySQLURL struct {
	scheme string
	form   mysqlURLForm
	dsn    string
	config *mysqldriver.Config
	// parsed is the URL for the forms net/url reads, kept so that
	// [MySQLURL.WithDatabase] can change one part and leave the rest as
	// written.
	parsed *url.URL
}

// ParseMySQLURL reads a MySQL-family database URL in any of its forms.
//
// It returns [ErrNotMySQLURL] when the scheme selects no MySQL-family driver,
// and an error naming the problem when the URL cannot be read. The error never
// repeats the URL, which may carry a password.
func ParseMySQLURL(rawURL string) (MySQLURL, error) {
	scheme, rest, ok := cutMySQLScheme(rawURL)
	if !ok {
		return MySQLURL{}, ErrNotMySQLURL
	}
	parsed := MySQLURL{scheme: scheme}
	switch {
	case strings.HasSuffix(strings.ToLower(scheme), mysqlSocketTransport):
		parsed.form = mysqlURLFormSocket
		if err := parsed.readSocketForm(rawURL); err != nil {
			return MySQLURL{}, err
		}
	case hasMySQLNetwork(rest):
		parsed.form = mysqlURLFormDriver
		parsed.dsn = rest
	default:
		parsed.form = mysqlURLFormURL
		if err := parsed.readURLForm(rawURL); err != nil {
			return MySQLURL{}, err
		}
	}
	config, err := mysqldriver.ParseDSN(parsed.dsn)
	if err != nil {
		return MySQLURL{}, fmt.Errorf("invalid %s URL: %w", scheme, err)
	}
	parsed.config = config
	return parsed, nil
}

// hasMySQLNetwork reports whether what follows the scheme is go-sql-driver's
// own grammar: a network and its address, with or without credentials before
// it. tcp and unix are one grammar with two networks, so recognizing one
// without the other reads a socket address as a host called "unix(".
func hasMySQLNetwork(rest string) bool {
	for _, network := range []string{"tcp(", "unix("} {
		if strings.HasPrefix(rest, network) || strings.Contains(rest, "@"+network) {
			return true
		}
	}
	return false
}

// readURLForm builds the DSN for scheme://user:pass@host:port/database.
//
// net/url decodes the credentials and the database, and the database is
// escaped again for the driver, which unescapes its database segment. Without
// that, a name carrying "/" or "%" reaches the driver as a different split.
func (u *MySQLURL) readURLForm(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid %s URL: %w", u.scheme, errors.Unwrap(err))
	}
	password, _ := parsed.User.Password()
	u.parsed = parsed
	u.dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s", parsed.User.Username(), password, parsed.Host,
		url.PathEscape(strings.TrimPrefix(parsed.Path, "/")))
	if parsed.RawQuery != "" {
		u.dsn += "?" + parsed.RawQuery
	}
	return nil
}

// readSocketForm builds the DSN for scheme+unix://user:pass@/path?database=name
// the way the pinned community binary does: the path is the socket, a host is
// ignored, the `database` parameter names the database, and every other
// parameter reaches the driver.
//
// An empty `database` is refused. The community binary hands it on and the
// server answers with a syntax error; reading it as no database would open the
// whole server for a URL that asked for one.
func (u *MySQLURL) readSocketForm(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid %s URL: %w", u.scheme, errors.Unwrap(err))
	}
	query := parsed.Query()
	database := query.Get(mysqlSocketDatabaseParam)
	if query.Has(mysqlSocketDatabaseParam) && database == "" {
		return fmt.Errorf("invalid %s URL: the %s parameter is empty; name a database, or leave the parameter out",
			u.scheme, mysqlSocketDatabaseParam)
	}
	query.Del(mysqlSocketDatabaseParam)
	u.parsed = parsed

	var dsn strings.Builder
	dsn.WriteString(parsed.User.Username())
	if password, ok := parsed.User.Password(); ok {
		dsn.WriteString(":" + password)
	}
	if dsn.Len() > 0 {
		dsn.WriteString("@")
	}
	dsn.WriteString("unix(" + parsed.Path + ")/" + url.PathEscape(database))
	if encoded := query.Encode(); encoded != "" {
		dsn.WriteString("?" + encoded)
	}
	u.dsn = dsn.String()
	return nil
}

// Scheme is the scheme as written.
func (u MySQLURL) Scheme() string {
	return u.scheme
}

// Dialect is the dialect the scheme spells: [platform.MySQL] or
// [platform.MariaDB]. Which of the two a server is, only the server says.
func (u MySQLURL) Dialect() string {
	return platform.NormalizeDialect(u.scheme)
}

// DSN is what the driver is handed. The driver's own form is passed on as
// written; the other forms are rebuilt in that grammar.
func (u MySQLURL) DSN() string {
	return u.dsn
}

// Database is the database the URL selects, or "" when it selects none and
// the session starts without a default database.
func (u MySQLURL) Database() string {
	return u.config.DBName
}

// Network is "tcp" or "unix".
func (u MySQLURL) Network() string {
	return u.config.Net
}

// Address is the server the driver dials: host:port for TCP, with the
// driver's defaults filled in, or the socket path.
func (u MySQLURL) Address() string {
	return u.config.Addr
}

// HostPort splits a TCP address. It reports false for a socket.
func (u MySQLURL) HostPort() (host, port string, ok bool) {
	if u.config.Net != "tcp" {
		return "", "", false
	}
	host, port, err := net.SplitHostPort(u.config.Addr)
	if err != nil {
		return u.config.Addr, "", true
	}
	return host, port, true
}

// User is the user the connection authenticates as.
func (u MySQLURL) User() string {
	return u.config.User
}

// Password is the password the connection authenticates with.
func (u MySQLURL) Password() string {
	return u.config.Passwd
}

// URL is the address as net/url carries it, for a caller that displays it or
// reads its query.
//
// The URL and socket forms are returned as written. The go-sql-driver form is
// one net/url refuses, so it is rendered in the form it is equivalent to: a
// TCP address as the URL form, a socket as the +unix form. The result is a
// copy the caller may change.
func (u MySQLURL) URL() *url.URL {
	if u.form != mysqlURLFormDriver {
		rendered := *u.parsed
		return &rendered
	}
	rendered := &url.URL{Scheme: strings.ToLower(u.scheme), User: mysqlUserinfo(u.config.User, u.config.Passwd)}
	query := u.driverQuery()
	if u.config.Net == "unix" {
		rendered.Scheme += mysqlSocketTransport
		rendered.Path = u.config.Addr
		query.Set(mysqlSocketDatabaseParam, u.config.DBName)
	} else {
		rendered.Host = u.config.Addr
		rendered.Path = "/" + u.config.DBName
	}
	rendered.RawQuery = query.Encode()
	return rendered
}

// driverQuery is the parameter list of a go-sql-driver DSN, which follows the
// first "?" after its last "/", as the driver reads it.
func (u MySQLURL) driverQuery() url.Values {
	_, raw, _ := strings.Cut(u.dsn[strings.LastIndex(u.dsn, "/")+1:], "?")
	query, err := url.ParseQuery(raw)
	if err != nil {
		// The driver read these parameters when the URL was parsed, so a
		// failure is unreachable; an empty list is what is left to show.
		return url.Values{}
	}
	return query
}

// mysqlUserinfo is the credentials as net/url carries them, or nil for none.
func mysqlUserinfo(user, password string) *url.Userinfo {
	switch {
	case password != "":
		return url.UserPassword(user, password)
	case user != "":
		return url.User(user)
	default:
		return nil
	}
}

// WithDatabase returns the URL in the form it was written in, selecting the
// named database instead, with everything else it carries unchanged.
func (u MySQLURL) WithDatabase(name string) string {
	switch u.form {
	case mysqlURLFormSocket:
		renamed := *u.parsed
		query := renamed.Query()
		query.Set(mysqlSocketDatabaseParam, name)
		renamed.RawQuery = query.Encode()
		return renamed.String()
	case mysqlURLFormDriver:
		config := u.config.Clone()
		config.DBName = name
		return u.scheme + "://" + config.FormatDSN()
	default:
		renamed := *u.parsed
		renamed.Path = "/" + name
		renamed.RawPath = ""
		return renamed.String()
	}
}

// cutMySQLScheme splits a MySQL-family URL at its "://".
func cutMySQLScheme(rawURL string) (scheme, rest string, ok bool) {
	scheme, rest, found := strings.Cut(rawURL, "://")
	if !found || !isURLScheme(scheme) {
		return "", rawURL, false
	}
	switch platform.NormalizeDialect(scheme) {
	case platform.MySQL, platform.MariaDB:
		return scheme, rest, true
	default:
		return "", rawURL, false
	}
}
