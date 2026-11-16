//go:build integration

package integration_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io/fs"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/testpki"
)

// These tests connect to a MySQL server whose certificate an authority the
// test generated signed, through a URL whose ssl-ca names that authority. The
// server is started by the test, so a local run and CI run the same fixture:
// CI's MySQL service presents the certificate the image generates for itself,
// which names no host and so cannot pass hostname verification.
//
// The server's certificate is installed through SQL rather than a mounted
// file, so the fixture needs nothing from the container runtime beyond the
// container: the files are written under secure_file_priv and the server
// reloads its TLS settings from them.
//
// The account the URLs log in as is created with REQUIRE SSL, so the server,
// not the client, says whether a connection was encrypted.

// tlsMySQLDevURL starts the server: the image CI's MySQL service runs.
const tlsMySQLDevURL = "docker://mysql/26.10/ptah_tls"

// tlsMySQLAccount is the account the URLs under test log in as.
const (
	tlsMySQLUser     = "ptah_tls"
	tlsMySQLPassword = "tls-Pa5wordX9" // #nosec G101 -- a made-up password for a disposable server
)

// tlsMySQLFiles is the directory secure_file_priv lets the server write to.
const tlsMySQLFiles = "/var/lib/mysql-files/"

// tlsMySQL is a disposable MySQL server presenting a certificate the test's
// authority signed.
type tlsMySQL struct {
	admin     *sql.DB
	host      string
	port      string
	authority *testpki.Authority
	// bundle holds the authority's certificate.
	bundle string
	// unrelated holds the certificate of an authority that signed nothing.
	unrelated string
}

// startTLSMySQL provisions the server, installs a certificate for the host
// this process dials it by, and creates the REQUIRE SSL account.
func startTLSMySQL(c *qt.C) *tlsMySQL {
	c.Helper()
	ctx := c.Context()
	instance, err := devdocker.Provision(ctx, tlsMySQLDevURL, devdocker.Options{})
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(instance.Close(), qt.IsNil) })

	parsed, err := atlasurl.ParseMySQLURL(instance.URL())
	c.Assert(err, qt.IsNil)
	host, port, _ := parsed.HostPort()
	admin, err := sql.Open("mysql", parsed.DSN())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = admin.Close() })
	admin.SetMaxOpenConns(1)

	authority, err := testpki.NewAuthority("ptah ssl-ca e2e authority")
	c.Assert(err, qt.IsNil)
	unrelated, err := testpki.NewAuthority("ptah ssl-ca e2e unrelated authority")
	c.Assert(err, qt.IsNil)
	dir := c.TempDir()
	server := &tlsMySQL{
		admin:     admin,
		host:      host,
		port:      port,
		authority: authority,
		bundle:    filepath.Join(dir, "ca.pem"),
		unrelated: filepath.Join(dir, "unrelated-ca.pem"),
	}
	c.Assert(os.WriteFile(server.bundle, authority.CertificatePEM(), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(server.unrelated, unrelated.CertificatePEM(), 0o600), qt.IsNil)

	server.present(c, "verified", host)
	for _, statement := range []string{
		"CREATE USER '" + tlsMySQLUser + "'@'%' IDENTIFIED BY '" + tlsMySQLPassword + "' REQUIRE SSL",
		"GRANT ALL PRIVILEGES ON ptah_tls.* TO '" + tlsMySQLUser + "'@'%'",
		"CREATE TABLE ptah_tls.tls_probe (id int PRIMARY KEY)",
	} {
		_, err := admin.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return server
}

// present makes the server present a certificate the authority issued for
// hosts, under file names that start with name: SELECT ... INTO DUMPFILE
// refuses to replace a file.
func (s *tlsMySQL) present(c *qt.C, name string, hosts ...string) {
	c.Helper()
	ctx := c.Context()
	leaf, err := s.authority.Issue(hosts...)
	c.Assert(err, qt.IsNil)
	files := []struct {
		variable string
		path     string
		content  []byte
	}{
		{variable: "ssl_ca", path: tlsMySQLFiles + name + "-ca.pem", content: s.authority.CertificatePEM()},
		{variable: "ssl_cert", path: tlsMySQLFiles + name + "-cert.pem", content: leaf.CertificatePEM},
		{variable: "ssl_key", path: tlsMySQLFiles + name + "-key.pem", content: leaf.KeyPEM},
	}
	for _, file := range files {
		// #nosec G201 -- the content is hex the test generated and the path a constant of this file.
		_, err := s.admin.ExecContext(ctx, fmt.Sprintf("SELECT X'%x' INTO DUMPFILE '%s'", file.content, file.path))
		c.Assert(err, qt.IsNil)
		// #nosec G201 -- the variable and the path are constants of this file.
		_, err = s.admin.ExecContext(ctx, fmt.Sprintf("SET GLOBAL %s = '%s'", file.variable, file.path))
		c.Assert(err, qt.IsNil)
	}
	_, err = s.admin.ExecContext(ctx, "ALTER INSTANCE RELOAD TLS")
	c.Assert(err, qt.IsNil)
}

// url is the account's URL of the server, by host, with query appended.
func (s *tlsMySQL) url(host, query string) string {
	return "mysql://" + tlsMySQLUser + ":" + tlsMySQLPassword + "@" + net.JoinHostPort(host, s.port) + "/ptah_tls?" + query
}

// caQuery is the query naming bundle in ssl-ca.
func caQuery(bundle string) string {
	return "ssl-ca=" + url.QueryEscape(bundle)
}

// connectionAttempts is the number of connections the server has been asked
// for, refused ones included.
func (s *tlsMySQL) connectionAttempts(c *qt.C) int {
	c.Helper()
	var name string
	var attempts int
	c.Assert(s.admin.QueryRowContext(c.Context(), "SHOW GLOBAL STATUS LIKE 'Connections'").Scan(&name, &attempts), qt.IsNil)
	return attempts
}

// encryption is the TLS version a connection's session runs under, which the
// server reports as "" for an unencrypted one.
func encryption(c *qt.C, conn *dbschema.DatabaseConnection) string {
	c.Helper()
	session, err := conn.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	defer func() { _ = session.Close() }()
	var name, version string
	c.Assert(session.QueryRowContext(c.Context(), "SHOW SESSION STATUS LIKE 'Ssl_version'").Scan(&name, &version), qt.IsNil)
	return version
}

// connectTLS opens a connection the way every command does.
func connectTLS(c *qt.C, rawURL string) (*dbschema.DatabaseConnection, error) {
	c.Helper()
	ctx, cancel := context.WithTimeout(c.Context(), time.Minute)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(ctx, rawURL)
	c.Cleanup(func() { closeConnection(conn) })
	return conn, err
}

// closeConnection closes a connection that may not have opened.
func closeConnection(conn *dbschema.DatabaseConnection) {
	if conn != nil {
		_ = conn.Close()
	}
}

// TestMySQLCABundleE2E_HappyPath reaches the server through the right root on
// every surface that shares the connection: the library, native inspection
// and migrations, and the compatibility binary.
func TestMySQLCABundleE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	server := startTLSMySQL(c)
	verified := server.url(server.host, caQuery(server.bundle))
	options := clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute}

	t.Run("a connection is verified and encrypted", func(t *testing.T) {
		c := qt.New(t)

		conn, err := connectTLS(c, verified)

		c.Assert(err, qt.IsNil)
		c.Assert(encryption(c, conn), qt.Matches, `TLSv1\.[23]`)
	})

	t.Run("tls=true beside ssl-ca changes nothing", func(t *testing.T) {
		c := qt.New(t)

		conn, err := connectTLS(c, server.url(server.host, "tls=true&"+caQuery(server.bundle)))

		c.Assert(err, qt.IsNil)
		c.Assert(encryption(c, conn), qt.Matches, `TLSv1\.[23]`)
	})

	t.Run("native db read", func(t *testing.T) {
		c := qt.New(t)

		got := clirun.Run(c, clirun.Ptah, options, "db", "read", "--db-url", verified)

		c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
		c.Assert(got.Stdout, qt.Contains, "tls_probe")
	})

	t.Run("native migrations up", func(t *testing.T) {
		c := qt.New(t)
		dir := c.TempDir()
		c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_create_tls_widgets.up.sql"),
			[]byte("CREATE TABLE tls_widgets (id int PRIMARY KEY);\n"), 0o600), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_create_tls_widgets.down.sql"),
			[]byte("DROP TABLE tls_widgets;\n"), 0o600), qt.IsNil)

		got := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--db-url", verified, "--migrations-dir", dir)

		c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
		var tables int
		c.Assert(server.admin.QueryRowContext(c.Context(),
			"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'ptah_tls' AND TABLE_NAME = 'tls_widgets'",
		).Scan(&tables), qt.IsNil)
		c.Assert(tables, qt.Equals, 1)
	})

	t.Run("compatibility schema inspect", func(t *testing.T) {
		c := qt.New(t)

		got := clirun.Run(c, clirun.Compat, options, "schema", "inspect", "-u", verified)

		c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
		c.Assert(got.Stdout, qt.Contains, `table "tls_probe"`)
	})
}

// TestMySQLCABundleE2E_FailurePath refuses every connection the bundle does
// not vouch for, without falling back to an unverified or unencrypted one, and
// refuses a bundle it cannot use before the server is asked for a connection.
func TestMySQLCABundleE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	server := startTLSMySQL(c)
	options := clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute}

	t.Run("an unrelated root", func(t *testing.T) {
		c := qt.New(t)
		before := server.connectionAttempts(c)

		conn, err := connectTLS(c, server.url(server.host, caQuery(server.unrelated)))

		c.Assert(err, qt.ErrorAs, new(x509.UnknownAuthorityError))
		c.Assert(err, qt.Not(qt.ErrorMatches), `(?s).*`+tlsMySQLPassword+`.*`)
		c.Assert(conn, qt.IsNil)
		c.Assert(server.connectionAttempts(c)-before, qt.Equals, 1)
	})

	t.Run("an unrelated root through the compatibility binary", func(t *testing.T) {
		c := qt.New(t)

		got := clirun.Run(c, clirun.Compat, options, "schema", "inspect", "-u", server.url(server.host, caQuery(server.unrelated)))

		c.Assert(got.ExitCode, qt.Equals, 1)
		c.Assert(got.Stderr, qt.Contains, "x509: certificate signed by unknown authority")
		c.Assert(got.Stderr, qt.Not(qt.Contains), tlsMySQLPassword)
	})

	t.Run("a missing bundle, before the server is asked for a connection", func(t *testing.T) {
		c := qt.New(t)
		missing := filepath.Join(c.TempDir(), "missing.pem")
		before := server.connectionAttempts(c)

		conn, err := connectTLS(c, server.url(server.host, caQuery(missing)))
		got := clirun.Run(c, clirun.Ptah, options, "db", "read", "--db-url", server.url(server.host, caQuery(missing)))

		c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
		c.Assert(conn, qt.IsNil)
		c.Assert(got.ExitCode, qt.Not(qt.Equals), 0)
		c.Assert(got.Stderr, qt.Contains, "the ssl-ca parameter names a CA bundle Ptah cannot use: read CA bundle: open "+missing)
		c.Assert(got.Stderr, qt.Not(qt.Contains), tlsMySQLPassword)
		c.Assert(server.connectionAttempts(c)-before, qt.Equals, 0)
	})

	t.Run("the strict compatibility profile", func(t *testing.T) {
		c := qt.New(t)
		strict := options
		strict.Env = append(os.Environ(), "PTAH_ATLAS_STRICT_COMPAT=1")

		got := clirun.Run(c, clirun.Compat, strict, "schema", "inspect", "-u", server.url(server.host, caQuery(server.bundle)))

		c.Assert(got.ExitCode, qt.Equals, 1)
		c.Assert(got.Stderr, qt.Contains, "Atlas Community Edition strict compatibility does not support the ssl-ca URL parameter")
	})

	t.Run("the account refuses an unencrypted login", func(t *testing.T) {
		c := qt.New(t)

		conn, err := connectTLS(c, server.url(server.host, "tls=false"))

		var refused *mysqldriver.MySQLError
		c.Assert(err, qt.ErrorAs, &refused)
		c.Assert(refused.Number, qt.Equals, uint16(1045))
		c.Assert(conn, qt.IsNil)
	})

	t.Run("a verified connection beside a refused one", func(t *testing.T) {
		c := qt.New(t)
		first, err := connectTLS(c, server.url(server.host, caQuery(server.bundle)))
		c.Assert(err, qt.IsNil)
		held, err := first.Conn(c.Context())
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() { _ = held.Close() })

		_, unrelated := connectTLS(c, server.url(server.host, caQuery(server.unrelated)))
		second, err := connectTLS(c, server.url(server.host, caQuery(server.bundle)))

		c.Assert(unrelated, qt.ErrorAs, new(x509.UnknownAuthorityError))
		c.Assert(err, qt.IsNil)
		// The first pool opens a connection of its own while it holds one,
		// after the refused connection loaded another root.
		c.Assert(encryption(c, first), qt.Matches, `TLSv1\.[23]`)
		c.Assert(encryption(c, second), qt.Matches, `TLSv1\.[23]`)
	})

	t.Run("the system roots do not trust the bundle's authority", func(t *testing.T) {
		c := qt.New(t)

		conn, err := connectTLS(c, server.url(server.host, "tls=true"))

		c.Assert(err, qt.ErrorAs, new(*tls.CertificateVerificationError))
		c.Assert(conn, qt.IsNil)
	})

	// Last: it changes the certificate the server presents.
	t.Run("a certificate for another host", func(t *testing.T) {
		c := qt.New(t)
		server.present(c, "mismatch", "mysql.invalid")

		conn, err := connectTLS(c, server.url(server.host, caQuery(server.bundle)))

		c.Assert(err, qt.ErrorAs, new(x509.HostnameError))
		c.Assert(conn, qt.IsNil)
	})
}
