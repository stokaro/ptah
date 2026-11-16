package atlasurl

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// This file reads the TLS parameters Ptah owns in a MySQL-family URL.
//
// `ssl-ca` names a PEM file of certificate authorities the server's
// certificate is verified against, for this connection only. The spelling is
// the MySQL and MariaDB client option for the same file (`mysql --ssl-ca`), and
// the parameter the Atlas URL documentation names for a custom CA. The pinned
// community binary v1.3.0 does not apply it: measured against MySQL 26.10 with
// a server certificate signed by a private CA, the parameter reaches the server
// as a session variable and the connection fails with Error 1064, or, under
// tls=true, the handshake fails against the system roots whichever CA the
// parameter names.
//
// go-sql-driver never sees these parameters. It reads every parameter it does
// not know as a system variable to SET, and a name with a hyphen is never a
// valid one, so no URL that reaches a server through the driver today can
// carry one. Ptah therefore reads every `ssl-` parameter: `ssl-ca` is applied,
// and any other, such as the client certificate and key the Atlas
// documentation also names, is refused rather than sent as a statement the
// server rejects.

// mysqlCAParam names the CA bundle of a MySQL-family URL.
const mysqlCAParam = "ssl-ca"

// mysqlOwnedParamPrefix starts every parameter Ptah reads out of a
// MySQL-family URL before the driver is handed the rest.
const mysqlOwnedParamPrefix = "ssl-"

// mysqlOwnedParams is what a MySQL-family URL's query holds of Ptah's
// parameters.
type mysqlOwnedParams struct {
	// caFiles is every value of ssl-ca, in order.
	caFiles []string
	// unsupported is the first ssl- parameter Ptah does not read.
	unsupported string
	// unreadable reports an ssl-ca value that is not valid query escaping.
	unreadable bool
}

// cutMySQLOwnedParams takes Ptah's parameters out of a raw query and returns
// the rest as written, byte for byte, for the driver.
//
// It is the one recognizer of those parameters for all three URL forms, so a
// form cannot hand the driver a parameter another form takes out.
func cutMySQLOwnedParams(rawQuery string) (string, mysqlOwnedParams) {
	var owned mysqlOwnedParams
	if rawQuery == "" {
		return "", owned
	}
	var kept []string
	for pair := range strings.SplitSeq(rawQuery, "&") {
		rawKey, rawValue, _ := strings.Cut(pair, "=")
		key, err := url.QueryUnescape(rawKey)
		if err != nil || !strings.HasPrefix(key, mysqlOwnedParamPrefix) {
			kept = append(kept, pair)
			continue
		}
		if key != mysqlCAParam {
			if owned.unsupported == "" {
				owned.unsupported = key
			}
			continue
		}
		value, err := url.QueryUnescape(rawValue)
		if err != nil {
			owned.unreadable = true
		}
		owned.caFiles = append(owned.caFiles, value)
	}
	return strings.Join(kept, "&"), owned
}

// splitMySQLDriverQuery splits go-sql-driver's own form at the "?" that starts
// its parameters.
//
// The driver finds that "?" after the last "/" of the DSN, which is the
// database separator only when no parameter value holds a "/". A CA path
// always does, so the parameters are found from the other end: after the
// network address, `tcp(...)` or `unix(...)`, and the "/" that follows it.
// It reports false when the DSN has no such shape, and the driver then says
// what is wrong with it.
func splitMySQLDriverQuery(dsn string) (head, query string, ok bool) {
	open := -1
	for _, network := range []string{"tcp(", "unix("} {
		at := -1
		switch {
		case strings.HasPrefix(dsn, network):
			at = 0
		case strings.Contains(dsn, "@"+network):
			at = strings.Index(dsn, "@"+network) + 1
		}
		if at >= 0 && (open < 0 || at < open) {
			open = at
		}
	}
	if open < 0 {
		return dsn, "", false
	}
	closing := strings.Index(dsn[open:], ")/")
	if closing < 0 {
		return dsn, "", false
	}
	databaseStart := open + closing + len(")/")
	question := strings.IndexByte(dsn[databaseStart:], '?')
	if question < 0 {
		return dsn, "", true
	}
	return dsn[:databaseStart+question], dsn[databaseStart+question+1:], true
}

// withMySQLQuery appends a raw query to a DSN or URL that may carry one.
func withMySQLQuery(base, rawQuery string) string {
	switch {
	case rawQuery == "":
		return base
	case strings.Contains(base, "?"):
		return base + "&" + rawQuery
	default:
		return base + "?" + rawQuery
	}
}

// errUnsupportedSSLParam is the reason for an ssl- parameter Ptah does not read.
var errUnsupportedSSLParam = errors.New("is not supported; ssl-ca is the only ssl- parameter Ptah reads, " +
	"and Ptah sends no client certificate, so ssl-cert and ssl-key are refused rather than ignored")

// checkCA refuses a CA bundle the rest of the URL contradicts, before any
// connection is attempted. Each refusal names parameters and never quotes the
// URL, so the password cannot reach the message.
func (u MySQLURL) checkCA(owned mysqlOwnedParams) error {
	switch {
	case owned.unsupported != "":
		return fmt.Errorf("the %s parameter %w", owned.unsupported, errUnsupportedSSLParam)
	case len(owned.caFiles) == 0:
		return nil
	case len(owned.caFiles) > 1:
		return errors.New("the ssl-ca parameter is given more than once; name one PEM file of CA certificates")
	case owned.unreadable:
		return errors.New("the ssl-ca parameter cannot be read as written; percent-encode a % in the path as %25")
	case owned.caFiles[0] == "":
		return errors.New("the ssl-ca parameter is empty; name a PEM file of CA certificates, as in ?ssl-ca=/etc/mysql/ca.pem")
	case u.config.Net != "tcp":
		return errors.New("ssl-ca verifies the server's certificate against the host the URL connects to, " +
			"and a socket address names no host; connect over TCP to use ssl-ca")
	}
	if contradiction := u.unverifiedTLS(); contradiction != "" {
		return fmt.Errorf("ssl-ca asks for TLS with a verified server certificate, and %s; remove one of the two parameters",
			contradiction)
	}
	return nil
}

// unverifiedTLS says how the URL's driver TLS settings would leave a
// connection unverified, or "" when they do not: tls=true and no tls at all
// are what ssl-ca reads as verified TLS.
func (u MySQLURL) unverifiedTLS() string {
	switch u.config.TLSConfig {
	case "false":
		return "tls=false turns TLS off"
	case "skip-verify":
		return "tls=skip-verify does not verify the certificate"
	case "preferred":
		return "tls=preferred does not verify the certificate and continues unencrypted when the server has no TLS"
	}
	if u.config.AllowFallbackToPlaintext {
		return "allowFallbackToPlaintext=true continues unencrypted when the server has no TLS"
	}
	return ""
}

// CAFile is the CA bundle the URL's ssl-ca parameter names, as written, or ""
// when it names none. A connection to a URL with one verifies the server's
// certificate and host name against that bundle alone, and refuses to continue
// without TLS.
func (u MySQLURL) CAFile() string {
	return u.caFile
}
