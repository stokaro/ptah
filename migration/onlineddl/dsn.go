package onlineddl

import (
	"fmt"
	"net/url"
	"strings"
)

// DSN carries the connection endpoints an external online-DDL tool needs.
type DSN struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
}

// ParseDatabaseURL extracts tool connection endpoints from the database URLs
// ptah accepts for the MySQL family:
//
//	mysql://user:pass@host:port/dbname
//	mysql://user:pass@tcp(host:port)/dbname
//
// (and the mariadb:// spellings of both). It mirrors how
// dbschema.ConnectToDatabase treats each form so the tool receives the same
// credentials ptah connects with: the go-sql-driver @tcp(...) form is passed
// to the driver verbatim, so its user/password are NOT percent-decoded, while
// the plain URL form is decoded by net/url. Host defaults to 127.0.0.1 and
// port to 3306 when absent; a missing database name is an error because both
// gh-ost and pt-online-schema-change require one.
func ParseDatabaseURL(dbURL string) (DSN, error) {
	var dsn DSN
	if strings.Contains(dbURL, "@tcp(") {
		dsn = parseTCPForm(dbURL)
	} else {
		var err error
		if dsn, err = parseURLForm(dbURL); err != nil {
			return DSN{}, err
		}
	}

	if dsn.Host == "" {
		dsn.Host = "127.0.0.1"
	}
	if dsn.Port == "" {
		dsn.Port = "3306"
	}
	if dsn.Database == "" {
		// The raw URL is deliberately not echoed: it may carry credentials.
		return DSN{}, fmt.Errorf("database URL carries no database name; online-DDL tools require one")
	}
	return dsn, nil
}

// parseURLForm parses the plain mysql://user:pass@host:port/db form, which
// net/url percent-decodes exactly as dbschema.convertMySQLURL does.
func parseURLForm(dbURL string) (DSN, error) {
	parsed, err := url.Parse(dbURL)
	if err != nil {
		return DSN{}, fmt.Errorf("failed to parse database URL: %w", err)
	}
	dsn := DSN{
		Host:     parsed.Hostname(),
		Port:     parsed.Port(),
		Database: strings.TrimPrefix(parsed.Path, "/"),
	}
	if parsed.User != nil {
		dsn.User = parsed.User.Username()
		dsn.Password, _ = parsed.User.Password()
	}
	return dsn, nil
}

// parseTCPForm parses mysql://user:pass@tcp(host:port)/db without
// percent-decoding, matching what ptah hands go-sql-driver for this form.
func parseTCPForm(dbURL string) DSN {
	body := dbURL
	for _, scheme := range []string{"mysql://", "mariadb://"} {
		if after, ok := strings.CutPrefix(body, scheme); ok {
			body = after
			break
		}
	}

	var dsn DSN
	if creds, rest, ok := strings.Cut(body, "@tcp("); ok {
		if user, pass, hasPass := strings.Cut(creds, ":"); hasPass {
			dsn.User, dsn.Password = user, pass
		} else {
			dsn.User = creds
		}
		body = rest
	}

	hostPort, rest, _ := strings.Cut(body, ")")
	if host, port, ok := strings.Cut(hostPort, ":"); ok {
		dsn.Host, dsn.Port = host, port
	} else {
		dsn.Host = hostPort
	}

	rest = strings.TrimPrefix(rest, "/")
	if db, _, ok := strings.Cut(rest, "?"); ok {
		dsn.Database = db
	} else {
		dsn.Database = rest
	}
	return dsn
}
