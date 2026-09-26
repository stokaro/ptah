package onlineddl

import (
	"errors"
	"fmt"

	"ptah.run/internal/atlasurl"
)

// DSN carries the connection endpoints an external online-DDL tool needs.
type DSN struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
}

// ParseDatabaseURL extracts tool connection endpoints from a MySQL-family
// database URL.
//
// The URL is read by atlasurl.ParseMySQLURL, which is also what
// dbschema.ConnectToDatabase reads it with, so the tool receives the
// credentials, the server and the database Ptah connects to, in every form the
// URL takes. The go-sql-driver form, user:pass@tcp(host:port)/dbname, is handed
// to the driver as written, so its credentials are not percent-decoded; the
// URL form is decoded by net/url. Host defaults to 127.0.0.1 and port to 3306,
// as the driver's do.
//
// A socket address is refused: gh-ost and pt-online-schema-change are given a
// host and a port here, and a socket path handed to either as a host would
// reach some other server or none. A missing database name is refused because
// both tools require one.
func ParseDatabaseURL(dbURL string) (DSN, error) {
	parsed, err := atlasurl.ParseMySQLURL(dbURL)
	if err != nil {
		return DSN{}, fmt.Errorf("failed to parse database URL: %w", err)
	}
	host, port, tcp := parsed.HostPort()
	if !tcp {
		return DSN{}, errors.New("database URL reaches the server through a Unix socket; online-DDL tools are given a TCP host and port")
	}
	if parsed.Database() == "" {
		// The raw URL is deliberately not echoed: it may carry credentials.
		return DSN{}, errors.New("database URL carries no database name; online-DDL tools require one")
	}
	return DSN{
		Host:     host,
		Port:     port,
		User:     parsed.User(),
		Password: parsed.Password(),
		Database: parsed.Database(),
	}, nil
}
