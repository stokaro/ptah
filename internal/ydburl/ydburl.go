// Package ydburl reads the endpoint and the database a ydb:// or ydbs:// URL
// names.
//
// Two callers need the same reading. The connection layer opens the database
// the URL names, and internal/atlasurl decides whether two URLs name the same
// database, which is what keeps a dev database from being the target. Read
// twice, the two could disagree about which of `ydb://h/a?database=/b` they
// mean, and a guard would then compare one database while the command opened
// another.
//
// It links no YDB SDK, because internal/atlasurl sits on paths that render SQL
// and must not pull a database driver.
package ydburl

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// The URL schemes and the gRPC port each one defaults to: a YDB server listens
// for plaintext on 2136 and for TLS on 2135.
const (
	PlaintextScheme = "ydb"
	TLSScheme       = "ydbs"

	PlaintextPort = "2136"
	TLSPort       = "2135"
)

// DatabaseParameter is the query parameter that names the database where the
// path does not.
const DatabaseParameter = "database"

// URL is a parsed YDB database URL.
type URL struct {
	// Secure reports TLS: the ydbs scheme.
	Secure bool
	// Host is the host name or address, without brackets.
	Host string
	// Port is the port the URL names, or the scheme's default port.
	Port string
	// Database is the database path with one leading slash and no trailing
	// one, such as /local; empty when the URL names none.
	Database string
	// User is the URL's user information, nil when it carries none.
	User *url.Userinfo
	// Query is the query string without the database parameter.
	Query url.Values
}

// ErrNotYDB reports a URL whose scheme is neither ydb nor ydbs.
var ErrNotYDB = errors.New("not a ydb:// or ydbs:// URL")

// Parse reads raw as a YDB URL.
func Parse(raw string) (URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return URL{}, err
	}
	return FromURL(parsed)
}

// FromURL reads an already parsed URL. The database comes from the path or
// from the database parameter. A URL naming it in both places is refused when
// the two differ, because the YDB SDK silently takes the parameter, and a
// reader of the path would then describe a database the command does not
// open. Spellings that differ only in their slashes (`local`, `/local`,
// `/local/`) name one database.
func FromURL(parsed *url.URL) (URL, error) {
	var secure bool
	switch strings.ToLower(parsed.Scheme) {
	case PlaintextScheme:
	case TLSScheme:
		secure = true
	default:
		return URL{}, fmt.Errorf("%w: %q", ErrNotYDB, parsed.Scheme)
	}
	// An opaque URL (ydb:host/database) has no host either, so this refuses
	// it too.
	if parsed.Hostname() == "" {
		return URL{}, errors.New("a YDB URL needs a host: write ydb://host:2136/database")
	}

	port := parsed.Port()
	if port == "" {
		port = PlaintextPort
		if secure {
			port = TLSPort
		}
	}

	query := parsed.Query()
	database := foldDatabase(parsed.Path)
	if values, named := query[DatabaseParameter]; named {
		if len(values) != 1 {
			return URL{}, errors.New("the database parameter is given more than once")
		}
		parameter := foldDatabase(values[0])
		if parameter == "" {
			return URL{}, errors.New("the database parameter is empty")
		}
		if database != "" && database != parameter {
			return URL{}, fmt.Errorf("the URL names database %s in its path and %s in the database parameter; name it once",
				database, parameter)
		}
		database = parameter
		delete(query, DatabaseParameter)
	}

	return URL{
		Secure:   secure,
		Host:     parsed.Hostname(),
		Port:     port,
		Database: database,
		User:     parsed.User,
		Query:    query,
	}, nil
}

// Endpoint is host:port, with an IPv6 address in brackets.
func (u URL) Endpoint() string {
	return net.JoinHostPort(u.Host, u.Port)
}

// foldDatabase spells a database path with one leading slash and no trailing
// one, and the empty path as "".
func foldDatabase(path string) string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return ""
	}
	return "/" + trimmed
}
