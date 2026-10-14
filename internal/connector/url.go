package connector

import (
	"errors"
	"net/url"
	"strings"

	"ptah.run/internal/atlasurl"
)

// ParseURL parses a database URL into the form a connection reads: the scheme
// names the dialect, and the path is where the database is read from.
//
// A MySQL-family URL is read by atlasurl.ParseMySQLURL, and only the scheme and
// the database it found are carried on. Its forms put the database in
// different places. The go-sql-driver form, user:pass@tcp(127.0.0.1:3306)/db or
// user:pass@unix(/tmp/mysql.sock)/db, is not a URL at all. In the socket form,
// mysql+unix://user:pass@/run/mysqld/mysqld.sock?database=db, the path is the
// socket. A reader of its own here would report a database called after the
// socket while the driver connects to db.
//
// A Windows absolute path is not an authority either. sqlite://C:\dir\app.db
// makes net/url read the drive letter's colon as a port separator and refuse
// the whole URL, which is why every command that provisions a local database
// failed on Windows: 1014 tests across more than thirty packages, 1382 of them
// reporting "invalid database URL". The path is carried as opaque instead,
// which is the shape the SQLite connector reads first.
func ParseURL(dbURL string) (*url.URL, error) {
	mysqlURL, err := atlasurl.ParseMySQLURL(dbURL)
	switch {
	case err == nil:
		return &url.URL{Scheme: strings.ToLower(mysqlURL.Scheme()), Path: "/" + mysqlURL.Database()}, nil
	case !errors.Is(err, atlasurl.ErrNotMySQLURL):
		return nil, err
	}

	// The Windows rule lives in atlasurl, which is where the other database-URL
	// parser already knew it.
	return atlasurl.Parse(dbURL)
}
