//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package dbschema_test

import (
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// TestConnectToDatabase_DialsTheSocketOfASocketURL opens each socket spelling
// the pinned community binary v1.3.0 accepts, pointed at a socket nothing
// listens on.
//
// An address the connector read fails at dial, naming the socket. One it did
// not read fails earlier: as an unsupported dialect when the scheme is not
// known, or at a dial of some other address when the path is taken for a
// database and the socket for nothing (stokaro/ptah#3755).
//
// The file is Unix-only because the socket path is written into a URL path: a
// Windows temporary directory starts with a drive letter, which is a different
// question from the one asked here.
func TestConnectToDatabase_DialsTheSocketOfASocketURL(t *testing.T) {
	tests := []struct {
		name   string
		scheme string
	}{
		{name: "mysql", scheme: "mysql+unix"},
		{name: "mariadb", scheme: "mariadb+unix"},
		{name: "maria", scheme: "maria+unix"},
		{name: "upper case", scheme: "MARIA+UNIX"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			socket := filepath.Join(c.TempDir(), "absent.sock")

			conn, err := dbschema.ConnectToDatabase(t.Context(), test.scheme+"://app:secret@"+socket+"?database=shop")

			c.Assert(err, qt.ErrorMatches, `(?s).*dial unix `+regexp.QuoteMeta(socket)+`.*`)
			c.Assert(conn, qt.IsNil)
		})
	}
}

// TestConnectToDatabase_RefusesAnEmptyDatabaseParameter refuses the socket URL
// the pinned community binary hands to the server as an empty database, where
// the server answers with a syntax error. The refusal comes before any dial,
// so the socket need not exist.
func TestConnectToDatabase_RefusesAnEmptyDatabaseParameter(t *testing.T) {
	c := qt.New(t)
	socket := filepath.Join(c.TempDir(), "absent.sock")

	conn, err := dbschema.ConnectToDatabase(t.Context(), "mysql+unix://app:secret@"+socket+"?database=")

	c.Assert(err, qt.ErrorMatches, `invalid database URL: invalid mysql\+unix URL: the database parameter is empty; name a database, or leave the parameter out`)
	c.Assert(conn, qt.IsNil)
}
