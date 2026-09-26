package dbschema_test

import (
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// TestConnectToDatabase_ReachesTheMySQLDriverForEveryMariaDBSpelling opens
// each MariaDB spelling of a socket address nothing listens on.
//
// The socket needs no server and no network. An address the connector read
// fails at dial, naming the socket path. An address it did not read fails
// earlier, as an invalid URL or an unsupported dialect: net/url refuses the
// unix() form, so the connector has to know the scheme is a MySQL-family one
// before it parses anything, and has to know it again to hand the driver the
// address without its scheme. The pinned community binary v1.3.0 opens
// `maria://` on every verb that takes a database URL (stokaro/ptah#3744).
func TestConnectToDatabase_ReachesTheMySQLDriverForEveryMariaDBSpelling(t *testing.T) {
	tests := []struct {
		name   string
		scheme string
	}{
		{name: "maria", scheme: "maria"},
		{name: "maria in upper case", scheme: "MARIA"},
		{name: "mariadb", scheme: "mariadb"},
		{name: "mariadb in upper case", scheme: "MARIADB"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			socket := filepath.Join(c.TempDir(), "absent.sock")

			conn, err := dbschema.ConnectToDatabase(t.Context(), test.scheme+"://app:secret@unix("+socket+")/shop")

			c.Assert(err, qt.ErrorMatches, `(?s).*dial unix `+regexp.QuoteMeta(socket)+`.*`)
			c.Assert(conn, qt.IsNil)
		})
	}
}

// TestConnectToDatabase_RefusesAMariaDBSpellingTheCommunityBinaryRefuses keeps
// the spelling a scheme of its own rather than a prefix: the pinned community
// binary v1.3.0 answers `unknown driver "maria+tcp"`.
func TestConnectToDatabase_RefusesAMariaDBSpellingTheCommunityBinaryRefuses(t *testing.T) {
	c := qt.New(t)

	conn, err := dbschema.ConnectToDatabase(t.Context(), "maria+tcp://app:secret@localhost:3306/shop")

	c.Assert(err, qt.ErrorMatches, `unsupported database dialect: maria\+tcp`)
	c.Assert(conn, qt.IsNil)
}
