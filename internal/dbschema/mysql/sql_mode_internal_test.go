package mysql

// White-box testing required: how a read writes a literal default back depends
// on the session's sql_mode, and no exported API reports which form a session
// selected; the replay end of it is covered by an engine test.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// A literal default is written back with its backslashes escaped unless the
// session reads a backslash as itself.
func TestSessionAccount_DefaultLiteralsFollowTheSQLMode(t *testing.T) {
	tests := []struct {
		name    string
		mariaDB bool
		sqlMode string
		want    defaultLiterals
	}{
		{name: "MySQL's default mode", sqlMode: "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES", want: literalsUnquoted},
		{name: "NO_BACKSLASH_ESCAPES", sqlMode: "STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES", want: literalsUnquotedVerbatim},
		{name: "NO_BACKSLASH_ESCAPES alone in lower case", sqlMode: "no_backslash_escapes", want: literalsUnquotedVerbatim},
		{name: "a mode that only contains the name", sqlMode: "NO_BACKSLASH_ESCAPES_X", want: literalsUnquoted},
		{name: "MariaDB, which writes the literal as SQL", mariaDB: true, sqlMode: "NO_BACKSLASH_ESCAPES", want: literalsQuoted},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			account := sessionAccount{mariaDB: test.mariaDB, noBackslashEscapes: namesNoBackslashEscapes(test.sqlMode)}

			c.Assert(account.defaultLiterals(), qt.Equals, test.want)
		})
	}
}
