package mysql

// White-box testing required: how a read writes a literal default back depends
// on the session's sql_mode, and no exported API reports which form a session
// selected; the replay end of it is covered by an engine test, and a read
// that refuses a backslash shows only the refusal.

import (
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
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

// The session read takes the sql_mode the server answers for the connection,
// so a session in NO_BACKSLASH_ESCAPES selects the verbatim form.
func TestReadSessionAccount_ReadsTheSQLMode(t *testing.T) {
	tests := []struct {
		name, version, sqlMode string
		want                   defaultLiterals
	}{
		{name: "MySQL's default mode", version: dbtest.MySQLVersion, sqlMode: dbtest.MySQLSQLMode, want: literalsUnquoted},
		{name: "MySQL in NO_BACKSLASH_ESCAPES", version: dbtest.MySQLVersion, sqlMode: "STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES", want: literalsUnquotedVerbatim},
		{name: "MariaDB in NO_BACKSLASH_ESCAPES", version: dbtest.MariaDBVersion, sqlMode: "NO_BACKSLASH_ESCAPES", want: literalsQuoted},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			unexpected := func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
				c.Fatalf("unexpected query: %s", query)
				return dbtest.QueryResult{}, nil
			}
			db := dbtest.Open(c, dbtest.MySQLSessionInMode(test.version, test.sqlMode, dbtest.MySQLAllGrants, unexpected))

			account, err := NewMySQLReader(db.SQL, "app").readSessionAccount(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(account.defaultLiterals(), qt.Equals, test.want)
		})
	}
}

// A column whose literal default holds a backslash, read in a session that
// reads the backslash as the character, fails the read and names the column.
func TestReadColumns_RefusesABackslashWithoutBackslashEscapes(t *testing.T) {
	c := qt.New(t)
	row := []driver.Value{
		"customers", "path", "varchar", "varchar(30)", "YES",
		`c:\tmp`, int64(30), nil, nil, int64(1),
		"utf8mb4", "utf8mb4_general_ci", "", nil, "",
	}
	reader := NewMySQLReader(columnsDB(c, [][]driver.Value{row}).SQL, "app")

	columnsByTable, err := reader.readColumnsByTable(t.Context(), "app", literalsUnquotedVerbatim)

	c.Assert(err, qt.ErrorMatches, `column customers.path: the default "c:\\\\tmp" holds a backslash, and the session's sql_mode names NO_BACKSLASH_ESCAPES.*`)
	c.Assert(columnsByTable, qt.IsNil)
}
