package pgextensioninstall_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/pgextensioninstall"
)

// TestRead_HappyPath pins what each statement names, with every spelling of a
// name and a version the server decodes.
func TestRead_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      pgextensioninstall.Statement
	}{
		{
			name: "bare create", statement: "CREATE EXTENSION pgcrypto",
			want: pgextensioninstall.Statement{Kind: pgextensioninstall.Create, Name: "pgcrypto"},
		},
		{
			name: "folded name, comments and a semicolon", statement: "create /* x */ extension -- y\n PgCrypto ;",
			want: pgextensioninstall.Statement{Kind: pgextensioninstall.Create, Name: "pgcrypto"},
		},
		{
			name: "every option in any order", statement: `CREATE EXTENSION IF NOT EXISTS "Earth" WITH CASCADE VERSION '1.2' SCHEMA "app"`,
			want: pgextensioninstall.Statement{Kind: pgextensioninstall.Create, Name: "Earth", Version: "1.2", IfNotExists: true, Cascade: true},
		},
		{
			name: "escape string version", statement: `CREATE EXTENSION pgcrypto VERSION E'1\x2e3'`,
			want: pgextensioninstall.Statement{Kind: pgextensioninstall.Create, Name: "pgcrypto", Version: "1.3"},
		},
		{
			name: "dollar-quoted version", statement: "CREATE EXTENSION pgcrypto VERSION $v$1.3$v$",
			want: pgextensioninstall.Statement{Kind: pgextensioninstall.Create, Name: "pgcrypto", Version: "1.3"},
		},
		{
			name: "Unicode escape name and version", statement: `CREATE EXTENSION U&"pg\0063rypto" VERSION U&'1!002e2' UESCAPE '!'`,
			want: pgextensioninstall.Statement{Kind: pgextensioninstall.Create, Name: "pgcrypto", Version: "1.2"},
		},
		{
			name: "Unicode escape name with its own escape character", statement: `CREATE EXTENSION U&"fuzzystr!006Datch" UESCAPE '!'`,
			want: pgextensioninstall.Statement{Kind: pgextensioninstall.Create, Name: "fuzzystrmatch"},
		},
		{
			name: "continued string version", statement: "CREATE EXTENSION pgcrypto VERSION '1.'\n'3'",
			want: pgextensioninstall.Statement{Kind: pgextensioninstall.Create, Name: "pgcrypto", Version: "1.3"},
		},
		{
			name: "quoted identifier version", statement: `CREATE EXTENSION pgcrypto VERSION "1.3"`,
			want: pgextensioninstall.Statement{Kind: pgextensioninstall.Create, Name: "pgcrypto", Version: "1.3"},
		},
		{
			name: "a word named if", statement: "CREATE EXTENSION if",
			want: pgextensioninstall.Statement{Kind: pgextensioninstall.Create, Name: "if"},
		},
		{
			name: "long name cut as the server stores it", statement: "CREATE EXTENSION " + strings.Repeat("a", 70),
			want: pgextensioninstall.Statement{Kind: pgextensioninstall.Create, Name: strings.Repeat("a", 63)},
		},
		{
			name: "update", statement: "ALTER EXTENSION citext UPDATE",
			want: pgextensioninstall.Statement{Kind: pgextensioninstall.Update, Name: "citext"},
		},
		{
			name: "update to an escape string", statement: "ALTER EXTENSION citext UPDATE TO E'1.6';",
			want: pgextensioninstall.Statement{Kind: pgextensioninstall.Update, Name: "citext", Version: "1.6"},
		},
		{
			name: "update of a Unicode escape name", statement: `ALTER EXTENSION U&"cit\0065xt" UPDATE TO $$1.6$$`,
			want: pgextensioninstall.Statement{Kind: pgextensioninstall.Update, Name: "citext", Version: "1.6"},
		},
		{
			name: "drop", statement: `DROP EXTENSION IF EXISTS cube, "Earth" CASCADE`,
			want: pgextensioninstall.Statement{Kind: pgextensioninstall.Drop, Dropped: []string{"cube", "Earth"}, Cascade: true},
		},
		{
			name: "drop restricted", statement: "DROP EXTENSION cube RESTRICT",
			want: pgextensioninstall.Statement{Kind: pgextensioninstall.Drop, Dropped: []string{"cube"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok, err := pgextensioninstall.Read(test.statement)
			c.Assert(err, qt.IsNil)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestRead_FailurePath pins the extension statements the reader recognizes
// and cannot read: each is reported with its kind alone and the reason, so a
// caller can never take it for a statement that installs nothing.
func TestRead_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		kind      pgextensioninstall.Kind
		err       string
	}{
		{name: "number version", statement: "CREATE EXTENSION pgcrypto VERSION 1.3", kind: pgextensioninstall.Create, err: `its version 1.3 is neither a name nor a string`},
		{name: "bit string version", statement: "CREATE EXTENSION pgcrypto VERSION B'1'", kind: pgextensioninstall.Create, err: `its version B'1' is not a string constant`},
		{name: "national string version", statement: "CREATE EXTENSION pgcrypto VERSION N'1.3'", kind: pgextensioninstall.Create, err: `its version N'1.3' is not a string constant`},
		{name: "bad escape", statement: `CREATE EXTENSION pgcrypto VERSION U&'\zzzz'`, kind: pgextensioninstall.Create, err: `its version U&'\\zzzz' is not a string the server reads`},
		{name: "string name", statement: "CREATE EXTENSION 'pgcrypto'", kind: pgextensioninstall.Create, err: `its extension name 'pgcrypto' is not a name`},
		{name: "no name", statement: "CREATE EXTENSION", kind: pgextensioninstall.Create, err: `it names no extension name`},
		{name: "empty quoted name", statement: `CREATE EXTENSION ""`, kind: pgextensioninstall.Create, err: `its extension name "" is not an identifier the server reads`},
		{name: "FROM", statement: "CREATE EXTENSION pgcrypto FROM unpackaged", kind: pgextensioninstall.Create, err: `"FROM" is not an option CREATE EXTENSION takes`},
		{name: "VERSION twice", statement: "CREATE EXTENSION pgcrypto VERSION '1.2' VERSION '1.3'", kind: pgextensioninstall.Create, err: `it names VERSION twice`},
		{name: "CASCADE twice", statement: "CREATE EXTENSION pgcrypto CASCADE CASCADE", kind: pgextensioninstall.Create, err: `it names CASCADE twice`},
		{name: "a second statement", statement: "CREATE EXTENSION pgcrypto; LOAD 'x'", kind: pgextensioninstall.Create, err: `text follows its semicolon`},
		{name: "unterminated", statement: "CREATE EXTENSION pgcrypto VERSION '1.3", kind: pgextensioninstall.Create, err: `its text does not close where the server reads it \(.*\)`},
		{name: "zero byte", statement: "CREATE EXTENSION pgcrypto -- \x00", kind: pgextensioninstall.Create, err: `it holds a zero byte`},
		{name: "update cascade", statement: "ALTER EXTENSION citext UPDATE TO '1.6' CASCADE", kind: pgextensioninstall.Update, err: `"CASCADE" follows the version, and ALTER EXTENSION ... UPDATE takes nothing there`},
		{name: "update to twice", statement: "ALTER EXTENSION citext UPDATE TO '1.5' TO '1.6'", kind: pgextensioninstall.Update, err: `"TO" follows the version, .*`},
		{name: "update to a number", statement: "ALTER EXTENSION citext UPDATE TO 1.6", kind: pgextensioninstall.Update, err: `its version 1.6 is neither a name nor a string`},
		{name: "update of a bad escape name", statement: `ALTER EXTENSION U&"\zz" UPDATE`, kind: pgextensioninstall.Update, err: `its extension name U&"\\zz" is not an identifier the server reads`},
		{name: "update of a bad escape name with its clause", statement: `ALTER EXTENSION U&"!zz" UESCAPE '!' UPDATE`, kind: pgextensioninstall.Update, err: `its extension name U&"!zz" is not an identifier the server reads`},
		{name: "update after a zero byte", statement: "ALTER EXTENSION citext UPDATE -- \x00\n; LOAD 'x'", kind: pgextensioninstall.Update, err: `it holds a zero byte`},
		{name: "drop with a trailing word", statement: "DROP EXTENSION cube CASCADE RESTRICT", kind: pgextensioninstall.Drop, err: `"RESTRICT" follows the names, .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok, err := pgextensioninstall.Read(test.statement)
			c.Assert(err, qt.ErrorMatches, test.err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.DeepEquals, pgextensioninstall.Statement{Kind: test.kind})
		})
	}
}

// TestRead_OtherStatements pins the statements that are not extension
// installs: every other ALTER EXTENSION form among them, so the replay guard
// never reads one as an update.
func TestRead_OtherStatements(t *testing.T) {
	tests := []string{
		"ALTER EXTENSION citext SET SCHEMA app",
		"ALTER EXTENSION citext ADD FUNCTION update(int)",
		"ALTER EXTENSION citext DROP TABLE t",
		"COMMENT ON EXTENSION citext IS 'x'",
		"CREATE TABLE extension (id int)",
		"SELECT 'CREATE EXTENSION pgcrypto'",
		"DO $$ BEGIN ALTER EXTENSION citext UPDATE; END $$",
		"ALTER EXTENSION",
		"",
	}
	for _, statement := range tests {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			got, ok, err := pgextensioninstall.Read(statement)
			c.Assert(err, qt.IsNil)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.DeepEquals, pgextensioninstall.Statement{})
		})
	}
}
