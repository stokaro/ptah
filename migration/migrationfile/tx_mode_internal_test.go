package migrationfile

// White-box testing required: transaction-mode source precedence, conservative
// loading, and target-dialect deferral are internal decisions whose exact
// source classification is not exposed through the public execution API.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func Test_parseAtlasFileTxMode_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want FileTxMode
	}{
		{
			name: "file",
			sql:  "-- atlas:txmode file\n\nCREATE TABLE users (id BIGINT);",
			want: FileTxModeFile,
		},
		{
			name: "none",
			sql:  "-- atlas:txmode none\n\nCREATE INDEX users_id_idx ON users (id);",
			want: FileTxModeNone,
		},
		{
			name: "ordinary comment text before key",
			sql:  "-- generated migration\n-- metadata: atlas:txmode none\n\nSELECT 1;",
			want: FileTxModeNone,
		},
		{
			name: "captured value is trimmed",
			sql:  "-- metadata atlas:txmode   file \t\n-- generated migration\n\nSELECT 1;",
			want: FileTxModeFile,
		},
		{
			name: "whitespace-only header separator",
			sql:  "-- atlas:txmode none\n \t\r\nSELECT 1;",
			want: FileTxModeNone,
		},
		{
			// The header ends at the first statement, not only at a blank line.
			// Requiring the blank line drops a directive the author wrote
			// directly above the statement it applies to, which is where an
			// author is most likely to write it.
			name: "statement immediately after the directive",
			sql:  "-- atlas:txmode none\nCREATE INDEX CONCURRENTLY i ON t (c);",
			want: FileTxModeNone,
		},
		{
			name: "statement immediately after a multi-line header",
			sql:  "-- generated migration\n-- atlas:txmode none\nSELECT 1;",
			want: FileTxModeNone,
		},
		{
			// Same rule from the other side: the directive was already read
			// when the indented line ended the header, so the indentation of a
			// LATER line cannot un-write it.
			name: "indented line after the directive",
			sql:  "-- atlas:txmode none\n  -- generated migration\n\nSELECT 1;",
			want: FileTxModeNone,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			mode, found, err := parseAtlasFileTxMode("001_create_users.sql", test.sql)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(mode, qt.Equals, test.want)
		})
	}
}

func Test_parseAtlasFileTxMode_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "multiple occurrences preserve order",
			sql:     "-- atlas:txmode none\n-- atlas:txmode file\n\nSELECT 1;",
			wantErr: `multiple txmode values found in file "001_create_users.sql": ["none" "file"]`,
		},
		{
			name:    "all",
			sql:     "-- atlas:txmode all\n\nSELECT 1;",
			wantErr: `txmode "all" is not allowed in file directive "001_create_users.sql". Use "file" instead`,
		},
		{
			name:    "unknown",
			sql:     "-- atlas:txmode statement\n\nSELECT 1;",
			wantErr: `unknown txmode "statement" found in file directive "001_create_users.sql"`,
		},
		{
			name:    "uppercase value",
			sql:     "-- atlas:txmode FILE\n\nSELECT 1;",
			wantErr: `unknown txmode "FILE" found in file directive "001_create_users.sql"`,
		},
		{
			name:    "extra tokens",
			sql:     "-- atlas:txmode file extra\n\nSELECT 1;",
			wantErr: `unknown txmode "file extra" found in file directive "001_create_users.sql"`,
		},
		{
			name:    "missing value",
			sql:     "-- atlas:txmode\n\nSELECT 1;",
			wantErr: `unknown txmode "" found in file directive "001_create_users.sql"`,
		},
		{
			name:    "tab after key",
			sql:     "-- atlas:txmode\tnone\n\nSELECT 1;",
			wantErr: `unknown txmode "" found in file directive "001_create_users.sql"`,
		},
		{
			name:    "space without value",
			sql:     "-- atlas:txmode \n\nSELECT 1;",
			wantErr: `unknown txmode "" found in file directive "001_create_users.sql"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			mode, found, err := parseAtlasFileTxMode("001_create_users.sql", test.sql)
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, test.wantErr)
			c.Assert(found, qt.IsTrue)
			c.Assert(mode, qt.Equals, FileTxModeUnspecified)
		})
	}
}

func Test_parseAtlasFileTxMode_Ignored(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "empty file",
			sql:  "",
		},
		{
			name: "leading blank line",
			sql:  "\n-- atlas:txmode none\n\nSELECT 1;",
		},
		{
			name: "leading whitespace",
			sql:  "  -- atlas:txmode none\n\nSELECT 1;",
		},
		{
			name: "missing blank separator",
			sql:  "-- atlas:txmode none\n",
		},
		{
			name: "occurrence after SQL",
			sql:  "SELECT 1;\n-- atlas:txmode none\n\n",
		},
		{
			name: "occurrence in later comment block",
			sql:  "-- generated migration\n\n-- atlas:txmode none\n\nSELECT 1;",
		},
		{
			name: "block comment",
			sql:  "/* atlas:txmode none */\n\nSELECT 1;",
		},
		{
			name: "uppercase key",
			sql:  "-- ATLAS:TXMODE none\n\nSELECT 1;",
		},
		{
			name: "leading header without occurrence",
			sql:  "-- generated migration\n-- no transaction mode\n\nSELECT 1;",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			mode, found, err := parseAtlasFileTxMode("001_create_users.sql", test.sql)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsFalse)
			c.Assert(mode, qt.Equals, FileTxModeUnspecified)
		})
	}
}

func Test_classifyAtlasTxtarDirective_HappyPath(t *testing.T) {
	c := qt.New(t)

	isTxtar, misplaced := classifyAtlasTxtarDirective("\n-- atlas:txtar\n\n-- migration.sql --\nSELECT 1;\n")
	c.Assert(isTxtar, qt.IsTrue)
	c.Assert(misplaced, qt.IsFalse)

	isTxtar, misplaced = classifyAtlasTxtarDirective("SELECT 1;\n-- atlas:txtar\n-- ordinary comment\n")
	c.Assert(isTxtar, qt.IsFalse)
	c.Assert(misplaced, qt.IsFalse)
}

func TestParseUp_TxtarSourceLineOffset(t *testing.T) {
	c := qt.New(t)

	parsed, err := ParseUp("1_drop.sql", `-- atlas:txtar

-- migration.sql --
DROP TABLE users;
`)

	c.Assert(err, qt.IsNil)
	c.Assert(parsed.SQL, qt.Equals, "DROP TABLE users;\n")
	c.Assert(parsed.TxMode, qt.Equals, FileTxModeUnspecified)
	c.Assert(parsed.SourceLineOffset, qt.Equals, 3)
}

func Test_classifyAtlasTxtarDirective_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "directive before section",
			sql:  "-- atlas:txmode none\n\n-- atlas:txtar\n\n-- migration.sql --\nSELECT 1;\n",
		},
		{
			name: "section before directive",
			sql:  "-- migration.sql --\n-- atlas:txtar\nSELECT 1;\n-- down.sql --\nDROP TABLE users;\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			isTxtar, misplaced := classifyAtlasTxtarDirective(test.sql)
			c.Assert(isTxtar, qt.IsFalse)
			c.Assert(misplaced, qt.IsTrue)
		})
	}
}

func TestParseFileTxMode_CoexistenceHappyPath(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantMode   FileTxMode
		wantSource FileTxModeSource
	}{
		{
			name:       "native true overrides Atlas file",
			sql:        "-- atlas:txmode file\n-- +ptah no_transaction\n\nSELECT 1;\n",
			wantMode:   FileTxModeNone,
			wantSource: FileTxModeSourcePtah,
		},
		{
			name:       "native false leaves Atlas none",
			sql:        "-- atlas:txmode none\n-- +ptah no_transaction=false\n\nSELECT 1;\n",
			wantMode:   FileTxModeNone,
			wantSource: FileTxModeSourceAtlas,
		},
		{
			name:       "native false leaves Atlas file",
			sql:        "-- atlas:txmode file\n-- +ptah no_transaction=false\n\nSELECT 1;\n",
			wantMode:   FileTxModeFile,
			wantSource: FileTxModeSourceAtlas,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := ParseFileTxMode("1_coexist.sql", test.sql)
			c.Assert(got.Err, qt.IsNil)
			c.Assert(got.Mode, qt.Equals, test.wantMode)
			c.Assert(got.Source, qt.Equals, test.wantSource)
		})
	}
}

func TestParseFileTxMode_CoexistenceFailurePath(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantErr    string
		wantSource FileTxModeSource
	}{
		{
			name:       "invalid Atlas mode is not hidden by native true",
			sql:        "-- atlas:txmode bogus\n-- +ptah no_transaction\n\nSELECT 1;\n",
			wantErr:    `unknown txmode "bogus" found in file directive "1_coexist.sql"`,
			wantSource: FileTxModeSourceAtlas,
		},
		{
			name:       "invalid native value is not hidden by Atlas none",
			sql:        "-- atlas:txmode none\n-- +ptah no_transaction=maybe\n\nSELECT 1;\n",
			wantErr:    `invalid \+ptah no_transaction value "maybe": expected true or false`,
			wantSource: FileTxModeSourcePtah,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := ParseFileTxMode("1_coexist.sql", test.sql)
			c.Assert(got.Err, qt.ErrorMatches, test.wantErr)
			c.Assert(got.Mode, qt.Equals, FileTxModeUnspecified)
			c.Assert(got.Source, qt.Equals, test.wantSource)
		})
	}
}
