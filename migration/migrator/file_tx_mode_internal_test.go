package migrator

// White-box testing required: transaction-mode source precedence, conservative
// loading, and target-dialect deferral are internal decisions whose exact
// source classification is not exposed through the public execution API.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
)

func Test_parseAtlasFileTxMode_HappyPath(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name string
		sql  string
		want MigrationFileTxMode
	}{
		{
			name: "file",
			sql:  "-- atlas:txmode file\n\nCREATE TABLE users (id BIGINT);",
			want: MigrationFileTxModeFile,
		},
		{
			name: "none",
			sql:  "-- atlas:txmode none\n\nCREATE INDEX users_id_idx ON users (id);",
			want: MigrationFileTxModeNone,
		},
		{
			name: "ordinary comment text before key",
			sql:  "-- generated migration\n-- metadata: atlas:txmode none\n\nSELECT 1;",
			want: MigrationFileTxModeNone,
		},
		{
			name: "captured value is trimmed",
			sql:  "-- metadata atlas:txmode   file \t\n-- generated migration\n\nSELECT 1;",
			want: MigrationFileTxModeFile,
		},
		{
			name: "whitespace-only header separator",
			sql:  "-- atlas:txmode none\n \t\r\nSELECT 1;",
			want: MigrationFileTxModeNone,
		},
		{
			// The header ends at the first statement, not only at a blank line.
			// Requiring the blank line drops a directive the author wrote
			// directly above the statement it applies to, which is where an
			// author is most likely to write it.
			name: "statement immediately after the directive",
			sql:  "-- atlas:txmode none\nCREATE INDEX CONCURRENTLY i ON t (c);",
			want: MigrationFileTxModeNone,
		},
		{
			name: "statement immediately after a multi-line header",
			sql:  "-- generated migration\n-- atlas:txmode none\nSELECT 1;",
			want: MigrationFileTxModeNone,
		},
		{
			// Same rule from the other side: the directive was already read
			// when the indented line ended the header, so the indentation of a
			// LATER line cannot un-write it.
			name: "indented line after the directive",
			sql:  "-- atlas:txmode none\n  -- generated migration\n\nSELECT 1;",
			want: MigrationFileTxModeNone,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			mode, found, err := parseAtlasFileTxMode("001_create_users.sql", test.sql)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(mode, qt.Equals, test.want)
		})
	}
}

func Test_parseAtlasFileTxMode_FailurePath(t *testing.T) {
	c := qt.New(t)
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
		c.Run(test.name, func(c *qt.C) {
			mode, found, err := parseAtlasFileTxMode("001_create_users.sql", test.sql)
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, test.wantErr)
			c.Assert(found, qt.IsTrue)
			c.Assert(mode, qt.Equals, MigrationFileTxModeUnspecified)
		})
	}
}

func Test_parseAtlasFileTxMode_Ignored(t *testing.T) {
	c := qt.New(t)
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
		c.Run(test.name, func(c *qt.C) {
			mode, found, err := parseAtlasFileTxMode("001_create_users.sql", test.sql)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsFalse)
			c.Assert(mode, qt.Equals, MigrationFileTxModeUnspecified)
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

func TestParseMigrationUp_TxtarSourceLineOffset(t *testing.T) {
	c := qt.New(t)

	parsed, err := ParseMigrationUp("1_drop.sql", `-- atlas:txtar

-- migration.sql --
DROP TABLE users;
`)

	c.Assert(err, qt.IsNil)
	c.Assert(parsed.SQL, qt.Equals, "DROP TABLE users;\n")
	c.Assert(parsed.TxMode, qt.Equals, MigrationFileTxModeUnspecified)
	c.Assert(parsed.SourceLineOffset, qt.Equals, 3)
}

func Test_classifyAtlasTxtarDirective_FailurePath(t *testing.T) {
	c := qt.New(t)
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
		c.Run(test.name, func(c *qt.C) {
			isTxtar, misplaced := classifyAtlasTxtarDirective(test.sql)
			c.Assert(isTxtar, qt.IsFalse)
			c.Assert(misplaced, qt.IsTrue)
		})
	}
}

func Test_parseMigrationFileTxMode_CoexistenceHappyPath(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name       string
		sql        string
		wantMode   MigrationFileTxMode
		wantSource migrationFileTxModeSource
	}{
		{
			name:       "native true overrides Atlas file",
			sql:        "-- atlas:txmode file\n-- +ptah no_transaction\n\nSELECT 1;\n",
			wantMode:   MigrationFileTxModeNone,
			wantSource: migrationFileTxModeSourcePtah,
		},
		{
			name:       "native false leaves Atlas none",
			sql:        "-- atlas:txmode none\n-- +ptah no_transaction=false\n\nSELECT 1;\n",
			wantMode:   MigrationFileTxModeNone,
			wantSource: migrationFileTxModeSourceAtlas,
		},
		{
			name:       "native false leaves Atlas file",
			sql:        "-- atlas:txmode file\n-- +ptah no_transaction=false\n\nSELECT 1;\n",
			wantMode:   MigrationFileTxModeFile,
			wantSource: migrationFileTxModeSourceAtlas,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got := parseMigrationFileTxMode("1_coexist.sql", test.sql)
			c.Assert(got.err, qt.IsNil)
			c.Assert(got.mode, qt.Equals, test.wantMode)
			c.Assert(got.source, qt.Equals, test.wantSource)
		})
	}
}

func Test_parseMigrationFileTxMode_CoexistenceFailurePath(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name       string
		sql        string
		wantErr    string
		wantSource migrationFileTxModeSource
	}{
		{
			name:       "invalid Atlas mode is not hidden by native true",
			sql:        "-- atlas:txmode bogus\n-- +ptah no_transaction\n\nSELECT 1;\n",
			wantErr:    `unknown txmode "bogus" found in file directive "1_coexist.sql"`,
			wantSource: migrationFileTxModeSourceAtlas,
		},
		{
			name:       "invalid native value is not hidden by Atlas none",
			sql:        "-- atlas:txmode none\n-- +ptah no_transaction=maybe\n\nSELECT 1;\n",
			wantErr:    `invalid \+ptah no_transaction value "maybe": expected true or false`,
			wantSource: migrationFileTxModeSourcePtah,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got := parseMigrationFileTxMode("1_coexist.sql", test.sql)
			c.Assert(got.err, qt.ErrorMatches, test.wantErr)
			c.Assert(got.mode, qt.Equals, MigrationFileTxModeUnspecified)
			c.Assert(got.source, qt.Equals, test.wantSource)
		})
	}
}

func TestMigrationTxModeParsingDefersDialectSpecificStringsToTarget(t *testing.T) {
	c := qt.New(t)
	invalidForPostgres := "SELECT 'prefix \\'\n-- +ptah no_transaction=maybe\nsuffix';\n"

	loaded, err := migrationFuncFromSQLStringWithMetadata("1_ambiguous.sql", invalidForPostgres, statementExecutionHooks{})
	c.Assert(err, qt.IsNil)
	c.Assert(loaded.txMode, qt.Equals, MigrationFileTxModeUnspecified)
	c.Assert(loaded.txModeErr, qt.IsNil)

	migration := &Migration{Description: "ambiguous"}
	setMigrationUp(migration, loaded)
	mysqlMode := migration.parsedUpTxModeForDialect(platform.MySQL)
	c.Assert(mysqlMode.err, qt.IsNil)
	c.Assert(mysqlMode.mode, qt.Equals, MigrationFileTxModeUnspecified)
	postgresMode := migration.parsedUpTxModeForDialect(platform.Postgres)
	c.Assert(postgresMode.err, qt.ErrorMatches, `invalid \+ptah no_transaction value "maybe": expected true or false`)

	parsed, err := ParseMigrationUp("1_ambiguous.sql", invalidForPostgres)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.TxMode, qt.Equals, MigrationFileTxModeUnspecified)
}

func TestMigrationTxModeParsingUsesTargetDialectForActualMarker(t *testing.T) {
	c := qt.New(t)
	markerForPostgres := "SELECT 'prefix \\'\n-- +ptah no_transaction\nsuffix';\n"
	migration := CreateMigrationFromSQL(1, "target-aware", markerForPostgres, markerForPostgres)

	mysqlMode := migration.parsedUpTxModeForDialect(platform.MySQL)
	c.Assert(mysqlMode.err, qt.IsNil)
	c.Assert(mysqlMode.mode, qt.Equals, MigrationFileTxModeUnspecified)
	postgresMode := migration.parsedUpTxModeForDialect(platform.Postgres)
	c.Assert(postgresMode.err, qt.IsNil)
	c.Assert(postgresMode.mode, qt.Equals, MigrationFileTxModeNone)
	c.Assert(postgresMode.source, qt.Equals, migrationFileTxModeSourcePtah)
	mysqlDownMode := migration.parsedDownTxModeForDialect(platform.MySQL)
	c.Assert(mysqlDownMode.err, qt.IsNil)
	c.Assert(mysqlDownMode.mode, qt.Equals, MigrationFileTxModeUnspecified)
	postgresDownMode := migration.parsedDownTxModeForDialect(platform.Postgres)
	c.Assert(postgresDownMode.err, qt.IsNil)
	c.Assert(postgresDownMode.mode, qt.Equals, MigrationFileTxModeNone)

	migration.UpTxMode = MigrationFileTxModeFile
	migration.DownTxMode = MigrationFileTxModeFile
	overridden := migration.parsedUpTxModeForDialect(platform.Postgres)
	c.Assert(overridden.mode, qt.Equals, MigrationFileTxModeFile)
	c.Assert(overridden.err, qt.IsNil)
	overriddenDown := migration.parsedDownTxModeForDialect(platform.Postgres)
	c.Assert(overriddenDown.mode, qt.Equals, MigrationFileTxModeFile)
	c.Assert(overriddenDown.err, qt.IsNil)
}
