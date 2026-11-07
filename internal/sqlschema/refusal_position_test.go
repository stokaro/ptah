package sqlschema_test

import (
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/sqlext"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
)

// statementsBefore stands in front of each refused statement, so it stands on
// line 7. An owner reads the span it claimed from its own text and adds the
// span's offset; an owner that left that out would report a place on the first
// lines of this text.
const statementsBefore = "-- Каталог: таблицы\nCREATE TABLE catalog (\n  note TEXT DEFAULT 'one\ntwo'\n);\n\n"

// Every owner's parser reports its refusal at its place in the document, on the
// dialect that reads it. wantAt is what the byte offset finds in the document.
func TestReadWith_RefusalPositionOfEachOwner(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		statement string
		wantErr   string
		wantAt    string
	}{
		{
			name: "the shared grammar", dialect: platform.Postgres,
			statement: "CREATE TABLE b (id INTEGER) BOGUS;",
			wantErr:   `line 7, column 29: unsupported table option: BOGUS`,
			wantAt:    "BOGUS",
		},
		{
			name: "a PostgreSQL extension", dialect: platform.Postgres,
			statement: "CREATE EXTENSION pg_trgm CASCADE;",
			wantErr:   `line 7, column 26: unsupported CREATE EXTENSION option "CASCADE"`,
			wantAt:    "CASCADE",
		},
		{
			name: "a PostgreSQL policy", dialect: platform.Postgres,
			statement: "CREATE POLICY p ON t BOGUS;",
			wantErr:   `line 7, column 22: unsupported CREATE POLICY clause: BOGUS`,
			wantAt:    "BOGUS",
		},
		{
			name: "a PostgreSQL range type", dialect: platform.Postgres,
			statement: "CREATE TYPE r AS RANGE (subtype = int4, bogus = 1);",
			wantErr:   `line 7, column 41: unsupported range option "bogus"`,
			wantAt:    "bogus",
		},
		{
			name: "a PostgreSQL domain", dialect: platform.Postgres,
			statement: `CREATE DOMAIN d text COLLATE "C";`,
			wantErr:   `line 7, column 22: unsupported domain clause "COLLATE"`,
			wantAt:    "COLLATE",
		},
		{
			name: "PostgreSQL default privileges", dialect: platform.Postgres,
			statement: "ALTER DEFAULT PRIVILEGES FOR NOPE r IN SCHEMA s GRANT SELECT ON TABLES TO x;",
			wantErr:   `line 7, column 30: expected ROLE or USER after FOR in ALTER DEFAULT PRIVILEGES`,
			wantAt:    "NOPE",
		},
		{
			name: "a PostgreSQL exclusion constraint", dialect: platform.Postgres,
			statement: "CREATE TABLE t (a int, EXCLUDE USING btree (a WITH =) ENFORCED);",
			wantErr:   `line 7, column 24: EXCLUDE: invalid feature value: PostgreSQL EXCLUDE: unrepresented or unexpected clause ENFORCED`,
			wantAt:    "EXCLUDE",
		},
		{
			name: "a PostgreSQL partition", dialect: platform.Postgres,
			statement: "CREATE TABLE e PARTITION OF events (a int) DEFAULT;",
			wantErr:   `line 7, column 16: table e: .*`,
			wantAt:    "PARTITION",
		},
		{
			name: "a PostgreSQL procedure", dialect: platform.Postgres,
			statement: "CREATE PROCEDURE p() EXTERNAL BOGUS AS $$ SELECT 1 $$;",
			wantErr:   `line 7, column 22: expected SECURITY after EXTERNAL`,
			wantAt:    "EXTERNAL",
		},
		{
			name: "a MySQL column option", dialect: platform.MySQL,
			statement: "CREATE TABLE t (v text CHARACTER SET 'utf8');",
			wantErr:   `line 7, column 38: expected a character set name after CHARACTER SET`,
			wantAt:    "'utf8'",
		},
		{
			name: "a MySQL index option", dialect: platform.MySQL,
			statement: "CREATE TABLE t (a int, KEY k (a) KEY_BLOCK_SIZE = x);",
			wantErr:   `line 7, column 34: KEY_BLOCK_SIZE: expected a non-negative integer`,
			wantAt:    "KEY_BLOCK_SIZE",
		},
		{
			name: "a SQL Server routine option", dialect: platform.SQLServer,
			statement: "CREATE PROCEDURE dbo.p WITH EXECUTE AS auditor AS SELECT 1;",
			wantErr:   `line 7, column 29: invalid feature value: EXECUTE AS auditor of dbo\.p: EXECUTE AS names CALLER, SELF, OWNER or a user name in quotes, not auditor`,
			wantAt:    "EXECUTE AS auditor",
		},
		{
			name: "a SQL Server trigger", dialect: platform.SQLServer,
			statement: "CREATE TRIGGER trg ON DATABASE FOR CREATE_TABLE AS SELECT 1;",
			wantErr:   `line 7, column 23: CREATE TRIGGER trg ON DATABASE is a DDL or logon trigger, .*`,
			wantAt:    "DATABASE",
		},
		{
			name: "a ClickHouse index", dialect: platform.ClickHouse,
			statement: "ALTER TABLE t ADD INDEX i;",
			wantErr:   `line 7, column 26: expected an indexed expression after ADD INDEX i`,
			wantAt:    ";",
		},
		{
			name: "a ClickHouse row policy", dialect: platform.ClickHouse,
			statement: "CREATE ROW POLICY p ON t BOGUS;",
			wantErr:   `line 7, column 26: ClickHouse ROW POLICY: unsupported row policy clause BOGUS \(got ""\)`,
			wantAt:    "BOGUS",
		},
		{
			name: "a Spanner row deletion policy", dialect: platform.Spanner,
			statement: "CREATE TABLE t (id bigint PRIMARY KEY, ts timestamptz) TTL '30 days' ON ts;",
			wantErr:   `line 7, column 60: expected INTERVAL after TTL`,
			wantAt:    "'30 days'",
		},
		{
			name: "a SQLite virtual table", dialect: platform.SQLite,
			statement: "CREATE VIRTUAL TABLE docs fts5(x);",
			wantErr:   `line 7, column 27: expected 'USING', got 'fts5'`,
			wantAt:    "fts5",
		},
		{
			name: "an Oracle domain", dialect: platform.Oracle,
			statement: "CREATE DOMAIN d AS NUMBER CONSTRAINT c CHECK (VALUE > 0);",
			wantErr:   `line 7, column 27: unsupported domain clause "CONSTRAINT"`,
			wantAt:    "CONSTRAINT",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			text := statementsBefore + test.statement

			_, _, err := sqlschema.ReadWith(t.Context(), []byte(text), test.dialect, nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			at, ok := errors.AsType[*ptaherr.ParseError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(strings.HasPrefix(text[at.Offset:], test.wantAt), qt.IsTrue, qt.Commentf("offset %d finds %q", at.Offset, text[at.Offset:]))
		})
	}
}

// A statement its owner declined is read again by itself, from its own text. Its
// refusal reports its place in the document all the same.
func TestReadWith_RefusalPositionOfADeclinedStatement(t *testing.T) {
	c := qt.New(t)
	var requests []sqlext.Request
	text := statementsBefore + "COMMENT ON TYPE other IS 5;"

	_, _, err := sqlschema.ReadWith(t.Context(), []byte(text), "custom", nil, sqlschema.Options{Owners: shapeOwners(shapeParser{requests: &requests})})

	c.Assert(err, qt.ErrorMatches, `line 7, column 26: expected a string constant for the comment text`)
	at, ok := errors.AsType[*ptaherr.ParseError](err)
	c.Assert(ok, qt.IsTrue)
	c.Assert(text[at.Offset:], qt.Equals, "5;")
}
