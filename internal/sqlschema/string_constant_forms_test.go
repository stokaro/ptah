package sqlschema_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// stringConstantDocument writes one string constant into each place a
// PostgreSQL schema file takes one and Ptah models the text: an enum label, a
// column default and a comment.
const stringConstantDocument = "CREATE TYPE mood AS ENUM (%[1]s, 'z');\n" +
	"CREATE TABLE t (c text DEFAULT %[1]s);\n" +
	"COMMENT ON COLUMN t.c IS %[1]s;\n"

// columnC answers the column c the document declares.
func columnC(database schemamodel.Database) schemamodel.Field {
	for _, field := range database.Fields {
		if field.Name == "c" {
			return field
		}
	}
	return schemamodel.Field{}
}

// firstEnumLabel answers the first label of the document's enum.
func firstEnumLabel(database schemamodel.Database) string {
	if len(database.Enums) == 0 || len(database.Enums[0].Values) == 0 {
		return ""
	}
	return database.Enums[0].Values[0]
}

// Each spelling is one string constant to PostgreSQL 18.6, which stores the
// text on the right for each of them, whether it is a comment, an enum label
// or a column default. Each reads as that text here too, in every place, and
// the default reads as the single-quoted spelling of the same text does
// (stokaro/ptah#3731).
func TestRead_StringConstantSpellings_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		spelling string
		want     string
	}{
		{name: "a continued string", spelling: "'first part, '\n'second part'", want: "first part, second part"},
		{name: "a line comment in the gap", spelling: "'a'  -- note\n  'b'", want: "ab"},
		{name: "a dollar-quoted string", spelling: "$c$dollar text$c$", want: "dollar text"},
		{name: "a dollar-quoted apostrophe", spelling: "$$it's$$", want: "it's"},
		{name: "a doubled quote", spelling: "'it''s'", want: "it's"},
		{name: "an escape string", spelling: `E'it\'s a\ttab\\ \x41'`, want: "it's a\ttab\\ A"},
		{name: "a continued escape string", spelling: "E'a\\t'\n'b\\t'", want: "a\tb\t"},
		{name: "a Unicode escape string", spelling: `U&'d\0061t\+000061'`, want: "data"},
		{name: "a Unicode escape string with its escape character", spelling: `U&'d!0061t!+000061' UESCAPE '!'`, want: "data"},
		{name: "a continued Unicode escape string", spelling: "U&'d\\0061'\n'\\0074a'", want: "data"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plain, _, err := sqlschema.Read(
				[]byte(fmt.Sprintf(stringConstantDocument, "'"+strings.ReplaceAll(test.want, "'", "''")+"'")), "postgres",
			)
			c.Assert(err, qt.IsNil)

			database, _, err := sqlschema.Read([]byte(fmt.Sprintf(stringConstantDocument, test.spelling)), "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(columnC(database).Comment, qt.Equals, test.want)
			c.Assert(firstEnumLabel(database), qt.Equals, test.want)
			c.Assert(columnC(database).Default, qt.Equals, columnC(plain).Default)
			c.Assert(columnC(database).DefaultExpr, qt.Equals, columnC(plain).DefaultExpr)
		})
	}
}

// Each document is one PostgreSQL 18.6 refuses, with a syntax error or an
// invalid escape, and it is refused here rather than read as some other text.
// The continued string on CockroachDB is stricter than that server, which
// joins it: its lexer does not share PostgreSQL's rule, so the file is refused
// rather than read by a rule the server may not apply.
func TestRead_StringConstantSpellings_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		document string
		dialect  string
		wantErr  string
	}{
		{
			name:     "two strings on one line",
			document: "CREATE TABLE t (c int);\nCOMMENT ON COLUMN t.c IS 'a' 'b';",
			dialect:  "postgres",
			wantErr:  `expected SQL keyword, got String at position \d+`,
		},
		{
			name:     "a block comment in the gap",
			document: "CREATE TABLE t (c int);\nCOMMENT ON COLUMN t.c IS 'a' /* x */\n'b';",
			dialect:  "postgres",
			wantErr:  `expected SQL keyword, got String at position \d+`,
		},
		{
			name:     "a dollar-quoted string followed by a quoted one",
			document: "CREATE TABLE t (c int);\nCOMMENT ON COLUMN t.c IS $$a$$\n'b';",
			dialect:  "postgres",
			wantErr:  `expected SQL keyword, got String at position \d+`,
		},
		{
			name:     "a quoted string followed by an escape string",
			document: "CREATE TABLE t (c int);\nCOMMENT ON COLUMN t.c IS 'a'\nE'b';",
			dialect:  "postgres",
			wantErr:  `expected SQL keyword, got String at position \d+`,
		},
		{
			name:     "a space after U&",
			document: "CREATE TABLE t (c int);\nCOMMENT ON COLUMN t.c IS U& '\\0041';",
			dialect:  "postgres",
			wantErr:  `expected a string constant for the comment text at position \d+`,
		},
		{
			name:     "a national character string",
			document: "CREATE TABLE t (c int);\nCOMMENT ON COLUMN t.c IS N'nat';",
			dialect:  "postgres",
			wantErr:  `expected a string constant for the comment text at position \d+`,
		},
		{
			name:     "an identifier",
			document: "CREATE TABLE t (c int);\nCOMMENT ON COLUMN t.c IS \"nat\";",
			dialect:  "postgres",
			wantErr:  `expected a string constant for the comment text at position \d+`,
		},
		{
			name:     "an invalid Unicode escape in a comment",
			document: "CREATE TABLE t (c int);\nCOMMENT ON COLUMN t.c IS U&'a\\b';",
			dialect:  "postgres",
			wantErr:  `U&'a\\b' is not a string constant the server reads, for the comment text at position \d+`,
		},
		{
			name:     "a hex digit as the escape character",
			document: "CREATE TABLE t (c int);\nCOMMENT ON COLUMN t.c IS U&'d!0061' UESCAPE 'a';",
			dialect:  "postgres",
			wantErr:  `U&'d!0061' UESCAPE 'a' is not a string constant the server reads, for the comment text at position \d+`,
		},
		{
			name:     "an invalid Unicode escape in an enum label",
			document: "CREATE TYPE mood AS ENUM (U&'\\0000');",
			dialect:  "postgres",
			wantErr:  `U&'\\0000' is not a string constant the server reads, for an enum label at position \d+`,
		},
		{
			name:     "an invalid escape in a column default",
			document: "CREATE TABLE t (c text DEFAULT E'\\u00');",
			dialect:  "postgres",
			wantErr:  `expected default value: E'\\u00' is not a string constant the server reads, for the default value at position \d+`,
		},
		{
			name:     "a continued string on CockroachDB",
			document: "CREATE TABLE t (c int);\nCOMMENT ON COLUMN t.c IS 'a'\n'b';",
			dialect:  "cockroachdb",
			wantErr:  `expected SQL keyword, got String at position \d+`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.document), test.dialect)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Fields, qt.HasLen, 0)
		})
	}
}
