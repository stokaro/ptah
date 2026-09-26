//go:build integration

package dbschema_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/schemafile"
)

// stringConstantFormsDocument puts one string constant in each place a
// PostgreSQL schema file takes one and Ptah keeps the text: an enum label, a
// column default and a column comment. %[2]s is the constant.
const stringConstantFormsDocument = `
CREATE TYPE "%[1]s".mood AS ENUM (%[2]s, 'z');
CREATE TABLE "%[1]s".t (c text DEFAULT %[2]s);
COMMENT ON COLUMN "%[1]s".t.c IS %[2]s;
`

// storedStringConstants answers what the server stored for the document's
// comment, first enum label and column default. The default is read by
// using it, which is the text a reader of the column sees.
func storedStringConstants(c *qt.C, conn *dbschema.DatabaseConnection, schemaName string) []string {
	c.Helper()
	var comment, label, value string
	c.Assert(conn.QueryRowContext(c.Context(), fmt.Sprintf(`
		SELECT col_description('"%[1]s".t'::regclass, 1),
		       (SELECT enumlabel FROM pg_enum WHERE enumtypid = '"%[1]s".mood'::regtype ORDER BY enumsortorder LIMIT 1)`,
		schemaName)).Scan(&comment, &label), qt.IsNil)
	c.Assert(conn.QueryRowContext(c.Context(), fmt.Sprintf(
		`INSERT INTO "%[1]s".t DEFAULT VALUES RETURNING c`, schemaName)).Scan(&value), qt.IsNil)
	return []string{comment, label, value}
}

// Each spelling is applied by the server itself, as psql applies a file, and
// the document then compares clean against what the server stored: Ptah
// reads each spelling as the one string the server does, in a comment, an
// enum label and a column default alike (stokaro/ptah#3731). The stored text
// is read back first, so a row cannot pass by comparing a misread against
// itself.
func TestPostgresLiveStringConstantSpellingsCompareClean(t *testing.T) {
	tests := []struct {
		name     string
		spelling string
		want     string
	}{
		{name: "a continued string", spelling: "'first part, '\n'second part'", want: "first part, second part"},
		{name: "a line comment in the gap", spelling: "'a'  -- note\n  'b'", want: "ab"},
		{name: "a dollar-quoted string", spelling: "$c$it's dollar text$c$", want: "it's dollar text"},
		{name: "an escape string", spelling: `E'it\'s a\ttab\\ \x41'`, want: "it's a\ttab\\ A"},
		{name: "a continued escape string", spelling: "E'a\\t'\n'b\\t'", want: "a\tb\t"},
		{name: "a Unicode escape string", spelling: `U&'d\0061t\+000061 \\'`, want: `data \`},
		{name: "a Unicode escape string with its escape character", spelling: `U&'d!0061t!+000061' UESCAPE '!'`, want: "data"},
		{name: "a continued Unicode escape string", spelling: "U&'d\\0061'\n'\\0074a'", want: "data"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, schemaName := newFormsSchema(c)
			document := fmt.Sprintf(stringConstantFormsDocument, schemaName, test.spelling)
			_, err := conn.ExecContext(c.Context(), document)
			c.Assert(err, qt.IsNil)
			c.Assert(storedStringConstants(c, conn, schemaName), qt.DeepEquals, []string{test.want, test.want, test.want})

			statements := planFormsDocument(c, conn, schemaName, map[string]string{
				"schema.sql": fmt.Sprintf(stringConstantFormsDocument, "%[1]s", test.spelling),
			})

			c.Assert(statements, qt.HasLen, 0)
		})
	}
}

// The control: a continued string that joins to other text than the server
// holds plans the change, in each place, so the clean comparisons above are
// not a comparison that sees no text at all.
func TestPostgresLiveContinuedStringChangePlans(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)
	_, err := conn.ExecContext(c.Context(), fmt.Sprintf(stringConstantFormsDocument, schemaName, "'first part, other part'"))
	c.Assert(err, qt.IsNil)

	statements := planFormsDocument(c, conn, schemaName, map[string]string{
		"schema.sql": fmt.Sprintf(stringConstantFormsDocument, "%[1]s", "'first part, '\n'second part'"),
	})

	c.Assert(statements, qt.Any(qt.Matches), `(?s).*COMMENT ON COLUMN \S+ IS 'first part, second part'`)
	c.Assert(statements, qt.Any(qt.Matches), `(?s).*ALTER COLUMN "c" SET DEFAULT 'first part, second part'`)
	c.Assert(statements, qt.Any(qt.Matches), `CREATE TYPE \S+ AS ENUM \('first part, second part', 'z'\)`)
}

// Each document is one PostgreSQL refuses, and Ptah refuses it too rather than
// reading the constant some other way: the server's refusal is measured here,
// beside Ptah's, so the pair cannot drift apart unnoticed.
func TestPostgresLiveRefusedStringConstantSpellings(t *testing.T) {
	tests := []struct {
		name     string
		spelling string
	}{
		{name: "two strings on one line", spelling: "'a' 'b'"},
		{name: "a block comment in the gap", spelling: "'a' /* x */\n'b'"},
		{name: "a quoted string followed by an escape string", spelling: "'a'\nE'b'"},
		{name: "a space after U&", spelling: `U& '\0041'`},
		{name: "an escape that is not hex", spelling: `U&'a\b'`},
		{name: "a hex digit as the escape character", spelling: `U&'d!0061' UESCAPE 'a'`},
		{name: "an escaped zero character", spelling: `E'\u0000'`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, schemaName := newFormsSchema(c)
			_, serverErr := conn.ExecContext(c.Context(), fmt.Sprintf(stringConstantFormsDocument, schemaName, test.spelling))
			c.Assert(serverErr, qt.IsNotNil)

			declared, err := schemafile.LoadSources(
				[]schemafile.Source{{URL: writeFormsDocument(c, schemaName, map[string]string{
					"schema.sql": fmt.Sprintf(stringConstantFormsDocument, "%[1]s", test.spelling),
				})}},
				schemafile.Options{Dialect: platform.Postgres},
			)

			c.Assert(err, qt.IsNotNil)
			c.Assert(declared, qt.IsNil)
		})
	}
}
