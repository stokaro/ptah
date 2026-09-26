//go:build integration

package dbschema_test

import (
	"database/sql"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// domainDefaultDocument declares a domain with the DEFAULT %[2]s and a table
// with a column of it, so the default can be read by using it.
const domainDefaultDocument = `
CREATE DOMAIN "%[1]s".dm AS %[2]s;
CREATE TABLE "%[1]s".t (id integer, v "%[1]s".dm);
`

// domainDefaultInUse answers what a row of the document's table stores when
// the domain default fills its column: the text a reader of the column sees,
// NULL when the domain has no default.
func domainDefaultInUse(c *qt.C, conn *dbschema.DatabaseConnection, schemaName string) sql.NullString {
	c.Helper()
	var value sql.NullString
	c.Assert(conn.QueryRowContext(c.Context(), fmt.Sprintf(
		`INSERT INTO "%[1]s".t (id) VALUES (1) RETURNING v::text`, schemaName)).Scan(&value), qt.IsNil)
	return value
}

// Each domain is created by the server itself, as psql applies a file, and the
// document then compares clean against what the server stored. The default is
// read back by using it first, so a row cannot pass by comparing a misread
// against itself. Kept with its quotes, a string default plans
// ALTER DOMAIN ... SET DEFAULT '''ab''' against the domain it declares
// (stokaro/ptah#3740).
func TestPostgresLiveDomainDefaultSpellingsCompareClean(t *testing.T) {
	tests := []struct {
		name   string
		domain string
		want   sql.NullString
	}{
		{name: "a string", domain: "text DEFAULT 'ab'", want: sql.NullString{String: "ab", Valid: true}},
		{name: "a doubled quote", domain: "text DEFAULT 'it''s'", want: sql.NullString{String: "it's", Valid: true}},
		{name: "a backslash", domain: `text DEFAULT 'a\b'`, want: sql.NullString{String: `a\b`, Valid: true}},
		{name: "a continued string", domain: "text DEFAULT 'a'\n'b'", want: sql.NullString{String: "ab", Valid: true}},
		{name: "a dollar-quoted string", domain: "text DEFAULT $$ab$$", want: sql.NullString{String: "ab", Valid: true}},
		{name: "an escape string", domain: `text DEFAULT E'a\x62'`, want: sql.NullString{String: "ab", Valid: true}},
		{name: "a Unicode escape string", domain: `text DEFAULT U&'\0061b'`, want: sql.NullString{String: "ab", Valid: true}},
		{name: "a string on a varchar domain", domain: "varchar(10) DEFAULT 'ab'", want: sql.NullString{String: "ab", Valid: true}},
		{name: "a string with a cast", domain: "text DEFAULT 'ab'::text", want: sql.NullString{String: "ab", Valid: true}},
		{name: "a negative number", domain: "integer DEFAULT -1", want: sql.NullString{String: "-1", Valid: true}},
		{name: "NULL", domain: "text DEFAULT NULL"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, schemaName := newFormsSchema(c)
			_, err := conn.ExecContext(c.Context(), fmt.Sprintf(domainDefaultDocument, schemaName, test.domain))
			c.Assert(err, qt.IsNil)
			c.Assert(domainDefaultInUse(c, conn, schemaName), qt.Equals, test.want)

			statements := planFormsDocument(c, conn, schemaName, map[string]string{
				"schema.sql": fmt.Sprintf(domainDefaultDocument, "%[1]s", test.domain),
			})

			c.Assert(statements, qt.HasLen, 0)
		})
	}
}

// The control: a document whose string default differs from the server's
// plans the change, and the plan, applied, makes the column default to the
// declared text rather than to the text with its quotes.
func TestPostgresLiveDomainDefaultChangeAppliesTheText(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)
	_, err := conn.ExecContext(c.Context(), fmt.Sprintf(domainDefaultDocument, schemaName, "text DEFAULT 'ab'"))
	c.Assert(err, qt.IsNil)
	document := map[string]string{"schema.sql": fmt.Sprintf(domainDefaultDocument, "%[1]s", "text DEFAULT 'it''s'")}

	statements := planFormsDocument(c, conn, schemaName, document)

	c.Assert(statements, qt.HasLen, 1)
	c.Assert(statements[0], qt.Matches, `(?s).*ALTER DOMAIN "[^"]+"\."dm" SET DEFAULT 'it''s';?`)
	_, err = conn.ExecContext(c.Context(), statements[0])
	c.Assert(err, qt.IsNil)
	c.Assert(domainDefaultInUse(c, conn, schemaName), qt.Equals, sql.NullString{String: "it's", Valid: true})
	c.Assert(planFormsDocument(c, conn, schemaName, document), qt.HasLen, 0)
}

// A domain the plan creates takes the declared text as its default, and the
// document then compares clean against it.
func TestPostgresLiveDomainDefaultCreatedFromTheDocument(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)

	settleFormsDocument(c, conn, schemaName, map[string]string{
		"schema.sql": fmt.Sprintf(domainDefaultDocument, "%[1]s", "text DEFAULT $q$it's$q$"),
	})

	c.Assert(domainDefaultInUse(c, conn, schemaName), qt.Equals, sql.NullString{String: "it's", Valid: true})
}
