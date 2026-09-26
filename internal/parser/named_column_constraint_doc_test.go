package parser_test

import (
	"os"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// sqlSchemaPage is the page whose failure-mode entry this test holds to the
// parser.
const sqlSchemaPage = "../../docs/site/src/content/docs/schema/sql.md"

// TestNamedColumnConstraintRefusal_IsQuotedAsThePageSaysItIs binds the quoted
// refusal to the code that produces it.
//
// A page that quotes program output is correct at the instant it was written
// and unowned afterwards: the next change to the message leaves a paragraph
// that answers the reader's question wrongly and with authority. This drives
// the real parser and compares what it says with what the page claims it says,
// so the two cannot drift apart (stokaro/ptah#2161).
func TestNamedColumnConstraintRefusal_IsQuotedAsThePageSaysItIs(t *testing.T) {
	c := qt.New(t)

	_, err := parser.NewParser(`CREATE TABLE t (b INTEGER CONSTRAINT c_x DEFAULT 1);`).Parse()

	c.Assert(err, qt.IsNotNil)
	c.Assert(collapseWhitespace(readSQLSchemaPage(c)), qt.Contains, collapseWhitespace(err.Error()),
		qt.Commentf("the refusal changed; update %s", sqlSchemaPage))
}

// TestNamedColumnConstraintRefusal_TheFixtureOnThePageIsTheOneThatFails is the
// control.
//
// The assertion above compares a message against a page, and a page that
// stopped containing the example would fail it -- but a page whose example SQL
// no longer produces that message would not. This drives the statement the page
// prints and asserts it is refused, so the pair stays a pair.
func TestNamedColumnConstraintRefusal_TheFixtureOnThePageIsTheOneThatFails(t *testing.T) {
	c := qt.New(t)

	page := readSQLSchemaPage(c)
	const fixture = "CREATE TABLE t (b INTEGER CONSTRAINT c_x DEFAULT 1);"

	c.Assert(page, qt.Contains, fixture)

	_, err := parser.NewParser(fixture).Parse()

	c.Assert(err, qt.IsNotNil)
}

// TestMySQLFamilyRefusal_IsQuotedAsThePageSaysItIs binds the page's MySQL
// family examples to the refusals the parser gives for them, as the two tests
// above bind the DEFAULT one: the page has to print each fixture, and each
// fixture has to produce the message the page quotes.
func TestMySQLFamilyRefusal_IsQuotedAsThePageSaysItIs(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		fixture string
	}{
		{
			name:    "a named column constraint",
			dialect: platform.MySQL,
			fixture: "CREATE TABLE t (a INT CONSTRAINT uq UNIQUE);",
		},
		{
			name:    "a column-level REFERENCES",
			dialect: platform.MySQL,
			fixture: "CREATE TABLE child (a INT REFERENCES parents (id));",
		},
		{
			name:    "a REFERENCES in MODIFY",
			dialect: platform.MariaDB,
			fixture: "ALTER TABLE c MODIFY a INT REFERENCES p (id);",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			page := readSQLSchemaPage(c)

			c.Assert(page, qt.Contains, test.fixture)

			_, err := parser.NewParser(test.fixture, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.IsNotNil)
			c.Assert(collapseWhitespace(page), qt.Contains, collapseWhitespace(err.Error()),
				qt.Commentf("the refusal changed; update %s", sqlSchemaPage))
		})
	}
}

// readSQLSchemaPage reads the documentation page the tests in this file
// compare against.
func readSQLSchemaPage(c *qt.C) string {
	c.Helper()

	body, err := os.ReadFile(sqlSchemaPage)
	c.Assert(err, qt.IsNil)
	c.Assert(len(body) > 0, qt.IsTrue)
	return string(body)
}

// collapseWhitespace folds every run of whitespace to one space, so a message
// the page hard-wraps and a message the parser emits on one line compare equal.
func collapseWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
