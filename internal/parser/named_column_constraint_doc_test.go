package parser_test

import (
	"os"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/parser"
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

	_, err := parser.NewParser(`CREATE TABLE t (b INTEGER CONSTRAINT c_x NOT NULL);`).Parse()

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
	const fixture = "CREATE TABLE t (b INTEGER CONSTRAINT c_x NOT NULL);"

	c.Assert(page, qt.Contains, fixture)

	_, err := parser.NewParser(fixture).Parse()

	c.Assert(err, qt.IsNotNil)
}

// readSQLSchemaPage reads the documentation page the two tests above compare
// against.
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
