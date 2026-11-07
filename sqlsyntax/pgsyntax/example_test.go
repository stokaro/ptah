package pgsyntax_test

import (
	"fmt"

	"github.com/stokaro/ptah/sqlsyntax/pgsyntax"
)

// ExampleStatements splits a migration body into its statements: the
// semicolons inside the dollar-quoted function body and the comment do not
// end a statement.
func ExampleStatements() {
	body := `CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql;
/* a comment; */
SELECT f();`

	statements, err := pgsyntax.Statements(body, pgsyntax.Options{})
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, statement := range statements {
		fmt.Println(statement.Text)
	}
	// Output:
	// CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql
	// SELECT f()
}

// ExampleQuoteIdentifier quotes a name so that its case and its quote survive.
func ExampleQuoteIdentifier() {
	fmt.Println(pgsyntax.QuoteIdentifier(`Order "Lines"`))
	fmt.Println(pgsyntax.QuoteString(`it's C:\tmp`))
	// Output:
	// "Order ""Lines"""
	// E'it''s C:\\tmp'
}
