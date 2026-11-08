package pgsyntax_test

import (
	"fmt"

	"ptah.run/sqlsyntax/pgsyntax"
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

// ExampleValue reads a Unicode escape string continued on the next line, with
// a UESCAPE clause naming its escape character: one constant over five tokens,
// which a reader that keeps one token per constant spans as one.
func ExampleValue() {
	text := "U&'d!0061'\n't!+000021' UESCAPE '!' AS greeting"

	tokens, err := pgsyntax.Scan(text, pgsyntax.Options{})
	if err != nil {
		fmt.Println(err)
		return
	}
	value, next, ok := pgsyntax.Value(text, tokens, 0, pgsyntax.Options{})
	if !ok {
		fmt.Println("no value")
		return
	}
	fmt.Println(value)
	fmt.Printf("%q\n", text[tokens[0].Start:tokens[next-1].End])
	// Output:
	// dat!
	// "U&'d!0061'\n't!+000021' UESCAPE '!'"
}
