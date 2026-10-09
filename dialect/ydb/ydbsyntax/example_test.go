package ydbsyntax_test

import (
	"fmt"

	"ptah.run/dialect/ydb/ydbsyntax"
)

// ExampleQuoteIdentifier shows that paths and literal dots keep their spelling,
// while embedded backticks cannot terminate the quoted identifier.
func ExampleQuoteIdentifier() {
	fmt.Println(ydbsyntax.QuoteIdentifier("jobs.daily/tick`name"))
	// Output:
	// `jobs.daily/tick\`name`
}

// ExampleStringLiteral shows the shared spelling used by column defaults and
// workload settings. Quotes, backslashes, and newlines remain literal data.
func ExampleStringLiteral() {
	fmt.Println(ydbsyntax.StringLiteral("it's\\ready\n"))
	// Output:
	// 'it\'s\\ready\n'
}
