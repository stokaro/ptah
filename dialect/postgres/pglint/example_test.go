package pglint_test

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/dialect/postgres/pglint"
)

// ExampleService shows the PostgreSQL owner answering one lint run: a host
// reads the rules as data, here the TX family, then asks for the findings of
// the codes the run enables over every file in one call.
func ExampleService() {
	for _, rule := range pglint.Rules() {
		if strings.HasPrefix(rule.Code, "TX") {
			fmt.Println(rule.Code, rule.Severity, rule.Title)
		}
	}

	request := lintext.CheckRequest{
		Target: lintext.Target{Name: "postgres"},
		Codes:  []string{"TX201"},
		Files: []lintext.File{{Name: "0000000001_add.up.sql", Version: 1, Up: true, Statements: []lintext.Statement{
			{Start: 0, End: 5, Line: 1, SQL: "BEGIN", Words: []string{"BEGIN"}},
		}}},
	}
	result, err := pglint.Service{}.CheckMigrations(context.Background(), request)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, finding := range result.Findings {
		fmt.Println(finding.Code, request.Files[finding.File].Name, "statement", finding.Statement)
	}
	// Output:
	// TX101 warning transactional and non-transactional statements mixed
	// TX201 warning transaction block embedded in migration
	// TX201 0000000001_add.up.sql statement 0
}
