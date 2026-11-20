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
		if strings.HasPrefix(rule.Name, "TX") {
			fmt.Println(rule.Name, rule.Severity, rule.Title)
		}
	}

	request := lintext.CheckRequest{
		Target: lintext.Target{Name: "postgres"},
		Names:  []string{"transaction/mixed-statements"},
		Files: []lintext.File{{Name: "0000000001_add.up.sql", Version: 1, Up: true, Statements: []lintext.Statement{
			{Start: 0, End: 24, Line: 1, SQL: "CREATE TABLE t (id int);", Words: []string{"CREATE", "TABLE", "t", "(", "id", "int", ")"}},
			{Start: 25, End: 65, Line: 2, SQL: "CREATE INDEX CONCURRENTLY i ON t (id);",
				Words: []string{"CREATE", "INDEX", "CONCURRENTLY", "i", "ON", "t", "(", "id", ")"}},
		}}},
	}
	result, err := pglint.Service{}.CheckMigrations(context.Background(), request)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, finding := range result.Findings {
		fmt.Println(finding.Name, request.Files[finding.File].Name, "statement", finding.Statement)
	}
	// Output:
	// transaction/mixed-statements warning transactional and non-transactional statements mixed
	// transaction/mixed-statements 0000000001_add.up.sql statement 1
}
