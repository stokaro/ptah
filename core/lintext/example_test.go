package lintext_test

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/lintext"
)

// ExampleChecks shows an in-process owner publishing one rule as data and
// answering a lint run's request with Go checks behind it. A host reads
// Rules without running anything, and calls CheckMigrations once with every
// file of the run.
func ExampleChecks() {
	checks := lintext.Checks{{
		Rule: lintext.Rule{Code: "XY101", Title: "Truncates a table", Severity: lintext.Error, Targets: []string{"postgres"}},
		Statement: func(_ lintext.Target, _ *lintext.File, statement *lintext.Statement) (bool, string) {
			return slices.Contains(statement.Words, "TRUNCATE"), "TRUNCATE removes every row of the table"
		},
	}}
	for _, rule := range checks.Rules() {
		fmt.Println(rule.Code, rule.Severity, rule.Targets)
	}

	request := lintext.CheckRequest{
		Target: lintext.Target{Name: "postgres"},
		Codes:  []string{"XY101"},
		Files: []lintext.File{{Name: "2.up.sql", Version: 2, Up: true, Statements: []lintext.Statement{
			{Start: 0, End: 16, Line: 1, SQL: "TRUNCATE TABLE a", Words: []string{"TRUNCATE", "TABLE", "A"}},
		}}},
	}
	result, err := checks.CheckMigrations(context.Background(), request)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, finding := range result.Findings {
		fmt.Printf("%s %s statement %d: %s\n", finding.Code, request.Files[finding.File].Name, finding.Statement, finding.Message)
	}
	// Output:
	// XY101 error [postgres]
	// XY101 2.up.sql statement 0: TRUNCATE removes every row of the table
}
