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

// renameOwner is an out-of-process owner's answer, written by hand: one
// baseline rule that reports the type a renamed column carried, which the
// rename statement does not say.
type renameOwner struct{}

func (renameOwner) CheckMigrations(_ context.Context, request lintext.CheckRequest) (lintext.CheckResult, error) {
	result := lintext.CheckResult{Complete: true}
	for index, file := range request.Files {
		for position, statement := range file.Statements {
			if !file.Up || !slices.Contains(statement.Words, "RENAME") {
				continue
			}
			// Asked on every call: the host decides from it what to read.
			result.BaselineSubjects = append(result.BaselineSubjects,
				lintext.BaselineSubject{Code: "XY301", File: index, Statements: []int{position}})
			if request.Baseline == nil {
				continue
			}
			for _, column := range request.Baseline.Columns {
				if column.Version == file.Version && column.Name == "id" {
					result.Findings = append(result.Findings, lintext.Finding{Code: "XY301", File: index, Statement: position,
						Message: "the renamed column was " + column.DataType})
				}
			}
		}
	}
	return result, nil
}

// ExampleService shows the two calls a host makes for a rule that reads the
// schema state a version starts from. The first carries no baseline, and the
// owner names the statement it wants the state for. The host reads that
// version's starting state from a dev database and calls again with it, and
// the owner's second answer is the one the host reports.
func ExampleService() {
	request := lintext.CheckRequest{
		Target: lintext.Target{Name: "postgres"},
		Codes:  []string{"XY301"},
		Files: []lintext.File{{Name: "0000000002_rename.up.sql", Version: 2, Up: true, Statements: []lintext.Statement{{
			Start: 0, End: 41, Line: 1, SQL: "ALTER TABLE users RENAME COLUMN id TO oid",
			Words: []string{"ALTER", "TABLE", "USERS", "RENAME", "COLUMN", "ID", "TO", "OID"},
		}}}},
	}
	var owner lintext.Service = renameOwner{}

	first, err := owner.CheckMigrations(context.Background(), request)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("first call:", len(first.Findings), "findings, subjects", first.BaselineSubjects)

	request.Baseline = &lintext.Baseline{Columns: []lintext.BaselineColumn{{Version: 2, Table: "users", Name: "id", DataType: "bigint"}}}
	second, err := owner.CheckMigrations(context.Background(), request)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, finding := range second.Findings {
		fmt.Println("second call:", finding.Code, request.Files[finding.File].Name, finding.Message)
	}
	// Output:
	// first call: 0 findings, subjects [{XY301 0 [0]}]
	// second call: XY301 0000000002_rename.up.sql the renamed column was bigint
}
